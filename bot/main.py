"""
main.py — FastAPI application: lifespan, REST routes, WebSocket hub.

Run with:
    python main.py
or:
    uvicorn main:app --host 0.0.0.0 --port 8765 --reload
"""
import asyncio
import json
import logging
import signal
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from config import load_config
from database import (
    get_performance,
    get_sessions,
    get_signal_log,
    init_db,
    set_config_bulk,
)
from engine.indicators import compute_all
from engine.orderflow import OrderFlowAnalyzer
from engine.trading import TradingEngine
from exchange.binance_rest import BinanceRestClient
from exchange.binance_ws import BinanceWebSocket
from notifications import notify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Global singletons (initialised in lifespan)
# ---------------------------------------------------------------------------

_rest: Optional[BinanceRestClient] = None
_ws:   Optional[BinanceWebSocket]  = None
_flow  = OrderFlowAnalyzer(window_seconds=30, depth_levels=5)
_engine: Optional[TradingEngine]   = None

# Connected WebSocket clients
_clients: Set[WebSocket] = set()

# Latest price feed
_last_price: float = 0.0
_last_candles_fetch: float = 0.0
_last_symbol_scan: float = 0.0
_last_price_rest_fetch: float = 0.0   # throttle REST mark-price fallback
_CANDLE_REFRESH_S = 30.0   # fetch new candles every N seconds
_PRICE_REST_FALLBACK_S = 10.0  # only poll REST price if WS hasn't delivered in N seconds


# ---------------------------------------------------------------------------
# Broadcast helper
# ---------------------------------------------------------------------------

def _broadcast(msg: Dict) -> None:
    """Enqueue a message to all connected clients (fire-and-forget)."""
    asyncio.get_event_loop().call_soon(
        lambda: asyncio.ensure_future(_do_broadcast(msg))
    )


async def _do_broadcast(msg: Dict) -> None:
    data = json.dumps(msg)
    disconnected: Set[WebSocket] = set()
    for ws in list(_clients):
        try:
            await ws.send_text(data)
        except Exception:
            disconnected.add(ws)
    _clients.difference_update(disconnected)


# ---------------------------------------------------------------------------
# Ticker loop (called from trading ticker)
# ---------------------------------------------------------------------------

async def _ticker_loop() -> None:
    global _last_price, _last_candles_fetch, _last_symbol_scan, _last_price_rest_fetch
    cfg = await load_config()

    while True:
        try:
            cfg = await load_config()
            now = time.time()
            if _last_price:
                price = _last_price
            elif now - _last_price_rest_fetch >= _PRICE_REST_FALLBACK_S:
                # WS not yet delivering prices — fetch once via REST until it does
                price = await _rest.get_mark_price(cfg.symbol)
                _last_price_rest_fetch = now
            else:
                await asyncio.sleep(1.0)
                continue

            # Refresh candles periodically
            if now - _last_candles_fetch >= _CANDLE_REFRESH_S:
                raw = await _rest.get_klines(cfg.symbol, interval=cfg.timeframe, limit=200)
                candles = [
                    {
                        "open":   float(k[1]),
                        "high":   float(k[2]),
                        "low":    float(k[3]),
                        "close":  float(k[4]),
                        "volume": float(k[5]),
                        "time":   int(k[0]) // 1000,
                    }
                    for k in raw
                ]
                _engine.update_candles(candles)
                _last_candles_fetch = now

                # Broadcast candles + indicators to UI
                ind = _engine.last_indicators
                await _do_broadcast({
                    "type":    "candles",
                    "candles": candles[-100:],   # last 100 for chart
                })
                await _do_broadcast({
                    "type":       "indicators",
                    "indicators": _safe_ind(ind),
                })

            # Broadcast price tick
            await _do_broadcast({
                "type":  "price",
                "price": price,
                "symbol": cfg.symbol,
            })

            # Run trading tick
            await _engine.tick(cfg, price)

            # Auto-switch logic — throttled by scan_interval_s
            if cfg.auto_switch and _engine._session is None:
                if now - _last_symbol_scan >= cfg.scan_interval_s:
                    await _maybe_switch_symbol(cfg)
                    _last_symbol_scan = now

        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("ticker_loop error: %s", exc, exc_info=True)

        await asyncio.sleep(1.0)


def _safe_ind(ind: Dict) -> Dict:
    """Convert indicators dict to JSON-safe (None → null preserved)."""
    out: Dict[str, Any] = {}
    for k, v in ind.items():
        if isinstance(v, dict):
            out[k] = v
        elif v is None:
            out[k] = None
        else:
            out[k] = v
    return out


async def _maybe_switch_symbol(cfg) -> None:
    """Auto-switch to highest-volume symbol if auto_switch is enabled."""
    try:
        top = await _rest.get_top_movers(n=1)
        if not top:
            return
        new_sym = top[0]["symbol"]
        if new_sym != cfg.symbol:
            global _last_candles_fetch
            logger.info("Auto-switch: %s → %s", cfg.symbol, new_sym)
            await set_config_bulk({"symbol": new_sym})
            await _ws.switch_symbol(new_sym)
            _last_candles_fetch = 0.0
            await _do_broadcast({"type": "symbol_ready", "symbol": new_sym})
            await _do_broadcast({
                "type":       "notification",
                "text":       f"Auto-switched: {cfg.symbol} → {new_sym}",
            })
            await notify(cfg.discord_webhook, "AUTO_SWITCH", {
                "old_symbol": cfg.symbol,
                "new_symbol": new_sym,
                "paper":      cfg.paper_mode,
            })
    except Exception as exc:
        logger.warning("Auto-switch error: %s", exc)


# ---------------------------------------------------------------------------
# WS trade/depth callbacks
# ---------------------------------------------------------------------------

def _on_trade(event: Dict) -> None:
    global _last_price
    _last_price = event["price"]
    _flow.on_trade(event)
    asyncio.get_event_loop().call_soon(
        lambda: asyncio.ensure_future(_do_broadcast({
            "type":  "trade",
            "price": event["price"],
            "qty":   event["qty"],
            "side":  "sell" if event["buyer_maker"] else "buy",
        }))
    )


def _on_depth(event: Dict) -> None:
    _flow.on_depth(event)
    asyncio.get_event_loop().call_soon(
        lambda: asyncio.ensure_future(_do_broadcast({
            "type": "depth",
            "bids": event["bids"][:10],
            "asks": event["asks"][:10],
        }))
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _rest, _ws, _engine

    logger.info("=== Bot starting ===")
    await init_db()
    cfg = await load_config()

    if cfg.paper_mode:
        logger.info("*** PAPER MODE ACTIVE — no real orders will be placed ***")

    _rest = BinanceRestClient(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        paper_mode=cfg.paper_mode,
    )
    await _rest.init()

    _engine = TradingEngine(_rest, _flow, _broadcast)
    await _engine.restore_state()

    _ws = BinanceWebSocket(cfg.symbol, on_trade=_on_trade, on_depth=_on_depth)
    await _ws.start()

    ticker_task = asyncio.create_task(_ticker_loop())

    # Graceful shutdown on SIGTERM
    loop = asyncio.get_event_loop()

    def _shutdown(sig, frame):
        logger.info("Received %s — shutting down…", sig)
        ticker_task.cancel()

    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("=== Bot ready on :8765 ===")
    yield

    # Cleanup
    logger.info("Shutting down…")
    ticker_task.cancel()
    try:
        await ticker_task
    except asyncio.CancelledError:
        pass
    await _ws.stop()
    await _rest.close()
    logger.info("=== Bot stopped ===")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="DCA+Hedge Trading Bot", lifespan=lifespan)

_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# REST routes
# ---------------------------------------------------------------------------

@app.get("/")
async def index():
    return FileResponse(str(_STATIC_DIR / "index.html"))


@app.get("/api/config")
async def api_get_config():
    from database import get_all_config
    return await get_all_config()


@app.post("/api/config")
async def api_set_config(body: Dict[str, str]):
    await set_config_bulk(body)
    return {"ok": True}


@app.get("/api/symbols")
async def api_symbols():
    syms = await _rest.get_usdt_perp_symbols()
    return {"symbols": sorted(syms)}


@app.get("/api/sessions")
async def api_sessions(limit: int = 100):
    rows = await get_sessions(limit)
    return {"sessions": rows}


@app.get("/api/signal_log")
async def api_signal_log(limit: int = 100):
    rows = await get_signal_log(limit)
    return {"signal_log": rows}


@app.get("/api/performance")
async def api_performance():
    return await get_performance()


@app.get("/api/balance")
async def api_balance():
    cfg = await load_config()
    if cfg.paper_mode:
        return {"usdt": None, "paper_mode": True}
    try:
        bal = await _rest.get_balance()
        return {"usdt": bal, "paper_mode": False}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/reset")
async def api_reset(body: Dict):
    if body.get("confirm") is not True:
        return JSONResponse(status_code=400, content={"error": "confirm:true required"})
    cfg = await load_config()
    price = _last_price or await _rest.get_mark_price(cfg.symbol)
    await _engine.force_close_all(cfg, price)
    return {"ok": True}


# ---------------------------------------------------------------------------
# WebSocket hub
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _clients.add(websocket)
    cfg = await load_config()

    # Send initial state
    try:
        await websocket.send_text(json.dumps({
            "type":       "init",
            "paper_mode": cfg.paper_mode,
            "symbol":     cfg.symbol,
            "timeframe":  cfg.timeframe,
        }))

        # Send open session state if any
        from database import get_open_session, get_open_hedges
        sess = await get_open_session()
        if sess:
            hedges = await get_open_hedges(sess["id"])
            await websocket.send_text(json.dumps({
                "type":        "session",
                "session":     sess,
                "hedges":      hedges,
                "trail_price": _engine._trail_price if _engine else None,
                "trail_active": _engine._trail_activated if _engine else False,
            }))

        # Send recent signal + indicators
        if _engine and _engine.last_signal:
            await websocket.send_text(json.dumps({
                "type": "signal",
                "data": _engine.last_signal,
            }))
        if _engine and _engine.last_indicators:
            await websocket.send_text(json.dumps({
                "type":       "indicators",
                "indicators": _safe_ind(_engine.last_indicators),
            }))

        while True:
            raw = await websocket.receive_text()
            await _handle_ws_message(websocket, raw, cfg)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug("ws_endpoint error: %s", exc)
    finally:
        _clients.discard(websocket)


async def _handle_ws_message(ws: WebSocket, raw: str, cfg) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    mtype = msg.get("type", "")

    if mtype == "get_sessions":
        limit = int(msg.get("limit", 100))
        sessions = await get_sessions(limit)
        hedges_map: Dict[int, list] = {}
        from database import get_open_hedges
        for s in sessions:
            if s["status"] == "open":
                hedges_map[s["id"]] = await get_open_hedges(s["id"])
        await ws.send_text(json.dumps({
            "type":     "sessions",
            "sessions": sessions,
            "hedges":   hedges_map,
        }))

    elif mtype == "get_signal_log":
        limit = int(msg.get("limit", 50))
        rows = await get_signal_log(limit)
        await ws.send_text(json.dumps({
            "type":       "signal_log",
            "signal_log": rows,
        }))

    elif mtype == "get_performance":
        perf = await get_performance()
        await ws.send_text(json.dumps({
            "type": "performance",
            "data": perf,
        }))

    elif mtype == "set_config":
        global _last_candles_fetch
        updates = {k: str(v) for k, v in msg.get("config", {}).items()}
        await set_config_bulk(updates)
        # Reload WS subscription if symbol changed
        if "symbol" in updates:
            new_sym = updates["symbol"]
            await _ws.switch_symbol(new_sym)
            _last_candles_fetch = 0.0
            await _do_broadcast({"type": "symbol_ready", "symbol": new_sym})
        # Reset candle fetch if timeframe changed so next tick fetches fresh candles
        if "timeframe" in updates:
            _last_candles_fetch = 0.0
        await ws.send_text(json.dumps({"type": "config_saved", "ok": True}))

    elif mtype == "get_config":
        from database import get_all_config
        cfg_data = await get_all_config()
        await ws.send_text(json.dumps({"type": "config", "config": cfg_data}))

    elif mtype == "get_candles":
        if _engine and _engine.candles:
            await ws.send_text(json.dumps({
                "type":    "candles",
                "candles": _engine.candles[-100:],
            }))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8765,
        log_level="info",
        access_log=False,
    )
