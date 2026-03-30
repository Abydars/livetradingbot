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
    close_hedge,
    close_session,
    delete_all_sessions,
    delete_closed_hedge,
    delete_session,
    get_all_closed_hedges,
    get_config,
    get_open_session,
    get_open_hedges,
    get_performance,
    get_sessions,
    get_signal_log,
    insert_pos_log,
    get_pos_log,
    clear_pos_log,
    init_db,
    set_config,
    set_config_bulk,
    update_session,
)
from engine.indicators import compute_all
from engine.orderflow import OrderFlowAnalyzer
from engine.trading import TradingEngine
from exchange.binance_rest import BinanceRestClient
from exchange.binance_ws import BinanceWebSocket
from exchange.order_executor import OrderExecutor
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
_flow: Optional[OrderFlowAnalyzer] = None
_engine: Optional[TradingEngine]   = None
_executor: Optional[OrderExecutor] = None
_binance_client = None  # BinanceClient instance (demo/live only)

# Connected WebSocket clients
_clients: Set[WebSocket] = set()

# Latest price feed
_last_price: float = 0.0
_last_candles_fetch: float = 0.0
_last_symbol_scan: float = 0.0
_last_price_rest_fetch: float = 0.0   # throttle REST mark-price fallback
_last_top_movers: list = []            # cached for new WS clients
_trading_active: bool = False          # persisted in config.trading_active
_last_switch_ts: float = 0.0          # timestamp of last auto-switch (for flow warmup)
_htf_bias:        str   = "NEUTRAL"   # current HTF EMA trend bias
_last_htf_fetch:  float = 0.0         # last time HTF was fetched
_htf_scanner_cache: dict = {}         # {symbol: (bias, fetched_ts)} — per-symbol HTF cache for scanner
_entry_wait_ts: float = 0.0           # timestamp when we switched to current candidate (0 = not waiting)
_tried_syms: set  = set()             # symbols already tried in current cycle (since last trade)
_prev_session_open: bool = False       # track trade close to trigger immediate scan
_exchange_error: Optional[str] = None  # last BinanceClient startup/connect error
_last_client_warn: float = 0.0        # debounce: don't re-broadcast every tick
_last_position_check: float = 0.0     # throttle REST position sync
_last_external_fill_price: float = 0.0   # fill price captured from ORDER_TRADE_UPDATE for external closes
_POSITION_CHECK_S = 60.0              # check Binance position every N seconds
_CANDLE_REFRESH_S = 30.0   # fetch new candles every N seconds
_PRICE_REST_FALLBACK_S = 10.0  # only poll REST price if WS hasn't delivered in N seconds
_CLIENT_WARN_INTERVAL = 60.0  # re-broadcast "client unavailable" at most once per minute


# ---------------------------------------------------------------------------
# Broadcast helper
# ---------------------------------------------------------------------------

def _broadcast(msg: Dict) -> None:
    """Enqueue a message to all connected clients (fire-and-forget)."""
    asyncio.get_running_loop().call_soon(
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

    # Whenever a position closes (session broadcast with session=None), push the
    # full trade history and performance to all clients.  This covers every close
    # path (TP, hard stop, manual close, external close, hedge promotion) without
    # relying on the frontend's prevOpen→!nowOpen transition, which fails when
    # the client connects after the position was already open.
    if msg.get("type") == "session" and msg.get("session") is None:
        try:
            sessions     = await get_sessions(200)
            hedge_trades = await get_all_closed_hedges(200)
            perf         = await get_performance()
            hist_msg = json.dumps({"type": "sessions", "sessions": sessions,
                                   "hedges": {}, "hedge_trades": hedge_trades})
            perf_msg = json.dumps({"type": "performance", "data": perf})
            disc2: Set[WebSocket] = set()
            for ws in list(_clients):
                try:
                    await ws.send_text(hist_msg)
                    await ws.send_text(perf_msg)
                except Exception:
                    disc2.add(ws)
            _clients.difference_update(disc2)
        except Exception as exc:
            logger.debug("sessions auto-push after close failed: %s", exc)


# ---------------------------------------------------------------------------
# Order flow helpers
# ---------------------------------------------------------------------------

def _htf_for_timeframe(tf: str) -> tuple:
    """Return (htf_timeframe, ttl_seconds) for the given base timeframe."""
    return {
        "1m":  ("15m", 15 * 60),
        "3m":  ("30m", 30 * 60),
        "5m":  ("1h",  60 * 60),
        "15m": ("4h",  240 * 60),
        "30m": ("4h",  240 * 60),
        "1h":  ("1d",  1440 * 60),
    }.get(tf, ("15m", 15 * 60))


def _flow_window_for_timeframe(tf: str) -> int:
    """Return order-flow window in seconds as ~50% of the candle period."""
    _TF_SECONDS = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
        "8h": 28800, "12h": 43200, "1d": 86400,
    }
    period = _TF_SECONDS.get(tf, 60)
    return max(30, period // 2)


# ---------------------------------------------------------------------------
# Ticker loop (called from trading ticker)
# ---------------------------------------------------------------------------

async def _sync_position_rest(cfg) -> None:
    """
    REST fallback: query Binance for the current position and close the session
    if the position no longer exists (pa≈0). Runs every _POSITION_CHECK_S seconds
    to catch any external closes that the user data stream may have missed.
    """
    if not (_engine and _engine._session and _binance_client):
        return
    try:
        positions = await _binance_client.get_positions(cfg.symbol)
        sess_dir  = _engine._session["direction"]
        for pos in positions:
            ps = pos.get("positionSide", "BOTH")
            pa = float(pos.get("positionAmt", 1))
            # In hedge mode, only match the exact leg (LONG or SHORT).
            # In one-way mode, position side is "BOTH".
            # Never match "BOTH" entries in hedge mode — they always show pa=0.
            if _executor and _executor._hedge_mode:
                if ps != sess_dir:
                    continue
            else:
                if ps not in (sess_dir, "BOTH"):
                    continue
            if abs(pa) < 1e-8:
                logger.warning("Position sync: %s %s shows pa=0 — closed externally", cfg.symbol, sess_dir)
                await _handle_external_close(0.0, "external_close")
                return
    except Exception as exc:
        logger.debug("Position sync REST check failed: %s", exc)


async def _ticker_loop() -> None:
    global _last_price, _last_candles_fetch, _last_symbol_scan, _last_price_rest_fetch, _prev_session_open, _last_client_warn, _last_position_check, _htf_bias, _last_htf_fetch
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

            # Always tick to manage any open position (TP/SL/DCA/hedge).
            # allow_entry=False when trading is stopped — existing position
            # continues to be managed but no new entries are opened.
            if _executor and not _executor.is_ready:
                if _trading_active:
                    if now - _last_client_warn >= _CLIENT_WARN_INTERVAL:
                        _last_client_warn = now
                        err = _exchange_error or "Exchange client unavailable — check API keys"
                        _on_exchange_error(err)
            else:
                flow_warmup_s = _flow_window_for_timeframe(cfg.timeframe)
                in_flow_warmup = (time.time() - _last_switch_ts) < flow_warmup_s

                # HTF EMA bias — refresh once per TTL; cheap (1 REST call, 70 candles)
                htf_tf, htf_ttl = _htf_for_timeframe(cfg.timeframe)
                if now - _last_htf_fetch >= htf_ttl:
                    try:
                        htf_raw = await _rest.get_klines(cfg.symbol, interval=htf_tf, limit=70)
                        if htf_raw and len(htf_raw) >= 50:
                            from exchange.binance_rest import _scan_ema as _ema
                            htf_closes = [float(k[4]) for k in htf_raw]
                            htf_ema21 = _ema(htf_closes, 21)
                            htf_ema50 = _ema(htf_closes, 50)
                            if htf_ema21 > htf_ema50 and htf_closes[-1] > htf_ema21:
                                _htf_bias = "LONG"
                            elif htf_ema21 < htf_ema50 and htf_closes[-1] < htf_ema21:
                                _htf_bias = "SHORT"
                            else:
                                _htf_bias = "NEUTRAL"
                            _last_htf_fetch = now
                            _broadcast({"type": "htf_bias", "bias": _htf_bias, "timeframe": htf_tf})
                    except Exception as exc:
                        logger.debug("HTF fetch failed: %s", exc)

                await _engine.tick(cfg, price, allow_entry=_trading_active, flow_warmup=in_flow_warmup, htf_bias=_htf_bias)

            # Detect trade close → trigger immediate symbol scan (auto_switch only)
            cur_session_open = _engine._session is not None
            if _prev_session_open and not cur_session_open and cfg.auto_switch:
                logger.info("Trade closed — triggering immediate symbol scan")
                _last_symbol_scan = 0.0
            _prev_session_open = cur_session_open

            # Symbol scan — always runs for sidebar; auto-switch is conditional
            if now - _last_symbol_scan >= cfg.scan_interval_s:
                _last_symbol_scan = now
                await _scan_symbols(cfg)

            # REST position sync — catch external closes missed by user data stream
            if _engine._session and now - _last_position_check >= _POSITION_CHECK_S:
                _last_position_check = now
                await _sync_position_rest(cfg)

        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("ticker_loop error: %s", exc, exc_info=True)
            _pl = {"type": "pos_log", "event": "failed", "ts": int(time.time()), "reason": str(exc)}
            _broadcast(_pl)
            asyncio.ensure_future(insert_pos_log("failed", _pl))

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


def _format_movers(top: list) -> list:
    """Convert raw get_top_movers rows to a lean, JSON-safe list for the UI."""
    out = []
    for t in top:
        out.append({
            "symbol":    t["symbol"],
            "score":     round(t["_score"], 2),
            "change":    round(float(t.get("priceChangePercent", 0)), 2),
            "volume":    round(float(t.get("quoteVolume", 0))),
            "price":     float(t.get("lastPrice", 0)),
            "bias":      t.get("_bias", ""),
            "vol_surge": t.get("_vol_surge", 1.0),
            "momentum":  t.get("_momentum", 0.0),
            "atr_pct":   t.get("_atr_pct", 0.0),
            "htf_bias":  t.get("_htf_bias", ""),
        })
    return out


async def _scan_symbols(cfg) -> None:
    """Fetch top-movers, broadcast to sidebar, and auto-switch if configured."""
    global _tried_syms, _entry_wait_ts, _htf_scanner_cache, _last_top_movers, _last_candles_fetch, _last_price, _last_price_rest_fetch, _last_switch_ts
    try:
        top = await _rest.get_top_movers(n=cfg.scanner_top_n, timeframe=cfg.timeframe)
        if not top:
            return

        # Per-symbol HTF bias with cache — HTF changes every 15+ minutes so
        # there is no need to re-fetch on every 10-second scan cycle.
        # Only symbols whose cache has expired trigger a real API call.
        try:
            from exchange.binance_rest import _scan_ema as _ema
            htf_tf, htf_ttl = _htf_for_timeframe(cfg.timeframe)
            now_ts = time.time()

            stale_syms = [
                t["symbol"] for t in top
                if now_ts - _htf_scanner_cache.get(t["symbol"], ("", 0.0))[1] >= htf_ttl
            ]

            if stale_syms:
                htf_klines_map = await _rest.get_klines_batch(
                    stale_syms, interval=htf_tf, limit=70
                )
                for sym, kl in htf_klines_map.items():
                    if len(kl) >= 50:
                        closes = [float(k[4]) for k in kl]
                        e21 = _ema(closes, 21)
                        e50 = _ema(closes, 50)
                        if e21 > e50 and closes[-1] > e21:
                            bias = "LONG"
                        elif e21 < e50 and closes[-1] < e21:
                            bias = "SHORT"
                        else:
                            bias = "NEUTRAL"
                    else:
                        bias = ""
                    _htf_scanner_cache[sym] = (bias, now_ts)

            for t in top:
                cached = _htf_scanner_cache.get(t["symbol"])
                t["_htf_bias"] = cached[0] if cached else ""
        except Exception as exc:
            logger.debug("Scanner HTF cache failed: %s", exc)

        _last_top_movers = _format_movers(top)
        await _do_broadcast({"type": "top_movers", "movers": _last_top_movers})

        # Auto-switch only when enabled and no open position
        if not cfg.auto_switch or not _trading_active:
            _entry_wait_ts = 0.0
            _tried_syms.clear()
            return
        if _engine._session is not None:
            # Position is open — clear cycle state so next entry search starts fresh
            _entry_wait_ts = 0.0
            _tried_syms.clear()
            return

        now_ts   = time.time()
        top_syms = [t["symbol"] for t in top]   # ordered best → worst

        # If current symbol dropped off the top list entirely, mark it as tried
        if cfg.symbol not in top_syms:
            _tried_syms.add(cfg.symbol)

        # Build candidate list: top symbols not yet tried, preserving rank order
        candidates = [s for s in top_syms if s not in _tried_syms]

        # All top symbols have been tried with no entry — reset and start over
        if not candidates:
            logger.info("Auto-switch: all top symbols tried with no entry — resetting cycle")
            _tried_syms.clear()
            candidates = top_syms[:]

        # Current symbol is still an active candidate — apply entry wait timer
        if cfg.symbol in candidates:
            next_candidate = next((s for s in candidates if s != cfg.symbol), None)

            if _entry_wait_ts == 0.0:
                # Start timer for current symbol
                _entry_wait_ts = now_ts
                logger.info(
                    "Auto-switch: waiting up to %.0fs on %s for entry  (next: %s)",
                    cfg.entry_wait_s, cfg.symbol, next_candidate or "—",
                )

            elapsed = now_ts - _entry_wait_ts

            await _do_broadcast({
                "type":      "entry_wait",
                "waiting":   True,
                "elapsed":   round(elapsed, 1),
                "timeout":   cfg.entry_wait_s,
                "symbol":    cfg.symbol,
                "candidate": next_candidate or "",
                "tried":     len(_tried_syms),
                "total":     len(top_syms),
            })

            if elapsed < cfg.entry_wait_s:
                return  # still within wait window

            # Timer expired — no entry on current symbol, mark it tried
            logger.info(
                "Auto-switch: %s — no entry in %.0fs, moving to next candidate",
                cfg.symbol, elapsed,
            )
            _tried_syms.add(cfg.symbol)
            candidates = [s for s in top_syms if s not in _tried_syms]

            if not candidates:
                # Just exhausted the last candidate — reset and use full list
                _tried_syms.clear()
                candidates = [s for s in top_syms if s != cfg.symbol]
                if not candidates:
                    return  # only one symbol in scanner, nothing to switch to

        # Pick the best available untried candidate
        new_sym = candidates[0]

        logger.info("Auto-switch: %s → %s  (tried: %s)", cfg.symbol, new_sym, sorted(_tried_syms))
        await set_config_bulk({"symbol": new_sym})
        # Fetch 200 candles for the new symbol BEFORE switching so the engine
        # is ready to compute a signal on the very first tick after switch —
        # no extra 30-second candle-refresh cycle needed.
        raw_candles = await _rest.get_klines(new_sym, interval=cfg.timeframe, limit=200)
        fresh_candles = [
            {
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
                "time":   int(k[0]) // 1000,
            }
            for k in raw_candles
        ] if raw_candles else []

        # WS resubscribe and Binance symbol prep are independent — run in parallel.
        await asyncio.gather(
            _ws.switch_symbol(new_sym),
            _executor.prepare_symbol(new_sym, cfg.leverage),
        )

        # Reset stale price so the next tick gets a fresh mark-price for new symbol.
        _last_price = 0.0
        _last_price_rest_fetch = 0.0

        # symbol_ready MUST go first — UI clears the chart on this message.
        # Candles sent after so they populate the freshly cleared chart.
        _entry_wait_ts = now_ts   # start timer immediately for the new symbol
        _tried_syms.discard(new_sym)   # new symbol is active candidate — remove from tried if present
        global _last_switch_ts, _htf_bias, _last_htf_fetch
        _last_switch_ts = time.time()
        _htf_bias = "NEUTRAL"
        _last_htf_fetch = 0.0
        _htf_scanner_cache.pop(new_sym, None)   # force fresh HTF fetch for new symbol on next scan
        await _do_broadcast({"type": "symbol_ready", "symbol": new_sym})

        if fresh_candles:
            _engine.update_candles(fresh_candles)
            _last_candles_fetch = time.time()
            await _do_broadcast({
                "type":    "candles",
                "candles": fresh_candles[-100:],
            })
            ind = _engine.last_indicators
            if ind:
                await _do_broadcast({
                    "type":       "indicators",
                    "indicators": _safe_ind(ind),
                })
        else:
            _last_candles_fetch = 0.0

        await _do_broadcast({
            "type": "notification",
            "text": f"Auto-switched: {cfg.symbol} → {new_sym}",
        })
    except Exception as exc:
        logger.warning("_scan_symbols error: %s", exc)
        _on_exchange_error(f"Symbol scan failed: {exc}")


# ---------------------------------------------------------------------------
# WS trade/depth callbacks + exchange error callback
# ---------------------------------------------------------------------------

def _on_exchange_error(msg: str) -> None:
    """Forward any Binance WS error to the position log in the UI."""
    logger.warning("Exchange error: %s", msg)
    _pl = {"type": "pos_log", "event": "failed", "ts": int(time.time()), "reason": msg}
    _broadcast(_pl)
    try:
        asyncio.get_running_loop().create_task(insert_pos_log("failed", _pl))
    except RuntimeError:
        pass


def _on_trade(event: Dict) -> None:
    global _last_price
    price = event["price"]
    _last_price = price
    _flow.on_trade(event)

    # Fast trail check on every WS tick — catches wicks that the 1s REST
    # poll would miss. Only runs when trail is armed and a session is open.
    if (
        _engine
        and _engine._session
        and _engine._trail_activated
        and _engine._trail_price is not None
        and not _engine._closing
    ):
        direction = _engine._session["direction"]
        trail_hit = (
            (direction == "LONG"  and price <= _engine._trail_price) or
            (direction == "SHORT" and price >= _engine._trail_price)
        )
        if trail_hit:
            avg_price = _engine._session["avg_price"]
            leverage  = _engine._session["leverage"]
            if direction == "LONG":
                pnl_pct = (price - avg_price) / avg_price * 100 * leverage
            else:
                pnl_pct = (avg_price - price) / avg_price * 100 * leverage

            logger.info(
                "WS trail hit @ %.6f  trail=%.6f  pnl=%.2f%%",
                price, _engine._trail_price, pnl_pct,
            )

            async def _do_trail_close():
                try:
                    cfg = await load_config()
                    if (
                        _engine
                        and _engine._session
                        and _engine._trail_activated
                        and not _engine._closing
                    ):
                        await _engine._close_position(cfg, price, pnl_pct, "trailing_tp")
                except Exception as e:
                    logger.error("WS trail close failed: %s", e)

            loop = asyncio.get_running_loop()
            loop.call_soon(lambda: asyncio.ensure_future(_do_trail_close()))

    # WS last resort SL check — real-time, catches spikes the 1s tick loop misses
    if (
        _engine
        and _engine._session
        and not _engine._trail_activated
        and not _engine._closing
    ):
        _sess     = _engine._session
        _dir      = _sess["direction"]
        _avg      = _sess["avg_price"]
        _lev      = _sess["leverage"]
        _buf      = getattr(_engine, "_last_resort_buffer_cache", 0.80)
        _lr_pct   = (1.0 / _lev) * _buf if _lev > 0 else 0.10
        _lr_hit   = (
            (_dir == "LONG"  and price <= _avg * (1 - _lr_pct)) or
            (_dir == "SHORT" and price >= _avg * (1 + _lr_pct))
        )
        if _lr_hit:
            _pnl = (
                (price - _avg) / _avg * 100 * _lev
                if _dir == "LONG"
                else (_avg - price) / _avg * 100 * _lev
            )
            logger.error(
                "WS last resort SL hit @ %.6f  pnl=%.2f%%",
                price, _pnl,
            )

            async def _do_lr_close(_p=price, _pnl=_pnl):
                try:
                    _cfg = await load_config()
                    if _engine and _engine._session and not _engine._closing:
                        await _engine._emergency_close(_cfg, _p, _pnl)
                except Exception as _e:
                    logger.error("WS last resort SL close failed: %s", _e)

            _loop = asyncio.get_running_loop()
            _loop.call_soon(lambda: asyncio.ensure_future(_do_lr_close()))

    asyncio.get_running_loop().call_soon(
        lambda: asyncio.ensure_future(_do_broadcast({
            "type":  "trade",
            "price": price,
            "qty":   event["qty"],
            "side":  "sell" if event["buyer_maker"] else "buy",
        }))
    )


def _on_depth(event: Dict) -> None:
    _flow.on_depth(event)
    asyncio.get_running_loop().call_soon(
        lambda: asyncio.ensure_future(_do_broadcast({
            "type": "depth",
            "bids": event["bids"][:10],
            "asks": event["asks"][:10],
        }))
    )


# ---------------------------------------------------------------------------
# User data stream callback (fill price sync)
# ---------------------------------------------------------------------------

async def _handle_external_close(fill_price: float, reason: str) -> None:
    """
    Called when Binance reports a position was closed externally
    (liquidation, manual close from exchange UI, another bot, etc.).
    Updates the DB session, clears engine state, and notifies the UI.
    """
    if not (_engine and _engine._session) or _engine._closing:
        return
    sess      = _engine._session
    direction = sess["direction"]
    avg_price = sess["avg_price"]
    leverage  = sess["leverage"]
    margin    = sess["margin"]
    symbol    = sess.get("symbol", "")

    if fill_price > 0:
        if direction == "LONG":
            pnl_pct = (fill_price - avg_price) / avg_price * 100 * leverage
        else:
            pnl_pct = (avg_price - fill_price) / avg_price * 100 * leverage
        realized_pnl = round(pnl_pct / 100 * margin, 4)
    else:
        pnl_pct = realized_pnl = 0.0

    for hedge in list(_engine._hedges):
        await close_hedge(hedge["id"], 0.0)

    await close_session(sess["id"], realized_pnl, reason,
                        exit_price=fill_price if fill_price > 0 else None)
    logger.warning(
        "External close [%s]: %s %s fill=%.6f pnl=%.4f",
        reason, direction, symbol, fill_price, realized_pnl,
    )

    label = {"liquidated": "LIQUIDATED", "adl_close": "AUTO-DELEVERAGED"}.get(reason, "Closed on exchange")
    price_str = f"@ {fill_price:.6f}" if fill_price > 0 else "(price unknown)"
    _broadcast({"type": "notification",
                "text": f"⚠ {label} {symbol} {price_str}  pnl={realized_pnl:+.4f}"})
    _pl = {"type": "pos_log", "event": reason, "ts": int(time.time()),
           "symbol": symbol, "price": fill_price, "pnl": realized_pnl}
    _broadcast(_pl)
    asyncio.ensure_future(insert_pos_log(reason, _pl))

    _engine._session         = None
    _engine._hedges          = []
    _engine._trail_activated = False
    _engine._trail_price     = None
    _engine._entry_adaptive  = {}
    _engine._pending_fills.clear()
    _engine._push_session()
    # _do_broadcast automatically pushes sessions+performance when session=None


async def _on_user_data(event: dict) -> None:
    """
    Handle Binance user data stream events:

    ORDER_TRADE_UPDATE / FILLED
      - Entry/DCA fills: correct avg_price in DB using true fill price.
      - Liquidation orders: detect and close session as "liquidated".

    ACCOUNT_UPDATE
      - If position for the tracked symbol goes to zero, the position was
        closed externally (manual close, liquidation, another bot).
    """
    global _last_external_fill_price
    etype = event.get("e")

    # ── ORDER_TRADE_UPDATE ────────────────────────────────────────────────
    if etype == "ORDER_TRADE_UPDATE":
        o = event.get("o", {})
        if o.get("X") != "FILLED":
            return
        # x = Execution Type per Binance docs.
        # Only process actual trade fills (TRADE) or liquidation executions (CALCULATED).
        # Skip AMENDMENT (order modified), EXPIRED, NEW, CANCELED etc.
        if o.get("x") not in ("TRADE", "CALCULATED"):
            return

        # Compute fill price from raw cumulative fields for maximum accuracy.
        # Z = Cumulative Quote Asset Transacted Quantity (total USDT)
        # z = Order Filled Accumulated Quantity (total contracts)
        # Z/z = true VWAP of the order, more precise than pre-computed "ap".
        # Fallback chain: Z/z → ap (Binance average) → L (last fill price).
        cum_quote_z = float(o.get("Z", 0))
        cum_qty_z   = float(o.get("z", 0))
        fill_qty    = cum_qty_z

        if cum_quote_z > 0 and cum_qty_z > 0:
            fill_price = cum_quote_z / cum_qty_z          # most accurate
        elif float(o.get("ap", 0)) > 0:
            fill_price = float(o.get("ap", 0))            # Binance-computed avg
        else:
            fill_price = float(o.get("L", 0))             # last fill price
        symbol     = o.get("s", "")
        order_id   = o.get("i")
        # "o" = current Order Type (per docs: "LIQUIDATION" for forced closes)
        # "ot" = Original Order Type (what the order was before modification)
        # "c" = Client Order Id — Binance reserves special prefixes:
        #   "autoclose-*"          → liquidation
        #   "adl_autoclose"        → auto-deleveraging (ADL)
        #   "settlement_autoclose-*" → delivery / delisting settlement
        order_type = o.get("o", "")
        client_id  = o.get("c", "")
        is_forced_close = (
            order_type == "LIQUIDATION"
            or client_id == "adl_autoclose"
            or client_id.startswith("autoclose-")
            or client_id.startswith("settlement_autoclose-")
        )

        realized_pnl_raw = float(o.get("rp", 0))
        logger.info(
            "Fill: orderId=%s %s type=%s client=%s fill=%.6f qty=%.4f rp=%+.4f",
            order_id, symbol, order_type, client_id, fill_price, fill_qty,
            realized_pnl_raw,
        )

        # Forced close (liquidation / ADL / settlement) — position closed by exchange.
        # L = last fill price (accurate); ap = average price (fallback).
        if is_forced_close:
            reason = "liquidated" if order_type == "LIQUIDATION" or client_id.startswith("autoclose-") else "adl_close"
            if _engine and _engine._session and not _engine._closing:
                sess_sym = _engine._session.get("symbol", "").upper()
                if sess_sym == symbol.upper():
                    forced_price = float(o.get("L") or o.get("ap") or 0)
                    await _handle_external_close(forced_price, reason)
            return

        # Entry / DCA fill price correction
        if not (_engine and _engine._session and fill_price > 0 and order_id):
            return

        order_id_int = int(order_id)
        pending = _engine._pending_fills.pop(order_id_int, None)
        if pending is None:
            # Not the bot's order. Check if this is a manual/external close
            # of the tracked position so we can capture the real fill price
            # before ACCOUNT_UPDATE fires with no price information.
            if (
                _engine and _engine._session
                and not _engine._closing
                and fill_price > 0
                and symbol.upper() == _engine._session.get("symbol", "").upper()
            ):
                sess_dir   = _engine._session["direction"]
                order_side = o.get("S", "")   # "BUY" or "SELL"
                is_closing_side = (
                    (sess_dir == "LONG"  and order_side == "SELL") or
                    (sess_dir == "SHORT" and order_side == "BUY")
                )
                if is_closing_side:
                    _oid = int(order_id) if order_id else 0
                    if _oid and _oid in _engine._bot_close_order_ids:
                        # This is the bot's own close order — remove from tracking set
                        # and do NOT treat it as an external close.
                        _engine._bot_close_order_ids.discard(_oid)
                        logger.debug(
                            "Bot close order %s filled @ %.6f — skipping external close capture",
                            _oid, fill_price,
                        )
                    else:
                        _last_external_fill_price = fill_price
                        logger.info(
                            "External close detected (ORDER_TRADE_UPDATE): "
                            "symbol=%s side=%s fill=%.6f — price captured for PnL calc",
                            symbol, order_side, fill_price,
                        )
            return  # close/hedge/external order — don't touch avg_price

        sess = _engine._session

        if pending["type"] == "entry":
            new_avg = fill_price
            # At entry, avg_price and entry_price must both equal the true fill
            # price. entry_price was set from the order response which may have
            # used the mark price as fallback — always correct both fields.
            # No threshold: ORDER_TRADE_UPDATE data is authoritative.
            old_entry = sess["entry_price"]
            old_avg   = sess["avg_price"]
            await update_session(
                sess["id"],
                avg_price=new_avg,
                entry_price=new_avg,
            )
            _engine._session = await get_open_session()
            logger.info(
                "Fill sync (entry): entry_price %.6f → %.6f, "
                "avg_price %.6f → %.6f  (true fill=%.6f)",
                old_entry, new_avg, old_avg, new_avg, fill_price,
            )
            _engine._push_session()

        else:
            # DCA: recompute blended average using the true fill price.
            prior_qty = pending["prior_qty"]
            prior_avg = pending["prior_avg"]
            new_qty   = pending.get("new_qty", fill_qty)
            total_qty = prior_qty + new_qty
            new_avg   = (prior_avg * prior_qty + fill_price * new_qty) / total_qty

            # Always apply the correction — no threshold. The true fill price
            # from ORDER_TRADE_UPDATE is more accurate than the order response
            # avgPrice which may have been the mark price fallback.
            old_avg = sess["avg_price"]
            await update_session(sess["id"], avg_price=new_avg)
            _engine._session = await get_open_session()
            logger.info(
                "Fill sync (dca): avg_price %.6f → %.6f  "
                "(true_fill=%.6f  prior_avg=%.6f  prior_qty=%.6f  new_qty=%.6f)",
                old_avg, new_avg, fill_price, prior_avg, prior_qty, new_qty,
            )
            _engine._push_session()

        return

    # ── ACCOUNT_UPDATE ────────────────────────────────────────────────────
    if etype == "ACCOUNT_UPDATE":
        if not (_engine and _engine._session and not _engine._closing):
            return
        sess     = _engine._session
        sess_sym = sess.get("symbol", "").upper()
        sess_dir = sess["direction"]          # "LONG" or "SHORT"

        for pos in event.get("a", {}).get("P", []):
            sym = pos.get("s", "").upper()
            ps  = pos.get("ps", "")           # "LONG", "SHORT", or "BOTH"
            pa  = float(pos.get("pa", 1))     # position amount (negative for SHORT)

            if sym != sess_sym:
                continue
            # In hedge mode match the exact leg; in one-way mode ps=="BOTH"
            if ps not in (sess_dir, "BOTH"):
                continue
            if abs(pa) < 1e-8:
                reason_raw = event.get("a", {}).get("m", "").upper()
                if reason_raw == "LIQUIDATION":
                    reason = "liquidated"
                elif reason_raw == "POSITION_ADL":
                    reason = "adl_close"
                else:
                    reason = "external_close"

                # Use fill price captured from ORDER_TRADE_UPDATE if available.
                # ORDER_TRADE_UPDATE fires before ACCOUNT_UPDATE so the price
                # should already be stored. Consume and clear it in one step.
                close_price = _last_external_fill_price
                _last_external_fill_price = 0.0
                if close_price > 0:
                    logger.info(
                        "External close (ACCOUNT_UPDATE): using captured fill=%.6f "
                        "for PnL calculation",
                        close_price,
                    )
                else:
                    logger.warning(
                        "External close (ACCOUNT_UPDATE): no fill price captured — "
                        "PnL will be recorded as 0. ORDER_TRADE_UPDATE may have "
                        "been missed (WS reconnect during close?)."
                    )

                await _handle_external_close(close_price, reason)
                return


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _rest, _ws, _flow, _engine, _executor, _binance_client, _trading_active

    logger.info("=== Bot starting ===")
    await init_db()
    cfg = await load_config()

    # Restore trading_active flag from DB
    ta_val = await get_config("trading_active")
    _trading_active = (ta_val == "1")
    logger.info("Trading active on startup: %s", _trading_active)
    logger.info("Trading mode: %s", cfg.trading_mode.upper())

    if cfg.paper_mode:
        logger.info("*** PAPER MODE ACTIVE — no real orders will be placed ***")

    # REST client for: klines, exchange info, top movers, qty rounding
    # Always uses production endpoints for market data
    _rest = BinanceRestClient(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        paper_mode=cfg.paper_mode,
        key_type=cfg.key_type,
    )
    await _rest.init()

    # BinanceClient for order placement (demo/live only)
    _binance_client = None
    if cfg.trading_mode in ("demo", "live"):
        if not cfg.api_key or not cfg.api_secret:
            logger.warning(
                "*** %s mode selected but API keys are missing — "
                "falling back to paper simulation. "
                "Set BINANCE_LIVE_KEY / BINANCE_LIVE_SECRET (live) or "
                "BINANCE_DEMO_KEY / BINANCE_DEMO_SECRET (demo). ***",
                cfg.trading_mode.upper(),
            )
        else:
            try:
                from binance_client import BinanceClient, BinanceMode
                mode = BinanceMode.DEMO if cfg.trading_mode == "demo" else BinanceMode.LIVE
                _binance_client = BinanceClient(
                    api_key=cfg.api_key,
                    api_secret=cfg.api_secret,
                    mode=mode,
                    market="futures",
                    key_type=cfg.key_type,
                    on_error=_on_exchange_error,
                )
                await _binance_client.start()
                await _binance_client.subscribe_user_data(_on_user_data)
                _exchange_error = None
            except Exception as exc:
                logger.error(
                    "BinanceClient startup failed (%s) — falling back to paper simulation. "
                    "Check your API keys and network.",
                    exc,
                )
                _exchange_error = f"BinanceClient startup failed: {exc}"
                if _binance_client:
                    try:
                        await _binance_client.stop()
                    except Exception:
                        pass
                _binance_client = None

    _flow = OrderFlowAnalyzer(
        window_seconds=_flow_window_for_timeframe(cfg.timeframe), depth_levels=5
    )
    _executor = OrderExecutor(_binance_client, _rest, cfg.trading_mode,
                              paper_slippage_pct=cfg.paper_slippage_pct)
    await _executor.init()
    await _executor.prepare_symbol(cfg.symbol, cfg.leverage)

    _engine = TradingEngine(_executor, _flow, _broadcast)
    await _engine.restore_state()

    # ── Startup position sync ─────────────────────────────────────────────
    # Paper mode: if trading was already stopped when the process died and
    # there is still an open session in the DB, close it now using the
    # current mark price.  The session would otherwise stay "open" forever
    # and never appear in trade history.
    if _engine._session and cfg.paper_mode and not _trading_active:
        try:
            sess      = _engine._session
            direction = sess["direction"]
            avg_price = sess["avg_price"]
            leverage  = sess["leverage"]
            margin    = sess["margin"]
            fill_price = await _rest.get_mark_price(cfg.symbol)
            if fill_price > 0:
                if direction == "LONG":
                    pnl_pct = (fill_price - avg_price) / avg_price * 100 * leverage
                else:
                    pnl_pct = (avg_price - fill_price) / avg_price * 100 * leverage
                realized_pnl = round(pnl_pct / 100 * margin, 4)
            else:
                realized_pnl = 0.0
            for hedge in list(_engine._hedges):
                await close_hedge(hedge["id"], 0.0)
            await close_session(sess["id"], realized_pnl, "manual_reset",
                                exit_price=fill_price if fill_price > 0 else None)
            logger.info(
                "Startup sync (paper): closed orphaned session %d  pnl=%.4f",
                sess["id"], realized_pnl,
            )
            _engine._session         = None
            _engine._hedges          = []
            _engine._trail_activated = False
            _engine._trail_price     = None
            _engine._entry_adaptive  = {}
        except Exception as exc:
            logger.error("Startup paper sync failed: %s", exc)

    # Live/demo: if the DB has an open session but Binance shows the position
    # is already gone (closed while the bot was stopped), record it as closed
    # so it appears in trade history.
    if _engine._session and not cfg.paper_mode:
        try:
            pos = await _rest.get_position_risk(cfg.symbol)
            if pos is None:
                # Position is flat on Binance — close it in the DB
                sess      = _engine._session
                direction = sess["direction"]
                avg_price = sess["avg_price"]
                leverage  = sess["leverage"]
                margin    = sess["margin"]
                # Use mark price as best approximation of the exit price
                fill_price = await _rest.get_mark_price(cfg.symbol)
                if fill_price > 0:
                    if direction == "LONG":
                        pnl_pct = (fill_price - avg_price) / avg_price * 100 * leverage
                    else:
                        pnl_pct = (avg_price - fill_price) / avg_price * 100 * leverage
                    realized_pnl = round(pnl_pct / 100 * margin, 4)
                else:
                    realized_pnl = 0.0

                for hedge in list(_engine._hedges):
                    await close_hedge(hedge["id"], 0.0)

                await close_session(sess["id"], realized_pnl, "external_close",
                                    exit_price=fill_price if fill_price > 0 else None)
                logger.warning(
                    "Startup sync: session %d was open in DB but position is flat "
                    "on Binance — closed with pnl=%.4f (approx mark price %.6f)",
                    sess["id"], realized_pnl, fill_price,
                )
                _engine._session         = None
                _engine._hedges          = []
                _engine._trail_activated = False
                _engine._trail_price     = None
                _engine._entry_adaptive  = {}
        except Exception as exc:
            logger.error("Startup position sync failed: %s", exc)

    _ws = BinanceWebSocket(
        cfg.symbol, cfg.trading_mode, on_trade=_on_trade, on_depth=_on_depth,
        on_error=_on_exchange_error,
    )
    await _ws.start()

    ticker_task = asyncio.create_task(_ticker_loop())

    # Graceful shutdown on SIGTERM
    loop = asyncio.get_running_loop()

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
    if _binance_client:
        await _binance_client.stop()
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


@app.get("/api/pos_log")
async def api_pos_log(limit: int = 30, before_id: Optional[int] = None):
    rows = await get_pos_log(limit, before_id)
    return {"pos_log": rows}


@app.delete("/api/pos_log")
async def api_clear_pos_log():
    await clear_pos_log()
    return {"ok": True}


@app.get("/api/performance")
async def api_performance():
    return await get_performance()


@app.get("/api/balance")
async def api_balance():
    cfg = await load_config()
    if cfg.paper_mode:
        return {"usdt": None, "paper_mode": True}
    try:
        if _binance_client:
            bal = await _binance_client.get_balance()
        else:
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
            "type":           "init",
            "paper_mode":     cfg.paper_mode,
            "trading_mode":   cfg.trading_mode,
            "symbol":         cfg.symbol,
            "timeframe":      cfg.timeframe,
            "trading_active": _trading_active,
        }))

        # Send open session state if any
        sess = await get_open_session()
        if sess:
            hedges = await get_open_hedges(sess["id"])
            tp_price, sl_price = _engine.get_level_prices() if _engine else (None, None)
            await websocket.send_text(json.dumps({
                "type":               "session",
                "session":            sess,
                "hedges":             hedges,
                "trail_price":        _engine._trail_price     if _engine else None,
                "trail_active":       _engine._trail_activated if _engine else False,
                "tp_price":           tp_price,
                "sl_price":           sl_price,
                "override_tp_price":  _engine._override_tp_price if _engine else None,
                "override_sl_price":  _engine._override_sl_price if _engine else None,
            }))

        # Surface any stored exchange startup error immediately
        if _exchange_error:
            await websocket.send_text(json.dumps({
                "type":   "pos_log",
                "event":  "failed",
                "ts":     int(time.time()),
                "reason": _exchange_error,
            }))

        # Send current HTF bias
        await websocket.send_text(json.dumps({
            "type": "htf_bias",
            "bias": _htf_bias,
            "timeframe": _htf_for_timeframe(cfg.timeframe)[0],
        }))

        # Send cached top-movers for the sidebar
        if _last_top_movers:
            await websocket.send_text(json.dumps({
                "type":   "top_movers",
                "movers": _last_top_movers,
            }))

        # Send cached candles immediately so the chart doesn't wait 30 s
        if _engine and _engine.candles:
            await websocket.send_text(json.dumps({
                "type":    "candles",
                "candles": _engine.candles[-100:],
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
        for s in sessions:
            if s["status"] == "open":
                hedges_map[s["id"]] = await get_open_hedges(s["id"])
        hedge_trades = await get_all_closed_hedges(limit)
        await ws.send_text(json.dumps({
            "type":         "sessions",
            "sessions":     sessions,
            "hedges":       hedges_map,
            "hedge_trades": hedge_trades,
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

    elif mtype == "set_trading_active":
        global _trading_active
        active = bool(msg.get("active", False))
        _trading_active = active
        await set_config("trading_active", "1" if active else "0")
        logger.info("Trading %s by user", "started" if active else "stopped")
        await _do_broadcast({"type": "trading_status", "active": active})

    elif mtype == "delete_session":
        sid = msg.get("id")
        if sid is not None:
            await delete_session(int(sid))
            sessions = await get_sessions(200)
            hedge_trades = await get_all_closed_hedges(200)
            perf = await get_performance()
            await ws.send_text(json.dumps({
                "type": "sessions", "sessions": sessions,
                "hedges": {}, "hedge_trades": hedge_trades,
            }))
            await ws.send_text(json.dumps({"type": "performance", "data": perf}))

    elif mtype == "delete_hedge":
        hid = msg.get("id")
        if hid is not None:
            await delete_closed_hedge(int(hid))
            sessions = await get_sessions(200)
            hedge_trades = await get_all_closed_hedges(200)
            perf = await get_performance()
            await ws.send_text(json.dumps({
                "type": "sessions", "sessions": sessions,
                "hedges": {}, "hedge_trades": hedge_trades,
            }))
            await ws.send_text(json.dumps({"type": "performance", "data": perf}))

    elif mtype == "set_level_override":
        if _engine and _engine._session:
            tp = msg.get("tp_price")
            sl = msg.get("sl_price")
            if tp is not None:
                _engine._override_tp_price = float(tp)
            if sl is not None:
                _engine._override_sl_price = float(sl)
            logger.info(
                "Level overrides updated: tp=%s sl=%s",
                _engine._override_tp_price, _engine._override_sl_price,
            )
            _engine._push_session()  # broadcast updated override state to all clients

    elif mtype == "manual_close":
        if _engine and _engine._session:
            cfg2 = await load_config()
            price = _last_price or await _rest.get_mark_price(cfg2.symbol)
            await _engine.force_close_all(cfg2, price)
        else:
            await ws.send_text(json.dumps({
                "type": "notification", "text": "No open position to close",
            }))

    elif mtype == "delete_all_sessions":
        await delete_all_sessions()
        sessions = await get_sessions(200)
        hedge_trades = await get_all_closed_hedges(200)
        perf = await get_performance()
        await ws.send_text(json.dumps({
            "type": "sessions", "sessions": sessions,
            "hedges": {}, "hedge_trades": hedge_trades,
        }))
        await ws.send_text(json.dumps({"type": "performance", "data": perf}))

    elif mtype == "set_config":
        global _last_candles_fetch, _binance_client, _executor, _ws, _flow
        updates = {k: str(v) for k, v in msg.get("config", {}).items()}

        # Block symbol/timeframe changes while trading is active
        if _trading_active and ("symbol" in updates or "timeframe" in updates):
            await ws.send_text(json.dumps({
                "type": "config_saved", "ok": False,
                "error": "Stop trading before changing symbol or timeframe",
            }))
            return

        # Block mode change while a session is open
        if "trading_mode" in updates and _engine and _engine._session:
            await ws.send_text(json.dumps({
                "type": "config_saved", "ok": False,
                "error": "Close the open position before changing mode",
            }))
            return

        await set_config_bulk(updates)

        # Handle trading_mode change — rebuild BinanceClient + executor + WS
        if "trading_mode" in updates:
            new_mode = updates["trading_mode"]
            cfg2 = await load_config()
            logger.info("Mode change → %s", new_mode.upper())

            # Tear down old BinanceClient if present
            if _binance_client:
                await _binance_client.stop()
                _binance_client = None

            if new_mode in ("demo", "live"):
                if not cfg2.api_key or not cfg2.api_secret:
                    logger.warning(
                        "Mode → %s but API keys missing — orders will be simulated. "
                        "Set BINANCE_LIVE_KEY/BINANCE_LIVE_SECRET (live) or "
                        "BINANCE_DEMO_KEY/BINANCE_DEMO_SECRET (demo).",
                        new_mode.upper(),
                    )
                    await ws.send_text(json.dumps({
                        "type": "notification",
                        "text": f"⚠ {new_mode.upper()} mode: API keys missing — simulating orders",
                    }))
                else:
                    try:
                        from binance_client import BinanceClient, BinanceMode
                        bc_mode = BinanceMode.DEMO if new_mode == "demo" else BinanceMode.LIVE
                        _binance_client = BinanceClient(
                            api_key=cfg2.api_key,
                            api_secret=cfg2.api_secret,
                            mode=bc_mode,
                            market="futures",
                            key_type=cfg2.key_type,
                            on_error=_on_exchange_error,
                        )
                        await _binance_client.start()
                        await _binance_client.subscribe_user_data(_on_user_data)
                        _exchange_error = None
                    except Exception as exc:
                        logger.error("BinanceClient init failed on mode change: %s", exc)
                        if _binance_client:
                            try:
                                await _binance_client.stop()
                            except Exception:
                                pass
                        _binance_client = None
                        _exchange_error = f"BinanceClient init failed: {exc}"
                        _on_exchange_error(_exchange_error)

            # Update REST client credentials for the new mode
            await _rest.set_credentials(
                cfg2.api_key, cfg2.api_secret, cfg2.paper_mode, cfg2.key_type
            )

            cfg2b = await load_config()
            _executor = OrderExecutor(_binance_client, _rest, new_mode,
                                      paper_slippage_pct=cfg2b.paper_slippage_pct)
            await _executor.init()
            _engine._executor = _executor

            # Reconnect WS to correct stream URL for the new mode
            await _ws.stop()
            _ws = BinanceWebSocket(
                cfg2.symbol, new_mode, on_trade=_on_trade, on_depth=_on_depth,
                on_error=_on_exchange_error,
            )
            await _ws.start()
            _last_candles_fetch = 0.0

            await _do_broadcast({"type": "mode_changed", "trading_mode": new_mode})

        # Reload WS subscription if symbol changed
        if "symbol" in updates:
            new_sym = updates["symbol"]
            await _ws.switch_symbol(new_sym)
            _last_candles_fetch = 0.0
            cfg2 = await load_config()
            await _executor.prepare_symbol(new_sym, cfg2.leverage)
            await _do_broadcast({"type": "symbol_ready", "symbol": new_sym})

        # Re-apply leverage immediately if it was changed
        if "leverage" in updates and "symbol" not in updates:
            cfg2 = await load_config()
            await _executor.prepare_symbol(cfg2.symbol, cfg2.leverage)

        # Reset candle fetch and re-create flow analyser if timeframe changed
        if "timeframe" in updates:
            _last_candles_fetch = 0.0
            new_tf  = updates["timeframe"]
            new_win = _flow_window_for_timeframe(new_tf)
            _flow   = OrderFlowAnalyzer(window_seconds=new_win, depth_levels=5)
            if _engine:
                _engine._flow = _flow
            logger.info("Order-flow window updated for %s tf: %ds", new_tf, new_win)

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

    elif mtype == "get_top_movers":
        if _last_top_movers:
            await ws.send_text(json.dumps({
                "type":   "top_movers",
                "movers": _last_top_movers,
            }))
        else:
            # Cold start — fetch immediately for this client
            try:
                cfg_cold = await load_config()
                top = await _rest.get_top_movers(n=cfg_cold.scanner_top_n, timeframe=cfg_cold.timeframe)
                movers = _format_movers(top)
                await ws.send_text(json.dumps({"type": "top_movers", "movers": movers}))
            except Exception:
                pass


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
