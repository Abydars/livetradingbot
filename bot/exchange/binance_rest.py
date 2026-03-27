"""
exchange/binance_rest.py — Binance USDT-M Futures REST client (async httpx).

Single shared httpx.AsyncClient instance.  All order endpoints are guarded by
a token-bucket rate-limiter (5 req/s).  Market-data endpoints use a separate
lighter limiter (20 req/s).

Paper-mode: order placement/cancellation returns a simulated response at the
current mark price without touching Binance.
"""
import asyncio
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://fapi.binance.com"
_RECV_WINDOW = 5000


# ---------------------------------------------------------------------------
# Simple token-bucket rate limiter
# ---------------------------------------------------------------------------

class _RateLimiter:
    def __init__(self, rate: float) -> None:
        self._rate = rate          # tokens per second
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


# ---------------------------------------------------------------------------
# Symbol metadata cache
# ---------------------------------------------------------------------------

class SymbolInfo:
    def __init__(self, symbol: str, step_size: float, min_qty: float,
                 tick_size: float, min_notional: float) -> None:
        self.symbol = symbol
        self.step_size = step_size
        self.min_qty = min_qty
        self.tick_size = tick_size
        self.min_notional = min_notional


# ---------------------------------------------------------------------------
# REST Client
# ---------------------------------------------------------------------------

class BinanceRestClient:
    """
    Async Binance USDT-M Futures REST client.

    Usage:
        client = BinanceRestClient(api_key, api_secret, paper_mode)
        await client.init()   # fetches exchange info
        ...
        await client.close()
    """

    def __init__(self, api_key: str, api_secret: str, paper_mode: bool = True) -> None:
        self._key = api_key
        self._secret = api_secret
        self.paper_mode = paper_mode

        self._client = httpx.AsyncClient(
            base_url=_BASE,
            headers={"X-MBX-APIKEY": api_key},
            timeout=10.0,
        )
        self._order_limiter = _RateLimiter(5)
        self._market_limiter = _RateLimiter(20)

        # symbol → SymbolInfo, populated by init()
        self.symbol_info: Dict[str, SymbolInfo] = {}

    async def init(self) -> None:
        """Fetch exchange info and populate symbol metadata."""
        await self._fetch_exchange_info()

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = _RECV_WINDOW
        query = urllib.parse.urlencode(params)
        sig = hmac.new(
            self._secret.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = sig
        return params

    # ------------------------------------------------------------------
    # Market Data (no auth required)
    # ------------------------------------------------------------------

    async def get_ticker_24hr(self, symbol: Optional[str] = None) -> Any:
        """Return 24hr ticker stats for one or all USDT perpetual symbols."""
        await self._market_limiter.acquire()
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        resp = await self._client.get("/fapi/v1/ticker/24hr", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 200,
    ) -> List[List[Any]]:
        """Return OHLCV klines: [[open_time, open, high, low, close, volume, ...], ...]"""
        await self._market_limiter.acquire()
        resp = await self._client.get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json()

    async def get_depth(self, symbol: str, limit: int = 20) -> Dict:
        """Return order-book snapshot {"bids": [...], "asks": [...]}."""
        await self._market_limiter.acquire()
        resp = await self._client.get(
            "/fapi/v1/depth",
            params={"symbol": symbol, "limit": limit},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "bids": [[float(p), float(q)] for p, q in data["bids"]],
            "asks": [[float(p), float(q)] for p, q in data["asks"]],
        }

    async def get_mark_price(self, symbol: str) -> float:
        """Return current mark price for a symbol."""
        await self._market_limiter.acquire()
        resp = await self._client.get(
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        resp.raise_for_status()
        return float(resp.json()["markPrice"])

    async def get_usdt_perp_symbols(self) -> List[str]:
        """Return list of active USDT perpetual contract symbols."""
        info = await self._get_exchange_info_raw()
        return [
            s["symbol"]
            for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        ]

    # ------------------------------------------------------------------
    # Account (auth required)
    # ------------------------------------------------------------------

    async def get_balance(self) -> float:
        """Return available USDT wallet balance."""
        await self._market_limiter.acquire()
        params = self._sign({})
        resp = await self._client.get("/fapi/v2/balance", params=params)
        resp.raise_for_status()
        for asset in resp.json():
            if asset.get("asset") == "USDT":
                return float(asset["availableBalance"])
        return 0.0

    async def get_position_risk(self, symbol: str) -> Optional[Dict]:
        """Return current position risk for a symbol, or None if flat."""
        await self._market_limiter.acquire()
        params = self._sign({"symbol": symbol})
        resp = await self._client.get("/fapi/v2/positionRisk", params=params)
        resp.raise_for_status()
        for pos in resp.json():
            if pos["symbol"] == symbol and float(pos["positionAmt"]) != 0:
                return pos
        return None

    # ------------------------------------------------------------------
    # Orders (auth required; paper_mode short-circuits)
    # ------------------------------------------------------------------

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "BUY" | "SELL"
        qty: float,
        reduce_only: bool = False,
        current_price: float = 0.0,
    ) -> Dict:
        """
        Place a market order.  In paper mode, returns a simulated fill.
        side: "BUY" for LONG entry/hedge, "SELL" for SHORT entry/hedge.
        """
        qty = self._round_qty(symbol, qty)
        prefix = "[PAPER] " if self.paper_mode else ""
        logger.info(
            "%splace_market_order %s %s qty=%.4f reduce_only=%s",
            prefix, symbol, side, qty, reduce_only,
        )

        if self.paper_mode:
            fill_price = current_price or await self.get_mark_price(symbol)
            return {
                "orderId": int(time.time() * 1000),
                "symbol": symbol,
                "side": side,
                "origQty": str(qty),
                "avgPrice": str(fill_price),
                "status": "FILLED",
                "paper": True,
            }

        await self._order_limiter.acquire()
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        signed = self._sign(params)
        resp = await self._client.post("/fapi/v1/order", params=signed)
        resp.raise_for_status()
        return resp.json()

    async def close_position(
        self,
        symbol: str,
        side: str,          # opposite side to close: LONG→SELL, SHORT→BUY
        qty: float,
        current_price: float = 0.0,
    ) -> Dict:
        """Market close (reduceOnly)."""
        return await self.place_market_order(
            symbol, side, qty, reduce_only=True, current_price=current_price
        )

    async def cancel_all_orders(self, symbol: str) -> None:
        if self.paper_mode:
            logger.info("[PAPER] cancel_all_orders %s", symbol)
            return
        await self._order_limiter.acquire()
        params = self._sign({"symbol": symbol})
        resp = await self._client.delete("/fapi/v1/allOpenOrders", params=params)
        resp.raise_for_status()

    # ------------------------------------------------------------------
    # Exchange info / symbol metadata
    # ------------------------------------------------------------------

    async def _get_exchange_info_raw(self) -> Dict:
        await self._market_limiter.acquire()
        resp = await self._client.get("/fapi/v1/exchangeInfo")
        resp.raise_for_status()
        return resp.json()

    async def _fetch_exchange_info(self) -> None:
        info = await self._get_exchange_info_raw()
        for sym in info.get("symbols", []):
            name = sym["symbol"]
            step_size = 0.001
            min_qty = 0.001
            tick_size = 0.1
            min_notional = 5.0
            for f in sym.get("filters", []):
                ft = f.get("filterType", "")
                if ft == "LOT_SIZE":
                    step_size = float(f["stepSize"])
                    min_qty = float(f["minQty"])
                elif ft == "PRICE_FILTER":
                    tick_size = float(f["tickSize"])
                elif ft == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", f.get("minNotional", 5.0)))
            self.symbol_info[name] = SymbolInfo(
                symbol=name,
                step_size=step_size,
                min_qty=min_qty,
                tick_size=tick_size,
                min_notional=min_notional,
            )
        logger.info(
            "BinanceRestClient: loaded info for %d symbols", len(self.symbol_info)
        )

    def _round_qty(self, symbol: str, qty: float) -> float:
        """Round qty to exchange step size and enforce min_qty."""
        info = self.symbol_info.get(symbol)
        if info is None:
            # exchangeInfo not loaded yet; derive precision from the qty magnitude
            # rather than assuming a fixed 3 decimals (wrong for low-priced coins)
            import math
            if qty <= 0:
                return qty
            mag = math.floor(math.log10(abs(qty)))
            decimals = max(0, 8 - mag)   # smaller qty → more decimals, capped at 8
            return round(qty, decimals)
        step = info.step_size
        if step <= 0:
            return qty
        qty = max(qty, info.min_qty)
        # floor to step size
        qty = int(qty / step) * step
        # determine decimal places from step_size
        decimals = max(0, -int(f"{step:e}".split("e")[1]))
        return round(qty, decimals)

    def calc_qty(self, symbol: str, margin_usdt: float, leverage: int, price: float) -> float:
        """
        Calculate position quantity from margin * leverage / price.
        Applies step-size rounding and min_qty enforcement.
        """
        notional = margin_usdt * leverage
        raw_qty = notional / price
        return self._round_qty(symbol, raw_qty)

    # ------------------------------------------------------------------
    # Top movers for auto-switch
    # ------------------------------------------------------------------

    async def get_top_movers(self, n: int = 5, min_quote_volume: float = 20_000_000.0) -> List[Dict]:
        """
        Return top-n USDT perpetual symbols ranked by current momentum spike.

        Sorting by raw quoteVolume always returns BTC/ETH regardless of what is
        actually moving right now.  Instead we score each symbol by:

            score = abs(priceChangePercent) * log10(quoteVolume)

        This rewards symbols with a large *relative* price move that also have
        enough liquidity to trade.  A minimum 24-hr quoteVolume filter removes
        illiquid micro-caps.
        """
        import math
        tickers    = await self.get_ticker_24hr()
        usdt_perps = {s for s in self.symbol_info}
        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if sym not in usdt_perps:
                continue
            qv  = float(t.get("quoteVolume", 0))
            pcp = abs(float(t.get("priceChangePercent", 0)))
            if qv < min_quote_volume:          # skip illiquid symbols
                continue
            score = pcp * math.log10(max(qv, 1))
            candidates.append({**t, "_score": score})
        candidates.sort(key=lambda t: t["_score"], reverse=True)
        return candidates[:n]
