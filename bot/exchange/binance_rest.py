"""
exchange/binance_rest.py — Binance USDT-M Futures REST client (async httpx).

Single shared httpx.AsyncClient instance.  All order endpoints are guarded by
a token-bucket rate-limiter (5 req/s).  Market-data endpoints use a separate
lighter limiter (20 req/s).

Paper-mode: order placement/cancellation returns a simulated response at the
current mark price without touching Binance.
"""
import asyncio
import base64
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://fapi.binance.com"

# ---------------------------------------------------------------------------
# Lightweight indicator helpers — used by get_top_movers() only
# ---------------------------------------------------------------------------

def _scan_ema(prices: list, period: int) -> float:
    """Single EMA value from a price list."""
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2.0 / (period + 1)
    val = sum(prices[:period]) / period
    for p in prices[period:]:
        val = p * k + val * (1 - k)
    return val


def _scan_rsi(closes: list, period: int = 14) -> float:
    """Wilder RSI. Returns 50.0 if insufficient data."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i + 1] - closes[i] for i in range(len(closes) - 1)]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g  = sum(gains[:period]) / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100.0 - 100.0 / (1 + avg_g / avg_l)


def _scan_atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
    """Wilder ATR. Returns 0.0 if insufficient data."""
    if len(closes) < period + 1:
        return 0.0
    trs = [
        max(highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]))
        for i in range(1, len(closes))
    ]
    if len(trs) < period:
        return 0.0
    val = sum(trs[:period]) / period
    for tr in trs[period:]:
        val = (val * (period - 1) + tr) / period
    return val
_RECV_WINDOW = 5000

# Ed25519 signing (requires: pip install cryptography)
try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    _ED25519_AVAILABLE = True
except ImportError:
    _ED25519_AVAILABLE = False


# ---------------------------------------------------------------------------
# Key-type resolution helper
# ---------------------------------------------------------------------------

def _load_ed25519_key(api_secret: str):
    """
    Load an Ed25519 private key from any format Binance uses:
      - PEM PKCS#8 (with or without headers, with real or literal \\n)
      - Raw base64 (32 bytes decoded)
      - PKCS#8 DER base64 (48 bytes decoded)
    Returns an Ed25519PrivateKey instance.
    """
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key, load_der_private_key,
    )
    # Normalise literal \n sequences from .env files
    s = api_secret.strip().replace("\\n", "\n")

    if "-----BEGIN" in s:
        return load_pem_private_key(s.encode(), password=None)

    # Base64-encoded DER or raw bytes
    padded = s + "=" * (-len(s) % 4)
    raw = base64.b64decode(padded)
    if len(raw) == 32:
        return Ed25519PrivateKey.from_private_bytes(raw)
    if len(raw) == 48:
        return load_der_private_key(raw, password=None)
    raise ValueError(
        f"Unrecognised Ed25519 key length: {len(raw)} bytes "
        "(expected PEM, or base64 of 32 raw bytes or 48 DER bytes)"
    )


def _resolve_key_type(api_secret: str, key_type: str):
    """Return (resolved_key_type, ed25519_key_or_None)."""
    if key_type == "hmac":
        return "hmac", None
    if not _ED25519_AVAILABLE or not api_secret:
        return "hmac", None
    if key_type == "ed25519":
        key = _load_ed25519_key(api_secret)  # raises on failure
        return "ed25519", key
    # "auto" — try Ed25519, fall back to HMAC
    try:
        key = _load_ed25519_key(api_secret)
        return "ed25519", key
    except Exception:
        pass
    return "hmac", None


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
                 tick_size: float, min_notional: float,
                 status: str = "TRADING") -> None:
        self.symbol = symbol
        self.step_size = step_size
        self.min_qty = min_qty
        self.tick_size = tick_size
        self.min_notional = min_notional
        self.status = status


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

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        paper_mode: bool = True,
        key_type: str = "auto",
    ) -> None:
        self.paper_mode = paper_mode
        self._order_limiter = _RateLimiter(5)
        self._market_limiter = _RateLimiter(20)
        # symbol → SymbolInfo, populated by init()
        self.symbol_info: Dict[str, SymbolInfo] = {}
        # httpx client + signing — initialised via set_credentials
        self._client: Optional[httpx.AsyncClient] = None
        self._key = ""
        self._secret = ""
        self._key_type = "hmac"
        self._ed25519_key = None
        self._set_credentials_sync(api_key, api_secret, key_type)

    def _set_credentials_sync(self, api_key: str, api_secret: str, key_type: str) -> None:
        """Set signing credentials and (re)create the httpx client."""
        self._key    = api_key
        self._secret = api_secret
        self._key_type, self._ed25519_key = _resolve_key_type(api_secret, key_type)
        new_client = httpx.AsyncClient(
            base_url=_BASE,
            headers={"X-MBX-APIKEY": api_key},
            timeout=10.0,
        )
        old_client = self._client
        self._client = new_client
        if old_client is not None:
            # Schedule close of old client without blocking
            asyncio.get_event_loop().call_soon(
                lambda: asyncio.ensure_future(old_client.aclose())
            )
        logger.info(
            "BinanceRestClient credentials updated | key_type=%s paper=%s",
            self._key_type, self.paper_mode,
        )

    async def set_credentials(
        self, api_key: str, api_secret: str, paper_mode: bool, key_type: str = "auto"
    ) -> None:
        """Update credentials after a mode change (symbol_info cache preserved)."""
        self.paper_mode = paper_mode
        self._set_credentials_sync(api_key, api_secret, key_type)

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
        if self._key_type == "ed25519":
            sig = base64.b64encode(self._ed25519_key.sign(query.encode())).decode()
        else:
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

    async def get_klines_batch(
        self,
        symbols: List[str],
        interval: str = "1m",
        limit: int = 25,
    ) -> Dict[str, List]:
        """
        Fetch klines for multiple symbols concurrently.
        Returns {symbol: klines_list}. Symbols that fail are omitted silently.
        Rate limiter serialises requests at 20/s — 20 symbols ≈ 1 second total.
        """
        async def _fetch_one(sym: str):
            try:
                await self._market_limiter.acquire()
                resp = await self._client.get(
                    "/fapi/v1/klines",
                    params={"symbol": sym, "interval": interval, "limit": limit},
                )
                resp.raise_for_status()
                return sym, resp.json()
            except Exception:
                return sym, None

        results = await asyncio.gather(*[_fetch_one(s) for s in symbols])
        return {sym: data for sym, data in results if data}

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
        resp = await self._client.get("/fapi/v3/balance", params=params)
        resp.raise_for_status()
        for asset in resp.json():
            if asset.get("asset") == "USDT":
                return float(asset["availableBalance"])
        return 0.0

    async def get_position_risk(self, symbol: str) -> Optional[Dict]:
        """Return current position risk for a symbol, or None if flat."""
        await self._market_limiter.acquire()
        params = self._sign({"symbol": symbol})
        resp = await self._client.get("/fapi/v3/positionRisk", params=params)
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
                status=sym.get("status", ""),
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

    async def _fetch_24h_tickers(self) -> List[Dict]:
        """Fetch all USDT-M 24h tickers (shared by all scanner types)."""
        await self._market_limiter.acquire()
        resp = await self._client.get("/fapi/v1/ticker/24hr")
        resp.raise_for_status()
        return resp.json()

    async def get_top_movers(
        self,
        n: int = 5,
        min_quote_volume: float = 20_000_000.0,
        timeframe: str = "1m",
    ) -> List[Dict]:
        """
        Return top-n USDT perpetual symbols ranked for fast trading quality.

        Two-phase scoring:

        Phase 1 — Ticker filter (free, no extra calls):
          Reduces 200+ symbols to top-25 candidates using:
          - abs(priceChangePercent): raw 24h move size
          - (high-low)/price: 24h volatility proxy
          - Position in 24h range aligned with trend: freshness signal
          - log10(quoteVolume): liquidity gate

        Phase 2 — Kline deep score (25 concurrent kline requests, ~1s):
          For each candidate computes on the bot's actual timeframe:
          - volume_surge: last-3-candle avg / 10-candle baseline avg
          - momentum_pct: abs price change over last 5 candles
          - atr_pct: 14-period ATR as % of price
          - trend_aligned: EMA9 > EMA21 and price above/below correctly
          - rsi: RSI(14) — symbols outside 20–80 are penalised
          - bias: "LONG" if bullish setup, "SHORT" if bearish

        Final score:
          volume_surge×0.35 + momentum_pct×0.30 + atr_pct×0.20 + trend_bonus×0.15

        Symbols with RSI > 80 or < 20 receive a 50% score penalty.
        """
        import math

        # ── Phase 1: ticker quick-filter ───────────────────────────────
        tickers   = await self._fetch_24h_tickers()
        usdt_perps = {
            s for s, info in self.symbol_info.items()
            if info.status == "TRADING"
        }

        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if sym not in usdt_perps:
                continue
            qv = float(t.get("quoteVolume", 0))
            if qv < min_quote_volume:
                continue

            price = float(t.get("lastPrice", 0))
            high  = float(t.get("highPrice", 0))
            low   = float(t.get("lowPrice", 0))
            pcp   = float(t.get("priceChangePercent", 0))

            if price <= 0:
                continue

            # 24h range as % of price — volatility proxy
            rng     = high - low
            vol_pct = rng / price if price > 0 else 0

            # Position in 24h range (0=at low, 1=at high)
            # Aligned with trend direction = fresh move (not exhausted)
            range_pos   = (price - low) / rng if rng > 0 else 0.5
            trend_fresh = range_pos if pcp > 0 else (1 - range_pos)

            p1_score = (
                abs(pcp)
                * vol_pct
                * math.log10(max(qv, 1))
                * (0.4 + trend_fresh * 0.6)
            )

            candidates.append({
                **t,
                "_p1_score":  p1_score,
                "_score":     p1_score,   # will be replaced in phase 2
                "_bias":      "LONG" if pcp >= 0 else "SHORT",
                "_vol_surge": 1.0,
                "_momentum":  abs(pcp),
                "_atr_pct":   vol_pct * 100,
                "_scanner":   "momentum",
            })

        # Keep top 15 for phase 2 — Phase 1 already filters well enough.
        # Smaller pool = faster batch fetch (~300ms saved per scan cycle).
        candidates.sort(key=lambda x: x["_p1_score"], reverse=True)
        phase2_pool = candidates[:15]

        # ── Phase 2: kline deep score ──────────────────────────────────
        pool_syms  = [c["symbol"] for c in phase2_pool]
        klines_map = await self.get_klines_batch(
            pool_syms, interval=timeframe, limit=70
        )

        for c in phase2_pool:
            sym  = c["symbol"]
            data = klines_map.get(sym)
            if not data or len(data) < 15:
                continue

            highs  = [float(k[2]) for k in data]
            lows   = [float(k[3]) for k in data]
            closes = [float(k[4]) for k in data]
            vols   = [float(k[5]) for k in data]

            price = closes[-1]
            if price <= 0:
                continue

            # ── ATR — used as gate and normaliser, not as score component ──
            atr     = _scan_atr(highs, lows, closes, 14)
            atr_pct = (atr / price * 100) if price > 0 else 0.0

            # Gate: skip symbols that are too quiet or too chaotic to trade
            if atr_pct < 0.3:
                continue   # not enough volatility for reliable signals
            atr_penalty = 0.60 if atr_pct > 5.0 else 1.0   # chaotic = penalty

            # ── Volume surge: last-3-candle avg vs 10-candle baseline ──
            if len(vols) >= 13:
                recent_vol   = sum(vols[-3:]) / 3
                baseline_vol = sum(vols[-13:-3]) / 10
                vol_surge    = recent_vol / baseline_vol if baseline_vol > 0 else 1.0
            else:
                vol_surge = 1.0

            # ── Momentum with acceleration check ──
            # Base: abs price change over last 5 candles
            momentum_pct = abs(closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0.0

            # Acceleration: compare last 3 candles vs previous 3 candles
            # accel > 1 = momentum speeding up (good), < 1 = slowing down (bad)
            if len(closes) >= 7:
                recent_move = abs(closes[-1] - closes[-4]) / max(closes[-4], 1e-10)
                older_move  = abs(closes[-4] - closes[-7]) / max(closes[-7], 1e-10)
                accel = recent_move / older_move if older_move > 0.0001 else 1.0
                accel = min(max(accel, 0.3), 3.0)   # clamp 0.3x – 3x
            else:
                accel = 1.0
            momentum_score = momentum_pct * min(accel, 2.0)

            # ── RSI — tiered penalty ──
            rsi_val = _scan_rsi(closes, 14)
            if rsi_val > 80 or rsi_val < 20:
                rsi_penalty = 0.50   # hard exhaustion
            elif rsi_val > 70 or rsi_val < 30:
                rsi_penalty = 0.80   # near exhaustion
            else:
                rsi_penalty = 1.0

            # ── EMA trend: continuous strength instead of binary bonus ──
            ema9  = _scan_ema(closes, 9)
            ema21 = _scan_ema(closes, 21)
            ema50 = _scan_ema(closes, 50)
            # Use EMA21 vs EMA50 — matches signal engine TREND component (23% weight)
            # EMA9 vs EMA21 was causing scanner/signal engine disagreement
            bullish_setup = ema21 > ema50 and closes[-1] > ema21
            bearish_setup = ema21 < ema50 and closes[-1] < ema21

            if atr_pct > 0:
                ema_diff_pct   = abs(ema21 - ema50) / ema21
                trend_strength = min(ema_diff_pct / (atr_pct / 100), 1.0)   # normalised 0→1
            else:
                trend_strength = 0.0
            trend_score = trend_strength * 0.20   # max contribution 0.20

            # ── Signal tendency: lightweight pre-check aligned with signal engine ──
            # Uses EMA trend + MACD histogram as a proxy for what the signal engine
            # will compute. Symbols with clearer directional tendency rank higher,
            # reducing switches to symbols that immediately give NEUTRAL signal.
            if len(closes) >= 27:
                # MACD-like: 9-period EMA vs 26-period EMA delta, ATR-normalised
                ema26    = _scan_ema(closes, 26)
                macd_val = (ema9 - ema26) / ema26 if ema26 > 0 else 0.0
                macd_norm = abs(macd_val) / (atr_pct / 100) if atr_pct > 0 else 0.0
                macd_norm = min(macd_norm, 1.0)

                # Trend component (EMA21 vs EMA50 proxy using available data)
                trend_norm = min(abs(ema21 - ema50) / ema21 / (atr_pct / 100), 1.0) if atr_pct > 0 else 0.0

                signal_tendency = (macd_norm * 0.5 + trend_norm * 0.5)
            else:
                signal_tendency = 0.0

            # ── Direction bias ──
            if bullish_setup:
                bias = "LONG"
            elif bearish_setup:
                bias = "SHORT"
            else:
                bias = c["_bias"]

            # ── Normalise all components to 0→1 before applying weights ──
            # vol_surge is unbounded (pump can be 10+), cap at 3× as "maximum useful surge"
            # momentum_score = momentum_pct × accel, also unbounded, cap at 3.0
            # signal_tendency already 0→1 (clamped)
            # trend_score already 0→0.20, normalise back to 0→1
            vol_norm   = min(vol_surge / 3.0, 1.0)
            mom_norm   = min(momentum_score / 3.0, 1.0)
            sig_norm   = signal_tendency               # already 0→1
            trend_norm = min(trend_score / 0.20, 1.0) # 0.20 is max from trend_strength*0.20

            # Weights now sum to 1.0 and all inputs are in 0→1 range
            # Weights: vol_surge 0.30, momentum_accel 0.25, signal_tendency 0.20, trend 0.25
            base_score = (
                vol_norm   * 0.30
                + mom_norm * 0.25
                + sig_norm * 0.20
                + trend_norm * 0.25
            )
            final_score = base_score * rsi_penalty * atr_penalty

            c["_score"]     = final_score
            c["_bias"]      = bias
            c["_vol_surge"] = round(vol_surge, 2)
            c["_momentum"]  = round(momentum_pct, 3)
            c["_atr_pct"]   = round(atr_pct, 3)

        # Re-sort by final score and return top n
        phase2_pool.sort(key=lambda x: x["_score"], reverse=True)
        return phase2_pool[:n]

    async def get_top_movers_breakout(
        self,
        n: int = 5,
        min_quote_volume: float = 20_000_000.0,
        timeframe: str = "1m",
    ) -> List[Dict]:
        """
        Breakout scanner — finds symbols showing compression then expansion.
        Phase 1: same ticker filter as get_top_movers.
        Phase 2 score:
          bb_squeeze   (0.35) — BB width shrinking then expanding (compression → breakout)
          vol_confirm  (0.30) — volume spike on breakout candle vs baseline
          atr_expand   (0.20) — ATR increasing (volatility expanding, confirms breakout)
          price_break  (0.15) — price outside N-candle high/low
        """
        # ── Phase 1: same ticker filter ──
        tickers = await self._fetch_24h_tickers()
        if not tickers:
            return []

        import math
        usdt_perps = await self.get_usdt_perp_symbols()
        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or sym not in usdt_perps:
                continue
            try:
                pcp = float(t.get("priceChangePercent", 0))
                qv  = float(t.get("quoteVolume", 0))
                hp  = float(t.get("highPrice", 0))
                lp  = float(t.get("lowPrice", 0))
                cp  = float(t.get("lastPrice", 0))
            except (ValueError, TypeError):
                continue
            if qv < min_quote_volume or cp <= 0:
                continue
            vol_pct = (hp - lp) / cp if cp > 0 else 0
            range_pos  = (cp - lp) / (hp - lp) if (hp - lp) > 0 else 0.5
            trend_fresh = range_pos if pcp >= 0 else (1 - range_pos)
            p1_score = (
                abs(pcp)
                * vol_pct
                * math.log10(max(qv, 1))
                * (0.4 + trend_fresh * 0.6)
            )
            candidates.append({**t, "_p1_score": p1_score, "_score": p1_score,
                                "_bias": "LONG" if pcp >= 0 else "SHORT",
                                "_scanner": "breakout"})

        candidates.sort(key=lambda x: x["_p1_score"], reverse=True)
        phase2_pool = candidates[:15]

        # ── Phase 2: breakout-specific scoring ──
        pool_syms  = [c["symbol"] for c in phase2_pool]
        klines_map = await self.get_klines_batch(pool_syms, interval=timeframe, limit=25)

        for c in phase2_pool:
            sym  = c["symbol"]
            data = klines_map.get(sym)
            if not data or len(data) < 15:
                continue

            highs  = [float(k[2]) for k in data]
            lows   = [float(k[3]) for k in data]
            closes = [float(k[4]) for k in data]
            vols   = [float(k[5]) for k in data]
            price  = closes[-1]
            if price <= 0:
                continue

            atr     = _scan_atr(highs, lows, closes, 14)
            atr_pct = (atr / price * 100) if price > 0 else 0.0
            if atr_pct < 0.3:
                continue
            rsi_val = _scan_rsi(closes, 14)
            if rsi_val > 80 or rsi_val < 20:
                rsi_penalty = 0.50
            elif rsi_val > 70 or rsi_val < 30:
                rsi_penalty = 0.80
            else:
                rsi_penalty = 1.0

            # BB squeeze: compare current BB width to recent average
            from statistics import mean as _mean, stdev as _stdev
            bb_width_series = []
            for i in range(5, len(closes)):
                w = closes[i-5:i]
                if len(w) >= 5:
                    try:
                        sd = _stdev(w)
                        mid = _mean(w)
                        bb_width_series.append(sd * 4 / mid if mid > 0 else 0)
                    except Exception:
                        pass
            current_bb_w = bb_width_series[-1] if bb_width_series else 0
            avg_bb_w     = _mean(bb_width_series[-10:]) if len(bb_width_series) >= 10 else current_bb_w
            # Squeeze: width was below avg, now expanding
            squeeze_score = min((current_bb_w / avg_bb_w) if avg_bb_w > 0 else 1.0, 3.0) / 3.0

            # Volume confirmation on last candle
            baseline_vol  = _mean(vols[-11:-1]) if len(vols) >= 11 else (sum(vols)/len(vols))
            vol_spike     = min(vols[-1] / baseline_vol if baseline_vol > 0 else 1.0, 5.0) / 5.0

            # ATR expansion (current ATR vs recent)
            # Use simple proxy: last candle range vs ATR
            last_range = highs[-1] - lows[-1]
            atr_expand = min(last_range / atr if atr > 0 else 1.0, 3.0) / 3.0

            # Price breaking N-candle high/low (last 20 candles)
            lookback = min(20, len(closes) - 1)
            recent_high = max(highs[-lookback-1:-1])
            recent_low  = min(lows[-lookback-1:-1])
            if closes[-1] > recent_high:
                price_break = 1.0
                bias = "LONG"
            elif closes[-1] < recent_low:
                price_break = 1.0
                bias = "SHORT"
            else:
                # Proximity to breakout
                dist_up   = (recent_high - closes[-1]) / atr if atr > 0 else 1.0
                dist_down = (closes[-1] - recent_low)  / atr if atr > 0 else 1.0
                if dist_up < dist_down:
                    price_break = max(0, 1 - dist_up / 2)
                    bias = "LONG"
                else:
                    price_break = max(0, 1 - dist_down / 2)
                    bias = "SHORT"

            base_score  = (
                squeeze_score * 0.35
                + vol_spike   * 0.30
                + atr_expand  * 0.20
                + price_break * 0.15
            )
            final_score = base_score * rsi_penalty

            c["_score"]     = final_score
            c["_bias"]      = bias
            c["_vol_surge"] = round(vols[-1] / (baseline_vol or 1), 2)
            c["_momentum"]  = round((closes[-1] - closes[-2]) / closes[-2] * 100, 3) if len(closes) >= 2 else 0.0
            c["_atr_pct"]   = round(atr_pct, 3)
            c["_scanner"]   = "breakout"

        phase2_pool.sort(key=lambda x: x["_score"], reverse=True)
        return phase2_pool[:n]

    async def get_top_movers_trendpull(
        self,
        n: int = 5,
        min_quote_volume: float = 20_000_000.0,
        timeframe: str = "1m",
    ) -> List[Dict]:
        """
        Trend pullback scanner — finds symbols in uptrend that pulled back to EMA.
        Phase 1: same ticker filter.
        Phase 2 score:
          ema_align    (0.35) — EMA9 > EMA21 > EMA50 (clean trend structure)
          pullback_pct (0.30) — price pulled back close to EMA21 (0.5-2× ATR from EMA21)
          bounce       (0.25) — last 2-3 candles turning back in trend direction
          trend_slope  (0.10) — EMA21 slope (trend strengthening)
        """
        # ── Phase 1: same ticker filter ──
        tickers = await self._fetch_24h_tickers()
        if not tickers:
            return []

        import math
        usdt_perps = await self.get_usdt_perp_symbols()
        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or sym not in usdt_perps:
                continue
            try:
                pcp = float(t.get("priceChangePercent", 0))
                qv  = float(t.get("quoteVolume", 0))
                hp  = float(t.get("highPrice", 0))
                lp  = float(t.get("lowPrice", 0))
                cp  = float(t.get("lastPrice", 0))
            except (ValueError, TypeError):
                continue
            if qv < min_quote_volume or cp <= 0:
                continue
            vol_pct    = (hp - lp) / cp if cp > 0 else 0
            range_pos  = (cp - lp) / (hp - lp) if (hp - lp) > 0 else 0.5
            trend_fresh = range_pos if pcp >= 0 else (1 - range_pos)
            p1_score = (
                abs(pcp)
                * vol_pct
                * math.log10(max(qv, 1))
                * (0.4 + trend_fresh * 0.6)
            )
            candidates.append({**t, "_p1_score": p1_score, "_score": p1_score,
                                "_bias": "LONG" if pcp >= 0 else "SHORT",
                                "_scanner": "trendpull"})

        candidates.sort(key=lambda x: x["_p1_score"], reverse=True)
        phase2_pool = candidates[:15]

        # ── Phase 2: trend pullback scoring ──
        pool_syms  = [c["symbol"] for c in phase2_pool]
        klines_map = await self.get_klines_batch(pool_syms, interval=timeframe, limit=120)

        for c in phase2_pool:
            sym  = c["symbol"]
            data = klines_map.get(sym)
            if not data or len(data) < 55:
                continue

            highs  = [float(k[2]) for k in data]
            lows   = [float(k[3]) for k in data]
            closes = [float(k[4]) for k in data]
            price  = closes[-1]
            if price <= 0:
                continue

            atr     = _scan_atr(highs, lows, closes, 14)
            atr_pct = (atr / price * 100) if price > 0 else 0.0
            if atr_pct < 0.3:
                continue

            rsi_val = _scan_rsi(closes, 14)
            if rsi_val > 80 or rsi_val < 20:
                rsi_penalty = 0.50
            elif rsi_val > 70 or rsi_val < 30:
                rsi_penalty = 0.80
            else:
                rsi_penalty = 1.0

            ema9  = _scan_ema(closes, 9)
            ema21 = _scan_ema(closes, 21)
            ema50 = _scan_ema(closes, 50)

            # EMA alignment score
            if ema9 > ema21 > ema50:
                ema_align = 1.0
                bias = "LONG"
            elif ema9 < ema21 < ema50:
                ema_align = 1.0
                bias = "SHORT"
            elif (ema21 > ema50 and ema9 > ema50) or (ema21 < ema50 and ema9 < ema50):
                ema_align = 0.5
                bias = "LONG" if ema21 > ema50 else "SHORT"
            else:
                ema_align = 0.0
                bias = c["_bias"]

            if ema_align == 0.0:
                c["_score"] = 0.0
                continue

            # Pullback depth: price should be near EMA21 (0.3 to 2.0 × ATR away)
            dist_from_ema21 = abs(price - ema21)
            if atr > 0:
                dist_atrs = dist_from_ema21 / atr
                # Ideal pullback: 0.3-1.5 ATR from EMA21
                if 0.3 <= dist_atrs <= 1.5:
                    pullback_score = 1.0 - abs(dist_atrs - 0.9) / 0.9
                    pullback_score = max(pullback_score, 0.1)
                elif dist_atrs < 0.3:
                    pullback_score = 0.4  # at EMA21 — ok but not ideal
                else:
                    pullback_score = max(0, 1 - (dist_atrs - 1.5) / 1.5)  # fades as distance grows
            else:
                pullback_score = 0.0

            # Price should be on correct side approaching EMA21
            approaching_ema = (bias == "LONG" and price < ema9 and price > ema21 * 0.99) or \
                              (bias == "SHORT" and price > ema9 and price < ema21 * 1.01)
            if not approaching_ema:
                pullback_score *= 0.5

            # Bounce: last 3 candles turning back in trend direction
            if len(closes) >= 4:
                if bias == "LONG":
                    bounce = 1.0 if (closes[-1] > closes[-2] > closes[-3]) else \
                             0.5 if closes[-1] > closes[-2] else 0.0
                else:
                    bounce = 1.0 if (closes[-1] < closes[-2] < closes[-3]) else \
                             0.5 if closes[-1] < closes[-2] else 0.0
            else:
                bounce = 0.0

            # Trend slope: EMA21 now vs EMA21 10 candles ago
            ema21_old = _scan_ema(closes[:-10], 21) if len(closes) > 30 else ema21
            slope_pct = (ema21 - ema21_old) / ema21_old if ema21_old > 0 else 0
            trend_slope = min(abs(slope_pct) / (atr_pct / 100) if atr_pct > 0 else 0, 1.0)

            base_score  = (
                ema_align     * 0.35
                + pullback_score * 0.30
                + bounce         * 0.25
                + trend_slope    * 0.10
            )
            final_score = base_score * rsi_penalty

            c["_score"]     = final_score
            c["_bias"]      = bias
            c["_vol_surge"] = 1.0
            c["_momentum"]  = round((closes[-1] - closes[-6]) / closes[-6] * 100, 3) if len(closes) >= 6 else 0.0
            c["_atr_pct"]   = round(atr_pct, 3)
            c["_scanner"]   = "trendpull"

        phase2_pool.sort(key=lambda x: x["_score"], reverse=True)
        return phase2_pool[:n]

