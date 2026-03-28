"""
binance_client.py
=================
Production-grade async Binance client for USDT-M Futures & Spot.

Features:
  - Demo (testnet) or Live mode via BinanceMode enum
  - WebSocket API for ultra-fast order placement (no per-request TCP handshake)
  - Persistent aiohttp session for REST calls (connection pooling)
  - WebSocket market data streams (kline, depth, aggTrade, bookTicker, markPrice)
  - User data stream (order fills, position updates, balance changes)
  - Auto-reconnect with exponential backoff on all WebSocket connections
  - Keep-alive ping loop (every 3 min) to prevent Binance 24h WS disconnect
  - Listen key auto-refresh (every 30 min) for user data stream
  - Token bucket rate limiter (order rate + raw request rate)
  - HMAC-SHA256 request signing (REST)
  - Ed25519 signing for WebSocket API (required for ws-fapi live orders)
  - key_type="hmac" or "ed25519" — auto-detected from secret format
  - Position & account queries for futures
  - Structured logging with timestamps

Usage:
    import asyncio
    from binance_client import BinanceClient, BinanceMode, OrderSide, OrderType

    async def main():
        client = BinanceClient(
            api_key="YOUR_KEY",
            api_secret="YOUR_SECRET",
            mode=BinanceMode.DEMO,          # or BinanceMode.LIVE
            market="futures",               # "futures" or "spot"
        )
        async with client:
            # Place order via WebSocket API (fastest)
            result = await client.place_order_ws(
                symbol="BTCUSDT",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=0.001,
            )
            print(result)

    asyncio.run(main())
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

# Ed25519 signing for Binance WS API (requires: pip install cryptography)
try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    _ED25519_AVAILABLE = True
except ImportError:
    _ED25519_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BinanceClient")


# ---------------------------------------------------------------------------
# Enums & Constants
# ---------------------------------------------------------------------------
class BinanceMode(Enum):
    LIVE = "live"
    DEMO = "demo"  # testnet


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    STOP = "STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class TimeInForce(str, Enum):
    GTC = "GTC"   # Good Till Cancel
    IOC = "IOC"   # Immediate Or Cancel
    FOK = "FOK"   # Fill Or Kill
    GTX = "GTX"   # Post Only (maker only)


class PositionSide(str, Enum):
    BOTH = "BOTH"   # one-way mode
    LONG = "LONG"   # hedge mode
    SHORT = "SHORT" # hedge mode


class MarginType(str, Enum):
    ISOLATED = "ISOLATED"
    CROSSED = "CROSS"


# URL configs per mode and market
_URLS = {
    "futures": {
        BinanceMode.LIVE: {
            "rest":   "https://fapi.binance.com",
            "ws_api": "wss://ws-fapi.binance.com/ws-fapi/v1",
            "ws_stream": "wss://fstream.binance.com",
        },
        BinanceMode.DEMO: {
            "rest":      "https://demo-fapi.binance.com",
            "ws_api":    None,   # demo-fapi testnet does not support WS API order placement
            "ws_stream": "wss://fstream.binancefuture.com",
        },
    },
    "spot": {
        BinanceMode.LIVE: {
            "rest":   "https://api.binance.com",
            "ws_api": "wss://ws-api.binance.com/ws-api/v3",
            "ws_stream": "wss://stream.binance.com:9443",
        },
        BinanceMode.DEMO: {
            "rest":   "https://testnet.binance.vision",
            "ws_api": "wss://testnet.binance.vision/ws-api/v3",
            "ws_stream": "wss://testnet.binance.vision",
        },
    },
}

PING_INTERVAL_SEC   = 180   # keep WS alive (Binance disconnects at 24h, ping resets)
LISTEN_KEY_TTL_SEC  = 1800  # refresh listen key every 30 min


# ---------------------------------------------------------------------------
# Token Bucket Rate Limiter
# ---------------------------------------------------------------------------
class TokenBucket:
    """Thread-safe async token bucket for rate limiting."""

    def __init__(self, capacity: int, refill_rate: float):
        """
        capacity    : max tokens (burst size)
        refill_rate : tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self.capacity,
                self._tokens + elapsed * self.refill_rate
            )
            self._last_refill = now
            if self._tokens < tokens:
                wait = (tokens - self._tokens) / self.refill_rate
                logger.debug("Rate limit: sleeping %.3fs", wait)
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= tokens


# ---------------------------------------------------------------------------
# WebSocket manager with auto-reconnect
# ---------------------------------------------------------------------------
class WSConnection:
    """
    Manages a single WebSocket connection with:
      - Auto-reconnect (exponential backoff, max 60s)
      - Keep-alive ping loop
      - Message dispatch to registered handlers
      - Compatible with websockets < 12 (WebSocketClientProtocol)
        and >= 12 (ClientConnection, no .closed attribute)
    """

    def __init__(self, url: str, name: str = "ws", on_error: Callable = None):
        self.url = url
        self.name = name
        self._ws = None   # WebSocketClientProtocol or ClientConnection
        self._handlers: List[Callable] = []
        self._on_error = on_error
        self._running = False
        self._ready = asyncio.Event()

    @staticmethod
    def _is_open(ws) -> bool:
        """
        websockets < 12 : ws.closed  (bool property)
        websockets >= 12: ws.state   (State enum — OPEN / CLOSING / CLOSED)
        """
        if ws is None:
            return False
        # Try the legacy .closed attribute first
        closed = getattr(ws, "closed", None)
        if closed is not None:
            return not closed
        # websockets >= 12: check state enum
        state = getattr(ws, "state", None)
        if state is not None:
            try:
                from websockets.connection import OPEN
                return state == OPEN
            except ImportError:
                pass
        # Fallback: assume open if we have a ws object
        return True

    def add_handler(self, fn: Callable):
        self._handlers.append(fn)

    async def start(self):
        self._running = True
        asyncio.create_task(self._run_loop())
        await self._ready.wait()

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def send(self, payload: dict):
        if not self._is_open(self._ws):
            raise ConnectionError(f"[{self.name}] WebSocket not connected")
        await self._ws.send(json.dumps(payload))

    async def _run_loop(self):
        backoff = 1
        while self._running:
            try:
                logger.info("[%s] Connecting to %s", self.name, self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,   # we manage pings manually
                    max_size=None,
                    open_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._ready.set()
                    backoff = 1
                    logger.info("[%s] Connected", self.name)
                    ping_task = asyncio.create_task(self._ping_loop())
                    try:
                        async for raw in ws:
                            data = json.loads(raw)
                            # serverShutdown: server closes conn after 24h; reconnect cleanly
                            if data.get("event") == "serverShutdown" or data.get("e") == "serverShutdown":
                                msg = f"[{self.name}] serverShutdown received — reconnecting"
                                logger.warning(msg)
                                if self._on_error:
                                    self._on_error(msg)
                                break
                            for handler in self._handlers:
                                asyncio.create_task(handler(data))
                    finally:
                        ping_task.cancel()
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                if not self._running:
                    break
                self._ready.clear()
                msg = f"[{self.name}] Disconnected: {e}. Reconnecting in {backoff}s…"
                logger.warning(msg)
                if self._on_error:
                    self._on_error(msg)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                msg = f"[{self.name}] Unexpected error: {e}"
                logger.error(msg)
                if self._on_error:
                    self._on_error(msg)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _ping_loop(self):
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            try:
                if self._is_open(self._ws):
                    await self._ws.ping()
                    logger.debug("[%s] Ping sent", self.name)
            except Exception as e:
                logger.warning("[%s] Ping failed: %s", self.name, e)


# ---------------------------------------------------------------------------
# Main BinanceClient
# ---------------------------------------------------------------------------
class BinanceClient:
    """
    Async Binance client supporting Futures and Spot, demo and live.

    Fastest order path:  place_order_ws()   → WebSocket API
    Standard order path: place_order_rest() → REST with connection pooling

    All public methods are coroutines. Use as async context manager or
    call start() / stop() manually.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        mode: BinanceMode = BinanceMode.DEMO,
        market: str = "futures",   # "futures" or "spot"
        recv_window: int = 5000,
        key_type: str = "auto",    # "auto" | "hmac" | "ed25519"
        on_error: Callable = None,  # optional callback(msg: str) for WS errors
        # Rate limits (Binance default: 1200 weight/min, 300 orders/10s)
        order_rate_capacity: int = 300,
        order_rate_refill: float = 30.0,   # tokens/sec → 300/10s
        request_rate_capacity: int = 1200,
        request_rate_refill: float = 20.0,  # tokens/sec → 1200/60s
    ):
        assert market in ("futures", "spot"), "market must be 'futures' or 'spot'"
        self.api_key    = api_key
        self.api_secret = api_secret
        self.mode       = mode
        self.market     = market
        self.recv_window = recv_window

        # Determine signing method
        if key_type == "auto":
            self._key_type = self._detect_key_type(api_secret)
        else:
            self._key_type = key_type

        self._ed25519_key = None
        if self._key_type == "ed25519":
            try:
                self._ed25519_key = self._load_ed25519_key(api_secret)
            except (ValueError, RuntimeError) as e:
                logger.warning(
                    "key_type='ed25519' was requested but loading failed (%s). "
                    "Falling back to HMAC. "
                    "To use Ed25519: create a new Ed25519 API key at "
                    "Binance → API Management → Create API → Ed25519, "
                    "then paste the base64 private key as BINANCE_LIVE_SECRET.",
                    e
                )
                self._key_type = "hmac"

        urls = _URLS[market][mode]
        self._rest_base    = urls["rest"]
        self._ws_api_url   = urls["ws_api"]
        self._ws_stream_url = urls["ws_stream"]

        # Rate limiters
        self._order_limiter   = TokenBucket(order_rate_capacity, order_rate_refill)
        self._request_limiter = TokenBucket(request_rate_capacity, request_rate_refill)

        # REST session (persistent, connection pooling)
        self._session: Optional[aiohttp.ClientSession] = None
        self._on_error = on_error

        # WS API message rate limiter: Binance limit = 10 incoming msgs/sec
        # We stay well under with capacity=8, refill=8/s
        self._ws_msg_limiter = TokenBucket(capacity=8, refill_rate=8.0)

        # WebSocket API connection (order placement)
        self._ws_api: Optional[WSConnection] = None
        self._ws_api_pending: Dict[str, asyncio.Future] = {}

        # Stream subscriptions: stream_name → list of callbacks
        self._streams: Dict[str, WSConnection] = {}
        self._stream_handlers: Dict[str, List[Callable]] = {}

        # User data stream
        self._listen_key: Optional[str] = None
        self._user_stream_ws: Optional[WSConnection] = None
        self._user_data_handlers: List[Callable] = []

        self._started = False
        logger.info(
            "BinanceClient init | mode=%s market=%s base=%s key_type=%s",
            mode.value, market, self._rest_base, self._key_type
        )


    # ------------------------------------------------------------------
    # Key type detection & signing
    # ------------------------------------------------------------------
    @staticmethod
    def _detect_key_type(secret: str) -> str:
        """
        Detect key type from the secret string.

        Ed25519 private keys come in several forms:
          - PEM PKCS#8:  starts with '-----BEGIN PRIVATE KEY-----'
          - Raw base64:  base64-encoded 32 bytes  → 44 chars
          - DER base64:  base64-encoded 48 bytes  → 64 chars  (Binance API Management)

        HMAC secrets from Binance are 64-character hex strings.
        """
        stripped = secret.strip().replace("\\n", "\n")
        # PEM format — definitive signal
        if "-----BEGIN PRIVATE KEY-----" in stripped or "-----BEGIN ED25519 PRIVATE KEY-----" in stripped:
            return "ed25519"
        # Pure hex strings are always HMAC
        if all(c in "0123456789abcdefABCDEF" for c in stripped):
            return "hmac"
        # Try base64 decode and check for known Ed25519 lengths (32 raw or 48 DER)
        try:
            padded = stripped + "=" * (-len(stripped) % 4)
            decoded = base64.b64decode(padded)
            if len(decoded) in (32, 48):
                return "ed25519"
        except Exception:
            pass
        return "hmac"

    @staticmethod
    def _load_ed25519_key(secret: str):
        if not _ED25519_AVAILABLE:
            raise RuntimeError(
                "Ed25519 signing requires the 'cryptography' package. "
                "Install it with: pip install cryptography"
            )
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key,
            load_der_private_key,
        )

        stripped = secret.strip()

        # Normalise literal \n sequences that appear when PEM keys are stored
        # in .env files (python-dotenv doesn't expand escape sequences).
        if "\\n" in stripped:
            stripped = stripped.replace("\\n", "\n")

        # ── PEM format: -----BEGIN PRIVATE KEY----- ──────────────────────
        if "-----BEGIN" in stripped:
            try:
                # Normalize line endings and re-encode to bytes
                pem_bytes = stripped.encode("utf-8")
                return load_pem_private_key(pem_bytes, password=None)
            except Exception as e:
                raise ValueError(f"Failed to load PEM Ed25519 private key: {e}") from e

        # ── Base64-encoded DER or raw bytes ───────────────────────────────
        padded = stripped + "=" * (-len(stripped) % 4)
        try:
            raw = base64.b64decode(padded)
        except Exception as e:
            raise ValueError(f"api_secret is not valid base64 or PEM: {e}") from e

        if len(raw) == 32:
            return Ed25519PrivateKey.from_private_bytes(raw)

        if len(raw) == 48:
            try:
                return load_der_private_key(raw, password=None)
            except Exception as e:
                raise ValueError(f"Failed to load DER Ed25519 key: {e}") from e

        raise ValueError(
            f"Unrecognised Ed25519 key length: {len(raw)} bytes "
            f"(expected PEM, or base64 of 32/48 bytes)."
        )

    def _sign(self, params: dict) -> str:
        """HMAC-SHA256 signature — used for REST requests."""
        query = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _sign_ws(self, params: dict) -> str:
        """
        Sign for WebSocket API per official Binance docs:
        - Sort params alphabetically
        - Build 'key=value&...' string
        - Ed25519: standard base64  |  HMAC: hex
        Note: _ws_api_call handles signing inline; this method kept for external use.
        """
        payload = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        if self._key_type == "ed25519" and self._ed25519_key:
            raw_sig = self._ed25519_key.sign(payload.encode("ASCII"))
            return base64.b64encode(raw_sig).decode("ASCII")
        return hmac.new(
            self.api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _sign_rest(self, params: dict) -> str:
        """
        Sign for REST API (fapi).
        Ed25519: standard base64 WITH padding  (required by Binance REST API)
        HMAC:    hex digest
        """
        query = urlencode(params)
        if self._key_type == "ed25519" and self._ed25519_key:
            raw_sig = self._ed25519_key.sign(query.encode("utf-8"))
            # REST API requires standard base64 with padding
            return base64.b64encode(raw_sig).decode("ascii")
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    async def start(self):
        if self._started:
            return
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers={
                "X-MBX-APIKEY": self.api_key,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=aiohttp.ClientTimeout(total=10, connect=3),
        )
        # Start WebSocket API for fast order placement (live only)
        await self._start_ws_api()
        ws_api_status = "WS API active" if self._ws_api_url else "WS API unavailable (REST fallback)"
        self._started = True
        logger.info("BinanceClient started [%s/%s] | %s", self.mode.value, self.market, ws_api_status)

    async def stop(self):
        if not self._started:
            return
        # Stop all stream connections
        for ws in self._streams.values():
            await ws.stop()
        if self._ws_api:
            await self._ws_api.stop()
        if self._user_stream_ws:
            await self._user_stream_ws.stop()
        if self._session:
            await self._session.close()
        self._started = False
        logger.info("BinanceClient stopped")

    # ------------------------------------------------------------------
    # REST helpers
    # ------------------------------------------------------------------
    def _signed_params(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window
        params["signature"] = self._sign_rest(params)
        return params

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        signed: bool = False,
        weight: int = 1,
    ) -> Any:
        await self._request_limiter.acquire(weight)
        url = self._rest_base + endpoint
        if params is None:
            params = {}
        if signed:
            params = self._signed_params(params)

        try:
            async with self._session.request(
                method,
                url,
                params=params if method == "GET" else None,
                data=urlencode(params) if method != "GET" else None,
            ) as resp:
                data = await resp.json()
                if resp.status not in (200, 201):
                    raise BinanceAPIError(resp.status, data)
                return data
        except aiohttp.ClientError as e:
            raise BinanceConnectionError(str(e)) from e

    async def get(self, endpoint: str, params: dict = None, signed: bool = False, weight: int = 1):
        return await self._request("GET", endpoint, params, signed, weight)

    async def post(self, endpoint: str, params: dict = None, signed: bool = True, weight: int = 1):
        return await self._request("POST", endpoint, params, signed, weight)

    async def delete(self, endpoint: str, params: dict = None, signed: bool = True):
        return await self._request("DELETE", endpoint, params, signed)

    async def put(self, endpoint: str, params: dict = None, signed: bool = True):
        return await self._request("PUT", endpoint, params, signed)

    # ------------------------------------------------------------------
    # WebSocket API (ultra-fast order placement)
    # ------------------------------------------------------------------
    async def _start_ws_api(self):
        if not self._ws_api_url:
            logger.info("WS API not available in %s mode — orders will use REST", self.mode.value)
            return
        self._ws_api = WSConnection(self._ws_api_url, name="ws-api", on_error=self._on_error)
        self._ws_api.add_handler(self._on_ws_api_message)
        await self._ws_api.start()

    async def _on_ws_api_message(self, data: dict):
        req_id = data.get("id")
        if req_id and req_id in self._ws_api_pending:
            fut = self._ws_api_pending.pop(req_id)
            if not fut.done():
                fut.set_result(data)

    async def _ws_api_call(self, method: str, params: dict, sign: bool = True) -> dict:
        """
        Send a signed request over the WebSocket API.

        Per official Binance Futures WS API docs:
          https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info

        Signing rules:
          1. Add apiKey, timestamp (INT), and optional recvWindow to params
          2. Sort ALL params (including apiKey) alphabetically by key
          3. Build signing payload: 'key=value&key=value...' (sorted)
          4. Sign payload with Ed25519 → standard base64
          5. Add signature to params
          6. Send JSON with all params including signature

        Type rules per docs:
          - INT params (timestamp, recvWindow, orderId) → JSON integers
          - DECIMAL params (price, quantity) → JSON strings per REST docs,
            but WS example shows numbers; we use strings to be safe
          - ENUM params (side, type, etc.) → JSON strings
        """
        if sign:
            p = dict(params)
            p["apiKey"]     = self.api_key
            p["timestamp"]  = int(time.time() * 1000)   # INT — must be integer
            p["recvWindow"] = self.recv_window            # INT — optional but recommended

            # Sort all params alphabetically — Binance signing requirement
            payload = "&".join(f"{k}={v}" for k, v in sorted(p.items()))

            if self._key_type == "ed25519" and self._ed25519_key:
                raw_sig = self._ed25519_key.sign(payload.encode("ASCII"))
                p["signature"] = base64.b64encode(raw_sig).decode("ASCII")
            else:
                # Per Binance docs: WS API (ws-fapi) ONLY supports Ed25519 for
                # session auth. HMAC will always return -1022. The bot will fall
                # back to REST for every order — functional but slower.
                logger.warning(
                    "WS API: HMAC key in use. Binance ws-fapi requires Ed25519. "
                    "Create an Ed25519 API key for native WS order placement."
                )
                p["signature"] = hmac.new(
                    self.api_secret.encode("utf-8"),
                    payload.encode("utf-8"),
                    hashlib.sha256
                ).hexdigest()

            logger.debug("WS API | method=%s payload=%s", method, payload)
        else:
            p = params

        req_id = str(uuid.uuid4())
        payload_json = {"id": req_id, "method": method, "params": p}

        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        self._ws_api_pending[req_id] = fut

        # Respect Binance 10 incoming messages/sec limit
        await self._ws_msg_limiter.acquire()

        try:
            await self._ws_api.send(payload_json)
        except Exception as conn_err:
            self._ws_api_pending.pop(req_id, None)
            raise BinanceConnectionError(f"WS send failed: {conn_err}") from conn_err

        try:
            result = await asyncio.wait_for(fut, timeout=10)
        except asyncio.TimeoutError:
            self._ws_api_pending.pop(req_id, None)
            raise BinanceTimeoutError(f"WS API timeout for method={method}")

        if result.get("status") not in (200, 201, None):
            err = result.get("error", {})
            if isinstance(err, dict) and err.get("code") == -1022:
                logger.error(
                    "WS API -1022: signature invalid (key_type=%s). "
                    "Verify the Ed25519 private key matches the public key "
                    "uploaded to Binance API Management.",
                    self._key_type
                )
            raise BinanceAPIError(result.get("status"), err)

        return result.get("result", result)

    async def diagnose_ws_api(self) -> dict:
        """
        Step-by-step WS API diagnostic per official Binance docs.
        Tests: ping → time → session.logon (Ed25519 auth) → session.status
        """
        results = {}

        # Step 1: unauthenticated ping
        try:
            await self._ws_api_call("ping", {}, sign=False)
            results["ping"] = "OK"
            logger.info("WS diagnose | ping: OK")
        except Exception as e:
            results["ping"] = f"FAIL: {e}"
            logger.error("WS diagnose | ping FAILED: %s", e)
            return results  # no point continuing

        # Step 2: unauthenticated server time
        try:
            r = await self._ws_api_call("time", {}, sign=False)
            results["time"] = f"OK serverTime={r.get('serverTime')}"
            logger.info("WS diagnose | time: %s", results["time"])
        except Exception as e:
            results["time"] = f"FAIL: {e}"
            logger.error("WS diagnose | time FAILED: %s", e)

        # Step 3: session.logon — the official Ed25519 auth test
        # Per docs: only Ed25519 keys are supported for session auth
        try:
            await self._ws_api_call("session.logon", {})
            results["session.logon"] = "OK — Ed25519 signing is correct!"
            logger.info("WS diagnose | session.logon: OK")
        except BinanceAPIError as e:
            code = e.data.get("code") if isinstance(e.data, dict) else None
            msg  = e.data.get("msg",  "") if isinstance(e.data, dict) else str(e)
            if code == -1022:
                results["session.logon"] = f"FAIL -1022: signature invalid — check Ed25519 key matches API key"
            elif code == -2015:
                results["session.logon"] = f"FAIL -2015: API key auth failed — check key ID and permissions"
            else:
                results["session.logon"] = f"FAIL code={code}: {msg}"
            logger.error("WS diagnose | session.logon FAILED: %s", results["session.logon"])

        # Step 4: session.status (no signing needed after logon)
        try:
            r = await self._ws_api_call("session.status", {}, sign=False)
            api_key_val = r.get("apiKey", "null")
            results["session.status"] = f"OK apiKey={str(api_key_val)[:16]}..."
            logger.info("WS diagnose | session.status: %s", results["session.status"])
        except Exception as e:
            results["session.status"] = f"FAIL: {e}"

        return results

    # ------------------------------------------------------------------
    # ORDER MANAGEMENT
    # ------------------------------------------------------------------

    # --- Place order via WebSocket API (FASTEST) ---
    async def place_order_ws(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        position_side: PositionSide = PositionSide.BOTH,
        reduce_only: bool = False,
        close_position: bool = False,
        callback_rate: Optional[float] = None,  # for TRAILING_STOP_MARKET
        working_type: str = "CONTRACT_PRICE",   # or MARK_PRICE
        new_client_order_id: Optional[str] = None,
    ) -> dict:
        await self._order_limiter.acquire()

        method = (
            "order.place" if self.market == "spot"
            else "order.place"   # same method name for futures WS API
        )

        params: dict = {
            "symbol":   symbol.upper(),
            "side":     side.value,
            "type":     order_type.value,
        }

        if quantity is not None:
            params["quantity"] = str(quantity)
        if price is not None:
            params["price"] = str(price)
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)
        if order_type == OrderType.LIMIT:
            params["timeInForce"] = time_in_force.value
        if self.market == "futures":
            params["positionSide"] = position_side.value
            if reduce_only and not close_position:
                params["reduceOnly"] = "true"
            if close_position:
                params["closePosition"] = "true"
            params["workingType"] = working_type
        if callback_rate is not None:
            params["callbackRate"] = str(callback_rate)
        if new_client_order_id:
            params["newClientOrderId"] = new_client_order_id

        t0 = time.perf_counter()

        # DEMO mode has ws_api_url=None — fall back to REST transparently.
        if not self._ws_api_url:
            logger.debug("place_order_ws: WS API unavailable (demo mode), using REST")
            return await self.place_order_rest(
                symbol=symbol, side=side, order_type=order_type,
                quantity=quantity, price=price, stop_price=stop_price,
                time_in_force=time_in_force, position_side=position_side,
                reduce_only=reduce_only, close_position=close_position,
                new_client_order_id=new_client_order_id,
            )

        try:
            result = await self._ws_api_call(method, params)
        except (BinanceAPIError, BinanceConnectionError) as e:
            # -1022 = WS API signing failed (HMAC key used on ws-fapi which needs Ed25519)
            # BinanceConnectionError = WS disconnected mid-send
            # In both cases fall back to REST so the order still executes.
            code = e.data.get("code") if isinstance(getattr(e, "data", None), dict) else None
            if isinstance(e, BinanceConnectionError) or code == -1022:
                logger.warning(
                    "place_order_ws: WS failed (%s) — falling back to REST. "
                    "For native WS speed on live, create an Ed25519 API key and "
                    "pass key_type='ed25519' with the base64 private key as api_secret.",
                    type(e).__name__
                )
                result = await self.place_order_rest(
                    symbol=symbol, side=side, order_type=order_type,
                    quantity=quantity, price=price, stop_price=stop_price,
                    time_in_force=time_in_force, position_side=position_side,
                    reduce_only=reduce_only, close_position=close_position,
                    new_client_order_id=new_client_order_id,
                )
            else:
                raise

        latency_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "place_order_ws | %s %s %s qty=%s | orderId=%s | %.1fms",
            side.value, order_type.value, symbol,
            quantity, result.get("orderId"), latency_ms
        )
        return result

    # --- Place order via REST (fallback) ---
    async def place_order_rest(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        position_side: PositionSide = PositionSide.BOTH,
        reduce_only: bool = False,
        close_position: bool = False,
        new_client_order_id: Optional[str] = None,
        new_order_resp_type: str = "RESULT",  # ACK = faster, no fill info
    ) -> dict:
        await self._order_limiter.acquire()

        endpoint = (
            "/fapi/v1/order" if self.market == "futures"
            else "/api/v3/order"
        )
        params: dict = {
            "symbol":       symbol.upper(),
            "side":         side.value,
            "type":         order_type.value,
            "newOrderRespType": new_order_resp_type,
        }
        if quantity is not None:
            params["quantity"] = quantity
        if price is not None:
            params["price"] = price
        if stop_price is not None:
            params["stopPrice"] = stop_price
        if order_type == OrderType.LIMIT:
            params["timeInForce"] = time_in_force.value
        if self.market == "futures":
            params["positionSide"] = position_side.value
            if reduce_only and not close_position:
                params["reduceOnly"] = "true"
            if close_position:
                params["closePosition"] = "true"
        if new_client_order_id:
            params["newClientOrderId"] = new_client_order_id

        t0 = time.perf_counter()
        result = await self.post(endpoint, params)
        latency_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "place_order_rest | %s %s %s qty=%s | orderId=%s | %.1fms",
            side.value, order_type.value, symbol,
            quantity, result.get("orderId"), latency_ms
        )
        return result

    async def cancel_order(self, symbol: str, order_id: int = None, orig_client_order_id: str = None) -> dict:
        """Cancel order via REST. Uses DELETE /fapi/v1/order."""
        endpoint = "/fapi/v1/order" if self.market == "futures" else "/api/v3/order"
        params = {"symbol": symbol.upper()}
        if order_id:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self.delete(endpoint, params)

    async def cancel_order_ws(self, symbol: str, order_id: int = None, orig_client_order_id: str = None) -> dict:
        """
        Cancel order via WS API (faster than REST).
        Method: order.cancel
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Cancel-Order
        Either order_id or orig_client_order_id must be provided.
        """
        params = {"symbol": symbol.upper()}
        if order_id:
            params["orderId"] = order_id          # LONG — integer
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._ws_api_call("order.cancel", params)

    async def query_order_ws(self, symbol: str, order_id: int = None, orig_client_order_id: str = None) -> dict:
        """
        Query order status via WS API.
        Method: order.status
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Query-Order
        Either order_id or orig_client_order_id must be provided.
        Response includes: avgPrice, executedQty, status, orderId, etc.
        """
        params = {"symbol": symbol.upper()}
        if order_id:
            params["orderId"] = order_id          # LONG — integer
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._ws_api_call("order.status", params)

    async def cancel_all_orders(self, symbol: str) -> dict:
        endpoint = "/fapi/v1/allOpenOrders" if self.market == "futures" else "/api/v3/openOrders"
        return await self.delete(endpoint, {"symbol": symbol.upper()})

    async def get_order(self, symbol: str, order_id: int) -> dict:
        endpoint = "/fapi/v1/order" if self.market == "futures" else "/api/v3/order"
        return await self.get(endpoint, {"symbol": symbol.upper(), "orderId": order_id}, signed=True)

    async def get_open_orders(self, symbol: Optional[str] = None) -> list:
        endpoint = "/fapi/v1/openOrders" if self.market == "futures" else "/api/v3/openOrders"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self.get(endpoint, params, signed=True)

    # --- Batch orders (futures only) ---
    async def place_batch_orders(self, orders: List[dict]) -> list:
        """Place up to 5 orders in a single REST call (futures only)."""
        assert self.market == "futures", "Batch orders only available for futures"
        await self._order_limiter.acquire(len(orders))
        params = {"batchOrders": json.dumps(orders)}
        return await self.post("/fapi/v1/batchOrders", params)

    # ------------------------------------------------------------------
    # POSITION & ACCOUNT
    # ------------------------------------------------------------------
    async def get_positions(self, symbol: Optional[str] = None) -> list:
        """
        REST Position Information V3.
        GET /fapi/v3/positionRisk
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-Information-V3
        Returns only symbols with open positions or open orders.
        Response fields: symbol, positionSide, positionAmt, entryPrice, markPrice,
                         unRealizedProfit, liquidationPrice, etc.
        """
        assert self.market == "futures", "Positions only available for futures"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self.get("/fapi/v3/positionRisk", params, signed=True, weight=5)

    async def get_positions_ws(self, symbol: Optional[str] = None) -> list:
        """
        WS Position Information V2.
        Method: v2/account.position
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/websocket-api/Position-Info-V2
        Returns only symbols with open positions or open orders.
        Note: use with user data stream ACCOUNT_UPDATE for real-time updates.
        """
        assert self.market == "futures", "Positions only available for futures"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._ws_api_call("v2/account.position", params)

    async def get_account(self) -> dict:
        endpoint = "/fapi/v2/account" if self.market == "futures" else "/api/v3/account"
        return await self.get(endpoint, signed=True, weight=5)

    async def get_balance(self) -> list:
        """
        REST Futures Account Balance V3.
        GET /fapi/v3/balance
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Futures-Account-Balance-V3
        Response fields: asset, balance, crossWalletBalance, availableBalance,
                         crossUnPnl, maxWithdrawAmount, marginAvailable, updateTime.
        """
        endpoint = "/fapi/v3/balance" if self.market == "futures" else "/api/v3/account"
        return await self.get(endpoint, signed=True, weight=5)

    async def get_balance_ws(self) -> list:
        """
        WS Futures Account Balance V2.
        Method: v2/account.balance
        Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/websocket-api
        Same response shape as REST balance.
        """
        assert self.market == "futures"
        return await self._ws_api_call("v2/account.balance", {})

    async def change_leverage(self, symbol: str, leverage: int) -> dict:
        assert self.market == "futures"
        return await self.post("/fapi/v1/leverage", {"symbol": symbol.upper(), "leverage": leverage})

    async def get_order(self, symbol: str, order_id: int) -> dict:
        """Fetch a single order by ID — used to retrieve fill price after MARKET placement."""
        endpoint = "/fapi/v1/order" if self.market == "futures" else "/api/v3/order"
        return await self.get(endpoint, {"symbol": symbol.upper(), "orderId": order_id}, signed=True)

    async def change_margin_type(self, symbol: str, margin_type: MarginType) -> dict:
        assert self.market == "futures"
        return await self.post("/fapi/v1/marginType", {
            "symbol": symbol.upper(),
            "marginType": margin_type.value
        })

    async def set_position_mode(self, dual_side: bool) -> dict:
        """True = hedge mode (LONG/SHORT), False = one-way mode (BOTH)."""
        assert self.market == "futures"
        return await self.post("/fapi/v1/positionSide/dual", {
            "dualSidePosition": "true" if dual_side else "false"
        })

    async def get_position_mode(self) -> bool:
        """
        Query current position mode.
        Returns True  = hedge mode  (orders require positionSide=LONG or SHORT)
                False = one-way mode (orders require positionSide=BOTH)
        """
        assert self.market == "futures"
        data = await self.get("/fapi/v1/positionSide/dual", signed=True)
        return bool(data.get("dualSidePosition", False))

    # ------------------------------------------------------------------
    # MARKET DATA (REST)
    # ------------------------------------------------------------------
    async def get_ticker(self, symbol: str) -> dict:
        endpoint = "/fapi/v1/ticker/bookTicker" if self.market == "futures" else "/api/v3/ticker/bookTicker"
        return await self.get(endpoint, {"symbol": symbol.upper()})

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list:
        endpoint = "/fapi/v1/klines" if self.market == "futures" else "/api/v3/klines"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        return await self.get(endpoint, params, weight=2)

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        endpoint = "/fapi/v1/depth" if self.market == "futures" else "/api/v3/depth"
        return await self.get(endpoint, {"symbol": symbol.upper(), "limit": limit})

    async def get_mark_price(self, symbol: str) -> dict:
        assert self.market == "futures"
        return await self.get("/fapi/v1/premiumIndex", {"symbol": symbol.upper()})

    async def get_funding_rate(self, symbol: str) -> dict:
        assert self.market == "futures"
        return await self.get("/fapi/v1/fundingRate", {"symbol": symbol.upper(), "limit": 1})

    async def get_exchange_info(self) -> dict:
        endpoint = "/fapi/v1/exchangeInfo" if self.market == "futures" else "/api/v3/exchangeInfo"
        return await self.get(endpoint, weight=40)

    async def get_server_time(self) -> int:
        endpoint = "/fapi/v1/time" if self.market == "futures" else "/api/v3/time"
        data = await self.get(endpoint)
        return data["serverTime"]

    async def ping(self) -> bool:
        endpoint = "/fapi/v1/ping" if self.market == "futures" else "/api/v3/ping"
        await self.get(endpoint)
        return True

    # ------------------------------------------------------------------
    # WEBSOCKET STREAMS
    # ------------------------------------------------------------------
    async def subscribe_kline(self, symbol: str, interval: str, callback: Callable):
        stream = f"{symbol.lower()}@kline_{interval}"
        await self._subscribe_stream(stream, callback)

    async def subscribe_agg_trade(self, symbol: str, callback: Callable):
        stream = f"{symbol.lower()}@aggTrade"
        await self._subscribe_stream(stream, callback)

    async def subscribe_book_ticker(self, symbol: str, callback: Callable):
        stream = f"{symbol.lower()}@bookTicker"
        await self._subscribe_stream(stream, callback)

    async def subscribe_mark_price(self, symbol: str, callback: Callable, frequency: str = "1s"):
        assert self.market == "futures"
        stream = f"{symbol.lower()}@markPrice@{frequency}"
        await self._subscribe_stream(stream, callback)

    async def subscribe_depth(self, symbol: str, callback: Callable, levels: int = 5, speed: str = "100ms"):
        stream = f"{symbol.lower()}@depth{levels}@{speed}"
        await self._subscribe_stream(stream, callback)

    async def subscribe_user_data(self, callback: Callable):
        """Subscribe to account/order/position updates via user data stream."""
        self._user_data_handlers.append(callback)
        if self._listen_key is None:
            await self._start_user_data_stream()

    async def _subscribe_stream(self, stream_name: str, callback: Callable):
        if stream_name not in self._streams:
            url = f"{self._ws_stream_url}/ws/{stream_name}"
            ws = WSConnection(url, name=stream_name, on_error=self._on_error)
            self._stream_handlers[stream_name] = []
            ws.add_handler(self._make_stream_handler(stream_name))
            self._streams[stream_name] = ws
            await ws.start()
        self._stream_handlers[stream_name].append(callback)

    def _make_stream_handler(self, stream_name: str):
        async def handler(data: dict):
            for cb in self._stream_handlers.get(stream_name, []):
                asyncio.create_task(cb(data))
        return handler

    # ------------------------------------------------------------------
    # USER DATA STREAM
    # ------------------------------------------------------------------
    async def _start_user_data_stream(self):
        endpoint = "/fapi/v1/listenKey" if self.market == "futures" else "/api/v3/userDataStream"
        data = await self.post(endpoint, signed=False)
        self._listen_key = data["listenKey"]
        logger.info("User data stream listenKey obtained")

        url = f"{self._ws_stream_url}/ws/{self._listen_key}"
        self._user_stream_ws = WSConnection(url, name="user-data-stream", on_error=self._on_error)
        self._user_stream_ws.add_handler(self._on_user_data)
        await self._user_stream_ws.start()

        # Auto-refresh listen key every 30 minutes
        asyncio.create_task(self._keep_listen_key_alive())

    async def _on_user_data(self, data: dict):
        for cb in self._user_data_handlers:
            asyncio.create_task(cb(data))

    async def _keep_listen_key_alive(self):
        endpoint = "/fapi/v1/listenKey" if self.market == "futures" else "/api/v3/userDataStream"
        while self._started and self._listen_key:
            await asyncio.sleep(LISTEN_KEY_TTL_SEC)
            try:
                await self.put(endpoint, {"listenKey": self._listen_key}, signed=False)
                logger.info("Listen key refreshed")
            except Exception as e:
                logger.warning("Failed to refresh listen key: %s", e)

    # ------------------------------------------------------------------
    # TP/SL helpers (futures)
    # ------------------------------------------------------------------
    async def set_tp_sl(
        self,
        symbol: str,
        side: OrderSide,          # opposite of position side
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        position_side: PositionSide = PositionSide.BOTH,
        working_type: str = "MARK_PRICE",
    ) -> list:
        """Place TP and/or SL orders in one call (uses batch if both provided)."""
        assert self.market == "futures"
        orders = []
        if tp_price:
            orders.append({
                "symbol": symbol.upper(),
                "side": side.value,
                "type": OrderType.TAKE_PROFIT_MARKET.value,
                "stopPrice": str(tp_price),
                "closePosition": "true",
                "positionSide": position_side.value,
                "workingType": working_type,
                "timeInForce": "GTE_GTC",
            })
        if sl_price:
            orders.append({
                "symbol": symbol.upper(),
                "side": side.value,
                "type": OrderType.STOP_MARKET.value,
                "stopPrice": str(sl_price),
                "closePosition": "true",
                "positionSide": position_side.value,
                "workingType": working_type,
                "timeInForce": "GTE_GTC",
            })
        if len(orders) == 1:
            return [await self.place_order_rest(
                symbol=symbol,
                side=side,
                order_type=OrderType(orders[0]["type"]),
                stop_price=float(orders[0]["stopPrice"]),
                position_side=position_side,
                close_position=True,
            )]
        elif len(orders) == 2:
            return await self.place_batch_orders(orders)
        return []

    async def close_position(self, symbol: str, position_side: PositionSide = PositionSide.BOTH) -> dict:
        """Market close an entire position."""
        assert self.market == "futures"
        positions = await self.get_positions(symbol)
        for pos in positions:
            if pos.get("positionSide") == position_side.value:
                amt = float(pos.get("positionAmt", 0))
                if amt == 0:
                    continue
                side = OrderSide.SELL if amt > 0 else OrderSide.BUY
                return await self.place_order_ws(
                    symbol=symbol,
                    side=side,
                    order_type=OrderType.MARKET,
                    quantity=abs(amt),
                    position_side=position_side,
                    reduce_only=True,
                )
        raise ValueError(f"No open position found for {symbol} {position_side.value}")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class BinanceAPIError(Exception):
    def __init__(self, status: int, data: Any):
        self.status = status
        self.data = data
        code = data.get("code") if isinstance(data, dict) else None
        msg  = data.get("msg")  if isinstance(data, dict) else str(data)
        super().__init__(f"Binance API error {status} (code={code}): {msg}")


class BinanceConnectionError(Exception):
    pass


class BinanceTimeoutError(Exception):
    pass


# ---------------------------------------------------------------------------
# Quick demo / smoke test
# ---------------------------------------------------------------------------
async def _demo():
    """
    Minimal smoke test using testnet credentials.
    Replace API_KEY / API_SECRET with your Binance Futures Testnet keys from:
    https://testnet.binancefuture.com
    """
    API_KEY    = "YOUR_TESTNET_API_KEY"
    API_SECRET = "YOUR_TESTNET_API_SECRET"

    async with BinanceClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        mode=BinanceMode.DEMO,
        market="futures",
    ) as client:

        # 1. Ping
        ok = await client.ping()
        print(f"Ping: {ok}")

        # 2. Server time
        st = await client.get_server_time()
        print(f"Server time: {st}")

        # 3. Book ticker
        ticker = await client.get_ticker("BTCUSDT")
        print(f"BTC bid={ticker['bidPrice']} ask={ticker['askPrice']}")

        # 4. Subscribe to book ticker stream
        async def on_book(data):
            print(f"[stream] BTC ask={data.get('a')} bid={data.get('b')}")

        await client.subscribe_book_ticker("BTCUSDT", on_book)

        # 5. Subscribe to 1m klines
        async def on_kline(data):
            k = data.get("k", {})
            if k.get("x"):  # candle closed
                print(f"[kline] close={k['c']} volume={k['v']}")

        await client.subscribe_kline("BTCUSDT", "1m", on_kline)

        # 6. Place a test market order (very small qty on testnet)
        # Uncomment to test:
        # result = await client.place_order_ws(
        #     symbol="BTCUSDT",
        #     side=OrderSide.BUY,
        #     order_type=OrderType.MARKET,
        #     quantity=0.001,
        # )
        # print(f"Order result: {result}")

        # Keep alive for a bit to receive stream data
        print("Listening to streams for 15s...")
        await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(_demo())