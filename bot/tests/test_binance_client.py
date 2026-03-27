"""
test_binance_client.py
======================
Test suite for binance_client.py.

Three layers of tests:
  1. Unit tests        — no network, mock everything (always run)
  2. Testnet tests     — hit Binance Futures Testnet (needs BINANCE_TESTNET_KEY)
  3. Live tests        — hit real Binance Futures    (needs BINANCE_LIVE_KEY)

──────────────────────────────────────────────────────────
.env file reference  (place in same directory as this file)
──────────────────────────────────────────────────────────

# ── Testnet credentials ──────────────────────────────────
BINANCE_TESTNET_KEY=your_testnet_api_key
BINANCE_TESTNET_SECRET=your_testnet_api_secret

# ── Live credentials ─────────────────────────────────────
# For WS API order placement, use an Ed25519 key.
# Generate keys with: https://github.com/binance/asymmetric-key-generator
# Upload the PUBLIC key to Binance API Management.
# Paste the PRIVATE key (full PEM block) as BINANCE_LIVE_SECRET.
# Use quotes in .env so the multi-line PEM is read correctly, OR
# store it on one line by replacing newlines with \n:
#   BINANCE_LIVE_SECRET=-----BEGIN PRIVATE KEY-----\nMC4CAQ...\n-----END PRIVATE KEY-----
BINANCE_LIVE_KEY=your_live_api_key_id
BINANCE_LIVE_SECRET=-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEI...\n-----END PRIVATE KEY-----
BINANCE_LIVE_KEY_TYPE=auto    # auto | hmac | ed25519

# ── Shared order config ───────────────────────────────────
# Symbol to trade (must be a USDT-M perpetual futures pair)
BINANCE_TEST_SYMBOL=BTCUSDT

# Minimum quantity for test orders (keep as small as exchange allows)
# BTC  min notional ≈ $100  → 0.004 BTC @ ~$25k  |  use 0.004
# ETH  min notional ≈ $100  → 0.04  ETH @ ~$2.5k |  use 0.04
# SOL  min notional ≈ $10   → 0.1   SOL @ ~$100  |  use 0.1
BINANCE_TEST_QUANTITY=0.004

# Leverage to set before running order tests (1 = no leverage, safest)
BINANCE_TEST_LEVERAGE=1

# ── Live safety gate ──────────────────────────────────────
# Set to true ONLY when you intentionally want to place real orders.
# If false/missing, live tests run READ-ONLY (balance, positions, market data).
BINANCE_LIVE_ENABLE_ORDERS=false

──────────────────────────────────────────────────────────
How to run:
    python test_binance_client.py

Testnet keys:  https://testnet.binancefuture.com
──────────────────────────────────────────────────────────
"""

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time
import traceback
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

# ── .env loader (no external dependency) ──────────────────────────────────
def _load_env(path: str = ".env"):
    env_file = Path(path)
    if not env_file.exists():
        # also try same directory as this script
        env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key   = key.strip()
        value = value.strip().strip('"').strip("'")
        # Unescape \n so PEM keys stored on a single line work correctly
        value = value.replace("\\n", "\n")
        os.environ.setdefault(key, value)   # don't override already-set env vars

_load_env()

# ══════════════════════════════════════════════════════════════════════════════
# TEST CONFIGURATION  (set these in your .env file)
# ══════════════════════════════════════════════════════════════════════════════

# ── Test mode ─────────────────────────────────────────────────────────────────
# Controls which integration suites run after the unit tests.
#   demo  → testnet only
#   live  → live only
#   both  → testnet first, then live (default)
#   none  → skip all integration tests (unit tests only)
TEST_MODE = os.getenv("BINANCE_TEST_MODE", "both").lower()

# ── Test mode ────────────────────────────────────────────────────────
# Controls which integration suites run (unit tests always run).
#   both  → testnet + live  (default)
#   demo  → testnet only
#   live  → live only
#   none  → unit tests only
# BINANCE_TEST_MODE=both

# ── Testnet credentials ───────────────────────────────────────────────────────
TESTNET_KEY    = os.getenv("BINANCE_TESTNET_KEY", "")
TESTNET_SECRET = os.getenv("BINANCE_TESTNET_SECRET", "")

# ── Live credentials ──────────────────────────────────────────────────────────
# For WS API order placement on live, you need an Ed25519 API key (not HMAC).
# To create one: Binance → API Management → Create API → choose Ed25519
# Binance will give you a base64-encoded private key — paste it as BINANCE_LIVE_SECRET.
# Set BINANCE_LIVE_KEY_TYPE=ed25519 to enable WS API signing.
# Without it, orders still work via REST fallback (~200ms) — just not WS-fast.
LIVE_KEY      = os.getenv("BINANCE_LIVE_KEY", "")
LIVE_SECRET   = os.getenv("BINANCE_LIVE_SECRET", "")
LIVE_KEY_TYPE = os.getenv("BINANCE_LIVE_KEY_TYPE", "auto")  # "auto" | "hmac" | "ed25519"

# ── Order parameters (shared between testnet & live) ──────────────────────────
# Symbol: must be a valid USDT-M perpetual futures pair
TEST_SYMBOL   = os.getenv("BINANCE_TEST_SYMBOL",   "BTCUSDT")

# Quantity: keep as small as the exchange minimum allows
#   BTCUSDT  min notional ~$100  →  0.004 BTC  (@ ~$25k)
#   ETHUSDT  min notional ~$100  →  0.04  ETH  (@ ~$2.5k)
#   SOLUSDT  min notional ~$10   →  0.1   SOL  (@ ~$100)
TEST_QTY      = float(os.getenv("BINANCE_TEST_QUANTITY", "0.004"))

# Leverage: 1 = no leverage (safest for testing)
TEST_LEVERAGE = int(os.getenv("BINANCE_TEST_LEVERAGE", "1"))

# ── Live safety gate ──────────────────────────────────────────────────────────
# Must explicitly set BINANCE_LIVE_ENABLE_ORDERS=true in .env to place real orders.
# If false/missing, live suite runs read-only (market data + account queries only).
LIVE_ORDERS_ENABLED = os.getenv("BINANCE_LIVE_ENABLE_ORDERS", "false").lower() == "true"

from urllib.parse import parse_qs, urlparse

# ── import the client ──────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from binance_client import (
    BinanceAPIError,
    BinanceClient,
    BinanceConnectionError,
    BinanceMode,
    BinanceTimeoutError,
    MarginType,
    OrderSide,
    OrderType,
    PositionSide,
    TimeInForce,
    TokenBucket,
)

# ── colours ────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
DIM    = "\033[2m"

# ── test result tracker ────────────────────────────────────────────────────
RESULTS: list[dict] = []

def _record(name: str, passed: bool, detail: str = "", duration_ms: float = 0):
    RESULTS.append({"name": name, "passed": passed, "detail": detail, "ms": duration_ms})
    icon   = f"{GREEN}✓{RESET}" if passed else f"{RED}✗{RESET}"
    timing = f"{DIM}({duration_ms:.0f}ms){RESET}" if duration_ms else ""
    detail_str = f"  {DIM}{detail}{RESET}" if detail else ""
    print(f"  {icon}  {name} {timing}{detail_str}")

async def run_test(name: str, coro):
    t0 = time.perf_counter()
    try:
        await coro
        _record(name, True, duration_ms=(time.perf_counter()-t0)*1000)
        return True
    except AssertionError as e:
        _record(name, False, f"AssertionError: {e}", (time.perf_counter()-t0)*1000)
        return False
    except Exception as e:
        _record(name, False, f"{type(e).__name__}: {e}", (time.perf_counter()-t0)*1000)
        traceback.print_exc()
        return False

def section(title: str):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")

def summary():
    total   = len(RESULTS)
    passed  = sum(1 for r in RESULTS if r["passed"])
    failed  = total - passed
    avg_ms  = sum(r["ms"] for r in RESULTS) / total if total else 0

    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  RESULTS: {GREEN}{passed} passed{RESET}  {RED}{failed} failed{RESET}  {DIM}/ {total} total{RESET}")
    print(f"  {DIM}avg latency: {avg_ms:.0f}ms{RESET}")
    if failed:
        print(f"\n{RED}  Failed tests:{RESET}")
        for r in RESULTS:
            if not r["passed"]:
                print(f"  {RED}✗  {r['name']}{RESET}")
                print(f"     {DIM}{r['detail']}{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}\n")
    return failed == 0


# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS  (no network)
# ══════════════════════════════════════════════════════════════════════════════

# ── Token Bucket ──────────────────────────────────────────────────────────
async def test_token_bucket_basic():
    tb = TokenBucket(capacity=10, refill_rate=10)
    await tb.acquire(5)
    assert tb._tokens == pytest_approx(5, abs=0.1), f"expected ~5 tokens, got {tb._tokens}"

def pytest_approx(value, abs=0.1):
    """tiny approx helper (no pytest dependency)"""
    class Approx:
        def __eq__(self, other):
            return abs_val >= abs(other - value)
        def __repr__(self): return f"~{value}±{abs}"
    abs_val = abs
    return value  # simplified: just return value for direct comparison

async def test_token_bucket_refills():
    tb = TokenBucket(capacity=10, refill_rate=100)
    await tb.acquire(10)                    # drain completely
    assert tb._tokens <= 0
    await asyncio.sleep(0.1)               # wait 100ms → should refill ~10 tokens
    await tb.acquire(1)                    # should succeed without long sleep
    assert True                             # if we got here without hanging, pass

async def test_token_bucket_capacity_cap():
    tb = TokenBucket(capacity=5, refill_rate=1000)
    await asyncio.sleep(0.01)
    await tb.acquire(1)
    # tokens should never exceed capacity
    assert tb._tokens <= 5

async def test_token_bucket_concurrent():
    """Multiple concurrent acquires should not exceed capacity."""
    tb = TokenBucket(capacity=10, refill_rate=1000)
    results = await asyncio.gather(*[tb.acquire(1) for _ in range(10)])
    assert len(results) == 10

# ── Signing ───────────────────────────────────────────────────────────────
async def test_signing_correctness():
    """HMAC-SHA256 signature should be deterministic and correct."""
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "testsecret"
    client._key_type = "hmac"
    client._ed25519_key = None
    params = {"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001", "timestamp": "1700000000000"}
    sig = client._sign(params)
    from urllib.parse import urlencode
    query = urlencode(params)
    expected = hmac.new(b"testsecret", query.encode(), hashlib.sha256).hexdigest()
    assert sig == expected, f"Signature mismatch: {sig} != {expected}"

async def test_signed_params_has_timestamp():
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "testsecret"
    client._key_type = "hmac"
    client._ed25519_key = None
    client.recv_window = 5000
    params = {"symbol": "BTCUSDT"}
    result = client._signed_params(params)
    assert "timestamp" in result
    assert "signature" in result
    assert "recvWindow" in result
    assert result["recvWindow"] == 5000
    # With HMAC, signature should be a 64-char hex string
    assert len(result["signature"]) == 64

async def test_signing_different_params_differ():
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "testsecret"
    client._key_type = "hmac"
    client._ed25519_key = None
    sig1 = client._sign({"a": "1"})
    sig2 = client._sign({"a": "2"})
    assert sig1 != sig2

async def test_key_type_detection_hmac():
    """64-char hex string (Binance HMAC secret) → detected as hmac."""
    secret = "a3f2b1c4d5e6" * 5 + "a3f2"   # 64 hex chars
    result = BinanceClient._detect_key_type(secret)
    assert result == "hmac", f"Expected hmac, got {result}"

async def test_key_type_detection_hmac_alphanumeric():
    """Alphanumeric HMAC secrets that happen to be ~44 chars must NOT be detected as ed25519."""
    import base64
    # Simulate an HMAC secret that is 44 chars but does NOT decode to 32 bytes
    secret = "abcdefghijklmnopqrstuvwxyz1234567890ABCDEF12"  # 44 chars, not valid b64 of 32 bytes
    result = BinanceClient._detect_key_type(secret)
    # Should be hmac (decoded bytes won't be 32)
    assert result == "hmac", f"Expected hmac, got {result}"

async def test_key_type_detection_ed25519():
    """Valid base64-encoded 32-byte value → detected as ed25519."""
    import base64
    raw = bytes(range(32))   # 32 distinct bytes
    secret = base64.b64encode(raw).decode()   # exactly 44 chars
    result = BinanceClient._detect_key_type(secret)
    assert result == "ed25519", f"Expected ed25519, got {result}"

async def test_key_type_detection_ed25519_pkcs8():
    """Base64-encoded 48-byte PKCS#8 DER key → detected as ed25519."""
    import base64
    raw = bytes(range(48))
    secret = base64.b64encode(raw).decode()
    result = BinanceClient._detect_key_type(secret)
    assert result == "ed25519", f"Expected ed25519, got {result}"

async def test_key_type_detection_ed25519_pem():
    """PEM private key header → detected as ed25519."""
    pem = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIABC\n-----END PRIVATE KEY-----"
    result = BinanceClient._detect_key_type(pem)
    assert result == "ed25519", f"Expected ed25519, got {result}"

async def test_load_ed25519_pem():
    """PEM PKCS#8 Ed25519 key loads successfully."""
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as _K
        from cryptography.hazmat.primitives.serialization import (
            Encoding, PrivateFormat, NoEncryption
        )
    except ImportError:
        return  # skip if cryptography not installed
    import base64
    # Generate a real key and export as PEM, then load it back
    real_key = _K.generate()
    pem_bytes = real_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    pem_str = pem_bytes.decode("utf-8")
    loaded = BinanceClient._load_ed25519_key(pem_str)
    # Verify it signs correctly
    sig = loaded.sign(b"test")
    assert len(sig) == 64

async def test_sign_ws_hmac_matches_sign():
    """_sign_ws with HMAC key should produce same result as manually signing sorted params."""
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "testsecret"
    client._key_type = "hmac"
    client._ed25519_key = None
    params = {"symbol": "BTCUSDT", "quantity": "0.001"}
    # _sign_ws sorts params alphabetically — manually replicate that
    payload = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    expected = hmac.new(b"testsecret", payload.encode(), hashlib.sha256).hexdigest()
    assert client._sign_ws(params) == expected

async def test_sign_ws_ed25519_is_urlsafe_base64():
    """_sign_ws with Ed25519 produces standard base64 (not urlsafe) per Binance docs."""
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as _K
    except ImportError:
        return
    import base64 as _b64
    key = _K.generate()
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "unused"
    client._key_type = "ed25519"
    client._ed25519_key = key
    params = {"symbol": "BTCUSDT", "timestamp": "1700000000000"}
    sig = client._sign_ws(params)
    # Must be valid standard base64 and decode to 64-byte Ed25519 signature
    decoded = _b64.b64decode(sig)
    assert len(decoded) == 64, f"Expected 64-byte Ed25519 signature, got {len(decoded)}"

async def test_sign_rest_ed25519_is_standard_base64():
    """_sign_rest with Ed25519 key produces standard base64 with padding (for REST API)."""
    import base64
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as _K
    except ImportError:
        return
    key = _K.generate()
    client = BinanceClient.__new__(BinanceClient)
    client.api_secret = "unused"
    client._key_type = "ed25519"
    client._ed25519_key = key
    params = {"symbol": "BTCUSDT", "timestamp": "1700000000000"}
    sig = client._sign_rest(params)
    # Standard base64: ends with '=' padding, 88 chars for 64-byte Ed25519 sig
    assert sig.endswith("="), "REST Ed25519 sig must have '=' padding"
    decoded = base64.b64decode(sig)
    assert len(decoded) == 64

# ── URL construction ──────────────────────────────────────────────────────
async def test_futures_live_urls():
    from binance_client import _URLS
    urls = _URLS["futures"][BinanceMode.LIVE]
    assert "fapi.binance.com" in urls["rest"]
    assert "ws-fapi.binance.com" in urls["ws_api"]
    assert "fstream.binance.com" in urls["ws_stream"]

async def test_futures_demo_urls():
    from binance_client import _URLS
    urls = _URLS["futures"][BinanceMode.DEMO]
    assert urls["rest"] == "https://demo-fapi.binance.com"
    assert urls["ws_api"] is None, "Demo testnet does not support WS API — should be None"
    assert urls["ws_stream"] == "wss://fstream.binancefuture.com"

async def test_spot_live_urls():
    from binance_client import _URLS
    urls = _URLS["spot"][BinanceMode.LIVE]
    assert "api.binance.com" in urls["rest"]
    assert "ws-api.binance.com" in urls["ws_api"]

async def test_client_sets_correct_urls():
    c = BinanceClient("k", "s", mode=BinanceMode.DEMO, market="futures")
    assert c._rest_base == "https://demo-fapi.binance.com"
    c2 = BinanceClient("k", "s", mode=BinanceMode.LIVE, market="futures")
    assert c2._rest_base == "https://fapi.binance.com"

# ── Enum coverage ─────────────────────────────────────────────────────────
async def test_enums():
    assert OrderSide.BUY.value  == "BUY"
    assert OrderSide.SELL.value == "SELL"
    assert OrderType.MARKET.value == "MARKET"
    assert OrderType.LIMIT.value  == "LIMIT"
    assert OrderType.STOP_MARKET.value == "STOP_MARKET"
    assert OrderType.TAKE_PROFIT_MARKET.value == "TAKE_PROFIT_MARKET"
    assert OrderType.TRAILING_STOP_MARKET.value == "TRAILING_STOP_MARKET"
    assert TimeInForce.GTC.value == "GTC"
    assert TimeInForce.IOC.value == "IOC"
    assert PositionSide.BOTH.value  == "BOTH"
    assert PositionSide.LONG.value  == "LONG"
    assert PositionSide.SHORT.value == "SHORT"
    assert MarginType.ISOLATED.value == "ISOLATED"
    assert MarginType.CROSSED.value  == "CROSS"

async def test_order_side_is_str():
    """OrderSide.value should equal the raw string (Python 3.11+ __str__ caveat)."""
    assert OrderSide.BUY == "BUY"         # __eq__ still works via str inheritance
    assert OrderSide.SELL.value == "SELL"  # use .value — f-string gives "OrderSide.SELL" in 3.11+

# ── REST _request mocking ─────────────────────────────────────────────────
async def test_rest_get_calls_correct_url():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"serverTime": 1700000000000})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_response)
    client._session = mock_session
    client._request_limiter = TokenBucket(1200, 20)

    result = await client.get("/fapi/v1/time")
    assert result["serverTime"] == 1700000000000
    mock_session.request.assert_called_once()
    call_args = mock_session.request.call_args
    assert call_args[0][0] == "GET"
    assert "/fapi/v1/time" in call_args[0][1]

async def test_rest_raises_api_error_on_non_200():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    mock_response = MagicMock()
    mock_response.status = 400
    mock_response.json = AsyncMock(return_value={"code": -1121, "msg": "Invalid symbol"})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_response)
    client._session = mock_session
    client._request_limiter = TokenBucket(1200, 20)

    try:
        await client.get("/fapi/v1/order", {"symbol": "INVALID"})
        assert False, "Should have raised BinanceAPIError"
    except BinanceAPIError as e:
        assert e.status == 400
        assert "Invalid symbol" in str(e)

async def test_rest_signed_adds_signature():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    captured = {}

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"orderId": 123})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    def capture_request(method, url, params=None, data=None):
        captured["params"] = params
        captured["data"] = data
        return mock_response

    mock_session = MagicMock()
    mock_session.request = capture_request
    client._session = mock_session
    client._request_limiter = TokenBucket(1200, 20)

    await client.get("/fapi/v1/order", {"symbol": "BTCUSDT"}, signed=True)
    # For GET, params are passed as query params
    assert captured["params"] is not None
    assert "signature" in captured["params"]
    assert "timestamp" in captured["params"]

# ── WS API message dispatch ───────────────────────────────────────────────
async def test_ws_api_pending_future_resolved():
    """Incoming WS message should resolve the matching pending future."""
    client = BinanceClient.__new__(BinanceClient)
    client._ws_api_pending = {}

    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    req_id = "test-req-123"
    client._ws_api_pending[req_id] = fut

    await client._on_ws_api_message({"id": req_id, "status": 200, "result": {"orderId": 999}})
    assert fut.done()
    result = fut.result()
    assert result["result"]["orderId"] == 999

async def test_ws_api_unknown_id_ignored():
    """Message with unknown id should not crash."""
    client = BinanceClient.__new__(BinanceClient)
    client._ws_api_pending = {}
    # Should not raise
    await client._on_ws_api_message({"id": "unknown-id", "status": 200})
    assert True

async def test_ws_api_no_id_ignored():
    """Message without id (e.g. stream event) should not crash."""
    client = BinanceClient.__new__(BinanceClient)
    client._ws_api_pending = {}
    await client._on_ws_api_message({"e": "trade", "s": "BTCUSDT"})
    assert True

# ── order param building ──────────────────────────────────────────────────
# LIVE mode ensures _ws_api_url is set. We also set _ws_api to a non-None
# sentinel so the check passes on both old (`or self._ws_api is None`) and
# new (`if not self._ws_api_url`) versions of the client.
async def test_place_order_ws_builds_correct_params():
    client = BinanceClient("key", "secret", mode=BinanceMode.LIVE, market="futures")
    client._order_limiter = TokenBucket(300, 30)
    client._ws_api = object()   # sentinel: satisfies any version of the fallback check

    captured_params = {}

    async def mock_ws_api_call(method, params, sign=True):
        captured_params.update(params)
        return {"orderId": 42, "status": "FILLED"}

    client._ws_api_call = mock_ws_api_call

    await client.place_order_ws(
        symbol="btcusdt",  # lowercase — should be uppercased
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=0.001,
        position_side=PositionSide.BOTH,
    )
    assert captured_params["symbol"] == "BTCUSDT"
    assert captured_params["side"] == "BUY"
    assert captured_params["type"] == "MARKET"
    assert captured_params["quantity"] == "0.001"
    assert captured_params["positionSide"] == "BOTH"

async def test_place_order_ws_limit_includes_tif():
    client = BinanceClient("key", "secret", mode=BinanceMode.LIVE, market="futures")
    client._order_limiter = TokenBucket(300, 30)
    client._ws_api = object()
    captured = {}
    async def mock_call(method, params, sign=True):
        captured.update(params)
        return {"orderId": 1}
    client._ws_api_call = mock_call

    await client.place_order_ws(
        symbol="ETHUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=0.1,
        price=3000.0,
        time_in_force=TimeInForce.IOC,
    )
    assert captured["type"] == "LIMIT"
    assert captured["price"] == "3000.0"
    assert captured["timeInForce"] == "IOC"

async def test_place_order_ws_reduce_only():
    client = BinanceClient("key", "secret", mode=BinanceMode.LIVE, market="futures")
    client._order_limiter = TokenBucket(300, 30)
    client._ws_api = object()
    captured = {}
    async def mock_call(method, params, sign=True):
        captured.update(params)
        return {"orderId": 2}
    client._ws_api_call = mock_call

    await client.place_order_ws(
        symbol="BTCUSDT", side=OrderSide.SELL,
        order_type=OrderType.MARKET, quantity=0.001, reduce_only=True,
    )
    assert captured.get("reduceOnly") == "true"
    assert "closePosition" not in captured

async def test_place_order_ws_close_position():
    client = BinanceClient("key", "secret", mode=BinanceMode.LIVE, market="futures")
    client._order_limiter = TokenBucket(300, 30)
    client._ws_api = object()
    captured = {}
    async def mock_call(method, params, sign=True):
        captured.update(params)
        return {"orderId": 3}
    client._ws_api_call = mock_call

    await client.place_order_ws(
        symbol="BTCUSDT", side=OrderSide.SELL,
        order_type=OrderType.MARKET, close_position=True,
    )
    assert captured.get("closePosition") == "true"
    assert "reduceOnly" not in captured

async def test_place_order_rest_builds_params():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._order_limiter = TokenBucket(300, 30)
    captured = {}

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"orderId": 55, "status": "NEW"})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    def capture(method, url, params=None, data=None):
        if data:
            from urllib.parse import parse_qs
            captured.update({k: v[0] for k, v in parse_qs(data).items()})
        return mock_response

    mock_session = MagicMock()
    mock_session.request = capture
    client._session = mock_session
    client._request_limiter = TokenBucket(1200, 20)

    await client.place_order_rest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=0.001,
    )
    assert captured.get("symbol") == "BTCUSDT"
    assert captured.get("side") == "BUY"
    assert captured.get("type") == "MARKET"
    assert "signature" in captured

# ── user data stream ──────────────────────────────────────────────────────
async def test_user_data_handler_dispatched():
    client = BinanceClient.__new__(BinanceClient)
    client._user_data_handlers = []
    received = []

    async def handler(data):
        received.append(data)

    client._user_data_handlers.append(handler)

    event = {"e": "ORDER_TRADE_UPDATE", "o": {"s": "BTCUSDT", "S": "BUY", "z": "0.001"}}
    await client._on_user_data(event)
    await asyncio.sleep(0.01)  # allow task to run
    assert len(received) == 1
    assert received[0]["e"] == "ORDER_TRADE_UPDATE"

async def test_user_data_multiple_handlers():
    client = BinanceClient.__new__(BinanceClient)
    client._user_data_handlers = []
    counts = [0, 0]

    async def h1(data): counts[0] += 1
    async def h2(data): counts[1] += 1

    client._user_data_handlers = [h1, h2]
    await client._on_user_data({"e": "ACCOUNT_UPDATE"})
    await asyncio.sleep(0.01)
    assert counts[0] == 1
    assert counts[1] == 1

# ── stream handler ────────────────────────────────────────────────────────
async def test_stream_handler_dispatches():
    client = BinanceClient.__new__(BinanceClient)
    client._stream_handlers = {}
    received = []

    async def cb(data):
        received.append(data)

    stream = "btcusdt@bookTicker"
    client._stream_handlers[stream] = [cb]
    handler = client._make_stream_handler(stream)
    await handler({"b": "50000", "a": "50001"})
    await asyncio.sleep(0.01)
    assert len(received) == 1
    assert received[0]["b"] == "50000"

# ── get_position_mode ─────────────────────────────────────────────────────
async def test_get_position_mode_hedge():
    """dualSidePosition=true  → returns True (hedge mode)."""
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._request_limiter = TokenBucket(1200, 20)

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"dualSidePosition": True})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_response)
    client._session = mock_session

    result = await client.get_position_mode()
    assert result is True

async def test_get_position_mode_one_way():
    """dualSidePosition=false → returns False (one-way mode)."""
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._request_limiter = TokenBucket(1200, 20)

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"dualSidePosition": False})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_response)
    client._session = mock_session

    result = await client.get_position_mode()
    assert result is False

# ── Exception types ───────────────────────────────────────────────────────
async def test_api_error_message():
    err = BinanceAPIError(400, {"code": -1121, "msg": "Invalid symbol."})
    assert "-1121" in str(err)
    assert "Invalid symbol" in str(err)
    assert err.status == 400

async def test_api_error_non_dict():
    err = BinanceAPIError(500, "Internal error")
    assert "500" in str(err)

async def test_market_validation():
    try:
        BinanceClient("k", "s", market="options")
        assert False, "Should raise AssertionError"
    except AssertionError:
        pass

# ── close_position ────────────────────────────────────────────────────────
async def test_close_position_long():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._order_limiter = TokenBucket(300, 30)

    placed = {}
    async def mock_positions(symbol):
        return [{"positionSide": "BOTH", "positionAmt": "0.05"}]
    async def mock_place_ws(symbol, side, order_type, quantity, position_side, reduce_only):
        placed.update({"side": side, "qty": quantity})
        return {"orderId": 99}

    client.get_positions = mock_positions
    client.place_order_ws = mock_place_ws

    await client.close_position("BTCUSDT")
    assert placed["side"] == OrderSide.SELL
    assert placed["qty"] == 0.05

async def test_close_position_short():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._order_limiter = TokenBucket(300, 30)

    placed = {}
    async def mock_positions(symbol):
        return [{"positionSide": "BOTH", "positionAmt": "-0.03"}]
    async def mock_place_ws(symbol, side, order_type, quantity, position_side, reduce_only):
        placed.update({"side": side, "qty": quantity})
        return {"orderId": 100}

    client.get_positions = mock_positions
    client.place_order_ws = mock_place_ws

    await client.close_position("BTCUSDT")
    assert placed["side"] == OrderSide.BUY
    assert placed["qty"] == 0.03

async def test_close_position_no_open_raises():
    client = BinanceClient("key", "secret", mode=BinanceMode.DEMO, market="futures")
    client._order_limiter = TokenBucket(300, 30)

    async def mock_positions(symbol):
        return [{"positionSide": "BOTH", "positionAmt": "0"}]

    client.get_positions = mock_positions
    try:
        await client.close_position("BTCUSDT")
        assert False, "Should raise ValueError"
    except ValueError as e:
        assert "No open position" in str(e)


# ══════════════════════════════════════════════════════════════════════════════
# SHARED TEST SUITES  (used for both testnet and live)
# ══════════════════════════════════════════════════════════════════════════════

async def run_market_data_tests(client: "BinanceClient", symbol: str):
    """Connectivity and market data tests — safe, read-only."""

    async def t_ping():
        ok = await client.ping()
        assert ok is True

    async def t_server_time():
        st = await client.get_server_time()
        assert isinstance(st, int)
        diff_ms = abs(st - int(time.time() * 1000))
        assert diff_ms < 5000, f"Server time drift too large: {diff_ms}ms"

    async def t_exchange_info():
        info = await client.get_exchange_info()
        assert "symbols" in info
        assert len(info["symbols"]) > 0

    async def t_book_ticker():
        ticker = await client.get_ticker(symbol)
        assert "bidPrice" in ticker
        assert "askPrice" in ticker
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
        assert ask >= bid > 0

    async def t_order_book():
        book = await client.get_order_book(symbol, limit=5)
        assert "bids" in book and "asks" in book
        assert len(book["bids"]) == 5
        assert len(book["asks"]) == 5

    async def t_klines():
        klines = await client.get_klines(symbol, "1m", limit=10)
        assert isinstance(klines, list)
        assert len(klines) == 10
        assert len(klines[0]) >= 6

    async def t_mark_price():
        mp = await client.get_mark_price(symbol)
        assert "markPrice" in mp
        assert float(mp["markPrice"]) > 0

    async def t_funding_rate():
        fr = await client.get_funding_rate(symbol)
        assert isinstance(fr, list)
        assert len(fr) > 0

    for name, coro in [
        ("Ping",                         t_ping()),
        ("Server time (drift < 5s)",     t_server_time()),
        ("Exchange info",                t_exchange_info()),
        (f"Book ticker {symbol}",        t_book_ticker()),
        ("Order book depth=5",           t_order_book()),
        ("Klines 1m limit=10",           t_klines()),
        ("Mark price",                   t_mark_price()),
        ("Funding rate",                 t_funding_rate()),
    ]:
        await run_test(name, coro)


async def run_account_tests(client: "BinanceClient", symbol: str):
    """Account and position queries — read-only."""

    async def t_balance():
        bal = await client.get_balance()
        assert isinstance(bal, list)
        usdt = next((b for b in bal if b.get("asset") == "USDT"), None)
        assert usdt is not None, "No USDT balance found"
        bal_val = float(usdt["balance"])
        assert bal_val >= 0
        print(f"\n     {DIM}→ USDT balance: {bal_val:.2f}{RESET}")

    async def t_positions():
        positions = await client.get_positions(symbol)
        assert isinstance(positions, list)
        # /fapi/v3/positionRisk only returns symbols with an active position
        # or open orders — empty list is valid when flat
        if positions:
            pos = positions[0]
            assert "positionAmt" in pos
            assert "entryPrice" in pos
            amt = float(pos["positionAmt"])
            print(f"\n     {DIM}→ {symbol} positionAmt={amt}{RESET}")
        else:
            print(f"\n     {DIM}→ {symbol} no open position (empty response is correct per v3 docs){RESET}")

    async def t_account():
        acc = await client.get_account()
        assert "totalWalletBalance" in acc or "assets" in acc

    async def t_open_orders():
        orders = await client.get_open_orders(symbol)
        assert isinstance(orders, list)
        print(f"\n     {DIM}→ open orders: {len(orders)}{RESET}")

    for name, coro in [
        ("Get balance (USDT present)",   t_balance()),
        (f"Get positions {symbol}",      t_positions()),
        ("Get account",                  t_account()),
        (f"Get open orders {symbol}",    t_open_orders()),
    ]:
        await run_test(name, coro)


async def run_order_tests(client: "BinanceClient", symbol: str, qty: float, leverage: int):
    """Order placement, cancellation, and latency benchmark."""

    # ── detect position mode first ─────────────────────────────────────────
    # -4061 = "Order's position side does not match user's setting"
    # Hedge mode  → positionSide must be LONG or SHORT (never BOTH)
    # One-way mode → positionSide must be BOTH (never LONG/SHORT)
    hedge_mode = await client.get_position_mode()
    buy_side   = PositionSide.LONG  if hedge_mode else PositionSide.BOTH
    sell_side  = PositionSide.SHORT if hedge_mode else PositionSide.BOTH
    mode_label = "hedge (LONG/SHORT)" if hedge_mode else "one-way (BOTH)"
    print(f"\n     {DIM}→ position mode: {mode_label}{RESET}")

    # ── set leverage before placing any orders ─────────────────────────────
    async def t_set_leverage():
        result = await client.change_leverage(symbol, leverage=leverage)
        assert result.get("leverage") == leverage or "leverage" in result
        print(f"\n     {DIM}→ leverage set to {leverage}x{RESET}")

    await run_test(f"Set leverage {leverage}x", t_set_leverage())

    # ── order lifecycle ────────────────────────────────────────────────────
    section("    Order Lifecycle")

    placed_order_id = None

    async def t_market_buy_ws():
        nonlocal placed_order_id
        t0 = time.perf_counter()
        result = await client.place_order_ws(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=qty,
            position_side=buy_side,
        )
        latency_ms = (time.perf_counter() - t0) * 1000
        assert "orderId" in result, f"No orderId in response: {result}"
        placed_order_id = result["orderId"]
        assert latency_ms < 5000, f"Order took too long: {latency_ms:.0f}ms"
        print(f"\n     {DIM}→ orderId={placed_order_id}  latency={latency_ms:.0f}ms{RESET}")

    async def t_market_sell_rest():
        # In hedge mode close the LONG we just opened; in one-way mode reduce_only
        if hedge_mode:
            result = await client.place_order_rest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=qty,
                position_side=buy_side,   # close the LONG side
            )
        else:
            result = await client.place_order_rest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=qty,
                position_side=PositionSide.BOTH,
                reduce_only=True,
            )
        assert "orderId" in result
        print(f"\n     {DIM}→ orderId={result['orderId']}{RESET}")

    async def t_limit_order_and_cancel():
        ticker = await client.get_ticker(symbol)
        current_price = float(ticker["bidPrice"])
        limit_price = round(current_price * 0.80, 1)   # 20% below — won't fill

        result = await client.place_order_ws(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=qty,
            price=limit_price,
            time_in_force=TimeInForce.GTC,
            position_side=buy_side,
        )
        assert "orderId" in result
        oid = result["orderId"]
        print(f"\n     {DIM}→ limit id={oid}  price={limit_price}{RESET}")

        cancel = await client.cancel_order(symbol, order_id=oid)
        assert cancel.get("status") in ("CANCELED", "CANCELLED", "NEW")

    async def t_cancel_all():
        ticker = await client.get_ticker(symbol)
        p = round(float(ticker["bidPrice"]) * 0.79, 1)

        for _ in range(2):
            await client.place_order_ws(
                symbol=symbol, side=OrderSide.BUY,
                order_type=OrderType.LIMIT, quantity=qty,
                price=p, time_in_force=TimeInForce.GTC,
                position_side=buy_side,
            )
        result = await client.cancel_all_orders(symbol)
        assert result is not None

    for name, coro in [
        ("Market BUY  (WS API)",         t_market_buy_ws()),
        ("Market SELL close (REST)",      t_market_sell_rest()),
        ("Limit order → cancel single",  t_limit_order_and_cancel()),
        ("Limit x2   → cancel all",      t_cancel_all()),
    ]:
        await run_test(name, coro)

    # ── latency benchmark ──────────────────────────────────────────────────
    section("    Latency Benchmark  (WS vs REST, 5 orders each)")

    async def t_ws_latency():
        ticker = await client.get_ticker(symbol)
        p = round(float(ticker["bidPrice"]) * 0.78, 1)
        latencies = []
        for _ in range(5):
            t0 = time.perf_counter()
            r = await client.place_order_ws(
                symbol=symbol, side=OrderSide.BUY,
                order_type=OrderType.LIMIT, quantity=qty,
                price=p, time_in_force=TimeInForce.GTC,
                position_side=buy_side,
            )
            latencies.append((time.perf_counter() - t0) * 1000)
            await client.cancel_order(symbol, order_id=r["orderId"])
            await asyncio.sleep(0.1)
        avg, mn, mx = sum(latencies)/len(latencies), min(latencies), max(latencies)
        print(f"\n     {DIM}WS   avg={avg:.0f}ms  min={mn:.0f}ms  max={mx:.0f}ms{RESET}")
        assert avg < 5000

    async def t_rest_latency():
        ticker = await client.get_ticker(symbol)
        p = round(float(ticker["bidPrice"]) * 0.77, 1)
        latencies = []
        for _ in range(5):
            t0 = time.perf_counter()
            r = await client.place_order_rest(
                symbol=symbol, side=OrderSide.BUY,
                order_type=OrderType.LIMIT, quantity=qty,
                price=p, time_in_force=TimeInForce.GTC,
                position_side=buy_side,
            )
            latencies.append((time.perf_counter() - t0) * 1000)
            await client.cancel_order(symbol, order_id=r["orderId"])
            await asyncio.sleep(0.1)
        avg, mn, mx = sum(latencies)/len(latencies), min(latencies), max(latencies)
        print(f"\n     {DIM}REST avg={avg:.0f}ms  min={mn:.0f}ms  max={mx:.0f}ms{RESET}")
        assert avg < 5000

    await run_test("WS API order latency  (5 orders)", t_ws_latency())
    await run_test("REST order latency    (5 orders)", t_rest_latency())


async def run_stream_tests(client: "BinanceClient", symbol: str):
    """WebSocket stream tests — listen for 3s each."""

    # Detect position mode for the user data stream order trigger
    hedge_mode = await client.get_position_mode()
    buy_side   = PositionSide.LONG if hedge_mode else PositionSide.BOTH

    async def t_book_ticker_stream():
        received = []
        async def cb(data): received.append(data)
        await client.subscribe_book_ticker(symbol, cb)
        await asyncio.sleep(3)
        assert len(received) > 0, "No book ticker messages received"
        msg = received[-1]
        assert "b" in msg and "a" in msg
        print(f"\n     {DIM}→ {len(received)} msgs  last bid={msg.get('b')}{RESET}")

    async def t_kline_stream():
        received = []
        async def cb(data): received.append(data)
        await client.subscribe_kline(symbol, "1m", cb)
        await asyncio.sleep(3)
        assert len(received) > 0
        assert "k" in received[-1]
        assert received[-1]["k"]["s"] == symbol

    async def t_mark_price_stream():
        received = []
        async def cb(data): received.append(data)
        await client.subscribe_mark_price(symbol, cb)
        await asyncio.sleep(3)
        assert len(received) > 0
        assert "p" in received[-1]

    async def t_depth_stream():
        received = []
        async def cb(data): received.append(data)
        await client.subscribe_depth(symbol, cb, levels=5)
        await asyncio.sleep(3)
        assert len(received) > 0
        assert "b" in received[-1] and "a" in received[-1]

    async def t_user_data_stream():
        received = []
        async def cb(data): received.append(data)
        await client.subscribe_user_data(cb)
        ticker = await client.get_ticker(symbol)
        p = round(float(ticker["bidPrice"]) * 0.76, 1)
        r = await client.place_order_ws(
            symbol=symbol, side=OrderSide.BUY,
            order_type=OrderType.LIMIT, quantity=TEST_QTY,
            price=p, time_in_force=TimeInForce.GTC,
            position_side=buy_side,
        )
        await asyncio.sleep(3)
        await client.cancel_order(symbol, order_id=r["orderId"])
        await asyncio.sleep(1)
        assert len(received) > 0, "No user data events received"
        print(f"\n     {DIM}→ events: {[m.get('e') for m in received]}{RESET}")

    for name, coro in [
        (f"bookTicker stream ({symbol})",      t_book_ticker_stream()),
        (f"kline 1m stream ({symbol})",        t_kline_stream()),
        (f"markPrice stream ({symbol})",       t_mark_price_stream()),
        (f"depth5 stream ({symbol})",          t_depth_stream()),
        ("User data stream (order event)",     t_user_data_stream()),
    ]:
        await run_test(name, coro)


async def run_futures_config_tests(client: "BinanceClient", symbol: str):
    """Leverage and margin type config."""

    async def t_set_margin_type():
        try:
            await client.change_margin_type(symbol, MarginType.ISOLATED)
        except BinanceAPIError as e:
            if "-4046" in str(e) or "No need" in str(e):
                return   # already isolated — that's fine
            raise

    await run_test("Set margin type ISOLATED", t_set_margin_type())


# ══════════════════════════════════════════════════════════════════════════════
# TESTNET INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

async def integration_tests(api_key: str, api_secret: str):
    section("INTEGRATION TESTS  (Binance Futures Testnet)")
    print(f"  {DIM}key:      {api_key[:8]}...{RESET}")
    print(f"  {DIM}symbol:   {TEST_SYMBOL}{RESET}")
    print(f"  {DIM}quantity: {TEST_QTY}{RESET}")
    print(f"  {DIM}leverage: {TEST_LEVERAGE}x{RESET}\n")

    async with BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        mode=BinanceMode.DEMO,
        market="futures",
    ) as client:

        section("  Connectivity & Market Data")
        await run_market_data_tests(client, TEST_SYMBOL)

        section("  Account & Position Queries")
        await run_account_tests(client, TEST_SYMBOL)

        section("  Order Lifecycle & Latency")
        await run_order_tests(client, TEST_SYMBOL, TEST_QTY, TEST_LEVERAGE)

        section("  WebSocket Streams  (3s each)")
        await run_stream_tests(client, TEST_SYMBOL)

        section("  Futures Config")
        await run_futures_config_tests(client, TEST_SYMBOL)


# ══════════════════════════════════════════════════════════════════════════════
# LIVE INTEGRATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

async def live_integration_tests(api_key: str, api_secret: str):
    section("LIVE TESTS  (Binance Futures — REAL MONEY)")
    print(f"  {DIM}key:           {api_key[:8]}...{RESET}")
    print(f"  {DIM}symbol:        {TEST_SYMBOL}{RESET}")
    print(f"  {DIM}quantity:      {TEST_QTY}{RESET}")
    print(f"  {DIM}leverage:      {TEST_LEVERAGE}x{RESET}")
    print(f"  {DIM}key_type:      {LIVE_KEY_TYPE}{RESET}")
    print(f"  {DIM}orders active: {LIVE_ORDERS_ENABLED}{RESET}\n")

    async with BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        mode=BinanceMode.LIVE,
        market="futures",
        key_type=LIVE_KEY_TYPE,
    ) as client:

        section("  Connectivity & Market Data")
        await run_market_data_tests(client, TEST_SYMBOL)

        # ── auth probe — catch -2015 before running signed endpoints ──────
        # -2015 = Invalid API-key, IP, or permissions
        # Most common causes:
        #   1. IP restriction on key — add your IP in Binance → API Management
        #   2. "Enable Futures" permission not checked on the key
        #   3. Wrong key/secret pasted in .env
        auth_ok = await _probe_auth(client)
        if not auth_ok:
            print(f"\n{RED}  ✗  Live API key auth failed (-2015). Skipping signed tests.{RESET}")
            print(f"  {YELLOW}  To fix:{RESET}")
            print(f"  {DIM}  1. Binance → API Management → Edit key → disable IP restriction{RESET}")
            print(f"  {DIM}     OR whitelist your IP: {RED}check the error output for your IP{RESET}")
            print(f"  {DIM}  2. Ensure 'Enable Futures' is checked on the key{RESET}")
            print(f"  {DIM}  3. Verify BINANCE_LIVE_KEY / BINANCE_LIVE_SECRET in .env are correct{RESET}")
            # Record skipped tests so they show in summary
            for name in [
                "Get balance (USDT present)", f"Get positions {TEST_SYMBOL}",
                "Get account", f"Get open orders {TEST_SYMBOL}",
            ]:
                _record(name, False, "Skipped — auth failed (-2015)")
            return

        section("  Account & Position Queries")
        await run_account_tests(client, TEST_SYMBOL)

        if LIVE_ORDERS_ENABLED:
            # ── WS API diagnostic — run before order tests ────────────────
            section("  WS API Diagnostic")
            print(f"  {DIM}Testing WS API connectivity and signing step by step...{RESET}\n")
            diag = await client.diagnose_ws_api()
            for step, result in diag.items():
                icon = f"{GREEN}✓{RESET}" if result.startswith("OK") else f"{RED}✗{RESET}"
                print(f"  {icon}  {step}: {DIM}{result}{RESET}")

            ws_signing_ok = diag.get("account.status", "").startswith("OK")
            if not ws_signing_ok:
                print(f"\n{YELLOW}  ⚠  WS API signing failed. Orders will use REST fallback.{RESET}")
                print(f"  {DIM}Check: Binance → API Management → Edit key → enable 'WebSocket API Trading'{RESET}\n")
            else:
                print(f"\n{GREEN}  ✓  WS API signing verified — native WS orders active!{RESET}\n")

            section("  Order Lifecycle & Latency  ⚠ REAL ORDERS")
            print(f"  {YELLOW}  Orders enabled — placing real trades on {TEST_SYMBOL} qty={TEST_QTY}{RESET}\n")
            await run_order_tests(client, TEST_SYMBOL, TEST_QTY, TEST_LEVERAGE)

            section("  WebSocket Streams  (3s each)")
            await run_stream_tests(client, TEST_SYMBOL)

            section("  Futures Config")
            await run_futures_config_tests(client, TEST_SYMBOL)
        else:
            print(f"\n{YELLOW}  ℹ  Order tests skipped — BINANCE_LIVE_ENABLE_ORDERS is not set to true.{RESET}")
            print(f"  {DIM}Add BINANCE_LIVE_ENABLE_ORDERS=true to .env to enable real order tests.{RESET}\n")

            section("  WebSocket Streams  (3s each)  — read-only")
            await run_stream_tests(client, TEST_SYMBOL)


async def _probe_auth(client: "BinanceClient") -> bool:
    """
    Quick signed request to verify the API key works.
    Returns True if auth is OK, False on -2015 / -2014 errors.
    Any other error is re-raised.
    """
    try:
        await client.get_balance()
        return True
    except BinanceAPIError as e:
        if e.status == 401 or (isinstance(e.data, dict) and e.data.get("code") in (-2015, -2014)):
            return False
        raise


# ══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  BINANCE CLIENT TEST SUITE{RESET}")
    print(f"  {DIM}binance_client.py{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")

    # ── unit tests ────────────────────────────────────────────────────────
    section("UNIT TESTS  (Token Bucket)")
    for name, coro in [
        ("Basic acquire deducts tokens",  test_token_bucket_basic()),
        ("Tokens refill over time",        test_token_bucket_refills()),
        ("Capacity cap respected",         test_token_bucket_capacity_cap()),
        ("Concurrent acquires (10x)",      test_token_bucket_concurrent()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (Signing)")
    for name, coro in [
        ("HMAC-SHA256 is correct",                    test_signing_correctness()),
        ("signed_params has timestamp",               test_signed_params_has_timestamp()),
        ("Different params → different sig",          test_signing_different_params_differ()),
        ("Key type detection → hmac (hex)",           test_key_type_detection_hmac()),
        ("Key type detection → hmac (alphanumeric)",  test_key_type_detection_hmac_alphanumeric()),
        ("Key type detection → ed25519 (raw 32b)",    test_key_type_detection_ed25519()),
        ("Key type detection → ed25519 (PKCS8 48b)",  test_key_type_detection_ed25519_pkcs8()),
        ("Key type detection → ed25519 (PEM)",        test_key_type_detection_ed25519_pem()),
        ("_sign_ws HMAC → sorted params hex",           test_sign_ws_hmac_matches_sign()),
        ("_sign_ws Ed25519 → standard b64 64 bytes",     test_sign_ws_ed25519_is_urlsafe_base64()),
        ("_sign_rest Ed25519 → standard b64 padded",    test_sign_rest_ed25519_is_standard_base64()),
        ("Load Ed25519 PEM key",                      test_load_ed25519_pem()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (URLs & Enums)")
    for name, coro in [
        ("Futures LIVE URLs",      test_futures_live_urls()),
        ("Futures DEMO URLs",      test_futures_demo_urls()),
        ("Spot LIVE URLs",         test_spot_live_urls()),
        ("Client sets URLs correctly", test_client_sets_correct_urls()),
        ("All enum values correct", test_enums()),
        ("OrderSide behaves as str", test_order_side_is_str()),
        ("Invalid market raises",  test_market_validation()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (REST — mocked)")
    for name, coro in [
        ("GET calls correct URL",           test_rest_get_calls_correct_url()),
        ("Non-200 raises BinanceAPIError",  test_rest_raises_api_error_on_non_200()),
        ("Signed GET includes signature",   test_rest_signed_adds_signature()),
        ("APIError message format",         test_api_error_message()),
        ("APIError non-dict body",          test_api_error_non_dict()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (WebSocket dispatch — mocked)")
    for name, coro in [
        ("Pending future resolved on match",  test_ws_api_pending_future_resolved()),
        ("Unknown req id safely ignored",     test_ws_api_unknown_id_ignored()),
        ("Message without id ignored",        test_ws_api_no_id_ignored()),
        ("User data dispatched to handler",   test_user_data_handler_dispatched()),
        ("Multiple user data handlers",       test_user_data_multiple_handlers()),
        ("Stream handler dispatches",         test_stream_handler_dispatches()),
        ("get_position_mode → hedge",         test_get_position_mode_hedge()),
        ("get_position_mode → one-way",       test_get_position_mode_one_way()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (Order param building — mocked)")
    for name, coro in [
        ("Market order params correct",       test_place_order_ws_builds_correct_params()),
        ("Limit order includes timeInForce",  test_place_order_ws_limit_includes_tif()),
        ("reduce_only=True param set",        test_place_order_ws_reduce_only()),
        ("close_position=True param set",     test_place_order_ws_close_position()),
        ("REST order builds signed params",   test_place_order_rest_builds_params()),
    ]:
        await run_test(name, coro)

    section("UNIT TESTS  (close_position logic — mocked)")
    for name, coro in [
        ("Long position → SELL order",         test_close_position_long()),
        ("Short position → BUY order",         test_close_position_short()),
        ("Zero position raises ValueError",    test_close_position_no_open_raises()),
    ]:
        await run_test(name, coro)

    # ── integration tests — controlled by BINANCE_TEST_MODE ──────────────
    run_demo = TEST_MODE in ("demo", "both")
    run_live = TEST_MODE in ("live", "both")

    if TEST_MODE == "none":
        print(f"\n{YELLOW}  ℹ  Integration tests skipped (BINANCE_TEST_MODE=none).{RESET}")

    if run_demo:
        if TESTNET_KEY and TESTNET_SECRET:
            await integration_tests(TESTNET_KEY, TESTNET_SECRET)
        else:
            print(f"\n{YELLOW}  ⚠  Testnet tests skipped — no testnet keys found.{RESET}")
            print(f"  {DIM}Add to .env:  BINANCE_TESTNET_KEY=...  BINANCE_TESTNET_SECRET=...{RESET}")
            print(f"  {DIM}Get keys at: https://testnet.binancefuture.com{RESET}")

    if run_live:
        if LIVE_KEY and LIVE_SECRET:
            await live_integration_tests(LIVE_KEY, LIVE_SECRET)
        else:
            print(f"\n{YELLOW}  ⚠  Live tests skipped — no live keys found.{RESET}")
            print(f"  {DIM}Add to .env:  BINANCE_LIVE_KEY=...  BINANCE_LIVE_SECRET=...{RESET}")
            print(f"  {DIM}              BINANCE_LIVE_ENABLE_ORDERS=false{RESET}")

    # ── summary ───────────────────────────────────────────────────────────
    ok = summary()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())