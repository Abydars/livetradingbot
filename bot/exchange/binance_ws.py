"""
exchange/binance_ws.py — Binance USDT-M Futures WebSocket streams.

Subscribes to:
  - {symbol}@aggTrade       — real-time trades (price, qty, buyer_maker)
  - {symbol}@depth20@100ms  — top-20 order book depth

Reconnects with exponential backoff on disconnect.
"""
import asyncio
import json
import logging
import time
from typing import Callable, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

_WS_BASE = "wss://fstream.binance.com/ws/"
_KEEPALIVE_INTERVAL = 20   # seconds between application-level pings
_BACKOFF_BASE = 2          # initial reconnect delay (seconds)
_BACKOFF_CAP = 30          # max reconnect delay (seconds)


class BinanceWebSocket:
    """
    Manages two combined Binance Futures WebSocket streams for a symbol.
    Calls `on_trade` and `on_depth` callbacks with parsed data.
    """

    def __init__(
        self,
        symbol: str,
        on_trade: Callable[[Dict], None],
        on_depth: Callable[[Dict], None],
    ) -> None:
        self.symbol = symbol.lower()
        self.on_trade = on_trade
        self.on_depth = on_depth

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_with_backoff())
        logger.info("BinanceWebSocket: started for %s", self.symbol)

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("BinanceWebSocket: stopped for %s", self.symbol)

    async def switch_symbol(self, new_symbol: str) -> None:
        """Stop current streams and restart for a new symbol."""
        logger.info(
            "BinanceWebSocket: switching %s → %s", self.symbol, new_symbol
        )
        self.symbol = new_symbol.lower()
        if self._ws is not None:
            await self._ws.close()
        # _run_with_backoff will reconnect automatically

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run_with_backoff(self) -> None:
        delay = _BACKOFF_BASE
        while self._running:
            try:
                await self._connect()
                delay = _BACKOFF_BASE  # reset on clean run
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning(
                    "BinanceWebSocket: disconnected (%s). Reconnecting in %ss…",
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, _BACKOFF_CAP)

    async def _connect(self) -> None:
        streams = (
            f"{self.symbol}@aggTrade"
            f"/{self.symbol}@depth20@100ms"
        )
        url = f"{_WS_BASE}{streams}"
        logger.info("BinanceWebSocket: connecting to %s", url)

        async with websockets.connect(
            url,
            ping_interval=None,   # we handle keepalives manually
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            logger.info("BinanceWebSocket: connected to %s", url)
            keepalive_task = asyncio.create_task(self._keepalive(ws))
            try:
                async for raw in ws:
                    if not self._running:
                        break
                    self._dispatch(raw)
            except ConnectionClosed as exc:
                logger.debug("BinanceWebSocket: connection closed: %s", exc)
            finally:
                keepalive_task.cancel()
                self._ws = None

    async def _keepalive(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send an application-level ping every _KEEPALIVE_INTERVAL seconds."""
        while True:
            await asyncio.sleep(_KEEPALIVE_INTERVAL)
            try:
                await ws.ping()
            except Exception:
                return

    def _dispatch(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        event = msg.get("e", "")

        if event == "aggTrade":
            self.on_trade(
                {
                    "price": float(msg["p"]),
                    "qty":   float(msg["q"]),
                    "buyer_maker": bool(msg["m"]),  # True → seller is aggressor
                    "time": int(msg["T"]),
                }
            )
        elif event == "depthUpdate":
            # depth20 stream uses depthUpdate events
            self.on_depth(
                {
                    "bids": [[float(p), float(q)] for p, q in msg.get("b", [])],
                    "asks": [[float(p), float(q)] for p, q in msg.get("a", [])],
                    "time": int(msg.get("T", time.time() * 1000)),
                }
            )
        # Ignore other event types (e.g. bookTicker)
