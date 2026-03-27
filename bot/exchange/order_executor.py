"""
exchange/order_executor.py — Unified order routing layer.

Translates TradingEngine's place_market_order() calls into:
  - Paper: simulated fill at current WS price (no Binance calls)
  - Demo/Live: BinanceClient.place_order_ws() with REST fallback

Handles positionSide correctly for both hedge and one-way account modes.
"""
import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from binance_client import BinanceClient
    from exchange.binance_rest import BinanceRestClient

logger = logging.getLogger(__name__)

_MODE_PREFIX = {
    "paper": "[PAPER] ",
    "demo":  "[DEMO] ",
    "live":  "[LIVE] ",
}


class OrderExecutor:
    """
    Routes market orders to paper simulation or real Binance exchange.

    Args:
        client: BinanceClient instance (None for paper mode)
        rest:   BinanceRestClient for qty rounding / calc_qty
        trading_mode: "paper" | "demo" | "live"
    """

    def __init__(self, client, rest, trading_mode: str) -> None:
        self._client = client
        self._rest = rest
        self.trading_mode = trading_mode
        self.paper_mode = trading_mode == "paper"
        self._hedge_mode = False  # set by init() after querying Binance

    @property
    def is_ready(self) -> bool:
        """True when orders can be placed (paper always ready; live/demo need a client)."""
        return self.paper_mode or self._client is not None

    async def init(self) -> None:
        """Fetch account position mode from Binance (demo/live only)."""
        if self._client and not self.paper_mode:
            try:
                self._hedge_mode = await self._client.get_position_mode()
                logger.info(
                    "OrderExecutor: position mode = %s",
                    "HEDGE" if self._hedge_mode else "ONE-WAY",
                )
            except Exception as exc:
                logger.warning("OrderExecutor: could not fetch position mode: %s", exc)
                self._hedge_mode = False

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage on Binance for this symbol before opening a position."""
        if self.paper_mode or not self._client:
            return
        try:
            await self._client.change_leverage(symbol, leverage)
            logger.info("OrderExecutor: leverage set to %dx for %s", leverage, symbol)
        except Exception as exc:
            logger.warning("OrderExecutor: could not set leverage: %s", exc)

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "BUY" | "SELL"
        qty: float,
        reduce_only: bool = False,
        current_price: float = 0.0,
    ) -> dict:
        """
        Place a market order using the appropriate execution path.

        Returns an order-result dict with at minimum:
            orderId, symbol, side, origQty, avgPrice, executedQty, status
        """
        prefix = _MODE_PREFIX.get(self.trading_mode, "")
        logger.info(
            "%splace_market_order %s %s qty=%.6f reduce_only=%s",
            prefix, symbol, side, qty, reduce_only,
        )

        if self.paper_mode:
            return {
                "orderId":    int(time.time() * 1000),
                "symbol":     symbol,
                "side":       side,
                "origQty":    str(qty),
                "avgPrice":   str(current_price),
                "executedQty": str(qty),
                "status":     "FILLED",
                "paper":      True,
            }

        if self._client is None:
            raise RuntimeError(
                f"Exchange client unavailable in {self.trading_mode} mode — "
                "check API keys and startup logs"
            )

        from binance_client import OrderSide, OrderType, PositionSide

        order_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL

        if self._hedge_mode:
            # Hedge mode: positionSide determines which side to open/close
            # Opening: BUY → LONG, SELL → SHORT
            # Closing (reduce_only): BUY closes SHORT, SELL closes LONG
            if not reduce_only:
                pos_side = PositionSide.LONG if side == "BUY" else PositionSide.SHORT
            else:
                pos_side = PositionSide.SHORT if side == "BUY" else PositionSide.LONG
            reduce = False  # reduceOnly is FORBIDDEN in hedge mode per docs
        else:
            # One-way mode: always BOTH, use reduceOnly for closes
            pos_side = PositionSide.BOTH
            reduce = reduce_only

        result = await self._client.place_order_ws(
            symbol=symbol,
            side=order_side,
            order_type=OrderType.MARKET,
            quantity=qty,
            position_side=pos_side,
            reduce_only=reduce,
        )

        # For MARKET orders avgPrice is the true fill price.
        # Fall back to current_price only if avgPrice is missing or zero.
        if not result.get("avgPrice") or float(result.get("avgPrice", 0)) == 0:
            result["avgPrice"] = str(current_price)

        return result

    def calc_qty(
        self,
        symbol: str,
        margin_usdt: float,
        leverage: int,
        price: float,
    ) -> float:
        """Delegate qty calculation to BinanceRestClient (handles step-size rounding)."""
        return self._rest.calc_qty(symbol, margin_usdt, leverage, price)
