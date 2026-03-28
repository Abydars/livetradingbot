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
        """Ensure hedge mode is enabled on Binance, then read back the active mode."""
        if self._client and not self.paper_mode:
            # Always request hedge mode — the bot's hedge logic requires it.
            # Binance returns code -4059 ("No need to change") if it's already set; that's fine.
            try:
                await self._client.set_position_mode(dual_side=True)
                logger.info("OrderExecutor: position mode set to HEDGE")
            except Exception as exc:
                if "-4059" in str(exc):
                    logger.debug("OrderExecutor: position mode already HEDGE")
                else:
                    logger.warning("OrderExecutor: could not set hedge mode: %s", exc)

            try:
                self._hedge_mode = await self._client.get_position_mode()
                logger.info(
                    "OrderExecutor: position mode = %s",
                    "HEDGE" if self._hedge_mode else "ONE-WAY",
                )
            except Exception as exc:
                logger.warning("OrderExecutor: could not fetch position mode: %s", exc)
                self._hedge_mode = False

    async def prepare_symbol(self, symbol: str, leverage: int) -> None:
        """
        Configure symbol-level settings on Binance before trading begins.
        Called at startup and on every auto-switch / symbol change so that
        the first order can be placed immediately without any setup latency.

          1. Margin type → ISOLATED  (risk-isolated per position)
          2. Leverage    → cfg.leverage

        Binance returns -4046 when margin type is already correct — silently ignored.
        """
        if self.paper_mode or not self._client:
            return

        from binance_client import MarginType

        symbol = symbol.upper()

        try:
            await self._client.change_margin_type(symbol, MarginType.ISOLATED)
            logger.info("OrderExecutor: margin type = ISOLATED for %s", symbol)
        except Exception as exc:
            if "-4046" in str(exc):
                logger.debug("OrderExecutor: margin type already ISOLATED for %s", symbol)
            else:
                logger.warning("OrderExecutor: could not set margin type for %s: %s", symbol, exc)

        try:
            await self._client.change_leverage(symbol, leverage)
            logger.info("OrderExecutor: leverage = %dx for %s", leverage, symbol)
        except Exception as exc:
            logger.warning("OrderExecutor: could not set leverage for %s: %s", symbol, exc)

    async def ensure_leverage(self, symbol: str, leverage: int) -> None:
        """
        Last-resort leverage sync just before entry — catches cases where
        leverage was changed in config after startup without a symbol switch.
        The upfront prepare_symbol() call handles the normal path.
        """
        if self.paper_mode or not self._client:
            return
        try:
            await self._client.change_leverage(symbol, leverage)
        except Exception as exc:
            logger.warning("OrderExecutor: could not set leverage: %s", exc)

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "BUY" | "SELL"
        qty: float,
        reduce_only: bool = False,
        current_price: float = 0.0,
        close_hedge: bool = False,
    ) -> dict:
        """
        Place a market order using the appropriate execution path.

        close_hedge=True: closing a bot-tracked hedge position (not the main position).
          - Hedge-mode account: flips positionSide to target the hedge leg (same as reduce_only).
          - One-way account: sends a plain opposing order without reduceOnly, because in one-way
            mode the hedge was just a partial close of the main position — there is no separate
            LONG/SHORT position on Binance to "reduce".

        Returns an order-result dict with at minimum:
            orderId, symbol, side, origQty, avgPrice, executedQty, status
        """
        prefix = _MODE_PREFIX.get(self.trading_mode, "")
        logger.info(
            "%splace_market_order %s %s qty=%.6f reduce_only=%s close_hedge=%s",
            prefix, symbol, side, qty, reduce_only, close_hedge,
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

        # Treat close_hedge the same as reduce_only for positionSide selection in hedge mode.
        is_close = reduce_only or close_hedge

        if self._hedge_mode:
            # Hedge mode: positionSide determines which side to open/close.
            # Opening: BUY → LONG, SELL → SHORT
            # Closing (reduce_only or close_hedge): BUY closes SHORT, SELL closes LONG
            if not is_close:
                pos_side = PositionSide.LONG if side == "BUY" else PositionSide.SHORT
            else:
                pos_side = PositionSide.SHORT if side == "BUY" else PositionSide.LONG
            reduce = False  # reduceOnly is FORBIDDEN in hedge mode per docs
        else:
            # One-way mode: always BOTH.
            # reduce_only=True for main-position closes (safe guard against accidental opens).
            # close_hedge: do NOT use reduceOnly — in one-way mode the hedge leg has no separate
            # Binance position; closing it just places an opposing order to restore the main.
            pos_side = PositionSide.BOTH
            reduce = reduce_only and not close_hedge

        result = await self._client.place_order_ws(
            symbol=symbol,
            side=order_side,
            order_type=OrderType.MARKET,
            quantity=qty,
            position_side=pos_side,
            reduce_only=reduce,
        )

        # For MARKET orders avgPrice is the true fill price.
        # Binance WS responses often return avgPrice="0" for market orders
        # because the response is sent before match-engine confirmation.
        # Use cumQuote/executedQty instead — both are always populated on FILLED
        # orders and give the true volume-weighted average fill price.
        avg_price_raw = result.get("avgPrice", "0")
        if not avg_price_raw or float(avg_price_raw) == 0:
            cum_quote    = float(result.get("cumQuote", 0))
            executed_qty = float(result.get("executedQty", 0))
            if cum_quote > 0 and executed_qty > 0:
                computed_avg = cum_quote / executed_qty
                result["avgPrice"] = str(computed_avg)
                logger.info(
                    "avgPrice was 0 in order response — computed from "
                    "cumQuote/executedQty: %.6f", computed_avg,
                )
            else:
                # Last resort: cumQuote also missing (paper mode, demo quirk,
                # or order not yet matched). Fall back to mark price and let
                # ORDER_TRADE_UPDATE correct it via _on_user_data.
                result["avgPrice"] = str(current_price)
                logger.warning(
                    "avgPrice and cumQuote both unavailable — using mark price "
                    "%.6f as temporary fill price. ORDER_TRADE_UPDATE will correct.",
                    current_price,
                )

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

    def round_qty(self, symbol: str, qty: float) -> float:
        """Round an arbitrary qty to the symbol's exchange step size."""
        return self._rest._round_qty(symbol, qty)
