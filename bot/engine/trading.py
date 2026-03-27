"""
engine/trading.py — DCA + Hedge state machine.

State flow:
  IDLE → open_main() → MAIN_OPEN
  MAIN_OPEN → price adverse → dca() (up to max_dca)
  MAIN_OPEN → price adverse beyond hedge_trigger → open_hedge()
  HEDGE_OPEN → price recovers → close_hedge()
  MAIN_OPEN/HEDGE_OPEN → TP hit → close_all()
  Any state → hard_stop hit → emergency_close()
"""
import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from config import BotConfig
from database import (
    close_hedge,
    close_session,
    create_hedge,
    create_session,
    get_open_hedges,
    get_open_session,
    log_signal,
    update_session,
)
from engine.indicators import compute_all
from engine.orderflow import OrderFlowAnalyzer
from engine.signal import SignalEngine
from exchange.binance_rest import BinanceRestClient
from notifications import notify

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Encapsulates all trading state.  Call tick() periodically.
    broadcast: callable(dict) → sends message to all connected WS clients.
    """

    def __init__(
        self,
        rest: BinanceRestClient,
        flow: OrderFlowAnalyzer,
        broadcast: Callable[[Dict], None],
    ) -> None:
        self._rest = rest
        self._flow = flow
        self._broadcast = broadcast
        self._signal_engine = SignalEngine()

        # Runtime state (re-loaded from DB on startup)
        self._session: Optional[Dict] = None
        self._hedges: List[Dict] = []

        # Trailing TP tracking
        self._trail_activated: bool = False
        self._trail_price: Optional[float] = None

        # DCA adverse pressure timer
        self._dca_pending_since: Optional[float] = None

        # Latest indicators (cached each tick for broadcast)
        self.last_signal: Dict = {}
        self.last_indicators: Dict = {}
        self.candles: List[Dict] = []

    # ------------------------------------------------------------------
    # Session broadcast helper
    # ------------------------------------------------------------------

    def _push_session(self) -> None:
        """Push current session + hedges + trade-level prices to all WS clients."""
        self._broadcast({
            "type":        "session",
            "session":     self._session,
            "hedges":      self._hedges,
            "trail_price": self._trail_price,
            "trail_active": self._trail_activated,
        })

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def restore_state(self) -> None:
        """Re-load open session + hedges from DB after restart."""
        self._session = await get_open_session()
        if self._session:
            self._hedges = await get_open_hedges(self._session["id"])
            logger.info(
                "TradingEngine: restored session id=%d dir=%s hedges=%d",
                self._session["id"],
                self._session["direction"],
                len(self._hedges),
            )

    # ------------------------------------------------------------------
    # Candle feed
    # ------------------------------------------------------------------

    def update_candles(self, candles: List[Dict]) -> None:
        self.candles = candles
        self.last_indicators = compute_all(candles)

    # ------------------------------------------------------------------
    # Main tick — called every N seconds by the scheduler
    # ------------------------------------------------------------------

    async def tick(self, cfg: BotConfig, price: float) -> None:
        ind = self.last_indicators
        flow_summary = self._flow.summarize()
        signal = self._signal_engine.compute(self.candles, flow_summary, ind)
        self.last_signal = signal

        # Broadcast signal to UI
        self._broadcast({"type": "signal", "data": signal})

        atr_val = ind.get("atr") or 0.0

        if self._session is None:
            await self._try_entry(cfg, price, signal, ind, atr_val)
        else:
            await self._manage_position(cfg, price, signal, ind, atr_val)

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    async def _try_entry(
        self,
        cfg: BotConfig,
        price: float,
        signal: Dict,
        ind: Dict,
        atr_val: float,
    ) -> None:
        direction = signal["direction"]
        strength  = signal["strength"]

        if direction == "NEUTRAL":
            await log_signal(cfg.symbol, "NEUTRAL", strength, signal["components"], "skip")
            return
        if strength < cfg.min_signal_strength:
            await log_signal(cfg.symbol, direction, strength, signal["components"], "skip")
            return
        if not signal["filters_passed"]:
            await log_signal(cfg.symbol, direction, strength, signal["components"], "skip")
            return

        qty = self._rest.calc_qty(cfg.symbol, cfg.margin_usdt, cfg.leverage, price)
        if qty <= 0:
            logger.warning("TradingEngine: qty=0, skipping entry")
            return

        side = "BUY" if direction == "LONG" else "SELL"
        order = await self._rest.place_market_order(
            cfg.symbol, side, qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        session_id = await create_session(
            symbol=cfg.symbol,
            direction=direction,
            entry_price=fill_price,
            qty=qty,
            margin=cfg.margin_usdt,
            leverage=cfg.leverage,
            entry_reason=signal["reason"],
            signal_strength=strength,
        )
        self._session = await get_open_session()
        self._hedges = []
        self._trail_activated = False
        self._trail_price = None
        self._dca_pending_since = None

        await log_signal(cfg.symbol, direction, strength, signal["components"], "entry")

        msg = (
            f"{'[PAPER] ' if cfg.paper_mode else ''}"
            f"OPEN {direction} @ {fill_price:.4f}  "
            f"qty={qty}  margin={cfg.margin_usdt}  strength={strength:.2f}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._push_session()

        await notify(cfg.discord_webhook, "TRADE_OPEN", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "margin":    cfg.margin_usdt,
            "strength":  strength,
            "paper":     cfg.paper_mode,
        })

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    async def _manage_position(
        self,
        cfg: BotConfig,
        price: float,
        signal: Dict,
        ind: Dict,
        atr_val: float,
    ) -> None:
        sess = self._session
        direction  = sess["direction"]
        avg_price  = sess["avg_price"]
        qty        = sess["qty"]
        leverage   = sess["leverage"]
        dca_count  = sess["dca_count"]
        hedge_count = sess["hedge_count"]

        # PnL calculation
        if direction == "LONG":
            pnl_pct = (price - avg_price) / avg_price * 100 * leverage
        else:
            pnl_pct = (avg_price - price) / avg_price * 100 * leverage

        # ---- Hard stop (4× hedge trigger) --------------------------------
        hard_stop_pct = cfg.hedge_trigger_pct * 4
        if pnl_pct <= -hard_stop_pct:
            logger.warning("TradingEngine: HARD STOP triggered pnl_pct=%.2f", pnl_pct)
            await self._emergency_close(cfg, price, pnl_pct)
            return

        # ---- Take-profit / trailing TP -----------------------------------
        if await self._check_tp(cfg, price, pnl_pct, avg_price, direction, qty):
            return

        # ---- Hedge management -------------------------------------------
        if self._hedges:
            await self._manage_hedges(cfg, price, pnl_pct, signal)

        # ---- Hedge trigger ----------------------------------------------
        if (
            not self._hedges
            and pnl_pct <= -cfg.hedge_trigger_pct
            and hedge_count < cfg.max_re_hedge
        ):
            # Hedge direction validation: if signal now agrees with main direction,
            # skip the hedge and let it recover
            if signal["direction"] == direction and signal["filters_passed"]:
                logger.info(
                    "TradingEngine: hedge skipped — signal agrees with main (%s)", direction
                )
                self._broadcast({"type": "notification",
                                 "text": f"Hedge skipped: signal agrees with {direction}"})
            else:
                await self._open_hedge(cfg, price, direction, qty)
            return

        # ---- DCA ---------------------------------------------------------
        if dca_count < cfg.max_dca:
            dca_step = self._calc_dca_step(cfg, atr_val, price)
            if pnl_pct <= -dca_step * leverage:
                await self._try_dca(cfg, price, direction, avg_price, qty, dca_count)

    # ------------------------------------------------------------------
    # Take-profit logic (trailing + fixed floor)
    # ------------------------------------------------------------------

    async def _check_tp(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
        avg_price: float,
        direction: str,
        qty: float,
    ) -> bool:
        """Returns True if position was closed."""
        tp_pct = cfg.tp_pct
        trail_pct = cfg.trail_pct
        min_pct = cfg.min_profit_pct
        sess = self._session

        # Initial TP threshold
        if pnl_pct >= tp_pct:
            if not self._trail_activated:
                # Activate trailing stop
                self._trail_activated = True
                if direction == "LONG":
                    self._trail_price = price * (1 - trail_pct / 100)
                else:
                    self._trail_price = price * (1 + trail_pct / 100)
                logger.info(
                    "TradingEngine: trailing TP activated @ %.4f", self._trail_price
                )
                return False
            else:
                # Update trailing stop
                if direction == "LONG":
                    new_trail = price * (1 - trail_pct / 100)
                    if new_trail > self._trail_price:
                        self._trail_price = new_trail
                else:
                    new_trail = price * (1 + trail_pct / 100)
                    if new_trail < self._trail_price:
                        self._trail_price = new_trail

        # Check if trailing stop hit
        if self._trail_activated and self._trail_price is not None:
            trail_hit = (
                (direction == "LONG" and price <= self._trail_price)
                or (direction == "SHORT" and price >= self._trail_price)
            )
            if trail_hit and pnl_pct >= min_pct:
                await self._close_position(cfg, price, pnl_pct, "trailing_tp")
                return True

        return False

    # ------------------------------------------------------------------
    # DCA
    # ------------------------------------------------------------------

    def _calc_dca_step(self, cfg: BotConfig, atr_val: float, price: float) -> float:
        """Return DCA step as a percentage of price (un-leveraged)."""
        fixed_step = cfg.dca_step_pct / 100
        if atr_val > 0 and price > 0:
            atr_step = atr_val * cfg.atr_dca_multiplier / price
            return max(fixed_step, atr_step) * 100
        return cfg.dca_step_pct

    async def _try_dca(
        self,
        cfg: BotConfig,
        price: float,
        direction: str,
        avg_price: float,
        qty: float,
        dca_count: int,
    ) -> None:
        """DCA with 5-second adverse pressure confirmation."""
        confirmed = self._flow.check_adverse_pressure(direction, required_seconds=5.0)
        if not confirmed:
            if self._dca_pending_since is None:
                self._dca_pending_since = time.time()
                logger.debug("TradingEngine: DCA pending — waiting for adverse pressure")
            return

        self._dca_pending_since = None

        new_qty = self._rest.calc_qty(cfg.symbol, cfg.margin_usdt, cfg.leverage, price)
        side = "BUY" if direction == "LONG" else "SELL"
        order = await self._rest.place_market_order(
            cfg.symbol, side, new_qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        total_qty = qty + new_qty
        new_avg = (avg_price * qty + fill_price * new_qty) / total_qty

        await update_session(
            self._session["id"],
            avg_price=new_avg,
            qty=total_qty,
            margin=self._session["margin"] + cfg.margin_usdt,
            dca_count=dca_count + 1,
        )
        self._session = await get_open_session()

        msg = (
            f"{'[PAPER] ' if cfg.paper_mode else ''}"
            f"DCA #{dca_count+1} {direction} @ {fill_price:.4f}  "
            f"new_avg={new_avg:.4f}  total_qty={total_qty:.4f}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._push_session()

        await log_signal(cfg.symbol, direction, 0.0, {}, "dca")
        await notify(cfg.discord_webhook, "TRADE_DCA", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "level":     dca_count + 1,
            "price":     fill_price,
            "new_avg":   new_avg,
            "total_margin": self._session["margin"],
            "paper": cfg.paper_mode,
        })

    # ------------------------------------------------------------------
    # Hedge
    # ------------------------------------------------------------------

    async def _open_hedge(
        self,
        cfg: BotConfig,
        price: float,
        main_dir: str,
        main_qty: float,
    ) -> None:
        hedge_dir  = "SHORT" if main_dir == "LONG" else "LONG"
        hedge_side = "SELL"  if main_dir == "LONG" else "BUY"
        hedge_qty  = self._rest.calc_qty(cfg.symbol, cfg.margin_usdt, cfg.leverage, price)

        order = await self._rest.place_market_order(
            cfg.symbol, hedge_side, hedge_qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        hedge_id = await create_hedge(
            session_id=self._session["id"],
            direction=hedge_dir,
            entry_price=fill_price,
            qty=hedge_qty,
            margin=cfg.margin_usdt,
        )
        await update_session(
            self._session["id"],
            hedge_count=self._session["hedge_count"] + 1,
        )
        self._session = await get_open_session()
        self._hedges = await get_open_hedges(self._session["id"])

        msg = (
            f"{'[PAPER] ' if cfg.paper_mode else ''}"
            f"HEDGE #{self._session['hedge_count']} {hedge_dir} @ {fill_price:.4f}  qty={hedge_qty}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._push_session()

        await log_signal(cfg.symbol, hedge_dir, 0.0, {}, "hedge")
        await notify(cfg.discord_webhook, "HEDGE_OPEN", {
            "symbol":    cfg.symbol,
            "direction": hedge_dir,
            "price":     fill_price,
            "qty":       hedge_qty,
            "paper":     cfg.paper_mode,
        })

    async def _manage_hedges(
        self,
        cfg: BotConfig,
        price: float,
        main_pnl_pct: float,
        signal: Dict,
    ) -> None:
        """Check if hedges should be closed (recovery or TP)."""
        sess = self._session
        main_dir = sess["direction"]

        for hedge in list(self._hedges):
            h_dir   = hedge["direction"]
            h_price = hedge["entry_price"]
            h_qty   = hedge["qty"]

            if h_dir == "LONG":
                h_pnl_pct = (price - h_price) / h_price * 100 * sess["leverage"]
            else:
                h_pnl_pct = (h_price - price) / h_price * 100 * sess["leverage"]

            # Close hedge if it hit TP
            if h_pnl_pct >= cfg.tp_pct:
                side = "SELL" if h_dir == "LONG" else "BUY"
                order = await self._rest.place_market_order(
                    cfg.symbol, side, h_qty, reduce_only=True, current_price=price
                )
                fill_price = float(order.get("avgPrice") or price)
                pnl = h_pnl_pct / 100 * hedge["margin"]
                await close_hedge(hedge["id"], pnl)
                self._hedges = [h for h in self._hedges if h["id"] != hedge["id"]]

                msg = f"{'[PAPER] ' if cfg.paper_mode else ''}HEDGE CLOSED (TP) @ {fill_price:.4f}"
                logger.info("TradingEngine: %s", msg)
                self._broadcast({"type": "notification", "text": msg})
                self._push_session()

                # If main is also profitable, close everything
                if main_pnl_pct >= cfg.min_profit_pct:
                    await self._close_position(cfg, price, main_pnl_pct, "hedge_tp_close")
                    return

    # ------------------------------------------------------------------
    # Close all
    # ------------------------------------------------------------------

    async def _close_position(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
        reason: str,
    ) -> None:
        sess = self._session
        direction = sess["direction"]
        qty       = sess["qty"]
        margin    = sess["margin"]

        # Close any open hedges first
        for hedge in list(self._hedges):
            h_side = "SELL" if hedge["direction"] == "LONG" else "BUY"
            order = await self._rest.place_market_order(
                cfg.symbol, h_side, hedge["qty"],
                reduce_only=True, current_price=price,
            )
            fill = float(order.get("avgPrice") or price)
            h_dir = hedge["direction"]
            if h_dir == "LONG":
                h_pnl = (fill - hedge["entry_price"]) / hedge["entry_price"] * hedge["margin"] * sess["leverage"]
            else:
                h_pnl = (hedge["entry_price"] - fill) / hedge["entry_price"] * hedge["margin"] * sess["leverage"]
            await close_hedge(hedge["id"], h_pnl)
        self._hedges = []

        # Close main
        side = "SELL" if direction == "LONG" else "BUY"
        order = await self._rest.place_market_order(
            cfg.symbol, side, qty, reduce_only=True, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        realized_pnl = pnl_pct / 100 * margin
        await close_session(sess["id"], round(realized_pnl, 4), reason)

        msg = (
            f"{'[PAPER] ' if cfg.paper_mode else ''}"
            f"CLOSE {direction} @ {fill_price:.4f}  "
            f"pnl={realized_pnl:+.4f} USDT ({pnl_pct:+.2f}%)  reason={reason}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})

        await log_signal(cfg.symbol, direction, 0.0, {}, "close")
        await notify(cfg.discord_webhook, "TRADE_CLOSE", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "pnl":       realized_pnl,
            "pnl_pct":   pnl_pct,
            "reason":    reason,
            "paper":     cfg.paper_mode,
        })

        self._session = None
        self._hedges  = []
        self._trail_activated = False
        self._trail_price = None
        self._dca_pending_since = None
        self._push_session()

    async def _emergency_close(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
    ) -> None:
        logger.error(
            "TradingEngine: EMERGENCY CLOSE  pnl_pct=%.2f  price=%.4f", pnl_pct, price
        )
        self._broadcast({
            "type": "notification",
            "text": f"⚠ HARD STOP triggered @ {price:.4f}  pnl={pnl_pct:+.2f}%",
        })
        await notify(cfg.discord_webhook, "HARD_STOP", {
            "symbol":  cfg.symbol,
            "price":   price,
            "pnl_pct": pnl_pct,
            "paper":   cfg.paper_mode,
        })
        await self._close_position(cfg, price, pnl_pct, "hard_stop")

    # ------------------------------------------------------------------
    # Forced close (API-level reset)
    # ------------------------------------------------------------------

    async def force_close_all(self, cfg: BotConfig, price: float) -> None:
        """Called from /api/reset. Closes open position at current price."""
        if self._session:
            direction = self._session["direction"]
            avg_price = self._session["avg_price"]
            leverage  = self._session["leverage"]
            if direction == "LONG":
                pnl_pct = (price - avg_price) / avg_price * 100 * leverage
            else:
                pnl_pct = (avg_price - price) / avg_price * 100 * leverage
            await self._close_position(cfg, price, pnl_pct, "manual_reset")
