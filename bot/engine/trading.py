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
from exchange.order_executor import OrderExecutor
from notifications import notify

logger = logging.getLogger(__name__)

_MODE_PREFIXES = {"paper": "[PAPER] ", "demo": "[DEMO] ", "live": "[LIVE] "}


def _mode_prefix(trading_mode: str) -> str:
    return _MODE_PREFIXES.get(trading_mode, "")


def _ts() -> int:
    return int(time.time())


class TradingEngine:
    """
    Encapsulates all trading state.  Call tick() periodically.
    broadcast: callable(dict) → sends message to all connected WS clients.
    """

    def __init__(
        self,
        executor: OrderExecutor,
        flow: OrderFlowAnalyzer,
        broadcast: Callable[[Dict], None],
    ) -> None:
        self._executor = executor
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

        # Pending fill tracking: order_id → {"type": "entry"|"dca", "prior_qty": float, "prior_avg": float}
        # Used by _on_user_data in main.py to compute correct blended average from true fill price.
        self._pending_fills: Dict[int, Dict] = {}

        # Adaptive risk parameters — two copies:
        #   _adaptive      : refreshed every tick (current market conditions)
        #   _entry_adaptive: locked at trade entry, used for ALL SL/TP decisions
        self._adaptive: Dict[str, float] = {}
        self._entry_adaptive: Dict[str, float] = {}

        # Latest indicators (cached each tick for broadcast)
        self.last_signal: Dict = {}
        self.last_indicators: Dict = {}
        self.candles: List[Dict] = []

    # ------------------------------------------------------------------
    # Session broadcast helper
    # ------------------------------------------------------------------

    def _push_session(self) -> None:
        """Push current session + hedges + trade-level prices to all WS clients."""
        tp_price = sl_price = None
        # Always use the LOCKED entry adaptive so displayed levels never drift
        ref = self._entry_adaptive or self._adaptive
        if self._session and ref:
            avg = self._session["avg_price"]
            d   = self._session["direction"]
            tp  = ref["tp_pct"]
            sl  = ref["hard_stop_pct"]
            if d == "LONG":
                tp_price = avg * (1 + tp / 100)
                sl_price = avg * (1 - sl / 100)
            else:
                tp_price = avg * (1 - tp / 100)
                sl_price = avg * (1 + sl / 100)
        self._broadcast({
            "type":         "session",
            "session":      self._session,
            "hedges":       self._hedges,
            "trail_price":  self._trail_price,
            "trail_active": self._trail_activated,
            "tp_price":     tp_price,
            "sl_price":     sl_price,
        })

    def _pos_log(self, event: str, **kw) -> None:
        """Broadcast a structured position-log entry to all connected clients."""
        self._broadcast({"type": "pos_log", "event": event, "ts": _ts(), **kw})

    @staticmethod
    def _compute_adaptive(atr: float, price: float) -> Dict[str, float]:
        """
        Derive all risk thresholds from ATR.
        All values are un-leveraged price percentages.

        Ordering guaranteed:
          dca_step < hedge_trigger < hard_stop
          trail_pct < tp_pct
          min_profit_pct < tp_pct
        """
        atr_pct = (atr / price * 100) if price > 0 else 1.0
        return {
            "tp_pct":            max(atr_pct * 2.5, 0.8),   # TP: 2.5× ATR, min 0.8%
            "trail_pct":         max(atr_pct * 1.0, 0.25),  # trail: 1.0× ATR, min 0.25%
            "min_profit_pct":    max(atr_pct * 0.3, 0.10),  # min to close: 0.3× ATR, min 0.10%
            "dca_step_pct":      max(atr_pct * 1.5, 0.50),  # DCA: 1.5× ATR, min 0.50%
            "hedge_trigger_pct": max(atr_pct * 3.0, 1.00),  # hedge: 3.0× ATR, min 1.00%
            "hard_stop_pct":     max(atr_pct * 5.0, 2.00),  # stop: 5.0× ATR, min 2.00%
        }

    # ------------------------------------------------------------------
    # Level prices helper (used by WS initial-state and _push_session)
    # ------------------------------------------------------------------

    def get_level_prices(self):
        """Return (tp_price, sl_price) for the current session, or (None, None)."""
        ref = self._entry_adaptive or self._adaptive
        if not self._session or not ref:
            return None, None
        avg = self._session["avg_price"]
        d   = self._session["direction"]
        tp  = ref["tp_pct"]
        sl  = ref["hard_stop_pct"]
        if d == "LONG":
            return avg * (1 + tp / 100), avg * (1 - sl / 100)
        else:
            return avg * (1 - tp / 100), avg * (1 + sl / 100)

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def restore_state(self) -> None:
        """Re-load open session + hedges from DB after restart."""
        self._session = await get_open_session()
        if self._session:
            self._hedges = await get_open_hedges(self._session["id"])
            # Restore trailing-stop state so it survives restarts
            self._trail_activated = bool(self._session.get("trail_active", 0))
            self._trail_price     = self._session.get("trail_price") or None
            logger.info(
                "TradingEngine: restored session id=%d dir=%s hedges=%d trail=%s@%.6f",
                self._session["id"],
                self._session["direction"],
                len(self._hedges),
                self._trail_activated,
                self._trail_price or 0.0,
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

        # Validate market conditions before committing to a trade
        if atr_val <= 0:
            logger.info("TradingEngine: skipping entry — ATR unavailable")
            return
        entry_adaptive = self._compute_adaptive(atr_val, price)
        if entry_adaptive["tp_pct"] < 0.4:
            logger.info(
                "TradingEngine: skipping entry — TP too tight (%.3f%% < 0.40%%)",
                entry_adaptive["tp_pct"],
            )
            await log_signal(cfg.symbol, direction, strength, signal["components"], "skip")
            return

        qty = self._executor.calc_qty(cfg.symbol, cfg.margin_usdt, cfg.leverage, price)
        if qty <= 0:
            logger.warning("TradingEngine: qty=0, skipping entry")
            return

        # Sync leverage to Binance before opening — Binance keeps its own
        # per-symbol leverage setting that defaults to 20x and must be set
        # explicitly, otherwise the exchange margin display will be wrong.
        await self._executor.ensure_leverage(cfg.symbol, cfg.leverage)

        side = "BUY" if direction == "LONG" else "SELL"
        order = await self._executor.place_market_order(
            cfg.symbol, side, qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        # Track this order so _on_user_data can correct fill price if avgPrice was "0"
        order_id = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {"type": "entry", "prior_qty": 0.0, "prior_avg": 0.0}

        session_id = await create_session(
            symbol=cfg.symbol,
            direction=direction,
            entry_price=fill_price,
            qty=qty,
            margin=cfg.margin_usdt,
            leverage=cfg.leverage,
            entry_reason=signal["reason"],
            signal_strength=strength,
            signal_price=price,
        )
        self._session = await get_open_session()
        self._hedges = []
        self._trail_activated = False
        self._trail_price = None
        self._dca_pending_since = None
        self._entry_adaptive = entry_adaptive  # locked for life of this trade

        await log_signal(cfg.symbol, direction, strength, signal["components"], "entry")

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"OPEN {direction} @ {fill_price:.4f}  "
            f"qty={qty}  margin={cfg.margin_usdt}  strength={strength:.2f}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("open", direction=direction, price=fill_price, qty=qty,
                      symbol=cfg.symbol, mode=cfg.trading_mode)
        self._push_session()

        await notify(cfg.discord_webhook, "TRADE_OPEN", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "margin":    cfg.margin_usdt,
            "strength":  strength,
            "trading_mode": cfg.trading_mode,
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
        sess        = self._session
        direction   = sess["direction"]
        avg_price   = sess["avg_price"]
        qty         = sess["qty"]
        leverage    = sess["leverage"]
        dca_count   = sess["dca_count"]
        hedge_count = sess["hedge_count"]

        # Refresh _adaptive with current ATR for informational purposes
        if atr_val > 0 and price > 0:
            self._adaptive = self._compute_adaptive(atr_val, price)

        # _entry_adaptive is locked at entry; if missing (e.g. after a restart),
        # initialise from current ATR once and keep it fixed from here on.
        if not self._entry_adaptive:
            self._entry_adaptive = self._adaptive or self._compute_adaptive(price * 0.005, price)
            logger.info(
                "TradingEngine: entry_adaptive initialised from current ATR "
                "(tp=%.3f%% sl=%.3f%%)",
                self._entry_adaptive["tp_pct"],
                self._entry_adaptive["hard_stop_pct"],
            )

        # ALL risk decisions use the locked entry levels — never the live ATR
        p = self._entry_adaptive

        # Un-leveraged price movement (positive = favourable for direction)
        if direction == "LONG":
            price_pct = (price - avg_price) / avg_price * 100
        else:
            price_pct = (avg_price - price) / avg_price * 100
        # Leveraged pnl — used only for dollar PnL calculations
        pnl_pct = price_pct * leverage

        # ---- Hard stop -------------------------------------------------------
        if price_pct <= -p["hard_stop_pct"]:
            logger.warning(
                "TradingEngine: HARD STOP  price_pct=%.2f%%  hard_stop=%.2f%%",
                price_pct, p["hard_stop_pct"],
            )
            await self._emergency_close(cfg, price, pnl_pct)
            return

        # ---- Take-profit / trailing TP ---------------------------------------
        if await self._check_tp(cfg, price, price_pct, pnl_pct, direction, p):
            return

        # ---- Hedge management -----------------------------------------------
        if self._hedges:
            await self._manage_hedges(cfg, price, price_pct, pnl_pct, p, signal)
            if self._session is None:
                return

        # ---- Hedge trigger --------------------------------------------------
        if (
            not self._hedges
            and price_pct <= -p["hedge_trigger_pct"]
            and hedge_count < cfg.max_re_hedge
        ):
            if signal["direction"] == direction and signal["filters_passed"]:
                logger.info("TradingEngine: hedge skipped — signal agrees with main (%s)", direction)
                self._broadcast({"type": "notification",
                                 "text": f"Hedge skipped: signal agrees with {direction}"})
            else:
                await self._open_hedge(cfg, price, direction, qty)
            return

        # ---- DCA ------------------------------------------------------------
        if dca_count < cfg.max_dca and price_pct <= -p["dca_step_pct"]:
            opposite = "SHORT" if direction == "LONG" else "LONG"
            if signal["direction"] == opposite and signal["filters_passed"]:
                logger.info(
                    "TradingEngine: DCA skipped — signal disagrees (%s vs main %s)",
                    signal["direction"], direction,
                )
                self._broadcast({"type": "notification",
                                 "text": f"DCA skipped: signal disagrees ({signal['direction']} vs {direction})"})
            else:
                await self._try_dca(cfg, price, direction, avg_price, qty, dca_count)

    # ------------------------------------------------------------------
    # Take-profit logic (trailing + fixed floor)
    # ------------------------------------------------------------------

    async def _check_tp(
        self,
        cfg: BotConfig,
        price: float,
        price_pct: float,
        pnl_pct: float,
        direction: str,
        p: Dict[str, float],
    ) -> bool:
        """Returns True if position was closed. price_pct and thresholds are un-leveraged price %."""
        tp_pct    = p["tp_pct"]
        trail_pct = p["trail_pct"]

        if price_pct >= tp_pct:
            if not self._trail_activated:
                # First time price reaches the TP level — arm the trail
                self._trail_activated = True
                if direction == "LONG":
                    self._trail_price = price * (1 - trail_pct / 100)
                else:
                    self._trail_price = price * (1 + trail_pct / 100)
                logger.info("TradingEngine: trailing TP activated @ %.6f", self._trail_price)
                await update_session(
                    self._session["id"],
                    trail_active=1,
                    trail_price=self._trail_price,
                )
                self._push_session()
                return False
            else:
                # Trail already armed — ratchet it in the favourable direction
                moved = False
                if direction == "LONG":
                    new_trail = price * (1 - trail_pct / 100)
                    if new_trail > self._trail_price:
                        self._trail_price = new_trail
                        moved = True
                else:
                    new_trail = price * (1 + trail_pct / 100)
                    if new_trail < self._trail_price:
                        self._trail_price = new_trail
                        moved = True
                if moved:
                    await update_session(
                        self._session["id"],
                        trail_price=self._trail_price,
                    )
                    self._push_session()

        if self._trail_activated and self._trail_price is not None:
            trail_hit = (
                (direction == "LONG"  and price <= self._trail_price)
                or (direction == "SHORT" and price >= self._trail_price)
            )
            # Close unconditionally when trail is hit — the hard stop at
            # hard_stop_pct is the only other exit and it's worse.
            # The old min_pct guard was removed because a large single-tick
            # adverse move could drop price through the trail AND below
            # min_pct in the same tick, leaving the trail permanently ignored.
            if trail_hit:
                await self._close_position(cfg, price, pnl_pct, "trailing_tp")
                return True

        return False

    # ------------------------------------------------------------------
    # DCA
    # ------------------------------------------------------------------

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

        new_qty = self._executor.calc_qty(cfg.symbol, cfg.margin_usdt, cfg.leverage, price)
        side = "BUY" if direction == "LONG" else "SELL"
        order = await self._executor.place_market_order(
            cfg.symbol, side, new_qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        # Track this order so _on_user_data can reblend with the true fill price.
        # Store the pre-DCA state so the callback can compute: (prior_avg*prior_qty + fill*new_qty) / total
        order_id = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {"type": "dca", "prior_qty": qty, "prior_avg": avg_price, "new_qty": new_qty}

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
            f"{_mode_prefix(cfg.trading_mode)}"
            f"DCA #{dca_count+1} {direction} @ {fill_price:.4f}  "
            f"new_avg={new_avg:.4f}  total_qty={total_qty:.4f}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("dca", direction=direction, price=fill_price, qty=new_qty,
                      dca_n=dca_count + 1, new_avg=round(new_avg, 6),
                      symbol=cfg.symbol, mode=cfg.trading_mode)
        self._push_session()

        await log_signal(cfg.symbol, direction, 0.0, {}, "dca")
        await notify(cfg.discord_webhook, "TRADE_DCA", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "level":     dca_count + 1,
            "price":     fill_price,
            "new_avg":   new_avg,
            "total_margin": self._session["margin"],
            "trading_mode": cfg.trading_mode,
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
        # Size the hedge to match the current main position (including any DCA).
        # Using main_qty directly ensures full exposure is offset, not just 1×margin.
        hedge_qty  = main_qty

        order = await self._executor.place_market_order(
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
            f"{_mode_prefix(cfg.trading_mode)}"
            f"HEDGE #{self._session['hedge_count']} {hedge_dir} @ {fill_price:.4f}  qty={hedge_qty}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("hedge_open", direction=hedge_dir, price=fill_price, qty=hedge_qty,
                      hedge_n=self._session["hedge_count"],
                      symbol=cfg.symbol, mode=cfg.trading_mode)
        self._push_session()

        await log_signal(cfg.symbol, hedge_dir, 0.0, {}, "hedge")
        await notify(cfg.discord_webhook, "HEDGE_OPEN", {
            "symbol":    cfg.symbol,
            "direction": hedge_dir,
            "price":     fill_price,
            "qty":       hedge_qty,
            "trading_mode": cfg.trading_mode,
        })

    async def _manage_hedges(
        self,
        cfg: BotConfig,
        price: float,
        main_price_pct: float,
        main_pnl_pct: float,
        p: Dict[str, float],
        signal: Dict,
    ) -> None:
        """Check if hedges should be closed (recovery or TP)."""
        sess    = self._session
        main_dir = sess["direction"]

        for hedge in list(self._hedges):
            h_dir   = hedge["direction"]
            h_price = hedge["entry_price"]
            h_qty   = hedge["qty"]

            if h_dir == "LONG":
                h_price_pct = (price - h_price) / h_price * 100
            else:
                h_price_pct = (h_price - price) / h_price * 100

            # ── Hedge TP: market kept going adverse for main ──────────────
            if h_price_pct >= p["tp_pct"]:
                side = "SELL" if h_dir == "LONG" else "BUY"
                order = await self._executor.place_market_order(
                    cfg.symbol, side, h_qty, close_hedge=True, current_price=price
                )
                fill_price = float(order.get("avgPrice") or price)
                h_pnl = h_price_pct / 100 * sess["leverage"] * hedge["margin"]
                await close_hedge(hedge["id"], h_pnl)
                self._hedges = [h for h in self._hedges if h["id"] != hedge["id"]]

                msg = f"{_mode_prefix(cfg.trading_mode)}HEDGE CLOSED (TP) @ {fill_price:.6f}"
                logger.info("TradingEngine: %s", msg)
                self._broadcast({"type": "notification", "text": msg})
                self._pos_log("hedge_close", direction=h_dir, price=fill_price,
                              pnl=round(h_pnl, 4), reason="tp",
                              symbol=cfg.symbol, mode=cfg.trading_mode)
                self._push_session()

                if main_price_pct >= p["min_profit_pct"]:
                    await self._close_position(cfg, price, main_pnl_pct, "hedge_tp_close")
                    return

            # ── Recovery close: main has recovered to breakeven ───────────
            # The hedge was protecting against further losses. Now that the
            # main is no longer losing, stop the hedge from bleeding further.
            elif main_price_pct >= 0:
                side = "SELL" if h_dir == "LONG" else "BUY"
                order = await self._executor.place_market_order(
                    cfg.symbol, side, h_qty, close_hedge=True, current_price=price
                )
                fill_price = float(order.get("avgPrice") or price)
                h_pnl = h_price_pct / 100 * sess["leverage"] * hedge["margin"]
                await close_hedge(hedge["id"], h_pnl)
                self._hedges = [h for h in self._hedges if h["id"] != hedge["id"]]

                msg = (
                    f"{_mode_prefix(cfg.trading_mode)}"
                    f"HEDGE CLOSED (recovery) @ {fill_price:.6f}  "
                    f"hedge_pnl={h_pnl:+.4f}"
                )
                logger.info("TradingEngine: %s", msg)
                self._broadcast({"type": "notification", "text": msg})
                self._pos_log("hedge_close", direction=h_dir, price=fill_price,
                              pnl=round(h_pnl, 4), reason="recovery",
                              symbol=cfg.symbol, mode=cfg.trading_mode)
                self._push_session()

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

        # Close any open hedges first and accumulate their PnL
        total_hedge_pnl = 0.0
        for hedge in list(self._hedges):
            h_side = "SELL" if hedge["direction"] == "LONG" else "BUY"
            order = await self._executor.place_market_order(
                cfg.symbol, h_side, hedge["qty"],
                close_hedge=True, current_price=price,
            )
            fill = float(order.get("avgPrice") or price)
            h_dir = hedge["direction"]
            if h_dir == "LONG":
                h_pnl = (fill - hedge["entry_price"]) / hedge["entry_price"] * hedge["margin"] * sess["leverage"]
            else:
                h_pnl = (hedge["entry_price"] - fill) / hedge["entry_price"] * hedge["margin"] * sess["leverage"]
            await close_hedge(hedge["id"], h_pnl)
            total_hedge_pnl += h_pnl
        self._hedges = []

        # Close main
        side = "SELL" if direction == "LONG" else "BUY"
        order = await self._executor.place_market_order(
            cfg.symbol, side, qty, reduce_only=True, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        # Include hedge PnL in the session total so history shows true net result
        realized_pnl = pnl_pct / 100 * margin + total_hedge_pnl
        await close_session(sess["id"], round(realized_pnl, 4), reason)

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"CLOSE {direction} @ {fill_price:.4f}  "
            f"pnl={realized_pnl:+.4f} USDT ({pnl_pct:+.2f}%)  reason={reason}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("close", direction=direction, price=fill_price,
                      pnl=round(realized_pnl, 4), pnl_pct=round(pnl_pct, 2),
                      reason=reason, symbol=cfg.symbol, mode=cfg.trading_mode)

        await log_signal(cfg.symbol, direction, 0.0, {}, "close")
        await notify(cfg.discord_webhook, "TRADE_CLOSE", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "pnl":       realized_pnl,
            "pnl_pct":   pnl_pct,
            "reason":    reason,
            "trading_mode": cfg.trading_mode,
        })

        self._session         = None
        self._hedges          = []
        self._trail_activated = False
        self._trail_price     = None
        self._dca_pending_since = None
        self._entry_adaptive  = {}
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
