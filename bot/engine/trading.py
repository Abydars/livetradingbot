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
    get_today_pnl,
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

        # Set True during _close_position / _emergency_close so that concurrent
        # ACCOUNT_UPDATE events (pa=0) don't trigger a spurious external-close.
        self._closing: bool = False

        # Trailing TP tracking
        self._trail_activated: bool = False
        self._trail_price: Optional[float] = None
        self._trail_pct_mult: float = 1.0   # tighten-only multiplier, updated on candle close

        # Pending fill tracking: order_id → {"type": "entry"|"dca", "prior_qty": float, "prior_avg": float}
        # Used by _on_user_data in main.py to compute correct blended average from true fill price.
        self._pending_fills: Dict[int, Dict] = {}

        # Track IDs of bot-initiated close orders so ORDER_TRADE_UPDATE
        # does not treat them as external closes.
        self._bot_close_order_ids: set = set()

        # Minimum time gate between DCAs — set on each DCA execution
        self._last_dca_time: Optional[float] = None

        # Smart SL: counts consecutive ticks of strong opposite signal in loss
        self._smart_sl_ticks: int = 0

        # Signal degradation exit: counts ticks of NEUTRAL while position losing
        self._signal_degraded_ticks: int = 0

        # Signal persistence: count consecutive ticks holding the same direction.
        # Entry only fires after signal holds for N ticks — prevents entering on
        # a single noisy tick that immediately flips back to NEUTRAL.
        self._entry_signal_ticks: int = 0
        self._entry_signal_dir:   str = "NEUTRAL"

        # Rescue mode: set after rescue DCA, arms a tight trail to minimize loss
        self._rescue_mode:        bool          = False
        self._rescue_trail_price: Optional[float] = None   # best price seen since rescue DCA

        # Adaptive risk parameters — two copies:
        #   _adaptive      : refreshed every tick (current market conditions)
        #   _entry_adaptive: locked at trade entry, updated on each DCA
        self._adaptive: Dict[str, float] = {}
        self._entry_adaptive: Dict[str, float] = {}

        # Manual level overrides set from the UI.
        # When set, these replace the ATR-computed TP arm / hard-stop prices.
        # Cleared automatically when DCA fires, a hedge opens, or position closes.
        self._override_tp_price: Optional[float] = None
        self._override_sl_price: Optional[float] = None

        # Breakeven stop — set after a DCA recovery to prevent giving back profit
        self._breakeven_stop_price: Optional[float] = None
        self._last_resort_buffer_cache: float = 0.80  # updated each tick from cfg

        # Stop cooldown: set to time.time() after hard stop, cleared on normal close.
        self._last_stop_time: Optional[float] = None

        # Partial TP: True once we have closed the first fraction; reset on full close.
        self._partial_tp_done: bool = False

        # Latest indicators (cached each tick for broadcast)
        self.last_signal: Dict = {}
        self._scanner_type: str = "momentum"   # scanner type that found current symbol
        self.last_indicators:  Dict = {}
        self._prev_indicators: Dict = {}   # indicators from the tick before last — used for slope checks
        self.candles: List[Dict] = []

    # ------------------------------------------------------------------
    # Session broadcast helper
    # ------------------------------------------------------------------

    def _push_session(self) -> None:
        """Push current session + hedges + trade-level prices to all WS clients."""
        tp_price = sl_price = None

        if self._session:
            entry    = self._session["entry_price"]
            avg      = self._session["avg_price"]
            d        = self._session["direction"]
            leverage = self._session["leverage"]

            # TP arm: requires _entry_adaptive (locked at entry).
            # Only compute if available — after restart it may not be set yet,
            # and will be populated on the next tick when _manage_position runs.
            ref = self._entry_adaptive or self._adaptive
            if ref:
                tp = ref["tp_pct"]
                if d == "LONG":
                    tp_price = entry * (1 + tp / 100)
                else:
                    tp_price = entry * (1 - tp / 100)

            # SL: does NOT need ref — only uses session values + buffer cache.
            # This works correctly even after restart when ref is empty.
            if self._breakeven_stop_price is not None:
                sl_price = self._breakeven_stop_price
            else:
                buffer  = self._last_resort_buffer_cache
                liq_pct = (1.0 / leverage) if leverage > 0 else 0.10
                sl_pct  = liq_pct * buffer
                if d == "LONG":
                    sl_price = avg * (1 - sl_pct)
                else:
                    sl_price = avg * (1 + sl_pct)
        self._broadcast({
            "type":                  "session",
            "session":               self._session,
            "hedges":                self._hedges,
            "trail_price":           self._trail_price,
            "trail_active":          self._trail_activated,
            "tp_price":              tp_price,
            "sl_price":              sl_price,
            "override_tp_price":     self._override_tp_price,
            "override_sl_price":     self._override_sl_price,
            "breakeven_stop_price":  self._breakeven_stop_price,
        })

    def _pos_log(self, event: str, **kw) -> None:
        """Broadcast a structured position-log entry to all connected clients
        and persist it to the database for reload on page refresh."""
        payload = {"type": "pos_log", "event": event, "ts": _ts(), **kw}
        self._broadcast(payload)
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            from database import insert_pos_log
            loop.create_task(insert_pos_log(event, payload))
        except RuntimeError:
            pass

    def _clear_level_overrides(self, reason: str = "") -> None:
        """Clear manual TP/SL overrides and notify the UI."""
        if self._override_tp_price is None and self._override_sl_price is None:
            return
        self._override_tp_price = None
        self._override_sl_price = None
        logger.info("TradingEngine: level overrides cleared (%s)", reason)
        self._broadcast({"type": "level_overrides_cleared"})

    @staticmethod
    def _compute_adaptive(
        atr: float,
        price: float,
        strength: float = 0.0,
    ) -> Dict[str, float]:
        """
        Derive all risk thresholds from ATR, optionally scaled by signal strength.

        strength: signal strength at entry (0.0–1.0).
          tp_pct and trail_pct scale up with stronger signals so high-conviction
          entries get more room to run before the trail fires.
          DCA, hedge, and stop thresholds are ATR-only (unchanged).

        Ordering guaranteed:
          dca_step < hedge_trigger < hard_stop
          trail_pct < tp_pct
          min_profit_pct < tp_pct
        """
        atr_pct  = (atr / price * 100) if price > 0 else 1.0
        s        = max(0.0, min(1.0, strength))
        tp_scale = 1.0 + s          # 0%→1.0×  35%→1.35×  70%→1.70×
        tr_scale = 1.0 + s * 0.5   # 0%→1.0×  35%→1.175× 70%→1.35×
        return {
            "tp_pct":            max(atr_pct * 2.5, 0.8)  * tp_scale,
            "trail_pct":         max(atr_pct * 1.0, 0.25) * tr_scale,
            "min_profit_pct":    max(atr_pct * 0.3, 0.10),
            "dca_step_pct":      max(atr_pct * 1.5, 0.50),
            "hedge_trigger_pct": max(atr_pct * 3.0, 1.00),
            "hard_stop_pct":     max(atr_pct * 5.0, 2.00),
        }

    @staticmethod
    def _estimate_fees(qty: float, price: float, taker_fee_pct: float) -> float:
        """
        Estimate taker fee for the EXIT leg of a position.
        Entry fees were paid at open and are not deducted here.
        notional = qty × exit_price; fee = notional × rate.
        """
        notional = qty * price
        return notional * (taker_fee_pct / 100)

    @staticmethod
    def _momentum_trail_mult(ind: Dict, direction: str) -> float:
        """
        Compute a tighten-only trail multiplier based on current momentum state.
        Returns 1.0 (unchanged), 0.7 (fading), or 0.5 (exhausted).

        STRONG    → 1.0: MACD hist aligned with direction AND RSI in healthy zone
        FADING    → 0.7: MACD hist flat/shrinking OR RSI approaching extreme
        EXHAUSTED → 0.5: MACD hist reversed against direction OR RSI beyond extreme

        direction: "LONG" or "SHORT" — used to interpret MACD and RSI correctly.
        """
        rsi_val   = ind.get("rsi")
        macd_data = ind.get("macd")

        if rsi_val is None or macd_data is None:
            return 1.0

        hist = macd_data.get("hist", 0.0)

        if direction == "LONG":
            macd_aligned  = hist > 0
            macd_reversed = hist < 0
            rsi_exhausted = rsi_val > 73
            rsi_fading    = 68 <= rsi_val <= 73
        else:  # SHORT
            macd_aligned  = hist < 0
            macd_reversed = hist > 0
            rsi_exhausted = rsi_val < 27
            rsi_fading    = 27 <= rsi_val <= 32

        if rsi_exhausted or macd_reversed:
            return 0.5
        if rsi_fading or not macd_aligned:
            return 0.7
        return 1.0

    @staticmethod
    def _count_reversal_signals(ind: Dict, direction: str) -> int:
        """
        Count reversal indicators confirming a potential bounce (0–4).
        Used to gate DCA entries when smart_dca_gate is enabled.
        Each signal must be genuinely extreme — not just slightly off-centre.
        """
        count = 0
        rsi_val   = ind.get("rsi")
        bb        = ind.get("bollinger")
        macd_data = ind.get("macd")

        # Signal 1: RSI genuinely oversold/overbought
        if rsi_val is not None:
            if direction == "LONG" and rsi_val < 35:      # was 38
                count += 1
            elif direction == "SHORT" and rsi_val > 65:   # was 62
                count += 1

        # Signal 2: Bollinger Band extreme touch (price at/beyond band)
        if bb is not None:
            pct_b = bb.get("pct_b", 0.5)
            if direction == "LONG" and pct_b <= 0.05:     # was 0.08
                count += 1
            elif direction == "SHORT" and pct_b >= 0.95:  # was 0.92
                count += 1

        # Signal 3: MACD histogram actively turning (not just near zero)
        # Must be positive for LONG (turning up) or negative for SHORT (turning down)
        # Previous threshold < 0.0001 fired on almost every tick
        if macd_data is not None:
            hist = macd_data.get("hist", 0.0)
            if direction == "LONG" and hist > 0:           # was > -0.0001
                count += 1
            elif direction == "SHORT" and hist < 0:        # was < 0.0001
                count += 1

        # Signal 4: StochRSI extreme (genuinely oversold/overbought)
        # k < 20 = oversold → LONG reversal likely
        # k > 80 = overbought → SHORT reversal likely
        # Fires ~20% of time vs BB width which fired ~100% of time
        sr = ind.get("stoch_rsi")
        if sr is not None:
            k = float(sr.get("k", 50.0))
            if direction == "LONG" and k < 20:
                count += 1
            elif direction == "SHORT" and k > 80:
                count += 1

        return count

    # ------------------------------------------------------------------
    # Level prices helper (used by WS initial-state and _push_session)
    # ------------------------------------------------------------------

    def get_level_prices(self):
        """Return (tp_price, sl_price) for the current session, or (None, None).
        Uses the same formulas as _push_session() for consistency.
        Manual overrides take priority."""
        if not self._session:
            return None, None
        entry    = self._session["entry_price"]
        avg      = self._session["avg_price"]
        d        = self._session["direction"]
        leverage = self._session["leverage"]

        # TP: needs _entry_adaptive — may be empty right after restart
        computed_tp = None
        ref = self._entry_adaptive or self._adaptive
        if ref:
            tp = ref["tp_pct"]
            computed_tp = entry * (1 + tp / 100) if d == "LONG" else entry * (1 - tp / 100)

        # SL: last resort SL — does NOT need ref
        if self._breakeven_stop_price is not None:
            computed_sl = self._breakeven_stop_price
        else:
            buffer  = self._last_resort_buffer_cache
            liq_pct = (1.0 / leverage) if leverage > 0 else 0.10
            sl_pct  = liq_pct * buffer
            computed_sl = avg * (1 - sl_pct) if d == "LONG" else avg * (1 + sl_pct)

        return (
            self._override_tp_price if self._override_tp_price is not None else computed_tp,
            self._override_sl_price if self._override_sl_price is not None else computed_sl,
        )

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

    def set_scanner_type(self, scanner_type: str) -> None:
        """Set which scanner type found the current symbol — affects signal weights."""
        self._scanner_type = scanner_type

    def update_candles(self, candles: List[Dict]) -> None:
        prev_last_time = self.candles[-1]["time"] if self.candles else 0
        self.candles = candles
        self._prev_indicators = self.last_indicators   # save before overwrite
        self.last_indicators  = compute_all(candles)

        new_last_time = candles[-1]["time"] if candles else 0
        if new_last_time != prev_last_time and self._trail_activated and self._session:
            self._adjust_trail_on_candle_close()

    def _adjust_trail_on_candle_close(self) -> None:
        """
        Called on every confirmed candle close while trail is active.
        Tightens _trail_pct_mult based on current momentum — never widens.
        Only the multiplier changes; the ratcheted trail_price is untouched.
        """
        direction = self._session["direction"]
        new_mult  = self._momentum_trail_mult(self.last_indicators, direction)

        if new_mult < self._trail_pct_mult:
            old_mult = self._trail_pct_mult
            self._trail_pct_mult = new_mult
            logger.info(
                "TradingEngine: trail tightened on candle close  "
                "mult %.2f → %.2f  (rsi=%.1f  macd_hist=%.6f)",
                old_mult,
                new_mult,
                self.last_indicators.get("rsi") or 0.0,
                (self.last_indicators.get("macd") or {}).get("hist", 0.0),
            )

    # ------------------------------------------------------------------
    # Main tick — called every N seconds by the scheduler
    # ------------------------------------------------------------------

    async def tick(self, cfg: BotConfig, price: float, allow_entry: bool = True, flow_warmup: bool = False, htf_bias: str = "NEUTRAL") -> None:
        ind = self.last_indicators
        flow_summary = self._flow.summarize()
        signal = self._signal_engine.compute(self.candles, flow_summary, ind, self._prev_indicators, scanner_type=self._scanner_type)
        self.last_signal = signal

        # Broadcast signal to UI
        self._broadcast({"type": "signal", "data": {
            **signal,
            "flow_warmup":     flow_warmup,
            "warmup_strength": None,   # filled in by re-broadcast in _try_entry if warmup fires
        }})

        atr_val = ind.get("atr") or 0.0

        if self._session is None:
            if allow_entry:
                await self._try_entry(cfg, price, signal, ind, atr_val, flow_warmup=flow_warmup, htf_bias=htf_bias)
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
        flow_warmup: bool = False,
        htf_bias: str = "NEUTRAL",
    ) -> None:
        direction = signal["direction"]
        strength  = signal["strength"]

        # During flow warmup window after auto-switch, the flow component (28% weight)
        # is zero because the WebSocket just subscribed and has no trade history yet.
        # BUG FIX: previous code checked `direction != "NEUTRAL"` first, but flow=0
        # is exactly what causes direction to become NEUTRAL (composite below threshold).
        # Fix: always recompute from non-flow components during warmup, determine
        # direction from adj_composite directly — do not rely on signal["direction"].
        if flow_warmup:
            comps         = signal["components"]
            non_flow_w    = 0.72   # 1.0 - flow weight (0.28)
            adj_composite = (
                comps.get("trend",    0.0) * 0.23 +
                comps.get("momentum", 0.0) * 0.19 +
                comps.get("mean_rev", 0.0) * 0.13 +
                comps.get("rsi",      0.0) * 0.10 +
                comps.get("stoch",    0.0) * 0.07
            ) / non_flow_w

            if abs(adj_composite) >= 0.25:
                # Non-flow technicals are directional — derive direction and strength
                direction    = "LONG" if adj_composite > 0 else "SHORT"
                adj_strength = min((abs(adj_composite) - 0.25) / 0.75, 1.0)
                logger.debug(
                    "TradingEngine: flow warmup — adj composite=%.3f dir=%s strength=%.2f",
                    adj_composite, direction, adj_strength,
                )
                strength = adj_strength

                # Re-apply entry filters using the warmup-derived direction.
                # The original signal had filters_passed=False because its direction
                # was NEUTRAL (flow=0 pulled composite below threshold) — not because
                # RSI/EMA filters failed. We must re-check with the corrected direction.
                _, warmup_filters_ok, warmup_reason = self._signal_engine._apply_filters(
                    direction, adj_composite, ind
                )
                if not warmup_filters_ok:
                    logger.debug(
                        "TradingEngine: flow warmup entry blocked by filter — %s",
                        warmup_reason,
                    )
                    return

                # Re-broadcast corrected signal so UI shows warmup direction/strength
                # instead of the original NEUTRAL/0% that was sent before _try_entry.
                self._broadcast({"type": "signal", "data": {
                    **signal,
                    "direction":       direction,
                    "strength":        round(adj_strength, 4),
                    "flow_warmup":     True,
                    "warmup_strength": round(adj_strength, 4),
                    "reason":          f"warmup: {warmup_reason}",
                }})
            else:
                # Non-flow components not directional enough — skip entry
                return

        if direction == "NEUTRAL":
            self._entry_signal_ticks = 0
            self._entry_signal_dir   = "NEUTRAL"
            return
        if strength < cfg.min_signal_strength:
            self._entry_signal_ticks = 0
            self._entry_signal_dir   = "NEUTRAL"
            return

        # filters_passed check: during warmup, already re-applied above with correct direction.
        # For normal (non-warmup) path, use original signal's filters_passed.
        if not flow_warmup and not signal["filters_passed"]:
            self._entry_signal_ticks = 0
            self._entry_signal_dir   = "NEUTRAL"
            return

        # Signal persistence filter: require the same directional signal to hold
        # for N consecutive ticks before allowing entry. A signal that appears for
        # one tick and disappears is likely noise. N scales with timeframe so that
        # on 1m the filter is 3 seconds and on 15m it is 45 seconds.
        persist_needed = max(3, cfg.tf_minutes * 3)
        if direction == self._entry_signal_dir:
            self._entry_signal_ticks += 1
        else:
            # Direction changed — reset counter and start fresh
            self._entry_signal_dir   = direction
            self._entry_signal_ticks = 1

        if self._entry_signal_ticks < persist_needed:
            logger.debug(
                "TradingEngine: entry pending — signal %s confirmed %d/%d ticks",
                direction, self._entry_signal_ticks, persist_needed,
            )
            return

        # HTF confirmation: if the higher timeframe has a clear directional bias,
        # block entries that go counter to it. "NEUTRAL" means no HTF data yet or
        # the EMAs are mixed — in that case we allow the entry through.
        if cfg.htf_filter and htf_bias != "NEUTRAL" and htf_bias != direction:
            logger.debug(
                "TradingEngine: entry blocked — HTF bias %s disagrees with signal %s",
                htf_bias, direction,
            )
            return

        # Flow confirmation gate: real-time order flow must agree with direction.
        # All other components (EMA, MACD, RSI) are historical — flow is right now.
        # LONG needs taker buyers dominant (flow > 0).
        # SHORT needs taker sellers dominant (flow < 0).
        # Skipped during flow warmup window (after auto-switch, flow window is empty).
        if not flow_warmup:
            flow_score = signal["components"].get("flow", 0.0)
            if direction == "LONG" and flow_score <= 0:
                logger.debug(
                    "TradingEngine: LONG blocked — flow negative (%.3f), sellers dominant",
                    flow_score,
                )
                return
            if direction == "SHORT" and flow_score >= 0:
                logger.debug(
                    "TradingEngine: SHORT blocked — flow positive (%.3f), buyers dominant",
                    flow_score,
                )
                return

        # Stop cooldown: block re-entry for N seconds after a hard stop
        if self._last_stop_time is not None:
            elapsed = time.time() - self._last_stop_time
            if elapsed < cfg.cooldown_after_stop_s:
                logger.debug(
                    "TradingEngine: entry blocked — stop cooldown %.0fs remaining",
                    cfg.cooldown_after_stop_s - elapsed,
                )
                return

        # Daily loss circuit breaker
        if cfg.max_daily_loss_usdt > 0:
            today_pnl = await get_today_pnl()
            if today_pnl < -cfg.max_daily_loss_usdt:
                logger.warning(
                    "TradingEngine: entry blocked — daily loss limit "
                    "(today=%.4f, limit=-%.4f)", today_pnl, cfg.max_daily_loss_usdt,
                )
                return

        # Validate market conditions before committing to a trade
        if atr_val <= 0:
            logger.info("TradingEngine: skipping entry — ATR unavailable")
            return
        entry_adaptive = self._compute_adaptive(atr_val, price, strength)
        if entry_adaptive["tp_pct"] < 0.4:
            logger.info(
                "TradingEngine: skipping entry — TP too tight (%.3f%% < 0.40%%)",
                entry_adaptive["tp_pct"],
            )
            await log_signal(cfg.symbol, direction, strength, signal["components"], "skip")
            return

        # Strength-based sizing: scale margin by signal strength (floored at strength_size_min)
        if cfg.strength_sizing:
            scale = max(strength, cfg.strength_size_min)
            effective_margin = cfg.margin_usdt * scale
        else:
            effective_margin = cfg.margin_usdt
        qty = self._executor.calc_qty(cfg.symbol, effective_margin, cfg.leverage, price)
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
            margin=effective_margin,
            leverage=cfg.leverage,
            entry_reason=signal["reason"],
            signal_strength=strength,
            signal_price=price,
        )
        self._session = await get_open_session()
        self._hedges = []
        self._trail_activated = False
        self._trail_price = None
        self._entry_adaptive = entry_adaptive  # locked for life of this trade
        self._last_resort_buffer_cache = cfg.last_resort_sl_buffer  # needed by first _push_session

        await log_signal(cfg.symbol, direction, strength, signal["components"], "entry")

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"OPEN {direction} @ {fill_price:.4f}  "
            f"qty={qty}  margin={effective_margin}  strength={strength:.2f}"
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
            "margin":    effective_margin,
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

        # Cache last_resort_sl_buffer so _push_session() can compute correct SL display
        self._last_resort_buffer_cache = cfg.last_resort_sl_buffer

        # ALL risk decisions use the locked entry levels — never the live ATR
        p = self._entry_adaptive

        entry_price = sess["entry_price"]

        # price_pct: movement from avg — used for hard stop, hedge trigger, DCA, PnL
        # entry_pct: movement from original entry — used only for TP arm trigger
        if direction == "LONG":
            price_pct = (price - avg_price)   / avg_price   * 100
            entry_pct = (price - entry_price) / entry_price * 100
        else:
            price_pct = (avg_price   - price) / avg_price   * 100
            entry_pct = (entry_price - price) / entry_price * 100
        pnl_pct = price_pct * leverage

        # ---- Stop loss checks -----------------------------------------------
        # Priority 1: Manual UI override
        if self._override_sl_price is not None:
            sl_hit = (
                (direction == "LONG"  and price <= self._override_sl_price) or
                (direction == "SHORT" and price >= self._override_sl_price)
            )
            if sl_hit:
                logger.warning(
                    "TradingEngine: OVERRIDE SL HIT  price=%.4f  sl=%.4f",
                    price, self._override_sl_price,
                )
                await self._emergency_close(cfg, price, pnl_pct)
                return

        # Priority 2: Breakeven stop — set after DCA recovery
        if self._breakeven_stop_price is not None:
            be_hit = (
                (direction == "LONG"  and price <= self._breakeven_stop_price) or
                (direction == "SHORT" and price >= self._breakeven_stop_price)
            )
            if be_hit:
                logger.info(
                    "TradingEngine: BREAKEVEN STOP HIT  price=%.4f  be_stop=%.4f",
                    price, self._breakeven_stop_price,
                )
                await self._close_position(cfg, price, pnl_pct, "breakeven_stop")
                return

        # Priority 3: Last resort SL — liquidation buffer, black-swan only
        # At 10× leverage liq is ~10% away; last_resort_sl_buffer=0.80 → fires at 8%.
        liq_distance_pct = (1.0 / leverage) * 100
        last_resort_pct  = liq_distance_pct * cfg.last_resort_sl_buffer
        if price_pct <= -last_resort_pct:
            logger.error(
                "TradingEngine: LAST RESORT SL HIT  price=%.4f  "
                "liq_dist=%.2f%%  buffer=%.2f  sl_pct=%.2f%%",
                price, liq_distance_pct, cfg.last_resort_sl_buffer, last_resort_pct,
            )
            await self._emergency_close(cfg, price, pnl_pct)
            return

        # ---- Take-profit / trailing TP (entry-based — never drifts with DCA) -
        if await self._check_tp(cfg, price, entry_pct, pnl_pct, direction, p):
            return

        # ---- Smart SL: signal-confirmed early exit (priority over DCA) ------
        # Fires when position has moved meaningfully against us (>= 50% of DCA
        # step) AND opposite signal confirmed for 5 consecutive ticks.
        # Using dca_step*0.5 as threshold (not min_profit_pct) prevents false
        # exits on tiny dips + momentary signal flips — position must be in a
        # real adverse move before Smart SL considers exiting.
        # 5 ticks (vs 3) gives more confirmation, reducing false positives.
        if (
            price_pct <= -(p["dca_step_pct"] * 0.5)
            and not self._trail_activated
            and not self._hedges
        ):
            opposite = "SHORT" if direction == "LONG" else "LONG"
            strong_opposite = (
                signal["direction"] == opposite
                and signal["filters_passed"]
            )
            if strong_opposite:
                self._smart_sl_ticks += 1
                if self._smart_sl_ticks >= max(5, 5 * cfg.tf_minutes):
                    logger.info(
                        "TradingEngine: SMART SL — signal %s str=%.2f "
                        "confirmed %d ticks, price_pct=%.3f%% dca=%d/%d",
                        signal["direction"], signal["strength"],
                        self._smart_sl_ticks, price_pct,
                        dca_count, cfg.max_dca,
                    )
                    self._smart_sl_ticks = 0
                    await self._close_position(cfg, price, pnl_pct, "smart_sl")
                    return
            else:
                self._smart_sl_ticks = 0
        else:
            self._smart_sl_ticks = 0

        # ---- Signal degradation exit: conviction gone while losing ----------
        # If position is losing by at least half a DCA step AND signal has been
        # NEUTRAL for 15 consecutive ticks (15 seconds), exit cleanly.
        # Using dca_step*0.5 threshold (same as Smart SL) prevents firing on
        # tiny dips. 15 ticks gives position time to develop before declaring
        # conviction lost. This complements Smart SL which handles OPPOSITE
        # signal — this handles the NEUTRAL case (neither confirms nor denies).
        if (
            price_pct <= -(p["dca_step_pct"] * 0.5)
            and not self._trail_activated
            and not self._hedges
            and not self._rescue_mode
            and signal["direction"] == "NEUTRAL"
        ):
            self._signal_degraded_ticks += 1
            if self._signal_degraded_ticks >= max(15, 15 * cfg.tf_minutes // 3):
                self._signal_degraded_ticks = 0
                if dca_count < cfg.max_dca:
                    # DCA available — rescue: lower avg, then tight trail
                    logger.info(
                        "TradingEngine: SIGNAL DEGRADATION — NEUTRAL %d ticks "
                        "price_pct=%.3f%% dca=%d/%d — executing rescue DCA",
                        max(15, 15 * cfg.tf_minutes // 3), price_pct,
                        dca_count, cfg.max_dca,
                    )
                    await self._try_rescue_dca(
                        cfg, price, direction, avg_price, qty, dca_count, atr_val
                    )
                else:
                    # All DCAs exhausted — exit immediately
                    logger.info(
                        "TradingEngine: SIGNAL DEGRADATION EXIT — NEUTRAL, "
                        "no DCA left, price_pct=%.3f%%",
                        price_pct,
                    )
                    await self._close_position(cfg, price, pnl_pct, "signal_degraded")
                return
        else:
            if not self._rescue_mode:
                self._signal_degraded_ticks = 0

        # ---- Rescue trail: signal-adaptive trail armed after rescue DCA ------
        # Once rescue mode is active, track the best price and exit on pullback.
        # Trail width adapts to current signal:
        #   Signal recovered (same dir) → loosen: 1.0 + strength (1.0×–2.0×)
        #   Signal NEUTRAL              → tight:  0.5×
        #   Signal OPPOSITE             → very tight: 0.3× (Smart SL also fires)
        # This gives the position room to run if conviction returns, while
        # exiting quickly if the signal stays gone or reverses.
        if self._rescue_mode and self._rescue_trail_price is not None:
            if signal["direction"] == direction and signal["filters_passed"]:
                rescue_mult = 1.0 + signal["strength"]   # 1.0× – 2.0×
            elif signal["direction"] == "NEUTRAL":
                rescue_mult = 0.5
            else:
                rescue_mult = 0.5   # opposite — tight but with room
            rescue_trail_pct = p["trail_pct"] * rescue_mult
            if direction == "LONG":
                if price > self._rescue_trail_price:
                    self._rescue_trail_price = price   # ratchet up
                rescue_level = self._rescue_trail_price * (1 - rescue_trail_pct / 100)
                trail_hit    = price <= rescue_level
            else:
                if price < self._rescue_trail_price:
                    self._rescue_trail_price = price   # ratchet down
                rescue_level = self._rescue_trail_price * (1 + rescue_trail_pct / 100)
                trail_hit    = price >= rescue_level
            if trail_hit:
                logger.info(
                    "TradingEngine: RESCUE TRAIL fired @ %.6f  "
                    "best=%.6f  trail_pct=%.3f%%  price_pct=%.3f%%",
                    price, self._rescue_trail_price, rescue_trail_pct, price_pct,
                )
                self._rescue_mode        = False
                self._rescue_trail_price = None
                await self._close_position(cfg, price, pnl_pct, "rescue_trail")
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
            # Force hedge open if price has moved 1.5× the hedge trigger regardless
            # of signal — at this depth the loss is significant enough that protection
            # takes priority over signal conviction.
            force_hedge = price_pct <= -(p["hedge_trigger_pct"] * 1.5)

            if signal["direction"] == direction and signal["filters_passed"] and not force_hedge:
                logger.info(
                    "TradingEngine: hedge skipped — signal agrees with main (%s)", direction
                )
                self._broadcast({"type": "notification",
                                 "text": f"Hedge skipped: signal agrees with {direction}"})
            else:
                if force_hedge and signal["direction"] == direction:
                    logger.warning(
                        "TradingEngine: hedge force-opened at %.2f%% adverse "
                        "(signal agrees but threshold 1.5× exceeded)",
                        abs(price_pct),
                    )
                await self._open_hedge(cfg, price, direction, qty)
            return

        # ---- Breakeven stop activation — only AFTER trail is armed ----------
        # Breakeven purpose: if trail is armed and price reverses hard, ensure
        # we exit near flat rather than giving back everything.
        # Do NOT arm before trail — that causes immediate flat closes after DCA.
        if (
            cfg.breakeven_stop
            and dca_count > 0
            and self._breakeven_stop_price is None
            and self._trail_activated                       # ← trail must be armed FIRST
            and price_pct >= p["min_profit_pct"]
        ):
            fee_pct = (cfg.taker_fee_pct / 100) * 2
            if direction == "LONG":
                be_price = avg_price * (1 + fee_pct)
            else:
                be_price = avg_price * (1 - fee_pct)
            self._breakeven_stop_price = be_price
            logger.info(
                "TradingEngine: breakeven stop SET @ %.6f  (avg=%.6f  trail_active=True)",
                be_price, avg_price,
            )
            self._broadcast({"type": "notification",
                             "text": f"Breakeven stop armed @ {be_price:.4f}"})
            self._push_session()

        # ---- Signal-aware exit: all DCAs used and price has recovered --------
        # Three-tier decision based on current signal direction and strength.
        if (
            dca_count > 0
            and price_pct > 0
            and not self._trail_activated
            and not self._hedges
        ):
            sig_dir      = signal.get("direction", "NEUTRAL")
            sig_strength = signal.get("strength", 0.0)
            sig_passed   = signal.get("filters_passed", False)
            strong_threshold = cfg.min_signal_strength * 1.5

            if sig_dir == direction and sig_passed:
                # Signal confirms trade direction — momentum still valid.
                # Arm the trail and let the position run rather than cutting early.
                if sig_strength >= strong_threshold:
                    # Strong confirmation: normal trail, full run.
                    logger.info(
                        "TradingEngine: DCA recovery — signal STRONG %s (%.2f) — "
                        "arming trail, letting position run",
                        sig_dir, sig_strength,
                    )
                    # Trail will be armed naturally on the next tick when
                    # _check_tp() sees entry_pct >= tp_pct. Nothing to do here
                    # except NOT closing — just return to let normal flow continue.
                else:
                    # Weak confirmation: arm trail but tighten it proactively.
                    # Position still has upside but conviction is low.
                    self._trail_pct_mult = min(self._trail_pct_mult, 0.7)
                    logger.info(
                        "TradingEngine: DCA recovery — signal WEAK %s (%.2f) — "
                        "trail tightened to %.1f×, letting position run",
                        sig_dir, sig_strength, self._trail_pct_mult,
                    )
                self._broadcast({
                    "type": "notification",
                    "text": (
                        f"DCA recovered — signal {sig_dir} ({sig_strength:.2f}) — "
                        f"trailing, not closing early"
                    ),
                })

            elif sig_dir != direction and sig_passed:
                # Signal has flipped opposite — momentum has ended or reversed.
                # Take the profit now before the reversal eats it back.
                logger.info(
                    "TradingEngine: DCA recovery — signal OPPOSITE %s vs %s — "
                    "closing now at +%.3f%% before reversal",
                    sig_dir, direction, price_pct,
                )
                await self._close_position(cfg, price, pnl_pct, "profit_first")
                return

            else:
                # Signal is NEUTRAL or filters not passed — uncertain market.
                # Arm the trail and set breakeven stop: protect capital but
                # don't force an early exit if momentum resumes.
                self._trail_pct_mult = min(self._trail_pct_mult, 0.7)
                logger.info(
                    "TradingEngine: DCA recovery — signal NEUTRAL — "
                    "arming tight trail + breakeven stop",
                )
                self._broadcast({
                    "type": "notification",
                    "text": (
                        f"DCA recovered — signal neutral — "
                        f"tight trail armed, breakeven protected"
                    ),
                })
                # Breakeven stop will be set by the existing block above on the
                # next tick when price_pct > 0 and dca_count > 0.

        # ---- DCA ------------------------------------------------------------
        if dca_count < cfg.max_dca and price_pct <= -p["dca_step_pct"] and not self._hedges:
            opposite = "SHORT" if direction == "LONG" else "LONG"

            # Gate 1: Signal must not actively disagree with main direction
            if signal["direction"] == opposite and signal["filters_passed"]:
                logger.info(
                    "TradingEngine: DCA skipped — signal disagrees (%s vs main %s)",
                    signal["direction"], direction,
                )
                self._broadcast({"type": "notification",
                                 "text": f"DCA skipped: signal disagrees ({signal['direction']} vs {direction})"})
                return  # hard return, not just else

            # Gate 1b: Require minimum signal conviction for DCA.
            # NEUTRAL signal (strength=0, no direction) means signal engine
            # has no view — do not average down without any supporting evidence.
            if signal["direction"] == "NEUTRAL" or signal["strength"] < cfg.min_signal_strength * 1.2:
                logger.info(
                    "TradingEngine: DCA skipped — no signal conviction "
                    "(dir=%s strength=%.2f)",
                    signal["direction"], signal["strength"],
                )
                self._broadcast({"type": "notification",
                                 "text": f"DCA skipped: no signal ({signal['direction']} str={signal['strength']:.2f})"})
                return

            # Gate 2: Smart DCA reversal-indicator gate
            if cfg.smart_dca_gate:
                reversal_count = self._count_reversal_signals(ind, direction)
                if reversal_count < cfg.smart_dca_signals:
                    logger.debug(
                        "TradingEngine: DCA gated — reversal signals %d/%d",
                        reversal_count, cfg.smart_dca_signals,
                    )
                    self._broadcast({"type": "notification",
                                     "text": f"DCA gated: {reversal_count}/{cfg.smart_dca_signals} reversal signals"})
                    return

            # Gate 3: Minimum time between DCAs (prevents rapid DCA burn)
            # Even if all other gates pass, enforce a cooldown between DCAs.
            now = time.time()
            min_dca_gap_s = max(p["dca_step_pct"] * 60, 120)  # at least 2 min, scales with dca_step
            if self._last_dca_time is not None:
                elapsed = now - self._last_dca_time
                if elapsed < min_dca_gap_s:
                    logger.debug(
                        "TradingEngine: DCA time-gated — %.0fs since last DCA (need %.0fs)",
                        elapsed, min_dca_gap_s,
                    )
                    return

            await self._try_dca(cfg, price, direction, avg_price, qty, dca_count, atr_val)

    # ------------------------------------------------------------------
    # Take-profit logic (trailing + fixed floor)
    # ------------------------------------------------------------------

    async def _check_tp(
        self,
        cfg: BotConfig,
        price: float,
        entry_pct: float,
        pnl_pct: float,
        direction: str,
        p: Dict[str, float],
    ) -> bool:
        """
        Returns True if position was closed.
        entry_pct: price movement from original entry price (not avg).
        Using entry_price keeps the TP arm target fixed — it never drifts
        downward when DCA lowers avg_price.
        """
        tp_pct = p["tp_pct"]
        # Apply tighten-only momentum multiplier. Floor at 0.10% so trail
        # never becomes so tight that a single tick triggers an exit.
        trail_pct = max(p["trail_pct"] * self._trail_pct_mult, 0.10)

        # TP arm: use manual override price if set, else entry-based %
        if self._override_tp_price is not None:
            tp_reached = (
                (direction == "LONG"  and price >= self._override_tp_price) or
                (direction == "SHORT" and price <= self._override_tp_price)
            )
        else:
            sess      = self._session
            dca_count = sess["dca_count"] if sess else 0
            if dca_count > 0:
                # After DCA, original entry is above avg. Using entry_pct means
                # trail requires price to reach entry + tp_pct — unreachable
                # because breakeven fires at avg + fees first.
                # Solution: arm trail from avg after DCA.
                avg_p = sess["avg_price"]
                if direction == "LONG":
                    pct_from_avg = (price - avg_p) / avg_p * 100
                else:
                    pct_from_avg = (avg_p - price) / avg_p * 100
                tp_reached = pct_from_avg >= tp_pct
            else:
                # No DCA — use entry-based TP (original behaviour)
                tp_reached = entry_pct >= tp_pct

        if tp_reached:
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
            if trail_hit:
                # Partial TP: close partial_tp_ratio fraction, keep the rest running
                if cfg.partial_tp and not self._partial_tp_done:
                    sess      = self._session
                    qty       = sess["qty"]
                    close_qty = self._executor.round_qty(
                        cfg.symbol, qty * cfg.partial_tp_ratio
                    )
                    if 0 < close_qty < qty:
                        side = "SELL" if direction == "LONG" else "BUY"
                        order = await self._executor.place_market_order(
                            cfg.symbol, side, close_qty, reduce_only=True, current_price=price
                        )
                        _oid = int(order.get("orderId", 0))
                        if _oid:
                            self._bot_close_order_ids.add(_oid)
                        fill_price = float(order.get("avgPrice") or price)
                        remain_qty    = qty - close_qty
                        remain_margin = sess["margin"] * (remain_qty / qty)
                        partial_realized = pnl_pct / 100 * (sess["margin"] * cfg.partial_tp_ratio)
                        await update_session(
                            self._session["id"],
                            qty=remain_qty,
                            margin=remain_margin,
                        )
                        self._session = await get_open_session()
                        self._partial_tp_done = True
                        # Disarm trail so it re-arms on the next TP touch
                        self._trail_activated = False
                        self._trail_price     = None
                        await update_session(self._session["id"], trail_active=0, trail_price=None)
                        msg = (
                            f"{_mode_prefix(cfg.trading_mode)}"
                            f"PARTIAL TP {direction} @ {fill_price:.4f}  "
                            f"pnl={partial_realized:+.4f}  remain={remain_qty:.6f}"
                        )
                        logger.info("TradingEngine: %s", msg)
                        self._broadcast({"type": "notification", "text": msg})
                        self._pos_log(
                            "partial_tp", direction=direction, price=fill_price,
                            pnl=round(partial_realized, 4),
                            symbol=cfg.symbol, mode=cfg.trading_mode,
                        )
                        self._push_session()
                        return False

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
        atr_val: float = 0.0,
    ) -> None:
        """DCA with 5-second adverse pressure confirmation."""
        confirmed = self._flow.check_adverse_pressure(direction, required_seconds=5.0)
        if not confirmed:
            logger.debug("TradingEngine: DCA pending — adverse pressure not confirmed yet")
            return

        self._last_dca_time = time.time()
        self._smart_sl_ticks = 0   # DCA fired — reset smart SL counter
        self._signal_degraded_ticks = 0

        # Geometric DCA sizing: multiply margin by dca_multiplier^dca_count
        dca_margin = cfg.margin_usdt * (cfg.dca_multiplier ** dca_count)
        new_qty = self._executor.calc_qty(cfg.symbol, dca_margin, cfg.leverage, price)
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
            margin=self._session["margin"] + dca_margin,
            dca_count=dca_count + 1,
        )
        self._session = await get_open_session()

        # Manual overrides no longer make sense after averaging down — clear them
        # so the engine resumes ATR-based levels for the new avg price.
        self._clear_level_overrides("dca")
        self._breakeven_stop_price = None  # will re-arm on next recovery above avg

        # Recalculate risk thresholds from current ATR at DCA time.
        # Market volatility may have changed since entry; refreshing here keeps
        # SL/dca/hedge thresholds aligned with actual conditions after each capital add.
        # tp_pct is preserved: the TP arm is anchored to entry_price and must not
        # drift when DCA lowers avg_price.
        if atr_val > 0 and price > 0:
            old_tp_pct = self._entry_adaptive.get("tp_pct")
            self._entry_adaptive = self._compute_adaptive(atr_val, price)
            if old_tp_pct is not None:
                self._entry_adaptive["tp_pct"] = old_tp_pct
            logger.info(
                "TradingEngine: entry_adaptive recalculated at DCA #%d "
                "(tp=%.3f%% sl=%.3f%%)",
                dca_count + 1,
                self._entry_adaptive["tp_pct"],
                self._entry_adaptive["hard_stop_pct"],
            )

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

    async def _try_rescue_dca(
        self,
        cfg: BotConfig,
        price: float,
        direction: str,
        avg_price: float,
        qty: float,
        dca_count: int,
        atr_val: float = 0.0,
    ) -> None:
        """
        Rescue DCA — bypasses signal gates and adverse pressure check.
        Called when signal degrades while position is losing. Lowers avg
        price then arms a tight trailing stop to minimize the eventual loss.
        """
        dca_margin = cfg.margin_usdt * (cfg.dca_multiplier ** dca_count)
        new_qty    = self._executor.calc_qty(cfg.symbol, dca_margin, cfg.leverage, price)
        side       = "BUY" if direction == "LONG" else "SELL"

        order = await self._executor.place_market_order(
            cfg.symbol, side, new_qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)

        order_id = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {
                "type": "dca", "prior_qty": qty,
                "prior_avg": avg_price, "new_qty": new_qty,
            }

        total_qty = qty + new_qty
        new_avg   = (avg_price * qty + fill_price * new_qty) / total_qty

        await update_session(
            self._session["id"],
            avg_price=new_avg,
            qty=total_qty,
            margin=self._session["margin"] + dca_margin,
            dca_count=dca_count + 1,
        )
        self._session = await get_open_session()

        self._clear_level_overrides("rescue_dca")
        self._breakeven_stop_price  = None
        self._last_dca_time         = time.time()
        self._smart_sl_ticks        = 0
        self._signal_degraded_ticks = 0

        # Recalculate ATR-based thresholds at DCA price, preserve tp_pct
        if atr_val > 0 and price > 0:
            old_tp_pct = self._entry_adaptive.get("tp_pct")
            self._entry_adaptive = self._compute_adaptive(atr_val, price)
            if old_tp_pct is not None:
                self._entry_adaptive["tp_pct"] = old_tp_pct

        # Arm rescue trail. Initialize best price with a buffer so the trail
        # doesn't fire on the very first tick due to spread/slippage.
        # Buffer = 1 DCA step in favorable direction gives the position
        # minimum room to breathe before trail can trigger.
        dca_step_pct = self._entry_adaptive.get("dca_step_pct", 0.5)
        if direction == "LONG":
            self._rescue_trail_price = fill_price * (1 - dca_step_pct / 100 * 0.5)
        else:
            self._rescue_trail_price = fill_price * (1 + dca_step_pct / 100 * 0.5)
        self._rescue_mode = True

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"RESCUE DCA #{dca_count+1} {direction} @ {fill_price:.4f}  "
            f"new_avg={new_avg:.4f}  tight trail armed"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log(
            "dca", direction=direction, price=fill_price, qty=new_qty,
            dca_n=dca_count + 1, new_avg=round(new_avg, 6),
            symbol=cfg.symbol, mode=cfg.trading_mode,
        )
        self._push_session()

        await log_signal(cfg.symbol, direction, 0.0, {}, "rescue_dca")
        await notify(cfg.discord_webhook, "TRADE_DCA", {
            "symbol":       cfg.symbol,
            "direction":    direction,
            "level":        dca_count + 1,
            "price":        fill_price,
            "new_avg":      new_avg,
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
            margin=self._session["margin"],  # full accumulated margin incl. DCA
        )
        await update_session(
            self._session["id"],
            hedge_count=self._session["hedge_count"] + 1,
        )
        self._session = await get_open_session()
        self._hedges = await get_open_hedges(self._session["id"])

        # Overrides are no longer valid once a hedge changes the risk picture
        self._clear_level_overrides("hedge_open")

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
                # PnL = (exit - entry) × qty (futures identity; margin/leverage cancel)
                if h_dir == "LONG":
                    h_pnl = (fill_price - h_price) * h_qty
                else:
                    h_pnl = (h_price - fill_price) * h_qty
                await close_hedge(hedge["id"], round(h_pnl, 4))
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

            # ── Recovery close: main has recovered past fee-breakeven ────
            # Use min_profit_pct (≥0.10%) rather than 0% so the main has
            # genuinely covered fees before protection is removed.
            elif main_price_pct >= p["min_profit_pct"]:
                # If signal still strongly confirms the hedge direction, defer one
                # tick — the recovery may be a wick, not a genuine reversal.
                if (
                    signal.get("direction") == h_dir
                    and signal.get("filters_passed", False)
                    and signal.get("strength", 0.0) >= cfg.min_signal_strength * 1.2
                ):
                    logger.debug(
                        "TradingEngine: recovery close deferred — signal still %s (hedge dir)",
                        h_dir,
                    )
                    continue

                side = "SELL" if h_dir == "LONG" else "BUY"
                order = await self._executor.place_market_order(
                    cfg.symbol, side, h_qty, close_hedge=True, current_price=price
                )
                fill_price = float(order.get("avgPrice") or price)
                if h_dir == "LONG":
                    h_pnl = (fill_price - h_price) * h_qty
                else:
                    h_pnl = (h_price - fill_price) * h_qty
                await close_hedge(hedge["id"], round(h_pnl, 4))
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
        self._closing = True
        try:
            await self._close_position_inner(cfg, price, pnl_pct, reason)
        finally:
            self._closing = False

    async def _close_position_inner(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
        reason: str,
    ) -> None:
        sess      = self._session
        direction = sess["direction"]
        qty       = sess["qty"]
        margin    = sess["margin"]
        leverage  = sess["leverage"]

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
                h_pnl = (fill - hedge["entry_price"]) * hedge["qty"]
            else:
                h_pnl = (hedge["entry_price"] - fill) * hedge["qty"]
            await close_hedge(hedge["id"], round(h_pnl, 4))
            total_hedge_pnl += h_pnl
        self._hedges = []

        # Close main
        side = "SELL" if direction == "LONG" else "BUY"
        order = await self._executor.place_market_order(
            cfg.symbol, side, qty, reduce_only=True, current_price=price
        )
        _oid = int(order.get("orderId", 0))
        if _oid:
            self._bot_close_order_ids.add(_oid)
        fill_price = float(order.get("avgPrice") or price)

        # Recalculate PnL from the actual fill price, not the tick mark price.
        # pnl_pct was computed from the last WS price tick in _manage_position();
        # fill_price is the true exchange-confirmed execution price.
        avg_price = sess["avg_price"]
        if direction == "LONG":
            actual_pnl_pct = (fill_price - avg_price) / avg_price * 100 * leverage
        else:
            actual_pnl_pct = (avg_price - fill_price) / avg_price * 100 * leverage

        # Deduct exit taker fee from realized PnL.
        fees = self._estimate_fees(qty, fill_price, cfg.taker_fee_pct)
        realized_pnl = actual_pnl_pct / 100 * margin - fees
        await close_session(sess["id"], round(realized_pnl, 4), reason,
                            exit_price=fill_price)

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"CLOSE {direction} @ {fill_price:.4f}  "
            f"pnl={realized_pnl:+.4f} USDT ({actual_pnl_pct:+.2f}%)  reason={reason}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("close", direction=direction, price=fill_price,
                      pnl=round(realized_pnl, 4), pnl_pct=round(actual_pnl_pct, 2),
                      reason=reason, symbol=cfg.symbol, mode=cfg.trading_mode)

        await log_signal(cfg.symbol, direction, 0.0, {}, "close")
        await notify(cfg.discord_webhook, "TRADE_CLOSE", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "pnl":       realized_pnl,
            "pnl_pct":   actual_pnl_pct,
            "reason":    reason,
            "trading_mode": cfg.trading_mode,
        })

        self._session                = None
        self._hedges                 = []
        self._trail_activated        = False
        self._trail_price            = None
        self._trail_pct_mult         = 1.0
        self._entry_adaptive         = {}
        self._override_tp_price      = None
        self._override_sl_price      = None
        self._breakeven_stop_price   = None
        self._partial_tp_done        = False
        self._last_stop_time         = None   # clear cooldown on normal close
        self._bot_close_order_ids.clear()
        self._last_dca_time          = None
        self._smart_sl_ticks         = 0
        self._signal_degraded_ticks  = 0
        self._entry_signal_ticks = 0
        self._entry_signal_dir   = "NEUTRAL"
        self._rescue_mode        = False
        self._rescue_trail_price = None
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
        if self._hedges:
            hedge_dir = self._hedges[0]["direction"]
            sig       = self.last_signal

            # Condition A: signal actively confirms hedge direction
            signal_confirms = (
                sig.get("direction") == hedge_dir
                and sig.get("strength", 0.0) >= cfg.min_signal_strength
                and sig.get("filters_passed", False)
            )
            # Condition B: main exhausted its full DCA budget before stopping out
            all_dcas_used = self._session["dca_count"] >= cfg.max_dca

            if signal_confirms and all_dcas_used:
                await self._hard_stop_promote_hedge(cfg, price, pnl_pct)
            else:
                if not signal_confirms:
                    logger.info(
                        "TradingEngine: hedge promotion skipped — "
                        "signal=%s str=%.2f (need %s @ min %.2f)",
                        sig.get("direction", "?"), sig.get("strength", 0.0),
                        hedge_dir, cfg.min_signal_strength,
                    )
                if not all_dcas_used:
                    logger.info(
                        "TradingEngine: hedge promotion skipped — "
                        "DCAs not exhausted (%d of %d used)",
                        self._session["dca_count"], cfg.max_dca,
                    )
                await self._close_position(cfg, price, pnl_pct, "hard_stop")
        else:
            await self._close_position(cfg, price, pnl_pct, "hard_stop")

        # Discord fires AFTER close — only notify if close actually completed
        await notify(cfg.discord_webhook, "HARD_STOP", {
            "symbol":  cfg.symbol,
            "price":   price,
            "pnl_pct": pnl_pct,
            "paper":   cfg.paper_mode,
        })

        # Arm stop cooldown — prevents immediate re-entry after a hard stop.
        # Cleared by _close_position_inner on the next normal TP close.
        self._last_stop_time = time.time()

    async def _hard_stop_promote_hedge(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
    ) -> None:
        """
        Main position hit hard stop while a hedge is open.

        Instead of closing everything:
          1. Close main Binance position.
          2. Close 50% of hedge on Binance (lock in partial profit).
          3. Promote remaining 50% to a new main session with full DCA budget.

        The 50% close reduces exposure before the new main starts, so even
        if max_dca DCAs fire on the promoted half the total size stays bounded.

        Edge case: if hedge qty == min_qty it cannot be halved — 100% is promoted.

        Example:
          Main LONG $10 → 2×DCA → $30 total → hedge SHORT $30 opened
          Main hits hard SL →
            close main (loss),
            close SHORT 50% at profit,
            new MAIN SHORT 50% qty / $15 margin, dca_count=0
        """
        self._closing = True
        try:
            sess      = self._session
            direction = sess["direction"]
            qty       = sess["qty"]
            margin    = sess["margin"]

            # ── 1. Close main Binance position ─────────────────────────
            side  = "SELL" if direction == "LONG" else "BUY"
            order = await self._executor.place_market_order(
                cfg.symbol, side, qty, reduce_only=True, current_price=price
            )
            _oid = int(order.get("orderId", 0))
            if _oid:
                self._bot_close_order_ids.add(_oid)
            fill_price = float(order.get("avgPrice") or price)

            # Recalculate from actual fill price
            avg_p = sess["avg_price"]
            if direction == "LONG":
                actual_pnl_pct = (fill_price - avg_p) / avg_p * 100 * leverage
            else:
                actual_pnl_pct = (avg_p - fill_price) / avg_p * 100 * leverage

            fees         = self._estimate_fees(qty, fill_price, cfg.taker_fee_pct)
            realized_pnl = actual_pnl_pct / 100 * margin - fees

            # ── 2. Close main DB session ────────────────────────────────
            await close_session(sess["id"], round(realized_pnl, 4), "hard_stop",
                                exit_price=fill_price)
            logger.info(
                "TradingEngine: HARD STOP main CLOSE %s @ %.4f  pnl=%+.4f",
                direction, fill_price, realized_pnl,
            )
            await notify(cfg.discord_webhook, "TRADE_CLOSE", {
                "symbol":    cfg.symbol,
                "direction": direction,
                "price":     fill_price,
                "pnl":       realized_pnl,
                "pnl_pct":   actual_pnl_pct,
                "reason":    "hard_stop",
                "trading_mode": cfg.trading_mode,
            })

            # Take the primary hedge; close any extras on Binance (shouldn't happen)
            primary_hedge = self._hedges[0]
            for extra in self._hedges[1:]:
                h_side = "SELL" if extra["direction"] == "LONG" else "BUY"
                await self._executor.place_market_order(
                    cfg.symbol, h_side, extra["qty"],
                    close_hedge=True, current_price=price,
                )
                await close_hedge(extra["id"], 0.0)

            h_dir      = primary_hedge["direction"]
            h_qty      = primary_hedge["qty"]
            h_margin   = primary_hedge["margin"]
            h_entry    = primary_hedge["entry_price"]

            if h_dir == "LONG":
                h_pnl_pct = (price - h_entry) / h_entry * 100
            else:
                h_pnl_pct = (h_entry - price) / h_entry * 100

            # ── 3. Close 50% of hedge on Binance ───────────────────────
            # Floor to step size; if result < min_qty promote 100% instead.
            half_qty = self._executor.round_qty(cfg.symbol, h_qty * 0.5)
            if half_qty <= 0:
                half_qty = 0.0  # skip partial close, promote full position

            if half_qty > 0:
                h_close_side = "SELL" if h_dir == "LONG" else "BUY"
                await self._executor.place_market_order(
                    cfg.symbol, h_close_side, half_qty,
                    close_hedge=True, current_price=price,
                )
                half_pnl = h_pnl_pct / 100 * half_qty * h_entry
                logger.info(
                    "TradingEngine: hedge partial close 50%% qty=%.6f  pnl=%+.4f",
                    half_qty, half_pnl,
                )
            else:
                half_pnl = 0.0

            # Remaining qty and proportional margin for the promoted session
            keep_qty    = h_qty - half_qty          # exact remainder (both step-aligned)
            keep_margin = h_margin * (keep_qty / h_qty) if h_qty > 0 else h_margin

            # ── 4. Mark hedge DB record closed ──────────────────────────
            # Full PnL attributed here; ongoing position tracked by new session.
            full_h_pnl = h_pnl_pct / 100 * h_qty * h_entry
            await close_hedge(primary_hedge["id"], round(full_h_pnl, 4))

            # ── 5. Create new main session for the promoted half ────────
            new_session_id = await create_session(
                symbol=cfg.symbol,
                direction=h_dir,
                entry_price=h_entry,
                qty=keep_qty,
                margin=round(keep_margin, 4),
                leverage=sess["leverage"],
                entry_reason="hedge_promoted",
                signal_strength=0.0,
                signal_price=h_entry,
            )
            # dca_count starts at 0 — full DCA budget available on the smaller base.
            # (No explicit update needed; create_session defaults to dca_count=0.)
            _ = new_session_id  # id not needed further

            # ── 6. Update engine state ──────────────────────────────────
            self._session              = await get_open_session()
            self._hedges               = []
            self._trail_activated      = False
            self._trail_price          = None
            self._trail_pct_mult       = 1.0
            # Seed entry_adaptive from current ATR so TP/SL are immediately active
            self._entry_adaptive         = self._adaptive or {}
            self._override_tp_price      = None
            self._override_sl_price      = None
            self._breakeven_stop_price   = None
            self._pending_fills.clear()  # orphaned fill trackers from old main session

            partial_note = f"50% closed @ {price:.4f}" if half_qty > 0 else "100% promoted (min qty)"
            msg = (
                f"{_mode_prefix(cfg.trading_mode)}"
                f"HEDGE PROMOTED → MAIN {h_dir} @ {h_entry:.4f}  "
                f"qty={keep_qty}  margin={round(keep_margin, 4)}  "
                f"({partial_note})  DCA budget reset"
            )
            logger.info("TradingEngine: %s", msg)
            self._broadcast({"type": "notification", "text": msg})
            self._pos_log(
                "hedge_promoted",
                direction=h_dir,
                price=h_entry,
                qty=keep_qty,
                symbol=cfg.symbol,
                mode=cfg.trading_mode,
            )
            self._push_session()

            await log_signal(cfg.symbol, h_dir, 0.0, {}, "hedge_promoted")
            await notify(cfg.discord_webhook, "HEDGE_PROMOTED", {
                "symbol":       cfg.symbol,
                "direction":    h_dir,
                "price":        h_entry,
                "qty":          keep_qty,
                "margin":       round(keep_margin, 4),
                "partial_close_qty": half_qty,
                "trading_mode": cfg.trading_mode,
            })
        finally:
            self._closing = False

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
