"""
engine/trading.py — Scalping state machine.

State flow:
  IDLE → _try_entry() [7 gates pass] → SCALP_OPEN
  SCALP_OPEN → price >= scalp_tp_price  → _close_position (take_profit)
  SCALP_OPEN → price <= scalp_sl_price  → _emergency_close (stop_loss)
  SCALP_OPEN → elapsed >= max_hold_candles → _close_position (time_exit)
  Any state → _emergency_close() for manual/override stops
"""
import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from config import BotConfig
from database import (
    close_session,
    create_session,
    get_open_session,
    get_today_pnl,
    log_signal,
    update_session,
)
from engine.indicators import compute_all, ema as EMA, rsi as RSI, atr as ATR
from engine.orderflow import OrderFlowAnalyzer
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

        # Runtime state (re-loaded from DB on startup)
        self._session: Optional[Dict] = None

        # Set True during _close_position / _emergency_close so that concurrent
        # ACCOUNT_UPDATE events (pa=0) don't trigger a spurious external-close.
        self._closing: bool = False

        # Pending fill tracking: order_id → {"type": "entry", "prior_qty": float, "prior_avg": float}
        # Used by _on_user_data in main.py to compute correct blended average from true fill price.
        self._pending_fills: Dict[int, Dict] = {}

        # Track IDs of bot-initiated close orders so ORDER_TRADE_UPDATE
        # does not treat them as external closes.
        self._bot_close_order_ids: set = set()

        # Manual level overrides set from the UI.
        # Cleared automatically when position closes.
        self._override_tp_price: Optional[float] = None
        self._override_sl_price: Optional[float] = None

        # Stop cooldown: set to time.time() after hard stop, cleared on normal close.
        self._last_stop_time: Optional[float] = None

        # --- Scalping state ---
        # Locked at entry, restored on restart from DB.
        self._scalp_tp_price: Optional[float] = None   # absolute TP price
        self._scalp_sl_price: Optional[float] = None   # absolute SL price
        self._scalp_tp_pct:   float = 0.0              # TP% used (for display)
        self._scalp_sl_pct:   float = 0.0              # SL% used (for display)
        self._scalp_atr_pct:  float = 0.0              # ATR% at entry (for display)

        # Time-based exit: candle open-time when entry was placed
        self._entry_candle_time: int = 0

        # Cooldown: timestamp of last position exit (any reason)
        self._last_exit_time: float = 0.0

        # Optional partial-TP trail (armed at 60% of tp_pct)
        self._trail_activated: bool = False
        self._trail_price: Optional[float] = None

        # Leverage locked at switch time — set by main.py after prepare_symbol().
        self._effective_leverage: int = 1

        # Latest indicators / candles (cached each tick for broadcast)
        self.last_signal: Dict = {}
        self._scanner_type: str = "momentum"
        self.last_indicators:  Dict = {}
        self._prev_indicators: Dict = {}
        self.candles: List[Dict] = []

    def reset_for_switch(self) -> None:
        """Reset all per-symbol state when switching to a new symbol."""
        self._last_stop_time    = None
        self._trail_activated   = False
        self._trail_price       = None
        self._flow.reset()
        self.candles            = []
        self.last_indicators    = {}
        self._prev_indicators   = {}
        self.last_signal        = {}
        self._effective_leverage = 1   # re-set by main.py after prepare_symbol()
        logger.info("TradingEngine: state reset for symbol switch")

    # ------------------------------------------------------------------
    # Session broadcast helper
    # ------------------------------------------------------------------

    def _push_session(self) -> None:
        """Push current session + scalp TP/SL prices to all WS clients."""
        tp_price = self._override_tp_price or self._scalp_tp_price
        sl_price = self._override_sl_price or self._scalp_sl_price

        # Compute elapsed candles for time-exit display
        elapsed_candles = 0
        if self._session and self._entry_candle_time and self.candles:
            tf_secs = max(60, (self.candles[-1]["time"] - self.candles[-2]["time"])
                          if len(self.candles) >= 2 else 60)
            cur_candle_time = self.candles[-1]["time"]
            elapsed_candles = max(0, (cur_candle_time - self._entry_candle_time) // tf_secs)

        self._broadcast({
            "type":              "session",
            "session":           self._session,
            "hedges":            [],
            "trail_price":       self._trail_price,
            "trail_active":      self._trail_activated,
            "tp_price":          tp_price,
            "sl_price":          sl_price,
            "override_tp_price": self._override_tp_price,
            "override_sl_price": self._override_sl_price,
            "dca_prices":        [],
            # Scalp-specific display fields
            "scalp_tp_pct":      self._scalp_tp_pct,
            "scalp_sl_pct":      self._scalp_sl_pct,
            "scalp_atr_pct":     self._scalp_atr_pct,
            "elapsed_candles":   elapsed_candles,
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
    def _compute_scalp_levels(
        atr: float,
        price: float,
        direction: str,
        entry_price: float,
        cfg,
    ) -> Dict[str, float]:
        """
        Compute ATR-based scalping TP and SL percentages and prices.

        Returns dict with:
          atr_pct, tp_pct, sl_pct, rr_ratio,
          tp_price, sl_price
        All clamped to configured min/max bounds.
        R:R is verified — returns rr_ratio < cfg.min_rr_ratio if rejected.
        """
        atr_pct = (atr / price * 100) if price > 0 else 1.0

        raw_tp = atr_pct * 2.5
        raw_sl = atr_pct * 1.0

        tp_pct = max(cfg.tp_pct_min, min(cfg.tp_pct_max, raw_tp))
        sl_pct = max(cfg.sl_pct_min, min(cfg.sl_pct_max, raw_sl))

        rr_ratio = tp_pct / sl_pct if sl_pct > 0 else 0.0

        if direction == "LONG":
            tp_price = entry_price * (1 + tp_pct / 100)
            sl_price = entry_price * (1 - sl_pct / 100)
        else:
            tp_price = entry_price * (1 - tp_pct / 100)
            sl_price = entry_price * (1 + sl_pct / 100)

        return {
            "atr_pct":  atr_pct,
            "tp_pct":   tp_pct,
            "sl_pct":   sl_pct,
            "rr_ratio": rr_ratio,
            "tp_price": tp_price,
            "sl_price": sl_price,
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

    # ------------------------------------------------------------------
    # Level prices helper (used by WS initial-state and _push_session)
    # ------------------------------------------------------------------

    def get_level_prices(self):
        """Return (tp_price, sl_price) for the current session, or (None, None).
        Manual overrides take priority over locked scalp levels."""
        if not self._session:
            return None, None
        tp = self._override_tp_price if self._override_tp_price is not None else self._scalp_tp_price
        sl = self._override_sl_price if self._override_sl_price is not None else self._scalp_sl_price
        return tp, sl

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def restore_state(self) -> None:
        """Re-load open session from DB after restart and re-arm TP/SL."""
        self._session = await get_open_session()
        if self._session:
            sess = self._session

            self._scalp_tp_price      = sess.get("scalp_tp_price") or None
            self._scalp_sl_price      = sess.get("scalp_sl_price") or None
            self._scalp_tp_pct        = float(sess.get("scalp_tp_pct") or 0.0)
            self._scalp_sl_pct        = float(sess.get("scalp_sl_pct") or 0.0)
            self._scalp_atr_pct       = float(sess.get("scalp_atr_pct") or 0.0)
            self._entry_candle_time   = int(sess.get("scalp_entry_candle_time") or 0)

            # Restore optional partial-TP trail
            self._trail_activated = bool(sess.get("trail_active", 0))
            self._trail_price     = sess.get("trail_price") or None

            logger.info(
                "TradingEngine: restored session id=%d dir=%s tp=%.6f sl=%.6f "
                "entry_candle=%d trail=%s",
                sess["id"],
                sess["direction"],
                self._scalp_tp_price or 0.0,
                self._scalp_sl_price or 0.0,
                self._entry_candle_time,
                self._trail_activated,
            )

    # ------------------------------------------------------------------
    # Candle feed
    # ------------------------------------------------------------------

    def set_scanner_type(self, scanner_type: str) -> None:
        """Set which scanner type found the current symbol — affects signal weights."""
        self._scanner_type = scanner_type

    def update_candles(self, candles: List[Dict]) -> None:
        self.candles          = candles
        self._prev_indicators = self.last_indicators
        self.last_indicators  = compute_all(candles)

    # ------------------------------------------------------------------
    # Main tick — called every N seconds by the scheduler
    # ------------------------------------------------------------------

    async def tick(self, cfg: BotConfig, price: float, allow_entry: bool = True, entry_block_reason: str = "", flow_warmup: bool = False, htf_bias: str = "NEUTRAL") -> None:
        ind          = self.last_indicators
        flow_summary = self._flow.summarize()

        # Build a lightweight signal dict for gate broadcast / HTF checks.
        # Direction is derived from EMA stack in Gate 3 of _try_entry,
        # so here we only compute a simple composite for the UI signal broadcast.
        ema9  = ind.get("ema9")
        ema21 = ind.get("ema21")
        ema50 = ind.get("ema50")
        if ema9 and ema21 and ema50:
            if ema9 > ema21 > ema50 and price > ema9:
                sig_dir = "LONG"
            elif ema9 < ema21 < ema50 and price < ema9:
                sig_dir = "SHORT"
            else:
                sig_dir = "NEUTRAL"
        else:
            sig_dir = "NEUTRAL"

        signal = {
            "direction": sig_dir,
            "strength": 0.5,
            "components": {"flow": flow_summary.get("score", 0.0)},
            "composite": flow_summary.get("score", 0.0),
            "filters_passed": True,
            "reason": "",
        }
        self.last_signal = signal

        self._broadcast({"type": "signal", "data": {
            **signal,
            "flow_warmup": flow_warmup,
        }})

        atr_val = ind.get("atr") or 0.0

        if self._session is None:
            if allow_entry:
                await self._try_entry(cfg, price, signal, ind, atr_val, flow_warmup=flow_warmup, htf_bias=htf_bias)
            else:
                self._broadcast({"type": "entry_blocked", "reason": entry_block_reason or "Trading paused — press Start to enable entries", "gates": {}})
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
        """
        7-gate scalping entry. All gates are evaluated and broadcast every call
        so the UI always shows current pass/fail status.
        Entry is ONLY on a closed candle — never mid-candle.
        """
        def _waiting(reason: str) -> None:
            self._broadcast({"type": "entry_blocked", "reason": reason, "gates": {}})

        # ── Pre-filters (broadcast reason, gates stay —) ──────────────────
        n = len(self.candles)
        if n < 60:
            _waiting(f"Warming up: {n}/60 candles loaded")
            return
        if atr_val <= 0:
            _waiting("Waiting for ATR to initialise")
            return

        direction = signal.get("direction", "NEUTRAL")
        if direction == "NEUTRAL":
            _waiting("No directional signal (EMA stack flat)")
            return

        c = self.candles[-2]   # last CLOSED candle
        high_  = c["high"];  low_ = c["low"]
        open_  = c["open"];  close_ = c["close"]
        full_range = high_ - low_
        if full_range <= 0:
            _waiting("Zero-range candle — skipping")
            return

        body       = abs(close_ - open_)
        upper_wick = high_ - max(open_, close_)
        lower_wick = min(open_, close_) - low_
        vol_last   = c["volume"]
        closes     = [c2["close"] for c2 in self.candles]

        # ── Pre-filter reasons (block entry but don't alter gate display) ─
        pre_block_reason = ""
        elapsed_since_exit = time.time() - self._last_exit_time
        if elapsed_since_exit < cfg.entry_cooldown_s:
            remaining = cfg.entry_cooldown_s - elapsed_since_exit
            pre_block_reason = f"Cooldown: {remaining:.0f}s remaining"
        elif cfg.htf_filter and htf_bias not in ("NEUTRAL", direction):
            pre_block_reason = f"HTF filter: bias {htf_bias} ≠ signal {direction}"
        elif cfg.max_daily_loss_usdt > 0:
            pass  # checked async below

        # ── Evaluate all 7 gates unconditionally ─────────────────────────
        gates: Dict[str, tuple] = {}

        # Gate 1 — Candle structure
        body_ratio = body / full_range
        if body_ratio < cfg.min_body_ratio:
            gates["CANDLE STRUCTURE"] = (False, f"body {body_ratio:.0%} < {cfg.min_body_ratio:.0%}")
        elif direction == "LONG" and close_ <= open_:
            gates["CANDLE STRUCTURE"] = (False, "bearish candle for LONG")
        elif direction == "LONG" and upper_wick / full_range > 0.35:
            gates["CANDLE STRUCTURE"] = (False, f"upper wick {upper_wick/full_range:.0%} > 35%")
        elif direction == "SHORT" and close_ >= open_:
            gates["CANDLE STRUCTURE"] = (False, "bullish candle for SHORT")
        elif direction == "SHORT" and lower_wick / full_range > 0.35:
            gates["CANDLE STRUCTURE"] = (False, f"lower wick {lower_wick/full_range:.0%} > 35%")
        else:
            gates["CANDLE STRUCTURE"] = (True, f"body {body_ratio:.0%}")

        # Gate 2 — Volume
        vol_window = [c2["volume"] for c2 in self.candles[-22:-2]]
        if len(vol_window) >= 10:
            vol_avg   = sum(vol_window) / len(vol_window)
            vol_ratio = vol_last / vol_avg if vol_avg > 0 else 0.0
            if vol_ratio < cfg.min_vol_ratio:
                gates["VOLUME"] = (False, f"{vol_ratio:.2f}× < {cfg.min_vol_ratio:.2f}×")
            else:
                gates["VOLUME"] = (True, f"{vol_ratio:.1f}×")
        else:
            gates["VOLUME"] = (False, "not enough history")

        # Gate 3 — EMA stack
        ema9  = EMA(closes, 9)
        ema21 = EMA(closes, 21)
        ema50 = EMA(closes, 50)
        if ema9 is None or ema21 is None or ema50 is None:
            gates["EMA STACK"] = (False, "indicator not ready")
        elif direction == "LONG":
            if ema9 > ema21 > ema50 and price > ema9:
                gates["EMA STACK"] = (True, f"e9={ema9:g}")
            else:
                gates["EMA STACK"] = (False, f"e9={ema9:g} e21={ema21:g} e50={ema50:g}")
        else:
            if ema9 < ema21 < ema50 and price < ema9:
                gates["EMA STACK"] = (True, f"e9={ema9:g}")
            else:
                gates["EMA STACK"] = (False, f"e9={ema9:g} e21={ema21:g} e50={ema50:g}")

        # Gate 4 — Order flow
        flow_data  = self._flow.summarize()
        flow_score = flow_data.get("score", 0.0)
        flow_count = flow_data.get("trade_count", 0)
        if flow_count < 10:
            gates["ORDER FLOW"] = (False, f"only {flow_count} trades (need 10)")
        elif direction == "LONG" and flow_score < cfg.min_flow_score:
            gates["ORDER FLOW"] = (False, f"score {flow_score:+.2f} < +{cfg.min_flow_score:.2f}")
        elif direction == "SHORT" and flow_score > -cfg.min_flow_score:
            gates["ORDER FLOW"] = (False, f"score {flow_score:+.2f} > -{cfg.min_flow_score:.2f}")
        else:
            gates["ORDER FLOW"] = (True, f"{flow_score:+.2f}")

        # Gate 5 — RSI zone
        rsi_val = RSI(closes, 14)
        if rsi_val is None:
            gates["RSI ZONE"] = (False, "indicator not ready")
        elif direction == "LONG" and not (35 <= rsi_val <= 75):
            gates["RSI ZONE"] = (False, f"RSI {rsi_val:.1f} outside 35–75")
        elif direction == "SHORT" and not (25 <= rsi_val <= 65):
            gates["RSI ZONE"] = (False, f"RSI {rsi_val:.1f} outside 25–65")
        else:
            gates["RSI ZONE"] = (True, f"{rsi_val:.1f}")

        # Gate 6 — ATR range
        atr_pct = (atr_val / price * 100) if price > 0 else 0.0
        if atr_pct < 0.15:
            gates["ATR RANGE"] = (False, f"{atr_pct:.3f}% < 0.15%")
        elif atr_pct > 3.0:
            gates["ATR RANGE"] = (False, f"{atr_pct:.3f}% > 3.0%")
        else:
            gates["ATR RANGE"] = (True, f"{atr_pct:.2f}%")

        # Gate 7 — R:R ratio
        levels   = self._compute_scalp_levels(atr_val, price, direction, price, cfg)
        rr_ratio = levels["rr_ratio"]
        if rr_ratio < cfg.min_rr_ratio:
            gates["R:R RATIO"] = (False, f"{rr_ratio:.2f} < {cfg.min_rr_ratio:.2f}")
        else:
            gates["R:R RATIO"] = (True, f"{rr_ratio:.2f}:1")

        # ── Broadcast full gate status ────────────────────────────────────
        all_passed = all(v[0] for v in gates.values())
        failed_gate = next((k for k, v in gates.items() if not v[0]), None)

        if pre_block_reason:
            broadcast_reason = pre_block_reason
            all_passed = False
        elif failed_gate:
            broadcast_reason = f"Entry blocked: {failed_gate} — {gates[failed_gate][1]}"
        else:
            broadcast_reason = f"All gates passed — entering {direction}"

        self._broadcast({
            "type":   "entry_blocked",
            "reason": broadcast_reason,
            "gates":  gates,
        })

        if not all_passed:
            if failed_gate == "R:R RATIO":
                await log_signal(cfg.symbol, direction, signal.get("strength", 0.0),
                                 signal.get("components", {}), "skip")
            return

        # ── Daily loss async check (only when all 7 gates pass) ───────────
        if cfg.max_daily_loss_usdt > 0:
            today_pnl = await get_today_pnl()
            if today_pnl < -cfg.max_daily_loss_usdt:
                self._broadcast({
                    "type":   "entry_blocked",
                    "reason": f"Daily loss limit reached ({today_pnl:.2f} USDT)",
                    "gates":  gates,
                })
                return

        # ── All gates passed — place order ────────────────────────────────
        effective_leverage = self._effective_leverage if self._effective_leverage > 0 else cfg.leverage

        # Fixed margin sizing — no strength scaling
        fee_factor = 1.0 + (cfg.taker_fee_pct / 100)
        fee_adjusted_margin = cfg.margin_usdt / fee_factor
        qty = self._executor.calc_qty(cfg.symbol, fee_adjusted_margin, effective_leverage, price)
        if qty <= 0:
            logger.warning("TradingEngine: qty=0, skipping entry")
            return

        side = "BUY" if direction == "LONG" else "SELL"
        order = await self._executor.place_market_order(cfg.symbol, side, qty, current_price=price)
        fill_price = float(order.get("avgPrice") or price)

        order_id = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {"type": "entry", "prior_qty": 0.0, "prior_avg": 0.0}

        # Recompute scalp levels from fill_price (accurate TP/SL prices)
        levels = self._compute_scalp_levels(atr_val, fill_price, direction, fill_price, cfg)

        await create_session(
            symbol=cfg.symbol,
            direction=direction,
            entry_price=fill_price,
            qty=qty,
            margin=cfg.margin_usdt,
            leverage=effective_leverage,
            entry_reason=f"scalp|{direction}",
            signal_strength=signal.get("strength", 0.0),
            signal_price=price,
        )
        self._session = await get_open_session()
        self._trail_activated = False
        self._trail_price     = None

        # Lock scalp levels
        self._scalp_tp_price    = levels["tp_price"]
        self._scalp_sl_price    = levels["sl_price"]
        self._scalp_tp_pct      = levels["tp_pct"]
        self._scalp_sl_pct      = levels["sl_pct"]
        self._scalp_atr_pct     = levels["atr_pct"]

        # Track candle time for time-based exit
        self._entry_candle_time = self.candles[-1]["time"] if self.candles else 0

        # Persist locked levels to DB for restart recovery
        await update_session(
            self._session["id"],
            scalp_tp_price=self._scalp_tp_price,
            scalp_sl_price=self._scalp_sl_price,
            scalp_tp_pct=self._scalp_tp_pct,
            scalp_sl_pct=self._scalp_sl_pct,
            scalp_atr_pct=self._scalp_atr_pct,
            scalp_entry_candle_time=self._entry_candle_time,
        )

        await log_signal(cfg.symbol, direction, signal.get("strength", 0.0),
                         signal.get("components", {}), "entry")

        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"SCALP {direction} @ {fill_price:.4f}  "
            f"TP={self._scalp_tp_price:.4f} (+{self._scalp_tp_pct:.2f}%)  "
            f"SL={self._scalp_sl_price:.4f} (-{self._scalp_sl_pct:.2f}%)  "
            f"R:R={rr_ratio:.2f}  ATR={atr_pct:.2f}%"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("open", direction=direction, price=fill_price, qty=qty,
                      symbol=cfg.symbol, mode=cfg.trading_mode,
                      tp=self._scalp_tp_price, sl=self._scalp_sl_price, rr=rr_ratio)
        self._push_session()

        await notify(cfg.discord_webhook, "TRADE_OPEN", {
            "symbol":    cfg.symbol,
            "direction": direction,
            "price":     fill_price,
            "margin":    cfg.margin_usdt,
            "tp_price":  self._scalp_tp_price,
            "sl_price":  self._scalp_sl_price,
            "rr_ratio":  rr_ratio,
            "trading_mode": cfg.trading_mode,
        })

    # ------------------------------------------------------------------
    # Position management — 3 exits: TP / SL / time
    # ------------------------------------------------------------------

    async def _manage_position(
        self,
        cfg: BotConfig,
        price: float,
        signal: Dict,
        ind: Dict,
        atr_val: float,
    ) -> None:
        sess      = self._session
        direction = sess["direction"]
        avg_price = sess["avg_price"]
        qty       = sess["qty"]
        leverage  = sess["leverage"]

        if direction == "LONG":
            pnl_pct = (price - avg_price) / avg_price * 100 * leverage
        else:
            pnl_pct = (avg_price - price) / avg_price * 100 * leverage

        # ── Recover locked levels on restart if missing ───────────────────
        if self._scalp_tp_price is None or self._scalp_sl_price is None:
            if atr_val > 0:
                levels = self._compute_scalp_levels(atr_val, avg_price, direction, avg_price, cfg)
                self._scalp_tp_price = levels["tp_price"]
                self._scalp_sl_price = levels["sl_price"]
                self._scalp_tp_pct   = levels["tp_pct"]
                self._scalp_sl_pct   = levels["sl_pct"]
                self._scalp_atr_pct  = levels["atr_pct"]
                logger.warning(
                    "TradingEngine: scalp levels missing — re-derived from ATR "
                    "tp=%.4f sl=%.4f", self._scalp_tp_price, self._scalp_sl_price,
                )
            else:
                return  # ATR not ready yet

        # ── Manual UI override SL ─────────────────────────────────────────
        if self._override_sl_price is not None:
            sl_hit = (
                (direction == "LONG"  and price <= self._override_sl_price) or
                (direction == "SHORT" and price >= self._override_sl_price)
            )
            if sl_hit:
                logger.warning("TradingEngine: OVERRIDE SL HIT  price=%.6f  sl=%.6f",
                               price, self._override_sl_price)
                await self._emergency_close(cfg, price, pnl_pct)
                return

        # ── Exit A — Take Profit ──────────────────────────────────────────
        tp_price = self._override_tp_price or self._scalp_tp_price

        # Optional partial-TP trail (arm at 60% of tp distance)
        if cfg.partial_tp and not self._trail_activated:
            entry = sess["entry_price"]
            tp_dist = abs(tp_price - entry)
            arm_dist = tp_dist * 0.60
            if direction == "LONG":
                arm_at = entry + arm_dist
                if price >= arm_at:
                    trail_dist = tp_dist * 0.30
                    self._trail_activated = True
                    self._trail_price     = price - trail_dist
                    await update_session(sess["id"], trail_active=1,
                                        trail_price=self._trail_price)
                    logger.info("TradingEngine: partial-TP trail armed @ %.6f  trail=%.6f",
                                price, self._trail_price)
            else:
                arm_at = entry - arm_dist
                if price <= arm_at:
                    trail_dist = tp_dist * 0.30
                    self._trail_activated = True
                    self._trail_price     = price + trail_dist
                    await update_session(sess["id"], trail_active=1,
                                        trail_price=self._trail_price)

        if self._trail_activated and self._trail_price is not None:
            trail_dist = abs(tp_price - sess["entry_price"]) * 0.30
            if direction == "LONG":
                new_trail = price - trail_dist
                if new_trail > self._trail_price:
                    self._trail_price = new_trail
                    await update_session(sess["id"], trail_price=self._trail_price)
                if price <= self._trail_price:
                    logger.info("TradingEngine: PARTIAL-TP TRAIL HIT  price=%.6f  trail=%.6f",
                                price, self._trail_price)
                    await self._close_position(cfg, price, pnl_pct, "take_profit_trail")
                    return
            else:
                new_trail = price + trail_dist
                if new_trail < self._trail_price:
                    self._trail_price = new_trail
                    await update_session(sess["id"], trail_price=self._trail_price)
                if price >= self._trail_price:
                    await self._close_position(cfg, price, pnl_pct, "take_profit_trail")
                    return
        else:
            # Hard TP: close the moment price crosses tp_price
            tp_hit = (
                (direction == "LONG"  and price >= tp_price) or
                (direction == "SHORT" and price <= tp_price)
            )
            if tp_hit:
                logger.info("TradingEngine: TAKE PROFIT HIT  price=%.6f  tp=%.6f  pnl=%.2f%%",
                            price, tp_price, pnl_pct)
                await self._close_position(cfg, price, pnl_pct, "take_profit")
                return

        # ── Exit B — Stop Loss (hard) ─────────────────────────────────────
        sl_price = self._scalp_sl_price
        sl_hit = (
            (direction == "LONG"  and price <= sl_price) or
            (direction == "SHORT" and price >= sl_price)
        )
        if sl_hit:
            logger.warning("TradingEngine: STOP LOSS HIT  price=%.6f  sl=%.6f  pnl=%.2f%%",
                           price, sl_price, pnl_pct)
            await self._emergency_close(cfg, price, pnl_pct)
            return

        # ── Exit C — Time-based flat exit ─────────────────────────────────
        if self._entry_candle_time and self.candles and len(self.candles) >= 2:
            tf_secs = max(60, self.candles[-1]["time"] - self.candles[-2]["time"])
            cur_candle_time = self.candles[-1]["time"]
            elapsed_candles = max(0, (cur_candle_time - self._entry_candle_time) // tf_secs)
            if elapsed_candles >= cfg.max_hold_candles:
                logger.info(
                    "TradingEngine: TIME EXIT — %d candles elapsed (max %d)  pnl=%.2f%%",
                    elapsed_candles, cfg.max_hold_candles, pnl_pct,
                )
                await self._close_position(cfg, price, pnl_pct, "time_exit")
                return

        # Update UI with latest position state
        self._push_session()

    # ------------------------------------------------------------------
    # Close helpers
    # ------------------------------------------------------------------

    async def _execute_close(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
        reason: str,
    ) -> None:
        """
        Inner close logic — no _closing management.
        Called by _close_position and _emergency_close which manage _closing
        themselves so the guard stays True across async sleeps/retries.
        """
        sess      = self._session
        direction = sess["direction"]
        avg_price = sess["avg_price"]
        qty       = sess["qty"]
        side      = "SELL" if direction == "LONG" else "BUY"

        # ── Place exit order ─────────────────────────────────────────
        if cfg.trading_mode == "paper":
            fill_price = price
            if direction == "LONG":
                fill_price *= (1 - cfg.paper_slippage_pct / 100)
            else:
                fill_price *= (1 + cfg.paper_slippage_pct / 100)
        else:
            order = await self._executor.place_market_order(
                cfg.symbol, side, qty, reduce_only=True, current_price=price
            )
            self._bot_close_order_ids.add(int(order.get("orderId", 0)))
            fill_price = float(order.get("avgPrice") or price)

        # ── PnL ──────────────────────────────────────────────────────
        if direction == "LONG":
            pnl_usdt = (fill_price - avg_price) * qty
        else:
            pnl_usdt = (avg_price - fill_price) * qty
        pnl_usdt -= self._estimate_fees(qty, fill_price, cfg.taker_fee_pct)

        # ── Persist + log ─────────────────────────────────────────────
        await close_session(sess["id"], pnl=pnl_usdt, reason=reason, exit_price=fill_price)
        self._pos_log(
            "close",
            direction=direction,
            price=fill_price,
            qty=qty,
            symbol=cfg.symbol,
            mode=cfg.trading_mode,
            pnl_usdt=pnl_usdt,
            reason=reason,
        )
        await notify(cfg.discord_webhook, "TRADE_CLOSE", {
            "symbol":       cfg.symbol,
            "direction":    direction,
            "price":        fill_price,
            "pnl_usdt":     pnl_usdt,
            "reason":       reason,
            "trading_mode": cfg.trading_mode,
        })

        # ── Reset scalp state ─────────────────────────────────────────
        self._session            = None
        self._scalp_tp_price     = None
        self._scalp_sl_price     = None
        self._scalp_tp_pct       = 0.0
        self._scalp_sl_pct       = 0.0
        self._scalp_atr_pct      = 0.0
        self._entry_candle_time  = 0
        self._trail_activated    = False
        self._trail_price        = None
        self._last_exit_time     = time.time()
        self._clear_level_overrides("position closed")

        # ── Notify UI ─────────────────────────────────────────────────
        sign = "+" if pnl_usdt >= 0 else ""
        msg  = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"CLOSE {direction} @ {fill_price:.4f}  "
            f"PnL={sign}{pnl_usdt:.2f} USDT  reason={reason}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._push_session()

        # ── Trigger immediate scanner run ─────────────────────────────
        try:
            import sys
            mod = sys.modules.get("__main__")
            if mod and hasattr(mod, "_force_scan"):
                mod._force_scan = True
        except Exception:
            pass

    async def _close_position(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
        reason: str,
    ) -> None:
        """Standard close: TP hit, time exit, manual reset, trail exit."""
        if self._closing:
            return
        self._closing = True
        try:
            await self._execute_close(cfg, price, pnl_pct, reason)
        except Exception:
            logger.exception("TradingEngine: _close_position failed (reason=%s)", reason)
            raise
        finally:
            self._closing = False

    async def _emergency_close(
        self,
        cfg: BotConfig,
        price: float,
        pnl_pct: float,
    ) -> None:
        """Hard stop-loss close. Holds _closing=True across retries so the
        tick loop cannot re-enter during asyncio.sleep()."""
        if self._closing:
            return
        self._closing = True  # hold True for entire duration incl. retry sleep
        try:
            sess      = self._session
            direction = sess["direction"] if sess else "?"
            sl_price  = self._scalp_sl_price or 0.0
            logger.warning(
                "TradingEngine: EMERGENCY CLOSE — stop loss hit  price=%.6f  sl=%.6f  pnl=%.2f%%",
                price, sl_price, pnl_pct,
            )
            self._last_stop_time = time.time()
            self._broadcast({
                "type": "notification",
                "text": f"STOP LOSS — closing {direction} @ {price:.4f}",
            })
            try:
                await self._execute_close(cfg, price, pnl_pct, "stop_loss")
                return  # success — skip retry
            except Exception:
                logger.exception("TradingEngine: _emergency_close first attempt failed — retrying in 2s")
            # _closing stays True during sleep — tick loop cannot re-enter
            await asyncio.sleep(2)
            try:
                await self._execute_close(cfg, price, pnl_pct, "stop_loss")
            except Exception:
                logger.exception("TradingEngine: _emergency_close retry also failed")
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
