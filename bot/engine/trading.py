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


def _find_liquidity_pool(
    candles_1m: List[Dict],
    bias: str,
    entry_price: float,
    sl_pts: float,
    cfg,
) -> float:
    """
    Find nearest equal highs (LONG) or equal lows (SHORT) in last 50 1M candles.
    Equal = two highs/lows within 0.05% of each other.
    Only looks beyond entry in trade direction.
    Returns pool price, or entry ± 2×sl_pts if none found.
    """
    tolerance_pct = 0.0005
    lookback = candles_1m[-50:] if len(candles_1m) >= 50 else candles_1m

    if bias == "LONG":
        highs = sorted(c["high"] for c in lookback if c["high"] > entry_price)
        for i in range(len(highs) - 1):
            h1, h2 = highs[i], highs[i + 1]
            if h2 > 0 and abs(h1 - h2) / h2 <= tolerance_pct:
                pool = (h1 + h2) / 2
                if (pool - entry_price) >= sl_pts * cfg.smc_min_rr:
                    return pool
        return entry_price + sl_pts * cfg.smc_min_rr
    else:
        lows = sorted((c["low"] for c in lookback if c["low"] < entry_price), reverse=True)
        for i in range(len(lows) - 1):
            l1, l2 = lows[i], lows[i + 1]
            if l2 > 0 and abs(l1 - l2) / l2 <= tolerance_pct:
                pool = (l1 + l2) / 2
                if (entry_price - pool) >= sl_pts * cfg.smc_min_rr:
                    return pool
        return entry_price - sl_pts * cfg.smc_min_rr


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

        # Breakeven stop: armed when price reaches 50% of TP distance → SL → entry
        self._breakeven_armed: bool = False

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
        self._breakeven_armed   = False
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
            "breakeven_armed":   self._breakeven_armed,
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
    def _compute_smc_levels(
        bias: str,
        entry_price: float,
        sweep_wick: float,
        candles_1m: List[Dict],
        cfg,
    ) -> Dict:
        """
        Compute SL and TP for an SMC trade.
        SL = sweep_wick ± (entry_price × smc_sl_buffer_pct / 100).
        TP = nearest equal-high/low liquidity pool, minimum smc_min_rr × SL distance.
        Returns rr_ratio=0.0 if SL > smc_max_sl_pct% of entry (trade rejected).
        """
        buf = entry_price * cfg.smc_sl_buffer_pct / 100
        if bias == "LONG":
            sl_price = sweep_wick - buf
            sl_pts   = entry_price - sl_price
        else:
            sl_price = sweep_wick + buf
            sl_pts   = sl_price - entry_price

        max_sl = entry_price * cfg.smc_max_sl_pct / 100
        if sl_pts > max_sl or sl_pts <= 0:
            return {"sl_price": sl_price, "tp_price": 0.0,
                    "sl_pts": sl_pts, "tp_pts": 0.0, "rr_ratio": 0.0}

        tp_price = _find_liquidity_pool(candles_1m, bias, entry_price, sl_pts, cfg)
        tp_pts   = abs(tp_price - entry_price)
        rr_ratio = tp_pts / sl_pts if sl_pts > 0 else 0.0

        if rr_ratio < cfg.smc_min_rr:
            tp_price = (entry_price + sl_pts * cfg.smc_min_rr
                        if bias == "LONG"
                        else entry_price - sl_pts * cfg.smc_min_rr)
            tp_pts   = abs(tp_price - entry_price)
            rr_ratio = cfg.smc_min_rr

        return {
            "sl_price": sl_price,
            "tp_price": tp_price,
            "sl_pts":   sl_pts,
            "tp_pts":   tp_pts,
            "rr_ratio": rr_ratio,
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
            self._breakeven_armed = bool(sess.get("breakeven_armed", 0))

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

    # ------------------------------------------------------------------
    # SMC strategy — static detection methods
    # ------------------------------------------------------------------

    @staticmethod
    def _get_15m_bias(candles_15m: List[Dict]) -> str:
        """
        15M bias: EMA 50 only.
        Price above EMA50 → LONG, below → SHORT.
        Within 0.10% of EMA50 → NEUTRAL (ambiguous, skip).
        """
        if len(candles_15m) < 50:
            return "NEUTRAL"
        closes = [c["close"] for c in candles_15m]
        ema50  = EMA(closes, 50)
        if ema50 is None:
            return "NEUTRAL"
        price    = closes[-1]
        diff_pct = abs(price - ema50) / ema50 * 100
        if diff_pct < 0.10:
            return "NEUTRAL"
        return "LONG" if price > ema50 else "SHORT"

    @staticmethod
    def _find_5m_zones(
        candles_5m: List[Dict],
        bias: str,
        cfg,
    ) -> List[Dict]:
        """
        Find Order Blocks and Fair Value Gaps on 5M in the bias direction.
        Returns up to 3 zones, most recent first.
        Each zone: {type, high, low, index}
        """
        if len(candles_5m) < 5 or bias == "NEUTRAL":
            return []

        zones    = []
        lookback = min(cfg.smc_ob_lookback, len(candles_5m) - 3)

        # Average body for displacement check
        bodies   = [abs(candles_5m[i]["close"] - candles_5m[i]["open"])
                    for i in range(-lookback, -1)]
        avg_body = sum(bodies) / len(bodies) if bodies else 0.0

        # ── Order Blocks ────────────────────────────────────────────────
        for i in range(-lookback, -2):
            c    = candles_5m[i]
            body = abs(c["close"] - c["open"])
            if body == 0:
                continue
            next_c = candles_5m[i + 1]
            disp   = abs(next_c["close"] - next_c["open"])
            if avg_body > 0 and disp < avg_body * cfg.smc_ob_strength_mult:
                continue
            if bias == "LONG":
                if c["close"] < c["open"] and next_c["close"] > next_c["open"]:
                    zones.append({"type": "OB", "high": c["open"],
                                  "low": c["low"], "index": i})
            else:
                if c["close"] > c["open"] and next_c["close"] < next_c["open"]:
                    zones.append({"type": "OB", "high": c["high"],
                                  "low": c["open"], "index": i})

        # ── Fair Value Gaps ──────────────────────────────────────────────
        min_gap = candles_5m[-1]["close"] * (cfg.smc_fvg_min_gap_pct / 100)
        for i in range(-lookback, -2):
            c1 = candles_5m[i]
            c3 = candles_5m[i + 2]
            if bias == "LONG":
                gap = c3["low"] - c1["high"]
                if gap >= min_gap:
                    zones.append({"type": "FVG", "high": c3["low"],
                                  "low": c1["high"], "index": i})
            else:
                gap = c1["low"] - c3["high"]
                if gap >= min_gap:
                    zones.append({"type": "FVG", "high": c1["low"],
                                  "low": c3["high"], "index": i})

        # Most recent first (highest negative index = closest to -1)
        zones.sort(key=lambda z: z["index"], reverse=True)
        return zones[:3]

    @staticmethod
    def _check_1m_trigger(
        candles_1m: List[Dict],
        zone: Dict,
        bias: str,
    ) -> Dict:
        """
        Check the last CLOSED 1M candle (candles_1m[-2]) for:
          1. Liquidity sweep (wick penetrated zone boundary)
          2. Rejection close (closed back inside/through zone)
          3. Candle strength (close position in top/bottom 40% of range)
        Returns {triggered, sweep_wick, reason}.
        """
        if len(candles_1m) < 3:
            return {"triggered": False, "sweep_wick": 0.0,
                    "reason": "not enough 1M candles"}
        c    = candles_1m[-2]
        rng  = c["high"] - c["low"]
        if rng <= 0:
            return {"triggered": False, "sweep_wick": 0.0,
                    "reason": "zero-range candle"}

        zh = zone["high"]
        zl = zone["low"]

        if bias == "LONG":
            if c["low"] >= zl:
                return {"triggered": False, "sweep_wick": c["low"],
                        "reason": f"no sweep — low {c['low']:.6g} >= zone_low {zl:.6g}"}
            if c["close"] <= zl:
                return {"triggered": False, "sweep_wick": c["low"],
                        "reason": f"no rejection — close {c['close']:.6g} <= zone_low {zl:.6g}"}
            close_pos = (c["close"] - c["low"]) / rng
            if close_pos < 0.60:
                return {"triggered": False, "sweep_wick": c["low"],
                        "reason": f"weak candle — close at {close_pos:.0%} of range (need top 40%)"}
            return {"triggered": True, "sweep_wick": c["low"],
                    "reason": "LONG trigger confirmed"}
        else:
            if c["high"] <= zh:
                return {"triggered": False, "sweep_wick": c["high"],
                        "reason": f"no sweep — high {c['high']:.6g} <= zone_high {zh:.6g}"}
            if c["close"] >= zh:
                return {"triggered": False, "sweep_wick": c["high"],
                        "reason": f"no rejection — close {c['close']:.6g} >= zone_high {zh:.6g}"}
            close_pos = (c["close"] - c["low"]) / rng
            if close_pos > 0.40:
                return {"triggered": False, "sweep_wick": c["high"],
                        "reason": f"weak candle — close at {close_pos:.0%} of range (need bottom 40%)"}
            return {"triggered": True, "sweep_wick": c["high"],
                    "reason": "SHORT trigger confirmed"}

    # ------------------------------------------------------------------
    # Main tick
    # ------------------------------------------------------------------

    async def tick(
        self,
        cfg: BotConfig,
        price: float,
        candles_5m: List[Dict],
        candles_15m: List[Dict],
        allow_entry: bool = True,
        htf_bias: str = "NEUTRAL",   # kept for UI display compat
    ) -> None:
        """Main tick — SMC 3-timeframe: 15M bias | 5M zone | 1M trigger."""
        bias_15m = self._get_15m_bias(candles_15m)
        zones_5m = (self._find_5m_zones(candles_5m, bias_15m, cfg)
                    if bias_15m != "NEUTRAL" else [])

        self.last_signal = {
            "direction":      bias_15m,
            "strength":       0.5,
            "composite":      0.0,
            "filters_passed": len(zones_5m) > 0,
            "reason":         f"15M {bias_15m} | {len(zones_5m)} zone(s) on 5M",
        }
        self._broadcast({"type": "signal", "data": self.last_signal})

        if self._session is None:
            if allow_entry:
                await self._try_entry(cfg, price, bias_15m, zones_5m,
                                      candles_5m, candles_15m)
            else:
                self._broadcast({
                    "type":   "entry_blocked",
                    "reason": "Trading paused — press Start to enable entries",
                    "gates":  {},
                })
        else:
            await self._manage_position(cfg, price)

    # ------------------------------------------------------------------
    # SMC entry
    # ------------------------------------------------------------------

    async def _try_entry(
        self,
        cfg: BotConfig,
        price: float,
        bias_15m: str,
        zones_5m: List[Dict],
        candles_5m: List[Dict],
        candles_15m: List[Dict],
    ) -> None:
        """
        SMC 3-timeframe entry.
        Gate 1: 15M bias (EMA50)
        Gate 2: 5M zone (OB or FVG) exists in bias direction
        Gate 3: 1M trigger (sweep + rejection + strength) on last closed candle
        Gate 4: SL within max_sl_pts and R:R >= smc_min_rr
        """
        def _block(reason: str, gates: dict = {}) -> None:
            self._broadcast({"type": "entry_blocked", "reason": reason,
                             "gates": gates})

        candles_1m = self.candles

        if len(candles_1m) < 10:
            _block(f"Warming up: {len(candles_1m)}/10 candles")
            return
        cooldown_left = cfg.entry_cooldown_s - (time.time() - self._last_exit_time)
        if cooldown_left > 0:
            _block(f"Cooldown: {cooldown_left:.0f}s remaining")
            return

        # ── Gate 1: 15M Bias ─────────────────────────────────────────────
        gates: Dict[str, tuple] = {}
        if bias_15m == "NEUTRAL":
            gates["15M BIAS"] = (False, "price too close to EMA50 — wait")
        else:
            if len(candles_15m) >= 50:
                e50 = EMA([c["close"] for c in candles_15m], 50)
                gates["15M BIAS"] = (True, f"{bias_15m} | EMA50={e50:.6g}")
            else:
                gates["15M BIAS"] = (True, bias_15m)

        # ── Gate 2: 5M Zone ───────────────────────────────────────────────
        if not zones_5m:
            gates["5M ZONE"] = (False,
                                f"no OB/FVG found in {bias_15m} direction")
        else:
            z = zones_5m[0]
            gates["5M ZONE"] = (True,
                                f"{z['type']} {z['low']:.6g}–{z['high']:.6g}")

        # ── Gate 3: 1M Trigger ────────────────────────────────────────────
        trigger     = {"triggered": False, "sweep_wick": 0.0,
                       "reason": "no zone to check"}
        active_zone = None
        if zones_5m:
            for zone in zones_5m:
                t = self._check_1m_trigger(candles_1m, zone, bias_15m)
                trigger = t
                if t["triggered"]:
                    active_zone = zone
                    break
        gates["1M TRIGGER"] = (trigger["triggered"], trigger["reason"])

        # ── Gate 4: SL size and R:R ───────────────────────────────────────
        levels = None
        if trigger["triggered"] and active_zone:
            levels = self._compute_smc_levels(
                bias_15m, price, trigger["sweep_wick"], candles_1m, cfg
            )
            sl_pts   = levels["sl_pts"]
            rr_ratio = levels["rr_ratio"]
            sl_pct   = sl_pts / price * 100 if price > 0 else 0.0
            max_sl_pct = cfg.smc_max_sl_pct
            if rr_ratio <= 0 or sl_pct > max_sl_pct:
                gates["SL / R:R"] = (False,
                    f"SL {sl_pct:.2f}% > max {max_sl_pct:.1f}%")
            elif rr_ratio < cfg.smc_min_rr:
                gates["SL / R:R"] = (False,
                    f"R:R {rr_ratio:.2f} < {cfg.smc_min_rr:.1f} minimum")
            else:
                gates["SL / R:R"] = (True,
                    f"SL={sl_pct:.2f}%  R:R={rr_ratio:.2f}:1")
        else:
            gates["SL / R:R"] = (False, "waiting for trigger")

        # ── Broadcast gate status ─────────────────────────────────────────
        all_passed   = all(v[0] for v in gates.values())
        failed_gate  = next((k for k, v in gates.items() if not v[0]), None)
        broadcast_reason = (
            f"All gates passed — entering {bias_15m}"
            if all_passed
            else f"Entry blocked: {failed_gate} — {gates[failed_gate][1]}"
        )
        self._broadcast({"type": "entry_blocked", "reason": broadcast_reason,
                         "gates": gates})
        if not all_passed:
            return

        # ── Daily loss check ──────────────────────────────────────────────
        if cfg.max_daily_loss_usdt > 0:
            today_pnl = await get_today_pnl()
            if today_pnl < -cfg.max_daily_loss_usdt:
                self._broadcast({
                    "type":   "entry_blocked",
                    "reason": f"Daily loss limit reached ({today_pnl:.2f} USDT)",
                    "gates":  gates,
                })
                return

        # ── Place order ───────────────────────────────────────────────────
        effective_leverage  = (self._effective_leverage
                               if self._effective_leverage > 0 else cfg.leverage)
        fee_factor          = 1.0 + (cfg.taker_fee_pct / 100)
        fee_adjusted_margin = cfg.margin_usdt / fee_factor
        qty = self._executor.calc_qty(cfg.symbol, fee_adjusted_margin,
                                      effective_leverage, price)
        if qty <= 0:
            logger.warning("TradingEngine: qty=0, skipping entry")
            return

        entry_direction = bias_15m
        if cfg.opposite_entry:
            entry_direction = "SHORT" if bias_15m == "LONG" else "LONG"

        side  = "BUY" if entry_direction == "LONG" else "SELL"
        order = await self._executor.place_market_order(
            cfg.symbol, side, qty, current_price=price
        )
        fill_price = float(order.get("avgPrice") or price)
        order_id   = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {
                "type": "entry", "prior_qty": 0.0, "prior_avg": 0.0
            }

        # Recompute levels from fill price using actual entry direction
        levels = self._compute_smc_levels(
            entry_direction, fill_price, trigger["sweep_wick"], candles_1m, cfg
        )

        await create_session(
            symbol=cfg.symbol, direction=entry_direction,
            entry_price=fill_price, qty=qty, margin=cfg.margin_usdt,
            leverage=effective_leverage,
            entry_reason=(
                f"smc|{entry_direction}|{active_zone['type']}"
                + ("|opposite" if cfg.opposite_entry else "")
            ),
            signal_strength=0.8, signal_price=price,
        )
        self._session         = await get_open_session()
        self._trail_activated = False
        self._trail_price     = None
        self._breakeven_armed = False

        self._scalp_tp_price    = levels["tp_price"]
        self._scalp_sl_price    = levels["sl_price"]
        self._scalp_tp_pct      = (abs(levels["tp_price"] - fill_price)
                                   / fill_price * 100 if fill_price else 0.0)
        self._scalp_sl_pct      = (abs(levels["sl_price"] - fill_price)
                                   / fill_price * 100 if fill_price else 0.0)
        self._scalp_atr_pct     = 0.0
        self._entry_candle_time = self.candles[-1]["time"] if self.candles else 0

        await update_session(
            self._session["id"],
            scalp_tp_price=self._scalp_tp_price,
            scalp_sl_price=self._scalp_sl_price,
            scalp_tp_pct=self._scalp_tp_pct,
            scalp_sl_pct=self._scalp_sl_pct,
            scalp_atr_pct=0.0,
            scalp_entry_candle_time=self._entry_candle_time,
            smc_zone_type=active_zone["type"],
            smc_zone_high=active_zone["high"],
            smc_zone_low=active_zone["low"],
            smc_sweep_low=trigger["sweep_wick"],
        )

        rr = levels["rr_ratio"]
        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"SMC {entry_direction} @ {fill_price:.6g}  "
            + (f"[signal={bias_15m}→FLIPPED]  " if cfg.opposite_entry else "")
            + f"TP={self._scalp_tp_price:.6g} (+{self._scalp_tp_pct:.2f}%)  "
            f"SL={self._scalp_sl_price:.6g} (-{self._scalp_sl_pct:.2f}%)  "
            f"R:R={rr:.2f}  Zone={active_zone['type']}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("open", direction=entry_direction, price=fill_price,
                      qty=qty, symbol=cfg.symbol, mode=cfg.trading_mode,
                      tp=self._scalp_tp_price, sl=self._scalp_sl_price, rr=rr)
        self._push_session()
        await notify(cfg.discord_webhook, "TRADE_OPEN", {
            "symbol": cfg.symbol, "direction": entry_direction,
            "price": fill_price, "margin": cfg.margin_usdt,
            "tp_price": self._scalp_tp_price, "sl_price": self._scalp_sl_price,
            "rr_ratio": rr, "trading_mode": cfg.trading_mode,
            "zone_type": active_zone["type"],
        })
    # ------------------------------------------------------------------
    # Position management — 3 exits: TP / SL / time
    # ------------------------------------------------------------------

    async def _manage_position(
        self,
        cfg: BotConfig,
        price: float,
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
            logger.warning(
                "TradingEngine: scalp levels missing — cannot manage position, waiting for levels"
            )
            return

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

        # ── Breakeven stop (arm when price reaches 50% of TP distance) ───
        if not self._breakeven_armed:
            entry    = sess["entry_price"]
            tp_ref   = self._override_tp_price or self._scalp_tp_price
            tp_dist  = abs(tp_ref - entry)
            half_tp  = tp_dist * 0.50
            be_hit   = (
                (direction == "LONG"  and price >= entry + half_tp) or
                (direction == "SHORT" and price <= entry - half_tp)
            )
            if be_hit and tp_dist > 0:
                self._breakeven_armed = True
                # Small buffer so SL sits just below entry (LONG) / above entry (SHORT)
                # to survive micro-wicks without immediately stopping out.
                new_sl = entry * 0.9997 if direction == "LONG" else entry * 1.0003
                self._scalp_sl_price  = new_sl
                await update_session(sess["id"], breakeven_armed=1,
                                     scalp_sl_price=self._scalp_sl_price)
                logger.info(
                    "TradingEngine: BREAKEVEN armed — SL moved to %.6f (entry %.6f)  price=%.6f",
                    new_sl, entry, price,
                )
                self._broadcast({
                    "type": "notification",
                    "text": f"Breakeven armed — SL moved to entry {entry:.4f}",
                })
                self._push_session()

        # ── Exit A — Take Profit ──────────────────────────────────────────
        tp_price = self._override_tp_price or self._scalp_tp_price

        # Optional partial-TP trail (arm at 60% of tp distance)
        if cfg.partial_tp and not self._trail_activated:
            entry = sess["entry_price"]
            tp_dist = abs(tp_price - entry)
            arm_dist = tp_dist * 0.60
            if direction == "LONG":
                arm_at = entry + arm_dist
                logger.debug(
                    "TradingEngine: trail ARM check LONG  entry=%.6f tp=%.6f "
                    "tp_dist=%.6f arm_at=%.6f price=%.6f",
                    entry, tp_price, tp_dist, arm_at, price,
                )
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
                logger.debug(
                    "TradingEngine: trail ARM check SHORT  entry=%.6f tp=%.6f "
                    "tp_dist=%.6f arm_at=%.6f price=%.6f",
                    entry, tp_price, tp_dist, arm_at, price,
                )
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
        self._breakeven_armed    = False
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
