"""
engine/trading.py — Session-Aware Pattern Strategy.

State flow:
  IDLE → _try_entry() [5 gates pass] → SESSION_OPEN
  SESSION_OPEN → price >= scalp_tp_price  → _close_position (take_profit)
  SESSION_OPEN → price <= scalp_sl_price  → _emergency_close (stop_loss)
  SESSION_OPEN → elapsed >= max_hold_candles → _close_position (time_exit)
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
    get_session_history,
    get_today_pnl,
    log_signal,
    update_session,
)
from engine.indicators import compute_all
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

        # Session state
        self._session_trades_today: Dict[str, int] = {"london": 0, "ny": 0}
        self._asian_high: float = 0.0
        self._asian_low:  float = 999_999_999.0
        self._asian_date: str   = ""   # "YYYY-MM-DD" of current asian range

        # Key levels cache (recomputed each tick)
        self._key_levels: List[Dict] = []

        # Pattern state
        self._last_pattern_candle: int = 0  # candle time of last detected pattern

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
        # Session strategy state
        self._asian_high = 0.0
        self._asian_low  = 999_999_999.0
        self._asian_date = ""
        self._session_trades_today = {"london": 0, "ny": 0}
        self._key_levels = []
        self._last_pattern_candle = 0
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
    # Session strategy — static helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _get_session(tz_offset: int = 5) -> str:
        """
        Returns current session name based on local time (PKT by default).
        Asian:  04:00–13:00 local
        London: 13:00–18:00 local
        NY:     18:00–23:00 local
        Off:    23:00–04:00 local
        """
        import datetime
        utc_now   = datetime.datetime.utcnow()
        local_now = utc_now + datetime.timedelta(hours=tz_offset)
        hour = local_now.hour
        if 4 <= hour < 13:
            return "asian"
        elif 13 <= hour < 18:
            return "london"
        elif 18 <= hour < 23:
            return "ny"
        else:
            return "off"

    @staticmethod
    def _get_local_date(tz_offset: int = 5) -> str:
        """Return current local date as YYYY-MM-DD string."""
        import datetime
        utc_now   = datetime.datetime.utcnow()
        local_now = utc_now + datetime.timedelta(hours=tz_offset)
        return local_now.strftime("%Y-%m-%d")

    @staticmethod
    def _minutes_since_session_open(session: str, tz_offset: int = 5) -> int:
        """Return minutes elapsed since session open."""
        import datetime
        session_opens = {"asian": 4, "london": 13, "ny": 18}
        if session not in session_opens:
            return 0
        utc_now   = datetime.datetime.utcnow()
        local_now = utc_now + datetime.timedelta(hours=tz_offset)
        open_hour = session_opens[session]
        open_time = local_now.replace(hour=open_hour, minute=0, second=0, microsecond=0)
        if local_now < open_time:
            open_time -= datetime.timedelta(days=1)
        return int((local_now - open_time).total_seconds() / 60)

    def _update_asian_range(self, candles_1m: List[Dict], session: str, cfg) -> None:
        """
        During Asian session: track rolling high/low from 1M candles.
        Resets at start of each new Asian session (new day).
        """
        today = self._get_local_date(cfg.session_timezone_offset)
        if today != self._asian_date:
            self._asian_date = today
            self._asian_high = 0.0
            self._asian_low  = 999_999_999.0

        if session != "asian":
            return

        for c in candles_1m[-10:]:
            if c["high"] > self._asian_high:
                self._asian_high = c["high"]
            if c["low"] < self._asian_low:
                self._asian_low = c["low"]

    @staticmethod
    def _compute_key_levels(
        candles_1d: List[Dict],
        candles_5m: List[Dict],
        asian_high: float,
        asian_low: float,
        price: float,
        cfg,
    ) -> List[Dict]:
        """
        Compute all active key levels sorted by distance to price.
        Returns: [{type, price, dist}, ...]
        """
        levels = []

        # PDH / PDL
        if len(candles_1d) >= 2:
            prev_day = candles_1d[-2]
            levels.append({"type": "PDH", "price": prev_day["high"]})
            levels.append({"type": "PDL", "price": prev_day["low"]})

        # Asian High / Low
        if asian_high > 0:
            levels.append({"type": "asian_high", "price": asian_high})
        if asian_low < 999_999_999.0:
            levels.append({"type": "asian_low", "price": asian_low})

        # Round Numbers
        interval = cfg.session_round_interval
        if interval == 0:
            if price > 10_000:
                interval = 500.0
            elif price > 1_000:
                interval = 100.0
            elif price > 100:
                interval = 50.0
            elif price > 10:
                interval = 5.0
            elif price > 1:
                interval = 1.0
            else:
                interval = 0.1

        if interval > 0:
            import math
            nearest_round = round(price / interval) * interval
            for multiplier in [-2, -1, 0, 1, 2]:
                rnd_price = nearest_round + multiplier * interval
                if rnd_price > 0:
                    levels.append({"type": "round", "price": rnd_price})

        # Previous Session High / Low (last 72 5M candles = ~6 hrs)
        if len(candles_5m) >= 72:
            prev_session = candles_5m[-144:-72]
            if prev_session:
                ps_high = max(c["high"] for c in prev_session)
                ps_low  = min(c["low"]  for c in prev_session)
                levels.append({"type": "prev_session_high", "price": ps_high})
                levels.append({"type": "prev_session_low",  "price": ps_low})

        max_dist = cfg.session_proximity_pts * 10
        result = []
        for lv in levels:
            dist = abs(lv["price"] - price)
            if dist <= max_dist:
                lv["dist"] = dist
                result.append(lv)

        result.sort(key=lambda x: x["dist"])
        return result

    @staticmethod
    def _get_historical_probability(
        session: str,
        asian_high: float,
        asian_low: float,
        price: float,
        history: List[Dict],
    ) -> Dict:
        """
        Analyze session history and return probability dict:
        {direction, probability, sample_size, reason}
        """
        if not history or len(history) < 5:
            return {"direction": "none", "probability": 0.0,
                    "sample_size": len(history) if history else 0,
                    "reason": f"insufficient history ({len(history) if history else 0} records, need 5+)"}

        if session == "london":
            hunted_high = sum(1 for h in history if h.get("london_hunted") == "asian_high")
            hunted_low  = sum(1 for h in history if h.get("london_hunted") == "asian_low")
            total       = hunted_high + hunted_low

            if total == 0:
                return {"direction": "none", "probability": 0.0,
                        "sample_size": len(history), "reason": "no hunt data yet"}

            prob_high = hunted_high / total
            prob_low  = hunted_low  / total

            if asian_high > 0 and abs(price - asian_high) < abs(price - asian_low):
                if prob_high >= 0.60:
                    return {"direction": "short", "probability": prob_high,
                            "sample_size": total,
                            "reason": f"London hunts Asian high {prob_high:.0%} of time"}
            else:
                if prob_low >= 0.60:
                    return {"direction": "long", "probability": prob_low,
                            "sample_size": total,
                            "reason": f"London hunts Asian low {prob_low:.0%} of time"}

            return {"direction": "none",
                    "probability": max(prob_high, prob_low),
                    "sample_size": total,
                    "reason": f"probability too low (high={prob_high:.0%}, low={prob_low:.0%})"}

        elif session == "ny":
            continuation = sum(1 for h in history if h.get("ny_behavior") == "continuation")
            reversal     = sum(1 for h in history if h.get("ny_behavior") == "reversal")
            total        = continuation + reversal

            if total == 0:
                return {"direction": "none", "probability": 0.0,
                        "sample_size": len(history), "reason": "no NY behavior data"}

            prob_cont = continuation / total
            prob_rev  = reversal / total

            if prob_cont >= 0.60:
                return {"direction": "continuation", "probability": prob_cont,
                        "sample_size": total,
                        "reason": f"NY continues London {prob_cont:.0%} of time"}
            elif prob_rev >= 0.60:
                return {"direction": "reversal", "probability": prob_rev,
                        "sample_size": total,
                        "reason": f"NY reverses London {prob_rev:.0%} of time"}

            return {"direction": "none", "probability": max(prob_cont, prob_rev),
                    "sample_size": total, "reason": "NY probability insufficient"}

        return {"direction": "none", "probability": 0.0,
                "sample_size": 0, "reason": f"unknown session: {session}"}

    @staticmethod
    def _check_candle_pattern(
        candles_1m: List[Dict],
        expected_direction: str,
    ) -> Dict:
        """
        Check last CLOSED candle (candles[-2]) for pin bar, engulfing, or inside bar.
        Returns {pattern, detected, sl_extreme, reason}
        """
        if len(candles_1m) < 4:
            return {"pattern": "none", "detected": False,
                    "sl_extreme": 0.0, "reason": "not enough candles"}

        c  = candles_1m[-2]
        c1 = candles_1m[-3]

        candle_range = c["high"] - c["low"]
        if candle_range <= 0:
            return {"pattern": "none", "detected": False,
                    "sl_extreme": 0.0, "reason": "zero range candle"}

        body       = abs(c["close"] - c["open"])
        body_pct   = body / candle_range
        upper_wick = c["high"] - max(c["open"], c["close"])
        lower_wick = min(c["open"], c["close"]) - c["low"]

        # ── Pin Bar ───────────────────────────────────────────────────────
        if expected_direction == "long":
            long_lower_wick = lower_wick >= candle_range * 0.60
            small_body      = body_pct <= 0.30
            close_high      = (c["close"] - c["low"]) / candle_range >= 0.60
            if long_lower_wick and small_body and close_high:
                return {"pattern": "pin_bar", "detected": True,
                        "sl_extreme": c["low"],
                        "reason": f"Bullish pin bar — lower wick {lower_wick/candle_range:.0%}"}
        else:
            long_upper_wick = upper_wick >= candle_range * 0.60
            small_body      = body_pct <= 0.30
            close_low       = (c["close"] - c["low"]) / candle_range <= 0.40
            if long_upper_wick and small_body and close_low:
                return {"pattern": "pin_bar", "detected": True,
                        "sl_extreme": c["high"],
                        "reason": f"Bearish pin bar — upper wick {upper_wick/candle_range:.0%}"}

        # ── Engulfing ─────────────────────────────────────────────────────
        c1_body_high = max(c1["open"], c1["close"])
        c1_body_low  = min(c1["open"], c1["close"])
        c_body_high  = max(c["open"],  c["close"])
        c_body_low   = min(c["open"],  c["close"])

        if expected_direction == "long":
            bullish_engulf = (
                c["close"] > c["open"] and
                c_body_low  < c1_body_low and
                c_body_high > c1_body_high
            )
            if bullish_engulf:
                return {"pattern": "engulfing", "detected": True,
                        "sl_extreme": c["low"],
                        "reason": "Bullish engulfing — strong momentum shift"}
        else:
            bearish_engulf = (
                c["close"] < c["open"] and
                c_body_high > c1_body_high and
                c_body_low  < c1_body_low
            )
            if bearish_engulf:
                return {"pattern": "engulfing", "detected": True,
                        "sl_extreme": c["high"],
                        "reason": "Bearish engulfing — strong momentum shift"}

        # ── Inside Bar Breakout ───────────────────────────────────────────
        c2 = candles_1m[-4] if len(candles_1m) >= 4 else None
        if c2:
            mother   = c1
            inside   = c2
            breakout = c

            inside_is_inside = (
                inside["high"] <= mother["high"] and
                inside["low"]  >= mother["low"]
            )
            if inside_is_inside:
                if expected_direction == "long" and breakout["close"] > mother["high"]:
                    return {"pattern": "inside_bar", "detected": True,
                            "sl_extreme": inside["low"],
                            "reason": f"Inside bar bullish breakout above {mother['high']:.6g}"}
                elif expected_direction == "short" and breakout["close"] < mother["low"]:
                    return {"pattern": "inside_bar", "detected": True,
                            "sl_extreme": inside["high"],
                            "reason": f"Inside bar bearish breakout below {mother['low']:.6g}"}

        return {"pattern": "none", "detected": False,
                "sl_extreme": 0.0,
                "reason": "no pattern detected (pin bar, engulfing, or inside bar)"}

    @staticmethod
    def _compute_session_levels(
        direction: str,
        entry_price: float,
        sl_extreme: float,
        key_levels: List[Dict],
        cfg,
    ) -> Dict:
        """
        Compute SL and TP for session trade.
        SL: sl_extreme ± cfg.session_sl_buffer_pts
        TP: next key level beyond entry with R:R >= session_min_rr, else fallback
        """
        buf = cfg.session_sl_buffer_pts

        if direction == "long":
            sl_price = sl_extreme - buf
            sl_dist  = entry_price - sl_price
        else:
            sl_price = sl_extreme + buf
            sl_dist  = sl_price - entry_price

        if sl_dist <= 0:
            return {"sl_price": sl_price, "tp_price": 0.0,
                    "sl_pts": 0.0, "tp_pts": 0.0, "rr_ratio": 0.0}

        tp_price = 0.0
        for lv in sorted(key_levels, key=lambda x: x["dist"]):
            if direction == "long" and lv["price"] > entry_price:
                tp_dist = lv["price"] - entry_price
                if tp_dist / sl_dist >= cfg.session_min_rr:
                    tp_price = lv["price"]
                    break
            elif direction == "short" and lv["price"] < entry_price:
                tp_dist = entry_price - lv["price"]
                if tp_dist / sl_dist >= cfg.session_min_rr:
                    tp_price = lv["price"]
                    break

        if tp_price == 0.0:
            if direction == "long":
                tp_price = entry_price + sl_dist * cfg.session_min_rr
            else:
                tp_price = entry_price - sl_dist * cfg.session_min_rr

        tp_pts   = abs(tp_price - entry_price)
        rr_ratio = tp_pts / sl_dist if sl_dist > 0 else 0.0

        return {
            "sl_price": sl_price,
            "tp_price": tp_price,
            "sl_pts":   sl_dist,
            "tp_pts":   tp_pts,
            "rr_ratio": rr_ratio,
        }

    # ------------------------------------------------------------------
    # Main tick
    # ------------------------------------------------------------------

    async def tick(
        self,
        cfg: BotConfig,
        price: float,
        candles_5m: List[Dict],
        candles_15m: List[Dict],
        candles_1d: List[Dict] = None,
        allow_entry: bool = True,
        htf_bias: str = "NEUTRAL",
    ) -> None:
        """Main tick — Session + Key Level + Pattern strategy."""
        if candles_1d is None:
            candles_1d = []

        candles_1m = self.candles

        session = self._get_session(cfg.session_timezone_offset)
        self._update_asian_range(candles_1m, session, cfg)

        self._key_levels = self._compute_key_levels(
            candles_1d, candles_5m,
            self._asian_high, self._asian_low,
            price, cfg,
        )

        session_display = session.upper()
        trades_left = cfg.session_max_trades - self._session_trades_today.get(session, 0)

        self.last_signal = {
            "direction":      "NEUTRAL",
            "strength":       0.5,
            "composite":      0.0,
            "filters_passed": session in ("london", "ny"),
            "reason":         (
                f"{session_display} | Asian H={self._asian_high:.4g} "
                f"L={self._asian_low:.4g} | Trades left: {trades_left}"
            ),
        }
        self._broadcast({"type": "signal", "data": self.last_signal})

        if self._session is None:
            if allow_entry:
                await self._try_entry(cfg, price, session, candles_5m, candles_1d)
            else:
                self._broadcast({
                    "type":   "entry_blocked",
                    "reason": "Trading paused — press Start to enable entries",
                    "gates":  {},
                })
        else:
            await self._manage_position(cfg, price)

    # ------------------------------------------------------------------
    # Session entry
    # ------------------------------------------------------------------

    async def _try_entry(
        self,
        cfg: BotConfig,
        price: float,
        session: str,
        candles_5m: List[Dict],
        candles_1d: List[Dict],
    ) -> None:
        """
        Session-Aware Pattern entry.
        Gate 1: Session active (London or NY, past first 5 min, trades remaining)
        Gate 2: Historical probability >= threshold
        Gate 3: Key level nearby (within session_proximity_pts)
        Gate 4: Candle pattern confirmed (pin bar, engulfing, inside bar)
        Gate 5: R:R >= session_min_rr
        """
        candles_1m = self.candles
        gates: Dict[str, tuple] = {}

        # ── Gate 1: Session Active ────────────────────────────────────────
        minutes_open = self._minutes_since_session_open(session, cfg.session_timezone_offset)

        if session == "asian":
            gates["SESSION"] = (False, "Asian session — observe only, no entries")
        elif session == "off":
            gates["SESSION"] = (False, "Off-hours — no trading")
        elif minutes_open < 5:
            gates["SESSION"] = (False, f"{session.upper()} open — waiting 5 min (spread wide)")
        else:
            trades_done = self._session_trades_today.get(session, 0)
            if trades_done >= cfg.session_max_trades:
                gates["SESSION"] = (False,
                    f"{session.upper()} max trades reached ({trades_done}/{cfg.session_max_trades})")
            else:
                remaining = cfg.session_max_trades - trades_done
                gates["SESSION"] = (True,
                    f"{session.upper()} | {minutes_open}min open | {remaining} trades left")

        # ── Gate 2: Historical Probability ───────────────────────────────
        history = await get_session_history(cfg.symbol, session, cfg.session_history_bars)
        prob    = self._get_historical_probability(
            session, self._asian_high, self._asian_low, price, history
        )

        if prob["probability"] < cfg.session_prob_threshold:
            gates["PROBABILITY"] = (
                False,
                f"{prob['probability']:.0%} < {cfg.session_prob_threshold:.0%} ({prob['reason']})"
            )
        else:
            gates["PROBABILITY"] = (
                True,
                f"{prob['probability']:.0%} → {prob['direction'].upper()} ({prob['sample_size']} samples)"
            )

        expected_dir = "none"
        if prob["direction"] in ("long", "continuation"):
            expected_dir = "long"
        elif prob["direction"] in ("short", "reversal"):
            expected_dir = "short"

        # ── Gate 3: Key Level Proximity ───────────────────────────────────
        nearby_level = None
        for lv in self._key_levels:
            if lv["dist"] <= cfg.session_proximity_pts:
                nearby_level = lv
                break

        if nearby_level is None:
            if self._key_levels:
                first = self._key_levels[0]
                gates["KEY LEVEL"] = (False,
                    f"no level within {cfg.session_proximity_pts:.0f}pts | nearest: "
                    f"{first['type']}@{first['price']:.4g} ({first['dist']:.1f}pts)")
            else:
                gates["KEY LEVEL"] = (False, "no levels computed")
        else:
            gates["KEY LEVEL"] = (True,
                f"{nearby_level['type']} @ {nearby_level['price']:.4g} "
                f"({nearby_level['dist']:.1f}pts away)")

        # ── Gate 4: Candle Pattern ────────────────────────────────────────
        pattern = {"pattern": "none", "detected": False, "sl_extreme": 0.0,
                   "reason": "waiting for pattern"}
        if expected_dir != "none" and gates.get("KEY LEVEL", (False,))[0]:
            pattern = self._check_candle_pattern(candles_1m, expected_dir)

        if pattern["detected"]:
            gates["PATTERN"] = (True,
                f"{pattern['pattern'].replace('_', ' ').title()} — {pattern['reason']}")
        else:
            gates["PATTERN"] = (False, pattern["reason"])

        # ── Gate 5: R:R Check ─────────────────────────────────────────────
        levels: Dict = {}
        if pattern["detected"] and expected_dir != "none":
            levels = self._compute_session_levels(
                expected_dir, price, pattern["sl_extreme"],
                self._key_levels, cfg,
            )
            rr = levels.get("rr_ratio", 0.0)
            if rr < cfg.session_min_rr:
                gates["R:R"] = (False, f"{rr:.2f} < {cfg.session_min_rr:.1f} minimum")
            else:
                gates["R:R"] = (True,
                    f"{rr:.2f}:1  SL={levels['sl_pts']:.1f}pts  TP→{levels['tp_price']:.4g}")
        else:
            gates["R:R"] = (False, "waiting for pattern to compute R:R")

        # ── Broadcast ─────────────────────────────────────────────────────
        all_passed  = all(v[0] for v in gates.values())
        failed_gate = next((k for k, v in gates.items() if not v[0]), None)
        broadcast_reason = (
            f"All gates passed — entering {expected_dir.upper()}"
            if all_passed
            else f"Entry blocked: {failed_gate} — {gates[failed_gate][1]}"
        )
        self._broadcast({
            "type":   "entry_blocked",
            "reason": broadcast_reason,
            "gates":  gates,
        })

        if not all_passed:
            return

        # ── Cooldown check ────────────────────────────────────────────────
        if time.time() - self._last_exit_time < cfg.entry_cooldown_s:
            return

        # ── Daily loss check ──────────────────────────────────────────────
        if cfg.max_daily_loss_usdt > 0:
            today_pnl = await get_today_pnl()
            if today_pnl < -cfg.max_daily_loss_usdt:
                return

        # ── Place order ───────────────────────────────────────────────────
        effective_leverage  = self._effective_leverage if self._effective_leverage > 0 else cfg.leverage
        fee_factor          = 1.0 + (cfg.taker_fee_pct / 100)
        fee_adjusted_margin = cfg.margin_usdt / fee_factor
        qty = self._executor.calc_qty(cfg.symbol, fee_adjusted_margin, effective_leverage, price)
        if qty <= 0:
            logger.warning("TradingEngine: qty=0, skipping entry")
            return

        entry_direction = expected_dir.upper()
        if cfg.opposite_entry:
            entry_direction = "SHORT" if entry_direction == "LONG" else "LONG"

        side  = "BUY" if entry_direction == "LONG" else "SELL"
        order = await self._executor.place_market_order(cfg.symbol, side, qty, current_price=price)
        fill_price = float(order.get("avgPrice") or price)

        order_id = int(order.get("orderId", 0))
        if order_id:
            self._pending_fills[order_id] = {"type": "entry", "prior_qty": 0.0, "prior_avg": 0.0}

        lv = self._compute_session_levels(
            entry_direction.lower(), fill_price,
            pattern["sl_extreme"], self._key_levels, cfg,
        )

        await create_session(
            symbol=cfg.symbol, direction=entry_direction,
            entry_price=fill_price, qty=qty, margin=cfg.margin_usdt,
            leverage=effective_leverage,
            entry_reason=f"session|{session}|{pattern['pattern']}|{nearby_level['type']}",
            signal_strength=prob["probability"],
            signal_price=price,
        )
        self._session         = await get_open_session()
        self._trail_activated = False
        self._trail_price     = None
        self._breakeven_armed = False

        self._scalp_tp_price    = lv["tp_price"]
        self._scalp_sl_price    = lv["sl_price"]
        self._scalp_tp_pct      = abs(lv["tp_price"] - fill_price) / fill_price * 100 if fill_price else 0.0
        self._scalp_sl_pct      = abs(lv["sl_price"] - fill_price) / fill_price * 100 if fill_price else 0.0
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
        )

        self._session_trades_today[session] = self._session_trades_today.get(session, 0) + 1

        rr  = lv["rr_ratio"]
        msg = (
            f"{_mode_prefix(cfg.trading_mode)}"
            f"SESSION {entry_direction} @ {fill_price:.6g}  "
            f"TP={self._scalp_tp_price:.6g} (+{self._scalp_tp_pct:.2f}%)  "
            f"SL={self._scalp_sl_price:.6g} (-{self._scalp_sl_pct:.2f}%)  "
            f"R:R={rr:.2f}  {session.upper()} | {pattern['pattern']} | {prob['probability']:.0%}"
        )
        logger.info("TradingEngine: %s", msg)
        self._broadcast({"type": "notification", "text": msg})
        self._pos_log("open", direction=entry_direction, price=fill_price, qty=qty,
                      symbol=cfg.symbol, mode=cfg.trading_mode,
                      tp=self._scalp_tp_price, sl=self._scalp_sl_price, rr=rr)
        self._push_session()
        await notify(cfg.discord_webhook, "TRADE_OPEN", {
            "symbol": cfg.symbol, "direction": entry_direction,
            "price": fill_price, "margin": cfg.margin_usdt,
            "tp_price": self._scalp_tp_price, "sl_price": self._scalp_sl_price,
            "rr_ratio": rr, "trading_mode": cfg.trading_mode,
            "session": session, "pattern": pattern["pattern"],
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
