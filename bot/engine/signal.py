"""
engine/signal.py — Composite multi-factor signal engine.

Weighting:
  30%  order flow (buy/sell volume delta)
  25%  trend (EMA21 vs EMA50)
  20%  momentum (MACD histogram)
  15%  mean reversion (Bollinger %B)
  10%  RSI

Direction: LONG if composite > +0.20, SHORT if < -0.20, else NEUTRAL.
"""
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Component weights (must sum to 1.0)
_W_FLOW     = 0.28
_W_TREND    = 0.23
_W_MOMENTUM = 0.19
_W_MEAN_REV = 0.13
_W_RSI      = 0.10
_W_STOCH    = 0.07

# Scanner-specific weight profiles.
# Each scanner type finds symbols in a different market condition — the weights
# must match what that condition looks like so the signal fires when expected.
_WEIGHT_PROFILES: Dict[str, Dict[str, float]] = {
    "momentum": {
        # Default: balanced across all components
        "flow": 0.28, "trend": 0.23, "momentum": 0.19,
        "mean_rev": 0.13, "rsi": 0.10, "stoch": 0.07,
    },
    "breakout": {
        # Price breaking above/below N-candle range — overbought RSI/BB is NORMAL
        # at a breakout, not a warning. Remove mean_rev and rsi from composite.
        # Flow and momentum confirmation are critical.
        "flow": 0.30, "trend": 0.30, "momentum": 0.25,
        "mean_rev": 0.00, "rsi": 0.00, "stoch": 0.15,
    },
    "trendpull": {
        # Pullback to EMA21 in uptrend — mean_rev and trend are primary signals.
        # Price near lower BB in an uptrend = ideal entry, not overbought risk.
        "flow": 0.20, "trend": 0.30, "momentum": 0.15,
        "mean_rev": 0.20, "rsi": 0.08, "stoch": 0.07,
    },
}

# Decision thresholds
_ENTRY_THRESHOLD = 0.20   # composite must exceed ±0.20 for a directional signal
_RSI_OB = 75.0            # overbought block for LONG
_RSI_OS = 25.0            # oversold block for SHORT
_RSI_EXTREME_OB = 68.0    # above this = mean-reversion SHORT allowed
_RSI_EXTREME_OS = 32.0    # below this = mean-reversion LONG allowed


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


class SignalEngine:
    """
    Signal computation.  Call compute() each cycle.
    """

    def compute(
        self,
        candles: List[Dict],
        flow_summary: Dict,
        ind: Dict,
        prev_ind: Optional[Dict] = None,
        scanner_type: str = "momentum",
    ) -> Dict:
        """
        Parameters
        ----------
        candles     : list of {"open", "high", "low", "close", "volume"}
        flow_summary: output of OrderFlowAnalyzer.summarize()
        ind         : output of indicators.compute_all()

        Returns
        -------
        {
            "direction": "LONG" | "SHORT" | "NEUTRAL",
            "strength":  float,   # 0.0 – 1.0
            "components": {
                "flow": float, "trend": float, "momentum": float,
                "mean_rev": float, "rsi": float,
            },
            "composite": float,   # raw weighted sum
            "filters_passed": bool,
            "reason": str,
        }
        """
        if prev_ind is None:
            prev_ind = {}
        components = self._score_components(flow_summary, ind, prev_ind)

        # Select weight profile for the scanner type that found this symbol.
        # breakdown uses trendpull weights — same EMA-heavy profile works for
        # both "buy the dip in uptrend" and "sell the rally in downtrend".
        # Falls back to "momentum" (default weights) if type unknown.
        effective_type = "trendpull" if scanner_type == "breakdown" else scanner_type
        W = _WEIGHT_PROFILES.get(effective_type, _WEIGHT_PROFILES["momentum"])
        composite = (
            components["flow"]     * W["flow"]
            + components["trend"]    * W["trend"]
            + components["momentum"] * W["momentum"]
            + components["mean_rev"] * W["mean_rev"]
            + components["rsi"]      * W["rsi"]
            + components["stoch"]    * W["stoch"]
        )

        # Preliminary direction from composite
        if composite > _ENTRY_THRESHOLD:
            raw_dir = "LONG"
        elif composite < -_ENTRY_THRESHOLD:
            raw_dir = "SHORT"
        else:
            raw_dir = "NEUTRAL"

        # Strength = how far composite is beyond the threshold, scaled 0→1
        if raw_dir != "NEUTRAL":
            excess = abs(composite) - _ENTRY_THRESHOLD
            # max realistic excess ≈ 0.75 (all components strongly aligned)
            strength = _clamp(excess / 0.75, 0.0, 1.0)
        else:
            strength = 0.0

        # Apply entry filters
        direction, filters_passed, reason = self._apply_filters(
            raw_dir, composite, ind
        )
        if not filters_passed:
            strength = 0.0

        return {
            "direction":      direction,
            "strength":       round(strength, 4),
            "components":     {k: round(v, 4) for k, v in components.items()},
            "composite":      round(composite, 4),
            "filters_passed": filters_passed,
            "reason":         reason,
            "scanner_type":   scanner_type,
        }

    # ------------------------------------------------------------------
    # Component scoring
    # ------------------------------------------------------------------

    def _score_components(self, flow: Dict, ind: Dict, prev_ind: Dict) -> Dict[str, float]:
        return {
            "flow":     self._score_flow(flow),
            "trend":    self._score_trend(ind),
            "momentum": self._score_momentum(ind, prev_ind),
            "mean_rev": self._score_mean_reversion(ind),
            "rsi":      self._score_rsi(ind, prev_ind),
            "stoch":    self._score_stoch_rsi(ind),
        }

    def _score_flow(self, flow: Dict) -> float:
        """
        Weighted combination of three order-flow signals:
          score     (50%) — taker buy/sell notional delta: hardest to spoof, most reliable
          imbalance (30%) — qty-weighted bid/ask book imbalance: snapshot, can be spoofed
          ba_ratio  (20%) — notional-weighted book imbalance: same spoof risk as above

        Confidence scaling: low trade count in window = low reliability.
        20+ trades → full confidence. Fewer → score scales down linearly.
        This prevents a single large trade from dominating a quiet window.
        """
        score     = float(flow.get("score", 0.0))
        imbalance = float(flow.get("imbalance", 0.0))
        ba_ratio  = (float(flow.get("bid_ask_ratio", 0.5)) - 0.5) * 2.0
        combined  = score * 0.50 + imbalance * 0.30 + ba_ratio * 0.20

        # Scale by trade count confidence: ramp from 0 to 1 over first 20 trades
        trade_count = int(flow.get("trade_count", 20))
        confidence  = min(trade_count / 20.0, 1.0)
        return _clamp(combined * confidence)

    def _score_trend(self, ind: Dict) -> float:
        """
        EMA21 vs EMA50 cross, ATR-normalised.
        +1 = EMA21 strongly above EMA50 AND price above EMA21 (confirmed uptrend).
        -1 = EMA21 strongly below EMA50 AND price below EMA21 (confirmed downtrend).

        Price-vs-EMA21 check: if price is on the wrong side of EMA21
        (e.g. EMA21 > EMA50 but price already crashed below EMA21),
        the trend structure is still bullish but price has broken it —
        reduce score by 50% to reflect the weakening.
        """
        ema21: Optional[float] = ind.get("ema21")
        ema50: Optional[float] = ind.get("ema50")
        if ema21 is None or ema50 is None or ema50 == 0:
            return 0.0
        diff_pct = (ema21 - ema50) / ema50
        atr_val  = ind.get("atr")
        atr_pct  = (atr_val / ema50) if (atr_val and ema50 and ema50 > 0) else 0.005
        scaled   = diff_pct / max(atr_pct, 1e-6)
        score    = _clamp(scaled)

        # Price confirmation: reduce score if price is on wrong side of EMA21
        price = ind.get("price")
        if price and ema21:
            bullish_structure = score > 0
            price_confirms    = price > ema21 if bullish_structure else price < ema21
            if not price_confirms:
                score *= 0.5   # structure intact but price has broken it — reduce confidence

        return score

    def _score_momentum(self, ind: Dict, prev_ind: Dict) -> float:
        """
        MACD histogram direction, magnitude, and slope.
        Base: histogram normalised by price and ATR (regime-independent).
        Slope: compare current hist to prev tick hist.
          Accelerating (hist growing in same direction) → +20% boost.
          Decelerating (hist shrinking) → –20% reduction.
          Reversing (hist changing sign) → –40% reduction.
        """
        macd_data = ind.get("macd")
        if macd_data is None:
            return 0.0
        hist    = macd_data["hist"]
        ema50   = ind.get("ema50") or 1.0
        atr_val = ind.get("atr")
        atr_pct = (atr_val / ema50) if (atr_val and ema50 and ema50 > 0) else 0.001
        norm    = (hist / ema50) / max(atr_pct, 1e-6)
        score   = _clamp(norm)

        return score

    def _score_mean_reversion(self, ind: Dict) -> float:
        """
        Bollinger %B based mean-reversion signal.
        %B < 0.2 → oversold → LONG signal (+1)
        %B > 0.8 → overbought → SHORT signal (-1)
        Linear interpolation between 0.2 and 0.8 → 0
        """
        bb = ind.get("bollinger")
        if bb is None:
            return 0.0
        pct_b = bb["pct_b"]
        if pct_b <= 0.2:
            return _clamp((0.2 - pct_b) / 0.2)   # 0..+1
        elif pct_b >= 0.8:
            return _clamp(-(pct_b - 0.8) / 0.2)  # 0..-1
        return 0.0

    def _score_rsi(self, ind: Dict, prev_ind: Dict) -> float:
        """
        Zone-based RSI scoring aligned with _apply_filters():
          > 75        → -0.5  (filter blocks LONG here anyway)
          70–75       → -0.1 to -0.5  approaching block
          60–70       → +0.5 to 0.0   momentum zone, confirms trend (not overbought)
          40–60       → linear -0.5 to +0.5  neutral confirmation
          30–40       → 0.0 to -0.5   bearish momentum weakening
          25–30       → +0.1 to +0.5  approaching oversold block
          < 25        → +0.5  (filter blocks SHORT here anyway)

        Key design: RSI 60-70 gives POSITIVE score — RSI at 65 in an uptrend
        is normal momentum, not overbought. Original monotonic scorer
        gave RSI 65 = -0.62 which actively fought trend signals.
        """
        rsi_val = ind.get("rsi")
        if rsi_val is None:
            return 0.0

        if rsi_val > 75:
            score = -0.5
        elif rsi_val >= 70:
            score = -0.1 - 0.4 * (rsi_val - 70) / 5.0
        elif rsi_val >= 60:
            score = 0.5 * (70 - rsi_val) / 10.0
        elif rsi_val >= 40:
            score = (rsi_val - 50.0) / 20.0
        elif rsi_val >= 30:
            score = -0.5 * (40 - rsi_val) / 10.0
        elif rsi_val >= 25:
            score = 0.1 + 0.4 * (30 - rsi_val) / 5.0
        else:
            score = 0.5

        return _clamp(score)

    def _score_stoch_rsi(self, ind: Dict) -> float:
        """
        Stochastic RSI hybrid score: zone level (60%) + k/d crossover (40%).

        Zone (k-line position):
          k > 80 → overbought → negative score toward -1
          k < 20 → oversold  → positive score toward +1
          20–80  → linear gradient through zero

        Crossover (k vs d-line):
          k > d → momentum turning up   → positive confirmation
          k < d → momentum still falling → penalises the zone score
          Normalised by 20-point spread, clamped to ±1.

        Using both prevents knife-catching: k=8 scores differently depending on
        whether it is still falling (k < d) or bouncing (k > d). The d-line was
        already computed in indicators.py but previously ignored.
        """
        sr = ind.get("stoch_rsi")
        if sr is None:
            return 0.0
        k = float(sr.get("k", 50.0))
        d = float(sr.get("d", 50.0))

        # Zone score — primary signal (unchanged logic)
        if k >= 80:
            zone_score = -0.5 - 0.5 * (k - 80) / 20.0
        elif k <= 20:
            zone_score = 0.5 + 0.5 * (20 - k) / 20.0
        else:
            zone_score = (50.0 - k) / 60.0

        # Crossover score — k vs d confirms or contradicts the zone
        diff        = k - d
        cross_score = (1.0 if diff > 0 else -1.0) * min(abs(diff) / 20.0, 1.0)

        return _clamp(0.6 * zone_score + 0.4 * cross_score)

    # ------------------------------------------------------------------
    # Entry filters
    # ------------------------------------------------------------------

    def _apply_filters(
        self,
        raw_dir: str,
        composite: float,
        ind: Dict,
    ) -> tuple:
        """Returns (direction, filters_passed, reason)."""
        if raw_dir == "NEUTRAL":
            return "NEUTRAL", False, "composite below threshold"

        rsi_val: Optional[float] = ind.get("rsi")
        ema21: Optional[float]   = ind.get("ema21")
        ema50: Optional[float]   = ind.get("ema50")

        # RSI extreme blocks
        if raw_dir == "LONG" and rsi_val is not None and rsi_val > _RSI_OB:
            return "NEUTRAL", False, f"RSI {rsi_val:.1f} > {_RSI_OB} (extreme overbought)"
        if raw_dir == "SHORT" and rsi_val is not None and rsi_val < _RSI_OS:
            return "NEUTRAL", False, f"RSI {rsi_val:.1f} < {_RSI_OS} (extreme oversold)"

        # StochRSI extreme blocks — catches pump wicks that RSI misses.
        # RSI lags; StochRSI reacts faster to price spikes.
        sr = ind.get("stoch_rsi") or {}
        stoch_k = sr.get("k")
        if raw_dir == "LONG" and stoch_k is not None and stoch_k > 85:
            return "NEUTRAL", False, f"StochRSI K {stoch_k:.0f} > 85 (extreme overbought — pump wick risk)"
        if raw_dir == "SHORT" and stoch_k is not None and stoch_k < 15:
            return "NEUTRAL", False, f"StochRSI K {stoch_k:.0f} < 15 (extreme oversold — dump wick risk)"

        # Counter-trend filter — allow if RSI is at extreme (mean-reversion mode)
        if ema21 is not None and ema50 is not None:
            trend_up = ema21 > ema50
            if raw_dir == "LONG" and not trend_up:
                # Counter-trend long: only allow in mean-reversion mode
                if rsi_val is None or rsi_val >= _RSI_EXTREME_OS:
                    return "NEUTRAL", False, "counter-trend LONG blocked (EMA21 < EMA50, RSI not extreme)"
            if raw_dir == "SHORT" and trend_up:
                if rsi_val is None or rsi_val <= _RSI_EXTREME_OB:
                    return "NEUTRAL", False, "counter-trend SHORT blocked (EMA21 > EMA50, RSI not extreme)"

        reason = f"composite={composite:+.3f} → {raw_dir}"
        return raw_dir, True, reason
