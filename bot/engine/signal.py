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

# Component weights
_W_FLOW     = 0.30
_W_TREND    = 0.25
_W_MOMENTUM = 0.20
_W_MEAN_REV = 0.15
_W_RSI      = 0.10

# Decision thresholds
_ENTRY_THRESHOLD = 0.20   # composite must exceed ±0.20 for a directional signal
_RSI_OB = 75.0            # overbought block for LONG
_RSI_OS = 25.0            # oversold block for SHORT
_RSI_EXTREME_OB = 70.0    # above this = mean-reversion SHORT allowed
_RSI_EXTREME_OS = 30.0    # below this = mean-reversion LONG allowed


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


class SignalEngine:
    """
    Stateless signal computation.  Call compute() each cycle.
    """

    def compute(
        self,
        candles: List[Dict],
        flow_summary: Dict,
        ind: Dict,
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
        components = self._score_components(flow_summary, ind)
        composite = (
            components["flow"]     * _W_FLOW
            + components["trend"]    * _W_TREND
            + components["momentum"] * _W_MOMENTUM
            + components["mean_rev"] * _W_MEAN_REV
            + components["rsi"]      * _W_RSI
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
            # max possible excess ≈ 0.80 (all components fully aligned)
            strength = _clamp(excess / 0.80, 0.0, 1.0)
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
        }

    # ------------------------------------------------------------------
    # Component scoring
    # ------------------------------------------------------------------

    def _score_components(self, flow: Dict, ind: Dict) -> Dict[str, float]:
        return {
            "flow":     self._score_flow(flow),
            "trend":    self._score_trend(ind),
            "momentum": self._score_momentum(ind),
            "mean_rev": self._score_mean_reversion(ind),
            "rsi":      self._score_rsi(ind),
        }

    def _score_flow(self, flow: Dict) -> float:
        """
        Combine three order-flow signals, each naturally in [-1, +1]:
          score      — taker buy/sell notional delta over rolling window
          imbalance  — qty-weighted bid/ask book imbalance (top N levels)
          ba_ratio   — notional-weighted bid/ask book imbalance (large orders)
        Average all three equally and clamp.
        """
        score     = float(flow.get("score", 0.0))
        imbalance = float(flow.get("imbalance", 0.0))
        # bid_ask_ratio is in [0, 1]; map to [-1, +1]
        ba_ratio  = (float(flow.get("bid_ask_ratio", 0.5)) - 0.5) * 2.0
        combined  = (score + imbalance + ba_ratio) / 3.0
        return _clamp(combined)

    def _score_trend(self, ind: Dict) -> float:
        """
        EMA21 vs EMA50 cross.
        +1 = EMA21 strongly above EMA50, -1 = strongly below.
        Normalised by ATR% so score magnitude is regime-independent.
        """
        ema21: Optional[float] = ind.get("ema21")
        ema50: Optional[float] = ind.get("ema50")
        if ema21 is None or ema50 is None or ema50 == 0:
            return 0.0
        diff_pct = (ema21 - ema50) / ema50
        atr_val = ind.get("atr")
        atr_pct = (atr_val / ema50) if (atr_val and ema50 and ema50 > 0) else 0.005
        scaled = diff_pct / max(atr_pct, 1e-6)
        return _clamp(scaled)

    def _score_momentum(self, ind: Dict) -> float:
        """
        MACD histogram direction and magnitude.
        1 ATR of histogram movement relative to price = ±1 score.
        """
        macd_data = ind.get("macd")
        if macd_data is None:
            return 0.0
        hist  = macd_data["hist"]
        ema50 = ind.get("ema50") or 1.0
        atr_val = ind.get("atr")
        atr_pct = (atr_val / ema50) if (atr_val and ema50 and ema50 > 0) else 0.001
        norm = (hist / ema50) / max(atr_pct, 1e-6)
        return _clamp(norm)

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

    def _score_rsi(self, ind: Dict) -> float:
        """
        Zone-based RSI scoring that aligns with _apply_filters():
          > 75        → -0.5  hard overbought penalty (filter blocks LONG here)
          70–75       → fade -0.1 → -0.5  approaching filter block
          60–70       → fade +0.5 → 0.0   weakening bullish momentum
          40–60       → linear -0.5 → +0.5 confirmation zone
          30–40       → fade 0.0 → -0.5   weakening bearish momentum
          25–30       → fade +0.1 → +0.5  approaching oversold block
          < 25        → +0.5  hard oversold penalty (filter blocks SHORT here)
        """
        rsi_val = ind.get("rsi")
        if rsi_val is None:
            return 0.0

        if rsi_val > 75:
            # Hard overbought — filter will block LONG entries anyway
            score = -0.5
        elif rsi_val >= 70:
            # Fade from -0.1 (at 75) to -0.5 is wrong direction; fade -0.1 → -0.5
            # as RSI climbs from 70 toward 75
            score = -0.1 - 0.4 * (rsi_val - 70) / 5.0
        elif rsi_val >= 60:
            # Momentum weakening: fade +0.5 → 0.0 as RSI climbs 60→70
            score = 0.5 * (70 - rsi_val) / 10.0
        elif rsi_val >= 40:
            # Confirmation zone: linear -0.5 → +0.5
            score = (rsi_val - 50.0) / 20.0
        elif rsi_val >= 30:
            # Momentum weakening bearish: fade 0.0 → -0.5 as RSI falls 40→30
            score = -0.5 * (40 - rsi_val) / 10.0
        elif rsi_val >= 25:
            # Approaching oversold block: fade +0.1 → +0.5 as RSI falls 30→25
            score = 0.1 + 0.4 * (30 - rsi_val) / 5.0
        else:
            # Hard oversold — filter will block SHORT entries anyway
            score = 0.5

        return _clamp(score)

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
