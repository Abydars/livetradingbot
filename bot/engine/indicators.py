"""
engine/indicators.py — Technical indicator calculations (pure functions, no I/O).

All functions accept plain Python lists and return float or dict.
Returns None when there is insufficient data.
"""
import math
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# EMA
# ---------------------------------------------------------------------------

def ema(prices: List[float], period: int) -> Optional[float]:
    """Exponential Moving Average."""
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    result = sum(prices[:period]) / period
    for price in prices[period:]:
        result = price * k + result * (1 - k)
    return result


def ema_series(prices: List[float], period: int) -> List[Optional[float]]:
    """Return EMA value for every index (None for warm-up period)."""
    if not prices:
        return []
    k = 2.0 / (period + 1)
    out: List[Optional[float]] = [None] * (period - 1)
    seed = sum(prices[:period]) / period
    out.append(seed)
    val = seed
    for price in prices[period:]:
        val = price * k + val * (1 - k)
        out.append(val)
    return out


# ---------------------------------------------------------------------------
# SMA
# ---------------------------------------------------------------------------

def sma(prices: List[float], period: int) -> Optional[float]:
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------

def rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """Wilder RSI."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))


# ---------------------------------------------------------------------------
# ATR
# ---------------------------------------------------------------------------

def atr(highs: List[float], lows: List[float], closes: List[float],
        period: int = 14) -> Optional[float]:
    """Average True Range using Wilder smoothing."""
    if len(closes) < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)

    if len(trs) < period:
        return None

    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return atr_val


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------

def macd(
    prices: List[float],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> Optional[Dict[str, float]]:
    """
    MACD = EMA(fast) - EMA(slow)
    Signal = EMA(MACD, signal_period)
    Histogram = MACD - Signal
    Returns {"macd": float, "signal": float, "hist": float}
    """
    min_len = slow + signal_period - 1
    if len(prices) < min_len:
        return None

    fast_series = ema_series(prices, fast)
    slow_series = ema_series(prices, slow)

    # Build MACD line where both EMAs are defined
    macd_line: List[float] = []
    for f, s in zip(fast_series, slow_series):
        if f is not None and s is not None:
            macd_line.append(f - s)

    if len(macd_line) < signal_period:
        return None

    sig_val = ema(macd_line, signal_period)
    if sig_val is None:
        return None
    macd_val = macd_line[-1]
    return {
        "macd":   round(macd_val, 8),
        "signal": round(sig_val, 8),
        "hist":   round(macd_val - sig_val, 8),
    }


# ---------------------------------------------------------------------------
# Bollinger Bands
# ---------------------------------------------------------------------------

def bollinger(
    prices: List[float],
    period: int = 20,
    std_dev: float = 2.0,
) -> Optional[Dict[str, float]]:
    """
    Bollinger Bands.
    Returns {"upper": float, "mid": float, "lower": float, "width": float, "pct_b": float}
    pct_b = (close - lower) / (upper - lower), clipped to [0, 1] when bands are non-zero.
    """
    if len(prices) < period:
        return None
    window = prices[-period:]
    mid = sum(window) / period
    variance = sum((p - mid) ** 2 for p in window) / period
    std = math.sqrt(variance)
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    band_width = upper - lower
    pct_b = (prices[-1] - lower) / band_width if band_width > 0 else 0.5
    return {
        "upper":  round(upper, 8),
        "mid":    round(mid, 8),
        "lower":  round(lower, 8),
        "width":  round(band_width, 8),
        "pct_b":  round(pct_b, 6),
    }


# ---------------------------------------------------------------------------
# Stochastic RSI
# ---------------------------------------------------------------------------

def stoch_rsi(
    prices: List[float],
    rsi_period: int = 14,
    stoch_period: int = 14,
    k_smooth: int = 3,
    d_smooth: int = 3,
) -> Optional[Dict[str, float]]:
    """
    Stochastic RSI.
    Returns {"k": float, "d": float} — both in range [0, 100].
    """
    min_len = rsi_period + stoch_period + max(k_smooth, d_smooth) + 1
    if len(prices) < min_len:
        return None

    # Build RSI series
    rsi_vals: List[float] = []
    for i in range(rsi_period, len(prices) + 1):
        r = rsi(prices[:i], rsi_period)
        if r is not None:
            rsi_vals.append(r)

    if len(rsi_vals) < stoch_period + k_smooth + d_smooth:
        return None

    # Stochastic of RSI
    stoch_vals: List[float] = []
    for i in range(stoch_period, len(rsi_vals) + 1):
        window = rsi_vals[i - stoch_period: i]
        lo = min(window)
        hi = max(window)
        if hi == lo:
            stoch_vals.append(50.0)
        else:
            stoch_vals.append((rsi_vals[i - 1] - lo) / (hi - lo) * 100.0)

    if len(stoch_vals) < k_smooth + d_smooth:
        return None

    # %K = SMA of stoch
    k_series: List[float] = []
    for i in range(k_smooth, len(stoch_vals) + 1):
        k_series.append(sum(stoch_vals[i - k_smooth: i]) / k_smooth)

    if len(k_series) < d_smooth:
        return None

    d_val = sum(k_series[-d_smooth:]) / d_smooth
    return {
        "k": round(k_series[-1], 4),
        "d": round(d_val, 4),
    }


# ---------------------------------------------------------------------------
# Convenience: compute all indicators from candle list
# ---------------------------------------------------------------------------

def compute_all(candles: List[Dict]) -> Dict:
    """
    Accept candles as [{"open": f, "high": f, "low": f, "close": f, "volume": f}, ...]
    Returns a dict with all indicator values (None if insufficient data).
    """
    closes = [c["close"] for c in candles]
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]

    ema9_v  = ema(closes, 9)
    ema21_v = ema(closes, 21)
    ema50_v = ema(closes, 50)
    rsi14_v = rsi(closes, 14)
    atr14_v = atr(highs, lows, closes, 14)
    macd_v  = macd(closes)
    bb_v    = bollinger(closes)
    srsi_v  = stoch_rsi(closes)

    return {
        "ema9":     ema9_v,
        "ema21":    ema21_v,
        "ema50":    ema50_v,
        "rsi":      rsi14_v,
        "atr":      atr14_v,
        "macd":     macd_v,
        "bollinger": bb_v,
        "stoch_rsi": srsi_v,
    }
