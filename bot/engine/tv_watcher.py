"""
TradingView Symbol Watcher — parallel signal monitoring.

Watches the top N TV-alerted symbols in the background, runs the full
signal engine on each (using cached candles), and returns the first symbol
that passes all entry gates. The main ticker loop calls this on each tick
to decide whether to switch.
"""
import asyncio
import time
import logging
from typing import Dict, List, Optional, Tuple

from engine.indicators import compute_all
from engine.signal import SignalEngine, _ENTRY_THRESHOLD
from config import BotConfig

logger = logging.getLogger(__name__)

_CANDLE_TTL_S  = 30.0   # refresh candles every 30s per symbol
_WATCH_COUNT   = 3       # monitor top N TV symbols

class TvWatcher:
    def __init__(self, rest_client):
        self._rest        = rest_client
        self._cache: Dict[str, dict] = {}  # {symbol: {candles, indicators, prev_indicators, last_fetch}}
        self._signal_eng  = SignalEngine()
        self._last_sig_ts: float = 0.0     # last time get_all_signals() ran

    async def refresh(self, symbols: List[str], cfg: BotConfig) -> None:
        """Fetch/update candles for watched symbols. Call periodically."""
        now = time.time()
        for sym in symbols[:_WATCH_COUNT]:
            entry = self._cache.get(sym, {})
            if now - entry.get("last_fetch", 0) < _CANDLE_TTL_S:
                continue
            try:
                raw = await self._rest.get_klines(sym, interval=cfg.timeframe, limit=100)
                if raw and len(raw) >= 60:
                    candles = [
                        {"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                         "close": float(k[4]), "volume": float(k[5]), "time": int(k[0])//1000}
                        for k in raw
                    ]
                    self._cache[sym] = {
                        "candles":         candles,
                        "indicators":      compute_all(candles),
                        "prev_indicators": compute_all(candles[:-1]) if len(candles) >= 2 else {},
                        "last_fetch":      now,
                    }
                    self._last_sig_ts = 0.0  # force signal recompute after candle refresh
                    logger.debug("TvWatcher: refreshed %s (%d candles)", sym, len(candles))
            except Exception as exc:
                logger.warning("TvWatcher: candle fetch failed for %s: %s", sym, exc)

    def best_entry(
        self,
        symbols: List[str],
        movers: List[dict],
        cfg: BotConfig,
        htf_bias: str = "NEUTRAL",
    ) -> Optional[Tuple[str, str, float]]:
        """
        Check top N symbols for a valid entry signal.
        Returns (symbol, direction, strength) for the first that passes all gates,
        or None if no symbol is ready.

        Gates checked:
          - Composite > threshold
          - Strength >= min_signal_strength
          - RSI/StochRSI extreme filter
          - Counter-trend filter
          - Wick rejection (last closed candle)
          - Candle direction confirmation
          - Volume proxy (flow substitute)
          - HTF filter
          - Signal held for >= 2 consecutive candle closes
        """
        for sym in symbols[:_WATCH_COUNT]:
            entry = self._cache.get(sym)
            if not entry or not entry.get("candles"):
                continue

            candles    = entry["candles"]
            ind        = entry["indicators"]
            mover_data = next((m for m in movers if m.get("symbol") == sym), {})
            scanner    = mover_data.get("scanner_type", "momentum")

            # Build a minimal flow dict using volume proxy
            # (real taker flow not available for non-active symbols)
            if len(candles) >= 2:
                c    = candles[-1]
                vol_avg = sum(x["volume"] for x in candles[-20:]) / 20 if len(candles) >= 20 else c["volume"]
                vol_ratio = c["volume"] / vol_avg if vol_avg > 0 else 1.0
                price_dir = 1.0 if c["close"] > c["open"] else -1.0
                flow_proxy = max(-1.0, min(1.0, (vol_ratio - 1.0) * price_dir))
            else:
                flow_proxy = 0.0

            flow = {
                "score":       flow_proxy,
                "imbalance":   flow_proxy * 0.5,
                "ba_ratio":    0.55 if flow_proxy > 0 else 0.45,
                "trade_count": 30,
            }

            # Set scanner type and compute signal
            signal = self._signal_eng.compute(candles, flow, ind, scanner_type=scanner)

            direction = signal["direction"]
            strength  = signal["strength"]

            if direction == "NEUTRAL":
                continue
            if strength < cfg.min_signal_strength:
                continue
            if not signal.get("filters_passed", False):
                continue

            # HTF filter
            if cfg.htf_filter and htf_bias not in ("NEUTRAL", "") and htf_bias != direction:
                continue

            # Flow gate: volume proxy must agree with direction
            if direction == "LONG"  and flow_proxy <= 0:
                continue
            if direction == "SHORT" and flow_proxy >= 0:
                continue

            # Wick rejection on last closed candle
            if len(candles) >= 2:
                c2    = candles[-2]
                rng   = c2["high"] - c2["low"]
                body  = abs(c2["close"] - c2["open"])
                is_doji = rng > 0 and body / rng < 0.10
                if not is_doji and rng > 0:
                    upper = c2["high"] - max(c2["open"], c2["close"])
                    lower = min(c2["open"], c2["close"]) - c2["low"]
                    if direction == "LONG"  and upper / rng > 0.60:
                        continue
                    if direction == "SHORT" and lower / rng > 0.60:
                        continue

            # Candle direction confirmation
            if len(candles) >= 2:
                c2 = candles[-2]
                if direction == "LONG"  and c2["close"] < c2["open"]:
                    continue
                if direction == "SHORT" and c2["close"] > c2["open"]:
                    continue

            # Signal persistence: must have been same direction on previous candle close
            # (candle-based substitute for tick-based persist_needed)
            if len(candles) >= 3:
                prev_ind_cached = entry.get("prev_indicators", {})
                prev_flow = {**flow}
                prev_sig  = self._signal_eng.compute(candles[:-1], prev_flow, prev_ind_cached)
                if prev_sig["direction"] != direction:
                    continue

            logger.info(
                "TvWatcher: %s %s strength=%.2f composite=%+.3f — ENTRY READY",
                sym, direction, strength, signal.get("composite", 0),
            )
            return (sym, direction, strength)

        return None

    def get_all_signals(
        self,
        symbols: List[str],
        movers:  List[dict],
        cfg:     BotConfig,
        htf_bias: str = "NEUTRAL",
    ) -> List[dict]:
        """
        Return bot signal engine result for every watched symbol.
        Used to update sidebar with real-time bot strength — not just TV composite score.
        Each entry: {symbol, direction, strength, composite, ready}
          ready=True means ALL entry gates passed (bot would take this trade).
        """
        results = []
        for sym in symbols[:_WATCH_COUNT]:
            entry = self._cache.get(sym)
            if not entry or not entry.get("candles"):
                continue

            candles    = entry["candles"]
            ind        = entry["indicators"]
            mover_data = next((m for m in movers if m.get("symbol") == sym), {})
            scanner    = mover_data.get("scanner_type", "momentum")

            if len(candles) >= 2:
                c         = candles[-1]
                vol_avg   = sum(x["volume"] for x in candles[-20:]) / 20 if len(candles) >= 20 else c["volume"]
                vol_ratio = c["volume"] / vol_avg if vol_avg > 0 else 1.0
                price_dir = 1.0 if c["close"] > c["open"] else -1.0
                flow_proxy = max(-1.0, min(1.0, (vol_ratio - 1.0) * price_dir))
            else:
                flow_proxy = 0.0

            flow = {
                "score":       flow_proxy,
                "imbalance":   flow_proxy * 0.5,
                "ba_ratio":    0.55 if flow_proxy > 0 else 0.45,
                "trade_count": 30,
            }

            signal    = self._signal_eng.compute(candles, flow, ind, scanner_type=scanner)
            direction = signal["direction"]
            strength  = signal["strength"]
            composite = signal.get("composite", 0.0)

            # Check all entry gates — same as best_entry()
            ready = True
            if direction == "NEUTRAL":                                              ready = False
            elif strength < cfg.min_signal_strength:                                ready = False
            elif not signal.get("filters_passed", False):                           ready = False
            elif cfg.htf_filter and htf_bias not in ("NEUTRAL","") and htf_bias != direction: ready = False
            elif direction == "LONG"  and flow_proxy <= 0:                          ready = False
            elif direction == "SHORT" and flow_proxy >= 0:                          ready = False
            else:
                if len(candles) >= 2:
                    c2    = candles[-2]
                    rng   = c2["high"] - c2["low"]
                    body  = abs(c2["close"] - c2["open"])
                    is_doji = rng > 0 and body / rng < 0.10
                    if not is_doji and rng > 0:
                        upper = c2["high"] - max(c2["open"], c2["close"])
                        lower = min(c2["open"], c2["close"]) - c2["low"]
                        if direction == "LONG"  and upper / rng > 0.60: ready = False
                        if direction == "SHORT" and lower / rng > 0.60: ready = False
                if ready and len(candles) >= 2:
                    c2 = candles[-2]
                    if direction == "LONG"  and c2["close"] < c2["open"]: ready = False
                    if direction == "SHORT" and c2["close"] > c2["open"]: ready = False
                if ready and len(candles) >= 3:
                    prev_ind_cached = entry.get("prev_indicators", {})
                    prev_flow = {**flow}
                    prev_sig  = self._signal_eng.compute(candles[:-1], prev_flow, prev_ind_cached)
                    if prev_sig["direction"] != direction: ready = False

            results.append({
                "symbol":    sym,
                "direction": direction,
                "strength":  round(strength, 3),
                "composite": round(composite, 3),
                "ready":     ready,
            })

        return results

    def signals_are_stale(self) -> bool:
        """True when candles were refreshed since last get_all_signals() call."""
        return self._last_sig_ts == 0.0

    def mark_signals_fresh(self) -> None:
        self._last_sig_ts = time.time()

    def invalidate(self, symbol: str) -> None:
        """Force candle refresh for a symbol on next cycle."""
        if symbol in self._cache:
            self._cache[symbol]["last_fetch"] = 0.0

    def clear(self) -> None:
        self._cache.clear()
        self._last_sig_ts = 0.0
