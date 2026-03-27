"""
engine/orderflow.py — Order-flow and book-imbalance analysis.

Aggregates real-time aggTrade events and depth snapshots into a
concise summary dict consumed by SignalEngine.
"""
import collections
import time
from typing import Deque, Dict, List, Tuple


# ---------------------------------------------------------------------------
# Trade bucket
# ---------------------------------------------------------------------------

_TradeEvent = Tuple[float, float, bool, float]  # (price, qty, buyer_maker, ts)


class OrderFlowAnalyzer:
    """
    Accumulates aggTrade + depth events and exposes a summarize() method.

    buy_volume  — notional filled by buy-aggressor trades (taker buys)
    sell_volume — notional filled by sell-aggressor trades (taker sells)
    imbalance   — (bid_qty - ask_qty) / (bid_qty + ask_qty) from top-N book levels
    """

    def __init__(self, window_seconds: float = 30.0, depth_levels: int = 5) -> None:
        self._window = window_seconds
        self._depth_levels = depth_levels

        # Rolling deque of (timestamp, buy_notional, sell_notional)
        self._trades: Deque[Tuple[float, float, float]] = collections.deque()

        # Latest depth snapshot
        self._bids: List[List[float]] = []   # [[price, qty], ...]
        self._asks: List[List[float]] = []

        # Track consecutive adverse-pressure seconds for DCA confirmation
        self._adverse_start: Dict[str, float] = {}   # direction → start_ts

    # ------------------------------------------------------------------
    # Feed methods (called by WS callbacks)
    # ------------------------------------------------------------------

    def on_trade(self, event: Dict) -> None:
        """
        event: {"price": float, "qty": float, "buyer_maker": bool, "time": int}
        buyer_maker=True means the buyer is the market maker → taker is SELLER
        """
        price = event["price"]
        qty   = event["qty"]
        notional = price * qty
        ts = time.time()

        if event["buyer_maker"]:
            # taker sold → sell aggressor
            self._trades.append((ts, 0.0, notional))
        else:
            # taker bought → buy aggressor
            self._trades.append((ts, notional, 0.0))

        self._prune()

    def on_depth(self, event: Dict) -> None:
        """event: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}"""
        self._bids = event.get("bids", [])
        self._asks = event.get("asks", [])

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summarize(self) -> Dict:
        """
        Returns:
        {
            "buy_volume":   float,   # sum of taker-buy notional in window
            "sell_volume":  float,   # sum of taker-sell notional in window
            "delta":        float,   # buy - sell
            "ratio":        float,   # buy / (buy + sell), 0.5 if no data
            "score":        float,   # -1..+1 normalised delta
            "imbalance":    float,   # (bid_qty - ask_qty) / (bid_qty + ask_qty)
            "bid_ask_ratio": float,  # bid_notional / ask_notional
        }
        """
        self._prune()

        buy_vol = sum(t[1] for t in self._trades)
        sell_vol = sum(t[2] for t in self._trades)
        total = buy_vol + sell_vol

        ratio = (buy_vol / total) if total > 0 else 0.5
        # score: +1 = all buys, -1 = all sells
        score = (ratio - 0.5) * 2.0

        # Book imbalance
        n = self._depth_levels
        bid_qty = sum(lvl[1] for lvl in self._bids[:n])
        ask_qty = sum(lvl[1] for lvl in self._asks[:n])
        book_total = bid_qty + ask_qty
        imbalance = (bid_qty - ask_qty) / book_total if book_total > 0 else 0.0

        # Bid/ask notional ratio
        bid_notional = sum(lvl[0] * lvl[1] for lvl in self._bids[:n])
        ask_notional = sum(lvl[0] * lvl[1] for lvl in self._asks[:n])
        ba_total = bid_notional + ask_notional
        ba_ratio = (bid_notional / ba_total) if ba_total > 0 else 0.5

        return {
            "buy_volume":    round(buy_vol, 2),
            "sell_volume":   round(sell_vol, 2),
            "delta":         round(buy_vol - sell_vol, 2),
            "ratio":         round(ratio, 4),
            "score":         round(score, 4),
            "imbalance":     round(imbalance, 4),
            "bid_ask_ratio": round(ba_ratio, 4),
        }

    # ------------------------------------------------------------------
    # DCA confirmation helper
    # ------------------------------------------------------------------

    def check_adverse_pressure(
        self,
        main_direction: str,
        required_seconds: float = 5.0,
    ) -> bool:
        """
        Returns True if order flow has shown adverse pressure (against
        main_direction) continuously for at least required_seconds.

        Adverse means:
        - LONG main → sell pressure (score < 0)
        - SHORT main → buy pressure (score > 0)
        """
        summary = self.summarize()
        score = summary["score"]

        if main_direction == "LONG":
            currently_adverse = score < -0.1
        else:
            currently_adverse = score > 0.1

        now = time.time()
        key = main_direction

        if currently_adverse:
            if key not in self._adverse_start:
                self._adverse_start[key] = now
            elapsed = now - self._adverse_start[key]
            return elapsed >= required_seconds
        else:
            self._adverse_start.pop(key, None)
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _prune(self) -> None:
        cutoff = time.time() - self._window
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()
