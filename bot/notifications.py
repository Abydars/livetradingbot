"""
notifications.py — Discord webhook notifications for trade events.

Only sends if discord_webhook config is non-empty.
All network errors are caught and logged; they never raise into the caller.
"""
import logging
import math
from typing import Dict

import httpx

logger = logging.getLogger(__name__)


def _fmt_p(p: float) -> str:
    """
    Format a price with enough decimal places for any coin magnitude.
    Uses 4 significant figures so low-priced coins (SHIB, PEPE) are readable.
    """
    if not p:
        return "0"
    mag = math.floor(math.log10(abs(p)))
    dec = max(2, -mag + 3)          # 4 sig figs: e.g. 0.000025 → dec=8 → "0.00002500"
    return f"{p:.{dec}f}"


def _fmt_qty(q: float) -> str:
    """Format quantity — strips unnecessary trailing zeros for large integers,
    uses significant figures for fractional quantities."""
    if not q:
        return "0"
    if q >= 1:
        return f"{q:g}"
    mag = math.floor(math.log10(abs(q)))
    dec = max(2, -mag + 3)
    return f"{q:.{dec}f}"

# Discord embed colours (decimal)
_COLOUR = {
    "TRADE_OPEN":      0x00C853,   # green
    "TRADE_DCA":       0x1565C0,   # blue
    "HEDGE_OPEN":      0x6A1B9A,   # purple
    "HEDGE_PROMOTED":  0xFF6D00,   # orange
    "TRADE_CLOSE_WIN": 0x00C853,
    "TRADE_CLOSE_LOSS": 0xD32F2F,
    "HARD_STOP":       0xD32F2F,   # red
}


async def notify(webhook_url: str, event: str, data: Dict) -> None:
    """
    Send a Discord embed for the given event.

    Silently does nothing if webhook_url is empty.
    """
    if not webhook_url:
        return

    embed = _build_embed(event, data)
    if embed is None:
        return

    payload = {"embeds": [embed]}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(webhook_url, json=payload)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("notify: failed to send Discord webhook (%s): %s", event, exc)


# ---------------------------------------------------------------------------
# Embed builders
# ---------------------------------------------------------------------------

def _build_embed(event: str, data: Dict) -> Dict:
    _mode = data.get("trading_mode", "")
    paper_tag = f" [{_mode.upper()}]" if _mode else ""
    symbol    = data.get("symbol", "")

    if event == "TRADE_OPEN":
        return {
            "title": f"📈 TRADE OPEN{paper_tag}",
            "color": _COLOUR["TRADE_OPEN"],
            "fields": [
                {"name": "Symbol",    "value": symbol,                          "inline": True},
                {"name": "Direction", "value": data.get("direction", ""),       "inline": True},
                {"name": "Price",     "value": _fmt_p(data.get('price', 0)),     "inline": True},
                {"name": "Margin",    "value": f"{data.get('margin', 0)} USDT", "inline": True},
                {"name": "Strength",  "value": f"{data.get('strength', 0):.2%}","inline": True},
            ],
        }

    if event == "TRADE_DCA":
        return {
            "title": f"🔁 DCA #{data.get('level', '?')}{paper_tag}",
            "color": _COLOUR["TRADE_DCA"],
            "fields": [
                {"name": "Symbol",        "value": symbol,                              "inline": True},
                {"name": "Direction",     "value": data.get("direction", ""),           "inline": True},
                {"name": "Fill Price",    "value": _fmt_p(data.get('price', 0)),           "inline": True},
                {"name": "New Avg",       "value": _fmt_p(data.get('new_avg', 0)),       "inline": True},
                {"name": "Total Margin",  "value": f"{data.get('total_margin', 0)} USDT","inline": True},
            ],
        }

    if event == "HEDGE_OPEN":
        return {
            "title": f"🛡 HEDGE OPEN{paper_tag}",
            "color": _COLOUR["HEDGE_OPEN"],
            "fields": [
                {"name": "Symbol",    "value": symbol,                        "inline": True},
                {"name": "Direction", "value": data.get("direction", ""),     "inline": True},
                {"name": "Price",     "value": _fmt_p(data.get('price', 0)),   "inline": True},
                {"name": "Qty",       "value": _fmt_qty(data.get("qty", 0)),  "inline": True},
            ],
        }

    if event == "TRADE_CLOSE":
        pnl    = data.get("pnl", 0)
        colour = _COLOUR["TRADE_CLOSE_WIN"] if pnl >= 0 else _COLOUR["TRADE_CLOSE_LOSS"]
        icon   = "✅" if pnl >= 0 else "❌"
        return {
            "title": f"{icon} TRADE CLOSED{paper_tag}",
            "color": colour,
            "fields": [
                {"name": "Symbol",    "value": symbol,                              "inline": True},
                {"name": "Direction", "value": data.get("direction", ""),           "inline": True},
                {"name": "Price",     "value": _fmt_p(data.get('price', 0)),         "inline": True},
                {"name": "PnL",       "value": f"{pnl:+.2f} USDT ({data.get('pnl_pct', 0):+.2f}%)", "inline": True},
                {"name": "Reason",    "value": data.get("reason", ""),              "inline": True},
            ],
        }

    if event == "HARD_STOP":
        return {
            "title": f"⚠️ HARD STOP TRIGGERED{paper_tag}",
            "color": _COLOUR["HARD_STOP"],
            "fields": [
                {"name": "Symbol",  "value": symbol,                              "inline": True},
                {"name": "Price",   "value": _fmt_p(data.get('price', 0)),         "inline": True},
                {"name": "PnL %",   "value": f"{data.get('pnl_pct', 0):+.2f}%",  "inline": True},
            ],
        }

    if event == "HEDGE_PROMOTED":
        return {
            "title": f"♻ HEDGE PROMOTED → MAIN{paper_tag}",
            "color": _COLOUR["HEDGE_PROMOTED"],
            "fields": [
                {"name": "Symbol",            "value": symbol,                                        "inline": True},
                {"name": "Direction",         "value": data.get("direction", ""),                     "inline": True},
                {"name": "Entry Price",       "value": _fmt_p(data.get("price", 0)),                  "inline": True},
                {"name": "Qty",               "value": _fmt_qty(data.get("qty", 0)),                  "inline": True},
                {"name": "Margin",            "value": f"{data.get('margin', 0)} USDT",               "inline": True},
                {"name": "Partial Close Qty", "value": _fmt_qty(data.get("partial_close_qty", 0)),    "inline": True},
            ],
        }

    return None
