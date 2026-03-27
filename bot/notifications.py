"""
notifications.py — Discord webhook notifications for trade events.

Only sends if discord_webhook config is non-empty.
All network errors are caught and logged; they never raise into the caller.
"""
import logging
from typing import Dict

import httpx

logger = logging.getLogger(__name__)

# Discord embed colours (decimal)
_COLOUR = {
    "TRADE_OPEN":  0x00C853,   # green
    "TRADE_DCA":   0x1565C0,   # blue
    "HEDGE_OPEN":  0x6A1B9A,   # purple
    "TRADE_CLOSE_WIN": 0x00C853,
    "TRADE_CLOSE_LOSS": 0xD32F2F,
    "HARD_STOP":   0xD32F2F,   # red
    "AUTO_SWITCH": 0xF9A825,   # gold
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
    paper_tag = " [PAPER]" if data.get("paper") else ""
    symbol    = data.get("symbol", "")

    if event == "TRADE_OPEN":
        return {
            "title": f"📈 TRADE OPEN{paper_tag}",
            "color": _COLOUR["TRADE_OPEN"],
            "fields": [
                {"name": "Symbol",    "value": symbol,                          "inline": True},
                {"name": "Direction", "value": data.get("direction", ""),       "inline": True},
                {"name": "Price",     "value": f"{data.get('price', 0):.4f}",   "inline": True},
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
                {"name": "Fill Price",    "value": f"{data.get('price', 0):.4f}",       "inline": True},
                {"name": "New Avg",       "value": f"{data.get('new_avg', 0):.4f}",     "inline": True},
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
                {"name": "Price",     "value": f"{data.get('price', 0):.4f}", "inline": True},
                {"name": "Qty",       "value": str(data.get("qty", 0)),       "inline": True},
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
                {"name": "Price",     "value": f"{data.get('price', 0):.4f}",       "inline": True},
                {"name": "PnL",       "value": f"{pnl:+.4f} USDT ({data.get('pnl_pct', 0):+.2f}%)", "inline": True},
                {"name": "Reason",    "value": data.get("reason", ""),              "inline": True},
            ],
        }

    if event == "HARD_STOP":
        return {
            "title": f"⚠️ HARD STOP TRIGGERED{paper_tag}",
            "color": _COLOUR["HARD_STOP"],
            "fields": [
                {"name": "Symbol",  "value": symbol,                              "inline": True},
                {"name": "Price",   "value": f"{data.get('price', 0):.4f}",       "inline": True},
                {"name": "PnL %",   "value": f"{data.get('pnl_pct', 0):+.2f}%",  "inline": True},
            ],
        }

    if event == "AUTO_SWITCH":
        return {
            "title": f"🔀 AUTO SWITCH{paper_tag}",
            "color": _COLOUR["AUTO_SWITCH"],
            "fields": [
                {"name": "From", "value": data.get("old_symbol", ""), "inline": True},
                {"name": "To",   "value": data.get("new_symbol", ""), "inline": True},
            ],
        }

    return None
