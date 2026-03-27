"""
config.py — Runtime config model built from the database config table.
Provides a typed snapshot of all settings for use in the engine/exchange layers.
"""
import os
from dataclasses import dataclass
from typing import Optional

from database import get_all_config


@dataclass
class BotConfig:
    # Trading params
    symbol: str
    leverage: int
    margin_usdt: float
    dca_step_pct: float
    max_dca: int
    tp_pct: float
    hedge_trigger_pct: float
    max_re_hedge: int
    min_profit_pct: float

    # New in v2
    atr_dca_multiplier: float
    trail_pct: float
    min_signal_strength: float

    # Mode
    paper_mode: bool

    # Notifications
    discord_webhook: str

    # Chart / candle timeframe
    timeframe: str

    # Auto-switch
    auto_switch: bool
    scan_interval_s: int

    # Exchange secrets (env-only, never in DB)
    api_key: str
    api_secret: str


async def load_config() -> BotConfig:
    """Load config from DB and merge with environment variables."""
    cfg = await get_all_config()

    def _f(key: str, default: float = 0.0) -> float:
        return float(cfg.get(key, default))

    def _i(key: str, default: int = 0) -> int:
        return int(float(cfg.get(key, default)))

    def _b(key: str, default: bool = False) -> bool:
        val = cfg.get(key, "1" if default else "0")
        return str(val).strip() not in ("0", "false", "False", "")

    def _s(key: str, default: str = "") -> str:
        return str(cfg.get(key, default)).strip()

    return BotConfig(
        symbol=_s("symbol", "BTCUSDT"),
        leverage=_i("leverage", 10),
        margin_usdt=_f("margin_usdt", 10.0),
        dca_step_pct=_f("dca_step_pct", 0.5),
        max_dca=_i("max_dca", 3),
        tp_pct=_f("tp_pct", 0.8),
        hedge_trigger_pct=_f("hedge_trigger_pct", 1.0),
        max_re_hedge=_i("max_re_hedge", 3),
        min_profit_pct=_f("min_profit_pct", 0.1),
        atr_dca_multiplier=_f("atr_dca_multiplier", 1.0),
        trail_pct=_f("trail_pct", 0.15),
        min_signal_strength=_f("min_signal_strength", 0.25),
        timeframe=_s("timeframe", "1m"),
        paper_mode=_b("paper_mode", True),
        discord_webhook=_s("discord_webhook", ""),
        auto_switch=_b("auto_switch", True),
        scan_interval_s=_i("scan_interval_s", 30),
        # Secrets from environment only
        api_key=os.environ.get("BINANCE_API_KEY", ""),
        api_secret=os.environ.get("BINANCE_SECRET", ""),
    )
