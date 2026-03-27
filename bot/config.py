"""
config.py — Runtime config model built from the database config table.
Provides a typed snapshot of all settings for use in the engine/exchange layers.
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from database import get_all_config

# Load .env from project root (parent of bot/) if present — does not override
# variables already set in the shell environment.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass


@dataclass
class BotConfig:
    # Trading params
    symbol: str
    leverage: int
    margin_usdt: float
    max_dca: int
    max_re_hedge: int
    min_signal_strength: float

    # Trading mode: "paper" | "demo" | "live"
    trading_mode: str

    # Key type for BinanceClient: "auto" | "hmac" | "ed25519"
    key_type: str

    # Notifications
    discord_webhook: str

    # Chart / candle timeframe
    timeframe: str

    # Auto-switch
    auto_switch: bool
    scan_interval_s: int
    switch_threshold: float  # min score ratio for #1 vs current to trigger switch

    # Exchange secrets (env-only, never in DB)
    api_key: str
    api_secret: str

    @property
    def paper_mode(self) -> bool:
        return self.trading_mode == "paper"


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

    trading_mode = _s("trading_mode", "paper")

    if trading_mode == "live":
        # New names preferred; fall back to legacy BINANCE_API_KEY / BINANCE_SECRET
        api_key    = (os.environ.get("BINANCE_LIVE_KEY")
                      or os.environ.get("BINANCE_API_KEY", ""))
        api_secret = (os.environ.get("BINANCE_LIVE_SECRET")
                      or os.environ.get("BINANCE_SECRET", ""))
        key_type   = os.environ.get("BINANCE_LIVE_KEY_TYPE", "auto")
    elif trading_mode == "demo":
        api_key    = (os.environ.get("BINANCE_DEMO_KEY")
                      or os.environ.get("BINANCE_API_KEY", ""))
        api_secret = (os.environ.get("BINANCE_DEMO_SECRET")
                      or os.environ.get("BINANCE_SECRET", ""))
        key_type   = "hmac"
    else:
        api_key = api_secret = key_type = ""

    return BotConfig(
        symbol=_s("symbol", "BTCUSDT"),
        leverage=_i("leverage", 10),
        margin_usdt=_f("margin_usdt", 10.0),
        max_dca=_i("max_dca", 3),
        max_re_hedge=_i("max_re_hedge", 3),
        min_signal_strength=_f("min_signal_strength", 0.25),
        timeframe=_s("timeframe", "1m"),
        trading_mode=trading_mode,
        key_type=key_type,
        discord_webhook=_s("discord_webhook", ""),
        auto_switch=_b("auto_switch", True),
        scan_interval_s=_i("scan_interval_s", 30),
        switch_threshold=_f("switch_threshold", 1.1),
        api_key=api_key,
        api_secret=api_secret,
    )
