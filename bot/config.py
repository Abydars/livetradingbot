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
    smart_dca_gate: bool
    smart_dca_signals: int
    breakeven_stop: bool
    last_resort_sl_buffer: float
    min_signal_strength: float
    cooldown_after_stop_s: int
    max_daily_loss_usdt: float
    dca_multiplier: float
    partial_tp: bool
    partial_tp_ratio: float
    taker_fee_pct: float
    paper_slippage_pct: float
    strength_sizing: bool
    strength_size_min: float
    stoch_signal: bool

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
    htf_filter: bool
    htf_timeframe: str   # empty = auto, otherwise e.g. "4h", "1d"
    flow_warmup_mult: float
    signal_persist_ticks: int
    switch_threshold: float  # min score ratio for #1 vs current to trigger switch
    entry_wait_candles: int # candles to wait with NEUTRAL signal before switching to next candidate
    scanner_top_n: int
    scanner_momentum:  bool
    scanner_breakout:  bool
    scanner_trendpull: bool
    scanner_breakdown: bool

    # Exchange secrets (env-only, never in DB)
    api_key: str
    api_secret: str

    @property
    def paper_mode(self) -> bool:
        return self.trading_mode == "paper"

    @property
    def tf_minutes(self) -> int:
        """Candle duration in minutes — used to scale tick-based timers."""
        return {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15,
            "30m": 30, "1h": 60, "2h": 120, "4h": 240,
        }.get(self.timeframe, 1)


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
        max_re_hedge=_i("max_re_hedge", 0),
        smart_dca_gate=_b("smart_dca_gate", True),
        smart_dca_signals=_i("smart_dca_signals", 2),
        breakeven_stop=_b("breakeven_stop", True),
        last_resort_sl_buffer=_f("last_resort_sl_buffer", 0.80),
        min_signal_strength=_f("min_signal_strength", 0.25),
        cooldown_after_stop_s=_i("cooldown_after_stop_s", 300),
        max_daily_loss_usdt=_f("max_daily_loss_usdt", 0.0),
        dca_multiplier=_f("dca_multiplier", 1.0),
        partial_tp=_b("partial_tp", False),
        partial_tp_ratio=_f("partial_tp_ratio", 0.5),
        taker_fee_pct=_f("taker_fee_pct", 0.04),
        paper_slippage_pct=_f("paper_slippage_pct", 0.05),
        strength_sizing=_b("strength_sizing", True),
        strength_size_min=_f("strength_size_min", 0.5),
        stoch_signal=_b("stoch_signal", True),
        timeframe=_s("timeframe", "1m"),
        trading_mode=trading_mode,
        key_type=key_type,
        discord_webhook=_s("discord_webhook", ""),
        auto_switch=_b("auto_switch", True),
        scan_interval_s=_i("scan_interval_s", 10),
        htf_filter=_b("htf_filter", True),
        htf_timeframe=_s("htf_timeframe", ""),
        flow_warmup_mult=_f("flow_warmup_mult", 1.0),
        signal_persist_ticks=_i("signal_persist_ticks", 2),
        switch_threshold=_f("switch_threshold", 1.1),
        entry_wait_candles=_i("entry_wait_candles", 3),
        scanner_top_n=_i("scanner_top_n", 10),
        scanner_momentum=_b("scanner_momentum",  True),
        scanner_breakout=_b("scanner_breakout",   True),
        scanner_trendpull=_b("scanner_trendpull", True),
        scanner_breakdown=_b("scanner_breakdown", True),
        api_key=api_key,
        api_secret=api_secret,
    )
