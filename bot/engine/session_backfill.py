"""
engine/session_backfill.py

One-time (or on-demand) historical backfill of session_history table.

Algorithm for each day in the backfill window:
  1. Fetch 1M klines for the full UTC day
  2. Split candles into session windows (PKT or configured timezone)
  3. Compute Asian high/low from Asian session candles
  4. Analyze London session: did price sweep Asian high/low? Which first?
     How many minutes after open? What direction moved after the sweep?
  5. Analyze NY session: continuation or reversal of London direction?
  6. Save to session_history (upsert — safe to re-run)
"""
import asyncio
import datetime
import logging
from typing import List, Optional, Tuple

from database import get_session_history, save_session_history

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def backfill_session_history(
    rest_client,
    cfg,
    days: int = 60,
    tz_offset: int = None,
    broadcast=None,
) -> None:
    """
    Analyze last `days` days of 1M historical data and populate
    session_history for cfg.symbol.

    Safe to call multiple times — uses upsert (INSERT OR REPLACE).
    Skips days that already have records.

    broadcast: optional callable(dict) — sends notifications to UI clients.
    """
    tz = tz_offset if tz_offset is not None else cfg.session_timezone_offset

    def _notify(text: str) -> None:
        logger.info("session_backfill: %s", text)
        if broadcast:
            broadcast({"type": "notification", "text": f"[Backfill] {text}"})

    # ── Check existing coverage ───────────────────────────────────────────
    existing = await get_session_history(cfg.symbol, "london", bars=days * 2)
    existing_dates = {r["date"] for r in existing}

    if len(existing_dates) >= int(days * 0.8):
        logger.info(
            "session_backfill: %s already has %d records — skipping",
            cfg.symbol, len(existing_dates),
        )
        return

    # ── Build list of dates to process ───────────────────────────────────
    today_utc = datetime.datetime.utcnow().date()
    dates_to_process = []
    for i in range(days, 0, -1):
        day = today_utc - datetime.timedelta(days=i)
        if day.strftime("%Y-%m-%d") not in existing_dates:
            dates_to_process.append(day)

    if not dates_to_process:
        logger.info("session_backfill: nothing new to backfill")
        return

    total = len(dates_to_process)
    _notify(f"Building session history for {cfg.symbol} — {total} days to fetch…")

    # ── Process each day ─────────────────────────────────────────────────
    processed = 0
    for idx, day in enumerate(dates_to_process):
        try:
            start_ms = int(
                datetime.datetime(day.year, day.month, day.day,
                                  tzinfo=datetime.timezone.utc).timestamp() * 1000
            )
            end_ms = start_ms + 86_400_000   # +24 hours

            raw = await _fetch_klines_range(
                rest_client, cfg.symbol, "1m", start_ms, end_ms
            )

            if not raw or len(raw) < 60:
                logger.debug(
                    "session_backfill: %s — only %d candles, skipping",
                    day, len(raw) if raw else 0,
                )
                continue

            candles = _parse_klines(raw)
            asian, london, ny = _split_sessions(candles, tz)

            if not asian or not london:
                logger.debug("session_backfill: %s — missing asian or london candles", day)
                continue

            asian_high = max(c["high"] for c in asian)
            asian_low  = min(c["low"]  for c in asian)
            date_str   = day.strftime("%Y-%m-%d")

            london_record = _analyze_london(
                london, asian_high, asian_low, date_str, cfg.symbol
            )
            ny_record = _analyze_ny(ny, london, date_str, cfg.symbol)

            if london_record:
                london_record["asian_high"] = asian_high
                london_record["asian_low"]  = asian_low
                await save_session_history(london_record)

            if ny_record:
                ny_record["asian_high"] = asian_high
                ny_record["asian_low"]  = asian_low
                await save_session_history(ny_record)

            processed += 1

            # Progress update every 10 days
            if processed % 10 == 0:
                _notify(f"{cfg.symbol} history: {processed}/{total} days ({processed*100//total}%)")

            # Small delay between days to avoid hammering the REST API
            await asyncio.sleep(0.3)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("session_backfill: error on %s: %s", day, exc)
            continue

    _notify(
        f"{cfg.symbol} session history ready — {processed} days loaded "
        f"({existing_dates.__len__() + processed} total records)"
    )


# ---------------------------------------------------------------------------
# REST helper — paginated kline fetch
# ---------------------------------------------------------------------------

async def _fetch_klines_range(
    rest_client,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list:
    """
    Fetch all 1M klines between start_ms and end_ms.
    Paginates automatically (Binance FAPI limit = 1500 per call).
    One full UTC day of 1M data = 1440 candles — fits in one request,
    but pagination is implemented for safety.
    """
    all_klines = []
    current_start = start_ms

    while current_start < end_ms:
        await rest_client._market_limiter.acquire()
        resp = await rest_client._client.get(
            "/fapi/v1/klines",
            params={
                "symbol":    symbol,
                "interval":  interval,
                "startTime": current_start,
                "endTime":   end_ms,
                "limit":     1500,
            },
        )
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break

        all_klines.extend(batch)

        last_open_ms = int(batch[-1][0])
        if last_open_ms <= current_start:
            break
        current_start = last_open_ms + 60_000   # advance by one 1m candle

        if len(batch) < 1500:
            break   # no more pages

    return all_klines


# ---------------------------------------------------------------------------
# Candle parsing
# ---------------------------------------------------------------------------

def _parse_klines(raw: list) -> list:
    return [
        {
            "time":   int(k[0]) // 1000,
            "open":   float(k[1]),
            "high":   float(k[2]),
            "low":    float(k[3]),
            "close":  float(k[4]),
            "volume": float(k[5]),
        }
        for k in raw
    ]


# ---------------------------------------------------------------------------
# Session splitter
# ---------------------------------------------------------------------------

def _split_sessions(
    candles: list,
    tz_offset: int,
) -> Tuple[list, list, list]:
    """
    Split 1M candles into Asian, London, NY buckets using local time.

    PKT (UTC+5) sessions:
      Asian:  04:00–13:00 local  (23:00–08:00 UTC, spans midnight)
      London: 13:00–18:00 local  (08:00–13:00 UTC)
      NY:     18:00–23:00 local  (13:00–18:00 UTC)
    """
    asian, london, ny = [], [], []

    for c in candles:
        utc_dt     = datetime.datetime.utcfromtimestamp(c["time"])
        local_hour = (utc_dt + datetime.timedelta(hours=tz_offset)).hour

        if 4 <= local_hour < 13:
            asian.append(c)
        elif 13 <= local_hour < 18:
            london.append(c)
        elif 18 <= local_hour < 23:
            ny.append(c)

    return asian, london, ny


# ---------------------------------------------------------------------------
# London analysis
# ---------------------------------------------------------------------------

def _analyze_london(
    london: list,
    asian_high: float,
    asian_low: float,
    date: str,
    symbol: str,
) -> dict:
    """
    Determine whether London swept Asian High or Low first, and in what
    direction price moved after the sweep.
    """
    if not london:
        return {}

    swept_high_at: Optional[int] = None
    swept_low_at:  Optional[int] = None

    for i, c in enumerate(london):
        if swept_high_at is None and c["high"] > asian_high:
            swept_high_at = i
        if swept_low_at is None and c["low"] < asian_low:
            swept_low_at = i

    if swept_high_at is None and swept_low_at is None:
        hunted    = "none"
        hunt_min  = None
        hunt_pts  = None
        direction = "none"

    elif swept_low_at is None or (
        swept_high_at is not None and swept_high_at <= swept_low_at
    ):
        # High swept first → SHORT setup
        hunted    = "asian_high"
        hunt_min  = swept_high_at
        hunt_pts  = round(london[swept_high_at]["high"] - asian_high, 4)
        after     = london[swept_high_at + 1:]
        direction = _direction_after_sweep(after, "short")

    else:
        # Low swept first → LONG setup
        hunted    = "asian_low"
        hunt_min  = swept_low_at
        hunt_pts  = round(asian_low - london[swept_low_at]["low"], 4)
        after     = london[swept_low_at + 1:]
        direction = _direction_after_sweep(after, "long")

    return {
        "date":             date,
        "session":          "london",
        "symbol":           symbol,
        "london_hunted":    hunted,
        "london_hunt_min":  hunt_min,
        "london_hunt_pts":  hunt_pts,
        "london_direction": direction,
        "ny_behavior":      None,
        "ny_open_price":    None,
        "ny_close_price":   None,
    }


def _direction_after_sweep(candles_after: list, expected: str) -> str:
    """
    After a sweep event, check the next 10 minutes of candles.
    If net move agrees with expected direction, return it; else 'none'.
    """
    if not candles_after:
        return "none"
    window = candles_after[:10]
    move   = window[-1]["close"] - window[0]["open"]
    if expected == "short" and move < 0:
        return "short"
    if expected == "long" and move > 0:
        return "long"
    return "none"


# ---------------------------------------------------------------------------
# NY analysis
# ---------------------------------------------------------------------------

def _analyze_ny(
    ny: list,
    london: list,
    date: str,
    symbol: str,
) -> dict:
    """
    Classify NY session as continuation or reversal of London direction.
    """
    if not ny or not london:
        return {}

    london_move = london[-1]["close"] - london[0]["open"]
    london_dir  = "long" if london_move > 0 else "short"

    ny_move = ny[-1]["close"] - ny[0]["open"]
    ny_dir  = "long" if ny_move > 0 else "short"

    behavior = "continuation" if london_dir == ny_dir else "reversal"

    return {
        "date":             date,
        "session":          "ny",
        "symbol":           symbol,
        "london_hunted":    None,
        "london_hunt_min":  None,
        "london_hunt_pts":  None,
        "london_direction": london_dir,
        "ny_behavior":      behavior,
        "ny_open_price":    round(ny[0]["open"],   6),
        "ny_close_price":   round(ny[-1]["close"],  6),
    }
