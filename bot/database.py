"""
database.py — aiosqlite schema, migrations, and CRUD helpers.
"""
import time
import aiosqlite
from typing import Any, Dict, List, Optional

DB_PATH = "trading_bot.db"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    open_time        REAL NOT NULL,
    close_time       REAL,
    symbol           TEXT NOT NULL DEFAULT 'BTCUSDT',
    direction        TEXT NOT NULL,
    entry_price      REAL NOT NULL,
    avg_price        REAL NOT NULL,
    exit_price       REAL,
    qty              REAL NOT NULL,
    margin           REAL NOT NULL,
    leverage         INTEGER NOT NULL,
    dca_count        INTEGER NOT NULL DEFAULT 0,
    hedge_count      INTEGER NOT NULL DEFAULT 0,
    pnl              REAL,
    status           TEXT NOT NULL DEFAULT 'open',
    exit_reason      TEXT,
    entry_reason     TEXT,
    signal_strength  REAL,
    signal_price     REAL
);

CREATE TABLE IF NOT EXISTS hedge_positions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    INTEGER NOT NULL,
    open_time     REAL NOT NULL,
    close_time    REAL,
    direction     TEXT NOT NULL,
    entry_price   REAL NOT NULL,
    qty           REAL NOT NULL,
    margin        REAL NOT NULL,
    pnl           REAL,
    status        TEXT NOT NULL DEFAULT 'open',
    FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS signal_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL NOT NULL,
    symbol      TEXT NOT NULL,
    direction   TEXT NOT NULL,
    strength    REAL NOT NULL,
    flow        REAL,
    trend       REAL,
    momentum    REAL,
    mean_rev    REAL,
    rsi_score   REAL,
    action      TEXT
);

CREATE TABLE IF NOT EXISTS pos_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    event   TEXT    NOT NULL,
    payload TEXT    NOT NULL,
    ts      INTEGER NOT NULL
);
"""

_DEFAULT_CONFIG: Dict[str, str] = {
    "symbol":              "BTCUSDT",
    "leverage":            "10",
    "margin_usdt":         "10",
    "max_dca":             "3",
    "max_re_hedge":        "0",
    "smart_dca_gate":          "1",
    "smart_dca_signals":       "2",
    "breakeven_stop":          "1",
    "last_resort_sl_buffer":   "0.80",
    "min_signal_strength": "0.35",
    "trading_mode":        "paper",
    "discord_webhook":     "",
    "auto_switch":         "1",
    "scan_interval_s":     "5",
    "timeframe":           "1m",
    "trading_active":      "0",
    "switch_threshold":    "1.1",
    "entry_wait_s":        "45",    # seconds to wait for entry before trying next symbol
    "cooldown_after_stop_s": "300",
    "max_daily_loss_usdt":   "0",
    "dca_multiplier":        "1.0",
    "partial_tp":            "0",
    "partial_tp_ratio":      "0.5",
    "taker_fee_pct":         "0.04",
    "paper_slippage_pct":    "0.05",
    "strength_sizing":       "1",
    "strength_size_min":     "0.5",
    "stoch_signal":          "1",
}



async def init_db() -> None:
    """Create tables and seed default config if missing."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_DDL)

        # Apply any missing columns for forward-compatibility
        await _migrate(db)

        # Seed default config keys that don't exist yet
        for key, val in _DEFAULT_CONFIG.items():
            await db.execute(
                "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)",
                (key, val),
            )
        await db.commit()


async def _migrate(db: aiosqlite.Connection) -> None:
    """Apply any schema migrations that may be missing on an older DB."""
    for col, defn in [
        # sessions columns added in v2
        ("entry_reason",    "TEXT"),
        ("signal_strength", "REAL"),
        ("symbol",          "TEXT DEFAULT 'BTCUSDT'"),
        # sessions columns added in v3 — trail state persistence
        ("trail_active",    "INTEGER DEFAULT 0"),
        ("trail_price",     "REAL"),
        # sessions columns added in v4 — signal price
        ("signal_price",    "REAL"),
        # sessions columns added in v5 — exit fill price
        ("exit_price",      "REAL"),
    ]:
        try:
            await db.execute(f"ALTER TABLE sessions ADD COLUMN {col} {defn}")
        except Exception:
            pass  # column already exists
    # Create pos_log table if missing (older DBs won't have it)
    await db.execute(
        """CREATE TABLE IF NOT EXISTS pos_log (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            event   TEXT    NOT NULL,
            payload TEXT    NOT NULL,
            ts      INTEGER NOT NULL
        )"""
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------

async def get_all_config() -> Dict[str, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT key, value FROM config")
        rows = await cursor.fetchall()
        return {r["key"]: r["value"] for r in rows}


async def get_config(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row["value"] if row else None


async def set_config(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await db.commit()


async def set_config_bulk(updates: Dict[str, str]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        for key, value in updates.items():
            await db.execute(
                "INSERT INTO config (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------

async def create_session(
    symbol: str,
    direction: str,
    entry_price: float,
    qty: float,
    margin: float,
    leverage: int,
    entry_reason: str = "",
    signal_strength: float = 0.0,
    signal_price: float = 0.0,
) -> int:
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO sessions
               (open_time, symbol, direction, entry_price, avg_price,
                qty, margin, leverage, dca_count, hedge_count, status,
                entry_reason, signal_strength, signal_price)
               VALUES (?,?,?,?,?,?,?,?,0,0,'open',?,?,?)""",
            (now, symbol, direction, entry_price, entry_price,
             qty, margin, leverage, entry_reason, signal_strength, signal_price),
        )
        await db.commit()
        return cursor.lastrowid


async def get_open_session() -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM sessions WHERE status='open' ORDER BY open_time DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def update_session(session_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    cols = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [session_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE sessions SET {cols} WHERE id=?", vals)
        await db.commit()


async def close_session(
    session_id: int,
    pnl: float,
    reason: str,
    exit_price: Optional[float] = None,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE sessions "
            "SET status='closed', close_time=?, pnl=?, exit_reason=?, exit_price=? "
            "WHERE id=?",
            (time.time(), pnl, reason, exit_price, session_id),
        )
        await db.commit()


async def get_sessions(limit: int = 100) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM sessions ORDER BY open_time DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Hedge position CRUD
# ---------------------------------------------------------------------------

async def create_hedge(
    session_id: int,
    direction: str,
    entry_price: float,
    qty: float,
    margin: float,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO hedge_positions
               (session_id, open_time, direction, entry_price, qty, margin, status)
               VALUES (?,?,?,?,?,?,'open')""",
            (session_id, time.time(), direction, entry_price, qty, margin),
        )
        await db.commit()
        return cursor.lastrowid


async def get_open_hedges(session_id: int) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM hedge_positions WHERE session_id=? AND status='open'",
            (session_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_all_closed_hedges(limit: int = 200) -> List[Dict[str, Any]]:
    """Return closed hedge positions joined with their parent session's symbol."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT h.*, s.symbol, s.leverage
               FROM hedge_positions h
               JOIN sessions s ON h.session_id = s.id
               WHERE h.status = 'closed'
               ORDER BY h.open_time DESC LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def delete_closed_hedge(hedge_id: int) -> None:
    """Delete a single closed hedge position by id."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM hedge_positions WHERE id=? AND status='closed'",
            (hedge_id,),
        )
        await db.commit()


async def close_hedge(hedge_id: int, pnl: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE hedge_positions SET status='closed', close_time=?, pnl=? WHERE id=?",
            (time.time(), pnl, hedge_id),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Signal log CRUD
# ---------------------------------------------------------------------------

async def log_signal(
    symbol: str,
    direction: str,
    strength: float,
    components: Dict[str, float],
    action: str,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO signal_log
               (timestamp, symbol, direction, strength, flow, trend, momentum, mean_rev, rsi_score, action)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                time.time(),
                symbol,
                direction,
                strength,
                components.get("flow"),
                components.get("trend"),
                components.get("momentum"),
                components.get("mean_rev"),
                components.get("rsi"),
                action,
            ),
        )
        # Trim to last 5000
        await db.execute(
            "DELETE FROM signal_log WHERE id NOT IN "
            "(SELECT id FROM signal_log ORDER BY id DESC LIMIT 5000)"
        )
        await db.commit()


async def get_signal_log(limit: int = 100) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM signal_log ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Position log — persisted across restarts
# ---------------------------------------------------------------------------

async def insert_pos_log(event: str, payload: dict) -> None:
    import json
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR IGNORE INTO pos_log (event, payload, ts)
               VALUES (?, ?, ?)""",
            (event, json.dumps(payload), payload.get("ts", int(time.time()))),
        )
        await db.execute(
            "DELETE FROM pos_log WHERE id NOT IN "
            "(SELECT id FROM pos_log ORDER BY id DESC LIMIT 2000)"
        )
        await db.commit()


async def get_pos_log(limit: int = 30, before_id: Optional[int] = None) -> List[Dict[str, Any]]:
    import json
    async with aiosqlite.connect(DB_PATH) as db:
        if before_id:
            cur = await db.execute(
                "SELECT id, event, payload, ts FROM pos_log "
                "WHERE id < ? ORDER BY id DESC LIMIT ?",
                (before_id, limit),
            )
        else:
            cur = await db.execute(
                "SELECT id, event, payload, ts FROM pos_log "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        rows = await cur.fetchall()
    result = []
    for row in rows:
        entry = json.loads(row[2])
        entry["_id"] = row[0]
        result.append(entry)
    return result


async def clear_pos_log() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM pos_log")
        await db.commit()


# ---------------------------------------------------------------------------
# Performance stats
# ---------------------------------------------------------------------------

async def delete_session(session_id: int) -> None:
    """Delete a single closed session (and its hedge positions) by id."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM hedge_positions WHERE session_id=?", (session_id,))
        await db.execute("DELETE FROM sessions WHERE id=? AND status='closed'", (session_id,))
        await db.commit()


async def delete_all_sessions() -> None:
    """Delete all closed sessions and their hedge positions."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM hedge_positions WHERE session_id IN "
            "(SELECT id FROM sessions WHERE status='closed')"
        )
        await db.execute("DELETE FROM sessions WHERE status='closed'")
        await db.commit()


async def get_performance() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            "SELECT pnl, open_time, close_time FROM sessions WHERE status='closed'"
        )
        session_rows = await cursor.fetchall()

        cursor2 = await db.execute(
            "SELECT pnl, open_time, close_time FROM hedge_positions WHERE status='closed'"
        )
        hedge_rows = await cursor2.fetchall()

    rows = list(session_rows) + list(hedge_rows)

    if not rows:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_duration_s": 0.0,
            "max_drawdown": 0.0,
            "total_pnl": 0.0,
        }

    pnls = [r["pnl"] for r in rows if r["pnl"] is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

    durations = [
        r["close_time"] - r["open_time"]
        for r in rows
        if r["close_time"] and r["open_time"]
    ]
    avg_duration = sum(durations) / len(durations) if durations else 0.0

    # Max drawdown from equity curve
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    return {
        "total_trades": len(rows),
        "win_rate": len(wins) / len(pnls) if pnls else 0.0,
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "avg_duration_s": round(avg_duration, 1),
        "max_drawdown": round(max_dd, 4),
        "total_pnl": round(sum(pnls), 4),
    }


async def get_today_pnl() -> float:
    """Sum of realized PnL for all sessions closed today (UTC midnight)."""
    import calendar
    t = time.gmtime()
    midnight_utc = float(calendar.timegm((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0)))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT COALESCE(SUM(pnl), 0.0) AS total FROM sessions "
            "WHERE status='closed' AND close_time >= ?",
            (midnight_utc,),
        )
        row = await cursor.fetchone()
        return float(row["total"]) if row else 0.0
