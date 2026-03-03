"""
db.py — PostgreSQL interface for the trading signal bot.
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create tables and indexes if they don't already exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id          SERIAL PRIMARY KEY,
                    ticker      VARCHAR(20)  NOT NULL,
                    signal_type VARCHAR(60)  NOT NULL,
                    instruction TEXT         NOT NULL,
                    entry       NUMERIC(14, 4),
                    sl          NUMERIC(14, 4),
                    tp          NUMERIC(14, 4),
                    rr_ratio    NUMERIC(6, 2),
                    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                );
            """)
            # One signal per ticker/type per trading day (NY time) — prevents duplicate spam
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS signals_daily_uniq
                ON signals (
                    ticker,
                    signal_type,
                    (created_at AT TIME ZONE 'America/New_York')::date
                );
            """)
            conn.commit()
    log.info("Database initialised.")


def save_signal(ticker: str, signal: dict) -> bool:
    """
    Persist a signal. Returns True if a new row was inserted,
    False if a duplicate was silently skipped.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO signals
                        (ticker, signal_type, instruction, entry, sl, tp, rr_ratio)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT signals_daily_uniq DO NOTHING
                    """,
                    (
                        ticker,
                        signal["type"],
                        signal["instruction"],
                        signal["entry"],
                        signal["sl"],
                        signal["tp"],
                        signal.get("rr", 0),
                    ),
                )
                conn.commit()
                return cur.rowcount > 0
            except Exception as exc:
                conn.rollback()
                log.error("DB save_signal error: %s", exc)
                return False


def get_recent_signals(
    limit: int = 50,
    hours: int = 24,
    ticker: str | None = None,
    signal_type: str | None = None,
) -> list[dict]:
    """Return signals from the last N hours, newest first."""
    conditions = ["created_at > NOW() - (%s * INTERVAL '1 hour')"]
    params: list = [hours]

    if ticker:
        conditions.append("ticker = %s")
        params.append(ticker.upper())
    if signal_type:
        conditions.append("signal_type = %s")
        params.append(signal_type.upper())

    params.append(limit)
    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            id, ticker, signal_type, instruction,
            entry::float, sl::float, tp::float, rr_ratio::float,
            created_at
        FROM signals
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        {
            "id":          row["id"],
            "ticker":      row["ticker"],
            "type":        row["signal_type"],
            "instruction": row["instruction"],
            "entry":       row["entry"],
            "sl":          row["sl"],
            "tp":          row["tp"],
            "rr":          row["rr_ratio"],
            "time":        row["created_at"].isoformat(),
        }
        for row in rows
    ]
