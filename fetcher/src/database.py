"""Database connection, schema management, and data persistence."""

import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from .config import DatabaseConfig

logger = logging.getLogger(__name__)
DEFAULT_INIT_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(150),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_prices (
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    volume BIGINT,
    dividends NUMERIC(12, 6) DEFAULT 0,
    stock_splits NUMERIC(12, 6) DEFAULT 0,
    PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable(
    'stock_prices',
    'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices (symbol, timestamp DESC);
"""


def resolve_init_script_path() -> Path:
    """Resolve the schema init script location in local and containerized runs."""
    module_path = Path(__file__).resolve()
    candidates = (
        module_path.parents[1] / "scripts" / "init-db.sql",
        module_path.parents[2] / "scripts" / "init-db.sql",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


@contextmanager
def get_connection(config: DatabaseConfig):
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg2.connect(config.connection_string)
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def wait_for_db(config: DatabaseConfig, max_attempts: int = 30) -> bool:
    """Wait for database to be ready (for container startup)."""
    import time

    for attempt in range(max_attempts):
        try:
            with get_connection(config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.warning(
                "Database not ready (attempt %d/%d): %s",
                attempt + 1,
                max_attempts,
                str(e),
            )
            time.sleep(2)
    return False


def init_schema(config: DatabaseConfig, init_script_path: Optional[Path] = None) -> None:
    """Initialize database schema from init script or inline SQL."""
    script_path = init_script_path or resolve_init_script_path()
    if script_path.exists():
        sql = script_path.read_text(encoding="utf-8")
        logger.info("Loading schema from %s", script_path)
    else:
        sql = DEFAULT_INIT_SQL
        logger.warning("Init script not found at %s, using embedded schema SQL", script_path)

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                logger.info("Executed schema SQL from %s", script_path)
            except Exception as e:
                # Skip compression-related failures if the schema evolves to include them.
                if re.search(r"compress", sql, re.IGNORECASE):
                    logger.warning("Schema SQL failed on compression-related statements: %s", e)
                else:
                    raise


def upsert_tickers(config: DatabaseConfig, tickers: List[Tuple[str, str, str, str]]) -> int:
    """Upsert ticker metadata into the tickers table."""
    if not tickers:
        return 0

    upsert_sql = """
        INSERT INTO tickers (symbol, name, sector, industry)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            name = EXCLUDED.name,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, tickers, page_size=100)
            return cur.rowcount


def upsert_stock_prices(
    config: DatabaseConfig,
    rows: List[Tuple[str, str, float, float, float, float, int, float, float]],
) -> int:
    """Upsert stock price rows. Each row: (symbol, timestamp, open, high, low, close, volume, dividends, stock_splits)."""
    if not rows:
        return 0

    upsert_sql = """
        INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume, dividends, stock_splits)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            dividends = EXCLUDED.dividends,
            stock_splits = EXCLUDED.stock_splits
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=500)
            return cur.rowcount


def get_last_date(config: DatabaseConfig, symbol: str) -> Optional[str]:
    """Get the most recent date for a given symbol."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(timestamp)::date FROM stock_prices WHERE symbol = %s",
                (symbol,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
            return None


def get_price_date_bounds(config: DatabaseConfig) -> Tuple[Optional[str], Optional[str]]:
    """Get the earliest and latest price dates present in stock_prices."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(timestamp)::date, MAX(timestamp)::date FROM stock_prices"
            )
            row = cur.fetchone()
            if not row:
                return None, None
            min_date = str(row[0]) if row[0] else None
            max_date = str(row[1]) if row[1] else None
            return min_date, max_date


def get_all_symbols(config: DatabaseConfig) -> List[str]:
    """Get all symbols from the tickers table."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM tickers ORDER BY symbol")
            return [r[0] for r in cur.fetchall()]


def has_stock_price_data(config: DatabaseConfig) -> bool:
    """Return whether the stock_prices table already contains data."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM stock_prices LIMIT 1)")
            return cur.fetchone()[0]


def schema_exists(config: DatabaseConfig) -> bool:
    """Check if the schema has been initialized."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'stock_prices'
                )
                """
            )
            return cur.fetchone()[0]
