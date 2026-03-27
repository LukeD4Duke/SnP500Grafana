"""Database connection, schema management, and data persistence."""

import json
import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Tuple

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

CREATE TABLE IF NOT EXISTS indicator_catalog (
    indicator_key TEXT PRIMARY KEY,
    indicator TEXT NOT NULL,
    output_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT NOT NULL,
    purpose_description TEXT NOT NULL DEFAULT '',
    value_interpretation TEXT NOT NULL DEFAULT '',
    source_library VARCHAR(32) NOT NULL,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    warmup_periods INTEGER NOT NULL DEFAULT 0,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE indicator_catalog
    ADD COLUMN IF NOT EXISTS purpose_description TEXT NOT NULL DEFAULT '';

ALTER TABLE indicator_catalog
    ADD COLUMN IF NOT EXISTS value_interpretation TEXT NOT NULL DEFAULT '';

ALTER TABLE indicator_catalog
    DROP COLUMN IF EXISTS insight_description;

CREATE TABLE IF NOT EXISTS stock_indicators (
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    indicator_key TEXT NOT NULL REFERENCES indicator_catalog (indicator_key) ON DELETE CASCADE,
    value_numeric DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, timestamp, indicator_key)
);

SELECT create_hypertable(
    'stock_indicators',
    'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE INDEX IF NOT EXISTS idx_stock_indicators_symbol_indicator_time
    ON stock_indicators (symbol, indicator_key, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_stock_indicators_indicator_time
    ON stock_indicators (indicator_key, timestamp DESC);
"""


def _json_value(value: object) -> str:
    """Return a JSON string without double-encoding existing JSON strings."""
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _decoded_json_value(value: object, default: object) -> object:
    """Decode a JSON column while tolerating adapter-specific return types."""
    if value is None:
        return default
    if isinstance(value, str):
        return json.loads(value)
    return value


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


def upsert_indicator_catalog(
    config: DatabaseConfig,
    rows: List[Tuple[str, str, str, str, str, str, str, str, object, int, bool]],
) -> int:
    """Upsert indicator definitions into the catalog table."""
    if not rows:
        return 0

    normalized_rows = [
        (
            indicator_key,
            indicator,
            output_name,
            display_name,
            category,
            purpose_description,
            value_interpretation,
            source_library,
            _json_value(default_params),
            warmup_periods,
            is_enabled,
        )
        for (
            indicator_key,
            indicator,
            output_name,
            display_name,
            category,
            purpose_description,
            value_interpretation,
            source_library,
            default_params,
            warmup_periods,
            is_enabled,
        ) in rows
    ]

    upsert_sql = """
        INSERT INTO indicator_catalog (
            indicator_key,
            indicator,
            output_name,
            display_name,
            category,
            purpose_description,
            value_interpretation,
            source_library,
            default_params,
            warmup_periods,
            is_enabled
        )
        VALUES %s
        ON CONFLICT (indicator_key) DO UPDATE SET
            indicator = EXCLUDED.indicator,
            output_name = EXCLUDED.output_name,
            display_name = EXCLUDED.display_name,
            category = EXCLUDED.category,
            purpose_description = EXCLUDED.purpose_description,
            value_interpretation = EXCLUDED.value_interpretation,
            source_library = EXCLUDED.source_library,
            default_params = EXCLUDED.default_params,
            warmup_periods = EXCLUDED.warmup_periods,
            is_enabled = EXCLUDED.is_enabled,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=200)
            return cur.rowcount


def get_indicator_catalog(config: DatabaseConfig, only_enabled: bool = False) -> List[dict]:
    """Fetch indicator catalog entries ordered by key."""
    query = """
        SELECT
            indicator_key,
            indicator,
            output_name,
            display_name,
            category,
            purpose_description,
            value_interpretation,
            source_library,
            default_params,
            warmup_periods,
            is_enabled,
            updated_at
        FROM indicator_catalog
    """
    params: Tuple[object, ...] = ()
    if only_enabled:
        query += " WHERE is_enabled = TRUE"
    query += " ORDER BY indicator_key"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    catalog: List[dict] = []
    for row in rows:
        catalog.append(
            {
                "indicator_key": row[0],
                "indicator": row[1],
                "output_name": row[2],
                "display_name": row[3],
                "category": row[4],
                "purpose_description": row[5],
                "value_interpretation": row[6],
                "source_library": row[7],
                "library": row[7],
                "default_params": _decoded_json_value(row[8], {}),
                "warmup_periods": row[9],
                "is_enabled": row[10],
                "updated_at": row[11],
            }
        )
    return catalog


def get_stock_price_history(
    config: DatabaseConfig,
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[dict]:
    """Fetch OHLCV history for a single symbol ordered by timestamp.

    Returns a list of dictionaries that can be passed directly to pandas.DataFrame().
    """
    query = """
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            dividends,
            stock_splits
        FROM stock_prices
        WHERE symbol = %s
    """
    params: List[object] = [symbol]
    if start_date is not None:
        query += " AND timestamp >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND timestamp <= %s"
        params.append(end_date)
    query += " ORDER BY timestamp"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            return [dict(zip(columns, row)) for row in rows]


def get_recent_stock_price_history(
    config: DatabaseConfig,
    symbol: str,
    limit: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[dict]:
    """Fetch the most recent OHLCV rows for a symbol, returned in ascending order."""
    if limit <= 0:
        return []

    query = """
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            dividends,
            stock_splits
        FROM (
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                dividends,
                stock_splits
            FROM stock_prices
            WHERE symbol = %s
    """
    params: List[object] = [symbol]
    if start_date is not None:
        query += " AND timestamp >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND timestamp <= %s"
        params.append(end_date)
    query += """
            ORDER BY timestamp DESC
            LIMIT %s
        ) recent_prices
        ORDER BY timestamp ASC
    """
    params.append(limit)

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            return [dict(zip(columns, row)) for row in rows]


def has_stock_split_in_window(
    config: DatabaseConfig,
    symbol: str,
    start_timestamp: str,
    end_timestamp: str,
) -> bool:
    """Return whether a symbol has any real non-zero stock split rows in the given window."""
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM stock_prices
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
              AND stock_splits IS NOT NULL
              AND stock_splits::text <> 'NaN'
              AND stock_splits <> 0
        )
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol, start_timestamp, end_timestamp))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def normalize_invalid_stock_splits(config: DatabaseConfig) -> int:
    """Rewrite persisted NaN stock split values to zero."""
    query = """
        UPDATE stock_prices
        SET stock_splits = 0
        WHERE stock_splits IS NOT NULL
          AND stock_splits::text = 'NaN'
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount


def get_max_enabled_indicator_warmup_period(config: DatabaseConfig) -> int:
    """Return the maximum warmup period across enabled indicators."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(warmup_periods), 0)
                FROM indicator_catalog
                WHERE is_enabled = TRUE
                """
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0


def upsert_stock_indicators(
    config: DatabaseConfig,
    rows: List[Tuple[str, str, str, Optional[float]]],
) -> int:
    """Upsert indicator rows into the long-form storage table."""
    if not rows:
        return 0

    normalized_rows = [
        (
            symbol,
            timestamp,
            indicator_key,
            value_numeric,
        )
        for (symbol, timestamp, indicator_key, value_numeric) in rows
    ]

    upsert_sql = """
        INSERT INTO stock_indicators (
            symbol,
            timestamp,
            indicator_key,
            value_numeric
        )
        VALUES %s
        ON CONFLICT (symbol, timestamp, indicator_key) DO UPDATE SET
            value_numeric = EXCLUDED.value_numeric,
            computed_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=500)
            return cur.rowcount


def delete_stock_indicators(
    config: DatabaseConfig,
    symbol: Optional[str] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    indicator_key: Optional[str] = None,
) -> int:
    """Delete indicator rows for a symbol/time window."""
    query = "DELETE FROM stock_indicators"
    conditions = []
    params: List[object] = []

    if symbol is not None:
        conditions.append("symbol = %s")
        params.append(symbol)
    if start_timestamp is not None:
        conditions.append("timestamp >= %s")
        params.append(start_timestamp)
    if end_timestamp is not None:
        conditions.append("timestamp <= %s")
        params.append(end_timestamp)
    if indicator_key is not None:
        conditions.append("indicator_key = %s")
        params.append(indicator_key)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
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
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN (
                      'tickers',
                      'stock_prices',
                      'indicator_catalog',
                      'stock_indicators'
                  )
                """
            )
            return cur.fetchone()[0] == 4
