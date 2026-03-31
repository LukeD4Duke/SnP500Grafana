"""Database connection, schema management, and data persistence."""

import json
import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from .config import DatabaseConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StockPriceUpsertResult:
    """Summary of persisted OHLCV changes from an upsert run."""

    affected_row_count: int
    changed_symbols: list[str]


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

CREATE TABLE IF NOT EXISTS signal_snapshots (
    snapshot_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES tickers (symbol) ON DELETE CASCADE,
    timeframe TEXT NOT NULL,
    last_timestamp TIMESTAMPTZ,
    close DOUBLE PRECISION,
    volume BIGINT,
    trend_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    momentum_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    relative_strength_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    structure_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    mean_reversion_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    volatility_risk_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0,
    final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    trend_state TEXT NOT NULL DEFAULT 'neutral',
    momentum_state TEXT NOT NULL DEFAULT 'neutral',
    volume_state TEXT NOT NULL DEFAULT 'neutral',
    relative_strength_state TEXT NOT NULL DEFAULT 'neutral',
    structure_state TEXT NOT NULL DEFAULT 'neutral',
    volatility_state TEXT NOT NULL DEFAULT 'neutral',
    regime_label TEXT NOT NULL DEFAULT 'neutral',
    recommendation_label TEXT NOT NULL DEFAULT 'watch',
    breakout_flag BOOLEAN NOT NULL DEFAULT FALSE,
    breakdown_flag BOOLEAN NOT NULL DEFAULT FALSE,
    overbought_flag BOOLEAN NOT NULL DEFAULT FALSE,
    oversold_flag BOOLEAN NOT NULL DEFAULT FALSE,
    trend_alignment_flag BOOLEAN NOT NULL DEFAULT FALSE,
    data_quality_flag BOOLEAN NOT NULL DEFAULT FALSE,
    drivers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, symbol, timeframe)
);

ALTER TABLE signal_snapshots
    ADD COLUMN IF NOT EXISTS last_timestamp TIMESTAMPTZ;

ALTER TABLE signal_snapshots
    ADD COLUMN IF NOT EXISTS drivers_json JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_signal_snapshots_timeframe_score
    ON signal_snapshots (timeframe, snapshot_date DESC, final_score DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_signal_snapshots_symbol_timeframe
    ON signal_snapshots (symbol, timeframe, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS rank_snapshots (
    snapshot_date DATE NOT NULL,
    timeframe TEXT NOT NULL,
    symbol VARCHAR(10) NOT NULL REFERENCES tickers (symbol) ON DELETE CASCADE,
    final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    bull_rank INTEGER,
    bear_rank INTEGER,
    regime_label TEXT NOT NULL DEFAULT 'neutral',
    recommendation_label TEXT NOT NULL DEFAULT 'watch',
    score_change_1w DOUBLE PRECISION,
    score_change_1m DOUBLE PRECISION,
    in_top20_bull BOOLEAN NOT NULL DEFAULT FALSE,
    in_top20_bear BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, timeframe, symbol)
);

CREATE INDEX IF NOT EXISTS idx_rank_snapshots_bull
    ON rank_snapshots (timeframe, snapshot_date DESC, bull_rank, symbol);

CREATE INDEX IF NOT EXISTS idx_rank_snapshots_bear
    ON rank_snapshots (timeframe, snapshot_date DESC, bear_rank, symbol);

CREATE TABLE IF NOT EXISTS market_breadth_snapshots (
    snapshot_date DATE NOT NULL,
    timeframe TEXT NOT NULL,
    universe_size INTEGER NOT NULL DEFAULT 0,
    bullish_count INTEGER NOT NULL DEFAULT 0,
    neutral_count INTEGER NOT NULL DEFAULT 0,
    bearish_count INTEGER NOT NULL DEFAULT 0,
    pct_above_ema20 DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_above_ema50 DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_above_ema200 DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_new_20d_high DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_new_20d_low DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_near_52w_high DOUBLE PRECISION NOT NULL DEFAULT 0,
    pct_near_52w_low DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    median_final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_market_breadth_snapshots_timeframe
    ON market_breadth_snapshots (timeframe, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS report_snapshots (
    snapshot_date DATE NOT NULL,
    report_kind TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    symbol VARCHAR(32) NOT NULL DEFAULT '__MARKET__',
    title TEXT NOT NULL DEFAULT '',
    final_score DOUBLE PRECISION,
    regime_label TEXT,
    recommendation_label TEXT,
    summary_text TEXT NOT NULL DEFAULT '',
    risk_text TEXT NOT NULL DEFAULT '',
    key_drivers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    report_markdown TEXT NOT NULL DEFAULT '',
    report_html TEXT NOT NULL DEFAULT '',
    storage_path TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, report_kind, timeframe, symbol)
);

CREATE INDEX IF NOT EXISTS idx_report_snapshots_lookup
    ON report_snapshots (report_kind, timeframe, snapshot_date DESC, symbol);

CREATE TABLE IF NOT EXISTS report_export_jobs (
    job_id TEXT PRIMARY KEY,
    report_kind TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    snapshot_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT NOT NULL DEFAULT '',
    html_path TEXT NOT NULL DEFAULT '',
    pdf_path TEXT NOT NULL DEFAULT '',
    html_download_url TEXT NOT NULL DEFAULT '',
    pdf_download_url TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_report_export_jobs_lookup
    ON report_export_jobs (report_kind, timeframe, scope, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_report_export_jobs_status
    ON report_export_jobs (status, created_at DESC);
"""


def _json_value(value: object) -> str:
    """Return a JSON string without double-encoding existing JSON strings."""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str, allow_nan=False)


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


def _rows_to_dicts(cursor) -> List[dict]:
    columns = [desc[0] for desc in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _prepare_dict_rows(
    rows: List[dict],
    columns: Tuple[str, ...],
    json_columns: Optional[set[str]] = None,
) -> List[Tuple[object, ...]]:
    normalized: List[Tuple[object, ...]] = []
    json_keys = json_columns or set()
    for row in rows:
        normalized.append(
            tuple(
                _json_value(row.get(column)) if column in json_keys else row.get(column)
                for column in columns
            )
        )
    return normalized


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
) -> StockPriceUpsertResult:
    """Upsert stock price rows. Each row: (symbol, timestamp, open, high, low, close, volume, dividends, stock_splits)."""
    if not rows:
        return StockPriceUpsertResult(affected_row_count=0, changed_symbols=[])

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
        WHERE (
            stock_prices.open,
            stock_prices.high,
            stock_prices.low,
            stock_prices.close,
            stock_prices.volume,
            stock_prices.dividends,
            stock_prices.stock_splits
        ) IS DISTINCT FROM (
            EXCLUDED.open,
            EXCLUDED.high,
            EXCLUDED.low,
            EXCLUDED.close,
            EXCLUDED.volume,
            EXCLUDED.dividends,
            EXCLUDED.stock_splits
        )
        RETURNING symbol
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=500)
            changed_symbols = sorted({str(row[0]) for row in cur.fetchall()})
            return StockPriceUpsertResult(
                affected_row_count=cur.rowcount,
                changed_symbols=changed_symbols,
            )


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


def get_recent_price_history_for_all_symbols(config: DatabaseConfig, limit: int) -> List[dict]:
    """Fetch the most recent OHLCV rows per symbol, returned in ascending timestamp order."""
    if limit <= 0:
        return []

    query = """
        WITH ranked_prices AS (
            SELECT
                symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                dividends,
                stock_splits,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS row_num
            FROM stock_prices
        )
        SELECT
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            dividends,
            stock_splits
        FROM ranked_prices
        WHERE row_num <= %s
        ORDER BY symbol, timestamp ASC
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
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


def get_price_history_dataset(
    config: DatabaseConfig,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    symbols: Optional[List[str]] = None,
) -> List[dict]:
    """Fetch OHLCV history across the universe or a symbol subset."""
    query = """
        SELECT
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            dividends,
            stock_splits
        FROM stock_prices
        WHERE 1 = 1
    """
    params: List[object] = []
    if start_date is not None:
        query += " AND timestamp >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND timestamp <= %s"
        params.append(end_date)
    if symbols:
        query += " AND symbol = ANY(%s)"
        params.append(symbols)
    query += " ORDER BY symbol, timestamp"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def upsert_signal_snapshots(config: DatabaseConfig, rows: List[dict]) -> int:
    """Upsert per-symbol analytics snapshots."""
    if not rows:
        return 0

    columns = (
        "snapshot_date",
        "symbol",
        "timeframe",
        "last_timestamp",
        "close",
        "volume",
        "trend_score",
        "momentum_score",
        "volume_score",
        "relative_strength_score",
        "structure_score",
        "mean_reversion_score",
        "volatility_risk_score",
        "risk_penalty",
        "final_score",
        "trend_state",
        "momentum_state",
        "volume_state",
        "relative_strength_state",
        "structure_state",
        "volatility_state",
        "regime_label",
        "recommendation_label",
        "breakout_flag",
        "breakdown_flag",
        "overbought_flag",
        "oversold_flag",
        "trend_alignment_flag",
        "data_quality_flag",
        "drivers_json",
    )
    normalized_rows = _prepare_dict_rows(rows, columns, json_columns={"drivers_json"})
    upsert_sql = """
        INSERT INTO signal_snapshots (
            snapshot_date,
            symbol,
            timeframe,
            last_timestamp,
            close,
            volume,
            trend_score,
            momentum_score,
            volume_score,
            relative_strength_score,
            structure_score,
            mean_reversion_score,
            volatility_risk_score,
            risk_penalty,
            final_score,
            trend_state,
            momentum_state,
            volume_state,
            relative_strength_state,
            structure_state,
            volatility_state,
            regime_label,
            recommendation_label,
            breakout_flag,
            breakdown_flag,
            overbought_flag,
            oversold_flag,
            trend_alignment_flag,
            data_quality_flag,
            drivers_json
        )
        VALUES %s
        ON CONFLICT (snapshot_date, symbol, timeframe) DO UPDATE SET
            last_timestamp = EXCLUDED.last_timestamp,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            trend_score = EXCLUDED.trend_score,
            momentum_score = EXCLUDED.momentum_score,
            volume_score = EXCLUDED.volume_score,
            relative_strength_score = EXCLUDED.relative_strength_score,
            structure_score = EXCLUDED.structure_score,
            mean_reversion_score = EXCLUDED.mean_reversion_score,
            volatility_risk_score = EXCLUDED.volatility_risk_score,
            risk_penalty = EXCLUDED.risk_penalty,
            final_score = EXCLUDED.final_score,
            trend_state = EXCLUDED.trend_state,
            momentum_state = EXCLUDED.momentum_state,
            volume_state = EXCLUDED.volume_state,
            relative_strength_state = EXCLUDED.relative_strength_state,
            structure_state = EXCLUDED.structure_state,
            volatility_state = EXCLUDED.volatility_state,
            regime_label = EXCLUDED.regime_label,
            recommendation_label = EXCLUDED.recommendation_label,
            breakout_flag = EXCLUDED.breakout_flag,
            breakdown_flag = EXCLUDED.breakdown_flag,
            overbought_flag = EXCLUDED.overbought_flag,
            oversold_flag = EXCLUDED.oversold_flag,
            trend_alignment_flag = EXCLUDED.trend_alignment_flag,
            data_quality_flag = EXCLUDED.data_quality_flag,
            drivers_json = EXCLUDED.drivers_json,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=250)
            return cur.rowcount


def upsert_rank_snapshots(config: DatabaseConfig, rows: List[dict]) -> int:
    """Upsert cross-sectional rank snapshots."""
    if not rows:
        return 0

    columns = (
        "snapshot_date",
        "timeframe",
        "symbol",
        "final_score",
        "bull_rank",
        "bear_rank",
        "regime_label",
        "recommendation_label",
        "score_change_1w",
        "score_change_1m",
        "in_top20_bull",
        "in_top20_bear",
    )
    normalized_rows = _prepare_dict_rows(rows, columns)
    upsert_sql = """
        INSERT INTO rank_snapshots (
            snapshot_date,
            timeframe,
            symbol,
            final_score,
            bull_rank,
            bear_rank,
            regime_label,
            recommendation_label,
            score_change_1w,
            score_change_1m,
            in_top20_bull,
            in_top20_bear
        )
        VALUES %s
        ON CONFLICT (snapshot_date, timeframe, symbol) DO UPDATE SET
            final_score = EXCLUDED.final_score,
            bull_rank = EXCLUDED.bull_rank,
            bear_rank = EXCLUDED.bear_rank,
            regime_label = EXCLUDED.regime_label,
            recommendation_label = EXCLUDED.recommendation_label,
            score_change_1w = EXCLUDED.score_change_1w,
            score_change_1m = EXCLUDED.score_change_1m,
            in_top20_bull = EXCLUDED.in_top20_bull,
            in_top20_bear = EXCLUDED.in_top20_bear,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=250)
            return cur.rowcount


def upsert_market_breadth_snapshots(config: DatabaseConfig, rows: List[dict]) -> int:
    """Upsert market breadth snapshots."""
    if not rows:
        return 0

    columns = (
        "snapshot_date",
        "timeframe",
        "universe_size",
        "bullish_count",
        "neutral_count",
        "bearish_count",
        "pct_above_ema20",
        "pct_above_ema50",
        "pct_above_ema200",
        "pct_new_20d_high",
        "pct_new_20d_low",
        "pct_near_52w_high",
        "pct_near_52w_low",
        "avg_final_score",
        "median_final_score",
    )
    normalized_rows = _prepare_dict_rows(rows, columns)
    upsert_sql = """
        INSERT INTO market_breadth_snapshots (
            snapshot_date,
            timeframe,
            universe_size,
            bullish_count,
            neutral_count,
            bearish_count,
            pct_above_ema20,
            pct_above_ema50,
            pct_above_ema200,
            pct_new_20d_high,
            pct_new_20d_low,
            pct_near_52w_high,
            pct_near_52w_low,
            avg_final_score,
            median_final_score
        )
        VALUES %s
        ON CONFLICT (snapshot_date, timeframe) DO UPDATE SET
            universe_size = EXCLUDED.universe_size,
            bullish_count = EXCLUDED.bullish_count,
            neutral_count = EXCLUDED.neutral_count,
            bearish_count = EXCLUDED.bearish_count,
            pct_above_ema20 = EXCLUDED.pct_above_ema20,
            pct_above_ema50 = EXCLUDED.pct_above_ema50,
            pct_above_ema200 = EXCLUDED.pct_above_ema200,
            pct_new_20d_high = EXCLUDED.pct_new_20d_high,
            pct_new_20d_low = EXCLUDED.pct_new_20d_low,
            pct_near_52w_high = EXCLUDED.pct_near_52w_high,
            pct_near_52w_low = EXCLUDED.pct_near_52w_low,
            avg_final_score = EXCLUDED.avg_final_score,
            median_final_score = EXCLUDED.median_final_score,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=64)
            return cur.rowcount


def upsert_report_snapshots(config: DatabaseConfig, rows: List[dict]) -> int:
    """Upsert report rows and rendered artifacts."""
    if not rows:
        return 0

    columns = (
        "snapshot_date",
        "report_kind",
        "timeframe",
        "symbol",
        "title",
        "final_score",
        "regime_label",
        "recommendation_label",
        "summary_text",
        "risk_text",
        "key_drivers_json",
        "report_markdown",
        "report_html",
        "storage_path",
    )
    normalized_rows = _prepare_dict_rows(rows, columns, json_columns={"key_drivers_json"})
    upsert_sql = """
        INSERT INTO report_snapshots (
            snapshot_date,
            report_kind,
            timeframe,
            symbol,
            title,
            final_score,
            regime_label,
            recommendation_label,
            summary_text,
            risk_text,
            key_drivers_json,
            report_markdown,
            report_html,
            storage_path
        )
        VALUES %s
        ON CONFLICT (snapshot_date, report_kind, timeframe, symbol) DO UPDATE SET
            title = EXCLUDED.title,
            final_score = EXCLUDED.final_score,
            regime_label = EXCLUDED.regime_label,
            recommendation_label = EXCLUDED.recommendation_label,
            summary_text = EXCLUDED.summary_text,
            risk_text = EXCLUDED.risk_text,
            key_drivers_json = EXCLUDED.key_drivers_json,
            report_markdown = EXCLUDED.report_markdown,
            report_html = EXCLUDED.report_html,
            storage_path = EXCLUDED.storage_path,
            updated_at = NOW()
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, normalized_rows, page_size=128)
            return cur.rowcount


def insert_report_export_job(config: DatabaseConfig, row: dict) -> int:
    """Insert a manual report export job row."""
    columns = (
        "job_id",
        "report_kind",
        "timeframe",
        "scope",
        "status",
        "snapshot_date",
        "created_at",
        "started_at",
        "completed_at",
        "error_message",
        "html_path",
        "pdf_path",
        "html_download_url",
        "pdf_download_url",
    )
    normalized_rows = _prepare_dict_rows([row], columns)
    insert_sql = """
        INSERT INTO report_export_jobs (
            job_id,
            report_kind,
            timeframe,
            scope,
            status,
            snapshot_date,
            created_at,
            started_at,
            completed_at,
            error_message,
            html_path,
            pdf_path,
            html_download_url,
            pdf_download_url
        )
        VALUES %s
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, normalized_rows, page_size=1)
            return cur.rowcount


def update_report_export_job(config: DatabaseConfig, job_id: str, **updates: object) -> int:
    """Update a manual report export job with the provided fields."""
    if not updates:
        return 0

    allowed_columns = {
        "status",
        "snapshot_date",
        "started_at",
        "completed_at",
        "error_message",
        "html_path",
        "pdf_path",
        "html_download_url",
        "pdf_download_url",
    }
    invalid_columns = sorted(set(updates) - allowed_columns)
    if invalid_columns:
        raise ValueError(f"Unsupported report export job columns: {', '.join(invalid_columns)}")

    assignments = ", ".join(f"{column} = %s" for column in updates)
    params = [updates[column] for column in updates]
    params.append(job_id)
    query = f"UPDATE report_export_jobs SET {assignments} WHERE job_id = %s"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            return cur.rowcount


def _decode_report_export_job_row(row: dict) -> dict:
    """Normalize report export job rows returned from psycopg2."""
    if not row:
        return row
    normalized = dict(row)
    snapshot_date = normalized.get("snapshot_date")
    if snapshot_date is not None:
        normalized["snapshot_date"] = str(snapshot_date)
    return normalized


def get_report_export_job(config: DatabaseConfig, job_id: str) -> Optional[dict]:
    """Return one manual report export job by id."""
    query = """
        SELECT
            job_id,
            report_kind,
            timeframe,
            scope,
            status,
            snapshot_date,
            created_at,
            started_at,
            completed_at,
            error_message,
            html_path,
            pdf_path,
            html_download_url,
            pdf_download_url
        FROM report_export_jobs
        WHERE job_id = %s
        LIMIT 1
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (job_id,))
            rows = _rows_to_dicts(cur)
            return _decode_report_export_job_row(rows[0]) if rows else None


def get_latest_report_export_job(
    config: DatabaseConfig,
    report_kind: Optional[str] = None,
    timeframe: Optional[str] = None,
    scope: Optional[str] = None,
    statuses: Optional[List[str]] = None,
) -> Optional[dict]:
    """Return the latest manual report export job matching the given filters."""
    query = """
        SELECT
            job_id,
            report_kind,
            timeframe,
            scope,
            status,
            snapshot_date,
            created_at,
            started_at,
            completed_at,
            error_message,
            html_path,
            pdf_path,
            html_download_url,
            pdf_download_url
        FROM report_export_jobs
        WHERE 1 = 1
    """
    params: List[object] = []
    if report_kind is not None:
        query += " AND report_kind = %s"
        params.append(report_kind)
    if timeframe is not None:
        query += " AND timeframe = %s"
        params.append(timeframe)
    if scope is not None:
        query += " AND scope = %s"
        params.append(scope)
    if statuses:
        query += " AND status = ANY(%s)"
        params.append(statuses)
    query += " ORDER BY created_at DESC LIMIT 1"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = _rows_to_dicts(cur)
            return _decode_report_export_job_row(rows[0]) if rows else None


def get_prior_signal_scores(
    config: DatabaseConfig,
    snapshot_date: str,
    timeframes: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    lookback_days: int = 40,
) -> List[dict]:
    """Fetch recent prior signal scores used to compute 1w/1m deltas."""
    query = """
        SELECT snapshot_date, timeframe, symbol, final_score
        FROM signal_snapshots
        WHERE snapshot_date < %s::date
          AND snapshot_date >= %s::date - (%s * INTERVAL '1 day')
    """
    params: List[object] = [snapshot_date, snapshot_date, lookback_days]
    if timeframes:
        query += " AND timeframe = ANY(%s)"
        params.append(timeframes)
    if symbols:
        query += " AND symbol = ANY(%s)"
        params.append(symbols)
    query += " ORDER BY timeframe, symbol, snapshot_date DESC"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def _resolve_latest_snapshot_date(
    cur,
    table_name: str,
    timeframe: Optional[str] = None,
    report_kind: Optional[str] = None,
) -> Optional[object]:
    query = f"SELECT MAX(snapshot_date) FROM {table_name} WHERE 1 = 1"
    params: List[object] = []
    if timeframe is not None:
        query += " AND timeframe = %s"
        params.append(timeframe)
    if report_kind is not None:
        query += " AND report_kind = %s"
        params.append(report_kind)
    cur.execute(query, tuple(params))
    row = cur.fetchone()
    return row[0] if row else None


def get_latest_signal_snapshots(
    config: DatabaseConfig,
    timeframe: Optional[str] = None,
    snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    """Fetch the latest signal snapshots for a timeframe/date."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            resolved_date = snapshot_date or _resolve_latest_snapshot_date(cur, "signal_snapshots", timeframe=timeframe)
            if resolved_date is None:
                return []
            query = """
                SELECT *
                FROM signal_snapshots
                WHERE snapshot_date = %s
            """
            params: List[object] = [resolved_date]
            if timeframe is not None:
                query += " AND timeframe = %s"
                params.append(timeframe)
            query += " ORDER BY final_score DESC, symbol"
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def get_latest_rank_snapshots(
    config: DatabaseConfig,
    timeframe: Optional[str] = None,
    snapshot_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    """Fetch the latest rank snapshots for dashboards and reports."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            resolved_date = snapshot_date or _resolve_latest_snapshot_date(cur, "rank_snapshots", timeframe=timeframe)
            if resolved_date is None:
                return []
            query = """
                SELECT *
                FROM rank_snapshots
                WHERE snapshot_date = %s
            """
            params: List[object] = [resolved_date]
            if timeframe is not None:
                query += " AND timeframe = %s"
                params.append(timeframe)
            query += " ORDER BY bull_rank NULLS LAST, bear_rank NULLS LAST, symbol"
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def get_latest_market_breadth_snapshots(
    config: DatabaseConfig,
    timeframe: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> List[dict]:
    """Fetch the latest market breadth rows."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            if snapshot_date is None:
                snapshot_date = _resolve_latest_snapshot_date(cur, "market_breadth_snapshots", timeframe=timeframe)
            if snapshot_date is None:
                return []
            query = """
                SELECT *
                FROM market_breadth_snapshots
                WHERE snapshot_date = %s
            """
            params: List[object] = [snapshot_date]
            if timeframe is not None:
                query += " AND timeframe = %s"
                params.append(timeframe)
            query += " ORDER BY timeframe"
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def get_latest_report_snapshots(
    config: DatabaseConfig,
    report_kind: Optional[str] = None,
    timeframe: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> List[dict]:
    """Fetch the latest stored report rows and rendered content."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            resolved_date = snapshot_date or _resolve_latest_snapshot_date(
                cur,
                "report_snapshots",
                timeframe=timeframe,
                report_kind=report_kind,
            )
            if resolved_date is None:
                return []
            query = """
                SELECT *
                FROM report_snapshots
                WHERE snapshot_date = %s
            """
            params: List[object] = [resolved_date]
            if report_kind is not None:
                query += " AND report_kind = %s"
                params.append(report_kind)
            if timeframe is not None:
                query += " AND timeframe = %s"
                params.append(timeframe)
            query += " ORDER BY report_kind, timeframe, symbol"
            cur.execute(query, tuple(params))
            return _rows_to_dicts(cur)


def get_report_snapshot_inputs(
    config: DatabaseConfig,
    report_kind: str,
    timeframe: str,
    snapshot_date: Optional[str] = None,
    top_n: int = 10,
) -> dict:
    """Bundle latest breadth, ranks, and signals for report generation."""
    ranks = get_latest_rank_snapshots(config, timeframe=timeframe, snapshot_date=snapshot_date)
    signals = get_latest_signal_snapshots(config, timeframe=timeframe, snapshot_date=snapshot_date)
    breadth = get_latest_market_breadth_snapshots(config, timeframe=timeframe, snapshot_date=snapshot_date)
    existing_reports = get_latest_report_snapshots(
        config,
        report_kind=report_kind,
        timeframe=timeframe,
        snapshot_date=snapshot_date,
    )
    return {
        "ranks": ranks[:top_n],
        "signals": signals,
        "breadth": breadth[0] if breadth else None,
        "existing_reports": existing_reports,
    }


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


def get_ticker_metadata(config: DatabaseConfig) -> List[dict]:
    """Return ticker metadata ordered by symbol."""
    query = """
        SELECT symbol, name, sector, industry, updated_at
        FROM tickers
        ORDER BY symbol
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            return [dict(zip(columns, row)) for row in rows]


def get_analytics_price_history(
    config: DatabaseConfig,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    symbols: Optional[List[str]] = None,
) -> List[dict]:
    """Return OHLCV history for analytics runs."""
    query = """
        SELECT
            sp.symbol,
            sp.timestamp,
            sp.open,
            sp.high,
            sp.low,
            sp.close,
            sp.volume,
            sp.dividends,
            sp.stock_splits,
            t.name,
            t.sector,
            t.industry
        FROM stock_prices sp
        LEFT JOIN tickers t ON t.symbol = sp.symbol
        WHERE 1 = 1
    """
    params: List[object] = []
    if start_date is not None:
        query += " AND sp.timestamp >= %s"
        params.append(start_date)
    if end_date is not None:
        query += " AND sp.timestamp <= %s"
        params.append(end_date)
    if symbols:
        query += " AND sp.symbol = ANY(%s)"
        params.append(symbols)
    query += " ORDER BY sp.symbol, sp.timestamp"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            return [dict(zip(columns, row)) for row in rows]


def get_latest_signal_snapshot_date(config: DatabaseConfig, timeframe: Optional[str] = None) -> Optional[str]:
    """Return the latest analytics snapshot date."""
    query = "SELECT MAX(snapshot_date) FROM signal_snapshots"
    params: Tuple[object, ...] = ()
    if timeframe is not None:
        query += " WHERE timeframe = %s"
        params = (timeframe,)

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None


def get_existing_signal_snapshot_dates(
    config: DatabaseConfig,
    timeframe: str,
) -> list[str]:
    """Return all persisted signal snapshot dates for a timeframe."""
    query = """
        SELECT snapshot_date
        FROM signal_snapshots
        WHERE timeframe = %s
        ORDER BY snapshot_date
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (timeframe,))
            rows = cur.fetchall()
            return [str(row[0]) for row in rows if row and row[0]]


def get_signal_snapshots(
    config: DatabaseConfig,
    snapshot_date: str,
    timeframe: str,
    limit: Optional[int] = None,
) -> List[dict]:
    """Return signal snapshots enriched with ticker metadata."""
    query = """
        SELECT
            ss.snapshot_date,
            ss.symbol,
            COALESCE(t.name, ss.symbol) AS name,
            t.sector,
            t.industry,
            ss.timeframe,
            ss.last_timestamp,
            ss.close,
            ss.volume,
            ss.trend_score,
            ss.momentum_score,
            ss.volume_score,
            ss.relative_strength_score,
            ss.structure_score,
            ss.mean_reversion_score,
            ss.volatility_risk_score,
            ss.risk_penalty,
            ss.final_score,
            ss.trend_state,
            ss.momentum_state,
            ss.volume_state,
            ss.relative_strength_state,
            ss.structure_state,
            ss.volatility_state,
            ss.regime_label,
            ss.recommendation_label,
            ss.breakout_flag,
            ss.breakdown_flag,
            ss.overbought_flag,
            ss.oversold_flag,
            ss.trend_alignment_flag,
            ss.data_quality_flag,
            ss.drivers_json,
            ss.updated_at
        FROM signal_snapshots ss
        LEFT JOIN tickers t ON t.symbol = ss.symbol
        WHERE ss.snapshot_date = %s
          AND ss.timeframe = %s
        ORDER BY ss.final_score DESC, ss.symbol
    """
    params: List[object] = [snapshot_date, timeframe]
    if limit is not None and limit > 0:
        query += " LIMIT %s"
        params.append(limit)

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            decoded_rows: List[dict] = []
            for row in rows:
                record = dict(zip(columns, row))
                record["drivers_json"] = _decoded_json_value(record.get("drivers_json"), [])
                decoded_rows.append(record)
            return decoded_rows


def get_signal_snapshot_scores_on_or_before(
    config: DatabaseConfig,
    timeframe: str,
    snapshot_date: str,
) -> dict[str, float]:
    """Return the latest available final score per symbol on or before the requested date."""
    query = """
        SELECT DISTINCT ON (symbol)
            symbol,
            final_score
        FROM signal_snapshots
        WHERE timeframe = %s
          AND snapshot_date <= %s
        ORDER BY symbol, snapshot_date DESC
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (timeframe, snapshot_date))
            rows = cur.fetchall()
            return {str(symbol): float(score) for symbol, score in rows if score is not None}


def get_latest_report_snapshot_rows(
    config: DatabaseConfig,
    report_kind: str,
    timeframe: str,
) -> List[dict]:
    """Return the latest stored report snapshot rows for a report kind/timeframe."""
    query = """
        WITH latest_report AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM report_snapshots
            WHERE report_kind = %s
              AND timeframe = %s
        )
        SELECT
            snapshot_date,
            report_kind,
            timeframe,
            symbol,
            title,
            final_score,
            regime_label,
            recommendation_label,
            summary_text,
            risk_text,
            key_drivers_json,
            report_markdown,
            report_html,
            storage_path,
            updated_at
        FROM report_snapshots
        WHERE report_kind = %s
          AND timeframe = %s
          AND snapshot_date = (SELECT snapshot_date FROM latest_report)
        ORDER BY symbol
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (report_kind, timeframe, report_kind, timeframe))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            decoded_rows: List[dict] = []
            for row in rows:
                record = dict(zip(columns, row))
                record["key_drivers_json"] = _decoded_json_value(record.get("key_drivers_json"), {})
                decoded_rows.append(record)
            return decoded_rows


def get_rank_snapshots(
    config: DatabaseConfig,
    snapshot_date: str,
    timeframe: str,
    side: str,
    limit: int,
) -> List[dict]:
    """Return ranked rows for a snapshot/timeframe."""
    if side not in {"bull", "bear"}:
        raise ValueError("side must be 'bull' or 'bear'")
    if limit <= 0:
        return []

    order_column = "bull_rank" if side == "bull" else "bear_rank"
    query = f"""
        SELECT
            rs.symbol,
            COALESCE(t.name, rs.symbol) AS name,
            t.sector,
            rs.final_score,
            rs.bull_rank,
            rs.bear_rank,
            rs.regime_label,
            rs.recommendation_label,
            rs.score_change_1w,
            rs.score_change_1m
        FROM rank_snapshots rs
        LEFT JOIN tickers t ON t.symbol = rs.symbol
        WHERE rs.snapshot_date = %s
          AND rs.timeframe = %s
          AND {order_column} IS NOT NULL
        ORDER BY {order_column} ASC, rs.symbol ASC
        LIMIT %s
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (snapshot_date, timeframe, limit))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or ()]
            return [dict(zip(columns, row)) for row in rows]


def get_market_breadth_snapshot(
    config: DatabaseConfig,
    snapshot_date: str,
    timeframe: str,
) -> Optional[dict]:
    """Return one market breadth snapshot row if present."""
    query = """
        SELECT
            snapshot_date,
            timeframe,
            universe_size,
            bullish_count,
            neutral_count,
            bearish_count,
            pct_above_ema20,
            pct_above_ema50,
            pct_above_ema200,
            pct_new_20d_high,
            pct_new_20d_low,
            pct_near_52w_high,
            pct_near_52w_low,
            avg_final_score,
            median_final_score
        FROM market_breadth_snapshots
        WHERE snapshot_date = %s
          AND timeframe = %s
        LIMIT 1
    """
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (snapshot_date, timeframe))
            row = cur.fetchone()
            if not row:
                return None
            columns = [desc[0] for desc in cur.description or ()]
            return dict(zip(columns, row))


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
