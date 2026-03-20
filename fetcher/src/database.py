"""Database connection, schema management, and data persistence."""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from .config import DatabaseConfig

logger = logging.getLogger(__name__)


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
    script_path = init_script_path or Path(__file__).parent.parent.parent / "scripts" / "init-db.sql"
    if not script_path.exists():
        raise FileNotFoundError(f"Init script not found: {script_path}")

    with open(script_path, "r") as f:
        sql = f.read()

    # Split into statements and execute separately for robustness
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                try:
                    cur.execute(stmt)
                    logger.info("Executed: %s...", stmt[:60])
                except Exception as e:
                    # Skip compression policy if not supported
                    if "compression" in stmt.lower() or "compress" in stmt.lower():
                        logger.warning("Skipping compression statement (may not be supported): %s", e)
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
