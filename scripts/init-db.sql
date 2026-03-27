-- S&P 500 Stock Analysis - TimescaleDB Schema
-- Run on first database initialization

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tickers metadata from Wikipedia (S&P 500 constituents)
CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(150),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Stock prices time-series (OHLCV + dividends, splits)
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

-- Convert to hypertable partitioned by time
-- Space partition by symbol for efficient single-ticker queries
SELECT create_hypertable(
    'stock_prices',
    'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);

-- Create index for symbol-based queries
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices (symbol, timestamp DESC);

-- Indicator catalog stores supported indicator definitions and default metadata.
CREATE TABLE IF NOT EXISTS indicator_catalog (
    indicator_key TEXT PRIMARY KEY,
    indicator TEXT NOT NULL,
    output_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT NOT NULL,
    source_library VARCHAR(32) NOT NULL,
    default_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    warmup_periods INTEGER NOT NULL DEFAULT 0,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indicator output rows are kept in a narrow long-form table for flexibility.
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

