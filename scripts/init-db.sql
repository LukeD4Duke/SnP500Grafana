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

