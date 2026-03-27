-- S&P 500 Stock Analysis - TimescaleDB Schema
-- Run on first database initialization

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
    last_timestamp TIMESTAMPTZ NOT NULL,
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
