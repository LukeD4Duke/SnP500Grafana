"""Environment-based configuration for the stock fetcher service."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    host: str
    port: int
    name: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.name} "
            f"user={self.user} password={self.password}"
        )

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class FetcherConfig:
    """yfinance and scheduler configuration."""

    chunk_size: int
    delay_seconds: float
    historical_start: str
    update_cron: str
    max_retries: int
    retry_delay_seconds: float
    symbol_retry_count: int = 2
    recovery_chunk_size: int = 5
    failed_symbol_log_limit: int = 20
    startup_post_sync_mode: str = "background"
    backfill_start: Optional[str] = None


@dataclass
class IndicatorConfig:
    """Indicator pipeline configuration."""

    enabled: bool
    rebuild_on_startup: bool
    batch_size: int
    incremental_lookback_rows: int


@dataclass
class AnalyticsConfig:
    """Analytics snapshot configuration."""

    enabled: bool
    timeframes: list[str]


@dataclass
class ReportingConfig:
    """Scheduled report generation configuration."""

    enabled: bool
    output_dir: str
    weekly_cron: str
    monthly_cron: str


@dataclass
class ReportExportConfig:
    """Manual report export service configuration."""

    output_dir: str
    public_url: str
    grafana_internal_url: str


def get_database_config() -> DatabaseConfig:
    """Load database configuration from environment."""
    password = os.environ.get("DB_PASSWORD")
    if not password:
        raise ValueError(
            "DB_PASSWORD environment variable is required. "
            "Set it in docker-compose or .env before deploying."
        )
    return DatabaseConfig(
        host=os.environ.get("DB_HOST", "timescaledb"),
        port=int(os.environ.get("DB_PORT", "5432")),
        name=os.environ.get("DB_NAME", "stocks"),
        user=os.environ.get("DB_USER", "postgres"),
        password=password,
    )


def get_fetcher_config() -> FetcherConfig:
    """Load fetcher configuration from environment."""
    startup_post_sync_mode = os.environ.get("STARTUP_POST_SYNC_MODE", "background").strip().lower()
    if startup_post_sync_mode not in {"background", "blocking"}:
        raise ValueError(
            "STARTUP_POST_SYNC_MODE must be either 'background' or 'blocking'. "
            f"Received: {startup_post_sync_mode!r}"
        )

    return FetcherConfig(
        chunk_size=int(os.environ.get("YFINANCE_CHUNK_SIZE", "50")),
        symbol_retry_count=int(os.environ.get("YFINANCE_SYMBOL_RETRIES", "2")),
        recovery_chunk_size=int(os.environ.get("YFINANCE_RECOVERY_CHUNK_SIZE", "5")),
        failed_symbol_log_limit=int(os.environ.get("YFINANCE_FAILED_SYMBOL_LOG_LIMIT", "20")),
        delay_seconds=float(os.environ.get("YFINANCE_DELAY_SEC", "2.5")),
        historical_start=os.environ.get("HISTORICAL_START", "2020-01-01"),
        startup_post_sync_mode=startup_post_sync_mode,
        backfill_start=os.environ.get("BACKFILL_START") or None,
        update_cron=os.environ.get("UPDATE_CRON", "0 23 * * *"),
        max_retries=int(os.environ.get("YFINANCE_MAX_RETRIES", "5")),
        retry_delay_seconds=float(os.environ.get("YFINANCE_RETRY_DELAY", "60")),
    )


def _get_bool_env(name: str, default: bool) -> bool:
    """Parse a boolean environment variable."""
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def get_indicator_config() -> IndicatorConfig:
    """Load indicator pipeline configuration from environment."""
    return IndicatorConfig(
        enabled=_get_bool_env("INDICATORS_ENABLED", True),
        rebuild_on_startup=_get_bool_env("INDICATOR_REBUILD_ON_STARTUP", False),
        batch_size=int(os.environ.get("INDICATOR_BATCH_SIZE", "25")),
        incremental_lookback_rows=int(os.environ.get("INDICATOR_INCREMENTAL_LOOKBACK_ROWS", "1000")),
    )


def get_analytics_config() -> AnalyticsConfig:
    """Load analytics snapshot configuration from environment."""
    raw_timeframes = os.environ.get("ANALYTICS_TIMEFRAMES", "daily,weekly,monthly")
    timeframes = [value.strip().lower() for value in raw_timeframes.split(",") if value.strip()]
    if not timeframes:
        timeframes = ["daily", "weekly", "monthly"]
    return AnalyticsConfig(
        enabled=_get_bool_env("ANALYTICS_ENABLED", True),
        timeframes=timeframes,
    )


def get_reporting_config() -> ReportingConfig:
    """Load report generation configuration from environment."""
    return ReportingConfig(
        enabled=_get_bool_env("REPORTS_ENABLED", True),
        output_dir=os.environ.get("REPORT_OUTPUT_DIR", "/app/reports"),
        weekly_cron=os.environ.get("REPORT_WEEKLY_CRON", "15 0 * * 1"),
        monthly_cron=os.environ.get("REPORT_MONTHLY_CRON", "30 0 1 * *"),
    )


def get_report_export_config() -> ReportExportConfig:
    """Load manual export service configuration from environment."""
    return ReportExportConfig(
        output_dir=os.environ.get("REPORT_OUTPUT_DIR", "/app/reports"),
        public_url=os.environ.get("REPORT_UI_PUBLIC_URL", "http://localhost:3002").rstrip("/"),
        grafana_internal_url=os.environ.get("GRAFANA_INTERNAL_URL", "http://grafana:3000").rstrip("/"),
    )
