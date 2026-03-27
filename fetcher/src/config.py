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
        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class FetcherConfig:
    """yfinance and scheduler configuration."""

    chunk_size: int
    symbol_retry_count: int
    recovery_chunk_size: int
    failed_symbol_log_limit: int
    delay_seconds: float
    historical_start: str
    update_cron: str
    max_retries: int
    retry_delay_seconds: float
    backfill_start: Optional[str] = None


@dataclass
class IndicatorConfig:
    """Indicator pipeline configuration."""

    enabled: bool
    rebuild_on_startup: bool
    batch_size: int


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
    return FetcherConfig(
        chunk_size=int(os.environ.get("YFINANCE_CHUNK_SIZE", "50")),
        symbol_retry_count=int(os.environ.get("YFINANCE_SYMBOL_RETRIES", "2")),
        recovery_chunk_size=int(os.environ.get("YFINANCE_RECOVERY_CHUNK_SIZE", "5")),
        failed_symbol_log_limit=int(os.environ.get("YFINANCE_FAILED_SYMBOL_LOG_LIMIT", "20")),
        delay_seconds=float(os.environ.get("YFINANCE_DELAY_SEC", "2.5")),
        historical_start=os.environ.get("HISTORICAL_START", "2020-01-01"),
        backfill_start=os.environ.get("BACKFILL_START") or None,
        update_cron=os.environ.get("UPDATE_CRON", "0 23 * * *"),  # 11 PM UTC = 6 PM ET
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
    )
