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
    delay_seconds: float
    historical_start: str
    backfill_start: Optional[str]
    update_cron: str
    max_retries: int
    retry_delay_seconds: float


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
        delay_seconds=float(os.environ.get("YFINANCE_DELAY_SEC", "2.5")),
        historical_start=os.environ.get("HISTORICAL_START", "2020-01-01"),
        backfill_start=os.environ.get("BACKFILL_START") or None,
        update_cron=os.environ.get("UPDATE_CRON", "0 23 * * *"),  # 11 PM UTC = 6 PM ET
        max_retries=int(os.environ.get("YFINANCE_MAX_RETRIES", "5")),
        retry_delay_seconds=float(os.environ.get("YFINANCE_RETRY_DELAY", "60")),
    )
