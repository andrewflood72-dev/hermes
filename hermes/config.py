"""Hermes configuration — loaded from environment variables and .env file."""

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database (shared Atlas instance)
    database_url: str = "postgresql+asyncpg://atlas:atlas_dev@localhost:5433/atlas"
    database_url_sync: str = "postgresql://atlas:atlas_dev@localhost:5433/atlas"

    # Redis
    redis_url: str = "redis://localhost:6380/1"

    # AI
    anthropic_api_key: str = ""

    # SERFF Scraper
    serff_base_url: str = "https://filingaccess.serff.com"
    scrape_delay_seconds: float = 3.0
    scrape_max_retries: int = 3
    scrape_session_timeout: int = 300

    # Filing Storage
    filing_storage_path: str = "./data/filings"

    # API
    hermes_api_key: str = "hermes-dev-key-change-me"
    hermes_api_port: int = 8001

    # Logging
    log_level: str = "INFO"


settings = Settings()
