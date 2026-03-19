"""Central configuration, paths, and logging setup for the ETL pipeline."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from pythonjsonlogger import jsonlogger


class Settings(BaseSettings):
    """Environment-driven settings for secure/runtime-safe configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    default_scrape_url: str = "https://example.com"
    request_timeout_seconds: int = 30
    playwright_headless: bool = True

    beneficiary_join_key: str = "beneficiary_id"
    gift_join_key: str = "beneficiary_id"

    extract_raise_on_error: bool = True
    api_max_retries: int = 3
    api_backoff_factor: float = 0.5

    aws_access_key_id: SecretStr | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_default_region: str | None = None

    log_level: str = "DEBUG"
    log_max_bytes: int = 5 * 1024 * 1024
    log_backup_count: int = 3


settings = Settings()

BASE_DIR = Path(__file__).parent.resolve()
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"

LOGS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

SQLITE_DB_PATH = BASE_DIR / "etl_output.db"
SQLITE_DATABASE_URL = f"sqlite:///{SQLITE_DB_PATH}"

DEFAULT_SCRAPE_URL = settings.default_scrape_url
REQUEST_TIMEOUT_SECONDS = settings.request_timeout_seconds
PLAYWRIGHT_HEADLESS = settings.playwright_headless

BENEFICIARY_JOIN_KEY = settings.beneficiary_join_key
GIFT_JOIN_KEY = settings.gift_join_key

EXTRACT_RAISE_ON_ERROR = settings.extract_raise_on_error
API_MAX_RETRIES = settings.api_max_retries
API_BACKOFF_FACTOR = settings.api_backoff_factor

MASTER_COLUMNS = [
    "id",
    "beneficiary_name",
    "gift_type",
    "amount",
    "date",
    "status",
    "source_url",
    "processed_at",
]

LOG_LEVEL = getattr(logging, settings.log_level.upper(), logging.DEBUG)
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
LOG_FILE = LOGS_DIR / "etl.log"
LOG_MAX_BYTES = settings.log_max_bytes
LOG_BACKUP_COUNT = settings.log_backup_count


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with rotating file + console JSON handlers."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)
    formatter = jsonlogger.JsonFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger
