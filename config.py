"""
config.py
Central configuration, paths, and logging setup for the ETL pipeline.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.resolve()
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = BASE_DIR / "output"

LOGS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────
SQLITE_DB_PATH = BASE_DIR / "etl_output.db"
SQLITE_DATABASE_URL = f"sqlite:///{SQLITE_DB_PATH}"

# ──────────────────────────────────────────────
# Extractor defaults
# ──────────────────────────────────────────────
DEFAULT_SCRAPE_URL = "https://example.com"         # Replace with your target URL
REQUEST_TIMEOUT_SECONDS = 30
PLAYWRIGHT_HEADLESS = True                          # Set False to debug visually

# ──────────────────────────────────────────────
# Transformer defaults
# ──────────────────────────────────────────────
BENEFICIARY_JOIN_KEY = "beneficiary_id"            # Shared column for entity mapping
GIFT_JOIN_KEY = "beneficiary_id"

# ──────────────────────────────────────────────
# Master table schema
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
LOG_FILE = LOGS_DIR / "etl.log"
LOG_MAX_BYTES = 5 * 1024 * 1024   # 5 MB
LOG_BACKUP_COUNT = 3


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger configured with:
      - RotatingFileHandler  → logs/etl.log (JSON)
      - StreamHandler        → stdout (Text/JSON depending on environment)
    """
    try:
        from pythonjsonlogger import jsonlogger
        has_json_logger = True
    except ImportError:
        has_json_logger = False

    logger = logging.getLogger(name)

    if logger.handlers:          # Avoid duplicate handlers on re-import
        return logger

    logger.setLevel(LOG_LEVEL)
    
    if has_json_logger:
        formatter = jsonlogger.JsonFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    else:
        # Fallback for plain text if jsonlogger isn't installed yet
        formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # File handler
    fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger
