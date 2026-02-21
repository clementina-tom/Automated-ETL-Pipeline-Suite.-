"""
pipeline.py
Top-level orchestrator: wire Extract → Transform → Validate → Load.
"""

import sys

import pandas as pd

import config
from config import get_logger
from extractor.web_extractor import WebExtractor
from extractor.playwright_extractor import PlaywrightExtractor
from transformer.cleaner import DataCleaner
from transformer.mapper import EntityMapper
from validator.schema_validator import SchemaValidator
from validator.id_validator import IDValidator
from loader.sqlite_loader import SQLiteLoader
from loader.csv_loader import CSVLoader

logger = get_logger("pipeline")

# ──────────────────────────────────────────────────────────────────────
# Sample data generator (remove when you have real sources)
# ──────────────────────────────────────────────────────────────────────

def _sample_beneficiaries() -> pd.DataFrame:
    """Synthetic Beneficiaries table for dry-run testing."""
    return pd.DataFrame({
        "beneficiary_id": ["B001", "B002", "B003"],
        "beneficiary_name": ["  Alice  ", "Bob", "Charlie"],
        "status": ["active", "inactive", "active"],
        "source_url": [config.DEFAULT_SCRAPE_URL] * 3,
    })


def _sample_gifts() -> pd.DataFrame:
    """Synthetic Gifts table for dry-run testing."""
    return pd.DataFrame({
        "beneficiary_id": ["B001", "B002", "B003"],
        "id": ["G001", "G002", "G003"],
        "gift_type": ["Cash", "In-Kind", "Cash"],
        "amount": [500.0, 200.0, 150.0],
        "date": ["2024-01-15", "2024-02-20", "2024-03-05"],
    })


# ──────────────────────────────────────────────────────────────────────
# Pipeline Tasks
# ──────────────────────────────────────────────────────────────────────

from prefect import flow, task

@task(name="Extract Data")
def extract_data(use_playwright: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Starting extraction...")
    # Replace the synthetic samples with real extractor calls later
    raw_beneficiaries = _sample_beneficiaries()
    raw_gifts = _sample_gifts()
    logger.info(
        "Extracted %d beneficiary rows and %d gift rows.",
        len(raw_beneficiaries),
        len(raw_gifts),
    )
    return raw_beneficiaries, raw_gifts


@task(name="Clean and Map Data")
def transform_data(raw_beneficiaries: pd.DataFrame, raw_gifts: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting transformation...")
    cleaner = DataCleaner(
        required_columns=["beneficiary_id"],
        date_columns=["date"],
    )
    clean_beneficiaries = cleaner.run(raw_beneficiaries)
    clean_gifts = cleaner.run(raw_gifts)

    mapper = EntityMapper(right_df=clean_gifts)
    master_df = mapper.run(clean_beneficiaries)
    logger.info("Master table shape: %s", master_df.shape)
    return master_df


@task(name="Validate Data")
def validate_data(master_df: pd.DataFrame) -> bool:
    logger.info("Starting validation...")
    schema_ok = SchemaValidator(
        required_columns=["id", "beneficiary_name", "gift_type", "amount", "date"],
    ).run(master_df)

    id_ok = IDValidator(id_column="id").run(master_df)
    
    if not (schema_ok and id_ok):
        logger.error("Validation failed.")
        return False
    return True


@task(name="Load Data")
def load_data(master_df: pd.DataFrame) -> None:
    logger.info("Starting load...")
    SQLiteLoader(table_name="master_table", if_exists="replace").run(master_df)
    CSVLoader(prefix="master_table").run(master_df)


# ──────────────────────────────────────────────────────────────────────
# Orchestrator Flow
# ──────────────────────────────────────────────────────────────────────

@flow(name="Automated ETL Pipeline", retries=2, retry_delay_seconds=60)
def run_pipeline(use_playwright: bool = False) -> None:
    """
    Execute the full ETL pipeline with Prefect orchestration.
    """
    logger.info("=" * 60)
    logger.info("ETL Pipeline starting.")
    logger.info("=" * 60)

    try:
        # 1. Extract
        raw_beneficiaries, raw_gifts = extract_data(use_playwright=use_playwright)

        # 2. Transform
        master_df = transform_data(raw_beneficiaries, raw_gifts)

        # 3. Validate
        is_valid = validate_data(master_df)
        if not is_valid:
            raise ValueError("Data validation failed. Aborting pipeline.")

        # 4. Load
        load_data(master_df)

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully.")
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("Pipeline encountered a fatal error: %s", exc)
        raise exc


if __name__ == "__main__":
    run_pipeline()

