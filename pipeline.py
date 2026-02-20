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
# Pipeline
# ──────────────────────────────────────────────────────────────────────

def run_pipeline(use_playwright: bool = False) -> None:
    """
    Execute the full ETL pipeline.

    Args:
        use_playwright: If True, use :class:`PlaywrightExtractor` for
                        JS-heavy pages.  Otherwise use :class:`WebExtractor`.

    Returns:
        None.  Exits with code 1 on fatal failure.
    """
    logger.info("=" * 60)
    logger.info("ETL Pipeline starting.")
    logger.info("=" * 60)

    try:
        # ── 1. EXTRACT ─────────────────────────────────────────────
        logger.info("[STEP 1/4] EXTRACT")

        # Replace the synthetic samples with real extractor calls:
        #   extractor = PlaywrightExtractor(config.DEFAULT_SCRAPE_URL)
        #   raw_df = extractor.run()
        raw_beneficiaries = _sample_beneficiaries()
        raw_gifts = _sample_gifts()
        logger.info(
            "Extracted %d beneficiary rows and %d gift rows.",
            len(raw_beneficiaries),
            len(raw_gifts),
        )

        # ── 2. TRANSFORM ───────────────────────────────────────────
        logger.info("[STEP 2/4] TRANSFORM")

        cleaner = DataCleaner(
            required_columns=["beneficiary_id"],
            date_columns=["date"],
        )
        clean_beneficiaries = cleaner.run(raw_beneficiaries)
        clean_gifts = cleaner.run(raw_gifts)

        mapper = EntityMapper(right_df=clean_gifts)
        master_df = mapper.run(clean_beneficiaries)
        logger.info("Master table shape: %s", master_df.shape)

        # ── 3. VALIDATE ────────────────────────────────────────────
        logger.info("[STEP 3/4] VALIDATE")

        schema_ok = SchemaValidator(
            required_columns=["id", "beneficiary_name", "gift_type", "amount"],
        ).run(master_df)

        id_ok = IDValidator(id_column="id").run(master_df)

        if not (schema_ok and id_ok):
            logger.error("Validation failed. Aborting load step.")
            sys.exit(1)

        # ── 4. LOAD ────────────────────────────────────────────────
        logger.info("[STEP 4/4] LOAD")

        SQLiteLoader(table_name="master_table", if_exists="replace").run(master_df)
        CSVLoader(prefix="master_table").run(master_df)

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully.")
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("Pipeline encountered a fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
