"""
pipeline.py
Top-level orchestrator for Extract → Transform → Validate → Load.
"""

from dataclasses import dataclass

import pandas as pd
from prefect import flow, task

import config
from config import get_logger
from extractor.base_extractor import BaseExtractor
from loader.base_loader import BaseLoader
from loader.csv_loader import CSVLoader
from loader.sqlite_loader import SQLiteLoader
from transformer.cleaner import DataCleaner
from transformer.mapper import EntityMapper
from validator.id_validator import IDValidator
from validator.schema_validator import SchemaValidator

logger = get_logger("pipeline")


@dataclass
class PipelineConfig:
    """Runtime configuration for ETL execution."""

    source_mode: str = "synthetic"  # synthetic | custom


def _sample_beneficiaries() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002", "B003"],
            "beneficiary_name": ["  Alice  ", "Bob", "Charlie"],
            "status": ["active", "inactive", "active"],
            "source_url": [config.DEFAULT_SCRAPE_URL] * 3,
        }
    )


def _sample_gifts() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002", "B003"],
            "id": ["G001", "G002", "G003"],
            "gift_type": ["Cash", "In-Kind", "Cash"],
            "amount": [500.0, 200.0, 150.0],
            "date": ["2024-01-15", "2024-02-20", "2024-03-05"],
        }
    )


def extract_data_sync(
    source_mode: str,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Starting extraction mode=%s", source_mode)

    if source_mode == "synthetic":
        raw_beneficiaries = _sample_beneficiaries()
        raw_gifts = _sample_gifts()
    elif source_mode == "custom":
        if beneficiaries_extractor is None or gifts_extractor is None:
            raise ValueError(
                "For source_mode='custom', both beneficiaries_extractor and gifts_extractor are required."
            )
        raw_beneficiaries = beneficiaries_extractor.run()
        raw_gifts = gifts_extractor.run()
    else:
        raise ValueError(f"Unsupported source_mode: {source_mode}")

    logger.info(
        "Extracted %d beneficiary rows and %d gift rows.",
        len(raw_beneficiaries),
        len(raw_gifts),
    )
    return raw_beneficiaries, raw_gifts


def transform_data_sync(raw_beneficiaries: pd.DataFrame, raw_gifts: pd.DataFrame) -> pd.DataFrame:
    cleaner = DataCleaner(required_columns=["beneficiary_id"], date_columns=["date"])
    clean_beneficiaries = cleaner.run(raw_beneficiaries)
    clean_gifts = cleaner.run(raw_gifts)
    return EntityMapper(right_df=clean_gifts).run(clean_beneficiaries)


def validate_data_sync(master_df: pd.DataFrame) -> bool:
    schema_ok = SchemaValidator(
        required_columns=["id", "beneficiary_name", "gift_type", "amount", "date"]
    ).run(master_df)
    id_ok = IDValidator(id_column="id").run(master_df)
    return schema_ok and id_ok


def load_data_sync(master_df: pd.DataFrame, loaders: list[BaseLoader] | None = None) -> None:
    active_loaders = loaders or [
        SQLiteLoader(table_name="master_table", if_exists="replace"),
        CSVLoader(prefix="master_table"),
    ]
    for loader in active_loaders:
        loader.run(master_df)


def run_pipeline_sync(
    config_obj: PipelineConfig | None = None,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
    loaders: list[BaseLoader] | None = None,
) -> pd.DataFrame:
    """Run the pipeline synchronously without Prefect orchestration."""
    cfg = config_obj or PipelineConfig()
    raw_beneficiaries, raw_gifts = extract_data_sync(
        source_mode=cfg.source_mode,
        beneficiaries_extractor=beneficiaries_extractor,
        gifts_extractor=gifts_extractor,
    )
    master_df = transform_data_sync(raw_beneficiaries, raw_gifts)
    if not validate_data_sync(master_df):
        raise ValueError("Data validation failed. Aborting pipeline.")
    load_data_sync(master_df, loaders=loaders)
    return master_df


@task(name="Extract Data")
def extract_data(
    source_mode: str,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return extract_data_sync(source_mode, beneficiaries_extractor, gifts_extractor)


@task(name="Clean and Map Data")
def transform_data(raw_beneficiaries: pd.DataFrame, raw_gifts: pd.DataFrame) -> pd.DataFrame:
    return transform_data_sync(raw_beneficiaries, raw_gifts)


@task(name="Validate Data")
def validate_data(master_df: pd.DataFrame) -> bool:
    return validate_data_sync(master_df)


@task(name="Load Data")
def load_data(master_df: pd.DataFrame, loaders: list[BaseLoader] | None = None) -> None:
    load_data_sync(master_df, loaders=loaders)


@flow(name="Automated ETL Pipeline", retries=2, retry_delay_seconds=60)
def run_pipeline(
    config_obj: PipelineConfig | None = None,
    beneficiaries_extractor: BaseExtractor | None = None,
    gifts_extractor: BaseExtractor | None = None,
    loaders: list[BaseLoader] | None = None,
) -> pd.DataFrame:
    """Execute the full ETL pipeline with Prefect orchestration."""
    cfg = config_obj or PipelineConfig()
    raw_beneficiaries, raw_gifts = extract_data(
        source_mode=cfg.source_mode,
        beneficiaries_extractor=beneficiaries_extractor,
        gifts_extractor=gifts_extractor,
    )
    master_df = transform_data(raw_beneficiaries, raw_gifts)
    if not validate_data(master_df):
        raise ValueError("Data validation failed. Aborting pipeline.")
    load_data(master_df, loaders=loaders)
    return master_df


if __name__ == "__main__":
    run_pipeline()
