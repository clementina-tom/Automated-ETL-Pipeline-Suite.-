"""Top-level orchestrator for Extract → Transform → Validate → Load."""

from dataclasses import dataclass, field
from pathlib import Path

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
class EntityConfig:
    """Entity extraction + cleaning configuration."""

    name: str
    extractor: BaseExtractor | None = None
    required_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Runtime configuration for ETL execution."""

    source_mode: str = "synthetic"  # synthetic | custom
    entities: list[EntityConfig] = field(default_factory=list)
    left_entity: str = "beneficiaries"
    right_entity: str = "gifts"
    join_left_key: str = config.BENEFICIARY_JOIN_KEY
    join_right_key: str = config.GIFT_JOIN_KEY
    output_columns: list[str] | None = None
    quarantine_invalid_rows: bool = False
    quarantine_dir: Path = config.OUTPUT_DIR / "quarantine"


def _sample_entity_data() -> dict[str, pd.DataFrame]:
    return {
        "beneficiaries": pd.DataFrame(
            {
                "beneficiary_id": ["B001", "B002", "B003"],
                "beneficiary_name": ["  Alice  ", "Bob", "Charlie"],
                "status": ["active", "inactive", "active"],
                "source_url": [config.DEFAULT_SCRAPE_URL] * 3,
            }
        ),
        "gifts": pd.DataFrame(
            {
                "beneficiary_id": ["B001", "B002", "B003"],
                "id": ["G001", "G002", "G003"],
                "gift_type": ["Cash", "In-Kind", "Cash"],
                "amount": [500.0, 200.0, 150.0],
                "date": ["2024-01-15", "2024-02-20", "2024-03-05"],
            }
        ),
    }


def _default_entities() -> list[EntityConfig]:
    return [
        EntityConfig(name="beneficiaries", required_columns=["beneficiary_id"]),
        EntityConfig(name="gifts", required_columns=["beneficiary_id"], date_columns=["date"]),
    ]


def extract_entities_sync(cfg: PipelineConfig) -> dict[str, pd.DataFrame]:
    logger.info("Starting extraction mode=%s", cfg.source_mode)
    entities = cfg.entities or _default_entities()

    if cfg.source_mode == "synthetic":
        sample_data = _sample_entity_data()
        extracted: dict[str, pd.DataFrame] = {}
        for entity in entities:
            if entity.name not in sample_data:
                raise ValueError(f"No synthetic sample configured for entity '{entity.name}'")
            extracted[entity.name] = sample_data[entity.name]
        return extracted

    if cfg.source_mode == "custom":
        extracted = {}
        for entity in entities:
            if entity.extractor is None:
                raise ValueError(
                    f"Entity '{entity.name}' is missing an extractor in source_mode='custom'"
                )
            extracted[entity.name] = entity.extractor.run()
        return extracted

    raise ValueError(f"Unsupported source_mode: {cfg.source_mode}")


def transform_entities_sync(cfg: PipelineConfig, entity_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    entities = cfg.entities or _default_entities()

    cleaned: dict[str, pd.DataFrame] = {}
    for entity in entities:
        cleaner = DataCleaner(
            required_columns=entity.required_columns,
            date_columns=entity.date_columns,
        )
        cleaned[entity.name] = cleaner.run(entity_frames[entity.name])

    if cfg.left_entity not in cleaned or cfg.right_entity not in cleaned:
        raise ValueError("Configured left_entity/right_entity not found in extracted entities")

    mapper = EntityMapper(
        right_df=cleaned[cfg.right_entity],
        left_key=cfg.join_left_key,
        right_key=cfg.join_right_key,
        output_columns=cfg.output_columns,
    )
    return mapper.run(cleaned[cfg.left_entity])


def _quarantine_invalid_rows(master_df: pd.DataFrame, cfg: PipelineConfig) -> Path:
    cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    path = cfg.quarantine_dir / f"invalid_rows_{ts}.csv"
    master_df.to_csv(path, index=False)
    logger.warning("Validation failed; quarantined rows to %s", path)
    return path


def validate_data_sync(master_df: pd.DataFrame, cfg: PipelineConfig) -> bool:
    schema_ok = SchemaValidator(
        required_columns=["id", "beneficiary_name", "gift_type", "amount", "date"]
    ).run(master_df)
    id_ok = IDValidator(id_column="id").run(master_df)
    passed = schema_ok and id_ok

    if not passed and cfg.quarantine_invalid_rows:
        _quarantine_invalid_rows(master_df, cfg)

    return passed


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

    # Backward-compat bridge for existing callers
    if not cfg.entities and (beneficiaries_extractor is not None or gifts_extractor is not None):
        cfg.entities = [
            EntityConfig(
                name="beneficiaries",
                extractor=beneficiaries_extractor,
                required_columns=["beneficiary_id"],
            ),
            EntityConfig(
                name="gifts",
                extractor=gifts_extractor,
                required_columns=["beneficiary_id"],
                date_columns=["date"],
            ),
        ]
        cfg.source_mode = "custom"

    frames = extract_entities_sync(cfg)
    master_df = transform_entities_sync(cfg, frames)
    if not validate_data_sync(master_df, cfg):
        raise ValueError("Data validation failed. Aborting pipeline.")
    load_data_sync(master_df, loaders=loaders)
    return master_df


@task(name="Extract Entities")
def extract_entities(config_obj: PipelineConfig) -> dict[str, pd.DataFrame]:
    return extract_entities_sync(config_obj)


@task(name="Transform Entities")
def transform_entities(config_obj: PipelineConfig, entity_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return transform_entities_sync(config_obj, entity_frames)


@task(name="Validate Data")
def validate_data(config_obj: PipelineConfig, master_df: pd.DataFrame) -> bool:
    return validate_data_sync(master_df, config_obj)


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

    if not cfg.entities and (beneficiaries_extractor is not None or gifts_extractor is not None):
        cfg.entities = [
            EntityConfig(
                name="beneficiaries",
                extractor=beneficiaries_extractor,
                required_columns=["beneficiary_id"],
            ),
            EntityConfig(
                name="gifts",
                extractor=gifts_extractor,
                required_columns=["beneficiary_id"],
                date_columns=["date"],
            ),
        ]
        cfg.source_mode = "custom"

    frames = extract_entities(cfg)
    master_df = transform_entities(cfg, frames)
    if not validate_data(cfg, master_df):
        raise ValueError("Data validation failed. Aborting pipeline.")
    load_data(master_df, loaders=loaders)
    return master_df


if __name__ == "__main__":
    run_pipeline()
