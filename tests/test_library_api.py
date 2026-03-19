"""Tests for the library-first interface."""

import pandas as pd

from etl_pipeline import ETLPipelineBuilder, PipelineConfig, run_etl
from extractor.base_extractor import BaseExtractor


class StaticExtractor(BaseExtractor):
    def __init__(self, source: str, data: pd.DataFrame):
        super().__init__(source)
        self._data = data

    def extract(self) -> pd.DataFrame:
        return self._data.copy()


def _beneficiaries_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002"],
            "beneficiary_name": ["Alice", "Bob"],
            "status": ["active", "inactive"],
            "source_url": ["http://source", "http://source"],
        }
    )


def _gifts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002"],
            "id": ["G001", "G002"],
            "gift_type": ["Cash", "In-Kind"],
            "amount": [100.0, 250.0],
            "date": ["2024-01-01", "2024-01-02"],
        }
    )


def test_run_etl_function_custom_sources(tmp_path):
    beneficiaries = StaticExtractor("beneficiaries", _beneficiaries_df())
    gifts = StaticExtractor("gifts", _gifts_df())

    result = run_etl(
        config=PipelineConfig(source_mode="custom"),
        beneficiaries_extractor=beneficiaries,
        gifts_extractor=gifts,
    )

    assert len(result) == 2
    assert {"id", "beneficiary_name", "gift_type", "amount"}.issubset(result.columns)


def test_builder_api_runs_pipeline_with_custom_extractors():
    pipeline = (
        ETLPipelineBuilder()
        .with_source_mode("custom")
        .with_beneficiaries_extractor(StaticExtractor("beneficiaries", _beneficiaries_df()))
        .with_gifts_extractor(StaticExtractor("gifts", _gifts_df()))
        .build()
    )

    result = pipeline.run()

    assert len(result) == 2
    assert result["id"].tolist() == ["G001", "G002"]
