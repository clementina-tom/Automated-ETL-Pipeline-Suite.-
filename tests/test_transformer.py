"""
tests/test_transformer.py
Unit tests for DataCleaner and EntityMapper.
"""

import pandas as pd
import pytest

from transformer.cleaner import DataCleaner
from transformer.mapper import EntityMapper


# ──────────────────────────────────────────────
# DataCleaner tests
# ──────────────────────────────────────────────

class TestDataCleaner:
    @pytest.fixture
    def dirty_df(self):
        return pd.DataFrame({
            "  First Name  ": ["  Alice  ", "Bob", "Bob"],
            "Gift Type": ["Cash", None, "In-Kind"],
            "Amount": [100, 200, 200],
        })

    def test_normalises_column_names(self, dirty_df):
        result = DataCleaner().run(dirty_df)
        assert "first_name" in result.columns
        assert "gift_type" in result.columns

    def test_strips_string_whitespace(self, dirty_df):
        result = DataCleaner().run(dirty_df)
        assert result["first_name"].iloc[0] == "Alice"

    def test_drops_duplicates(self, dirty_df):
        result = DataCleaner().run(dirty_df)
        assert len(result) < len(dirty_df)

    def test_drops_rows_with_missing_required_columns(self, dirty_df):
        result = DataCleaner(required_columns=["gift_type"]).run(dirty_df)
        assert result["gift_type"].isna().sum() == 0

    def test_parses_date_column(self):
        df = pd.DataFrame({"id": [1, 2], "date": ["2024-01-01", "2024-06-15"]})
        result = DataCleaner(date_columns=["date"]).run(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_empty_dataframe_returns_empty(self):
        result = DataCleaner().run(pd.DataFrame())
        assert result.empty


# ──────────────────────────────────────────────
# EntityMapper tests
# ──────────────────────────────────────────────

class TestEntityMapper:
    @pytest.fixture
    def beneficiaries(self):
        return pd.DataFrame({
            "beneficiary_id": ["B001", "B002"],
            "beneficiary_name": ["Alice", "Bob"],
            "status": ["active", "active"],
            "source_url": ["http://x.com", "http://x.com"],
        })

    @pytest.fixture
    def gifts(self):
        return pd.DataFrame({
            "beneficiary_id": ["B001", "B002"],
            "id": ["G001", "G002"],
            "gift_type": ["Cash", "In-Kind"],
            "amount": [500.0, 200.0],
            "date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
        })

    def test_merge_produces_master_columns(self, beneficiaries, gifts):
        mapper = EntityMapper(right_df=gifts)
        result = mapper.run(beneficiaries)
        # All available master columns should be present
        for col in ["id", "beneficiary_name", "gift_type", "amount"]:
            assert col in result.columns

    def test_processed_at_column_added(self, beneficiaries, gifts):
        mapper = EntityMapper(right_df=gifts)
        result = mapper.run(beneficiaries)
        assert "processed_at" in result.columns

    def test_row_count_matches_left_join(self, beneficiaries, gifts):
        mapper = EntityMapper(right_df=gifts, how="left")
        result = mapper.run(beneficiaries)
        assert len(result) == len(beneficiaries)

    def test_inner_join_excludes_unmatched(self, beneficiaries, gifts):
        # Add an unmatched beneficiary
        extra = pd.concat([beneficiaries, pd.DataFrame({
            "beneficiary_id": ["B999"],
            "beneficiary_name": ["NoGift"],
            "status": ["active"],
            "source_url": ["http://x.com"],
        })], ignore_index=True)
        mapper = EntityMapper(right_df=gifts, how="inner")
        result = mapper.run(extra)
        assert len(result) == 2
