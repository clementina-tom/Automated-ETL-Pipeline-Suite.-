"""tests/test_transformer.py
Unit tests for DataCleaner and EntityMapper.
"""

import pandas as pd

from etl_pipeline.transformers import DataCleaner, EntityMapper


class TestDataCleaner:
    def test_normalizes_columns_and_trims_strings(self):
        df = pd.DataFrame(
            {
                " Beneficiary ID ": ["B001", "B002"],
                "Beneficiary Name": [" Alice ", "Bob "],
            }
        )

        cleaner = DataCleaner(required_columns=["beneficiary_id"])
        result = cleaner.run(df)

        assert list(result.columns) == ["beneficiary_id", "beneficiary_name"]
        assert result.loc[0, "beneficiary_name"] == "Alice"
        assert result.loc[1, "beneficiary_name"] == "Bob"

    def test_drops_duplicates_and_required_nulls(self):
        df = pd.DataFrame(
            {
                "beneficiary_id": ["B001", "B001", None],
                "amount": [100, 100, 200],
            }
        )

        result = DataCleaner(required_columns=["beneficiary_id"]).run(df)

        # One duplicate row removed and null required row removed
        assert len(result) == 1
        assert result.iloc[0]["beneficiary_id"] == "B001"

    def test_parses_dates(self):
        df = pd.DataFrame({"date": ["2024-01-01", "invalid"]})
        result = DataCleaner(date_columns=["date"]).run(df)

        assert str(result["date"].dtype).startswith("datetime64")
        assert pd.isna(result.iloc[1]["date"])


class TestEntityMapper:
    def test_merges_and_adds_processed_at(self):
        beneficiaries = pd.DataFrame(
            {
                "beneficiary_id": ["B001"],
                "beneficiary_name": ["Alice"],
                "status": ["active"],
                "source_url": ["http://example.test"],
            }
        )
        gifts = pd.DataFrame(
            {
                "beneficiary_id": ["B001"],
                "id": ["G001"],
                "gift_type": ["Cash"],
                "amount": [100.0],
                "date": [pd.Timestamp("2024-01-01")],
            }
        )

        result = EntityMapper(right_df=gifts).run(beneficiaries)

        assert len(result) == 1
        assert result.iloc[0]["id"] == "G001"
        assert "processed_at" in result.columns

    def test_respects_output_columns(self):
        left = pd.DataFrame({"beneficiary_id": ["B001"], "beneficiary_name": ["Alice"]})
        right = pd.DataFrame({"beneficiary_id": ["B001"], "id": ["G001"]})

        mapper = EntityMapper(
            right_df=right,
            output_columns=["id", "beneficiary_name", "processed_at"],
        )
        result = mapper.run(left)

        assert list(result.columns) == ["id", "beneficiary_name", "processed_at"]
