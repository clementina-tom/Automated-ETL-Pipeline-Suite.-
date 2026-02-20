"""
tests/test_validator.py
Unit tests for SchemaValidator and IDValidator.
"""

import pandas as pd
import pytest

from validator.schema_validator import SchemaValidator
from validator.id_validator import IDValidator


# ──────────────────────────────────────────────
# SchemaValidator tests
# ──────────────────────────────────────────────

class TestSchemaValidator:
    @pytest.fixture
    def valid_df(self):
        return pd.DataFrame({
            "id": ["G001", "G002"],
            "beneficiary_name": ["Alice", "Bob"],
            "amount": [500.0, 200.0],
        })

    def test_passes_when_all_required_columns_present(self, valid_df):
        validator = SchemaValidator(required_columns=["id", "beneficiary_name", "amount"])
        assert validator.run(valid_df) is True

    def test_fails_when_column_missing(self, valid_df):
        validator = SchemaValidator(required_columns=["id", "gift_type"])
        assert validator.run(valid_df) is False

    def test_passes_dtype_assertion(self, valid_df):
        validator = SchemaValidator(
            required_columns=["amount"],
            dtype_map={"amount": "float64"},
        )
        assert validator.run(valid_df) is True

    def test_fails_dtype_mismatch(self, valid_df):
        # amount is float64, but we assert it should be int64
        validator = SchemaValidator(
            required_columns=["amount"],
            dtype_map={"amount": "int64"},
        )
        assert validator.run(valid_df) is False

    def test_empty_required_columns_always_passes(self, valid_df):
        validator = SchemaValidator(required_columns=[])
        assert validator.run(valid_df) is True


# ──────────────────────────────────────────────
# IDValidator tests
# ──────────────────────────────────────────────

class TestIDValidator:
    @pytest.fixture
    def clean_df(self):
        return pd.DataFrame({"id": ["G001", "G002", "G003"]})

    def test_passes_with_unique_non_null_ids(self, clean_df):
        assert IDValidator(id_column="id").run(clean_df) is True

    def test_fails_when_id_column_missing(self, clean_df):
        assert IDValidator(id_column="missing_col").run(clean_df) is False

    def test_fails_with_null_ids(self):
        df = pd.DataFrame({"id": ["G001", None, "G003"]})
        assert IDValidator(id_column="id").run(df) is False

    def test_fails_with_duplicate_ids(self):
        df = pd.DataFrame({"id": ["G001", "G001", "G003"]})
        assert IDValidator(id_column="id").run(df) is False

    def test_allows_duplicates_when_flag_set(self):
        df = pd.DataFrame({"id": ["G001", "G001", "G003"]})
        # Should warn but not fail
        assert IDValidator(id_column="id", allow_duplicates=True).run(df) is True
