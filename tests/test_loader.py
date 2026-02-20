"""
tests/test_loader.py
Unit tests for CSVLoader and SQLiteLoader.
"""

from pathlib import Path

import pandas as pd
import pytest

from loader.csv_loader import CSVLoader
from loader.sqlite_loader import SQLiteLoader


# ──────────────────────────────────────────────
# Shared fixture
# ──────────────────────────────────────────────

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": ["G001", "G002"],
        "beneficiary_name": ["Alice", "Bob"],
        "gift_type": ["Cash", "In-Kind"],
        "amount": [500.0, 200.0],
        "date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
        "status": ["active", "active"],
        "source_url": ["http://x.com", "http://x.com"],
        "processed_at": pd.Timestamp.utcnow(),
    })


# ──────────────────────────────────────────────
# CSVLoader tests
# ──────────────────────────────────────────────

class TestCSVLoader:
    def test_creates_csv_file(self, sample_df, tmp_path):
        loader = CSVLoader(output_dir=tmp_path)
        loader.run(sample_df)
        csvs = list(tmp_path.glob("etl_output_*.csv"))
        assert len(csvs) == 1

    def test_csv_has_correct_row_count(self, sample_df, tmp_path):
        loader = CSVLoader(output_dir=tmp_path)
        loader.run(sample_df)
        result = pd.read_csv(loader.latest_file())
        assert len(result) == len(sample_df)

    def test_empty_df_writes_no_file(self, tmp_path):
        loader = CSVLoader(output_dir=tmp_path)
        loader.run(pd.DataFrame())
        assert not list(tmp_path.glob("*.csv"))

    def test_custom_prefix_in_filename(self, sample_df, tmp_path):
        loader = CSVLoader(prefix="gifts", output_dir=tmp_path)
        loader.run(sample_df)
        csvs = list(tmp_path.glob("gifts_*.csv"))
        assert len(csvs) == 1


# ──────────────────────────────────────────────
# SQLiteLoader tests  (in-memory DB)
# ──────────────────────────────────────────────

class TestSQLiteLoader:
    @pytest.fixture
    def in_memory_loader(self):
        """Each test gets a fresh in-memory SQLite database."""
        return SQLiteLoader(
            table_name="test_table",
            db_url="sqlite:///:memory:",
            if_exists="replace",
        )

    def test_writes_rows_to_sqlite(self, sample_df, in_memory_loader):
        in_memory_loader.run(sample_df)
        result = in_memory_loader.query("SELECT * FROM test_table")
        assert len(result) == len(sample_df)

    def test_columns_match(self, sample_df, in_memory_loader):
        in_memory_loader.run(sample_df)
        result = in_memory_loader.query("SELECT * FROM test_table")
        assert set(sample_df.columns).issubset(set(result.columns))

    def test_empty_df_writes_nothing(self, in_memory_loader):
        in_memory_loader.run(pd.DataFrame())
        # No table was created — query must fail
        with pytest.raises(Exception):
            in_memory_loader.query("SELECT * FROM test_table")

    def test_append_mode_accumulates_rows(self, sample_df, tmp_path):
        db_url = f"sqlite:///{tmp_path / 'test.db'}"
        loader = SQLiteLoader(table_name="t", db_url=db_url, if_exists="append")
        loader.run(sample_df)
        loader.run(sample_df)
        result = loader.query("SELECT * FROM t")
        assert len(result) == len(sample_df) * 2
