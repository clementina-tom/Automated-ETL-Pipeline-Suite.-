"""Shared pytest fixtures."""

import pandas as pd
import pytest


@pytest.fixture
def sample_beneficiaries_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002"],
            "beneficiary_name": ["Alice", "Bob"],
            "status": ["active", "inactive"],
            "source_url": ["http://source", "http://source"],
        }
    )


@pytest.fixture
def sample_gifts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "beneficiary_id": ["B001", "B002"],
            "id": ["G001", "G002"],
            "gift_type": ["Cash", "In-Kind"],
            "amount": [100.0, 250.0],
            "date": ["2024-01-01", "2024-01-02"],
        }
    )
