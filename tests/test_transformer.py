import pandas as pd


def dirty_df() -> pd.DataFrame:
    return pd.DataFrame({
        "Gift Type": ["In-Kind", None, "In-Kind", "Cash"],
        "Amount": [100, 100, 100, 200],
        "Date": ["2021-01-01", "2021-01-01", "2021-01-01", "2021-01-02"],
    })
