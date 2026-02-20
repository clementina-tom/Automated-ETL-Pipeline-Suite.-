"""
validator/schema_validator.py
Validates that the DataFrame has the required columns and correct dtypes.
"""

from typing import Any

import pandas as pd

from validator.base_validator import BaseValidator


class SchemaValidator(BaseValidator):
    """
    Asserts that a DataFrame matches an expected schema.

    Args:
        required_columns: Columns that MUST exist (irrespective of dtype).
        dtype_map:        Optional ``{column: expected_dtype_str}`` mapping,
                          e.g. ``{"amount": "float64", "date": "datetime64[ns]"}``.

    Returns ``True`` only when every required column is present AND every
    dtype assertion passes.
    """

    def __init__(
        self,
        required_columns: list[str],
        dtype_map: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.required_columns = required_columns
        self.dtype_map: dict[str, str] = dtype_map or {}

    def validate(self, df: pd.DataFrame) -> bool:
        passed = True

        # 1. Required column presence
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            self.logger.error("Missing required columns: %s", missing)
            passed = False

        # 2. Dtype assertions (only for columns that exist)
        for col, expected_dtype in self.dtype_map.items():
            if col not in df.columns:
                continue  # Already caught above if it was required
            actual = str(df[col].dtype)
            if actual != expected_dtype:
                self.logger.error(
                    "Column '%s' dtype mismatch: expected '%s', got '%s'.",
                    col,
                    expected_dtype,
                    actual,
                )
                passed = False

        if passed:
            self.logger.info("Schema validation passed.")
        return passed
