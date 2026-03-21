import pandas as pd
from .base import BaseValidator

class SchemaValidator(BaseValidator):
    def __init__(self, required_columns: list[str], dtype_map: dict[str, str] | None = None) -> None:
        super().__init__()
        self.required_columns = required_columns
        self.dtype_map: dict[str, str] = dtype_map or {}

    def validate(self, df: pd.DataFrame) -> bool:
        passed = True
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            self.logger.error("Missing required columns: %s", missing)
            passed = False
        for col, expected_dtype in self.dtype_map.items():
            if col not in df.columns:
                continue
            actual = str(df[col].dtype)
            if actual != expected_dtype:
                self.logger.error("Column '%s' dtype mismatch: expected '%s', got '%s'.", col, expected_dtype, actual)
                passed = False
        if passed:
            self.logger.info("Schema validation passed.")
        return passed
