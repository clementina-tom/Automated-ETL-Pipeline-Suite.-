"""
validator/id_validator.py
Validates ID column integrity: no nulls, no duplicates.
"""

import pandas as pd

from validator.base_validator import BaseValidator


class IDValidator(BaseValidator):
    """
    Ensures an ID column is:

    - Present in the DataFrame.
    - Free of null / NaN values.
    - Free of duplicated values (i.e., acts as a primary key).

    Args:
        id_column: Name of the primary-key column to validate.
        allow_duplicates: Set ``True`` to warn instead of fail on duplicates
                          (useful for intermediate pipeline stages).
    """

    def __init__(
        self,
        id_column: str = "id",
        allow_duplicates: bool = False,
    ) -> None:
        super().__init__()
        self.id_column = id_column
        self.allow_duplicates = allow_duplicates

    def validate(self, df: pd.DataFrame) -> bool:
        passed = True

        # 1. Column existence
        if self.id_column not in df.columns:
            self.logger.error(
                "ID column '%s' is not present in the DataFrame.", self.id_column
            )
            return False   # Cannot run further checks — bail early

        series = df[self.id_column]

        # 2. Null check
        null_count = series.isna().sum()
        if null_count > 0:
            self.logger.error(
                "ID column '%s' contains %d null value(s).", self.id_column, null_count
            )
            passed = False

        # 3. Duplicate check
        dup_count = series.duplicated().sum()
        if dup_count > 0:
            dup_ids = series[series.duplicated(keep=False)].unique().tolist()
            if self.allow_duplicates:
                self.logger.warning(
                    "ID column '%s' has %d duplicate(s) (allowed): %s",
                    self.id_column,
                    dup_count,
                    dup_ids[:10],  # Show first 10 to avoid log spam
                )
            else:
                self.logger.error(
                    "ID column '%s' has %d duplicate(s): %s",
                    self.id_column,
                    dup_count,
                    dup_ids[:10],
                )
                passed = False

        if passed:
            self.logger.info(
                "ID validation passed for column '%s'.", self.id_column
            )
        return passed
