import pandas as pd
from .base import BaseValidator

class IDValidator(BaseValidator):
    def __init__(self, id_column: str = "id", allow_duplicates: bool = False) -> None:
        super().__init__()
        self.id_column = id_column
        self.allow_duplicates = allow_duplicates

    def validate(self, df: pd.DataFrame) -> bool:
        passed = True
        if self.id_column not in df.columns:
            self.logger.error("ID column '%s' is not present in the DataFrame.", self.id_column)
            return False
        series = df[self.id_column]
        null_count = series.isna().sum()
        if null_count > 0:
            self.logger.error("ID column '%s' contains %d null value(s).", self.id_column, null_count)
            passed = False
        dup_count = series.duplicated().sum()
        if dup_count > 0:
            dup_ids = series[series.duplicated(keep=False)].unique().tolist()
            if self.allow_duplicates:
                self.logger.warning("ID column '%s' has %d duplicate(s) (allowed): %s", self.id_column, dup_count, dup_ids[:10])
            else:
                self.logger.error("ID column '%s' has %d duplicate(s): %s", self.id_column, dup_count, dup_ids[:10])
                passed = False
        if passed:
            self.logger.info("ID validation passed for column '%s'.", self.id_column)
        return passed
