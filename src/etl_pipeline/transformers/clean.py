from dataclasses import dataclass
import pandas as pd
from ..base import BaseTransformer

@dataclass
class CleaningMetrics:
    input_rows: int = 0
    output_rows: int = 0
    duplicate_rows_dropped: int = 0
    required_null_rows_dropped: int = 0
    threshold_rows_dropped: int = 0

class DataCleaner(BaseTransformer):
    def __init__(
        self,
        required_columns: list[str] | None = None,
        date_columns: list[str] | None = None,
        drop_na_threshold: float | None = None,
        strip_whitespace: bool = True,
        drop_duplicates: bool = True,
    ) -> None:
        super().__init__()
        self.required_columns = required_columns or []
        self.date_columns = date_columns or []
        self.drop_na_threshold = drop_na_threshold
        self.strip_whitespace = strip_whitespace
        self.drop_duplicates = drop_duplicates
        self.metrics = CleaningMetrics()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        self.metrics.input_rows = len(df)
        
        # Normalise columns
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )

        if self.strip_whitespace:
            obj_cols = df.select_dtypes(include="object").columns
            df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())

        if self.drop_duplicates:
            before = len(df)
            df = df.drop_duplicates().reset_index(drop=True)
            self.metrics.duplicate_rows_dropped = before - len(df)

        if self.required_columns:
            existing = [c for c in self.required_columns if c in df.columns]
            before = len(df)
            df = df.dropna(subset=existing)
            self.metrics.required_null_rows_dropped = before - len(df)

        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        self.metrics.output_rows = len(df)
        return df
