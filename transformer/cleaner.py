"""DataCleaner: normalise columns, trim strings, drop duplicates/nulls, parse dates."""

from dataclasses import dataclass

import pandas as pd

from transformer.base_transformer import BaseTransformer


@dataclass
class CleaningMetrics:
    input_rows: int
    output_rows: int
    duplicate_rows_dropped: int
    required_null_rows_dropped: int
    threshold_rows_dropped: int


class DataCleaner(BaseTransformer):
    """Configurable DataFrame hygiene pipeline."""

    def __init__(
        self,
        required_columns: list[str] | None = None,
        date_columns: list[str] | None = None,
        drop_na_threshold: float | None = None,
    ) -> None:
        super().__init__()
        self.required_columns = required_columns or []
        self.date_columns = date_columns or []
        self.drop_na_threshold = drop_na_threshold
        self.metrics = CleaningMetrics(0, 0, 0, 0, 0)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        self.metrics.input_rows = len(df)

        df = self._normalise_columns(df)
        df = self._strip_strings(df)
        df = self._drop_duplicates(df)
        df = self._handle_nulls(df)
        df = self._parse_dates(df)

        self.metrics.output_rows = len(df)
        self.logger.info("Cleaning metrics: %s", self.metrics)
        return df

    def _normalise_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )
        self.logger.debug("Normalised columns: %s", list(df.columns))
        return df

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(keep="first").reset_index(drop=True)
        dropped = before - len(df)
        self.metrics.duplicate_rows_dropped += dropped
        if dropped:
            self.logger.info("Dropped %d duplicate rows.", dropped)
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.required_columns:
            existing = [c for c in self.required_columns if c in df.columns]
            before = len(df)
            df = df.dropna(subset=existing)
            dropped = before - len(df)
            self.metrics.required_null_rows_dropped += dropped
            if dropped:
                self.logger.info(
                    "Dropped %d rows with nulls in required columns: %s",
                    dropped,
                    existing,
                )

        if self.drop_na_threshold is not None:
            before = len(df)
            df = df.dropna(thresh=int(len(df.columns) * (1 - self.drop_na_threshold)))
            dropped = before - len(df)
            self.metrics.threshold_rows_dropped += dropped
            if dropped:
                self.logger.info(
                    "Dropped %d rows below null threshold (%.0f%%).",
                    dropped,
                    self.drop_na_threshold * 100,
                )
        return df

    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                self.logger.debug("Parsed date column: %s", col)
        return df
