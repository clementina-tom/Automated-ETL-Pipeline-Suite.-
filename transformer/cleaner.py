"""
transformer/cleaner.py
DataCleaner: normalise column names, trim whitespace, drop duplicates/nulls.
"""

import pandas as pd

from transformer.base_transformer import BaseTransformer


class DataCleaner(BaseTransformer):
    """
    Performs standard DataFrame hygiene:

    1. Normalise column names   → snake_case, no leading/trailing spaces.
    2. Strip string whitespace  → all object columns.
    3. Drop full duplicate rows.
    4. Drop rows where *required_columns* contain NaN.
    5. Standardise date columns → pandas datetime.

    All steps are configurable via constructor arguments so the cleaner
    can be shared across different pipelines without subclassing.
    """

    def __init__(
        self,
        required_columns: list[str] | None = None,
        date_columns: list[str] | None = None,
        drop_na_threshold: float | None = None,   # e.g. 0.5 → drop rows missing >50 % of values
    ) -> None:
        super().__init__()
        self.required_columns = required_columns or []
        self.date_columns = date_columns or []
        self.drop_na_threshold = drop_na_threshold

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df = self._normalise_columns(df)
        df = self._strip_strings(df)
        df = self._drop_duplicates(df)
        df = self._handle_nulls(df)
        df = self._parse_dates(df)

        return df

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    def _normalise_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )
        self.logger.debug("Normalised columns: %s", list(df.columns))
        return df

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip())
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(keep='first')
        df = df.reset_index(drop=True)
        dropped = before - len(df)
        if dropped:
            self.logger.info("Dropped %d duplicate rows.", dropped)
        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop rows missing required columns
        if self.required_columns:
            existing = [c for c in self.required_columns if c in df.columns]
            before = len(df)
            df = df.dropna(subset=existing)
            dropped = before - len(df)
            if dropped:
                self.logger.info(
                    "Dropped %d rows with nulls in required columns: %s",
                    dropped,
                    existing,
                )

        # Optional threshold-based drop
        if self.drop_na_threshold is not None:
            before = len(df)
            df = df.dropna(thresh=int(len(df.columns) * (1 - self.drop_na_threshold)))
            self.logger.info(
                "Dropped %d rows below null threshold (%.0f%%).",
                before - len(df),
                self.drop_na_threshold * 100,
            )
        return df

    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                self.logger.debug("Parsed date column: %s", col)
        return df
