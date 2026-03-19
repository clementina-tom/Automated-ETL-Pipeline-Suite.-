import pandas as pd

from ..base import BaseTransformer


class DataCleaner(BaseTransformer):
    def __init__(self, strip_whitespace: bool = True, drop_duplicates: bool = True):
        self.strip_whitespace = strip_whitespace
        self.drop_duplicates = drop_duplicates

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = out.columns.str.strip().str.lower().str.replace(r"[^\w]+", "_", regex=True)
        if self.strip_whitespace:
            obj_cols = out.select_dtypes(include="object").columns
            out[obj_cols] = out[obj_cols].apply(lambda s: s.str.strip())
        if self.drop_duplicates:
            out = out.drop_duplicates().reset_index(drop=True)
        return out
