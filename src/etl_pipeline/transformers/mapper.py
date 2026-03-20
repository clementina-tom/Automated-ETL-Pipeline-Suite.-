import pandas as pd
from ..base import BaseTransformer
from .. import config

class EntityMapper(BaseTransformer):
    def __init__(
        self,
        right_df: pd.DataFrame,
        left_key: str = config.BENEFICIARY_JOIN_KEY,
        right_key: str = config.GIFT_JOIN_KEY,
        how: str = "left",
        output_columns: list[str] | None = None,
    ) -> None:
        super().__init__()
        self.right_df = right_df
        self.left_key = left_key
        self.right_key = right_key
        self.how = how
        self.output_columns = output_columns or config.MASTER_COLUMNS

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        merged = pd.merge(
            df,
            self.right_df,
            left_on=self.left_key,
            right_on=self.right_key,
            how=self.how,
            suffixes=("_left", "_right"),
        )
        merged["processed_at"] = pd.Timestamp.utcnow()
        keep = [c for c in self.output_columns if c in merged.columns]
        return merged[keep]
