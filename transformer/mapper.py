"""
transformer/mapper.py
EntityMapper: merge/join two DataFrames (e.g. Beneficiaries ↔ Gifts).
"""

import pandas as pd

import config
from transformer.base_transformer import BaseTransformer


class EntityMapper(BaseTransformer):
    """
    Join two DataFrames on a shared key column to produce a unified master table.

    The canonical use-case is pairing a Beneficiaries frame with a Gifts frame::

        mapper = EntityMapper(
            right_df=gifts_df,
            left_key="beneficiary_id",
            right_key="beneficiary_id",
            how="left",
        )
        master_df = mapper.run(beneficiaries_df)

    Post-join, a ``processed_at`` timestamp column is added automatically.
    """

    def __init__(
        self,
        right_df: pd.DataFrame,
        left_key: str = config.BENEFICIARY_JOIN_KEY,
        right_key: str = config.GIFT_JOIN_KEY,
        how: str = "left",              # "left" | "inner" | "outer" | "right"
        output_columns: list[str] | None = None,
    ) -> None:
        """
        Args:
            right_df:       The second DataFrame to join (e.g., Gifts).
            left_key:       Join key on the left (main) DataFrame.
            right_key:      Join key on the right DataFrame.
            how:            Pandas merge type.
            output_columns: Optional list of columns to keep post-merge.
                            Defaults to :data:`config.MASTER_COLUMNS`.
        """
        super().__init__()
        self.right_df = right_df
        self.left_key = left_key
        self.right_key = right_key
        self.how = how
        self.output_columns = output_columns or config.MASTER_COLUMNS

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            "Merging on %s=%s (how=%s). Left rows: %d, Right rows: %d",
            self.left_key,
            self.right_key,
            self.how,
            len(df),
            len(self.right_df),
        )

        merged = pd.merge(
            df,
            self.right_df,
            left_on=self.left_key,
            right_on=self.right_key,
            how=self.how,
            suffixes=("_left", "_right"),
        )
        self.logger.info("Merged rows: %d", len(merged))

        # Add processing timestamp
        merged["processed_at"] = pd.Timestamp.utcnow()

        # Filter to only the desired output columns (skip missing ones gracefully)
        keep = [c for c in self.output_columns if c in merged.columns]
        missing = set(self.output_columns) - set(keep)
        if missing:
            self.logger.warning(
                "Output columns not found after merge and will be skipped: %s", missing
            )
        return merged[keep]
