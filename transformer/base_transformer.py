"""
transformer/base_transformer.py
Abstract base class for all transformers.
"""

from abc import ABC, abstractmethod

import pandas as pd

from config import get_logger


class BaseTransformer(ABC):
    """
    Every transformer must implement :meth:`transform`.
    Shared logging and a :meth:`run` wrapper are provided here.
    """

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean, reshape, or enrich *df*.

        Args:
            df: Input DataFrame from a previous pipeline step.

        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        ...

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Public entry point with error handling around :meth:`transform`.

        Returns:
            pd.DataFrame: Transformed data or the original *df* on failure.
        """
        self.logger.info(
            "Starting transformation [%s]. Input rows: %d",
            self.__class__.__name__,
            len(df),
        )
        try:
            result = self.transform(df)
            self.logger.info(
                "Transformation complete. Output rows: %d", len(result)
            )
            return result
        except Exception as exc:
            self.logger.exception(
                "Transformation failed in %s: %s", self.__class__.__name__, exc
            )
            return df
