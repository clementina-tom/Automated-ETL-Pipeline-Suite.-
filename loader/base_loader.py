"""
loader/base_loader.py
Abstract base class for all loaders.
"""

from abc import ABC, abstractmethod

import pandas as pd

from config import get_logger


class BaseLoader(ABC):
    """
    Every loader must implement :meth:`load`.
    Shared logging and a :meth:`run` wrapper are provided here.
    """

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        """
        Persist *df* to the destination (DB table, file, etc.).

        Args:
            df: Final, validated DataFrame to store.
        """
        ...

    def run(self, df: pd.DataFrame) -> None:
        """
        Public entry point with error handling around :meth:`load`.
        """
        self.logger.info(
            "Starting load [%s]. Rows to write: %d",
            self.__class__.__name__,
            len(df),
        )
        try:
            self.load(df)
            self.logger.info("Load complete [%s].", self.__class__.__name__)
        except Exception as exc:
            self.logger.exception(
                "Load failed in %s: %s", self.__class__.__name__, exc
            )
            raise
