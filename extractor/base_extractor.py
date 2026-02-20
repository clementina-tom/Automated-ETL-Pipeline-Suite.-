"""
extractor/base_extractor.py
Abstract base class for all extractors.
"""

from abc import ABC, abstractmethod

import pandas as pd

from config import get_logger


class BaseExtractor(ABC):
    """
    Every extractor must implement :meth:`extract`.
    Shared logging and a basic :meth:`run` wrapper with error handling
    are provided here so concrete subclasses stay focused on extraction logic.
    """

    def __init__(self, source: str) -> None:
        """
        Args:
            source: URL, file path, or API endpoint to extract data from.
        """
        self.source = source
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Pull data from :attr:`source` and return it as a raw DataFrame.

        Returns:
            pd.DataFrame: Raw, un-transformed data.

        Raises:
            NotImplementedError: If subclass does not implement this method.
        """
        ...

    def run(self) -> pd.DataFrame:
        """
        Public entry point: calls :meth:`extract` with full error handling.

        Returns:
            pd.DataFrame: Raw data or empty DataFrame on failure.
        """
        self.logger.info("Starting extraction from: %s", self.source)
        try:
            df = self.extract()
            self.logger.info(
                "Extraction complete. Rows returned: %d", len(df)
            )
            return df
        except Exception as exc:
            self.logger.exception(
                "Extraction failed for source '%s': %s", self.source, exc
            )
            return pd.DataFrame()
