"""extractor/base_extractor.py: Abstract base class for all extractors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

import config
from config import get_logger


@dataclass
class ExtractionResult:
    data: pd.DataFrame
    error: Exception | None = None

    @property
    def success(self) -> bool:
        return self.error is None


class BaseExtractor(ABC):
    """
    Every extractor must implement :meth:`extract`.
    Shared logging and a basic :meth:`run` wrapper with error handling
    are provided here so concrete subclasses stay focused on extraction logic.
    """

    def __init__(
        self,
        source: str,
        raise_on_error: bool = config.EXTRACT_RAISE_ON_ERROR,
    ) -> None:
        """
        Args:
            source: URL, file path, or API endpoint to extract data from.
            raise_on_error: If True, extraction errors are re-raised.
                            If False, :meth:`run` returns an empty DataFrame.
        """
        self.source = source
        self.raise_on_error = raise_on_error
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Pull data from :attr:`source` and return it as a raw DataFrame.

        Returns:
            pd.DataFrame: Raw, un-transformed data.
        """
        ...

    def run(self) -> pd.DataFrame:
        """
        Public entry point: calls :meth:`extract` with full error handling.

        Returns:
            pd.DataFrame: Raw data.

        Raises:
            Exception: Re-raised when ``raise_on_error`` is True.
        """
        self.logger.info("Starting extraction from: %s", self.source)
        try:
            df = self.extract()
            self.logger.info("Extraction complete. Rows returned: %d", len(df))
            return df
        except Exception as exc:
            self.logger.exception("Extraction failed for source '%s': %s", self.source, exc)
            if self.raise_on_error:
                raise
            self.logger.warning("Returning empty DataFrame because raise_on_error=False")
            return pd.DataFrame()
