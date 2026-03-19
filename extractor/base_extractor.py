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
    """Base class for extractors with consistent logging and error behavior."""

    def __init__(
        self,
        source: str,
        raise_on_error: bool = config.EXTRACT_RAISE_ON_ERROR,
    ) -> None:
        self.source = source
        self.raise_on_error = raise_on_error
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        ...

    def run_result(self) -> ExtractionResult:
        """Run extractor and return structured result instead of raising."""
        self.logger.info("Starting extraction from: %s", self.source)
        try:
            df = self.extract()
            self.logger.info("Extraction complete. Rows returned: %d", len(df))
            return ExtractionResult(data=df)
        except Exception as exc:
            self.logger.exception("Extraction failed for source '%s': %s", self.source, exc)
            return ExtractionResult(data=pd.DataFrame(), error=exc)

    def run(self) -> pd.DataFrame:
        """Run extractor and return DataFrame, optionally fail-fast."""
        result = self.run_result()
        if result.success:
            return result.data
        if self.raise_on_error and result.error is not None:
            raise result.error
        self.logger.warning("Returning empty DataFrame because raise_on_error=False")
        return result.data
