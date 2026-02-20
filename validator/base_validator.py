"""
validator/base_validator.py
Abstract base class for all validators.
"""

from abc import ABC, abstractmethod

import pandas as pd

from config import get_logger


class BaseValidator(ABC):
    """
    Every validator must implement :meth:`validate`.
    Shared logging and a :meth:`run` wrapper that always returns a bool
    are provided here.
    """

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        """
        Inspect *df* and return True if it passes all checks.

        Args:
            df: DataFrame to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        ...

    def run(self, df: pd.DataFrame) -> bool:
        """
        Public entry point with error handling around :meth:`validate`.

        Returns:
            bool: Validation result; False on unexpected errors.
        """
        self.logger.info(
            "Running validator [%s]. Input rows: %d",
            self.__class__.__name__,
            len(df),
        )
        try:
            result = self.validate(df)
            status = "PASSED" if result else "FAILED"
            self.logger.info("Validator %s: %s", self.__class__.__name__, status)
            return result
        except Exception as exc:
            self.logger.exception(
                "Validator %s raised an error: %s", self.__class__.__name__, exc
            )
            return False
