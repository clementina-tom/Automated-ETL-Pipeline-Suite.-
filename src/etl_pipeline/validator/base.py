from abc import ABC, abstractmethod
import pandas as pd
from ..config import get_logger

class BaseValidator(ABC):
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        ...

    def run(self, df: pd.DataFrame) -> bool:
        self.logger.info("Running validator [%s]. Input rows: %d", self.__class__.__name__, len(df))
        try:
            result = self.validate(df)
            status = "PASSED" if result else "FAILED"
            self.logger.info("Validator %s: %s", self.__class__.__name__, status)
            return result
        except Exception as exc:
            self.logger.exception("Validator %s raised an error: %s", self.__class__.__name__, exc)
            return False
