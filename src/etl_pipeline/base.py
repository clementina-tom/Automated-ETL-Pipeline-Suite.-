from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator

import pandas as pd

from .results import ExtractionResult


class BaseExtractor(ABC):
    def __init__(self, name: str, raise_on_error: bool = True) -> None:
        self.name = name
        self.raise_on_error = raise_on_error

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        ...

    async def aextract(self) -> pd.DataFrame:
        return self.extract()

    def stream(self, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        df = self.extract()
        if df.empty:
            return
        for idx in range(0, len(df), chunk_size):
            yield df.iloc[idx : idx + chunk_size]

    async def astream(self, chunk_size: int = 1000) -> AsyncIterator[pd.DataFrame]:
        df = await self.aextract()
        if df.empty:
            return
        for idx in range(0, len(df), chunk_size):
            yield df.iloc[idx : idx + chunk_size]

    def run_result(self) -> ExtractionResult:
        try:
            df = self.extract()
            return ExtractionResult(data=df, success=True, metadata={"source": self.name})
        except Exception as exc:
            if self.raise_on_error:
                raise
            return ExtractionResult(
                data=pd.DataFrame(),
                success=False,
                metadata={"source": self.name},
                errors=[str(exc)],
            )


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

    async def atransform(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.transform(df)


class BaseLoader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        ...

    async def aload(self, df: pd.DataFrame) -> None:
        self.load(df)
