from pathlib import Path

import pandas as pd

from .base import BaseLoader


class CSVLoader(BaseLoader):
    def __init__(self, path: Path):
        self.path = path

    def load(self, df: pd.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, index=False)
