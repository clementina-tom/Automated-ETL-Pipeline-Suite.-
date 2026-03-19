from pathlib import Path

import pandas as pd


class Checkpoint:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, key: str, df: pd.DataFrame) -> Path:
        path = self.base_dir / f"{key}.parquet"
        df.to_parquet(path, index=False)
        return path

    def load(self, key: str) -> pd.DataFrame | None:
        path = self.base_dir / f"{key}.parquet"
        if not path.exists():
            return None
        return pd.read_parquet(path)
