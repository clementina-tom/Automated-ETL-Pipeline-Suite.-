from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .base import BaseLoader
from . import config


class CSVLoader(BaseLoader):
    def __init__(
        self,
        prefix: str = "etl_output",
        output_dir: Path = config.OUTPUT_DIR,
        encoding: str = "utf-8-sig",
        index: bool = False,
    ) -> None:
        super().__init__()
        self.prefix = prefix
        self.output_dir = Path(output_dir)
        self.encoding = encoding
        self.index = index

    def load(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self.prefix}_{timestamp}.csv"
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=self.index, encoding=self.encoding)

    def latest_file(self) -> Path | None:
        csvs = sorted(self.output_dir.glob(f"{self.prefix}_*.csv"))
        return csvs[-1] if csvs else None


class SQLiteLoader(BaseLoader):
    def __init__(
        self,
        table_name: str = "master_table",
        db_url: str = config.SQLITE_DATABASE_URL,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.db_url = db_url
        self.if_exists = if_exists
        self.index = index
        self._engine = create_engine(db_url)

    def load(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        try:
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
            versioned_df = df.copy()
            versioned_df["run_id"] = timestamp
            versioned_df.to_sql(
                name=self.table_name,
                con=self._engine,
                if_exists=self.if_exists,
                index=self.index,
            )
        except SQLAlchemyError:
            raise

    def query(self, sql: str) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn)
