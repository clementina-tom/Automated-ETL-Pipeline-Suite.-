from importlib import import_module
from importlib.util import find_spec

import pandas as pd
from sqlalchemy import create_engine

from ..base import BaseExtractor


class DatabaseExtractor(BaseExtractor):
    def __init__(self, name: str, connection_string: str, query: str, raise_on_error: bool = True):
        super().__init__(name=name, raise_on_error=raise_on_error)
        self.connection_string = connection_string
        self.query = query

    def extract(self) -> pd.DataFrame:
        engine = create_engine(self.connection_string)
        with engine.connect() as connection:
            return pd.read_sql_query(self.query, connection)

    async def aextract(self) -> pd.DataFrame:
        if find_spec("aiosqlite") is None or not self.connection_string.startswith("sqlite:///"):
            return self.extract()
        aiosqlite = import_module("aiosqlite")
        db_path = self.connection_string.replace("sqlite:///", "")
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute(self.query)
            rows = await cursor.fetchall()
            columns = [c[0] for c in cursor.description]
            return pd.DataFrame(rows, columns=columns)
