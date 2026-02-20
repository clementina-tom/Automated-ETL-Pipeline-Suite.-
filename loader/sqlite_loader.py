"""
loader/sqlite_loader.py
Persist a DataFrame to a SQLite database via SQLAlchemy.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import config
from loader.base_loader import BaseLoader


class SQLiteLoader(BaseLoader):
    """
    Write a DataFrame to a SQLite table using SQLAlchemy.

    - Creates the database and table automatically if they do not exist.
    - Supports ``replace``, ``append``, and ``fail`` if-exists strategies.

    Args:
        table_name:  Target SQL table name.
        db_url:      SQLAlchemy database URL.  Defaults to the project's
                     central SQLite path defined in ``config``.
        if_exists:   ``"replace"`` | ``"append"`` | ``"fail"``
                     (pandas ``to_sql`` semantics).
        index:       Whether to write the DataFrame index as a column.
    """

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
            self.logger.warning("DataFrame is empty — nothing to write to SQLite.")
            return

        try:
            df.to_sql(
                name=self.table_name,
                con=self._engine,
                if_exists=self.if_exists,
                index=self.index,
            )
            self.logger.info(
                "Wrote %d rows to table '%s' in %s.",
                len(df),
                self.table_name,
                self.db_url,
            )
        except SQLAlchemyError as exc:
            self.logger.exception(
                "SQLAlchemy error while writing to '%s': %s", self.table_name, exc
            )
            raise

    def query(self, sql: str) -> pd.DataFrame:
        """Convenience method: run a SQL statement and return results as a DataFrame."""
        with self._engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn)
