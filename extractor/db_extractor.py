"""
extractor/db_extractor.py
A data extractor for fetching records from a SQL database.
"""

import pandas as pd
from sqlalchemy import create_engine

from extractor.base_extractor import BaseExtractor


class DatabaseExtractor(BaseExtractor):
    """
    Extract data from a SQL database using SQLAlchemy.
    """

    def __init__(
        self,
        connection_string: str,
        query: str,
        raise_on_error: bool = True,
    ) -> None:
        super().__init__(source=connection_string, raise_on_error=raise_on_error)
        self.query = query

    def extract(self) -> pd.DataFrame:
        self.logger.debug("Running SQL query: %s", self.query)
        engine = create_engine(self.source)
        with engine.connect() as connection:
            df = pd.read_sql_query(self.query, connection)

        self.logger.info("Database extraction complete. Rows returned: %d", len(df))
        return df
