"""
extractor/db_extractor.py
A data extractor for fetching records from a SQL database.
"""

import pandas as pd
from sqlalchemy import create_engine

from config import get_logger
from extractor.base_extractor import BaseExtractor

logger = get_logger("db_extractor")


class DatabaseExtractor(BaseExtractor):
    """
    Extracts data using SQLAlchemy.
    """

    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    def run(self) -> pd.DataFrame:
        logger.info("Starting db extraction...")
        
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                df = pd.read_sql_query(self.query, connection)
                
            logger.info("Extracted %d rows from database.", len(df))
            return df
        except Exception as e:
            logger.error("Failed executing database extraction: %s", e)
            raise
