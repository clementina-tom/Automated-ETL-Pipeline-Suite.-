"""
extractor/api_extractor.py
A data extractor for fetching records from an external REST API.
"""

import math
from typing import Any

import pandas as pd
import requests

import config
from config import get_logger
from extractor.base_extractor import BaseExtractor

logger = get_logger("api_extractor")


class APIExtractor(BaseExtractor):
    """
    Extracts data from a paginated REST API endpoint.
    Expects data in a JSON array or a specific 'data' key.
    """

    def __init__(self, endpoint_url: str, auth_token: str | None = None, page_size: int = 100):
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.page_size = page_size
        self.headers = {}
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"

    def run(self) -> pd.DataFrame:
        logger.info("Starting extraction from API: %s", self.endpoint_url)
        all_records: list[dict[str, Any]] = []
        page = 1

        while True:
            params = {"page": page, "size": self.page_size}
            logger.debug("Fetching page %d...", page)

            response = requests.get(
                self.endpoint_url,
                headers=self.headers,
                params=params,
                timeout=config.REQUEST_TIMEOUT_SECONDS,
            )

            # Raise an exception for HTTP errors
            response.raise_for_status()

            data = response.json()
            # Handle standard APIs that wrap results in a 'data' array
            records = data.get("data", []) if isinstance(data, dict) else data

            if not records:
                break

            all_records.extend(records)
            
            # Simple pagination logic: if we get fewer records than page_size, we're likely done.
            if len(records) < self.page_size:
                break

            page += 1

        logger.info("API Extraction completed. Total records retrieved: %d", len(all_records))
        return pd.DataFrame(all_records)
