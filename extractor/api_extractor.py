"""
extractor/api_extractor.py
A data extractor for fetching records from an external REST API.
"""

from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from extractor.base_extractor import BaseExtractor


class APIExtractor(BaseExtractor):
    """
    Extract data from a paginated REST API endpoint.

    The extractor expects either:
    - a JSON array response, or
    - a JSON object containing a ``data`` array.
    """

    def __init__(
        self,
        endpoint_url: str,
        auth_token: str | None = None,
        page_size: int = 100,
    ) -> None:
        super().__init__(endpoint_url)
        self.auth_token = auth_token
        self.page_size = page_size
        self.headers: dict[str, str] = {}
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"

    def extract(self) -> pd.DataFrame:
        all_records: list[dict[str, Any]] = []
        page = 1

        while True:
            params = {"page": page, "size": self.page_size}
            self.logger.debug("Fetching API page %d from %s", page, self.source)

            response = requests.get(
                self.source,
                headers=self.headers,
                params=params,
                timeout=config.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            payload = response.json()
            records = payload.get("data", []) if isinstance(payload, dict) else payload

            if not records:
                break

            all_records.extend(records)

            # If fewer rows than requested, we've likely reached the final page.
            if len(records) < self.page_size:
                break

            page += 1

        self.logger.info("API extraction complete. Total records: %d", len(all_records))
        return pd.DataFrame(all_records)
