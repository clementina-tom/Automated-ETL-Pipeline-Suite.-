from importlib import import_module
from importlib.util import find_spec

import pandas as pd
import requests

from ..base import BaseExtractor


class APIExtractor(BaseExtractor):
    def __init__(self, name: str, url: str, params: dict | None = None, raise_on_error: bool = True):
        super().__init__(name=name, raise_on_error=raise_on_error)
        self.url = url
        self.params = params or {}

    def extract(self) -> pd.DataFrame:
        response = requests.get(self.url, params=self.params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        return pd.DataFrame(payload)

    async def aextract(self) -> pd.DataFrame:
        if find_spec("aiohttp") is None:
            return self.extract()
        aiohttp = import_module("aiohttp")
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.url, params=self.params) as response:
                response.raise_for_status()
                payload = await response.json()
                if isinstance(payload, dict) and "data" in payload:
                    payload = payload["data"]
                return pd.DataFrame(payload)
