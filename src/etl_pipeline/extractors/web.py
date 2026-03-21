import pandas as pd
import requests
from bs4 import BeautifulSoup

from ..base import BaseExtractor
from .. import config

class WebExtractor(BaseExtractor):
    def __init__(
        self,
        source: str = config.DEFAULT_SCRAPE_URL,
        timeout: int = config.REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(source)
        self.timeout = timeout
        self._session = requests.Session()

    def extract(self) -> pd.DataFrame:
        response = self._fetch()
        soup = BeautifulSoup(response.text, "lxml")
        return self._parse(soup)

    def _fetch(self) -> requests.Response:
        response = self._session.get(self.source, timeout=self.timeout)
        response.raise_for_status()
        return response

    def _parse(self, soup: BeautifulSoup) -> pd.DataFrame:
        table = soup.find("table")
        if table is None:
            return pd.DataFrame()
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rows.append(cells)
        if not rows:
            return pd.DataFrame(columns=headers)
        return pd.DataFrame(rows, columns=headers if headers else None)
