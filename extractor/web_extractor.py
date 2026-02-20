"""
extractor/web_extractor.py
Static-page extractor using requests + BeautifulSoup.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config
from extractor.base_extractor import BaseExtractor


class WebExtractor(BaseExtractor):
    """
    Fetch and parse a static HTML page.

    Override :meth:`_parse` in a subclass to customise how rows are extracted
    from the parsed soup.  The default implementation extracts all <table>
    rows as a DataFrame.
    """

    def __init__(
        self,
        source: str = config.DEFAULT_SCRAPE_URL,
        timeout: int = config.REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(source)
        self.timeout = timeout
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        response = self._fetch()
        soup = BeautifulSoup(response.text, "lxml")
        return self._parse(soup)

    # ------------------------------------------------------------------
    # Helpers  (override these in subclasses for custom parsing)
    # ------------------------------------------------------------------

    def _fetch(self) -> requests.Response:
        """GET the source URL and raise on HTTP errors."""
        self.logger.debug("GET %s (timeout=%ss)", self.source, self.timeout)
        response = self._session.get(self.source, timeout=self.timeout)
        response.raise_for_status()
        return response

    def _parse(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Default parser: extract the first <table> found on the page.

        Returns an empty DataFrame if no table is found.
        """
        table = soup.find("table")
        if table is None:
            self.logger.warning("No <table> found at %s", self.source)
            return pd.DataFrame()

        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rows.append(cells)

        if not rows:
            self.logger.warning("Table found but contains no rows.")
            return pd.DataFrame(columns=headers)

        return pd.DataFrame(rows, columns=headers if headers else None)
