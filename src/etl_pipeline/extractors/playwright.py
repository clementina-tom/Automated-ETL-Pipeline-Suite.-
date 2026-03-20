import asyncio
from typing import Any
import pandas as pd
from playwright.async_api import async_playwright, Page

from ..base import BaseExtractor
from .. import config

class PlaywrightExtractor(BaseExtractor):
    def __init__(
        self,
        source: str = config.DEFAULT_SCRAPE_URL,
        headless: bool = config.PLAYWRIGHT_HEADLESS,
        browser: str = "chromium",
    ) -> None:
        super().__init__(source)
        self.headless = headless
        self.browser_type = browser

    def extract(self) -> pd.DataFrame:
        return asyncio.run(self._async_extract())

    async def _async_extract(self) -> pd.DataFrame:
        async with async_playwright() as pw:
            browser_launcher = getattr(pw, self.browser_type)
            browser = await browser_launcher.launch(headless=self.headless)
            try:
                page = await browser.new_page()
                await page.goto(self.source, wait_until="networkidle")
                return await self._parse_page(page)
            finally:
                await browser.close()

    async def _parse_page(self, page: Page) -> pd.DataFrame:
        header_handles = await page.query_selector_all("table th")
        headers = [await h.inner_text() for h in header_handles]
        row_handles = await page.query_selector_all("table tr")
        rows: list[list[Any]] = []
        for tr in row_handles:
            cells = await tr.query_selector_all("td")
            cell_texts = [await c.inner_text() for c in cells]
            if cell_texts:
                rows.append(cell_texts)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=headers if headers else None)
