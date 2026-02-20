"""
extractor/playwright_extractor.py
JavaScript-capable extractor using Microsoft Playwright (async API wrapped synchronously).
"""

import asyncio
from typing import Any

import pandas as pd
from playwright.async_api import async_playwright, Page

import config
from extractor.base_extractor import BaseExtractor


class PlaywrightExtractor(BaseExtractor):
    """
    Render and scrape JavaScript-heavy pages using Playwright.

    By default this extractor:
      1. Navigates to :attr:`source`.
      2. Waits for the network to be idle.
      3. Calls :meth:`_parse_page` to extract data from the rendered DOM.

    Override :meth:`_parse_page` in a subclass to add custom interactions
    (clicking, scrolling, form submission, etc.) before scraping.

    Example::

        class MyExtractor(PlaywrightExtractor):
            async def _parse_page(self, page: Page) -> pd.DataFrame:
                await page.click("#load-more")
                await page.wait_for_selector(".data-row")
                rows = await page.query_selector_all(".data-row")
                …
    """

    def __init__(
        self,
        source: str = config.DEFAULT_SCRAPE_URL,
        headless: bool = config.PLAYWRIGHT_HEADLESS,
        browser: str = "chromium",          # "chromium" | "firefox" | "webkit"
    ) -> None:
        super().__init__(source)
        self.headless = headless
        self.browser_type = browser

    # ------------------------------------------------------------------
    # BaseExtractor interface
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """Run the async Playwright session synchronously."""
        return asyncio.run(self._async_extract())

    # ------------------------------------------------------------------
    # Async internals
    # ------------------------------------------------------------------

    async def _async_extract(self) -> pd.DataFrame:
        async with async_playwright() as pw:
            browser_launcher = getattr(pw, self.browser_type)
            browser = await browser_launcher.launch(headless=self.headless)
            try:
                page = await browser.new_page()
                self.logger.debug(
                    "Navigating to %s (browser=%s, headless=%s)",
                    self.source,
                    self.browser_type,
                    self.headless,
                )
                await page.goto(self.source, wait_until="networkidle")
                return await self._parse_page(page)
            finally:
                await browser.close()

    async def _parse_page(self, page: Page) -> pd.DataFrame:
        """
        Default parser: extract all rows from the first <table> on the page.

        Override in a subclass for custom page interactions or richer parsing.
        """
        # Try to grab table headers
        header_handles = await page.query_selector_all("table th")
        headers = [await h.inner_text() for h in header_handles]

        # Grab all data rows
        row_handles = await page.query_selector_all("table tr")
        rows: list[list[Any]] = []
        for tr in row_handles:
            cells = await tr.query_selector_all("td")
            cell_texts = [await c.inner_text() for c in cells]
            if cell_texts:
                rows.append(cell_texts)

        if not rows:
            self.logger.warning("No table rows found at %s", self.source)
            return pd.DataFrame()

        return pd.DataFrame(rows, columns=headers if headers else None)
