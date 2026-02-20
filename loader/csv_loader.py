"""
loader/csv_loader.py
Persist a DataFrame to a timestamped CSV file in the output directory.
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import config
from loader.base_loader import BaseLoader


class CSVLoader(BaseLoader):
    """
    Write a DataFrame to a CSV file.

    Files are saved to *output_dir* and named::

        <prefix>_YYYYMMDD_HHMMSS.csv

    This ensures each pipeline run produces a unique, traceable artefact.

    Args:
        prefix:      Filename prefix (default: ``"etl_output"``).
        output_dir:  Directory to write to.  Defaults to ``config.OUTPUT_DIR``.
        encoding:    File encoding (default: ``"utf-8-sig"`` for Excel compat).
        index:       Whether to write the row index.
    """

    def __init__(
        self,
        prefix: str = "etl_output",
        output_dir: Path = config.OUTPUT_DIR,
        encoding: str = "utf-8-sig",
        index: bool = False,
    ) -> None:
        super().__init__()
        self.prefix = prefix
        self.output_dir = Path(output_dir)
        self.encoding = encoding
        self.index = index

    def load(self, df: pd.DataFrame) -> None:
        if df.empty:
            self.logger.warning("DataFrame is empty — nothing to write to CSV.")
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{self.prefix}_{timestamp}.csv"
        filepath = self.output_dir / filename

        df.to_csv(filepath, index=self.index, encoding=self.encoding)
        self.logger.info(
            "Wrote %d rows to '%s'.", len(df), filepath
        )

    def latest_file(self) -> Path | None:
        """Return the path to the most-recently written CSV in *output_dir*, or None."""
        csvs = sorted(self.output_dir.glob(f"{self.prefix}_*.csv"))
        return csvs[-1] if csvs else None
