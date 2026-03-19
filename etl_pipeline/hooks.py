from typing import Protocol

import pandas as pd


class PipelineHook(Protocol):
    def on_extract_start(self, source_name: str) -> None: ...
    def on_extract_end(self, source_name: str, rows: int) -> None: ...
    def on_transform(self, transform_name: str, rows_in: int, rows_out: int) -> None: ...
    def on_validate_fail(self, errors: list[str]) -> None: ...
    def on_load_end(self, loader_name: str, rows: int) -> None: ...


class NoopHook:
    def on_extract_start(self, source_name: str) -> None:
        return

    def on_extract_end(self, source_name: str, rows: int) -> None:
        return

    def on_transform(self, transform_name: str, rows_in: int, rows_out: int) -> None:
        return

    def on_validate_fail(self, errors: list[str]) -> None:
        return

    def on_load_end(self, loader_name: str, rows: int) -> None:
        return
