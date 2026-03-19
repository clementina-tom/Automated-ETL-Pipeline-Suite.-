"""Tests for BaseExtractor error behavior."""

import pandas as pd
import pytest

from extractor.base_extractor import BaseExtractor


class FailingExtractor(BaseExtractor):
    def extract(self) -> pd.DataFrame:
        raise RuntimeError("boom")


def test_extractor_raises_when_raise_on_error_true():
    extractor = FailingExtractor("test://source", raise_on_error=True)
    with pytest.raises(RuntimeError):
        extractor.run()


def test_extractor_returns_empty_df_when_raise_on_error_false():
    extractor = FailingExtractor("test://source", raise_on_error=False)
    result = extractor.run()
    assert result.empty
