import pytest

hypothesis = pytest.importorskip("hypothesis")
st = hypothesis.strategies
given = hypothesis.given

import pandas as pd

from etl_pipeline.transformers import DataCleaner


@given(st.lists(st.text(min_size=0, max_size=10), min_size=1, max_size=20))
def test_cleaner_preserves_row_count_for_single_column(values):
    df = pd.DataFrame({"Raw Col": values})
    out = DataCleaner(strip_whitespace=True, drop_duplicates=False).transform(df)
    assert len(out) == len(df)
