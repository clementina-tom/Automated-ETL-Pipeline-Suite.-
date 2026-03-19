"""Generic users/orders example using the domain-agnostic API."""

import pandas as pd

from etl_pipeline import ETLPipelineBuilder, BaseExtractor, JoinSpec
from etl_pipeline.transformers import DataCleaner


class StaticExtractor(BaseExtractor):
    def __init__(self, name: str, data: pd.DataFrame):
        super().__init__(name)
        self._data = data

    def extract(self) -> pd.DataFrame:
        return self._data.copy()


users = StaticExtractor("users", pd.DataFrame({"user_id": [1, 2], "name": [" A ", "B"]}))
orders = StaticExtractor("orders", pd.DataFrame({"user_id": [1, 2], "order_id": [10, 11]}))

result = (
    ETLPipelineBuilder()
    .add_extractor("users", users)
    .add_extractor("orders", orders)
    .add_join(JoinSpec(left="users", right="orders", left_on="user_id", right_on="user_id"))
    .add_transformer(DataCleaner())
    .build()
    .run()
)

print(result.data)
