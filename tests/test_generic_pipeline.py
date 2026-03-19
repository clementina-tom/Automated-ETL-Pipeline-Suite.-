import pandas as pd

from etl_pipeline import BaseExtractor, ETLPipelineBuilder, JoinSpec
from etl_pipeline.transformers import DataCleaner


class StaticExtractor(BaseExtractor):
    def __init__(self, name: str, df: pd.DataFrame):
        super().__init__(name=name)
        self.df = df

    def extract(self) -> pd.DataFrame:
        return self.df.copy()


def test_n_entity_pipeline_join_and_transform():
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

    assert len(result) == 2
    assert result.loc[0, "name"] == "A"
