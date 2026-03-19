"""Async extraction example."""

import asyncio

from etl_pipeline.pipeline import Pipeline
from etl_pipeline.extractors import APIExtractor


async def main() -> None:
    pipeline = Pipeline(extractors={
        "users": APIExtractor(name="users", url="https://jsonplaceholder.typicode.com/users")
    })
    result = await pipeline.arun()
    print(result.metrics)


if __name__ == "__main__":
    asyncio.run(main())
