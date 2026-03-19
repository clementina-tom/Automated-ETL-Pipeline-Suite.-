# etl-pipeline (Domain-Agnostic ETL Library)

A generic ETL **library** for any domain (users/orders, IoT, finance, social media), with sync + async execution, hooks, middleware, schema controls, and optional checkpointing.

## Architecture

```mermaid
flowchart LR
  E[Extractors (N)] --> J[Join Engine]
  J --> T[Transform Chain]
  T --> V[Schema/Validation]
  V --> L[Loaders (N)]
  E -.hooks/middleware.-> O[Observability]
  T -.lineage.-> O
```

## Install

```bash
pip install etl-pipeline
```

Extras:

```bash
pip install "etl-pipeline[web]"    # playwright/bs4/lxml
pip install "etl-pipeline[s3]"     # boto3
pip install "etl-pipeline[async]"  # aiohttp/aiosqlite/asyncpg
```

## Quick generic example

```python
from etl_pipeline import ETLPipelineBuilder, JoinSpec
from etl_pipeline.extractors import APIExtractor
from etl_pipeline.transformers import DataCleaner

pipeline = (
    ETLPipelineBuilder()
    .add_extractor("users", APIExtractor(name="users", url="https://api.example.com/users"))
    .add_extractor("orders", APIExtractor(name="orders", url="https://api.example.com/orders"))
    .add_join(JoinSpec(left="users", right="orders", left_on="user_id", right_on="user_id"))
    .add_transformer(DataCleaner())
    .build()
)

result = pipeline.run()
print(result.success, result.metrics)
```

## Declarative config

```python
from etl_pipeline.pipeline import Pipeline
from etl_pipeline.plugins import PluginRegistry
```

Use `Pipeline.from_dict(...)` or `Pipeline.from_yaml(...)` with registry mappings.

## Advanced features included

- Async-first extractor/transformer/loader interfaces
- Schema registry + evolution strategies
- Immutable lineage context and Mermaid export
- Middleware + hooks system
- Flow control config and checkpoint support
- Structured `ExtractionResult` and `PipelineResult`

## Migration notes

- Old beneficiaries/gifts concepts are now examples, not core architecture.
- Use `add_extractor(...)` + `add_join(...)` for domain-agnostic composition.
- Legacy aliases remain in builder (`with_beneficiaries_extractor`, `with_gifts_extractor`) for compatibility.

## Examples

See `examples/`:
- `users_orders.py`
- `async_extraction.py`

## ADRs

See `docs/adr/0001-domain-agnostic-library.md`.
