# Automated ETL Pipeline Suite

A modular Python ETL toolkit for extraction, transformation, validation, and loading with optional Prefect orchestration.

---

## What this project provides

- Pluggable extractors (`WebExtractor`, `PlaywrightExtractor`, `APIExtractor`, `DatabaseExtractor`)
- Reusable transforms (`DataCleaner`, `EntityMapper`)
- Validation layer (`SchemaValidator`, `IDValidator`)
- Multiple loaders (`SQLiteLoader`, `CSVLoader`, `S3Loader`)
- Library-first API (`run_etl`, `ETLPipeline`, `ETLPipelineBuilder`)
- Optional Prefect flow wrapper in `pipeline.py`

---

## Python & system requirements

- Python **3.10+** (project metadata requires `>=3.10`)
- `pip`
- For Playwright extractor: browser dependencies + Chromium

---

## Installation

### Option A: Install as a local package (recommended)

```bash
python -m pip install --upgrade pip
pip install -e .
```

### Option B: Install from requirements file

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Playwright setup (only if using PlaywrightExtractor)

```bash
playwright install --with-deps chromium
```

---

## Repository structure (actual paths)

```text
.
├── config.py
├── pipeline.py
├── etl_pipeline/          # Library interface
│   ├── __init__.py
│   └── library.py
├── extractor/
├── transformer/
├── validator/
├── loader/
├── tests/
├── pyproject.toml
├── requirements.txt
└── Dockerfile
```

---

## Quick start (CLI/script)

Run the default Prefect-backed flow with synthetic sample input:

```bash
python pipeline.py
```

Artifacts produced:

- `output/master_table_YYYYMMDD_HHMMSS.csv`
- `etl_output.db`
- `logs/etl.log`

---


- `output/master_table_YYYYMMDD_HHMMSS.csv`
- `etl_output.db`
- `logs/etl.log`

---

## Use as a library

### One-shot API (`run_etl`)

```python
from etl_pipeline import PipelineConfig, run_etl
from extractor.api_extractor import APIExtractor
from extractor.web_extractor import WebExtractor

beneficiaries = WebExtractor("https://example.com/beneficiaries")
gifts = APIExtractor("https://example.com/api/gifts", page_size=100)

master_df = run_etl(
    config=PipelineConfig(source_mode="custom"),
    beneficiaries_extractor=beneficiaries,
    gifts_extractor=gifts,
)
```

### Builder API (`ETLPipelineBuilder`)

```python
from etl_pipeline import ETLPipelineBuilder
from extractor.web_extractor import WebExtractor
from extractor.api_extractor import APIExtractor

pipeline = (
    ETLPipelineBuilder()
    .with_source_mode("custom")
    .with_beneficiaries_extractor(WebExtractor("https://example.com/beneficiaries"))
    .with_gifts_extractor(APIExtractor("https://example.com/api/gifts", page_size=100))
    .build()
)

master_df = pipeline.run()
```

---

## Running tests

```bash
pytest -v
```

The suite includes:

- unit tests for transformers, validators, loaders
- integration-style tests using a real local HTTP server
- integration-style tests using a temporary SQLite database

---

## Configuration notes

Core defaults are in `config.py`:

- `DEFAULT_SCRAPE_URL`
- `REQUEST_TIMEOUT_SECONDS`
- `SQLITE_DATABASE_URL`
- `MASTER_COLUMNS`

Pipeline runtime mode is configured with `PipelineConfig` in `pipeline.py`:

- `source_mode="synthetic"` for sample input
- `source_mode="custom"` for injected extractors

---

## Troubleshooting

### `ModuleNotFoundError` for `etl_pipeline`
Install the package in editable mode from repo root:

```bash
pip install -e .
```

### Playwright errors about missing browser
Install Chromium:

```bash
playwright install --with-deps chromium
```

### Validation errors abort the run
Inspect logs at `logs/etl.log` and verify extracted columns include:

- `id`
- `beneficiary_name`
- `gift_type`
- `amount`
- `date`

---

## CI

## Library Usage

You can use this project as an importable library:

```python
from etl_pipeline import ETLPipelineBuilder
from extractor.web_extractor import WebExtractor
from extractor.api_extractor import APIExtractor

pipeline = (
    ETLPipelineBuilder()
    .with_source_mode("custom")
    .with_beneficiaries_extractor(WebExtractor("https://example.com/beneficiaries"))
    .with_gifts_extractor(APIExtractor("https://example.com/api/gifts", page_size=100))
    .build()
)

master_df = pipeline.run()
```

For one-shot execution, use `run_etl(...)` from `etl_pipeline`.

## Migration notes

## Real-source Testing

The suite now includes integration-style tests that use:
- a real local HTTP server for `WebExtractor` and `APIExtractor`
- a real temporary SQLite database for `DatabaseExtractor`

Run all tests with:

```bash
pytest -v
```

See `examples/`:
- `users_orders.py`
- `async_extraction.py`

## CI

GitHub Actions runs tests across Python 3.11 and 3.12 and verifies Docker build.
