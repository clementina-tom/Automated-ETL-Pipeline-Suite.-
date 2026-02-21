# ETL Pipeline Suite

![CI](https://github.com/YOUR_USERNAME/etl_pipeline/actions/workflows/verify.yml/badge.svg)

A modular, object-oriented Python ETL pipeline for web and API data extraction, transformation, validation, and loading. Orchestrated by Prefect.

---

## Project Structure

```
etl_pipeline/
├── config.py                    # Central settings & structured JSON logging
├── pipeline.py                  # Prefect Orchestrator (Extract→Transform→Validate→Load)
├── requirements.txt
├── Dockerfile                   # For containerization
├── .dockerignore
│
├── extractor/
│   ├── base_extractor.py
│   ├── web_extractor.py
│   ├── playwright_extractor.py
│   ├── api_extractor.py         # REST API source
│   └── db_extractor.py          # SQLAlchemy DB source
│
├── transformer/
│   ├── base_transformer.py
│   ├── cleaner.py
│   └── mapper.py
│
├── validator/
│   ├── base_validator.py
│   ├── schema_validator.py
│   └── id_validator.py
│
├── loader/
│   ├── base_loader.py
│   ├── sqlite_loader.py         # Versioned SQLAlchemy SQLite output
│   ├── csv_loader.py            # Versioned timestamped CSV output
│   └── s3_loader.py             # Push to AWS S3 buckets
│
├── tests/
│
├── .github/workflows/
│   └── verify.yml               # CI with scheduled runs & Docker build check
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure your source

Edit `config.py`:

```python
DEFAULT_SCRAPE_URL = "https://your-target-site.com"
BENEFICIARY_JOIN_KEY = "beneficiary_id"
SQLITE_DB_PATH = BASE_DIR / "etl_output.db"
```

### 3. Run the pipeline (dry-run with synthetic data)

```bash
python pipeline.py
```

Outputs:
- `output/master_table_YYYYMMDD_HHMMSS.csv`
- `etl_output.db` (SQLite)
- `logs/etl.log`

### 4. Run tests

```bash
python -m pytest tests/ -v
```

---

## Extending the Pipeline

| Task | What to do |
|---|---|
| New data source | Subclass `BaseExtractor`, override `extract()` |
| Custom JS interaction | Subclass `PlaywrightExtractor`, override `_parse_page(page)` |
| New cleaning rule | Subclass `DataCleaner` or extend `transform()` |
| Different join logic | Subclass `EntityMapper`, set `how="inner"` etc. |
| Load to PostgreSQL | Subclass `BaseLoader`, swap the SQLAlchemy URL |

---

## Master Table Schema

| Column | Type | Description |
|---|---|---|
| `id` | str | Primary key (Gift ID) |
| `beneficiary_name` | str | Recipient full name |
| `gift_type` | str | Cash / In-Kind / Other |
| `amount` | float | Monetary value |
| `date` | datetime | Date of gift |
| `status` | str | Beneficiary status |
| `source_url` | str | Scraped from URL |
| `processed_at` | datetime | UTC pipeline run time |

---

## GitHub Actions

CI runs on every push to `main` / `develop` and on all PRs.
Installs Playwright with Chromium and runs `pytest` across Python 3.11 and 3.12.
