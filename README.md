# Job Posting Radar

Pipeline for ingesting, normalizing, embedding, and searching Polish tech job postings.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
# Fetch two pages from No Fluff Jobs into data/raw/nofluff/<today>
python scripts/run_ingest.py --pages 2
```

## Configuration

- Defaults live in `app/config.py` and can be overridden with environment variables (see fields in `AppSettings`).
- Create a `.env` file if you need to change base URLs, timeouts, or rate limits.
- No Fluff Jobs ingestion calls `https://nofluffjobs.com/api/joboffers/main` with `pageTo/pageSize` pagination, region/language/salary params, and keeps only offers with disclosed salaries. Per-offer detail pages are fetched from `/job/<slug>`.

## Layout (MVP)

- `app/ingest`: source clients and fetch orchestration.
- `data/raw`: raw payloads by source and date.
- `scripts/run_ingest.py`: CLI to run ingestion.

More stages (normalize/embed/dedupe/search) will follow the same structure with thin CLI wrappers in `scripts/`.

Notes:
- No Fluff Jobs ingestion fetches detail pages by default; use `--skip-details` to disable.