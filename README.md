# Chicago 311 Data Quality

A small pipeline to:
- fetch Chicago 311 service requests
- run basic data-quality checks (uniqueness, dates, coords, legacy flags, “information only” heuristic)
- write human-readable reports to `notes/`

## Quick start
```bash
# 1) create & activate a virtual env (Windows PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# 2) install deps
pip install -r requirements.txt

# 3) configure your token
copy .env.example .env   # then edit .env and set SOCRATA_APP_TOKEN

# 4) run quality checks (writes notes/data_quality_checks.md)
python src/explore_311.py --source api --limit 50000 --out notes/data_quality_checks.md --mark-done

# 5) ingest recent data (writes data/raw_311.parquet and notes/data_ingest_summary.md)
python src/fetch.py --days 90 --out data/raw_311.parquet --summary notes/data_ingest_summary.md