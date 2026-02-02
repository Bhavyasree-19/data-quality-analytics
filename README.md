# Data Quality & Analytics - E-commerce

A production-style data quality pipeline for an e-commerce domain. It generates realistic customer, order, and web-event data, runs rule-based validation and anomaly checks, and produces both an HTML report and a Streamlit dashboard.

## What this project shows
- A realistic **data quality pipeline** for RDBMS + NoSQL sources
- **Rule-driven validation** with severity and SLA tracking
- **Schema contracts** to detect missing/invalid fields
- **Run metadata + history** for governance and audits
- **Interactive monitoring** via Streamlit

## Pipeline (simple view)
1) **Generate data** (SQLite + JSONL)
2) **Validate quality** (rules + schema + anomalies)
3) **Report & monitor** (HTML + dashboard)

## Data model
- **customers** (SQLite)
- **orders** (SQLite)
- **web_events** (JSONL)

See field-level details in `docs\DATA_DICTIONARY.md`.

## Quality checks
- Not null, uniqueness, regex validation
- Range checks, allowed values, referential integrity
- Schema required fields + schema type validation
- Z-score anomaly detection

Severity and SLA thresholds are defined in `config\quality_profile.yml`.

## Project structure
```
data-quality-analytics/
  config/
    data_profile.yml
    quality_rules.yml
    quality_profile.yml
    schema.yml
  data/
    raw/
      rdbms.db
      web_events.jsonl
    processed/
      quality_results.json
      run_history.jsonl
  reports/
    quality_report.html
    quality_summary.png
  src/
    data_generator.py
    quality_checks.py
    report_builder.py
    main.py
  tests/
    test_quality_checks.py
  streamlit_app.py
  docs/
    ARCHITECTURE.md
    DATA_DICTIONARY.md
    QUALITY_POLICY.md
    RUNBOOK.md
  requirements.txt
  requirements-dev.txt
```

## Quickstart
```
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python src\main.py
```
Open the report: `reports\quality_report.html`

## Simplest run (Windows)
- Double click `run.bat` to generate data + checks + report.
- Double click `dashboard.bat` to open the Streamlit dashboard.

## Streamlit dashboard
```
.\.venv\Scripts\python -m streamlit run streamlit_app.py
```

## Run tests
```
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m pytest -q
```

## CI
- GitHub Actions runs pytest on every push and pull request (`.github/workflows/ci.yml`).

## Configuration
- `config\data_profile.yml`: dataset sizes + date ranges
- `config\quality_rules.yml`: validation rules
- `config\schema.yml`: schema contracts (required columns + types)
- `config\quality_profile.yml`: severity map + SLA thresholds

## Outputs
- `data\processed\quality_results.json`: latest run results
- `data\processed\run_history.jsonl`: run-by-run history
- `reports\quality_report.html`: human-readable report

## Notes
- The generator injects a few bad records intentionally to show failures.
- Data is synthetic (no real customer data). Use your own source to go live.
