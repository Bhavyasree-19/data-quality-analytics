# Architecture

## Data sources
- `data/raw/rdbms.db`: SQLite database with `customers` and `orders` tables.
- `data/raw/web_events.jsonl`: JSONL event log simulating NoSQL ingestion.

## Configuration
- `config/data_profile.yml`: dataset sizes and date ranges.
- `config/quality_rules.yml`: data quality rules per dataset.

## Validation pipeline
1. **Ingestion**: Load RDBMS tables and JSONL into Pandas.
2. **Validation**: Apply rules from `config/quality_rules.yml`.
3. **Anomaly checks**: Z-score outlier detection on numeric columns.
4. **Reporting**: Build HTML report + summary chart.
5. **Monitoring**: Streamlit dashboard for interactive insights.

## Governance mapping
- **Completeness**: `not_null` checks.
- **Uniqueness**: `unique` checks.
- **Validity**: `regex`, `ranges`, and `allowed_values`.
- **Integrity**: `referential_integrity` across datasets.
- **Monitoring**: report generation with pass rates and samples.
