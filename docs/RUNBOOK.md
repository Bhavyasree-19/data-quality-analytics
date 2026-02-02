# Runbook

## Run the full pipeline
```
.\.venv\Scripts\python src\main.py
```

## Generate only (custom sizes)
```
.\.venv\Scripts\python src\data_generator.py --customers 1200 --orders 6000 --web-events 15000
```

## Rebuild report only
```
.\.venv\Scripts\python src\report_builder.py
```

## View outputs
- `reports\quality_report.html`
- `data\processed\quality_results.json`
- `data\processed\run_history.jsonl`

## Common issues
- If results are empty, re-run the pipeline.
- If you see many schema failures, check `config\schema.yml` and the generator.
