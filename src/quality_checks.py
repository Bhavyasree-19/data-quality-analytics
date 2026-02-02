import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
SCHEMA_PATH = BASE_DIR / "config" / "schema.yml"
PROFILE_PATH = BASE_DIR / "config" / "quality_profile.yml"
RUN_HISTORY_PATH = BASE_DIR / "data" / "processed" / "run_history.jsonl"


def load_yaml_optional(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def build_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def detect_invalid_int(series: pd.Series) -> pd.Index:
    numeric = pd.to_numeric(series, errors="coerce")
    invalid = series[series.notna() & numeric.isna()].index
    non_int = series[series.notna() & numeric.notna() & (numeric % 1 != 0)].index
    return invalid.union(non_int)


def detect_invalid_float(series: pd.Series) -> pd.Index:
    numeric = pd.to_numeric(series, errors="coerce")
    return series[series.notna() & numeric.isna()].index


def detect_invalid_bool(series: pd.Series) -> pd.Index:
    bool_types = (bool, np.bool_)
    return series[series.notna() & ~series.apply(lambda x: isinstance(x, bool_types))].index


def detect_invalid_string(series: pd.Series) -> pd.Index:
    return series[series.notna() & ~series.apply(lambda x: isinstance(x, str))].index


def detect_invalid_datetime(series: pd.Series) -> pd.Index:
    parsed = pd.to_datetime(series, errors="coerce")
    return series[series.notna() & parsed.isna()].index


TYPE_VALIDATORS = {
    "int": detect_invalid_int,
    "float": detect_invalid_float,
    "bool": detect_invalid_bool,
    "string": detect_invalid_string,
    "date": detect_invalid_datetime,
    "datetime": detect_invalid_datetime,
}


@dataclass
class CheckResult:
    dataset: str
    check: str
    check_type: str
    severity: str
    status: str
    failed_count: int
    total_count: int
    sample: List[Any]


class DataQualityRunner:
    def __init__(
        self,
        rules_path: Path,
        schema_path: Optional[Path] = None,
        profile_path: Optional[Path] = None,
    ):
        self.rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
        self.schema = load_yaml_optional(schema_path or SCHEMA_PATH)
        self.profile = load_yaml_optional(profile_path or PROFILE_PATH)
        self.severity_map = self.profile.get("severity_map", {})
        self.sla = self.profile.get("sla", {})
        self.results: List[CheckResult] = []
        self.metrics: Dict[str, Any] = {}
        self.run_id = build_run_id()

    def load_datasets(self) -> Dict[str, pd.DataFrame]:
        datasets = {}
        conn = sqlite3.connect(RAW_DIR / "rdbms.db")
        datasets["customers"] = pd.read_sql_query("SELECT * FROM customers", conn)
        datasets["orders"] = pd.read_sql_query("SELECT * FROM orders", conn)
        conn.close()

        events = []
        with open(RAW_DIR / "web_events.jsonl", "r", encoding="utf-8") as f:
            for line in f:
                events.append(json.loads(line))
        datasets["web_events"] = pd.DataFrame(events)
        return datasets

    def run(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        self.results = []
        self.metrics = {
            "row_counts": {},
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "run_id": self.run_id,
        }
        for name, df in datasets.items():
            self.metrics["row_counts"][name] = int(len(df))
            schema_rules = self.schema.get(name, {})
            self._schema_required(name, df, schema_rules.get("required", {}))
            self._schema_types(name, df, schema_rules.get("required", {}))
            rules = self.rules.get(name, {})
            self._not_null(name, df, rules.get("not_null", []))
            self._unique(name, df, rules.get("unique", []))
            self._regex(name, df, rules.get("regex", {}))
            self._ranges(name, df, rules.get("ranges", {}))
            self._allowed_values(name, df, rules.get("allowed_values", {}))
            self._referential_integrity(name, df, rules.get("referential_integrity", {}), datasets)
            self._anomaly_detection(name, df)

        summary = self._summarize_results()
        self.metrics.update(summary)

        return {
            "results": [r.__dict__ for r in self.results],
            "metrics": self.metrics,
        }

    def _append(self, dataset, check, failed_idx, total, sample):
        check_type = check.split(":")[0]
        severity = self.severity_map.get(check_type, "medium")
        status = "PASS" if failed_idx == 0 else "FAIL"
        self.results.append(
            CheckResult(
                dataset=dataset,
                check=check,
                check_type=check_type,
                severity=severity,
                status=status,
                failed_count=failed_idx,
                total_count=total,
                sample=sample,
            )
        )

    def _summarize_results(self) -> Dict[str, Any]:
        df = pd.DataFrame([r.__dict__ for r in self.results])
        if df.empty:
            return {
                "total_checks": 0,
                "failed_checks": 0,
                "pass_rate": 0.0,
                "critical_failures": 0,
                "sla": {"pass": False},
            }
        total_checks = int(len(df))
        failed_checks = int((df["status"] == "FAIL").sum())
        pass_rate = round((total_checks - failed_checks) / max(total_checks, 1) * 100, 1)
        critical_failures = int(((df["status"] == "FAIL") & (df["severity"] == "critical")).sum())
        sla_pass = True
        min_pass_rate = self.sla.get("min_pass_rate")
        max_critical_failures = self.sla.get("max_critical_failures")
        if min_pass_rate is not None and pass_rate < min_pass_rate:
            sla_pass = False
        if max_critical_failures is not None and critical_failures > max_critical_failures:
            sla_pass = False
        return {
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "pass_rate": pass_rate,
            "critical_failures": critical_failures,
            "sla": {
                "min_pass_rate": min_pass_rate,
                "max_critical_failures": max_critical_failures,
                "pass": sla_pass,
            },
        }

    def _schema_required(self, dataset, df, required: Dict[str, str]):
        for col in required.keys():
            if col not in df.columns:
                self._append(
                    dataset,
                    f"schema_required:{col}",
                    1,
                    0,
                    [{"missing_column": col}],
                )

    def _schema_types(self, dataset, df, required: Dict[str, str]):
        for col, expected_type in required.items():
            if col not in df.columns:
                continue
            validator = TYPE_VALIDATORS.get(expected_type)
            if not validator:
                continue
            invalid_idx = validator(df[col])
            if invalid_idx.empty:
                self._append(dataset, f"schema_type:{col}", 0, len(df), [])
                continue
            sample = df.loc[invalid_idx].head(5).to_dict("records")
            self._append(dataset, f"schema_type:{col}", len(invalid_idx), len(df), sample)

    def _not_null(self, dataset, df, cols):
        for col in cols:
            failed = df[df[col].isna()]
            self._append(dataset, f"not_null:{col}", len(failed), len(df), failed.head(5).to_dict("records"))

    def _unique(self, dataset, df, cols):
        for col in cols:
            dupes = df[df[col].duplicated(keep=False)]
            self._append(dataset, f"unique:{col}", len(dupes), len(df), dupes.head(5).to_dict("records"))

    def _regex(self, dataset, df, regex_map: Dict[str, str]):
        for col, pattern in regex_map.items():
            mask = df[col].fillna("").astype(str).str.match(pattern) == False
            failed = df[mask]
            self._append(dataset, f"regex:{col}", len(failed), len(df), failed.head(5).to_dict("records"))

    def _ranges(self, dataset, df, ranges_map: Dict[str, Dict[str, float]]):
        for col, bounds in ranges_map.items():
            series = pd.to_numeric(df[col], errors="coerce")
            min_val = bounds.get("min")
            max_val = bounds.get("max")
            mask = pd.Series(False, index=df.index)
            if min_val is not None:
                mask |= series < min_val
            if max_val is not None:
                mask |= series > max_val
            failed = df[mask]
            self._append(dataset, f"range:{col}", len(failed), len(df), failed.head(5).to_dict("records"))

    def _allowed_values(self, dataset, df, allowed_map: Dict[str, List[Any]]):
        for col, allowed in allowed_map.items():
            failed = df[~df[col].isin(allowed)]
            self._append(dataset, f"allowed:{col}", len(failed), len(df), failed.head(5).to_dict("records"))

    def _referential_integrity(self, dataset, df, ref_map, datasets):
        for col, ref in ref_map.items():
            ref_table, ref_col = ref.split(".")
            allowed = set(datasets[ref_table][ref_col].dropna().unique())
            failed = df[~df[col].isin(allowed)]
            self._append(dataset, f"fk:{col}->{ref}", len(failed), len(df), failed.head(5).to_dict("records"))

    def _anomaly_detection(self, dataset, df):
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            series = df[col].dropna()
            if series.empty:
                continue
            z = (series - series.mean()) / (series.std() if series.std() != 0 else 1)
            outliers = series[abs(z) > 3]
            sample = df.loc[outliers.index].head(5).to_dict("records")
            self._append(dataset, f"anomaly_zscore:{col}", len(outliers), len(df), sample)


def main():
    runner = DataQualityRunner(BASE_DIR / "config" / "quality_rules.yml")
    datasets = runner.load_datasets()
    output = runner.run(datasets)
    out_path = BASE_DIR / "data" / "processed" / "quality_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    history_entry = {
        "run_id": output["metrics"].get("run_id"),
        "generated_at": output["metrics"].get("generated_at"),
        "total_checks": output["metrics"].get("total_checks"),
        "failed_checks": output["metrics"].get("failed_checks"),
        "pass_rate": output["metrics"].get("pass_rate"),
        "critical_failures": output["metrics"].get("critical_failures"),
        "sla": output["metrics"].get("sla"),
        "row_counts": output["metrics"].get("row_counts"),
    }
    with open(RUN_HISTORY_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(history_entry) + "\n")
    print(f"Wrote results to {out_path}")


if __name__ == "__main__":
    main()
