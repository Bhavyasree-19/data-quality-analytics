from pathlib import Path
import sys

import pandas as pd
import yaml

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.append(str(SRC_DIR))

from quality_checks import DataQualityRunner


def write_rules(tmp_path, rules):
    path = tmp_path / "rules.yml"
    path.write_text(yaml.safe_dump(rules), encoding="utf-8")
    return path


def write_schema(tmp_path, schema):
    path = tmp_path / "schema.yml"
    path.write_text(yaml.safe_dump(schema), encoding="utf-8")
    return path


def run_checks(tmp_path, rules, datasets, schema=None):
    rules_path = write_rules(tmp_path, rules)
    schema_path = write_schema(tmp_path, schema) if schema else None
    runner = DataQualityRunner(rules_path, schema_path=schema_path)
    output = runner.run(datasets)
    return output["results"]


def find_result(results, dataset, check):
    for result in results:
        if result["dataset"] == dataset and result["check"] == check:
            return result
    raise AssertionError(f"Missing result for {dataset}:{check}")


def test_not_null(tmp_path):
    rules = {"customers": {"not_null": ["email"]}}
    df = pd.DataFrame({"email": ["a@b.com", None]})
    results = run_checks(tmp_path, rules, {"customers": df})
    result = find_result(results, "customers", "not_null:email")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 1


def test_unique(tmp_path):
    rules = {"customers": {"unique": ["email"]}}
    df = pd.DataFrame({"email": ["a@b.com", "a@b.com", "b@b.com"]})
    results = run_checks(tmp_path, rules, {"customers": df})
    result = find_result(results, "customers", "unique:email")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 2


def test_regex(tmp_path):
    rules = {"customers": {"regex": {"email": "^.+@.+\\..+$"}}}
    df = pd.DataFrame({"email": ["ok@test.com", "bad-email"]})
    results = run_checks(tmp_path, rules, {"customers": df})
    result = find_result(results, "customers", "regex:email")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 1


def test_ranges(tmp_path):
    rules = {"customers": {"ranges": {"age": {"min": 18, "max": 65}}}}
    df = pd.DataFrame({"age": [25, 17, 70]})
    results = run_checks(tmp_path, rules, {"customers": df})
    result = find_result(results, "customers", "range:age")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 2


def test_allowed_values(tmp_path):
    rules = {"customers": {"allowed_values": {"status": ["active", "inactive"]}}}
    df = pd.DataFrame({"status": ["active", "paused"]})
    results = run_checks(tmp_path, rules, {"customers": df})
    result = find_result(results, "customers", "allowed:status")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 1


def test_referential_integrity(tmp_path):
    rules = {"orders": {"referential_integrity": {"customer_id": "customers.customer_id"}}}
    customers = pd.DataFrame({"customer_id": [1, 2, 3]})
    orders = pd.DataFrame({"customer_id": [1, 2, 99]})
    results = run_checks(tmp_path, rules, {"customers": customers, "orders": orders})
    result = find_result(results, "orders", "fk:customer_id->customers.customer_id")
    assert result["status"] == "FAIL"
    assert result["failed_count"] == 1


def test_anomaly_detection(tmp_path):
    rules = {"orders": {}}
    values = [10] * 50 + [1000]
    orders = pd.DataFrame({"order_total": values})
    results = run_checks(tmp_path, rules, {"orders": orders})
    result = find_result(results, "orders", "anomaly_zscore:order_total")
    assert result["status"] == "FAIL"
    assert result["failed_count"] >= 1


def test_schema_required(tmp_path):
    rules = {"customers": {}}
    schema = {"customers": {"required": {"customer_id": "int", "email": "string"}}}
    df = pd.DataFrame({"email": ["a@b.com"]})
    results = run_checks(tmp_path, rules, {"customers": df}, schema=schema)
    result = find_result(results, "customers", "schema_required:customer_id")
    assert result["status"] == "FAIL"


def test_schema_type(tmp_path):
    rules = {"customers": {}}
    schema = {"customers": {"required": {"age": "int"}}}
    df = pd.DataFrame({"age": ["bad", 22]})
    results = run_checks(tmp_path, rules, {"customers": df}, schema=schema)
    result = find_result(results, "customers", "schema_type:age")
    assert result["status"] == "FAIL"
