import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
RESULTS_PATH = BASE_DIR / "data" / "processed" / "quality_results.json"


@st.cache_data
def load_results():
    if not RESULTS_PATH.exists():
        return None, None
    raw = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(raw.get("results", []))
    metrics = raw.get("metrics", {})
    return df, metrics


def run_pipeline():
    subprocess.check_call([sys.executable, str(BASE_DIR / "src" / "main.py")])
    load_results.clear()


def main():
    st.set_page_config(page_title="Data Quality Dashboard", layout="wide")
    st.title("Data Quality and Analytics Dashboard")

    with st.sidebar:
        st.header("Controls")
        if st.button("Run pipeline"):
            with st.spinner("Running data generator and quality checks..."):
                try:
                    run_pipeline()
                    st.success("Pipeline completed")
                except subprocess.CalledProcessError as exc:
                    st.error(f"Pipeline failed: {exc}")

    df, metrics = load_results()
    if df is None or df.empty:
        st.warning("No results found. Run the pipeline to generate data and checks.")
        st.code("python src/main.py")
        return

    if "severity" not in df.columns:
        df["severity"] = "medium"
    if "check_type" not in df.columns:
        df["check_type"] = df["check"].str.split(":").str[0]

    dataset_options = sorted(df["dataset"].unique())
    status_options = ["PASS", "FAIL"]
    severity_options = sorted(df["severity"].unique())

    with st.sidebar:
        st.subheader("Filters")
        selected_datasets = st.multiselect("Datasets", dataset_options, default=dataset_options)
        selected_status = st.multiselect("Status", status_options, default=status_options)
        selected_severity = st.multiselect("Severity", severity_options, default=severity_options)

        if metrics:
            st.subheader("Metadata")
            run_id = metrics.get("run_id")
            generated_at = metrics.get("generated_at")
            sla = metrics.get("sla", {})
            if run_id:
                st.write(f"Run ID: {run_id}")
            if generated_at:
                st.write(f"Generated at: {generated_at}")
            if sla:
                st.write(f"SLA pass: {sla.get('pass')}")
            row_counts = metrics.get("row_counts", {})
            if row_counts:
                st.write("Row counts")
                for name, count in row_counts.items():
                    st.write(f"- {name}: {count}")

    filtered = df.copy()
    if selected_datasets:
        filtered = filtered[filtered["dataset"].isin(selected_datasets)]
    if selected_status:
        filtered = filtered[filtered["status"].isin(selected_status)]
    if selected_severity:
        filtered = filtered[filtered["severity"].isin(selected_severity)]

    if filtered.empty:
        st.warning("No checks match the selected filters.")
        return

    total_checks = len(filtered)
    failed_checks = int((filtered["status"] == "FAIL").sum())
    pass_rate = round((total_checks - failed_checks) / max(total_checks, 1) * 100, 1)

    critical_failures = int(
        ((filtered["status"] == "FAIL") & (filtered["severity"] == "critical")).sum()
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total checks", total_checks)
    col2.metric("Failed checks", failed_checks)
    col3.metric("Pass rate (%)", pass_rate)
    col4.metric("Critical failures", critical_failures)

    summary = (
        filtered.groupby(["dataset", "status"], as_index=False)
        .size()
        .pivot(index="dataset", columns="status", values="size")
        .fillna(0)
    )
    summary["total"] = summary.sum(axis=1)
    summary["pass_rate"] = (summary.get("PASS", 0) / summary["total"] * 100).round(1)

    st.subheader("Pass rate by dataset")
    st.bar_chart(summary[["pass_rate"]])

    st.subheader("Checks")
    display_cols = [
        "dataset",
        "check",
        "check_type",
        "severity",
        "status",
        "failed_count",
        "total_count",
    ]
    st.dataframe(filtered[display_cols], use_container_width=True)

    st.subheader("Inspect a check")
    filtered = filtered.assign(check_id=filtered["dataset"] + " | " + filtered["check"])
    check_options = filtered["check_id"].unique().tolist()
    selected_check = st.selectbox("Check", check_options)
    selected_row = filtered[filtered["check_id"] == selected_check].iloc[0]
    st.write(f"Status: {selected_row['status']}")
    st.write(f"Failed rows: {selected_row['failed_count']} of {selected_row['total_count']}")
    st.json(selected_row["sample"])


if __name__ == "__main__":
    main()
