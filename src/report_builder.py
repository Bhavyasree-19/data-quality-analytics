import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
RESULTS_PATH = BASE_DIR / "data" / "processed" / "quality_results.json"
REPORT_DIR = BASE_DIR / "reports"


def build_summary(df):
    summary = (
        df.groupby(["dataset", "status"], as_index=False)
        .size()
        .pivot(index="dataset", columns="status", values="size")
        .fillna(0)
    )
    summary["total_checks"] = summary.sum(axis=1)
    summary["pass_rate"] = (summary.get("PASS", 0) / summary["total_checks"] * 100).round(1)
    return summary.reset_index()


def plot_summary(summary):
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(summary["dataset"], summary["pass_rate"], color="#60a5fa")
    ax.set_ylim(0, 100)
    ax.set_title("Data Quality Pass Rate by Dataset")
    ax.set_ylabel("Pass Rate (%)")
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    out_path = REPORT_DIR / "quality_summary.png"
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    return out_path


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    raw = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    df = pd.DataFrame(raw["results"])
    metrics = raw.get("metrics", {})
    run_id = metrics.get("run_id", "n/a")
    generated_at = metrics.get("generated_at", "n/a")
    pass_rate = metrics.get("pass_rate", 0)
    total_checks = metrics.get("total_checks", 0)
    failed_checks = metrics.get("failed_checks", 0)
    critical_failures = metrics.get("critical_failures", 0)
    sla = metrics.get("sla", {})
    sla_status = "PASS" if sla.get("pass") else "FAIL"
    summary = build_summary(df)
    chart_path = plot_summary(summary)

    if "severity" not in df.columns:
        df["severity"] = "medium"
    severity_summary = (
        df.groupby(["severity", "status"], as_index=False)
        .size()
        .pivot(index="severity", columns="status", values="size")
        .fillna(0)
        .reset_index()
    )

    table_html = df[
        ["dataset", "check", "check_type", "severity", "status", "failed_count", "total_count"]
    ].to_html(index=False)
    summary_html = summary.to_html(index=False)
    severity_html = severity_summary.to_html(index=False)

    report_html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Data Quality Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #0f172a; }}
    h1 {{ margin-bottom: 6px; }}
    .muted {{ color: #475569; }}
    img {{ max-width: 100%; height: auto; margin: 16px 0; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
    th, td {{ border: 1px solid #e2e8f0; padding: 8px; text-align: left; }}
    th {{ background: #f8fafc; }}
    .fail {{ color: #dc2626; font-weight: 700; }}
    .pass {{ color: #16a34a; font-weight: 700; }}
    .tag {{ display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 12px; background: #e2e8f0; }}
    .tag.pass {{ background: #dcfce7; color: #166534; }}
    .tag.fail {{ background: #fee2e2; color: #991b1b; }}
  </style>
</head>
<body>
  <h1>Data Quality & Analytics Report</h1>
  <p class="muted">Generated from synthetic RDBMS + NoSQL datasets.</p>
  <h2>Run Summary</h2>
  <ul>
    <li><strong>Run ID:</strong> {run_id}</li>
    <li><strong>Generated at:</strong> {generated_at}</li>
    <li><strong>Total checks:</strong> {total_checks}</li>
    <li><strong>Failed checks:</strong> {failed_checks}</li>
    <li><strong>Pass rate:</strong> {pass_rate}%</li>
    <li><strong>Critical failures:</strong> {critical_failures}</li>
    <li><strong>SLA status:</strong> <span class="tag {sla_status.lower()}">{sla_status}</span></li>
  </ul>
  <h2>Pass Rate Summary</h2>
  {summary_html}
  <h2>Severity Summary</h2>
  {severity_html}
  <img src="{chart_path.name}" alt="Pass rate chart" />
  <h2>Detailed Checks</h2>
  {table_html}
  <script>
    document.querySelectorAll('table tr').forEach((row) => {{
      const statusCell = row.cells[4];
      if (!statusCell) return;
      const status = statusCell.textContent.trim();
      if (status === 'FAIL') statusCell.classList.add('fail');
      if (status === 'PASS') statusCell.classList.add('pass');
    }});
  </script>
</body>
</html>
"""

    (REPORT_DIR / "quality_report.html").write_text(report_html.strip(), encoding="utf-8")
    print("Report saved to reports/quality_report.html")


if __name__ == "__main__":
    main()
