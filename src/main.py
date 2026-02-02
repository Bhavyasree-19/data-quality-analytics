import argparse
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def run_step(step):
    print(f"\n==> {step}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run the data quality pipeline.")
    parser.add_argument(
        "--only",
        choices=["generate", "checks", "report"],
        help="Run a single step instead of the full pipeline.",
    )
    parser.add_argument("--skip-generate", action="store_true", help="Skip data generation.")
    parser.add_argument("--skip-checks", action="store_true", help="Skip quality checks.")
    parser.add_argument("--skip-report", action="store_true", help="Skip report build.")
    return parser.parse_args()


def main():
    args = parse_args()
    steps = [
        ("generate", [sys.executable, str(BASE_DIR / "src" / "data_generator.py")]),
        ("checks", [sys.executable, str(BASE_DIR / "src" / "quality_checks.py")]),
        ("report", [sys.executable, str(BASE_DIR / "src" / "report_builder.py")]),
    ]

    for name, cmd in steps:
        if args.only and name != args.only:
            continue
        if args.skip_generate and name == "generate":
            continue
        if args.skip_checks and name == "checks":
            continue
        if args.skip_report and name == "report":
            continue
        run_step(" ".join(cmd))
        subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
