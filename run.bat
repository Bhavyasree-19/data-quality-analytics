@echo off
setlocal
cd /d %~dp0

if not exist .venv\Scripts\python.exe (
  python -m venv .venv
)

.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python src\main.py

echo.
echo Report generated: reports\quality_report.html
endlocal
