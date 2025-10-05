@echo off
setlocal
cd /d %~dp0

REM Ensure venv exists
if not exist ".venv\Scripts\python.exe" (
  py -3.11 -m venv .venv
)

REM Install dependencies quietly
".venv\Scripts\python.exe" -m pip install -q -r requirements.txt

REM ---------------- Auto-detect PORT ----------------
REM 1) Honor existing environment PORT if provided
if defined PORT goto have_port

REM 2) Try to read last used port from file
if exist "current_port.txt" (
  for /f "usebackq delims=" %%P in ("current_port.txt") do set PORT=%%P
)

REM 3) Default to 5051 if still undefined
if not defined PORT set PORT=5051

:have_port
REM Persist selected port for other scripts/tools to consume
>"current_port.txt" echo %PORT%
REM --------------------------------------------------

start "XKE App" ".venv\Scripts\python.exe" -m src.invoke

echo Launched XamKwe Echo on http://127.0.0.1:%PORT%/
endlocal
