@echo off
setlocal
cd /d %~dp0

REM Ensure venv exists
if not exist ".venv\Scripts\python.exe" (
  py -3.11 -m venv .venv
)

REM Install dependencies quietly
".venv\Scripts\python.exe" -m pip install -q -r requirements.txt

REM Start app on port 5051
set PORT=5051
start "XKE App" ".venv\Scripts\python.exe" -m src.invoke

echo Launched XamKwe Echo on http://127.0.0.1:5051/
endlocal
