@echo off
setlocal
cd /d %~dp0

REM Launch the threshold watcher via PowerShell without prompts about policy
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0go_threshold.ps1"

endlocal
