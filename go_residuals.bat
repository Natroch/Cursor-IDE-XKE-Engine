@echo off
setlocal
cd /d %~dp0

REM Launch the residual worker via PowerShell without policy prompts
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0go_residuals.ps1"

endlocal
