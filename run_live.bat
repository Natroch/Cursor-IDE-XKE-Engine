.env
@echo off
cd /d C:\Users\Administrator\Downloads\xamkwe-echo-engine-main\xamkwe-echo-engine-main
call .\.venv\Scripts\activate.bat
python --version
python approve.py
python send_invoke.py
echo.
echo ===== Finished. Press any key =====
pause