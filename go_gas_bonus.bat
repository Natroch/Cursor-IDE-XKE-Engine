@echo off
setlocal
cd /d %~dp0

REM Run gas bonus watcher (sends $50 USDT at each $1,000 level and notifies)
powershell -NoProfile -ExecutionPolicy Bypass -Command 
  ". .\.venv\Scripts\Activate.ps1; pip install -q -r requirements.txt; $env:RPC_URL=$env:RPC_URL; $env:PRIVATE_KEY=(Get-Content -Raw .\pk.txt).Trim(); $env:TOKEN_ADDRESS=$env:TOKEN_ADDRESS; $env:USDT_DECIMALS='6'; $env:GAS_LEVEL_STEP='1000'; $env:GAS_BONUS_USDT='50'; $env:GAS_RECIPIENT=$env:GAS_RECIPIENT; $env:WEBHOOK_URL=$env:WEBHOOK_URL; python .\gas_bonus_watcher.py" 

endlocal
