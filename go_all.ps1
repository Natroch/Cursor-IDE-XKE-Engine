param(
  [int]$Port = 5051,
  [int]$ResidualPoll = 10
)

Set-Location -Path $PSScriptRoot

if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  py -3.11 -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
pip install -q -r requirements.txt

# Safe defaults (you can still override in .env)
if (-not $env:BINANCE_ENABLE) { $env:BINANCE_ENABLE = "true" }
if (-not $env:BINANCE_SYMBOLS) { $env:BINANCE_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT" }

$env:PORT = "$Port"

# Start app (background)
$python = Join-Path $PWD ".venv\Scripts\python.exe"
Start-Process -FilePath $python -ArgumentList "-m","src.invoke" -RedirectStandardOutput .\app.log -RedirectStandardError .\app.err -WindowStyle Minimized
Start-Sleep -Seconds 3

# Start residual worker (background)
Start-Process -FilePath $python -ArgumentList "-m","src.residual_worker" -RedirectStandardOutput .\residual.log -RedirectStandardError .\residual.err -WindowStyle Minimized

# Start gas bonus watcher if configured
if ($env:RPC_URL -and $env:PRIVATE_KEY -and ($env:GAS_RECIPIENT)) {
  Start-Process -FilePath $python -ArgumentList ".\gas_bonus_watcher.py" -WindowStyle Minimized
}

# Open browser to metrics
Start-Process ("http://127.0.0.1:{0}/metrics.json" -f $Port)
Write-Host "Launched: app (port $Port), residual worker, and gas watcher (if configured)." -ForegroundColor Green
run with powershell

