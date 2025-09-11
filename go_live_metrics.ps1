param(
  [int]$Port = 5050
)

Set-Location -Path $PSScriptRoot

if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  py -3.11 -m venv .venv
}

. .\.venv\Scripts\Activate.ps1

# Ensure dependencies
pip install -q -r requirements.txt

$env:PORT = "$Port"

# Start app in background with logs
$python = Join-Path $PWD ".venv\Scripts\python.exe"
Start-Process -FilePath $python -ArgumentList "-m","src.invoke" -RedirectStandardOutput .\app.log -RedirectStandardError .\app.err -WindowStyle Minimized

Start-Sleep -Seconds 2

# Open health and metrics
Start-Process "http://127.0.0.1:$Port/health"
Start-Process "http://127.0.0.1:$Port/metrics.json"

Write-Host "App started on http://127.0.0.1:$Port" -ForegroundColor Green
