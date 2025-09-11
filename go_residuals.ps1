param(
  [int]$PollSeconds = 10
)

Set-Location -Path $PSScriptRoot

if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  py -3.11 -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
pip install -q -r requirements.txt

$python = Join-Path $PWD ".venv\Scripts\python.exe"
Start-Process -FilePath $python -ArgumentList "-m","src.residual_worker" -RedirectStandardOutput .\residual.log -RedirectStandardError .\residual.err -WindowStyle Minimized
Write-Host "Residual worker launched (poll=$PollSeconds s). Logs: residual.log" -ForegroundColor Green
