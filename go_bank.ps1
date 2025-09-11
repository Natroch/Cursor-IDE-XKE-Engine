param(
  [string]$Name = $env:BANK_BENEFICIARY_NAME,
  [string]$Account = $env:BANK_ACCOUNT_NUMBER,
  [string]$Branch = $env:BANK_BRANCH_CODE,
  [string]$Amount = $env:BANK_AMOUNT,
  [string]$Reference = $env:BANK_REFERENCE
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

cd $PSScriptRoot
if (!(Test-Path .venv)) { python -m venv .venv }
.\.venv\Scripts\Activate.ps1
pip install --disable-pip-version-check -r requirements.txt | Out-Host

if ($Name) { $env:BANK_BENEFICIARY_NAME = $Name }
if ($Account) { $env:BANK_ACCOUNT_NUMBER = $Account }
if ($Branch) { $env:BANK_BRANCH_CODE = $Branch }
if ($Amount) { $env:BANK_AMOUNT = $Amount }
if ($Reference) { $env:BANK_REFERENCE = $Reference }

if (-not $env:BANK_API_URL -or -not $env:BANK_API_KEY) {
  Write-Host "Set BANK_API_URL and BANK_API_KEY in your .env or environment." -ForegroundColor Yellow
}

python .\run_bank.py | Tee-Object -Variable output | Out-Host
if ($LASTEXITCODE -eq 0) {
  Write-Host "Bank payout call completed" -ForegroundColor Green
} else {
  Write-Host "Bank payout failed" -ForegroundColor Red
}

