param(
  [string]$Recipient = $env:RECIPIENT,
  [string]$AmountUSDT = $env:AMOUNT_USDT
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

cd $PSScriptRoot
if (!(Test-Path .venv)) { python -m venv .venv }
.\.venv\Scripts\Activate.ps1

pip install --disable-pip-version-check -r requirements.txt | Out-Host

if (-not (Test-Path .\pk.txt) -and -not $env:PRIVATE_KEY) {
  $sec = Read-Host "Paste private key (0x...64 hex)" -AsSecureString
  $b = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
  $env:PRIVATE_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto($b)
}
if (Test-Path .\pk.txt) { $env:PRIVATE_KEY = (Get-Content -Raw .\pk.txt).Trim() }

if (-not $env:RPC_URL) { $env:RPC_URL = "https://opbnb-mainnet-rpc.bnbchain.org" }
if (-not $env:CHAIN_ID) { $env:CHAIN_ID = "204" }
if (-not $env:CONTRACT_ABI_PATH -and -not $env:CONTRACT_ABI_JSON) { $env:CONTRACT_ABI_PATH = "contract-abi.json" }
if (-not $env:USDT_DECIMALS) { $env:USDT_DECIMALS = "18" }

if ($Recipient) { $env:RECIPIENT = $Recipient }
if ($AmountUSDT) { $env:AMOUNT_USDT = $AmountUSDT }

python .\xke_payout_v2.py | Tee-Object -Variable output | Out-Host
if ($LASTEXITCODE -eq 0 -and $output -match "confirmed: True") {
  Write-Host "Payout confirmed" -ForegroundColor Green
  exit 0
} else {
  Write-Host "Payout failed or unconfirmed" -ForegroundColor Red
  exit 1
}

