param(
  [int]$IntervalSeconds = 300,  # 5 minutes
  [string]$RpcUrl = "https://eth.llamarpc.com",
  [string]$TokenAddress = "0xdAC17F958D2ee523a2206206994597C13D831ec7", # USDT ERC20
  [string]$LunoRecipient = "0x86d76f4453DFB4742ebBD8F88019C2c7191eB743", # Luno USDT ERC20
  [string]$GasRecipient = ""
)

Set-Location -Path $PSScriptRoot

if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  py -3.11 -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
pip install -q -r requirements.txt

# Load PRIVATE_KEY from pk.txt
if (-not (Test-Path .\pk.txt)) {
  Write-Host "pk.txt not found. Create pk.txt with your 0x... private key" -ForegroundColor Red
  exit 1
}
$pk = Get-Content -Raw .\pk.txt

if (-not $GasRecipient -or $GasRecipient -eq "") {
  $GasRecipient = Read-Host "Enter your MetaMask (USDT) wallet address for gas reserve (0x...)"
}

$env:RPC_URL = $RpcUrl
$env:PRIVATE_KEY = $pk.Trim()
$env:TOKEN_ADDRESS = $TokenAddress
$env:USDT_DECIMALS = "6"

# Threshold and payout
$env:THRESHOLD_USDT = "5000"
$env:PAYOUT_USDT = "1000"
$env:RECIPIENT = $LunoRecipient

# Gas reserve settings
$env:GAS_USDT_PER_TRIGGER = "100"
$env:GAS_RECIPIENT = $GasRecipient

$python = Join-Path $PWD ".venv\Scripts\python.exe"

Write-Host "Threshold watcher started. Checks every $IntervalSeconds seconds." -ForegroundColor Green
while ($true) {
  try {
    & $python .\threshold_payout.py
  } catch {
    Write-Host "Run failed: $_" -ForegroundColor Yellow
  }
  Start-Sleep -Seconds $IntervalSeconds
}

