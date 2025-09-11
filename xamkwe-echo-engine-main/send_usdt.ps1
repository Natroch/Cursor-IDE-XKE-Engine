param(
    [Parameter(Mandatory = $true)] [string]$Recipient,
    [Parameter(Mandatory = $true)] [decimal]$AmountUSDT,
    [string]$PrivateKeyPath = ".\pk.txt",
    [string]$RpcUrl = "https://eth.llamarpc.com",
    [string]$ChainId = "1",
    [string]$TokenAddress = "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    [int]$TokenDecimals = 6
)

# Stop on errors
$ErrorActionPreference = "Stop"

Write-Host "→ Starting automated USDT send..." -ForegroundColor Cyan

# Ensure we are in the script directory
Set-Location -LiteralPath (Split-Path -Parent $MyInvocation.MyCommand.Path)

# Create and/or activate virtual environment
if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) {
    Write-Host "→ Creating Python virtual environment (.venv)..." -ForegroundColor DarkCyan
    py -m venv .venv
}

Write-Host "→ Activating virtual environment..." -ForegroundColor DarkCyan
. .\.venv\Scripts\Activate.ps1

# Install requirements if web3 is missing
try {
    python -c "import web3" 2>$null | Out-Null
} catch {
    Write-Host "→ Installing Python dependencies (requirements.txt)..." -ForegroundColor DarkCyan
    pip install -r requirements.txt
}

# Load private key
if (-not (Test-Path $PrivateKeyPath)) {
    throw "Private key file not found at: $PrivateKeyPath"
}

$privateKey = (Get-Content -Raw $PrivateKeyPath).Trim()
if ($privateKey -notmatch '^0x[0-9a-fA-F]{64}$') {
    throw "The private key loaded from $PrivateKeyPath does not look valid (expected 0x + 64 hex)."
}

# Export environment variables for the Python script
$env:RPC_URL = $RpcUrl
$env:CHAIN_ID = $ChainId
$env:USDT_ADDRESS = $TokenAddress
$env:USDT_DECIMALS = "$TokenDecimals"
$env:PRIVATE_KEY = $privateKey
$env:RECIPIENT = $Recipient
$env:AMOUNT_USDT = ([string]([Math]::Round($AmountUSDT, 2)))

# Ensure no contract-based call interferes with direct ERC-20 transfer
$env:CONTRACT_ADDRESS = $null
$env:CONTRACT_ABI_PATH = $null
$env:CONTRACT_ABI_JSON = $null

Write-Host "→ Sending $($env:AMOUNT_USDT) USDT to $Recipient via direct ERC-20 transfer..." -ForegroundColor Cyan

python .\send_direct_erc20.py

if ($LASTEXITCODE -ne 0) {
    throw "send_direct_erc20.py exited with code $LASTEXITCODE"
}

Write-Host "✔ Done. If a transaction hash was printed above, open it on Etherscan to verify an 'ERC-20 Token Transfer'." -ForegroundColor Green

