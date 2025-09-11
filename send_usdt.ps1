param(
    [Parameter(Mandatory = $true)] [string]$Recipient,
    [Parameter(Mandatory = $true)] [decimal]$AmountUSDT,
    [string]$PrivateKeyPath = ".\pk.txt",
    [string]$RpcUrl = "https://eth.llamarpc.com",
    [string]$ChainId = "1",
    [string]$TokenAddress = "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    [int]$TokenDecimals = 6
)
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath (Split-Path -Parent $MyInvocation.MyCommand.Path)
if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) { py -m venv .venv }
. .\.venv\Scripts\Activate.ps1
try { python -c "import web3" 2>$null | Out-Null } catch { pip install -r requirements.txt }
if (-not (Test-Path $PrivateKeyPath)) { throw "Private key file not found at: $PrivateKeyPath" }
$privateKey = (Get-Content -Raw $PrivateKeyPath).Trim()
if ($privateKey -notmatch '^0x[0-9a-fA-F]{64}$') { throw "The private key loaded from $PrivateKeyPath does not look valid (expected 0x + 64 hex)." }
$env:RPC_URL = $RpcUrl
$env:CHAIN_ID = $ChainId
$env:USDT_ADDRESS = $TokenAddress
$env:USDT_DECIMALS = "$TokenDecimals"
$env:PRIVATE_KEY = $privateKey
$env:RECIPIENT = $Recipient
$env:AMOUNT_USDT = ([string]([Math]::Round($AmountUSDT, 2)))
$env:CONTRACT_ADDRESS = $null
$env:CONTRACT_ABI_PATH = $null
$env:CONTRACT_ABI_JSON = $null
python .\send_direct_erc20.py
if ($LASTEXITCODE -ne 0) { throw "send_direct_erc20.py exited with code $LASTEXITCODE" }