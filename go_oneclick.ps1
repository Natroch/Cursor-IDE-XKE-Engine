param(
  [int]$Port = 5051,
  [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info([string]$msg) { Write-Host $msg -ForegroundColor Cyan }
function Write-Warn([string]$msg) { Write-Host $msg -ForegroundColor Yellow }
function Write-Ok([string]$msg)   { Write-Host $msg -ForegroundColor Green }
function Write-Err([string]$msg)  { Write-Host $msg -ForegroundColor Red }

try {
  Set-Location -Path $PSScriptRoot
} catch {}

# 1) Ensure Python venv
if (-not (Test-Path .\.venv\Scripts\python.exe)) {
  Write-Info "Creating virtual environment (.venv)"
  try {
    py -3.11 -m venv .venv
  } catch {
    py -3 -m venv .venv
  }
}

$python = Join-Path $PWD ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
  throw "Python virtual environment missing at $python"
}

# 2) Install requirements (optional)
if (-not $SkipInstall) {
  Write-Info "Installing requirements (this may take a minute)..."
  try {
    & $python -m pip install -q -r requirements.txt
  } catch {
    Write-Warn "pip install reported an issue. Continuing because the app can run without some optional packages."
  }
}

# 3) Load .env if present
if (Test-Path .\.env) {
  Write-Info "Loading .env"
  Get-Content .\.env | ForEach-Object {
    $line = $_.Trim()
    if (-not $line) { return }
    if ($line.StartsWith('#')) { return }
    $kv = $line -split '=', 2
    if ($kv.Length -eq 2) {
      $k = $kv[0].Trim()
      $v = $kv[1].Trim().Trim('"')
      if ($k) { [Environment]::SetEnvironmentVariable($k, $v) }
    }
  }
}

# 4) Defaults and inferred values
if (-not $env:RPC_URL) { $env:RPC_URL = "https://eth.llamarpc.com" }
if (-not $env:USDT_ADDRESS) { $env:USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7" }
if (-not $env:USDT_DECIMALS) { $env:USDT_DECIMALS = "6" }
if (-not $env:PORT) { $env:PORT = "$Port" } else { $env:PORT = "$($env:PORT)" }

# CONTRACT_ADDRESS from file if not set
if (-not $env:CONTRACT_ADDRESS) {
  if (Test-Path .\deployed_address.txt) {
    $addr = (Get-Content .\deployed_address.txt | Select-Object -First 1).Trim()
    if ($addr) { $env:CONTRACT_ADDRESS = $addr }
  }
}

# HMAC secret (dev default if missing)
if (-not $env:API_HMAC_SECRET) {
  $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
  $bytes = New-Object byte[] 32
  $rng.GetBytes($bytes)
  $env:API_HMAC_SECRET = ("XKE_DEV_" + ([System.BitConverter]::ToString($bytes).Replace('-', '').ToLower()))
}

Write-Info "Config"
Write-Host ("  RPC_URL           : {0}" -f $env:RPC_URL)
Write-Host ("  CONTRACT_ADDRESS  : {0}" -f ($env:CONTRACT_ADDRESS ?? "(not set)"))
Write-Host ("  USDT_ADDRESS      : {0}" -f $env:USDT_ADDRESS)
Write-Host ("  USDT_DECIMALS     : {0}" -f $env:USDT_DECIMALS)
Write-Host ("  PORT              : {0}" -f $env:PORT)

# 5) Start backend if not alive
$base = "http://127.0.0.1:$($env:PORT)"
$alive = $false
try {
  $h = Invoke-RestMethod -Method Get -Uri ("{0}/api/v1/health" -f $base) -TimeoutSec 5
  $alive = $true
} catch { $alive = $false }

if (-not $alive) {
  Write-Info "Starting backend on port $($env:PORT)"
  Start-Process -FilePath $python -ArgumentList "-u","-m","src.invoke" -NoNewWindow
  Start-Sleep -Seconds 3
}

# Wait up to ~20s for health
for ($i=0; $i -lt 20; $i++) {
  try {
    $h = Invoke-RestMethod -Method Get -Uri ("{0}/api/v1/health" -f $base) -TimeoutSec 5
    if ($h.ok) { break }
  } catch {}
  Start-Sleep -Milliseconds 750
}

try {
  $h = Invoke-RestMethod -Method Get -Uri ("{0}/api/v1/health" -f $base)
  Write-Ok   ("Health: ok={0} chain_id={1} gas_gwei={2}" -f $h.ok, $h.chain_id, $h.gas_gwei)
  Write-Host ("Breaker: {0}  DB: {1}  RPC: {2}" -f $h.breaker, $h.db, $h.rpc)
} catch {
  Write-Err "Backend did not report healthy. Check logs (app.err)."
}

# 6) Show USDT balance at the recovery contract
try {
  $b = Invoke-RestMethod -Method Get -Uri ("{0}/api/v1/balance" -f $base)
  Write-Info "Recovery USDT balance"
  Write-Host ("  token    : {0}" -f $b.token)
  if ($b.decimals -ne $null) { Write-Host ("  decimals : {0}" -f $b.decimals) }
  Write-Host ("  raw      : {0}" -f $b.raw)
  if ($b.human -ne $null) { Write-Host ("  human    : {0}" -f $b.human) }
} catch {
  Write-Warn "Could not read balance. Ensure CONTRACT_ADDRESS and RPC_URL are correct."
}

Write-Ok ("XamKwe Echo API ready at {0}" -f $base)
Write-Info "Next: send USDT to CONTRACT_ADDRESS, then re-run this script to see balance."

