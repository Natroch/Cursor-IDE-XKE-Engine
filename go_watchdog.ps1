param(
  [int]$Port = 5051,
  [int]$IntervalSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info([string]$msg) { Write-Host $msg -ForegroundColor Cyan }
function Write-Warn([string]$msg) { Write-Host $msg -ForegroundColor Yellow }
function Write-Ok([string]$msg)   { Write-Host $msg -ForegroundColor Green }
function Write-Err([string]$msg)  { Write-Host $msg -ForegroundColor Red }

try { Set-Location -Path $PSScriptRoot } catch {}

Write-Info ("Watchdog started. Port={0} Interval={1}s" -f $Port, $IntervalSeconds)

while ($true) {
  $ok = $false
  try {
    $url = "http://127.0.0.1:$Port/api/v1/health"
    $r = Invoke-RestMethod -Method Get -Uri $url -TimeoutSec 5
    if ($r.ok -eq $true) { $ok = $true }
  } catch { $ok = $false }

  if (-not $ok) {
    Write-Warn "Health failed. Restarting engine..."
    try {
      Start-Process powershell -ArgumentList '-ExecutionPolicy Bypass -File "./go_oneclick.ps1" -SkipInstall' -WindowStyle Hidden
    } catch {
      Write-Err $_.Exception.Message
    }
  }

  Start-Sleep -Seconds $IntervalSeconds
}

