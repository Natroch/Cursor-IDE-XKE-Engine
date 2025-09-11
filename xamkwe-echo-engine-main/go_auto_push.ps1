# Auto-push changes to GitHub on an interval
# Usage: Run this script from the repo root. It will loop and push any changes it finds.

param(
    [int]$IntervalSeconds = 30,
    [string]$CommitPrefix = "chore:auto-sync"
)

$ErrorActionPreference = "Stop"

function Ensure-Git() {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-Host "git not found in PATH. Please install Git and re-run." -ForegroundColor Yellow
        exit 1
    }
}

function Ensure-Repo() {
    if (-not (Test-Path .git)) {
        git init -b main | Out-Null
    }
}

Ensure-Git
Ensure-Repo

Write-Host "[auto-push] Started. Interval: $IntervalSeconds s" -ForegroundColor Cyan

while ($true) {
    try {
        $changed = git status --porcelain
        if ($changed) {
            git add -A | Out-Null
            $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
            git commit -m "$CommitPrefix $ts" | Out-Null
            git push | Out-Null
            Write-Host "[auto-push] Pushed at $ts" -ForegroundColor Green
        }
    } catch {
        Write-Host "[auto-push] Error: $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds $IntervalSeconds
}

