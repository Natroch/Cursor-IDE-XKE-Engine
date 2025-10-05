param(
  [int]$Port = 0,
  [int]$IntervalSeconds = 5
)

# Auto-detect port: 1) environment PORT, 2) current_port.txt, 3) default 5051
if ($Port -eq 0) {
  if ($env:PORT) { $Port = [int]$env:PORT }
  elseif (Test-Path -LiteralPath "current_port.txt") {
    try { $Port = [int](Get-Content -LiteralPath "current_port.txt" -TotalCount 1) } catch { $Port = 0 }
  }
  if ($Port -eq 0) { $Port = 5051 }
}

while ($true) {
  try {
    $m = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/metrics.json" -TimeoutSec 5
    "{0} | USDT: {1} | Residual: {2} | Pool: {3}" -f (Get-Date -Format "HH:mm:ss"), $m.usdt_balance, $m.total_residual, $m.pool_total
  } catch { "unreachable:$Port" }
  Start-Sleep -Seconds $IntervalSeconds
}
