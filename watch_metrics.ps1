param(
  [int]$Port = 5050,
  [int]$IntervalSeconds = 5
)

while ($true) {
  try {
    $m = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/metrics.json" -TimeoutSec 5
    "{0} | USDT: {1} | Residual: {2} | Pool: {3}" -f (Get-Date -Format "HH:mm:ss"), $m.usdt_balance, $m.total_residual, $m.pool_total
  } catch { "unreachable" }
  Start-Sleep -Seconds $IntervalSeconds
}
