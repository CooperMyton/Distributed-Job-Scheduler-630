# scripts/start_workers.ps1
# Launch N worker processes, each in its own terminal window.
#
# Usage:
#   .\scripts\start_workers.ps1           # starts 3 workers (default)
#   .\scripts\start_workers.ps1 -n 5      # starts 5 workers
#   .\scripts\start_workers.ps1 -n 2 -coordinator http://127.0.0.1:8000
#
# Each worker gets a stable ID (worker-1, worker-2, ...) so you can
# identify them in the coordinator logs and kill specific ones.
#
# Run from your project root with venv active.

param(
    [int]$n = 3,
    [string]$coordinator = "http://127.0.0.1:8000"
)

$projectRoot = Get-Location

Write-Host "Starting $n workers..." -ForegroundColor Cyan
Write-Host "Coordinator: $coordinator" -ForegroundColor Cyan
Write-Host ""

for ($i = 1; $i -le $n; $i++) {
    $workerId = "worker-$i"
    $command = "cd '$projectRoot'; venv\Scripts\activate; python -m worker.worker --worker-id $workerId --coordinator $coordinator; Read-Host 'Press Enter to close'"

    Start-Process powershell -ArgumentList "-NoExit", "-Command", $command
    Write-Host "  Started $workerId" -ForegroundColor Green
    Start-Sleep -Milliseconds 300   # stagger startup slightly
}

Write-Host ""
Write-Host "All $n workers started." -ForegroundColor Cyan
Write-Host "Watch the coordinator terminal for registration messages." -ForegroundColor Yellow
Write-Host "To kill a specific worker, run: .\scripts\kill_worker.ps1 -workerId worker-1" -ForegroundColor Yellow