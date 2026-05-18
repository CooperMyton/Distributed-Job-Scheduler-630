# scripts/kill_worker.ps1
# Simulate a worker crash by force-killing its process.
#
# Usage:
#   .\scripts\kill_worker.ps1 -workerId worker-1     # kill by worker ID
#   .\scripts\kill_worker.ps1 -procId 12345             # kill by PID
#   .\scripts\kill_worker.ps1 -random                # kill a random worker
#
# This is the primary tool for Experiments 1, 2, and 4.
# After killing, watch the coordinator log for:
#   WARNING  Worker TIMEOUT detected: worker=worker-1
#   WARNING  Hot takeover queued: job=... checkpoint_step=...
#
# Run from your project root.

param(
    [string]$workerId = "",
    [int]$procId = 0,
    [switch]$random
)

function Get-WorkerProcesses {
    # Find all python processes running worker.worker
    Get-WmiObject Win32_Process |
        Where-Object { $_.Name -like "python*" -and $_.CommandLine -like "*worker.worker*" } |
        Select-Object ProcessId, CommandLine
}

# ── Kill by worker ID ──────────────────────────────────────────────────────────
if ($workerId -ne "") {
    $procs = Get-WmiObject Win32_Process |
        Where-Object { $_.Name -like "python*" -and $_.CommandLine -like "*$workerId*" }

    if ($procs -eq $null) {
        Write-Host "No process found for worker-id '$workerId'" -ForegroundColor Red
        Write-Host "Running worker processes:" -ForegroundColor Yellow
        Get-WorkerProcesses | ForEach-Object { Write-Host "  PID=$($_.ProcessId): $($_.CommandLine)" }
        exit 1
    }

    foreach ($proc in $procs) {
        Write-Host "Killing $workerId (PID=$($proc.ProcessId))..." -ForegroundColor Red
        Stop-Process -Id $proc.ProcessId -Force
        Write-Host "Done. Watch coordinator for failure detection (~10s)." -ForegroundColor Yellow
    }
    exit 0
}

# ── Kill by PID ────────────────────────────────────────────────────────────────
if ($procId -ne 0) {
    Write-Host "Killing PID $procId..." -ForegroundColor Red
    Stop-Process -Id $procId -Force
    Write-Host "Done. Watch coordinator for failure detection (~10s)." -ForegroundColor Yellow
    exit 0
}

# ── Kill random worker ─────────────────────────────────────────────────────────
if ($random) {
    $procs = Get-WorkerProcesses
    if ($procs -eq $null) {
        Write-Host "No worker processes found." -ForegroundColor Red
        exit 1
    }
    $target = $procs | Get-Random
    Write-Host "Randomly killing PID=$($target.ProcessId)..." -ForegroundColor Red
    Write-Host "Command: $($target.CommandLine)" -ForegroundColor Gray
    Stop-Process -Id $target.ProcessId -Force
    Write-Host "Done. Watch coordinator for failure detection (~10s)." -ForegroundColor Yellow
    exit 0
}

# ── No args: list running workers ─────────────────────────────────────────────
Write-Host "Running worker processes:" -ForegroundColor Cyan
$procs = Get-WorkerProcesses
if ($procs -eq $null) {
    Write-Host "  None found." -ForegroundColor Yellow
} else {
    $procs | ForEach-Object {
        Write-Host "  PID=$($_.ProcessId): $($_.CommandLine)" -ForegroundColor White
    }
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\scripts\kill_worker.ps1 -workerId worker-1"
    Write-Host "  .\scripts\kill_worker.ps1 -procId 12345"
    Write-Host "  .\scripts\kill_worker.ps1 -random"
}