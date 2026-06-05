# Hot Takeover Manual

This manual explains how to install, run, test, package, and demonstrate the Hot Takeover distributed job scheduler.

## 1. Install

Python 3.10 is recommended.

```powershell
py -3.10 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

If the virtual environment is broken or points to a missing Python install:

```powershell
Remove-Item -Recurse -Force venv
py -3.10 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Start Coordinator

Terminal 1:

```powershell
python -m coordinator.main
```

Expected output:

```text
Hot Takeover Coordinator starting up
Monitor thread started
Uvicorn running on http://127.0.0.1:8000
```

API docs:

```text
http://127.0.0.1:8000/docs
```

## 3. Start Workers

Terminal 2:

```powershell
.\scripts\start_workers.ps1 -n 2
```

Expected behavior:

- Two worker windows start.
- Coordinator logs worker registration.
- Workers begin polling for jobs.

## 4. Submit a Job

Terminal 3:

```powershell
python scripts\submit_jobs.py --count 1 --task CountingTask --target 300 --step-sleep 0.1 --wait
```

Expected behavior:

- Job is submitted.
- One worker accepts the job.
- Worker periodically pushes checkpoints.
- Job eventually completes.

## 5. Demonstrate Takeover

While the job is running:

```powershell
.\scripts\kill_worker.ps1 -random
```

Wait about 10-12 seconds.

Look for these coordinator log messages:

```text
Worker TIMEOUT detected
Hot takeover queued
Job assigned ... takeover=True
Job completed
```

The replacement worker should receive the job with the latest checkpoint and complete it.

## 6. Run Automated Experiments

Clear metrics before a fresh run:

```powershell
Remove-Item -Force metrics.jsonl -ErrorAction SilentlyContinue
```

Run failure detection experiment:

```powershell
python scripts\run_experiment.py --experiment 1 --trials 5
python -m metrics.analyze --experiment 1
```

Run checkpoint overhead experiment:

```powershell
python scripts\run_experiment.py --experiment 3
python -m metrics.analyze --experiment 3
```

Run state fidelity experiment:

```powershell
python scripts\run_experiment.py --experiment 5 --trials 2
python -m metrics.analyze --experiment 5
```


## 7. Troubleshooting

If PowerShell blocks scripts:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

If metrics look wrong, clear old metrics:

```powershell
Remove-Item -Force metrics.jsonl -ErrorAction SilentlyContinue
```

If port 8000 is busy, stop the old coordinator process.
