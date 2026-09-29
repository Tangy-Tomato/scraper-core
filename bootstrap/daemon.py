import os
import sys
import time
import subprocess
import urllib.request
import json
from pathlib import Path

# Load settings directly from .env
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from worker.config import WORKER_CONFIG

def check_for_updates() -> bool:
    """Queries Master API over Tailscale mesh to check version synchronization."""
    url = f"{WORKER_CONFIG.MASTER_URL}/api/v1/heartbeat"
    payload = json.dumps({
        "worker_id": WORKER_CONFIG.WORKER_ID,
        "current_version": WORKER_CONFIG.LOCAL_VERSION
    }).encode("utf-8")
    
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {WORKER_CONFIG.AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("command") == "UPDATE"
    except Exception:
        return False

def apply_git_update():
    """Pulls upstream code changes and upgrades required packages."""
    try:
        subprocess.run(["git", "fetch", "--all"], cwd=BASE_DIR, check=True)
        subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=BASE_DIR, check=True)
        subprocess.run(
            [sys.executable.replace("pythonw.exe", "python.exe"), "-m", "pip", "install", "-r", "requirements_worker.txt"],
            cwd=BASE_DIR,
            check=True
        )
    except Exception as e:
        time.sleep(10)

def main():
    pythonw = sys.executable
    worker_script = str(BASE_DIR / "worker" / "main_worker.py")

    while True:
        # Check if code requires synchronization before launching worker
        if check_for_updates():
            apply_git_update()

        # Spawn Worker Node via pythonw (completely invisible, zero taskbar presence)
        proc = subprocess.Popen(
            [pythonw, worker_script],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Supervise the process
        while proc.poll() is None:
            time.sleep(30)
            if check_for_updates():
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                apply_git_update()
                break

        time.sleep(5)

if __name__ == "__main__":
    main()