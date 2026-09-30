import os
import sys
import time
import subprocess
import urllib.request
import json
from pathlib import Path

# Static cluster layout
CLUSTER_ROOT = Path(r"C:\ProgramData\GMapEliteCluster")
REPO_DIR = CLUSTER_ROOT / "app" / "scraper-core"
CONFIG_DIR = CLUSTER_ROOT / "config"
ENV_PATH = CONFIG_DIR / ".env"
LOG_DIR = CLUSTER_ROOT / "logs"

# Ensure runtime paths resolve to the private deployed runtime/venv
PYTHONW_EXE = CLUSTER_ROOT / "venv" / "Scripts" / "pythonw.exe"
if not PYTHONW_EXE.exists():
    PYTHONW_EXE = CLUSTER_ROOT / "runtime" / "pythonw.exe"
PYTHON_CLI = Path(str(PYTHONW_EXE).replace("pythonw.exe", "python.exe"))

# Add repository root to path
sys.path.insert(0, str(REPO_DIR))
from bootstrap.config_resolver import fetch_and_apply_dead_drop


def load_env_map() -> dict:
    """Safely extracts key-values from the central config directory."""
    data = {}
    if ENV_PATH.exists():
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    data[k] = v
    return data


def check_for_updates(env_map: dict) -> tuple[bool, bool]:
    """
    Returns (master_reachable, update_available).
    If unreachable, triggers the dead-drop recovery mechanism.
    """
    master_url = env_map.get("MASTER_URL", "").rstrip("/")
    worker_id = env_map.get("WORKER_ID", "")
    auth_token = env_map.get("AUTH_TOKEN", "")
    local_version = env_map.get("LOCAL_VERSION", "1.0.0")

    if not master_url:
        return False, False

    url = f"{master_url}/api/v1/heartbeat"
    payload = json.dumps({
        "worker_id": worker_id,
        "current_version": local_version
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return True, (data.get("command") == "UPDATE")
    except Exception:
        # Master connection failed; attempt out-of-band dead-drop recovery
        dead_drop_url = env_map.get("DEAD_DROP_URL")
        decryption_key = env_map.get("DECRYPTION_KEY")
        if dead_drop_url and decryption_key:
            fetch_and_apply_dead_drop(dead_drop_url, decryption_key)
        return False, False


def apply_git_update():
    """Pulls upstream code changes from Git repository into app/scraper-core."""
    try:
        subprocess.run(["git", "fetch", "--all"], cwd=str(REPO_DIR), check=True, capture_output=True)
        subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=str(REPO_DIR), check=True, capture_output=True)
        subprocess.run(
            [str(PYTHON_CLI), "-m", "pip", "install", "-r", "requirements_worker.txt"],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True
        )
    except Exception:
        time.sleep(10)


def main():
    worker_script = str(REPO_DIR / "worker" / "main_worker.py")
    proc = None

    while True:
        env_map = load_env_map()
        master_reachable, update_needed = check_for_updates(env_map)

        if update_needed:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                proc = None
            apply_git_update()

        # Supervise the headless Worker process
        if proc is None or proc.poll() is not None:
            proc = subprocess.Popen(
                [str(PYTHONW_EXE), worker_script],
                cwd=str(REPO_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

        # Check update/heartbeat interval every 30 seconds
        time.sleep(30)


if __name__ == "__main__":
    main()