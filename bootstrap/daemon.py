import json
import os
from pathlib import Path
from socket import gethostname
import subprocess
import sys
import time
import urllib.request

CLUSTER_ROOT = Path(r"C:\ProgramData\GMapEliteCluster")
REPO_DIR = CLUSTER_ROOT / "app" / "scraper-core"
CONFIG_DIR = CLUSTER_ROOT / "config"
ENV_PATH = CONFIG_DIR / ".env"
LOG_DIR = CLUSTER_ROOT / "logs"
WORKER_LOG = LOG_DIR / "worker.log"
DAEMON_LOG = LOG_DIR / "daemon.log"

PYTHONW_EXE = CLUSTER_ROOT / "venv" / "Scripts" / "pythonw.exe"
if not PYTHONW_EXE.exists():
    PYTHONW_EXE = CLUSTER_ROOT / "runtime" / "Python" / "pythonw.exe"
PYTHON_CLI = Path(str(PYTHONW_EXE).replace("pythonw.exe", "python.exe"))

sys.path.insert(0, str(REPO_DIR))
from bootstrap.config_resolver import fetch_and_apply_dead_drop


def log_daemon(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] [DAEMON] {message}\n"
    try:
        with open(DAEMON_LOG, "a", encoding="utf-8") as f:
            f.write(formatted)
    except Exception:
        pass


def rotate_log_if_needed(log_path: Path, max_bytes: int = 10 * 1024 * 1024) -> None:
    """Rotates the log file if it exceeds the specified maximum size."""
    try:
        if log_path.exists() and log_path.stat().st_size >= max_bytes:
            backup_path = log_path.with_suffix(".log.1")
            if backup_path.exists():
                backup_path.unlink()
            log_path.rename(backup_path)
            log_daemon(f"Rotated {log_path.name} to {backup_path.name}")
    except Exception as exc:
        log_daemon(f"Failed to rotate log {log_path.name}: {exc}")


def load_env_map() -> dict:
    data = {}
    if ENV_PATH.exists():
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    data[k] = v
    return data


def check_for_updates(env_map: dict) -> tuple[bool, bool, bool]:
    """Returns (master_reachable, update_needed, config_changed)."""
    master_url = env_map.get("MASTER_URL", "").rstrip("/")
    worker_id = env_map.get("WORKER_ID", "")
    auth_token = env_map.get("AUTH_TOKEN", "")
    local_version = env_map.get("LOCAL_VERSION", "1.0.0")

    config_changed = False
    if not master_url:
        return False, False, False

    # Contract adherence: Matches shared.contracts.HeartbeatRequest strictly
    payload = json.dumps({
        "worker_id": worker_id,
        "version": local_version,
        "hostname": gethostname(),
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{master_url}/api/v1/heartbeat",
        data=payload,
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return True, (data.get("command") == "UPDATE"), False
    except Exception as exc:
        log_daemon(f"Master heartbeat failed ({exc}); querying out-of-band dead-drop...")
        dead_drop_url = env_map.get("CONFIG_RAW_URL") or env_map.get("DEAD_DROP_URL")
        decryption_key = env_map.get("DECRYPTION_KEY")
        if dead_drop_url and decryption_key:
            config_changed = fetch_and_apply_dead_drop(dead_drop_url, decryption_key)
            if config_changed:
                log_daemon("Dead-drop payload decrypted and applied successfully.")
        return False, False, config_changed


def apply_git_update() -> None:
    log_daemon("Initiating cluster code update via Git...")
    try:
        subprocess.run(["git", "fetch", "--all"], cwd=str(REPO_DIR), check=True, capture_output=True)
        subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=str(REPO_DIR), check=True, capture_output=True)
        subprocess.run(
            [str(PYTHON_CLI), "-m", "pip", "install", "-r", "requirements_worker.txt", "--quiet"],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True,
        )
        log_daemon("Code update and dependency sync completed successfully.")
    except Exception as exc:
        log_daemon(f"Git code update failed: {exc}")
        time.sleep(10)


def main():
    worker_script = str(REPO_DIR / "worker" / "main_worker.py")
    proc = None
    log_file_handle = None

    log_daemon("GMapElite Worker Daemon supervisor initialized.")

    while True:
        env_map = load_env_map()
        master_reachable, update_needed, config_changed = check_for_updates(env_map)

        # Terminate active worker if configuration changed or update is flagged
        if config_changed or update_needed:
            if proc and proc.poll() is None:
                log_daemon("Terminating active worker process for restart...")
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                proc = None

            if update_needed:
                apply_git_update()

        # Supervise and respawn child worker process
        if proc is None or proc.poll() is not None:
            if proc is not None:
                log_daemon(f"Worker process exited with code {proc.poll()}. Restarting...")

            if log_file_handle:
                try:
                    log_file_handle.close()
                except Exception:
                    pass

            rotate_log_if_needed(WORKER_LOG)
            log_file_handle = open(WORKER_LOG, "a", encoding="utf-8")

            proc = subprocess.Popen(
                [str(PYTHONW_EXE), worker_script],
                cwd=str(REPO_DIR),
                stdout=log_file_handle,
                stderr=subprocess.STDOUT,
            )
            log_daemon(f"Worker node launched headlessly (PID: {proc.pid}).")

        time.sleep(30)


if __name__ == "__main__":
    main()