from __future__ import annotations

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
SECRET_KEY_PATH = CONFIG_DIR / "secret.key"

LOG_DIR = CLUSTER_ROOT / "logs"
WORKER_LOG = LOG_DIR / "worker.log"
DAEMON_LOG = LOG_DIR / "daemon.log"

PYTHONW_EXE = CLUSTER_ROOT / "venv" / "Scripts" / "pythonw.exe"
if not PYTHONW_EXE.exists():
    PYTHONW_EXE = CLUSTER_ROOT / "runtime" / "Python" / "pythonw.exe"

PYTHON_CLI = Path(str(PYTHONW_EXE).replace("pythonw.exe", "python.exe"))

GIT_EXE = CLUSTER_ROOT / "runtime" / "Git" / "cmd" / "git.exe"

sys.path.insert(0, str(REPO_DIR))

from bootstrap.config_resolver import fetch_and_apply_dead_drop


def log_daemon(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] [DAEMON] {message}\n"

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(DAEMON_LOG, "a", encoding="utf-8") as f:
            f.write(formatted)
    except Exception:
        pass


def rotate_log_if_needed(
    log_path: Path,
    max_bytes: int = 10 * 1024 * 1024,
) -> None:
    try:
        if log_path.exists() and log_path.stat().st_size >= max_bytes:
            backup_path = log_path.with_suffix(".log.1")

            if backup_path.exists():
                backup_path.unlink()

            log_path.rename(backup_path)
            log_daemon(
                f"Rotated {log_path.name} to {backup_path.name}"
            )
    except Exception as exc:
        log_daemon(
            f"Failed to rotate log {log_path.name}: {exc}"
        )


def load_env_map() -> dict[str, str]:
    data: dict[str, str] = {}

    if not ENV_PATH.exists():
        return data

    try:
        with open(ENV_PATH, "r", encoding="utf-8-sig") as f:
            for raw_line in f:
                line = raw_line.strip()

                if not line:
                    continue

                if line.startswith("#"):
                    continue

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)

                key = key.strip()
                value = value.strip()

                if (
                    len(value) >= 2
                    and value[0] == value[-1]
                    and value[0] in ("'", '"')
                ):
                    value = value[1:-1]

                data[key] = value

    except Exception as exc:
        log_daemon(f"Failed to load {ENV_PATH}: {exc}")

    return data


def check_for_updates(
    env_map: dict[str, str],
) -> tuple[bool, bool, bool]:
    """
    Returns:
        master_reachable,
        update_needed,
        config_changed
    """

    master_url = env_map.get("MASTER_URL", "").rstrip("/")
    worker_id = env_map.get("WORKER_ID", "")
    auth_token = env_map.get("AUTH_TOKEN", "")
    local_version = env_map.get("LOCAL_VERSION", "1.0.0")

    if not master_url:
        log_daemon("MASTER_URL is missing.")
        return False, False, False

    if not worker_id:
        log_daemon("WORKER_ID is missing.")
        return False, False, False

    if not auth_token:
        log_daemon("AUTH_TOKEN is missing.")
        return False, False, False

    payload = json.dumps(
        {
            "worker_id": worker_id,
            "version": local_version,
            "hostname": gethostname(),
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        f"{master_url}/api/v1/heartbeat",
        data=payload,
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        command = data.get("command")

        return True, command == "UPDATE", False

    except Exception as exc:
        log_daemon(
            f"Master heartbeat failed ({exc}); "
            "checking configuration raw URL..."
        )

        config_url = env_map.get("CONFIG_RAW_URL", "").strip()

        if not config_url:
            log_daemon("CONFIG_RAW_URL is missing.")
            return False, False, False

        try:
            config_changed = fetch_and_apply_dead_drop(
                config_url,
                SECRET_KEY_PATH,
            )

            if config_changed:
                log_daemon(
                    "Encrypted configuration downloaded, "
                    "decrypted and applied."
                )

            return False, False, config_changed

        except Exception as config_exc:
            log_daemon(
                f"Configuration update failed: {config_exc}"
            )
            return False, False, False


def configure_git_remote(env_map: dict[str, str]) -> None:
    repo_url = env_map.get("MAIN_REPO_URL", "").strip()
    branch = env_map.get("MAIN_REPO_BRANCH", "main").strip() or "main"

    if not repo_url:
        log_daemon("MAIN_REPO_URL is missing; Git remote unchanged.")
        return

    if not GIT_EXE.exists():
        raise FileNotFoundError(
            f"Bundled Git executable not found: {GIT_EXE}"
        )

    result = subprocess.run(
        [
            str(GIT_EXE),
            "remote",
            "get-url",
            "origin",
        ],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
    )

    current_url = result.stdout.strip()

    if current_url != repo_url:
        subprocess.run(
            [
                str(GIT_EXE),
                "remote",
                "set-url",
                "origin",
                repo_url,
            ],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True,
            text=True,
        )

        log_daemon(
            f"Git origin configured: {repo_url}"
        )

    subprocess.run(
        [
            str(GIT_EXE),
            "config",
            "remote.origin.fetch",
            f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
        ],
        cwd=str(REPO_DIR),
        check=True,
        capture_output=True,
        text=True,
    )


def apply_git_update(env_map: dict[str, str]) -> bool:
    branch = env_map.get("MAIN_REPO_BRANCH", "main").strip() or "main"

    log_daemon(
        f"Initiating cluster code update from branch '{branch}'..."
    )

    try:
        configure_git_remote(env_map)

        subprocess.run(
            [
                str(GIT_EXE),
                "fetch",
                "--prune",
                "origin",
                branch,
            ],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True,
            text=True,
        )

        subprocess.run(
            [
                str(GIT_EXE),
                "reset",
                "--hard",
                f"origin/{branch}",
            ],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True,
            text=True,
        )

        subprocess.run(
            [
                str(PYTHON_CLI),
                "-m",
                "pip",
                "install",
                "-r",
                "requirements_worker.txt",
                "--quiet",
            ],
            cwd=str(REPO_DIR),
            check=True,
            capture_output=True,
            text=True,
        )

        log_daemon(
            "Code update and dependency sync completed successfully."
        )

        return True

    except Exception as exc:
        log_daemon(
            f"Git code update failed: {exc}"
        )
        return False


def restart_worker(
    proc: subprocess.Popen | None,
) -> None:
    if proc is None:
        return

    if proc.poll() is not None:
        return

    log_daemon(
        "Terminating active worker process for restart..."
    )

    proc.terminate()

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log_daemon(
            "Worker did not terminate within 10 seconds; killing it."
        )
        proc.kill()

        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def launch_worker(
    worker_script: Path,
    log_file_handle,
) -> subprocess.Popen:
    if not PYTHONW_EXE.exists():
        raise FileNotFoundError(
            f"Python executable not found: {PYTHONW_EXE}"
        )

    if not worker_script.exists():
        raise FileNotFoundError(
            f"Worker script not found: {worker_script}"
        )

    return subprocess.Popen(
        [
            str(PYTHONW_EXE),
            str(worker_script),
        ],
        cwd=str(REPO_DIR),
        stdout=log_file_handle,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    worker_script = REPO_DIR / "worker" / "main_worker.py"

    proc: subprocess.Popen | None = None
    log_file_handle = None

    log_daemon(
        "GMapElite Worker Daemon supervisor initialized."
    )

    while True:
        try:
            env_map = load_env_map()

            if not env_map:
                log_daemon(
                    "No worker configuration found; "
                    "waiting before retry."
                )
                time.sleep(10)
                continue

            master_reachable, update_needed, config_changed = (
                check_for_updates(env_map)
            )

            if config_changed or update_needed:
                restart_worker(proc)
                proc = None

                if log_file_handle:
                    try:
                        log_file_handle.close()
                    except Exception:
                        pass

                    log_file_handle = None

                if update_needed:
                    update_ok = apply_git_update(env_map)

                    if not update_ok:
                        log_daemon(
                            "Code update failed; "
                            "worker will not be restarted from an "
                            "unverified update."
                        )
                        time.sleep(30)
                        continue

                # Reload configuration after an update.
                env_map = load_env_map()

            if proc is None or proc.poll() is not None:
                if proc is not None:
                    log_daemon(
                        f"Worker process exited with code "
                        f"{proc.poll()}. Restarting..."
                    )

                if log_file_handle:
                    try:
                        log_file_handle.close()
                    except Exception:
                        pass

                rotate_log_if_needed(WORKER_LOG)

                log_file_handle = open(
                    WORKER_LOG,
                    "a",
                    encoding="utf-8",
                )

                proc = launch_worker(
                    worker_script,
                    log_file_handle,
                )

                log_daemon(
                    f"Worker node launched headlessly "
                    f"(PID: {proc.pid})."
                )

            # Keep the supervisor loop predictable.
            # The actual worker polling intervals are controlled by the worker.
            time.sleep(30)

        except Exception as exc:
            log_daemon(
                f"Supervisor loop error: {exc}"
            )
            time.sleep(30)


if __name__ == "__main__":
    main()
