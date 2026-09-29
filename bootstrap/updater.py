import argparse
import json
import os
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "bootstrap" / ".updater_state.json"
MAX_FAILED_STARTS = 3


def read_env_file(path):
    values = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def git(*args, check=True):
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
    )


def current_version():
    exact = git("describe", "--tags", "--exact-match", check=False)
    if exact.returncode == 0:
        return exact.stdout.strip()
    return git("rev-parse", "--short", "HEAD").stdout.strip()


def load_state():
    if not STATE_FILE.exists():
        return {}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temp, STATE_FILE)


def heartbeat(env, version):
    base_url = env.get("MASTER_URL", "").rstrip("/")
    token = env.get("AUTH_TOKEN", "")
    worker_id = env.get("WORKER_ID", "")
    if not base_url or not token or not worker_id:
        raise ValueError("MASTER_URL, AUTH_TOKEN, and WORKER_ID are required in .env")
    payload = json.dumps(
        {"worker_id": worker_id, "version": version, "hostname": socket.gethostname()}
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/api/v1/heartbeat",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def restore_previous(state):
    previous = state.get("previous_version")
    failed_target = state.get("failed_target")
    if previous:
        git("checkout", previous)
        state["failed_target"] = failed_target
        state["failed_starts"] = 0
        save_state(state)
        print(f"Rolled back to {previous}; update {failed_target} is marked failed.", file=sys.stderr)


def update():
    env = read_env_file(ROOT / ".env")
    state = load_state()
    version = current_version()
    result = heartbeat(env, version)
    target = result["target_version"]

    if state.get("failed_target") == target:
        print(f"Skipping previously failed update target {target}.", file=sys.stderr)
        return
    if not result.get("update_required"):
        state["failed_starts"] = 0
        save_state(state)
        return

    previous = version
    git("fetch", "--tags", "--prune")
    git("checkout", target)
    install = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            str(ROOT / "requirements_worker.txt"),
        ],
        cwd=ROOT,
        check=False,
    )
    if install.returncode != 0:
        git("checkout", previous)
        raise RuntimeError(f"dependency installation failed for {target}")
    state.update(
        {
            "previous_version": previous,
            "failed_target": None,
            "failed_starts": 0,
        }
    )
    save_state(state)


def record_failure():
    state = load_state()
    state["failed_starts"] = int(state.get("failed_starts", 0)) + 1
    if state["failed_starts"] >= MAX_FAILED_STARTS and state.get("previous_version"):
        failed_target = current_version()
        state["failed_target"] = failed_target
        restore_previous(state)
        return
    save_state(state)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-failure", action="store_true")
    args = parser.parse_args()
    if args.record_failure:
        record_failure()
    else:
        update()


if __name__ == "__main__":
    main()
