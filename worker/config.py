import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    master_url: str
    worker_id: str
    auth_token: str
    version: str
    poll_min_seconds: float
    poll_max_seconds: float
    relay_dir: Path


def load_settings() -> Settings:
    worker_id = os.getenv("WORKER_ID", "").strip()
    if not worker_id:
        raise ValueError("WORKER_ID must be configured in the machine's .env file")
    master_url = os.getenv("MASTER_URL", "").strip().rstrip("/")
    token = os.getenv("AUTH_TOKEN", "").strip()
    if not master_url:
        raise ValueError("MASTER_URL must be configured")
    if not token:
        raise ValueError("AUTH_TOKEN must be configured")
    tag = subprocess.run(
        ["git", "describe", "--tags", "--exact-match"],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    version = (
        tag.stdout.strip()
        if tag.returncode == 0
        else subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    )
    relay_dir = Path(os.getenv("WORKER_RELAY_DIR", str(ROOT_DIR / "worker_tmp")))
    if not relay_dir.is_absolute():
        relay_dir = ROOT_DIR / relay_dir
    return Settings(
        master_url=master_url,
        worker_id=worker_id,
        auth_token=token,
        version=version or "unknown",
        poll_min_seconds=float(os.getenv("WORKER_POLL_MIN_SECONDS", "2")),
        poll_max_seconds=float(os.getenv("WORKER_POLL_MAX_SECONDS", "20")),
        relay_dir=relay_dir,
    )
