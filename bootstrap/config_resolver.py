import os
import sys
import json
import base64
import hashlib
import hmac
import urllib.request
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
CONFIG_META_PATH = BASE_DIR / "config_state.json"


def load_local_version() -> int:
    if not CONFIG_META_PATH.exists():
        return 0
    try:
        with open(CONFIG_META_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("config_version", 0)
    except Exception:
        return 0


def record_local_version(version: int):
    with open(CONFIG_META_PATH, "w", encoding="utf-8") as f:
        json.dump({"config_version": version}, f)


def fetch_and_apply_dead_drop(raw_repo_url: str, decryption_key: str) -> bool:
    """
    Fetches encrypted config from a public raw GitHub URL.
    Decrypts, validates monotonic versioning, and rewrites .env.
    """
    current_version = load_local_version()
    
    # 1. Fetch encrypted blob from public raw endpoint
    try:
        req = urllib.request.Request(
            raw_repo_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            if response.status != 200:
                return False
            ciphertext = response.read().strip()
    except Exception:
        return False

    # 2. Decrypt ciphertext
    try:
        cipher = Fernet(decryption_key.encode("utf-8"))
        decrypted_bytes = cipher.decrypt(ciphertext)
        payload = json.loads(decrypted_bytes.decode("utf-8"))
    except (InvalidToken, Exception):
        return False

    # 3. Guard against replay attacks
    new_version = payload.get("config_version", 0)
    if new_version <= current_version:
        return False

    env_vars = payload.get("env", {})
    if not env_vars:
        return False

    # 4. Atomic .env rewrite
    temp_env = ENV_PATH.with_suffix(".tmp")
    try:
        with open(temp_env, "w", encoding="utf-8") as f:
            f.write(f"# Auto-generated via Dead-Drop v{new_version}\n")
            for k, v in env_vars.items():
                f.write(f"{k}={v}\n")
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_env, ENV_PATH)
        record_local_version(new_version)
        return True
    except Exception:
        if temp_env.exists():
            temp_env.unlink()
        return False