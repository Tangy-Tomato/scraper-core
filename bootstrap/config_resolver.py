import json
import os
from pathlib import Path
import urllib.request
from cryptography.fernet import Fernet, InvalidToken

CLUSTER_ROOT = Path(r"C:\ProgramData\GMapEliteCluster")
CONFIG_DIR = CLUSTER_ROOT / "config"
ENV_PATH = CONFIG_DIR / ".env"
CONFIG_META_PATH = CONFIG_DIR / "config_state.json"

CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def load_local_version() -> int:
    """Reads the current configuration epoch from outside the git directory."""
    if not CONFIG_META_PATH.exists():
        return 0
    try:
        with open(CONFIG_META_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("config_version", 0)
    except Exception:
        return 0


def record_local_version(version: int) -> None:
    """Atomically commits configuration version to disk."""
    temp_path = CONFIG_META_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump({"config_version": version}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, CONFIG_META_PATH)


def fetch_and_apply_dead_drop(raw_repo_url: str, decryption_key: str) -> bool:
    """Pulls encrypted config from the dedicated GitHub config repository,
    validates monotonic versioning, and rewrites config/.env atomically.
    Preserves machine-static keys: CONFIG_RAW_URL, DEAD_DROP_URL, DECRYPTION_KEY, WORKER_ID.
    """
    if not raw_repo_url or not decryption_key:
        return False

    current_version = load_local_version()

    # 1. Fetch encrypted blob from public raw endpoint
    try:
        req = urllib.request.Request(
            raw_repo_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            if response.status != 200:
                return False
            ciphertext = response.read().strip()
    except Exception:
        return False

    # 2. Decrypt ciphertext using local Fernet key
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

    # 4. Extract existing static bootstrap keys to prevent overwriting
    preserved_keys = {}
    if ENV_PATH.exists():
        try:
            with open(ENV_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        if k in ("CONFIG_RAW_URL", "DEAD_DROP_URL", "DECRYPTION_KEY", "WORKER_ID"):
                            preserved_keys[k] = v
        except Exception:
            pass

    # 5. Atomic .env rewrite
    temp_env = ENV_PATH.with_suffix(".tmp")
    try:
        with open(temp_env, "w", encoding="utf-8") as f:
            f.write(f"# Auto-generated via Dead-Drop Epoch v{new_version}\n")
            for k, v in preserved_keys.items():
                f.write(f"{k}={v}\n")
            for k, v in env_vars.items():
                if k not in preserved_keys:
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