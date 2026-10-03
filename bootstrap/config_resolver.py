from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


CONFIG_DIR = Path(r"C:\ProgramData\GMapEliteCluster\config")
ENV_PATH = CONFIG_DIR / ".env"
SECRET_KEY_PATH = CONFIG_DIR / "secret.key"

MIN_ENCRYPTED_SIZE = 16 + 16  # IV + at least one AES block
AES_KEY_SIZE = 32
AES_BLOCK_SIZE_BITS = 128


def _load_key(key_path: Path) -> bytes:
    if not key_path.exists():
        raise FileNotFoundError(
            f"Secret key not found: {key_path}"
        )

    key = key_path.read_bytes()

    if len(key) != AES_KEY_SIZE:
        raise ValueError(
            f"Secret key must be exactly {AES_KEY_SIZE} bytes; "
            f"got {len(key)}"
        )

    return key


def _decrypt_payload(
    encrypted: bytes,
    key: bytes,
) -> bytes:
    if len(encrypted) < MIN_ENCRYPTED_SIZE:
        raise ValueError(
            "Encrypted configuration is too short."
        )

    iv = encrypted[:16]
    ciphertext = encrypted[16:]

    if len(ciphertext) == 0 or len(ciphertext) % 16 != 0:
        raise ValueError(
            "Encrypted configuration ciphertext is not a valid AES block sequence."
        )

    cipher = Cipher(
        algorithms.AES(key),
        modes.CBC(iv),
    )

    decryptor = cipher.decryptor()
    padded_plaintext = (
        decryptor.update(ciphertext)
        + decryptor.finalize()
    )

    unpadder = padding.PKCS7(AES_BLOCK_SIZE_BITS).unpadder()

    plaintext = (
        unpadder.update(padded_plaintext)
        + unpadder.finalize()
    )

    if not plaintext.strip():
        raise ValueError(
            "Decrypted configuration is empty."
        )

    return plaintext


def _parse_env(text: str) -> dict[str, str]:
    values: dict[str, str] = {}

    for raw_line in text.splitlines():
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

        if key:
            values[key] = value

    return values


def _write_atomically(
    destination: Path,
    plaintext: bytes,
) -> None:
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=".env.",
        suffix=".tmp",
        dir=str(destination.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(fd, "wb") as f:
            f.write(plaintext)
            f.flush()
            os.fsync(f.fileno())

        os.replace(
            temporary_path,
            destination,
        )

    finally:
        if temporary_path.exists():
            try:
                temporary_path.unlink()
            except Exception:
                pass


def fetch_and_apply_dead_drop(
    config_url: str,
    key_path: Path | str = SECRET_KEY_PATH,
) -> bool:
    """
    Download config.env.enc, decrypt it using the local 32-byte AES key,
    preserve the local WORKER_ID, and atomically replace config\.env.

    Returns True when the downloaded configuration was successfully applied.
    """

    if not config_url:
        raise ValueError("CONFIG_RAW_URL is empty.")

    key_path = Path(key_path)

    key = _load_key(key_path)

    request = Request(
        config_url,
        headers={
            "User-Agent": "GMapElite-Worker/1.0",
            "Accept": "application/octet-stream",
        },
        method="GET",
    )

    with urlopen(request, timeout=20) as response:
        encrypted = response.read()

    plaintext = _decrypt_payload(
        encrypted,
        key,
    )

    decoded = plaintext.decode(
        "utf-8-sig"
    )

    downloaded_env = _parse_env(decoded)

    # The shared encrypted configuration must never control worker identity.
    # WORKER_ID belongs only to this machine.
    existing_worker_id = None

    if ENV_PATH.exists():
        try:
            existing_text = ENV_PATH.read_text(
                encoding="utf-8-sig"
            )
            existing_env = _parse_env(existing_text)
            existing_worker_id = existing_env.get(
                "WORKER_ID"
            )
        except Exception:
            existing_worker_id = None

    downloaded_env.pop(
        "WORKER_ID",
        None,
    )

    if existing_worker_id:
        downloaded_env["WORKER_ID"] = existing_worker_id

    # The resolver deliberately requires the critical runtime settings.
    required = (
        "MASTER_URL",
        "AUTH_TOKEN",
        "MAIN_REPO_URL",
        "MAIN_REPO_BRANCH",
        "CONFIG_RAW_URL",
        "LOCAL_VERSION",
    )

    missing = [
        key
        for key in required
        if not downloaded_env.get(key)
    ]

    if missing:
        raise ValueError(
            "Downloaded configuration is missing required keys: "
            + ", ".join(missing)
        )

    # Rebuild the file in a deterministic KEY=VALUE form.
    output_lines = []

    for key, value in downloaded_env.items():
        output_lines.append(
            f"{key}={value}"
        )

    output = (
        "\n".join(output_lines)
        + "\n"
    ).encode("utf-8")

    _write_atomically(
        ENV_PATH,
        output,
    )

    return True


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python config_resolver.py <CONFIG_RAW_URL>"
        )

    fetch_and_apply_dead_drop(
        sys.argv[1],
        SECRET_KEY_PATH,
    )

    print("Configuration downloaded, decrypted and applied.")
