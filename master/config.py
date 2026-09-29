import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
MASTER_API_URL = os.getenv("MASTER_API_URL", "").strip().rstrip("/")
CORS_ALLOW_ORIGINS = tuple(
    origin.strip()
    for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",")
    if origin.strip()
)


@dataclass(frozen=True)
class Settings:
    database_url: str
    auth_token: str
    master_api_url: str
    raw_html_dir: Path
    task_lease_seconds: int
    max_upload_bytes: int
    max_uncompressed_html_bytes: int
    database_pool_min: int
    database_pool_max: int
    cors_allow_origins: tuple[str, ...]
    stage2_batch_size: int
    stage4_batch_size: int
    parser_poll_seconds: float
    stage4_timeout_seconds: int


def load_settings() -> Settings:
    token = os.getenv("MASTER_AUTH_TOKEN", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not token:
        raise ValueError("MASTER_AUTH_TOKEN must be configured")
    if not database_url:
        raise ValueError("DATABASE_URL must be configured")

    raw_html_dir = Path(os.getenv("RAW_HTML_DIR", "raw_html_storage"))
    if not raw_html_dir.is_absolute():
        raw_html_dir = ROOT_DIR / raw_html_dir
    return Settings(
        database_url=database_url,
        auth_token=token,
        master_api_url=MASTER_API_URL or "http://127.0.0.1:8000",
        raw_html_dir=raw_html_dir,
        task_lease_seconds=int(os.getenv("TASK_LEASE_SECONDS", "900")),
        max_upload_bytes=int(os.getenv("MAX_UPLOAD_BYTES", str(256 * 1024 * 1024))),
        max_uncompressed_html_bytes=int(
            os.getenv("MAX_UNCOMPRESSED_HTML_BYTES", str(1024 * 1024 * 1024))
        ),
        database_pool_min=int(os.getenv("DATABASE_POOL_MIN", "1")),
        database_pool_max=int(os.getenv("DATABASE_POOL_MAX", "10")),
        cors_allow_origins=CORS_ALLOW_ORIGINS,
        stage2_batch_size=int(os.getenv("STAGE2_BATCH_SIZE", "20")),
        stage4_batch_size=int(os.getenv("STAGE4_BATCH_SIZE", "25")),
        parser_poll_seconds=float(os.getenv("PARSER_POLL_SECONDS", "10")),
        stage4_timeout_seconds=int(os.getenv("STAGE4_TIMEOUT_SECONDS", "1800")),
    )
