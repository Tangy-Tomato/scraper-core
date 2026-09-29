from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from master.config import load_settings


ROOT_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT_DIR / "Raw_HTML_Cache"
CACHE_TTL_DAYS = 80
ARCHIVE_CACHE_DIR = CACHE_DIR / "archives"
Delete_Archive_Dir = CACHE_DIR / "delete_archive"
QUARANTINE_DIR = ROOT_DIR / "System_Temp" / "quarantine"

_SETTINGS = load_settings()
_DATABASE_URL = urlparse(_SETTINGS.database_url)
_DATABASE_QUERY = parse_qs(_DATABASE_URL.query)
DB_CONFIG = {
    "dbname": unquote(_DATABASE_URL.path.lstrip("/")),
    "user": unquote(_DATABASE_URL.username or ""),
    "password": unquote(_DATABASE_URL.password or ""),
    "host": _DATABASE_URL.hostname or "localhost",
}
if _DATABASE_URL.port:
    DB_CONFIG["port"] = str(_DATABASE_URL.port)
if "sslmode" in _DATABASE_QUERY:
    DB_CONFIG["sslmode"] = _DATABASE_QUERY["sslmode"][0]

CANONICAL_COLUMNS = [
    "id",
    "search_keyword",
    "search_location",
    "order_ids",
    "query_type",
    "name",
    "url",
    "website",
    "rating",
    "reviews",
    "category",
    "phone",
    "phone_type",
    "scraped_date",
    "emails",
    "alternative_phones",
    "linkedin",
    "facebook",
    "twitter",
    "instagram",
    "data_status",
    "found_web_mobile",
    "is_valid",
    "lead_status",
    "first_seen_date",
    "last_updated_date",
    "dead_email",
    "enriched_at",
    "enrichment_version",
    "http_status",
    "domain_state",
    "ssl_status",
    "mobile_friendly",
    "has_contact_info",
    "quality_score",
    "tags",
    "last_checked_at",
    "raw_extracted",
]
