#!/usr/bin/env python3
import os
import re
import sys
import time
import json
import ssl
import socket
import hashlib
import random
import logging
import requests
import csv
import ast
import threading
import psycopg2
import gc
import sqlite3
import uuid
import warnings
from queue import Empty
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from playwright.sync_api import sync_playwright
from pathlib import Path
import multiprocessing
from datetime import datetime, timezone
import phonenumbers
import dns.resolver
import whois
from lib.settings import DB_CONFIG

# --- WARNING FILTERS ---
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress noisy underlying socket error logs from urllib3
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

# --- PATHS & CONFIGURATION ---
ROOT_DIR = Path(__file__).resolve().parent
SYSTEM_TEMP_DIR = ROOT_DIR / "System_Temp"
NETWORK_CACHE_DB = SYSTEM_TEMP_DIR / "network_cache.db"
CACHE_TTL_DAYS = 80
CACHE_TTL_SECONDS = CACHE_TTL_DAYS * 24 * 3600
TARPIT_DIR = ROOT_DIR / "Tarpit_Locks"
RawHtmlCacheDir = ROOT_DIR / "Raw_HTML_Cache"
HTML_CACHE_DIR = RawHtmlCacheDir / "html_cache"

INPUT_CSV = SYSTEM_TEMP_DIR / "processing_buffer.csv"
SYNC_BUFFER_CSV = SYSTEM_TEMP_DIR / "enriched_sync_buffer.csv"
TEMP_ONLINE_CSV = SYSTEM_TEMP_DIR / "temp_online_queue.csv"
BLOCKLIST_FILE = ROOT_DIR / "dead_sites_blocklist.txt"
QUARANTINE_CSV = SYSTEM_TEMP_DIR / "quarantine.csv"
RUN_SUMMARY_JSON = SYSTEM_TEMP_DIR / "run_summary.json"

for d in [SYSTEM_TEMP_DIR, HTML_CACHE_DIR, TARPIT_DIR]:
    os.makedirs(d, exist_ok=True)

# --- WORKER & CONCURRENCY CONFIGURATION ---
ONLINE_WORKERS = 15
OFFLINE_WORKERS = max(1, multiprocessing.cpu_count() - 1)
PLAYWRIGHT_WORKERS = 2
MAX_URLS_PER_BROWSER = 20
REQUEST_TIMEOUT = (10, 20)
PLAYWRIGHT_TIMEOUT = 30000
FLUSH_BATCH_SIZE = 100

# New configurable defaults per user decision
PW_QUEUE_MAXSIZE = 2000
OFFLINE_BATCH_SIZE = 2000
CURRENT_VERSION = 1

# DNS Configuration
DNS_SERVERS = ["8.8.8.8", "1.1.1.1"]
DNS_TIMEOUT = 4.0

# Quality score weights (sum to 100)
QUALITY_SCORE_WEIGHTS = {
    "reachability": 30,
    "contact": 30,
    "ssl": 10,
    "mobile": 10,
    "social_reviews": 10,
    "perf_seo": 10,
}

# --- GLOBAL MX RESOLVER (SINGLETON) ---
GLOBAL_MX_RESOLVER = dns.resolver.Resolver(configure=False)
GLOBAL_MX_RESOLVER.nameservers = DNS_SERVERS

# Target Schema Columns
STAGE_4_COLUMNS = [
    "Emails",
    "Alternative_Phones",
    "LinkedIn",
    "Facebook",
    "Twitter",
    "Instagram",
    "Data_Status",
    "Found_Web_Mobile",
    "reviews",
    "is_valid",
    "lead_status",
    "first_seen_date",
    "last_updated_date",
]

# New CSV columns to append (order per user)
NEW_COLUMNS = [
    "Dead_Email",
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

# Authoritative final CSV schema. The order is intentionally identical to the
# processing-buffer columns followed by the enrichment columns. This is the
# contract consumed by the PostgreSQL staging COPY operation.
FINAL_CSV_COLUMNS = [
    "id",
    "Search_Keyword",
    "Search_Location",
    "order_ids",
    "query_type",
    "name",
    "url",
    "website",
    "rating",
    "reviews",
    "category",
    "phone",
    "Phone_Type",
    "Scraped_Date",
    "Emails",
    "Alternative_Phones",
    "LinkedIn",
    "Facebook",
    "Twitter",
    "Instagram",
    "Data_Status",
    "Found_Web_Mobile",
    "is_valid",
    "lead_status",
    "first_seen_date",
    "last_updated_date",
    "Dead_Email",
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

BASE_PROCESSING_COLUMNS = [
    "id",
    "Search_Keyword",
    "Search_Location",
    "order_ids",
    "query_type",
    "name",
    "url",
    "website",
    "rating",
    "reviews",
    "category",
    "phone",
    "Phone_Type",
    "Scraped_Date",
]

BOOLEAN_COLUMNS = {
    "is_valid",
    "Dead_Email",
    "mobile_friendly",
    "has_contact_info",
}

INTEGER_COLUMNS = {
    "reviews",
    "enrichment_version",
    "http_status",
    "quality_score",
}

REAL_COLUMNS = {"rating"}
TIMESTAMP_COLUMNS = {
    "Scraped_Date",
    "first_seen_date",
    "last_updated_date",
    "enriched_at",
    "last_checked_at",
}
TEXT_COLUMNS = {
    "id",
    "Search_Keyword",
    "Search_Location",
    "query_type",
    "name",
    "url",
    "website",
    "category",
    "phone",
    "Phone_Type",
    "Emails",
    "Alternative_Phones",
    "LinkedIn",
    "Facebook",
    "Twitter",
    "Instagram",
    "Data_Status",
    "Found_Web_Mobile",
    "lead_status",
    "domain_state",
    "ssl_status",
}
JSONB_COLUMNS = {"tags", "raw_extracted"}


def _normalize_text(value, column):
    if value is None:
        return ""
    if isinstance(value, bool):
        raise ValueError(f"column {column!r} received boolean {value!r}; expected text")
    return str(value).replace("\x00", "").replace("\r", " ").replace("\n", " ")


def _normalize_boolean(value, column):
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and value in (0, 1):
        return "true" if value else "false"
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes"}:
            return "true"
        if normalized in {"false", "f", "0", "no"}:
            return "false"
    raise ValueError(f"column {column!r} received {value!r}; expected boolean")


def _normalize_integer(value, column):
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    if isinstance(value, bool):
        raise ValueError(
            f"column {column!r} received boolean {value!r}; expected integer"
        )
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"column {column!r} received non-integral value {value!r}")
        return str(int(value))
    text = str(value).strip().replace(",", "")
    if not re.fullmatch(r"[+-]?\d+", text):
        raise ValueError(f"column {column!r} received {value!r}; expected integer")
    return str(int(text))


def _normalize_real(value, column):
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    if isinstance(value, bool):
        raise ValueError(f"column {column!r} received boolean {value!r}; expected real")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"column {column!r} received {value!r}; expected real"
        ) from exc
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError(f"column {column!r} received non-finite real value {value!r}")
    return format(number, ".15g")


def _normalize_timestamp(value, column):
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
    text = str(value).strip()
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(
            f"column {column!r} received invalid timestamp {value!r}"
        ) from exc
    # PostgreSQL target columns are timestamp without time zone. The existing
    # pipeline stores UTC clock time, so preserve the supplied clock value and
    # remove only the timezone marker rather than silently converting it.
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")


def _coerce_order_id_collection(value):
    """Return order IDs as a deterministic list of strings.

    processing_buffer.csv stores this field as text, commonly a Python list
    representation such as "['ORDER_1']". The enrichment pipeline may also
    hand us an actual list/tuple/set. No arbitrary scalar is interpreted as an
    array; that would hide upstream corruption.
    """
    if value is None:
        return []

    if isinstance(value, bool):
        raise ValueError(
            f"column 'order_ids' received boolean {value!r}; expected empty/list/tuple/set"
        )

    if isinstance(value, str):
        text = value.strip()
        if not text or text in {"[]", "()", "{}", "None", "null"}:
            return []

        # Accept an already serialized PostgreSQL text-array literal. This is
        # mainly defensive; the processing buffer normally contains Python-list
        # text, not PostgreSQL array syntax.
        if text.startswith("{") and text.endswith("}"):
            return _parse_pg_array_literal(text)

        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError) as exc:
            raise ValueError(
                f"column 'order_ids' received unsupported value {value!r}"
            ) from exc
        return _coerce_order_id_collection(parsed)

    if isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
        normalized = []
        for item in values:
            if item is None or isinstance(item, (dict, list, tuple, set, frozenset)):
                raise ValueError(
                    f"column 'order_ids' contains unsupported element {item!r}"
                )
            if isinstance(item, bool):
                raise ValueError(
                    f"column 'order_ids' contains boolean element {item!r}"
                )
            text = str(item).strip()
            if text:
                normalized.append(text)
        return sorted(set(normalized))

    raise ValueError(
        f"column 'order_ids' received type {type(value).__name__}; expected empty/list/tuple/set"
    )


def _parse_pg_array_literal(text):
    """Parse a one-dimensional PostgreSQL array literal into string elements."""
    if text == "{}":
        return []
    if not (text.startswith("{") and text.endswith("}")):
        raise ValueError(f"invalid PostgreSQL array literal: {text!r}")

    body = text[1:-1]
    values = []
    token = []
    in_quotes = False
    escaped = False

    for char in body:
        if escaped:
            token.append(char)
            escaped = False
            continue
        if char == "\\" and in_quotes:
            escaped = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == "," and not in_quotes:
            item = "".join(token)
            values.append(item)
            token = []
            continue
        token.append(char)

    if escaped or in_quotes:
        raise ValueError(f"invalid PostgreSQL array literal: {text!r}")

    values.append("".join(token))
    return [v for v in values if v != ""]


def _serialize_pg_text_array(value):
    values = _coerce_order_id_collection(value)
    if not values:
        return "{}"

    escaped = []
    for item in values:
        # Quote every element. This is unambiguous for commas, quotes,
        # backslashes, braces and whitespace and is valid PostgreSQL array syntax.
        item = item.replace("\\", "\\\\").replace('"', '\\"')
        escaped.append(f'"{item}"')
    return "{" + ",".join(escaped) + "}"


def _normalize_jsonb(value, column, empty_value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return empty_value

    if isinstance(value, (dict, list, tuple, set, frozenset)):
        if isinstance(value, (set, frozenset)):
            value = sorted(value)
        elif isinstance(value, tuple):
            value = list(value)
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"column {column!r} received invalid JSON: {value!r}"
            ) from exc
        return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))

    raise ValueError(
        f"column {column!r} received unsupported JSON value of type {type(value).__name__}"
    )


def normalize_and_validate_final_row(row):
    """Normalize one completed enrichment dictionary to the PostgreSQL CSV contract."""
    if not isinstance(row, dict):
        raise ValueError(f"final row must be dict, received {type(row).__name__}")

    missing = [column for column in FINAL_CSV_COLUMNS if column not in row]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    extra = [column for column in row if column not in FINAL_CSV_COLUMNS]
    if extra:
        # Extras are ignored by the existing writer contract, but rejecting them
        # here catches accidental schema drift before serialization.
        raise ValueError(f"unexpected columns: {extra}")

    normalized = {}
    for column in FINAL_CSV_COLUMNS:
        value = row.get(column)

        if column == "order_ids":
            normalized[column] = _serialize_pg_text_array(value)
        elif column == "tags":
            normalized[column] = _normalize_jsonb(value, column, "[]")
        elif column == "raw_extracted":
            normalized[column] = _normalize_jsonb(value, column, "{}")
        elif column in BOOLEAN_COLUMNS:
            normalized[column] = _normalize_boolean(value, column)
        elif column in INTEGER_COLUMNS:
            normalized[column] = _normalize_integer(value, column)
        elif column in REAL_COLUMNS:
            normalized[column] = _normalize_real(value, column)
        elif column in TIMESTAMP_COLUMNS:
            normalized[column] = _normalize_timestamp(value, column)
        elif column in TEXT_COLUMNS:
            normalized[column] = _normalize_text(value, column)
        else:
            raise ValueError(f"column {column!r} has no normalization rule")

    return normalized


def validate_final_schema(headers):
    if list(headers) != FINAL_CSV_COLUMNS:
        raise RuntimeError(
            "Final CSV schema mismatch.\n"
            f"Expected: {FINAL_CSV_COLUMNS}\n"
            f"Received: {list(headers)}"
        )


def quarantine_invalid_row(row, error):
    """Persist an invalid row for diagnosis without contaminating the final CSV."""
    try:
        with open(QUARANTINE_CSV, "a", newline="", encoding="utf-8") as qf:
            writer = csv.writer(qf)
            writer.writerow(
                [
                    "final_schema_validation_failed",
                    json.dumps(
                        {
                            "id": str(row.get("id", "")),
                            "error": str(error),
                            "row": row,
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                ]
            )
    except Exception:
        logger.exception("Failed to write schema-validation quarantine record")


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

# --- GLOBALS & LOCKS ---
DEAD_SITES = set()
BLOCKLIST_LOCK = threading.Lock()

# --- CSV field size override (prevents field too large errors) ---
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# --- PURE FUNCTIONS ---
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.I)


# --- NETWORK CACHE ENGINE (SQLITE WAL MODE) ---
def init_network_cache_db():
    with sqlite3.connect(
        str(NETWORK_CACHE_DB), timeout=60, isolation_level=None
    ) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS network_cache (
                domain TEXT NOT NULL,
                check_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (domain, check_type)
            );
        """)
    logger.info(f"[SYSTEM] Network Cache DB initialized at {NETWORK_CACHE_DB}")


def get_network_cache(domain: str, check_type: str) -> dict:
    if not domain:
        return None
    try:
        with sqlite3.connect(str(NETWORK_CACHE_DB), timeout=30) as conn:
            cur = conn.execute(
                "SELECT payload, updated_at FROM network_cache WHERE domain = ? AND check_type = ?",
                (domain, check_type),
            )
            row = cur.fetchone()
            if row:
                payload_str, updated_at = row
                if (time.time() - updated_at) <= CACHE_TTL_SECONDS:
                    return json.loads(payload_str)
    except Exception as e:
        logger.debug(f"[CACHE READ ERROR] {e}")
    return None


def set_network_cache(domain: str, check_type: str, payload: dict) -> None:
    if not domain:
        return
    payload_str = json.dumps(payload)
    now = time.time()
    for attempt in range(5):
        try:
            with sqlite3.connect(
                str(NETWORK_CACHE_DB), timeout=30, isolation_level=None
            ) as conn:
                conn.execute(
                    """
                    INSERT INTO network_cache (domain, check_type, payload, updated_at) 
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(domain, check_type) 
                    DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at;
                    """,
                    (domain, check_type, payload_str, now),
                )
            break
        except sqlite3.OperationalError:
            time.sleep(0.1 * (attempt + 1))
        except Exception as e:
            logger.debug(f"[CACHE WRITE ERROR] {e}")
            break


# --- DEDUP DB (PER-RUN DISK BACKED) ---
def init_dedup_db(run_id):
    db_path = SYSTEM_TEMP_DIR / f"dedup_{run_id}.db"
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY, seen_at TEXT);")
    return conn, db_path


def mark_id_seen(conn, row_id):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn.execute(
            "INSERT OR REPLACE INTO seen (id, seen_at) VALUES (?, ?);", (row_id, now)
        )
    except sqlite3.OperationalError:
        for _ in range(3):
            time.sleep(0.1)
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO seen (id, seen_at) VALUES (?, ?);",
                    (row_id, now),
                )
                return
            except sqlite3.OperationalError:
                continue


def is_id_seen(conn, row_id):
    cur = conn.execute("SELECT 1 FROM seen WHERE id = ? LIMIT 1;", (row_id,))
    return cur.fetchone() is not None


# --- UTILITY FUNCTIONS ---
def get_cache_hash(url):
    if not isinstance(url, str) or not url.strip():
        return ""
    clean = url.lower().strip()
    clean = re.sub(r"^https?://", "", clean)
    clean = re.sub(r"^www\.", "", clean)
    clean = clean.split("?")[0].split("#")[0].rstrip("/")
    return hashlib.md5(clean.encode("utf-8")).hexdigest()


def extract_safe_integer(val, default=0):
    if not val:
        return default
    match = re.search(r"\d+", str(val).replace(",", ""))
    return int(match.group()) if match else default


def normalize_url(url):
    if not url:
        return ""
    u = url.strip()
    if not u.startswith("http"):
        u = "https://" + u
    return u


def classify_scraped_phone(phone_str, default_region="IN"):
    try:
        parsed = phonenumbers.parse(phone_str, default_region)
        if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(
            parsed
        ):
            country_code = parsed.country_code
            national = phonenumbers.format_number(
                parsed, phonenumbers.PhoneNumberFormat.NATIONAL
            ).replace(" ", "")
            return f"+{country_code} {national}", (
                "Mobile"
                if phonenumbers.number_type(parsed)
                == phonenumbers.PhoneNumberType.MOBILE
                else "Landline"
            )
    except Exception:
        pass
    return None, None


def validate_email(email_str, enable_mx=True, mx_timeout=2, mx_retries=1):
    if not email_str:
        return None, False
    email = email_str.strip().lower()

    invalid_exts = (
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".svg",
        ".js",
        ".css",
        ".pdf",
    )
    if any(email.endswith(ext) for ext in invalid_exts):
        return None, False
    if not EMAIL_REGEX.fullmatch(email):
        return None, False

    dead_email = False
    if enable_mx:
        domain = email.split("@", 1)[1]
        cached = get_network_cache(domain, "MX")
        if cached is not None:
            dead_email = not cached.get("has_mx", False)
            return (email, dead_email) if not dead_email else (None, True)

        has_mx = False
        try:
            GLOBAL_MX_RESOLVER.lifetime = mx_timeout
            GLOBAL_MX_RESOLVER.timeout = mx_timeout
            attempts = 0
            while attempts <= mx_retries:
                try:
                    GLOBAL_MX_RESOLVER.resolve(domain, "MX")
                    has_mx = True
                    break
                except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
                    break
                except dns.exception.Timeout:
                    attempts += 1
                    if attempts > mx_retries:
                        break
                    time.sleep(0.5 * attempts)
                except Exception:
                    break
            dead_email = not has_mx
            set_network_cache(domain, "MX", {"has_mx": has_mx})
        except Exception:
            dead_email = False

    return email, dead_email


def extract_data_pure(html_content, base_url):
    emails, mobiles, landlines = set(), set(), set()
    socials = {"LinkedIn": "", "Facebook": "", "Twitter": "", "Instagram": ""}
    if not html_content:
        return emails, mobiles, landlines, set(), socials, {}

    soup = BeautifulSoup(html_content, "html.parser")
    semantic_keywords = ["contact", "about", "reach", "enquiry", "touch", "location"]
    internal_links = set()

    for a in soup.find_all("a", href=True):
        href_raw = a["href"]
        href = href_raw.strip().lower()
        if "linkedin.com/company" in href or "linkedin.com/in" in href:
            socials["LinkedIn"] = href_raw
        elif "facebook.com" in href and "sharer" not in href:
            socials["Facebook"] = href_raw
        elif "twitter.com" in href or "x.com" in href:
            socials["Twitter"] = href_raw
        elif "instagram.com" in href:
            socials["Instagram"] = href_raw

        if href.startswith("mailto:"):
            e_raw = href_raw.split(":", 1)[1].split("?")[0]
            e, dead = validate_email(e_raw)
            if e:
                emails.add(e)
        elif any(kw in href or kw in a.get_text().lower() for kw in semantic_keywords):
            if (
                "%20" in href
                or "contact-form-7" in href
                or href.startswith(("#", "javascript:", "tel:"))
            ):
                continue
            try:
                full_link = urljoin(base_url, href_raw)
                parsed_link = urlparse(full_link)
                if (
                    parsed_link.scheme in ["http", "https"]
                    and parsed_link.netloc == urlparse(base_url).netloc
                ):
                    internal_links.add(full_link)
            except Exception:
                pass

    for script in soup(["script", "style", "noscript"]):
        script.decompose()
    clean_text = soup.get_text(separator=" | ")

    for raw_email in EMAIL_REGEX.findall(clean_text):
        e, dead = validate_email(raw_email)
        if e:
            emails.add(e)

    phone_candidates = re.findall(r"[\+\d][\d\-\s\(\)\.]{6,}", clean_text)
    for raw_phone in phone_candidates:
        num, ptype = classify_scraped_phone(raw_phone)
        if ptype == "Mobile":
            mobiles.add(num)
        elif ptype == "Landline":
            landlines.add(num)

    mobile_friendly = bool(soup.find("meta", attrs={"name": "viewport"}))
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    meta_desc = bool(soup.find("meta", attrs={"name": "description"}))
    has_reviews = bool(re.search(r"\breview(s)?\b", clean_text, re.I)) or bool(
        soup.find(attrs={"itemtype": re.compile("Review", re.I)})
    )

    raw_extracted = {
        "title": title,
        "meta_description": meta_desc,
        "has_reviews": has_reviews,
    }

    return (
        emails,
        mobiles,
        landlines,
        internal_links,
        socials,
        {"mobile_friendly": mobile_friendly, "raw": raw_extracted},
    )


def null_byte_cleaner(file_obj):
    for line in file_obj:
        if "\0" in line:
            yield line.replace("\0", "")
        else:
            yield line


def clear_poison_pills():
    found_poison = 0
    current_time = time.time()
    for file in os.listdir(TARPIT_DIR):
        if file.endswith(".lock"):
            filepath = os.path.join(TARPIT_DIR, file)
            try:
                if current_time - os.path.getctime(filepath) > 300:
                    with open(filepath, "r", encoding="utf-8") as f:
                        poison_url = f.read().strip()
                    if poison_url:
                        mark_as_dead(poison_url)
                        found_poison += 1
                os.remove(filepath)
            except Exception:
                pass
    if found_poison > 0:
        logger.info(f"[SYSTEM] Purged {found_poison} Tarpit URLs.")


def load_blocklist():
    if os.path.exists(BLOCKLIST_FILE):
        with open(BLOCKLIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                domain = line.strip()
                if domain:
                    DEAD_SITES.add(domain)


def mark_as_dead(url):
    with BLOCKLIST_LOCK:
        if url not in DEAD_SITES:
            DEAD_SITES.add(url)
            with open(BLOCKLIST_FILE, "a", encoding="utf-8") as f:
                f.write(url + "\n")
                f.flush()
                os.fsync(f.fileno())


# --- SESSION MGMT ---
thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        retries = Retry(
            total=4, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {"User-Agent": random.choice(USER_AGENTS), "Connection": "close"}
        )
        session.verify = True  # enable SSL verification by default
        thread_local.session = session
    return thread_local.session


# --- WRITER PROCESS (multiprocessing) ---
def unified_writer_process(write_q, dynamic_headers):
    buffer_count = 0
    file_exists = (
        os.path.exists(SYNC_BUFFER_CSV) and os.path.getsize(SYNC_BUFFER_CSV) > 0
    )

    try:
        validate_final_schema(dynamic_headers)

        with open(SYNC_BUFFER_CSV, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f, fieldnames=FINAL_CSV_COLUMNS, extrasaction="raise"
            )

            if not file_exists:
                writer.writeheader()

            while True:
                try:
                    row = write_q.get(timeout=5)
                except Exception:
                    continue

                if row is None:
                    if buffer_count > 0:
                        f.flush()
                        os.fsync(f.fileno())
                    break

                try:
                    normalized_row = normalize_and_validate_final_row(row)
                except Exception as validation_error:
                    row_id = str(row.get("id", ""))
                    logger.error(
                        "[SCHEMA VALIDATION FAILED] "
                        f"row_id={row_id!r}: {validation_error}"
                    )
                    quarantine_invalid_row(row, validation_error)
                    continue

                writer.writerow(normalized_row)
                buffer_count += 1

                if buffer_count >= FLUSH_BATCH_SIZE:
                    f.flush()
                    os.fsync(f.fileno())
                    buffer_count = 0

    except Exception as e:
        logger.exception(f"[WRITER PROCESS ERROR] {e}")


# --- NETWORK EVALUATION HANDLERS ---
def rdap_domain_state(domain):
    try:
        rdap_host = "rdap.org"
        url = f"https://{rdap_host}/domain/{domain}"
        headers = {"Host": rdap_host, "User-Agent": random.choice(USER_AGENTS)}
        resp = requests.get(url, headers=headers, timeout=5, verify=True)
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", [])
            if isinstance(status, list):
                status_lower = [s.lower() for s in status]
                if any(
                    "clienthold" in s or "serverhold" in s or "inactive" in s
                    for s in status_lower
                ):
                    return "suspended"
            events = data.get("events", [])
            for ev in events:
                if ev.get("eventAction") == "expiration":
                    when = ev.get("eventDate")
                    if when:
                        try:
                            exp = datetime.fromisoformat(when.replace("Z", "+00:00"))
                            if exp < datetime.now(timezone.utc):
                                return "expired"
                        except Exception:
                            pass
            return "exists"
        else:
            return "unknown"
    except Exception:
        return "unknown"


def whois_domain_state(domain):
    try:
        w = whois.whois(domain)
        exp = w.expiration_date
        if isinstance(exp, list):
            exp = exp[0]
        if exp:
            if isinstance(exp, str):
                try:
                    exp_dt = datetime.fromisoformat(exp)
                except Exception:
                    return "unknown"
            else:
                exp_dt = exp
            if exp_dt and exp_dt < datetime.now():
                return "expired"
            else:
                return "exists"
        if w.text and ("parked" in w.text.lower() or "for sale" in w.text.lower()):
            return "parked"
        return "unknown"
    except Exception:
        return "unknown"


def classify_domain_state(domain):
    if not domain:
        return "unknown"
    cached = get_network_cache(domain, "DOMAIN_STATE")
    if cached:
        return cached.get("domain_state", "unknown")

    state = rdap_domain_state(domain)
    if not state or state == "unknown":
        state = whois_domain_state(domain)

    set_network_cache(domain, "DOMAIN_STATE", {"domain_state": state})
    return state


def classify_ssl(url):
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return "invalid"

        cached = get_network_cache(hostname, "SSL")
        if cached:
            return cached.get("ssl_status", "invalid")

        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        context = ssl.create_default_context()
        ssl_status = "invalid"
        with socket.create_connection((hostname, port), timeout=5) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()
                not_after = cert.get("notAfter")
                if not_after:
                    try:
                        exp = datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z")
                    except Exception:
                        try:
                            exp = datetime.strptime(not_after, "%b %d %H:%M:%S %Y")
                        except Exception:
                            exp = None
                    if exp and exp < datetime.utcnow():
                        ssl_status = "expired"
                    else:
                        ssl_status = "valid"
                else:
                    ssl_status = "valid"

        set_network_cache(hostname, "SSL", {"ssl_status": ssl_status})
        return ssl_status
    except Exception:
        return "invalid"


def compute_quality_score(
    http_status, has_contact, ssl_status, mobile, social_reviews, perf_seo
):
    score = 0
    w = QUALITY_SCORE_WEIGHTS
    reach = 1 if (http_status and 200 <= int(http_status) < 400) else 0
    score += int(w["reachability"] * reach)
    score += int(w["contact"] * (1 if has_contact else 0))
    score += int(w["ssl"] * (1 if ssl_status == "valid" else 0))
    score += int(w["mobile"] * (1 if mobile else 0))
    score += int(w["social_reviews"] * (1 if social_reviews else 0))
    score += int(w["perf_seo"] * (1 if perf_seo else 0))
    return max(0, min(100, score))


# --- ONLINE WORKER ---
def process_row_online(row, pw_queue, write_q, write_queue_watchdog):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row["enriched_at"] = now_utc
    row["enrichment_version"] = CURRENT_VERSION

    url_raw = row.get("website", "").strip()
    if not url_raw:
        row.setdefault("is_valid", True)
        row.setdefault("lead_status", row.get("lead_status", ""))
        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
        row.setdefault("last_updated_date", now_utc)
        row["Data_Status"] = "Ghost"
        row["http_status"] = ""
        row["domain_state"] = ""
        row["ssl_status"] = ""
        row["mobile_friendly"] = False
        row["has_contact_info"] = False
        row["quality_score"] = 0
        row["tags"] = json.dumps([])
        row["last_checked_at"] = now_utc
        row["raw_extracted"] = json.dumps({})
        write_q.put(row)
        return

    url = normalize_url(url_raw)
    url_hash = get_cache_hash(url)
    lock_path = os.path.join(TARPIT_DIR, f"{url_hash}.lock")
    cache_path = os.path.join(HTML_CACHE_DIR, f"{url_hash}.html")

    try:
        with open(lock_path, "w", encoding="utf-8") as f:
            f.write(url)
    except Exception:
        pass

    session = get_session()
    row_tags = []
    http_status = None
    ssl_status = ""
    domain_state = ""
    mobile_friendly_flag = False
    raw_extracted_payload = {}

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        http_status = resp.status_code

        try:
            ssl_status = classify_ssl(url)
            if ssl_status and ssl_status != "valid":
                row_tags.append(f"ssl:{ssl_status}")
        except Exception:
            ssl_status = "invalid"

        try:
            domain = urlparse(url).hostname
            domain_state = classify_domain_state(domain) or "unknown"
            if domain_state != "exists":
                row_tags.append(f"domain:{domain_state}")
        except Exception:
            domain_state = "unknown"

        if resp.status_code == 404:
            mark_as_dead(url)
            row["lead_status"] = "Dead"
            has_stage3_contact = (row.get("Phone_Type") == "Mobile") or bool(
                row.get("Emails", "").strip()
            )
            row["Data_Status"] = "Fallback" if has_stage3_contact else "Ghost"
            row.setdefault("is_valid", True)
            row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
            row.setdefault("last_updated_date", now_utc)
            row["http_status"] = 404
            row["domain_state"] = domain_state
            row["ssl_status"] = ssl_status
            row["mobile_friendly"] = False
            row["has_contact_info"] = False
            row["quality_score"] = 0
            row["tags"] = json.dumps(row_tags)
            row["last_checked_at"] = now_utc
            row["raw_extracted"] = json.dumps({})
            write_q.put(row)
            return

        if (
            resp.status_code in [403, 401, 406, 429]
            or 'id="root"' in resp.text.lower()
            or "wix" in resp.text.lower()
            or "nuxt" in resp.text.lower()
        ):
            task = {"row": row, "url": url, "lock": lock_path, "cache": cache_path}
            enqueue_with_backpressure(pw_queue, task, write_q, write_queue_watchdog)
            return

        try:
            with open(cache_path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(resp.text)
        except Exception:
            pass

        emails, mobiles, landlines, contact_links, socials, meta = extract_data_pure(
            resp.text, url
        )
        raw_extracted_payload = meta.get("raw", {})

        if (not emails or not mobiles) and contact_links:
            for link in list(contact_links)[:2]:
                try:
                    c_resp = session.get(link, timeout=(10, 20))
                    ce, cm, cl, _, cs, cmeta = extract_data_pure(c_resp.text, url)
                    emails.update(ce)
                    mobiles.update(cm)
                    landlines.update(cl)
                    raw_extracted_payload.update(cmeta.get("raw", {}))
                    for key, val in cs.items():
                        if val and not socials.get(key):
                            socials[key] = val
                    if emails and mobiles:
                        break
                except Exception:
                    continue

        if not emails and not mobiles and not landlines:
            task = {"row": row, "url": url, "lock": lock_path, "cache": cache_path}
            enqueue_with_backpressure(pw_queue, task, write_q, write_queue_watchdog)
            return

        existing_emails = [
            e.strip() for e in str(row.get("Emails", "")).split(",") if e.strip()
        ]
        emails.update(existing_emails)

        normalized_mobiles = set()
        normalized_landlines = set()
        for m in mobiles:
            num, ptype = classify_scraped_phone(m)
            if num and ptype == "Mobile":
                normalized_mobiles.add(num)
        for l in landlines:
            num, ptype = classify_scraped_phone(l)
            if num and ptype == "Landline":
                normalized_landlines.add(num)

        row["Emails"] = ", ".join(sorted(emails))
        row["Alternative_Phones"] = ", ".join(
            sorted(normalized_mobiles) + sorted(normalized_landlines)
        )
        row["Found_Web_Mobile"] = "Yes" if normalized_mobiles else "No"
        for k, v in socials.items():
            if v:
                row[k] = v

        row.setdefault("is_valid", True)
        row.setdefault("lead_status", row.get("lead_status", ""))
        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
        row.setdefault("last_updated_date", now_utc)

        has_genuine_mobile = bool(normalized_mobiles) or (
            row.get("Phone_Type") == "Mobile"
        )
        has_email = bool(emails)
        row["Data_Status"] = "Premium" if (has_genuine_mobile or has_email) else "Ghost"

        mobile_friendly_flag = bool(meta.get("mobile_friendly", False))
        has_contact_info = bool(emails or normalized_mobiles or normalized_landlines)
        tags = row_tags.copy()
        if socials and any(socials.values()):
            tags.append("has_social_links")
        if meta.get("raw", {}).get("has_reviews"):
            tags.append("has_reviews")

        quality_score = compute_quality_score(
            http_status=resp.status_code,
            has_contact=has_contact_info,
            ssl_status=ssl_status,
            mobile=mobile_friendly_flag,
            social_reviews=("has_social_links" in tags or "has_reviews" in tags),
            perf_seo=False,
        )

        row["http_status"] = resp.status_code
        row["domain_state"] = domain_state
        row["ssl_status"] = ssl_status
        row["mobile_friendly"] = mobile_friendly_flag
        row["has_contact_info"] = has_contact_info
        row["quality_score"] = quality_score
        row["tags"] = json.dumps(tags)
        row["last_checked_at"] = now_utc
        row["raw_extracted"] = json.dumps(raw_extracted_payload)

        write_q.put(row)

    except requests.exceptions.RequestException as e:
        logger.debug(f"[HTTP ERROR] {e} for {url}")
        row["lead_status"] = "Dead"
        has_stage3_contact = (row.get("Phone_Type") == "Mobile") or bool(
            row.get("Emails", "").strip()
        )
        row["Data_Status"] = "Fallback" if has_stage3_contact else "Ghost"
        row.setdefault("is_valid", True)
        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
        row.setdefault("last_updated_date", now_utc)
        row["http_status"] = (
            getattr(e.response, "status_code", None) if hasattr(e, "response") else None
        )
        row["domain_state"] = domain_state or ""
        row["ssl_status"] = ssl_status or ""
        row["mobile_friendly"] = mobile_friendly_flag
        row["has_contact_info"] = False
        row["quality_score"] = 0
        row["tags"] = json.dumps(row_tags)
        row["last_checked_at"] = now_utc
        row["raw_extracted"] = json.dumps(raw_extracted_payload)
        write_q.put(row)
        return
    except Exception as e:
        logger.exception(f"[ONLINE WORKER ERROR] {e}")
        has_stage3_contact = (row.get("Phone_Type") == "Mobile") or bool(
            row.get("Emails", "").strip()
        )
        row["Data_Status"] = "Fallback" if has_stage3_contact else "Ghost"
        row.setdefault("is_valid", True)
        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
        row.setdefault("last_updated_date", now_utc)
        row["http_status"] = http_status or ""
        row["domain_state"] = domain_state or ""
        row["ssl_status"] = ssl_status or ""
        row["mobile_friendly"] = mobile_friendly_flag
        row["has_contact_info"] = False
        row["quality_score"] = 0
        row["tags"] = json.dumps(row_tags)
        row["last_checked_at"] = now_utc
        row["raw_extracted"] = json.dumps(raw_extracted_payload)
        write_q.put(row)
        return
    finally:
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except Exception:
                pass


def enqueue_with_backpressure(
    pw_queue, task, write_q, write_queue_watchdog, max_attempts=5, base_delay=0.5
):
    attempts = 0
    while attempts < max_attempts:
        try:
            pw_queue.put(task, block=True, timeout=2)
            return
        except Exception:
            attempts += 1
            time.sleep(base_delay * (2 ** (attempts - 1)))
            logger.warning(
                f"[BACKPRESSURE] pw_queue full, retry {attempts}/{max_attempts}"
            )
            if write_queue_watchdog and not write_queue_watchdog.is_alive():
                logger.warning(
                    "[WATCHDOG] Writer process not alive; attempting restart"
                )
                break
    logger.error(
        "[BACKPRESSURE] Failed to enqueue to pw_queue after retries; aborting run to avoid data loss"
    )
    os._exit(1)


# --- PLAYWRIGHT WORKER ---
def playwright_worker(task_queue, write_q):
    while True:
        try:
            first_task = task_queue.get(timeout=3)
        except Exception:
            continue

        if first_task is None:
            break

        shutdown_flag = False

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = browser.new_context(user_agent=random.choice(USER_AGENTS))
                context.on("dialog", lambda dialog: dialog.dismiss())

                tasks_to_process = [first_task]
                while len(tasks_to_process) < MAX_URLS_PER_BROWSER:
                    try:
                        t = task_queue.get_nowait()
                        if t is None:
                            shutdown_flag = True
                            break
                        tasks_to_process.append(t)
                    except Empty:
                        break

                for task in tasks_to_process:
                    row = task["row"]
                    url = task["url"]
                    lock_path = task["lock"]
                    cache_path = task["cache"]

                    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    row["enriched_at"] = now_utc
                    row["enrichment_version"] = CURRENT_VERSION

                    page = context.new_page()
                    page.route(
                        "**/*",
                        lambda route: (
                            route.abort()
                            if route.request.resource_type in ["image", "media", "font"]
                            else route.continue_()
                        ),
                    )
                    raw_extracted_payload = {}
                    try:
                        nav_attempts = 0
                        nav_success = False
                        while nav_attempts < 2 and not nav_success:
                            try:
                                page.goto(
                                    url,
                                    timeout=PLAYWRIGHT_TIMEOUT,
                                    wait_until="domcontentloaded",
                                )
                                time.sleep(2)
                                nav_success = True
                            except Exception:
                                nav_attempts += 1
                                time.sleep(1)
                        if not nav_success:
                            raise Exception(
                                "Playwright navigation failed after retries"
                            )

                        html = page.content()
                        with open(
                            cache_path, "w", encoding="utf-8", errors="ignore"
                        ) as f:
                            f.write(html)
                        emails, mobiles, landlines, _, socials, meta = (
                            extract_data_pure(html, url)
                        )
                        raw_extracted_payload = meta.get("raw", {})

                        existing_emails = [
                            e.strip()
                            for e in str(row.get("Emails", "")).split(",")
                            if e.strip()
                        ]
                        emails.update(existing_emails)

                        normalized_mobiles = set()
                        normalized_landlines = set()
                        for m in mobiles:
                            num, ptype = classify_scraped_phone(m)
                            if num and ptype == "Mobile":
                                normalized_mobiles.add(num)
                        for l in landlines:
                            num, ptype = classify_scraped_phone(l)
                            if num and ptype == "Landline":
                                normalized_landlines.add(num)

                        row["Emails"] = ", ".join(sorted(emails))
                        row["Alternative_Phones"] = ", ".join(
                            sorted(normalized_mobiles) + sorted(normalized_landlines)
                        )
                        row["Found_Web_Mobile"] = "Yes" if normalized_mobiles else "No"
                        row.update(socials)

                        row.setdefault("is_valid", True)
                        row.setdefault("lead_status", row.get("lead_status", ""))
                        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
                        row.setdefault("last_updated_date", now_utc)

                        has_genuine_mobile = bool(normalized_mobiles) or (
                            row.get("Phone_Type") == "Mobile"
                        )
                        has_email = bool(emails)
                        row["Data_Status"] = (
                            "Premium" if (has_genuine_mobile or has_email) else "Ghost"
                        )

                        mobile_friendly_flag = bool(meta.get("mobile_friendly", False))
                        has_contact_info = bool(
                            emails or normalized_mobiles or normalized_landlines
                        )
                        tags = []
                        if socials and any(socials.values()):
                            tags.append("has_social_links")
                        if meta.get("raw", {}).get("has_reviews"):
                            tags.append("has_reviews")
                        ssl_status = classify_ssl(url)
                        domain = urlparse(url).hostname
                        domain_state = classify_domain_state(domain)
                        quality_score = compute_quality_score(
                            http_status=200,
                            has_contact=has_contact_info,
                            ssl_status=ssl_status,
                            mobile=mobile_friendly_flag,
                            social_reviews=(
                                "has_social_links" in tags or "has_reviews" in tags
                            ),
                            perf_seo=False,
                        )

                        row["http_status"] = 200
                        row["domain_state"] = domain_state
                        row["ssl_status"] = ssl_status
                        row["mobile_friendly"] = mobile_friendly_flag
                        row["has_contact_info"] = has_contact_info
                        row["quality_score"] = quality_score
                        row["tags"] = json.dumps(tags)
                        row["last_checked_at"] = now_utc
                        row["raw_extracted"] = json.dumps(raw_extracted_payload)

                    except Exception as e:
                        logger.exception(f"[PLAYWRIGHT WORKER ERROR] {e}")
                        has_stage3_contact = (
                            row.get("Phone_Type") == "Mobile"
                        ) or bool(row.get("Emails", "").strip())
                        row["Data_Status"] = (
                            "Fallback" if has_stage3_contact else "Ghost"
                        )
                        row.setdefault("is_valid", True)
                        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
                        row.setdefault("last_updated_date", now_utc)
                        row["http_status"] = ""
                        row["domain_state"] = ""
                        row["ssl_status"] = ""
                        row["mobile_friendly"] = False
                        row["has_contact_info"] = False
                        row["quality_score"] = 0
                        row["tags"] = json.dumps([])
                        row["last_checked_at"] = now_utc
                        row["raw_extracted"] = json.dumps(raw_extracted_payload)
                    finally:
                        write_q.put(row)
                        try:
                            page.close()
                        except Exception:
                            pass
                        if os.path.exists(lock_path):
                            try:
                                os.remove(lock_path)
                            except Exception:
                                pass

                context.close()
                browser.close()
                gc.collect()
        except Exception as e:
            logger.exception(f"[PLAYWRIGHT WORKER CRITICAL] {e}")
        if shutdown_flag:
            break


# --- OFFLINE WORKER ---
def process_offline_row(row, cache_path):
    try:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row["enriched_at"] = now_utc
        row["enrichment_version"] = CURRENT_VERSION

        try:
            if os.path.exists(cache_path):
                with open(cache_path, "r", encoding="utf-8", errors="ignore") as cf:
                    first_line = cf.readline()
                    if first_line.startswith("<!--"):
                        html = cf.read()
                    else:
                        cf.seek(0)
                        html = cf.read()
            else:
                html = ""
        except Exception:
            html = ""

        row.setdefault("is_valid", True)
        row.setdefault("lead_status", row.get("lead_status", ""))
        row.setdefault("first_seen_date", row.get("Scraped_Date", ""))
        row.setdefault("last_updated_date", now_utc)

        emails, mobiles, landlines, _, socials, meta = extract_data_pure(
            html, row.get("website", "")
        )
        existing_emails = [
            e.strip() for e in str(row.get("Emails", "")).split(",") if e.strip()
        ]
        emails.update(existing_emails)

        normalized_mobiles = set()
        normalized_landlines = set()
        for m in mobiles:
            num, ptype = classify_scraped_phone(m)
            if num and ptype == "Mobile":
                normalized_mobiles.add(num)
        for l in landlines:
            num, ptype = classify_scraped_phone(l)
            if num and ptype == "Landline":
                normalized_landlines.add(num)

        row["Emails"] = ", ".join(sorted(emails))
        row["Alternative_Phones"] = ", ".join(
            sorted(normalized_mobiles) + sorted(normalized_landlines)
        )
        row["Found_Web_Mobile"] = "Yes" if normalized_mobiles else "No"
        row.update(socials)
        row["reviews"] = extract_safe_integer(row.get("reviews"))

        has_genuine_mobile = bool(normalized_mobiles) or (
            row.get("Phone_Type") == "Mobile"
        )
        has_email = bool(emails)
        row["Data_Status"] = "Premium" if (has_genuine_mobile or has_email) else "Ghost"

        mobile_friendly_flag = bool(meta.get("mobile_friendly", False))
        has_contact_info = bool(emails or normalized_mobiles or normalized_landlines)
        ssl_status = classify_ssl(row.get("website", "")) if row.get("website") else ""
        domain = urlparse(row.get("website", "")).hostname if row.get("website") else ""
        domain_state = classify_domain_state(domain) if domain else "unknown"
        tags = []
        if socials and any(socials.values()):
            tags.append("has_social_links")
        if meta.get("raw", {}).get("has_reviews"):
            tags.append("has_reviews")
        quality_score = compute_quality_score(
            http_status=200,
            has_contact=has_contact_info,
            ssl_status=ssl_status,
            mobile=mobile_friendly_flag,
            social_reviews=("has_social_links" in tags or "has_reviews" in tags),
            perf_seo=False,
        )

        row["http_status"] = 200
        row["domain_state"] = domain_state
        row["ssl_status"] = ssl_status
        row["mobile_friendly"] = mobile_friendly_flag
        row["has_contact_info"] = has_contact_info
        row["quality_score"] = quality_score
        row["tags"] = json.dumps(tags)
        row["last_checked_at"] = now_utc
        row["raw_extracted"] = json.dumps(meta.get("raw", {}))

        return row
    except Exception:
        row["Data_Status"] = "Ghost"
        return row


# --- MASTER EXECUTION ---
def main():
    print("=" * 60)
    print(" STAGE 4: EMAIL & CONTACT EXTRACTOR (PHASE ISOLATION CORE) ")
    print("=" * 60)

    if not os.path.exists(INPUT_CSV) or os.path.getsize(INPUT_CSV) < 100:
        logger.info(
            f"[SYSTEM] Buffer missing or empty (< 100 bytes): {INPUT_CSV}. Graceful Exit."
        )
        sys.exit(0)

    load_blocklist()
    clear_poison_pills()
    init_network_cache_db()

    counters = {
        "total_rows": 0,
        "skipped_by_db_version": 0,
        "skipped_by_dedup": 0,
        "offline_cached_rows": 0,
        "temp_online_rows": 0,
        "pw_tasks_enqueued": 0,
        "http_processed": 0,
        "playwright_processed": 0,
        "writer_puts": 0,
        "quarantined": 0,
    }

    # Query PostgreSQL for existing completed IDs
    completed_ids = set()
    try:
        logger.info(
            "Querying PostgreSQL for completed IDs (enrichment_version >= CURRENT_VERSION)..."
        )
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, enrichment_version FROM master_leads WHERE id IS NOT NULL;"
        )
        for r in cursor.fetchall():
            try:
                rid = str(r[0])
                ver = int(r[1]) if r[1] is not None else None
                if ver is not None and ver >= CURRENT_VERSION:
                    completed_ids.add(rid)
            except Exception:
                continue
        conn.close()
    except Exception as e:
        logger.error(f"Failed to connect to Postgres. Error: {e}")
        logger.error("Fail-fast enabled: aborting run due to DB connection failure.")
        sys.exit(1)

    # Local Resume Protection: Read SYNC_BUFFER_CSV and add ALL existing IDs
    if os.path.exists(SYNC_BUFFER_CSV):
        try:
            with open(SYNC_BUFFER_CSV, "r", encoding="utf-8-sig", newline="") as f:
                existing_reader = csv.DictReader(f)
                existing_headers = list(existing_reader.fieldnames or [])
                validate_final_schema(existing_headers)
                for r in existing_reader:
                    vid = str(r.get("id", "")).strip()
                    if vid:
                        completed_ids.add(vid)
        except Exception as e:
            logger.error(f"Failed to read local SYNC_BUFFER_CSV for resume: {e}")

    logger.info(
        f"[SYSTEM] Total Completed IDs found (Postgres + Sync Buffer): {len(completed_ids)}"
    )

    run_id = uuid.uuid4().hex
    dedup_conn, dedup_db_path = init_dedup_db(run_id)
    logger.info(f"[SYSTEM] Dedup DB initialized at {dedup_db_path}")

    write_queue = multiprocessing.Queue(maxsize=10000)
    pw_queue = multiprocessing.Queue(maxsize=PW_QUEUE_MAXSIZE)

    logger.info("[PHASE 1] Scanning for Cached Files (CPU Bound)...")

    offline_batch = []
    writer_proc = None

    if not os.path.exists(QUARANTINE_CSV):
        with open(QUARANTINE_CSV, "w", newline="", encoding="utf-8") as qf:
            qwriter = csv.writer(qf)
            qwriter.writerow(["reason", "row_json"])

    with open(INPUT_CSV, "r", encoding="utf-8-sig") as f_in, open(
        TEMP_ONLINE_CSV, "w", newline="", encoding="utf-8"
    ) as f_temp:

        reader = csv.DictReader(null_byte_cleaner(f_in))
        processing_headers = list(reader.fieldnames or [])

        if processing_headers != BASE_PROCESSING_COLUMNS:
            raise RuntimeError(
                "Processing buffer schema mismatch.\n"
                f"Expected: {BASE_PROCESSING_COLUMNS}\n"
                f"Received: {processing_headers}"
            )

        # Preserve the processing-buffer columns exactly, then append the enrichment
        # columns in the fixed order required by the final PostgreSQL staging schema.
        dynamic_headers = list(processing_headers)
        for col in STAGE_4_COLUMNS:
            if col not in dynamic_headers:
                dynamic_headers.append(col)
        for col in NEW_COLUMNS:
            if col not in dynamic_headers:
                dynamic_headers.append(col)

        validate_final_schema(dynamic_headers)

        writer_proc = multiprocessing.Process(
            target=unified_writer_process,
            args=(write_queue, dynamic_headers),
            daemon=False,
        )
        writer_proc.start()
        write_queue_watchdog = writer_proc

        temp_writer = csv.DictWriter(
            f_temp, fieldnames=dynamic_headers, extrasaction="ignore"
        )
        temp_writer.writeheader()

        with ProcessPoolExecutor(max_workers=OFFLINE_WORKERS) as cpu_executor:
            for row in reader:
                counters["total_rows"] += 1
                row_id = str(row.get("id", "")).strip()
                if not row_id:
                    website = row.get("website", "").strip()
                    if website:
                        nid = get_cache_hash(normalize_url(website))
                        row_id = nid
                        row["id"] = row_id
                    else:
                        with open(
                            QUARANTINE_CSV, "a", newline="", encoding="utf-8"
                        ) as qf:
                            qwriter = csv.writer(qf)
                            qwriter.writerow(
                                ["missing_id_and_no_website", json.dumps(row)]
                            )
                        counters["quarantined"] += 1
                        continue

                if row_id in completed_ids:
                    counters["skipped_by_db_version"] += 1
                    continue
                if is_id_seen(dedup_conn, row_id):
                    counters["skipped_by_dedup"] += 1
                    continue
                completed_ids.add(row_id)
                mark_id_seen(dedup_conn, row_id)

                row.update(
                    {
                        "Emails": "",
                        "Alternative_Phones": "",
                        "LinkedIn": "",
                        "Facebook": "",
                        "Twitter": "",
                        "Instagram": "",
                        "Data_Status": "",
                        "Found_Web_Mobile": "No",
                        "is_valid": True,
                        "lead_status": row.get("lead_status", ""),
                        "first_seen_date": row.get("Scraped_Date", ""),
                        "last_updated_date": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "Dead_Email": False,
                        "enriched_at": "",
                        "enrichment_version": "",
                        "http_status": "",
                        "domain_state": "",
                        "ssl_status": "",
                        "mobile_friendly": False,
                        "has_contact_info": False,
                        "quality_score": 0,
                        "tags": json.dumps([]),
                        "last_checked_at": "",
                        "raw_extracted": json.dumps({}),
                    }
                )
                clean_web_url = row.get("website", "").strip()

                if not clean_web_url or clean_web_url in DEAD_SITES:
                    has_stage3_contact = (row.get("Phone_Type") == "Mobile") or bool(
                        row.get("Emails", "").strip()
                    )
                    row["Data_Status"] = "Fallback" if has_stage3_contact else "Ghost"
                    row["enriched_at"] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    row["enrichment_version"] = CURRENT_VERSION
                    write_queue.put(row)
                    counters["writer_puts"] += 1
                    continue

                if not clean_web_url.startswith("http"):
                    clean_web_url = "https://" + clean_web_url

                url_hash = get_cache_hash(clean_web_url)
                cache_path = os.path.join(HTML_CACHE_DIR, f"{url_hash}.html")

                if os.path.exists(cache_path):
                    offline_batch.append((row, cache_path))
                    counters["offline_cached_rows"] += 1
                    if len(offline_batch) >= OFFLINE_BATCH_SIZE:
                        futures = [
                            cpu_executor.submit(process_offline_row, t[0], t[1])
                            for t in offline_batch
                        ]
                        for future in as_completed(futures):
                            try:
                                res = future.result()
                                if res is not None:
                                    write_queue.put(res)
                                    counters["writer_puts"] += 1
                            except Exception as e:
                                logger.error(f"[CPU WORKER ERROR] {e}")
                        offline_batch.clear()
                        gc.collect()
                else:
                    temp_writer.writerow(row)
                    counters["temp_online_rows"] += 1

            if offline_batch:
                futures = [
                    cpu_executor.submit(process_offline_row, t[0], t[1])
                    for t in offline_batch
                ]
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        if res is not None:
                            write_queue.put(res)
                            counters["writer_puts"] += 1
                    except Exception as e:
                        logger.error(f"[CPU WORKER ERROR] {e}")
                offline_batch.clear()
                gc.collect()

    logger.info("✅ [PHASE 1 COMPLETE] All cached files processed. CPU pool closed.")

    # PHASE 2: Online Processing
    logger.info("[PHASE 2] Booting Network Threads and Processors for live scraping...")

    pw_processes = []
    for _ in range(PLAYWRIGHT_WORKERS):
        p = multiprocessing.Process(
            target=playwright_worker, args=(pw_queue, write_queue), daemon=False
        )
        p.start()
        pw_processes.append(p)

    import queue

    online_queue = queue.Queue(maxsize=10000)

    def network_consumer():
        while True:
            row = online_queue.get()
            if row is None:
                online_queue.task_done()
                break
            try:
                process_row_online(row, pw_queue, write_queue, write_queue_watchdog)
                counters["http_processed"] += 1
            except Exception:
                logger.exception("[NETWORK CONSUMER ERROR]")
            online_queue.task_done()

    http_threads = []
    for _ in range(ONLINE_WORKERS):
        t = threading.Thread(target=network_consumer, daemon=True)
        t.start()
        http_threads.append(t)

    if os.path.exists(TEMP_ONLINE_CSV):
        with open(TEMP_ONLINE_CSV, "r", encoding="utf-8") as f_temp:
            reader = csv.DictReader(f_temp)
            for row in reader:
                row_id = str(row.get("id", "")).strip()
                if row_id and (
                    row_id in completed_ids or is_id_seen(dedup_conn, row_id)
                ):
                    continue
                if row_id:
                    completed_ids.add(row_id)
                    mark_id_seen(dedup_conn, row_id)
                online_queue.put(row)

    logger.info("Queue loaded. Waiting for HTTP network threads to empty...")
    for _ in range(ONLINE_WORKERS):
        online_queue.put(None)
    for t in http_threads:
        t.join()

    logger.info("HTTP finished. Waiting for Playwright processes to empty...")
    for _ in range(PLAYWRIGHT_WORKERS):
        pw_queue.put(None)
    for p in pw_processes:
        p.join()

    logger.info("Playwright finished. Finalizing disk writes...")
    write_queue.put(None)
    writer_proc.join(timeout=30)
    if writer_proc.is_alive():
        logger.warning("[WRITER] Writer process did not exit in time; terminating")
        try:
            writer_proc.terminate()
            writer_proc.join(timeout=10)
        except Exception:
            pass

    if os.path.exists(TEMP_ONLINE_CSV):
        try:
            os.remove(TEMP_ONLINE_CSV)
        except Exception:
            pass

    try:
        dedup_conn.close()
        if dedup_db_path.exists():
            os.remove(dedup_db_path)
            logger.info(f"[SYSTEM] Removed ephemeral dedup DB {dedup_db_path}")
    except Exception:
        logger.exception("Failed to cleanup dedup DB")

    try:
        with open(RUN_SUMMARY_JSON, "w", encoding="utf-8") as rsf:
            json.dump(counters, rsf, indent=2)
    except Exception:
        pass

    logger.info("\n✅ [SUCCESS] Stage 4 Extraction Complete!")
    logger.info(f"[RUN SUMMARY] {json.dumps(counters)}")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
