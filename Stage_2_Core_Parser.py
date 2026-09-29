import os
import csv
import re
import sys
import json
import time
import hashlib
import sqlite3
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup
from lib import io_helpers as ioh

# Get the root directory (where this script is located)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ================= CONFIGURATION =================

CACHE_DIR = os.path.join(BASE_DIR, "Raw_HTML_Cache")
SYSTEM_TEMP_DIR = os.path.join(BASE_DIR, "System_Temp")
JSON_MAP_PATH = os.path.join(BASE_DIR, "query_map.json")

# The unified intermediate buffer for Stage 4
BUFFER_CSV = os.path.join(SYSTEM_TEMP_DIR, "processing_buffer.csv")
PROGRESS_LOG = os.path.join(SYSTEM_TEMP_DIR, "stage2_progress.log")

# Per-run deduplication database.
# This follows the same disk-backed SQLite approach used by Stage 4.
DEDUP_DB_PATH = os.path.join(SYSTEM_TEMP_DIR, f"stage2_dedup_{os.getpid()}.db")

os.makedirs(SYSTEM_TEMP_DIR, exist_ok=True)


# ================= PROCESSING BUFFER SCHEMA =================

HEADERS = [
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


# ================= CORE LOGIC & FILTERS =================


def classify_and_format_phone(raw_phone):
    if not raw_phone:
        return "", ""

    clean_num = re.sub(r"\D", "", str(raw_phone))

    if not clean_num:
        return "", ""

    while clean_num.startswith("0"):
        clean_num = clean_num[1:]

    while clean_num.startswith("91") and len(clean_num) > 10:
        clean_num = clean_num[2:]

    if clean_num.startswith("18"):
        return "", ""

    if len(clean_num) == 10 and clean_num[0] in "6789":
        return clean_num, "Mobile"

    if len(clean_num) == 10 and clean_num[0] in "12345":
        return "0" + clean_num, "Landline"

    return clean_num, "Unknown"


# ================= ORDER ID NORMALIZATION =================


def normalize_order_ids(value):
    """
    Normalize every supported order_ids representation into the
    PostgreSQL-compatible array literal used by the downstream pipeline.

    Supported input:
        None
        ""
        []
        ()
        set()
        list / tuple / set
        string representation of a Python collection
        single scalar value

    Output:
        "{}"
        "{value}"
        "{value1,value2,...}"
    """

    if value is None:
        return "{}"

    if isinstance(value, str):
        raw = value.strip()

        if not raw or raw.lower() in {"none", "null", "nan"}:
            return "{}"

        # Existing PostgreSQL array representation.
        if raw.startswith("{") and raw.endswith("}"):
            inner = raw[1:-1].strip()

            if not inner:
                return "{}"

            values = [
                item.strip().strip('"').replace('"', '""')
                for item in inner.split(",")
                if item.strip()
            ]

            return "{" + ",".join(values) + "}" if values else "{}"

        # Python-style collection representation.
        if (
            (raw.startswith("[") and raw.endswith("]"))
            or (raw.startswith("(") and raw.endswith(")"))
            or (raw.startswith("{") and raw.endswith("}"))
        ):
            try:
                import ast

                parsed = ast.literal_eval(raw)

                if isinstance(parsed, (list, tuple, set)):
                    value = parsed
                elif isinstance(parsed, dict):
                    value = list(parsed.keys())
                else:
                    value = [parsed]

            except (ValueError, SyntaxError):
                value = [raw]

        else:
            value = [raw]

    elif isinstance(value, dict):
        value = list(value.keys())

    elif isinstance(value, (list, tuple, set)):
        value = list(value)

    else:
        value = [value]

    normalized = []

    for item in value:
        if item is None:
            continue

        item = str(item).strip()

        if not item:
            continue

        if item.lower() in {"none", "null", "nan"}:
            continue

        # PostgreSQL array element escaping.
        item = item.replace("\\", "\\\\")
        item = item.replace('"', '\\"')
        item = item.replace("{", "\\{")
        item = item.replace("}", "\\}")

        normalized.append(item)

    if not normalized:
        return "{}"

    return "{" + ",".join(normalized) + "}"


# ================= CSV VALUE NORMALIZATION =================


def normalize_csv_text(value):
    """
    Remove physical record-breaking characters while preserving the
    actual field value.
    """

    if value is None:
        return ""

    if isinstance(value, str):
        return value.replace("\x00", "").replace("\r", " ").replace("\n", " ")

    return value


def normalize_lead_for_buffer(lead):
    """
    Final schema boundary before a lead reaches csv.DictWriter.

    This does NOT change the existing extraction logic.
    It only guarantees that the dictionary returned by parse_card()
    conforms to the processing-buffer contract.
    """

    normalized = {}

    for column in HEADERS:
        value = lead.get(column, "")

        if column == "order_ids":
            value = normalize_order_ids(value)

        elif column == "rating":
            if value is None or str(value).strip() == "":
                value = "0"

            try:
                value = float(str(value).strip())
            except (ValueError, TypeError):
                value = 0

        elif column == "reviews":
            if value is None or str(value).strip() == "":
                value = 0

            try:
                value = int(float(str(value).replace(",", "").strip()))
            except (ValueError, TypeError):
                value = 0

        elif column == "Scraped_Date":
            value = normalize_csv_text(value)

        else:
            value = normalize_csv_text(value)

        normalized[column] = value

    return normalized


# ================= DEDUPLICATION =================


def init_dedup_db():
    """
    Same disk-backed SQLite deduplication strategy used by Stage 4.
    """

    conn = sqlite3.connect(
        DEDUP_DB_PATH,
        timeout=30,
        isolation_level=None,
    )

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            id TEXT PRIMARY KEY,
            seen_at TEXT
        );
        """)

    return conn


def mark_id_seen(conn, row_id):
    """
    Same insertion strategy used by Stage 4.
    """

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO seen (id, seen_at)
            VALUES (?, ?);
            """,
            (row_id, now),
        )

    except sqlite3.OperationalError:
        for _ in range(3):
            time.sleep(0.1)

            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO seen (id, seen_at)
                    VALUES (?, ?);
                    """,
                    (row_id, now),
                )
                return

            except sqlite3.OperationalError:
                continue


def is_id_seen(conn, row_id):
    """
    Same lookup strategy used by Stage 4.
    """

    cur = conn.execute(
        "SELECT 1 FROM seen WHERE id = ? LIMIT 1;",
        (row_id,),
    )

    return cur.fetchone() is not None


def load_existing_buffer_ids(conn):
    """
    Load IDs already physically present in processing_buffer.csv.

    This prevents a previously written business from being written
    again when new HTML files contain the same business.
    """

    if not os.path.exists(BUFFER_CSV):
        return 0

    loaded = 0

    try:
        with open(
            BUFFER_CSV,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:

            reader = csv.DictReader(f)

            if not reader.fieldnames:
                return 0

            if "id" not in reader.fieldnames:
                raise RuntimeError(
                    "Existing processing_buffer.csv does not contain "
                    "the required 'id' column."
                )

            for row in reader:
                row_id = str(row.get("id", "")).strip()

                if not row_id:
                    continue

                if not is_id_seen(conn, row_id):
                    mark_id_seen(conn, row_id)
                    loaded += 1

    except Exception:
        raise

    return loaded


def deduplicate_batch(conn, leads):
    """
    Deduplicate the worker output in the main process.

    Workers remain completely independent and do not share SQLite
    connections. This avoids SQLite/process concurrency problems.
    """

    unique_leads = []
    duplicates = 0

    for lead in leads:
        row_id = str(lead.get("id", "")).strip()

        if not row_id:
            continue

        if is_id_seen(conn, row_id):
            duplicates += 1
            continue

        mark_id_seen(conn, row_id)
        unique_leads.append(lead)

    return unique_leads, duplicates


# ================= EXISTING CARD PARSER =================


def parse_card(card, metadata):
    full_text = card.get_text(" ", strip=True).lower()

    if "permanently closed" in full_text or "temporarily closed" in full_text:
        return None

    data = {
        "Search_Keyword": metadata.get(
            "Search_Keyword",
            "Unknown",
        ),
        "Search_Location": metadata.get(
            "Search_Location",
            "Unknown",
        ),
        "Scraped_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    link_tag = card.find(
        "a",
        class_="hfpxzc",
    )

    data["url"] = link_tag.get("href") if link_tag else ""

    data["website"] = ""

    website_tag = card.find(
        "a",
        attrs={"data-value": "Website"},
    )

    if website_tag and website_tag.get("href"):
        raw_web_url = website_tag.get("href")

        if raw_web_url.startswith("/url?q="):
            raw_web_url = raw_web_url.split("/url?q=")[1].split("&")[0]

        data["website"] = raw_web_url

    data["reviews"] = 0

    reviews_tag = card.find(
        "span",
        class_="UY7F9",
    )

    if reviews_tag:
        rev_str = (
            reviews_tag.get_text(strip=True)
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
        )

        data["reviews"] = int(rev_str) if rev_str.isdigit() else 0

    min_reviews = int(metadata.get("minimum_reviews", 0))

    if data["reviews"] < min_reviews:
        return None

    data["name"] = link_tag.get("aria-label") if link_tag else ""

    if not data["name"]:
        title_div = card.find(
            "div",
            class_="qBF1Pd",
        )

        if title_div:
            data["name"] = title_div.get_text(strip=True)

    rating_tag = card.find(
        "span",
        class_="MW4etd",
    )

    data["rating"] = rating_tag.get_text(strip=True) if rating_tag else "0"

    raw_phone = None

    phone_span = card.find(
        "span",
        class_="UsdlK",
    )

    if phone_span:
        raw_phone = phone_span.get_text(strip=True)

    else:
        phone_match = re.search(
            r"((\+?\d{1,4}[ -]?)?(\d{3,6}[ -]?\d{3,6}))",
            full_text,
        )

        if phone_match:
            raw_phone = phone_match.group(0)

    formatted_phone, phone_type = classify_and_format_phone(raw_phone)

    data["phone"] = formatted_phone
    data["Phone_Type"] = phone_type

    if not data["website"] and not data["phone"]:
        return None

    data["category"] = ""

    info_divs = card.find_all(
        "div",
        class_="W4Efsd",
    )

    for div in info_divs:
        spans = div.find_all(
            "span",
            recursive=False,
        )

        for span in spans:
            inner_text = span.get_text(strip=True)

            if (
                not inner_text
                or inner_text == "·"
                or "Open" in inner_text
                or "Close" in inner_text
            ):
                continue

            clean_text = inner_text.split("·")[0].strip()

            if len(clean_text) > 3 and not any(char.isdigit() for char in clean_text):
                data["category"] = clean_text
                break

        if data["category"]:
            break

    return data


# ================= WORKER PROCESS =================


def process_html_file(args):
    file_path, metadata = args

    if not os.path.exists(file_path):
        return [], metadata.get("hash_id")

    try:
        # Read header via helper and then read rest of file skipping first line
        file_meta = ioh.read_header(file_path)

        with open(
            file_path,
            "r",
            encoding="utf-8",
        ) as f:
            _ = f.readline()
            rest_html = f.read()

        soup = BeautifulSoup(
            rest_html,
            "html.parser",
        )

    except Exception:
        return [], metadata.get("hash_id")

    cards = soup.find_all(
        "div",
        class_="Nv2PK",
    )

    if not cards:
        cards = soup.find_all(
            "div",
            role="article",
        )

    valid_leads = []

    for card in cards:
        try:
            lead = parse_card(
                card,
                metadata,
            )

            if lead:
                # File-level metadata.
                lead["order_ids"] = normalize_order_ids(file_meta.get("order_ids", []))

                lead["query_type"] = file_meta.get(
                    "query_type",
                    metadata.get(
                        "query_type",
                        "Unknown",
                    ),
                )

                lead["Scraped_Date"] = file_meta.get(
                    "scraped_date",
                    lead.get("Scraped_Date"),
                )

                # UNIQUE ID PER BUSINESS
                unique_identifier = lead.get("url", "").strip()

                if not unique_identifier:
                    unique_identifier = (
                        f"{lead.get('name', '')}_" f"{lead.get('phone', '')}"
                    )

                lead["id"] = hashlib.md5(unique_identifier.encode("utf-8")).hexdigest()

                # Final processing-buffer schema normalization.
                lead = normalize_lead_for_buffer(lead)

                valid_leads.append(lead)

        except Exception:
            continue

    return (
        valid_leads,
        metadata.get("hash_id"),
    )


# ================= ORCHESTRATOR =================


def main():

    if sys.platform == "win32":
        import multiprocessing

        multiprocessing.freeze_support()

    print("=" * 60)
    print(" CORE PARSER: CHUNKED MEMORY-SAFE ARCHITECTURE")
    print("=" * 60)

    # --------------------------------------------------
    # 1. Ingest JSON Source of Truth
    # --------------------------------------------------

    if not os.path.exists(JSON_MAP_PATH):
        print(f"[FATAL] JSON mapping not found at: " f"{JSON_MAP_PATH}")
        sys.exit(1)

    try:
        with open(
            JSON_MAP_PATH,
            "r",
            encoding="utf-8",
        ) as f:
            query_map = json.load(f)

    except json.JSONDecodeError as e:
        print(f"[FATAL] Invalid JSON format: {e}")
        sys.exit(1)

    # --------------------------------------------------
    # 2. Resume Logic
    # --------------------------------------------------

    completed_hashes = set()

    if os.path.exists(PROGRESS_LOG):
        with open(
            PROGRESS_LOG,
            "r",
            encoding="utf-8",
        ) as f:

            for line in f:
                hash_id = line.strip()

                if hash_id:
                    completed_hashes.add(hash_id)

    print(f"[INFO] Resume State: " f"{len(completed_hashes)} files already processed.")

    # --------------------------------------------------
    # 3. Build Worker Payload
    # --------------------------------------------------

    tasks = []

    for hash_id, metadata in query_map.items():

        if hash_id in completed_hashes:
            continue

        file_path = os.path.join(
            CACHE_DIR,
            f"{hash_id}.html",
        )

        metadata["hash_id"] = hash_id

        tasks.append((file_path, metadata))

    print(f"[INFO] Tasks remaining in queue: " f"{len(tasks)}")

    if not tasks:
        print("[SUCCESS] All files processed. Exiting.")
        sys.exit(0)
# --------------------------------------------------
    # 4. Initialize Deduplication
    # --------------------------------------------------

    dedup_conn = init_dedup_db()

    try:
        existing_buffer_ids = load_existing_buffer_ids(dedup_conn)

        if existing_buffer_ids:
            print(
                f"[INFO] Existing processing-buffer "
                f"IDs loaded into dedup index: "
                f"{existing_buffer_ids}"
            )

        # --------------------------------------------------
        # 4.5 Authoritative Processing Buffer Schema Boundary
        # --------------------------------------------------
        # Idempotently guarantees the 14-column CSV contract on disk 
        # prior to any worker execution or writing.
        buffer_needs_header = (
            not os.path.exists(BUFFER_CSV) or os.path.getsize(BUFFER_CSV) == 0
        )

        if buffer_needs_header:
            with open(BUFFER_CSV, "w", newline="", encoding="utf-8-sig") as f_init:
                init_writer = csv.DictWriter(f_init, fieldnames=HEADERS)
                init_writer.writeheader()
                f_init.flush()
                os.fsync(f_init.fileno())
            print(
                f"[SYSTEM] Initialized authoritative {os.path.basename(BUFFER_CSV)} "
                f"with {len(HEADERS)} schema columns."
            )

        # --------------------------------------------------
        # 5. CHUNKED Processing
        # --------------------------------------------------

        max_workers = os.cpu_count()
        CHUNK_SIZE = 1000

        total_leads_extracted = 0
        total_duplicates_skipped = 0

        # Processing loop operates strictly in append mode with guaranteed headers
        with open(
            BUFFER_CSV,
            "a",
            newline="",
            encoding="utf-8-sig",
        ) as f, open(
            PROGRESS_LOG,
            "a",
            encoding="utf-8",
            buffering=1,
        ) as log_file:

            writer = csv.DictWriter(
                f,
                fieldnames=HEADERS,
                extrasaction="ignore",
            )

            with ProcessPoolExecutor(max_workers=max_workers) as executor:

                for i in range(
                    0,
                    len(tasks),
                    CHUNK_SIZE,
                ):

                    chunk = tasks[i : i + CHUNK_SIZE]

                    futures = {
                        executor.submit(
                            process_html_file,
                            task,
                        ): task
                        for task in chunk
                    }

                    buffer = []
                    completed_batch_hashes = []

                    # --------------------------------------------------
                    # Collect worker results
                    # --------------------------------------------------

                    for future in as_completed(futures):

                        try:
                            leads, parent_hash = future.result()

                            if leads:
                                buffer.extend(leads)

                            if parent_hash:
                                completed_batch_hashes.append(parent_hash)

                        except Exception as e:
                            print(f"\n[!] Worker crashed: {e}")

                    # --------------------------------------------------
                    # Deduplicate before writing
                    # --------------------------------------------------

                    if buffer:
                        unique_buffer, duplicates = deduplicate_batch(
                            dedup_conn,
                            buffer,
                        )

                        total_duplicates_skipped += duplicates

                        buffer = unique_buffer

                    # --------------------------------------------------
                    # Atomic-ish bounded write retry
                    # --------------------------------------------------

                    written = False

                    for attempt in range(5):

                        try:

                            if buffer:
                                writer.writerows(buffer)

                                total_leads_extracted += len(buffer)

                            for h in completed_batch_hashes:
                                log_file.write(f"{h}\n")

                            f.flush()
                            log_file.flush()

                            written = True
                            break

                        except PermissionError:

                            time.sleep(0.2 * (attempt + 1))

                            continue

                        except Exception as write_err:

                            print(
                                "\n[!] Non-permission "
                                f"write failure: "
                                f"{write_err}"
                            )

                            raise

                    if not written:
                        raise OSError(
                            13,
                            "Sapphire OS locked file "
                            "persistently across maximum "
                            "backoff matrix thresholds.",
                        )

                    del futures
                    del buffer

                    processed_count = min(
                        i + CHUNK_SIZE,
                        len(tasks),
                    )

                    sys.stdout.write(
                        "[*] Processed "
                        f"{processed_count}/"
                        f"{len(tasks)} files | "
                        f"Total Saved: "
                        f"{total_leads_extracted} | "
                        f"Duplicates Skipped: "
                        f"{total_duplicates_skipped}\n"
                    )

                    sys.stdout.flush()

        print(
            "\n\n[SUCCESS] Wrote new records "
            "securely to "
            f"{os.path.basename(BUFFER_CSV)}"
        )

        print(f"[INFO] Total unique leads written: " f"{total_leads_extracted}")

        print(f"[INFO] Total duplicates skipped: " f"{total_duplicates_skipped}")

    finally:

        try:
            dedup_conn.close()
        except Exception:
            pass

        try:
            if os.path.exists(DEDUP_DB_PATH):
                os.remove(DEDUP_DB_PATH)
        except Exception:
            pass

    print("=" * 60)
    print(" STAGE 2 COMPLETE. " "READY FOR STAGE 4 EXTRACTION.")
    print("=" * 60)


if __name__ == "__main__":
    main()
