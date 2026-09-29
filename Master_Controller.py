import os
import sys
import json
import time
import shutil
import hashlib
import logging
import random
import subprocess
import pandas as pd
import psycopg2
import httpx
from pathlib import Path
from datetime import datetime
import urllib.parse
from lib import io_helpers as ioh, settings as cfg
from master.config import MASTER_API_URL, load_settings

# ================= CONFIGURATION & PATHS =================
ROOT_DIR = Path(__file__).resolve().parent

# Core Directories
ORDERS_DIR = ROOT_DIR / "Orders"
PENDING_DIR = ORDERS_DIR / "Pending"
COMPLETED_DIR = ORDERS_DIR / "Completed"
TEMP_DIR = ROOT_DIR / "System_Temp"
DELIVERIES_DIR = ROOT_DIR / "Final_Deliveries"

# Relay Directories (Google Drive)
RELAY_DIR = Path(r"E:\Google maps Data Elite\Gmap_Extractor_Elite\Elite_Scraper_Relay")
JOBS_PENDING = RELAY_DIR / "Jobs_Pending"
JOBS_PROCESSING = RELAY_DIR / "Jobs_Processing"
HTML_DROPOFF = RELAY_DIR / "HTML_Dropoff"

# Pipeline Configuration Files
QUERY_MAP_FILE = ROOT_DIR / "query_map.json"
# use canonical cache dir from lib.settings
RAW_CACHE_DIR = cfg.CACHE_DIR

# Script Paths
STAGE_2 = ROOT_DIR / "Stage_2_Core_Parser.py"
STAGE_4 = ROOT_DIR / "Stage_4_Email_Extractor.py"
WATCHDOG_SCRIPT = ROOT_DIR / "Watchdog.py"

DB_CONFIG = cfg.DB_CONFIG

for d in [
    PENDING_DIR,
    COMPLETED_DIR,
    TEMP_DIR,
    DELIVERIES_DIR,
    RAW_CACHE_DIR,
]:
    os.makedirs(d, exist_ok=True)

DISTRIBUTED_API_MODE = bool(MASTER_API_URL)
if not DISTRIBUTED_API_MODE:
    for d in [JOBS_PENDING, JOBS_PROCESSING, HTML_DROPOFF]:
        os.makedirs(d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

STATUS_FILE = os.path.join(ROOT_DIR, "system_status.json")

# ================= HELPER FUNCTIONS =================

def extract_priority_from_filename(filename: str) -> int:
    """
    Extracts priority from '<name>_<priority>.json'.
    Falls back gracefully to 1 if string split or integer conversion fails.
    """
    try:
        # Strip extension safely
        stem = Path(filename).stem  # e.g., 'client_order_alpha_10'
        
        # Split strictly from the rightmost underscore
        parts = stem.rsplit("_", 1)
        if len(parts) < 2:
            return 1

        priority = int(parts[1])
        # Enforce strict 1-10 boundary
        return max(1, min(10, priority))
    except (ValueError, IndexError):
        # Unhappy path: Malformed filename or non-integer suffix -> default priority
        return 1


def update_status(order_id, stage, message):
    status_data = {
        "current_order": order_id,
        "stage": stage,
        "message": message,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status_data, f)


def generate_id(text):
    if not text:
        return "UNKNOWN"
    return hashlib.md5(text.strip().encode("utf-8")).hexdigest()


def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").strip()


def run_stage(script_path, stage_name):
    logger.info(f"🚀 Initiating {stage_name}...")
    if not os.path.exists(script_path):
        logger.error(f"[FATAL ERROR] {script_path} not found!")
        sys.exit(1)

    try:
        subprocess.run([sys.executable, script_path], check=True)
        logger.info(f"✅ {stage_name} Completed Successfully.\n")
    except subprocess.CalledProcessError as e:
        logger.error(
            f"❌ [PIPELINE HALT] {stage_name} crashed with exit code {e.returncode}."
        )
        sys.exit(1)


def enqueue_distributed_tasks(tasks):
    settings = load_settings()
    with httpx.Client(
        base_url=settings.master_api_url,
        headers={"Authorization": f"Bearer {settings.auth_token}"},
        timeout=30,
    ) as client:
        for offset in range(0, len(tasks), 1000):
            payload_tasks = [
                {
                    "task_key": task["hash_id"],
                    "target_url": task["target_url"],
                    "task_type": task["type"],
                    "search_keyword": task["Search_Keyword"],
                    "search_location": task["Search_Location"],
                    "order_id": task["order_id"],
                    "query_type": task["query_type"],
                    "minimum_reviews": task["minimum_reviews"],
                }
                for task in tasks[offset : offset + 1000]
            ]
            response = client.post(
                "/api/v1/tasks/enqueue", json={"tasks": payload_tasks}
            )
            response.raise_for_status()


def is_valid_html(file_path):
    if not os.path.exists(file_path):
        return False
    try:
        if os.path.getsize(file_path) < 1000:
            return False
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().lower()
        if (
            "recaptcha" in content
            or "unusual traffic" in content
            or "consent.google.com" in content
        ):
            return False
        return True
    except:
        return False


# ================= CORE LOGIC =================
def process_order(order_file):
    filepath = os.path.join(PENDING_DIR, order_file)
    logger.info(f"=== INGESTING NEW ORDER: {order_file} ===")

    # 1. READ PARAMETERS
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            order = json.load(f)

        order_id = str(order.get("order_id", "0000"))
        client_name = str(order.get("client_name", "UnknownClient"))
        keywords = [k.strip() for k in order.get("keywords", []) if k.strip()]
        locations = [l.strip() for l in order.get("locations", []) if l.strip()]
        direct_queries = [
            dq.strip() for dq in order.get("direct_queries", []) if dq.strip()
        ]

        min_reviews = int(order.get("minimum_reviews", 0))
        max_leads = int(order.get("max_leads_required", 1000))
        global_search = bool(order.get("global_search", False))

    except Exception as e:
        logger.error(f"[FATAL] Failed to parse JSON {order_file}: {e}")
        return

    if not keywords and not locations and not direct_queries:
        logger.error(
            f"[FATAL] Order {order_id} has no keywords, locations, or direct queries."
        )
        return

    # 2. MATRIX GENERATION -> produce structured JSON task payloads
    job_tasks = []
    query_map = {}

    # Flow A: Standard Matrix (keywords x locations)
    for kw in keywords:
        for loc in locations:
            query = f"{kw} in {loc}"
            hash_id = generate_id(query)
            task = {
                "hash_id": hash_id,
                "type": "text_search",
                "target_url": query,
                "Search_Keyword": kw,
                "Search_Location": loc,
                "order_id": order_id,
                "query_type": "Matrix",
                "minimum_reviews": min_reviews,
            }
            job_tasks.append(task)
            query_map[hash_id] = task

    # Flow B: Direct Queries
    for dq in direct_queries:
        hash_id = generate_id(dq)
        task = {
            "hash_id": hash_id,
            "type": "text_search",
            "target_url": dq,
            "Search_Keyword": dq,
            "Search_Location": "Direct",
            "order_id": order_id,
            "query_type": "Direct",
            "minimum_reviews": min_reviews,
        }
        job_tasks.append(task)
        query_map[hash_id] = task

    # Flow C: Coordinate Grid (HARDENED INTEGER INDEXING)
    bbox = order.get("bounding_box")
    if bbox and keywords:
        try:
            if isinstance(bbox, dict):
                min_lat = float(
                    bbox.get("min_lat", bbox.get("minLat", bbox.get("south", 0)))
                )
                max_lat = float(
                    bbox.get("max_lat", bbox.get("maxLat", bbox.get("north", 0)))
                )
                min_lng = float(
                    bbox.get("min_lng", bbox.get("minLng", bbox.get("west", 0)))
                )
                max_lng = float(
                    bbox.get("max_lng", bbox.get("maxLng", bbox.get("east", 0)))
                )
            elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                min_lat, min_lng, max_lat, max_lng = map(float, bbox)
            else:
                raise ValueError("Unsupported bounding_box format")

            step = float(order.get("step_size", 0.01))
            zoom = int(order.get("zoom", 14))

            # Deterministic calculation of total steps
            lat_steps = int(round((max_lat - min_lat) / step)) + 1
            lng_steps = int(round((max_lng - min_lng) / step)) + 1

            for lat_idx in range(lat_steps):
                # Recalculate from base to strictly eliminate floating-point drift
                lat = round(min_lat + (lat_idx * step), 6)

                for lng_idx in range(lng_steps):
                    lng = round(min_lng + (lng_idx * step), 6)

                    for kw in keywords:
                        safe_kw = urllib.parse.quote_plus(kw)
                        target_url = f"https://www.google.com/maps/search/{safe_kw}/@{lat:.4f},{lng:.4f},{zoom}z"
                        hash_id = generate_id(target_url)
                        task = {
                            "hash_id": hash_id,
                            "type": "grid_url",
                            "target_url": target_url,
                            "Search_Keyword": kw,
                            "Search_Location": f"Grid_{lat:.4f}_{lng:.4f}",
                            "order_id": order_id,
                            "query_type": "Grid",
                            "minimum_reviews": min_reviews,
                        }
                        job_tasks.append(task)
                        query_map[hash_id] = task
        except Exception as e:
            logger.warning(f"Failed to generate grid tasks: {e}")

    # Persist query_map
    with open(QUERY_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(query_map, f, indent=4)

    logger.info(
        f"Generated {len(job_tasks)} distinct tasks ({len(direct_queries)} direct)."
    )

    if DISTRIBUTED_API_MODE:
        update_status(order_id, "Stage 1", "Enqueuing tasks through the Master API...")
        try:
            enqueue_distributed_tasks(job_tasks)
        except (httpx.HTTPError, ValueError):
            logger.exception("Distributed task enqueue failed; order remains pending.")
            update_status(order_id, "Stage 1 Error", "Task enqueue failed; retry required.")
            return
        shutil.move(filepath, os.path.join(COMPLETED_DIR, order_file))
        update_status(
            order_id,
            "Queued",
            f"Submitted {len(job_tasks)} tasks to the distributed cluster.",
        )
        return

    # 3. DISPATCHER: ZERO NETWORK CALLS LOGIC
    update_status(order_id, "Stage 1", "Dispatching tasks to Cloud Relay Cluster...")
    unprocessed_tasks = []
    skipped_tasks = 0

    for task in job_tasks:
        cache_path = RAW_CACHE_DIR / f"{task['hash_id']}.html"
        if cache_path.exists():
            try:
                meta = ioh.read_header(cache_path)
                # If no header (legacy file) -> do not delete, force re-scrape
                if not meta:
                    unprocessed_tasks.append(task)
                    continue

                scraped_date = meta.get('scraped_date')
                order_ids = meta.get('order_ids', []) or []
                # compute age days
                try:
                    sd = datetime.fromisoformat(scraped_date)
                    age_days = (datetime.utcnow() - sd).days
                except Exception:
                    age_days = 9999

                if age_days > cfg.CACHE_TTL_DAYS:
                    # stale: move to archive and force network scrape
                    try:
                        archive_dir = cfg.ARCHIVE_CACHE_DIR / datetime.utcnow().strftime("%Y%m%d")
                        archive_dir.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(cache_path), str(archive_dir / cache_path.name))
                    except Exception:
                        logger.warning(f"Failed to archive stale cache: {cache_path}")
                    unprocessed_tasks.append(task)
                else:
                    # cache hit: append order_id atomically; do not modify scraped_date
                    try:
                        ioh.append_order_id_atomic(cache_path, order_id)
                    except Exception:
                        logger.warning(f"Failed to append order_id to header: {cache_path}")
                    skipped_tasks += 1
            except Exception:
                # header read error or other IO issue: quarantine and re-scrape
                try:
                    qdir = cfg.QUARANTINE_DIR
                    qdir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(cache_path), str(qdir / cache_path.name))
                except Exception:
                    logger.warning(f"Failed to quarantine corrupt file: {cache_path}")
                unprocessed_tasks.append(task)
        else:
            unprocessed_tasks.append(task)

    logger.info(
        f"[CACHE HIT] Skipping {skipped_tasks} tasks that are already successfully cached locally."
    )

    if unprocessed_tasks:
        QUERIES_PER_JOB = 2
        chunks = [
            unprocessed_tasks[i : i + QUERIES_PER_JOB]
            for i in range(0, len(unprocessed_tasks), QUERIES_PER_JOB)
        ]
        logger.info(
            f"Chunking {len(unprocessed_tasks)} tasks into {len(chunks)} job tickets."
        )

        for i, chunk in enumerate(chunks):
            ticket_name = (
                f"job_ticket_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{i}.json"
            )
            ticket_path = JOBS_PENDING / ticket_name
            with open(ticket_path, "w", encoding="utf-8") as f:
                json.dump({"job_id": ticket_name, "tasks": chunk}, f)

        # 4. VACUUM & WAIT LOOP
        logger.info(f"===================================")
        logger.info(f" ⏳ VACUUM LOOP: Polling HTML dropoff")
        logger.info(f"===================================")
        while True:
            for html_file in HTML_DROPOFF.glob("*.html"):
                try:
                    local_target = RAW_CACHE_DIR / html_file.name
                    if os.path.exists(local_target):
                        shutil.move(local_target, cfg.Delete_Archive_Dir / local_target.name)
                    shutil.move(str(html_file), str(local_target))
                    logger.info(f"[VACUUM] Retrieved {html_file.name}")
                except Exception as e:
                    pass

            pending_jobs = list(JOBS_PENDING.glob("*.json"))
            processing_jobs = list(JOBS_PROCESSING.glob("*.json"))

            if not pending_jobs and not processing_jobs:
                logger.info(
                    f"✅ Cluster processing complete. All jobs picked up and processed."
                )
                time.sleep(10)
                for html_file in HTML_DROPOFF.glob("*.html"):
                    try:
                        local_target = RAW_CACHE_DIR / html_file.name
                        if os.path.exists(local_target):
                            shutil.move(local_target, cfg.Delete_Archive_Dir / local_target.name)
                        shutil.move(str(html_file), str(local_target))
                    except:
                        pass
                break
            time.sleep(5)
    else:
        logger.info(
            "ℹ️ All queries locally cached. Bypassing cluster dispatcher entirely."
        )

    # 5. ORCHESTRATION (DOWNSTREAM PIPELINE)
    update_status(order_id, "Stage 2", "Parsing raw HTML into structured data...")
    run_stage(STAGE_2, "STAGE 2: Core Data Parser")

    # update_status(order_id, "Stage 4", "Enriching leads with emails & contacts...")
    # run_stage(WATCHDOG_SCRIPT, "STAGE 4: Email & Contact Extractor (Protected)")

    # logger.info("\n[SYSTEM] Triggering Database Sync...")
    # try:
    #     subprocess.run(["python", "db_sync.py"], check=True)
    #     logger.info("[SYSTEM] Database Sync Complete. Buffer flushed to Master DB.")
    # except subprocess.CalledProcessError as e:
    #     logger.error(f"[FATAL] Database sync failed. Leads are still in buffer.")
    # # Run sanitization (2% poison logic) before exports
    # try:
    #     subprocess.run(["python", "Stage_5_Database_Sanitization.py"], check=True)
    #     logger.info("[SYSTEM] Database Sanitization Complete.")
    # except subprocess.CalledProcessError:
    #     logger.warning("[WARN] Database Sanitization failed or was skipped.")

    # # 6. POSTGRESQL WATERFALL SORTER & FILTER (RE-ARCHITECTED)
    # update_status(order_id, "Export", "Querying DB and sorting leads for delivery...")
    # logger.info("⚙️ Extracting Leads from Master Database...")

    # Array safeguards to prevent psycopg2 from crashing on empty lists
    # all_kws = list(set([task["Search_Keyword"] for task in job_tasks])) or [
    #     "__NULL_KW__"
    # ]
    # all_locs = list(set([task["Search_Location"] for task in job_tasks])) or [
    #     "__NULL_LOC__"
    # ]

    # # Create fuzzy patterns for direct queries to bridge metadata fragmentation
    # direct_patterns = (
    #     [f"%{dq}%" for dq in direct_queries] if direct_queries else ["__NULL_DQ__"]
    # )

    # if all_kws == ["__NULL_KW__"]:
    #     logger.error("[FATAL] No keywords to query.")
    #     shutil.move(filepath, os.path.join(COMPLETED_DIR, order_file))
    #     return

    # try:
    #     conn = psycopg2.connect(**DB_CONFIG)
    #     cur = conn.cursor()

    #     # Strict, deterministic export: use order_ids membership
    #     logger.info("🔎 Exporting by order_id membership (deterministic).")
    #     query = "SELECT * FROM master_leads WHERE %s = ANY(order_ids) AND is_valid = TRUE AND lead_status != 'Dead'"
    #     cur.execute(query, (order_id,))

    #     # Native mapping bypasses pandas SQLalchemy deprecation warning
    #     columns = [desc[0] for desc in cur.description]
    #     records = cur.fetchall()
    #     master_df = pd.DataFrame(records, columns=columns)

    #     cur.close()
    #     conn.close()

    # except Exception as e:
    #     logger.error(f"[FATAL] DB Extraction failed: {e}")
    #     shutil.move(filepath, os.path.join(COMPLETED_DIR, order_file))
    #     return

    # if master_df.empty:
    #     logger.warning(
    #         f"[WARNING] No valid leads found in DB for order criteria. Ensure Stage 2 is actually parsing DB records."
    #     )
    #     shutil.move(filepath, os.path.join(COMPLETED_DIR, order_file))
    #     return

    # # THE ANTI-CHAOS AGGREGATOR: Merge 1km grids into a single Export Location
    # master_df["search_location"] = master_df["search_location"].apply(
    #     lambda x: "Target_Area_Export" if str(x).startswith("Grid_") else x
    # )

    # # ASSIGN TIERS FOR SORTING (Premium = 1, Fallback = 2)
    # master_df["Tier"] = (
    #     master_df["data_status"].map({"Premium": "1", "Fallback": "2"}).fillna("3")
    # )

    # # ENSURE REVIEWS ARE NUMERIC FOR SORTING
    # master_df["reviews"] = (
    #     pd.to_numeric(master_df["reviews"], errors="coerce").fillna(0).astype(int)
    # )

    # # FILTER BY MINIMUM REVIEWS
    # master_df = master_df[master_df["reviews"] >= min_reviews]

    # # SORT: Best Data First (Tier), then Highest Reviews
    # master_df.sort_values(by=["Tier", "reviews"], ascending=[True, False], inplace=True)
    # master_df.drop_duplicates(subset=["url"], keep="first", inplace=True)

    # # CAPITALIZE COLUMNS to match your canonical export format
    # master_df.rename(
    #     columns={
    #         "search_keyword": "Search_Keyword",
    #         "search_location": "Search_Location",
    #         "data_status": "Data_Status",
    #     },
    #     inplace=True,
    # )

    # total_available = len(master_df)
    # final_df = master_df.head(max_leads)

    # premium_count = len(final_df[final_df["Tier"] == "1"])
    # fallback_count = len(final_df[final_df["Tier"] == "2"])

    # logger.info(f"[YIELD] Found {total_available} matching leads.")
    # logger.info(
    #     f"[DELIVERING] {len(final_df)} leads (Premium: {premium_count} | Fallback: {fallback_count})."
    # )

    # # 7. CLIENT EXPORT
    # logger.info("📦 Generating Client Deliverables...")

    # safe_order = sanitize_filename(order_id)
    # safe_client = sanitize_filename(client_name)
    # client_dir = os.path.join(DELIVERIES_DIR, f"{safe_order}_{safe_client}")
    # os.makedirs(client_dir, exist_ok=True)

    # grouped = final_df.groupby(["Search_Keyword", "Search_Location"])
    # files_created = 0

    # for (kw, loc), group in grouped:
    #     safe_kw = sanitize_filename(kw)
    #     safe_loc = sanitize_filename(loc)
    #     kw_dir = os.path.join(client_dir, safe_kw)
    #     os.makedirs(kw_dir, exist_ok=True)
    #     csv_path = os.path.join(kw_dir, f"{safe_loc}.csv")

    #     cols_to_drop = ["Search_Keyword", "Search_Location", "Data_Status", "Tier"]
    #     final_group = group.drop(columns=cols_to_drop, errors="ignore")
    #     final_group.to_csv(csv_path, index=False, encoding="utf-8-sig")
    #     files_created += 1

    # logger.info(f"✅ Packaged {files_created} CSV files in {client_dir}.")

    # logger.info("📋 Generating Consolidated Master Export...")
    # master_export = final_df.copy()
    # master_export["Category"] = (
    #     master_export["Search_Keyword"] + " in " + master_export["Search_Location"]
    # )
    # cols_to_drop_master = ["Data_Status", "Tier"]
    # master_export = master_export.drop(columns=cols_to_drop_master, errors="ignore")
    # master_csv_path = os.path.join(client_dir, f"{safe_order}_Master.csv")
    # master_export.to_csv(master_csv_path, index=False, encoding="utf-8-sig")

    # logger.info(
    #     f"📋 Master Export: {len(master_export)} leads → {os.path.basename(master_csv_path)}"
    # )

    # 8. CLEANUP
    logger.info("🧹 Performing System Cleanup...")
    # update_status(order_id, "Completed", f"Delivered {len(final_df)} leads.")
    shutil.move(filepath, os.path.join(COMPLETED_DIR, order_file))
    logger.info(f"=== ORDER {order_id} COMPLETED SUCCESSFULLY ===\n")


def main():
    print("=" * 60)
    print(" DATA FACTORY: MASTER DISPATCHER CONTROLLER ONLINE ")
    print("=" * 60)

    while True:
        pending_files = [
            f for f in os.listdir(PENDING_DIR)
            if f.endswith(".json")
        ]

        if not pending_files:
            logger.info(f"No pending orders found in {PENDING_DIR}. Exiting.")
            sys.exit(0)

        logger.info(f"Found {len(pending_files)} pending orders in queue.")

        # 1. Map filenames to their evaluated priority
        file_priorities = [
            (f, extract_priority_from_filename(f))
            for f in pending_files
        ]

        # 2. Determine highest priority level present
        max_priority = max(
            priority for _, priority in file_priorities
        )

        # 3. Filter all files sharing this maximum priority
        top_tier_files = [
            f for f, priority in file_priorities
            if priority == max_priority
        ]

        # 4. Pick randomly among highest-priority candidates
        selected_file = random.choice(top_tier_files)

        logger.info(
            f"Selected order '{selected_file}' "
            f"(Priority: {max_priority}) "
            f"from {len(top_tier_files)} candidate(s) at top priority."
        )

        # 5. Process the selected order.
        # When this finishes, the loop starts again and
        # checks PENDING_DIR for another JSON file.
        process_order(selected_file)


if __name__ == "__main__":
    main()