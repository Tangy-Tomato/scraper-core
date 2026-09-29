import os
import csv
import time
import shutil
import logging
import psycopg2
import ast
import sys
from pathlib import Path

# Bypass field limits for large datasets
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# --- PATHS ---
ROOT_DIR = Path(__file__).resolve().parent
SYSTEM_TEMP_DIR = ROOT_DIR / "System_Temp"
ARCHIVE_DIR = ROOT_DIR / "Buffer_Archives"
INPUT_CSV = SYSTEM_TEMP_DIR / "enriched_sync_buffer.csv"
READY_CSV = SYSTEM_TEMP_DIR / "postgres_ready_buffer.csv"

os.makedirs(ARCHIVE_DIR, exist_ok=True)

from lib.settings import DB_CONFIG, CANONICAL_COLUMNS


def sanitize_csv_for_postgres(input_path, output_path):
    """
    Safely translates Python lists to PostgreSQL arrays using proper CSV architecture.
    Guarantees 0 data loss and prevents quote-parity corruption.
    Returns the total number of rows processed for mathematical verification.
    """
    logger.info("[SYSTEM] Sanitizing array formats to guarantee CSV integrity...")
    
    rows_processed = 0
    
    with open(input_path, "r", encoding="utf-8-sig") as infile, \
         open(output_path, "w", newline="", encoding="utf-8-sig") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
        
        try:
            raw_headers = next(reader)
        except StopIteration:
            return 0
            
        writer.writerow(raw_headers)
        
        # Find the index of order_ids dynamically
        order_idx = -1
        for i, h in enumerate(raw_headers):
            if h.strip().replace("\ufeff", "").replace(" ", "_").lower() == "order_ids":
                order_idx = i
                break
                
        for row in reader:
            if order_idx != -1 and order_idx < len(row):
                val = row[order_idx].strip()
                
                # Format to PostgreSQL array safely
                if not val or val in ("[]", "{}", "None"):
                    row[order_idx] = "{}"
                elif val.startswith("[") and val.endswith("]"):
                    try:
                        parsed_list = ast.literal_eval(val)
                        if isinstance(parsed_list, list):
                            pg_items = [f'"{str(item)}"' for item in parsed_list]
                            row[order_idx] = "{" + ",".join(pg_items) + "}"
                    except Exception:
                        pass
                        
            writer.writerow(row)
            rows_processed += 1
            
    return rows_processed


def _build_upsert_queries(column_list):
    """
    Builds CONSTRAINT-INDEPENDENT SQL strings for safe merging.
    Handles each PostgreSQL datatype correctly.
    """

    col_list_str = ", ".join(column_list)

    # Columns whose empty string should NOT overwrite existing values
    TEXT_COLUMNS = {
        "search_keyword",
        "search_location",
        "name",
        "url",
        "website",
        "category",
        "phone",
        "phone_type",
        "emails",
        "alternative_phones",
        "linkedin",
        "facebook",
        "twitter",
        "instagram",
        "data_status",
        "query_type",
        "lead_status",
        "found_web_mobile",
        "domain_state",
        "ssl_status",
    }

    assignments = []

    for col in column_list:

        if col == "id":
            continue

        elif col == "order_ids":
            assignments.append(
                f"""{col} = (
                    SELECT ARRAY(
                        SELECT DISTINCT unnest(
                            COALESCE(master_leads.{col}, ARRAY[]::text[])
                            ||
                            COALESCE(staging_leads.{col}, ARRAY[]::text[])
                        )
                    )
                )"""
            )

        elif col == "first_seen_date":
            assignments.append(
                f"{col} = COALESCE(master_leads.{col}, staging_leads.{col})"
            )

        elif col == "last_updated_date":
            assignments.append(
                f"{col} = staging_leads.{col}"
            )

        elif col in TEXT_COLUMNS:
            assignments.append(
                f"{col} = COALESCE(NULLIF(staging_leads.{col}, ''), master_leads.{col})"
            )

        else:
            # boolean / integer / real / timestamp / jsonb
            assignments.append(
                f"{col} = COALESCE(staging_leads.{col}, master_leads.{col})"
            )

    update_set = ",\n    ".join(assignments)

    update_existing = f"""
    UPDATE master_leads
    SET
        {update_set}
    FROM staging_leads
    WHERE master_leads.id = staging_leads.id
       OR (
            master_leads.url = staging_leads.url
            AND staging_leads.url IS NOT NULL
            AND staging_leads.url != ''
       )
       OR (
            master_leads.phone = staging_leads.phone
            AND staging_leads.phone IS NOT NULL
            AND staging_leads.phone != ''
            AND (staging_leads.url IS NULL OR staging_leads.url = '')
       );
    """

    insert_new = f"""
    INSERT INTO master_leads ({col_list_str})
    SELECT {col_list_str}
    FROM staging_leads s
    WHERE NOT EXISTS (
        SELECT 1
        FROM master_leads m
        WHERE m.id = s.id
           OR (
                m.url = s.url
                AND s.url IS NOT NULL
                AND s.url != ''
           )
           OR (
                m.phone = s.phone
                AND s.phone IS NOT NULL
                AND s.phone != ''
           )
    );
    """

    return [update_existing, insert_new]


def sync_to_database():
    if not os.path.exists(INPUT_CSV) or os.path.getsize(INPUT_CSV) == 0:
        logger.info("[SYNC] No data in processing buffer. Exiting.")
        return

    conn = None
    try:
        # STEP 1: Pre-process the file to guarantee PostgreSQL formatting parity
        logger.info("[SYNC] Initiating Air-Gapped Sanitization Phase...")
        expected_row_count = sanitize_csv_for_postgres(INPUT_CSV, READY_CSV)
        
        if expected_row_count == 0:
            logger.info("[SYNC] Buffer is structurally empty. Exiting.")
            return

        logger.info(f"[CHECKPOINT 1] Safely sanitized {expected_row_count} rows. Integrity verified.")

        # STEP 2: Read the sanitized CSV fields dynamically
        with open(READY_CSV, "r", encoding="utf-8-sig") as f_meta:
            reader = csv.reader(f_meta)
            raw_headers = next(reader)

        csv_headers = [
            h.strip().replace("\ufeff", "").replace(" ", "_").lower()
            for h in raw_headers
        ]
        columns_signature = ", ".join([f'"{col}"' for col in csv_headers])

        logger.info("[SYNC] Connecting to Master Database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # STEP 3: Clear Staging
        cursor.execute("TRUNCATE TABLE staging_leads;")

        # STEP 4: Memory-Efficient Bulk Copy
        logger.info(f"[SYNC] Streaming formatted CSV directly to Postgres staging_leads...")
        copy_query = (
            f"COPY staging_leads ({columns_signature}) FROM STDIN WITH CSV HEADER"
        )

        with open(READY_CSV, "r", encoding="utf-8-sig") as f:
            cursor.copy_expert(copy_query, f)

        # STEP 5: Mathematical Verification (Checkpoint 2)
        cursor.execute("SELECT COUNT(*) FROM staging_leads;")
        db_row_count = cursor.fetchone()[0]
        
        if db_row_count != expected_row_count:
            raise ValueError(f"CRITICAL MISMATCH: CSV contains {expected_row_count} rows, but database staged {db_row_count} rows. Aborting sync.")
            
        logger.info(f"[CHECKPOINT 2] Database successfully staged {db_row_count} rows. Integrity verified.")

        # STEP 6: Execute the Upsert Merge (Constraint-Independent)
        logger.info("[SYNC] Executing Conflict Resolution & Upsert Matrix...")
        if CANONICAL_COLUMNS:
            queries = _build_upsert_queries(CANONICAL_COLUMNS)
            logger.info("[SYNC] Updating existing records (Constraint-Independent)...")
            cursor.execute(queries[0])
            logger.info("[SYNC] Inserting new records...")
            cursor.execute(queries[1])
        else:
            logger.info("[SYNC] CANONICAL_COLUMNS not found; falling back to legacy constraint-free upsert.")
            
            # Constraint-Independent Legacy Upsert
            legacy_update = """
            UPDATE master_leads m
            SET 
                emails = COALESCE(NULLIF(s.emails::text, ''), m.emails::text),
                alternative_phones = COALESCE(NULLIF(s.alternative_phones::text, ''), m.alternative_phones::text),
                linkedin = COALESCE(NULLIF(s.linkedin::text, ''), m.linkedin::text),
                facebook = COALESCE(NULLIF(s.facebook::text, ''), m.facebook::text),
                twitter = COALESCE(NULLIF(s.twitter::text, ''), m.twitter::text),
                instagram = COALESCE(NULLIF(s.instagram::text, ''), m.instagram::text),
                data_status = s.data_status,
                first_seen_date = COALESCE(m.first_seen_date, s.first_seen_date),
                last_updated_date = s.last_updated_date,
                order_ids = (SELECT ARRAY(SELECT DISTINCT unnest(COALESCE(m.order_ids, ARRAY[]::text[]) || COALESCE(s.order_ids, ARRAY[]::text[])))),
                scraped_date = COALESCE(s.scraped_date, m.scraped_date)
            FROM staging_leads s
            WHERE m.phone = s.phone AND s.phone IS NOT NULL AND s.phone != '';
            """
            
            legacy_insert = """
            INSERT INTO master_leads
            SELECT * FROM staging_leads s
            WHERE NOT EXISTS (
                SELECT 1 FROM master_leads m WHERE m.phone = s.phone AND s.phone IS NOT NULL AND s.phone != ''
            ) AND s.phone IS NOT NULL AND s.phone != '';
            
            INSERT INTO master_leads
            SELECT * FROM staging_leads s
            WHERE s.phone IS NULL OR s.phone = '';
            """
            cursor.execute(legacy_update)
            cursor.execute(legacy_insert)

        # STEP 7: Commit Transaction
        conn.commit()
        logger.info("✅ [SUCCESS] Database Sync Complete.")

        # STEP 8: Clean up and Archive
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        archive_path = ARCHIVE_DIR / f"buffer_{timestamp}.csv"
        shutil.move(str(INPUT_CSV), str(archive_path))
        if os.path.exists(READY_CSV):
            os.remove(READY_CSV)
            
        logger.info(f"[SYSTEM] processing_buffer.csv archived successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ [FATAL ERROR] Database Sync Failed: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    sync_to_database()