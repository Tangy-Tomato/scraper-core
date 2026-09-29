import gzip
import json
import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

import Stage_2_Core_Parser as legacy_parser
from master.config import load_settings


ROOT_DIR = Path(__file__).resolve().parent.parent
SETTINGS = load_settings()
RAW_HTML_DIR = SETTINGS.raw_html_dir
DATABASE_URL = SETTINGS.database_url
BATCH_SIZE = SETTINGS.stage2_batch_size
POLL_SECONDS = SETTINGS.parser_poll_seconds
logger = logging.getLogger(__name__)


def parse_task(conn: psycopg.Connection, task: dict) -> int:
    raw_path = RAW_HTML_DIR / (task["raw_file_path"] or f'{task["task_id"]}.html.gz')
    metadata = {
        "hash_id": task["task_key"],
        "Search_Keyword": task["search_keyword"],
        "Search_Location": task["search_location"],
        "minimum_reviews": task["minimum_reviews"],
        "query_type": task["query_type"],
        "order_id": task["order_id"],
    }
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".html",
            dir=RAW_HTML_DIR,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write("<!--")
            temp_file.write(
                json.dumps(
                    {
                        "order_ids": task.get("order_ids")
                        or ([task["order_id"]] if task["order_id"] else []),
                        "scraped_date": (
                            task["completed_at"].astimezone(timezone.utc).isoformat()
                            if task["completed_at"]
                            else datetime.now(timezone.utc).isoformat()
                        ),
                        "query_type": task["query_type"],
                    }
                )
            )
            temp_file.write("-->\n")
            with gzip.open(
                raw_path, "rt", encoding="utf-8", errors="replace"
            ) as source:
                while chunk := source.read(1024 * 1024):
                    temp_file.write(chunk)
        leads, _ = legacy_parser.process_html_file((str(temp_path), metadata))
    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)

    with conn.transaction():
        for lead in leads:
            conn.execute(
                """
                INSERT INTO leads (
                    id, task_id, search_keyword, search_location, order_ids,
                    query_type, name, url, website, rating, reviews, category,
                    phone, phone_type, scraped_date, first_seen_date,
                    last_updated_date
                )
                VALUES (
                    %s, %s, %s, %s, %s::text[], %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s::timestamptz, %s::timestamptz, %s::timestamptz
                )
                ON CONFLICT (id) DO UPDATE SET
                    task_id = COALESCE(leads.task_id, EXCLUDED.task_id),
                    order_ids = ARRAY(
                        SELECT DISTINCT unnest(leads.order_ids || EXCLUDED.order_ids)
                    ),
                    search_keyword = COALESCE(NULLIF(EXCLUDED.search_keyword, ''), leads.search_keyword),
                    search_location = COALESCE(NULLIF(EXCLUDED.search_location, ''), leads.search_location),
                    last_updated_date = now(),
                    data_status = CASE
                        WHEN leads.data_status = 'ENRICHED' THEN leads.data_status
                        ELSE 'RAW'
                    END
                """,
                (
                    lead["id"],
                    task["task_id"],
                    lead["Search_Keyword"],
                    lead["Search_Location"],
                    lead["order_ids"],
                    lead["query_type"],
                    lead["name"],
                    lead["url"],
                    lead["website"],
                    lead["rating"],
                    lead["reviews"],
                    lead["category"],
                    lead["phone"],
                    lead["Phone_Type"],
                    lead["Scraped_Date"],
                    lead["Scraped_Date"],
                    lead["Scraped_Date"],
                ),
            )
        conn.execute(
            """
            UPDATE scraping_tasks
            SET parse_status = 'COMPLETED',
                parsed_at = now(),
                parse_lease_until = NULL,
                updated_at = now()
            WHERE task_id = %s
            """,
            (task["task_id"],),
        )
    return len(leads)


def claim_tasks(conn: psycopg.Connection) -> list[dict]:
    with conn.transaction():
        rows = conn.execute(
            """
            WITH candidates AS (
                SELECT task_id
                FROM scraping_tasks
                WHERE status = 'COMPLETED'
                  AND (
                      parse_status = 'PENDING'
                      OR (parse_status = 'PROCESSING' AND parse_lease_until < now())
                  )
                ORDER BY completed_at, task_id
                  LIMIT %s
                  FOR UPDATE SKIP LOCKED
            )
            UPDATE scraping_tasks AS task
            SET parse_status = 'PROCESSING',
                parse_lease_until = now() + interval '1 hour',
                parse_attempt_count = task.parse_attempt_count + 1,
                updated_at = now()
            FROM candidates
            WHERE task.task_id = candidates.task_id
            RETURNING task.task_id, task.task_key, task.raw_file_path,
                      task.search_keyword, task.search_location,
                      task.minimum_reviews, task.query_type, task.order_id,
                      task.order_ids,
                      task.completed_at, task.parse_attempt_count
            """,
            (BATCH_SIZE,),
        ).fetchall()
    keys = (
        "task_id",
        "task_key",
        "raw_file_path",
        "search_keyword",
        "search_location",
        "minimum_reviews",
        "query_type",
        "order_id",
        "order_ids",
        "completed_at",
        "parse_attempt_count",
    )
    return [dict(zip(keys, row)) for row in rows]


def run_once(conn: psycopg.Connection) -> int:
    tasks = claim_tasks(conn)
    parsed = 0
    for task in tasks:
        try:
            parsed += parse_task(conn, task)
        except Exception as exc:
            logger.exception("Failed to parse completed task %s", task["task_id"])
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE scraping_tasks
                    SET parse_status = CASE
                            WHEN parse_attempt_count >= 5 THEN 'FAILED'
                            ELSE 'PENDING'
                        END,
                        parse_lease_until = NULL,
                        last_error = %s,
                        updated_at = now()
                    WHERE task_id = %s AND parse_status = 'PROCESSING'
                    """,
                    (str(exc)[:2000], task["task_id"]),
                )
    return parsed


def main() -> None:
    RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO)
    with psycopg.connect(DATABASE_URL) as conn:
        while True:
            count = run_once(conn)
            if count:
                logger.info("Parsed %s leads", count)
            else:
                time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
