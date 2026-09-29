import logging
import multiprocessing
import queue
import threading
import time
from datetime import datetime, timezone
from queue import Empty

import psycopg

import Stage_4_Email_Extractor as legacy_enricher
from master.config import load_settings


SETTINGS = load_settings()
DATABASE_URL = SETTINGS.database_url
BATCH_SIZE = SETTINGS.stage4_batch_size
POLL_SECONDS = SETTINGS.parser_poll_seconds
logger = logging.getLogger(__name__)


def _timestamp_text(value) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


def _legacy_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "Search_Keyword": row["search_keyword"],
        "Search_Location": row["search_location"],
        "order_ids": row["order_ids"] or [],
        "query_type": row["query_type"],
        "name": row["name"],
        "url": row["url"],
        "website": row["website"],
        "rating": row["rating"] or 0,
        "reviews": row["reviews"] or 0,
        "category": row["category"],
        "phone": row["phone"],
        "Phone_Type": row["phone_type"],
        "Scraped_Date": _timestamp_text(row["scraped_date"]),
        "Emails": row["emails"],
        "Alternative_Phones": row["alternative_phones"],
        "LinkedIn": row["linkedin"],
        "Facebook": row["facebook"],
        "Twitter": row["twitter"],
        "Instagram": row["instagram"],
        "Data_Status": row["data_status"],
        "Found_Web_Mobile": row["found_web_mobile"],
        "is_valid": row["is_valid"],
        "lead_status": row["lead_status"],
        "first_seen_date": _timestamp_text(row["first_seen_date"]),
        "last_updated_date": _timestamp_text(row["last_updated_date"]),
    }


def _run_legacy_enrichment(rows: list[dict]) -> list[dict]:
    legacy_enricher.init_network_cache_db()
    playwright_queue: queue.Queue = queue.Queue()
    output_queue: queue.Queue = queue.Queue()
    playwright_thread = threading.Thread(
        target=legacy_enricher.playwright_worker,
        args=(playwright_queue, output_queue),
        daemon=True,
    )
    playwright_thread.start()
    try:
        for row in rows:
            legacy_enricher.process_row_online(
                row, playwright_queue, output_queue, None
            )
    finally:
        playwright_queue.put(None)
    playwright_thread.join(timeout=SETTINGS.stage4_timeout_seconds)
    if playwright_thread.is_alive():
        raise TimeoutError("Stage 4 Playwright worker exceeded its time limit")
    results = []
    while True:
        try:
            results.append(output_queue.get_nowait())
        except Empty:
            break
    if len(results) != len(rows):
        raise RuntimeError(
            f"enricher returned {len(results)} rows for {len(rows)} inputs"
        )
    return results


def _enrichment_process(rows: list[dict], result_queue) -> None:
    try:
        result_queue.put((True, _run_legacy_enrichment(rows)))
    except Exception as exc:
        result_queue.put((False, str(exc)))


def _run_isolated_enrichment(rows: list[dict]) -> list[dict]:
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue(maxsize=1)
    process = context.Process(target=_enrichment_process, args=(rows, result_queue))
    process.start()
    deadline = time.monotonic() + SETTINGS.stage4_timeout_seconds
    outcome = None
    try:
        while time.monotonic() < deadline:
            try:
                outcome = result_queue.get(timeout=min(1, deadline - time.monotonic()))
                break
            except Empty:
                if not process.is_alive():
                    break
        if outcome is None:
            if process.is_alive():
                process.terminate()
            process.join()
            if process.exitcode not in (None, 0):
                raise RuntimeError(
                    "Stage 4 enrichment process exited without a result "
                    f"(exit code {process.exitcode})"
                )
            raise TimeoutError("Stage 4 enrichment process timed out")
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError("Stage 4 enrichment process did not exit")
        succeeded, result = outcome
        if process.exitcode != 0 or not succeeded:
            raise RuntimeError(f"Stage 4 enrichment process failed: {result}")
        return result
    finally:
        result_queue.close()
        result_queue.join_thread()
        if process.is_alive():
            process.terminate()
            process.join()
        if process.exitcode is not None:
            process.close()


def run_once(conn: psycopg.Connection) -> int:
    with conn.transaction():
        rows = conn.execute(
            """
            WITH candidates AS (
                SELECT id
                FROM leads
                WHERE data_status = 'RAW'
                   OR (
                       data_status = 'ENRICHING'
                       AND enrichment_lease_until < now()
                   )
                ORDER BY id
                   LIMIT %s
                   FOR UPDATE SKIP LOCKED
            )
            UPDATE leads AS lead
            SET data_status = 'ENRICHING',
                enrichment_lease_until = now() + interval '1 hour'
            FROM candidates
            WHERE lead.id = candidates.id
            RETURNING to_jsonb(lead)
            """,
            (BATCH_SIZE,),
        ).fetchall()
        claimed = [row[0] for row in rows]

    if not claimed:
        return 0
    try:
        enriched = _run_isolated_enrichment([_legacy_row(row) for row in claimed])
        by_id = {row["id"]: row for row in enriched}
        with conn.transaction():
            for original in claimed:
                row = by_id[original["id"]]
                conn.execute(
                    """
                    UPDATE leads SET
                        emails = %s, alternative_phones = %s, linkedin = %s,
                        facebook = %s, twitter = %s, instagram = %s,
                        data_status = 'ENRICHED', found_web_mobile = %s,
                        is_valid = %s, lead_status = %s, first_seen_date = %s,
                        last_updated_date = %s, dead_email = %s, enriched_at = %s,
                        enrichment_version = %s, http_status = %s,
                        domain_state = %s, ssl_status = %s, mobile_friendly = %s,
                        has_contact_info = %s, quality_score = %s, tags = %s::jsonb,
                        last_checked_at = %s, raw_extracted = %s::jsonb,
                        enrichment_lease_until = NULL
                    WHERE id = %s AND data_status = 'ENRICHING'
                    """,
                    (
                        row.get("Emails", ""),
                        row.get("Alternative_Phones", ""),
                        row.get("LinkedIn", ""),
                        row.get("Facebook", ""),
                        row.get("Twitter", ""),
                        row.get("Instagram", ""),
                        row.get("Found_Web_Mobile", "No"),
                        row.get("is_valid", True),
                        row.get("lead_status", ""),
                        row.get("first_seen_date") or None,
                        row.get("last_updated_date") or None,
                        row.get("Dead_Email", False),
                        row.get("enriched_at") or datetime.now(timezone.utc),
                        row.get("enrichment_version") or 1,
                        row.get("http_status") or None,
                        row.get("domain_state", ""),
                        row.get("ssl_status", ""),
                        row.get("mobile_friendly", False),
                        row.get("has_contact_info", False),
                        row.get("quality_score", 0),
                        row.get("tags", "[]"),
                        row.get("last_checked_at") or None,
                        row.get("raw_extracted", "{}"),
                        original["id"],
                    ),
                )
    except Exception:
        with conn.transaction():
            conn.execute(
                """
                UPDATE leads
                SET data_status = 'RAW', enrichment_lease_until = NULL
                WHERE id = ANY(%s) AND data_status = 'ENRICHING'
                """,
                ([row["id"] for row in claimed],),
            )
        raise
    return len(enriched)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    with psycopg.connect(DATABASE_URL) as conn:
        while True:
            count = run_once(conn)
            if count:
                logger.info("Enriched %s leads", count)
            else:
                time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
