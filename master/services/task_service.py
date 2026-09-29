from datetime import datetime
from uuid import UUID

from psycopg import Connection

from shared.contracts import ScrapeTask, TaskEnqueueItem


def enqueue_tasks(conn: Connection, tasks: list[TaskEnqueueItem]) -> tuple[int, int]:
    inserted = 0
    with conn.transaction():
        for task in tasks:
            result = conn.execute(
                """
                INSERT INTO scraping_tasks (
                    task_key, target_url, task_type, search_keyword,
                    search_location, order_id, order_ids, query_type, minimum_reviews
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::text[], %s, %s)
                ON CONFLICT (task_key) DO NOTHING
                RETURNING task_id
                """,
                (
                    task.task_key,
                    task.target_url,
                    task.task_type,
                    task.search_keyword,
                    task.search_location,
                    task.order_id,
                    [task.order_id] if task.order_id else [],
                    task.query_type,
                    task.minimum_reviews,
                ),
            ).fetchone()
            if result:
                inserted += 1
                task_id = result[0]
            else:
                existing = conn.execute(
                    """
                    SELECT task_id FROM scraping_tasks
                    WHERE task_key = %s
                    FOR UPDATE
                    """,
                    (task.task_key,),
                ).fetchone()
                if not existing:
                    raise RuntimeError(
                        f"task key {task.task_key!r} disappeared during enqueue"
                    )
                task_id = existing[0]
            conn.execute(
                """
                UPDATE scraping_tasks
                SET minimum_reviews = LEAST(minimum_reviews, %s),
                    updated_at = now()
                WHERE task_id = %s
                """,
                (task.minimum_reviews, task_id),
            )
            if task.order_id:
                conn.execute(
                    """
                    UPDATE scraping_tasks
                    SET order_ids = ARRAY(
                        SELECT DISTINCT unnest(scraping_tasks.order_ids || %s::text[])
                    ),
                    updated_at = now()
                    WHERE task_id = %s
                    """,
                    ([task.order_id], task_id),
                )
                conn.execute(
                    """
                    UPDATE leads
                    SET order_ids = ARRAY(
                        SELECT DISTINCT unnest(leads.order_ids || %s::text[])
                    )
                    WHERE task_id = %s
                    """,
                    ([task.order_id], task_id),
                )
    return inserted, len(tasks) - inserted


def acquire_task(
    conn: Connection,
    worker_id: str,
    version: str,
    lease_seconds: int,
) -> tuple[ScrapeTask | None, datetime | None]:
    lease_until = None
    task = None
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO workers (worker_id, version, last_seen_at)
            VALUES (%s, %s, now())
            ON CONFLICT (worker_id) DO UPDATE
            SET version = EXCLUDED.version, last_seen_at = now()
            """,
            (worker_id, version),
        )
        conn.execute(
            """
            UPDATE scraping_tasks
            SET status = 'FAILED',
                last_error = COALESCE(last_error, 'maximum attempts exceeded'),
                updated_at = now()
            WHERE status = 'PROCESSING'
              AND lease_until < now()
              AND attempt_count >= max_attempts
            """
        )
        row = conn.execute(
            """
            WITH candidate AS (
                SELECT task_id
                FROM scraping_tasks
                WHERE attempt_count < max_attempts
                  AND (
                      status = 'PENDING'
                      OR (status = 'PROCESSING' AND lease_until < now())
                  )
                ORDER BY priority DESC, created_at, task_id
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED
            )
            UPDATE scraping_tasks AS task
            SET status = 'PROCESSING',
                worker_id = %s,
                attempt_count = task.attempt_count + 1,
                lease_until = now() + (%s * interval '1 second'),
                started_at = COALESCE(task.started_at, now()),
                updated_at = now()
            FROM candidate
            WHERE task.task_id = candidate.task_id
            RETURNING task.task_id, task.task_key, task.target_url,
                      task.task_type, task.search_keyword, task.search_location,
                      task.order_id, task.query_type, task.minimum_reviews,
                      task.lease_until
            """,
            (worker_id, lease_seconds),
        ).fetchone()
        if row:
            task = ScrapeTask(
                task_id=str(row[0]),
                task_key=row[1],
                target_url=row[2],
                task_type=row[3],
                search_keyword=row[4],
                search_location=row[5],
                order_id=row[6],
                query_type=row[7],
                minimum_reviews=row[8],
            )
            lease_until = row[9]
    return task, lease_until


def get_task_status(conn: Connection, task_id: UUID) -> tuple[str, str | None]:
    with conn.transaction():
        row = conn.execute(
            "SELECT status, worker_id FROM scraping_tasks WHERE task_id = %s",
            (task_id,),
        ).fetchone()
    if not row:
        raise LookupError(f"task {task_id} does not exist")
    return row[0], row[1]


def mark_complete(conn: Connection, task_id: UUID, worker_id: str) -> bool:
    with conn.transaction():
        row = conn.execute(
            """
            UPDATE scraping_tasks
            SET status = 'COMPLETED',
                raw_file_path = %s,
                completed_at = now(),
                lease_until = NULL,
                updated_at = now()
            WHERE task_id = %s
              AND worker_id = %s
              AND status = 'PROCESSING'
            RETURNING task_id
            """,
            (f"{task_id}.html.gz", task_id, worker_id),
        ).fetchone()
        if row:
            return False

        current = conn.execute(
            "SELECT status FROM scraping_tasks WHERE task_id = %s FOR UPDATE",
            (task_id,),
        ).fetchone()
        if current and current[0] == "COMPLETED":
            return True
        if not current:
            raise LookupError(f"task {task_id} does not exist")
        raise PermissionError("task is not leased to this worker")


def fail_task(
    conn: Connection, task_id: UUID, worker_id: str, reason: str, retryable: bool
) -> str:
    with conn.transaction():
        row = conn.execute(
            """
            UPDATE scraping_tasks
            SET status = CASE
                    WHEN %s AND attempt_count < max_attempts THEN 'PENDING'
                    ELSE 'FAILED'
                END,
                worker_id = NULL,
                lease_until = NULL,
                last_error = %s,
                updated_at = now()
            WHERE task_id = %s
              AND worker_id = %s
              AND status = 'PROCESSING'
            RETURNING status
            """,
            (retryable, reason[:2000], task_id, worker_id),
        ).fetchone()
        if not row:
            current = conn.execute(
                "SELECT status FROM scraping_tasks WHERE task_id = %s",
                (task_id,),
            ).fetchone()
            if not current:
                raise LookupError(f"task {task_id} does not exist")
            if current[0] in {"COMPLETED", "FAILED"}:
                return current[0]
            raise PermissionError("task is not leased to this worker")
        return row[0]


def heartbeat_worker(
    conn: Connection, worker_id: str, version: str, hostname: str
) -> None:
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO workers (worker_id, version, hostname, last_seen_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (worker_id) DO UPDATE
            SET version = EXCLUDED.version,
                hostname = EXCLUDED.hostname,
                last_seen_at = now()
            """,
            (worker_id, version, hostname),
        )


def get_target_version(conn: Connection) -> str:
    row = conn.execute(
        "SELECT value FROM system_config WHERE key = 'target_version'"
    ).fetchone()
    return row[0] if row else "v1.0.0"
