import os
import uuid
from concurrent.futures import ThreadPoolExecutor
import unittest
from pathlib import Path


class AcquisitionQueryTests(unittest.TestCase):
    def test_task_claim_uses_postgres_skip_locked(self):
        source = (
            Path(__file__).resolve().parents[1]
            / "master"
            / "services"
            / "task_service.py"
        ).read_text(encoding="utf-8")
        self.assertIn("FOR UPDATE SKIP LOCKED", source)

    @unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "TEST_DATABASE_URL not set")
    def test_fifty_concurrent_workers_receive_unique_tasks(self):
        import psycopg
        from psycopg import sql

        from master.services.task_service import acquire_task

        database_url = os.environ["TEST_DATABASE_URL"]
        schema = f"concurrency_test_{uuid.uuid4().hex}"
        with psycopg.connect(database_url, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema))
            )
            conn.execute(
                """
                CREATE TABLE workers (
                    worker_id text PRIMARY KEY,
                    version text NOT NULL,
                    hostname text NOT NULL DEFAULT '',
                    last_seen_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE scraping_tasks (
                    task_id uuid PRIMARY KEY,
                    task_key text NOT NULL,
                    target_url text NOT NULL,
                    task_type text NOT NULL DEFAULT 'text_search',
                    search_keyword text NOT NULL DEFAULT '',
                    search_location text NOT NULL DEFAULT '',
                    order_id text NOT NULL DEFAULT '',
                    query_type text NOT NULL DEFAULT 'Unknown',
                    minimum_reviews integer NOT NULL DEFAULT 0,
                    priority integer NOT NULL DEFAULT 0,
                    status text NOT NULL DEFAULT 'PENDING',
                    worker_id text,
                    attempt_count integer NOT NULL DEFAULT 0,
                    max_attempts integer NOT NULL DEFAULT 4,
                    lease_until timestamptz,
                    started_at timestamptz,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO scraping_tasks (task_id, task_key, target_url)
                VALUES (%s, %s, %s)
                """,
                [
                    (uuid.uuid4(), f"task-{index}", f"https://example.test/{index}")
                    for index in range(50)
                ],
            )

        def acquire(index):
            with psycopg.connect(database_url) as conn:
                conn.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(schema))
                )
                task, _ = acquire_task(conn, f"worker-{index}", "test", 900)
                return task.task_id if task else None

        try:
            with ThreadPoolExecutor(max_workers=50) as pool:
                claimed = list(pool.map(acquire, range(50)))
            self.assertEqual(len(claimed), 50)
            self.assertEqual(len(set(claimed)), 50)
        finally:
            with psycopg.connect(database_url, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )


if __name__ == "__main__":
    unittest.main()
