from pathlib import Path

import psycopg
from master.config import load_settings


def main() -> None:
    database_url = load_settings().database_url
    migrations_dir = Path(__file__).resolve().parent
    files = sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.sql"))
    if not files:
        raise RuntimeError(f"No SQL migrations found in {migrations_dir}")
    with psycopg.connect(database_url) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_name text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        for path in files:
            applied = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE migration_name = %s",
                (path.name,),
            ).fetchone()
            if applied:
                continue
            with conn.transaction():
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute(
                    "INSERT INTO schema_migrations (migration_name) VALUES (%s)",
                    (path.name,),
                )
            print(f"Applied {path.name}")


if __name__ == "__main__":
    main()
