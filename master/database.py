from collections.abc import Generator

from fastapi import Request
from psycopg import Connection
from psycopg_pool import ConnectionPool


def create_pool(database_url: str, min_size: int, max_size: int) -> ConnectionPool:
    return ConnectionPool(
        conninfo=database_url,
        min_size=min_size,
        max_size=max_size,
        open=False,
    )


def get_db_conn(request: Request) -> Generator[Connection, None, None]:
    pool: ConnectionPool = request.app.state.db_pool
    with pool.connection() as conn:
        yield conn
