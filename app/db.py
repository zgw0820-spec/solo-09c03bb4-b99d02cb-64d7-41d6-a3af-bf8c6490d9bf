"""Database connection helpers.

The app intentionally uses short-lived connections instead of a pool: request
volume is modest, and the index-build path needs plain autocommit connections
anyway (CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction block).
"""

import os
import time

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ops:ops@localhost:5432/faultlogs"
)


def connect(autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, autocommit=autocommit)


def wait_for_schema(timeout_s: int = 240) -> None:
    """Block until the init script has created the schema.

    The Postgres container reports healthy via pg_isready before the init
    scripts have finished, so API instances must tolerate the tables not
    existing yet for a while.
    """
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while True:
        attempt += 1
        try:
            with connect() as conn:
                conn.execute("SELECT 1 FROM index_build_jobs LIMIT 1")
                conn.execute("SELECT 1 FROM device_fault_logs LIMIT 1")
            return
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError("database schema not ready in time") from exc
            time.sleep(min(5.0, 0.5 * attempt))
