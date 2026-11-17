#!/usr/bin/env python3
"""Start a local PostgreSQL (via the `pgserver` wheel, no root needed), create
the ops role + faultlogs database, apply db/init.sql, and print a conninfo
string on the last line. Idempotent: re-running reuses the existing cluster.

pgserver stops the server when the owning Python process exits, so for a
long-lived database run this with --keep-alive in the background; it writes
the DSN to <pgdata>/dsn.txt and then sleeps until killed.

Usage:
  python3 tests/local_db.py /tmp/ops-pgdata                 # setup once, print DSN
  python3 tests/local_db.py /tmp/ops-pgdata --keep-alive    # setup + stay alive
"""

import pathlib
import sys
import time

import pgserver
import psycopg

WORKDIR = pathlib.Path(__file__).resolve().parent.parent
INIT_SQL = WORKDIR / "db" / "init.sql"


def main():
    pgdata = sys.argv[1]
    keep_alive = "--keep-alive" in sys.argv[2:]
    srv = pgserver.get_server(pgdata)
    uri = srv.get_uri()  # unix-socket URI for the maintenance db
    print(f"# postgres up: {uri}", file=sys.stderr)

    with psycopg.connect(uri, autocommit=True) as conn:
        conn.execute(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ops') "
            "THEN CREATE ROLE ops LOGIN PASSWORD 'ops'; END IF; END $$"
        )
        if not conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = 'faultlogs'"
        ).fetchone():
            conn.execute("CREATE DATABASE faultlogs OWNER ops")

    # conninfo dict -> connect to faultlogs as ops
    kw = psycopg.conninfo.conninfo_to_dict(uri)
    kw.update(dbname="faultlogs", user="ops", password="ops")
    dsn = psycopg.conninfo.make_conninfo(**kw)

    with psycopg.connect(dsn, autocommit=True) as conn:
        if not conn.execute("SELECT to_regclass('index_build_jobs')").fetchone()[0]:
            print("# applying db/init.sql (preload takes a while)...", file=sys.stderr)
            # Multiple statements in one simple-query message run inside an
            # implicit transaction, which VACUUM forbids -> run it separately.
            sql = INIT_SQL.read_text()
            body, _, _vacuum = sql.partition("VACUUM ANALYZE device_fault_logs;")
            conn.execute(body)
            conn.execute("VACUUM ANALYZE device_fault_logs")
        version = conn.execute("SHOW server_version").fetchone()[0]
        n = conn.execute("SELECT count(*) FROM device_fault_logs").fetchone()[0]
        print(f"# server_version={version} preloaded_logs={n}", file=sys.stderr)

    pathlib.Path(pgdata, "dsn.txt").write_text(dsn)
    print(dsn)  # last stdout line: consumable by callers

    if keep_alive:
        print("# keep-alive: holding postgres open (kill to stop)", file=sys.stderr)
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
