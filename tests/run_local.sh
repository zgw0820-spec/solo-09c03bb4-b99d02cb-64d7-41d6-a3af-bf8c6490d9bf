#!/usr/bin/env bash
# Run the full self-test locally (no Docker): pgserver PostgreSQL + two
# uvicorn instances on 127.0.0.1:8001/8002.
set -euo pipefail

VENV=${VENV:-/tmp/ops-venv}
PGDATA=${PGDATA:-/tmp/ops-pgdata}
cd "$(dirname "$0")/.."

rm -f "$PGDATA/dsn.txt"
"$VENV/bin/python" tests/local_db.py "$PGDATA" --keep-alive &
DBKEEPER=$!
trap 'kill $DBKEEPER 2>/dev/null || true' EXIT

until [ -f "$PGDATA/dsn.txt" ]; do sleep 1; done
DSN=$(cat "$PGDATA/dsn.txt")
echo "using DSN: $DSN"

"$VENV/bin/python" tests/selftest.py --mode local --dsn "$DSN" --workdir "$PWD"
