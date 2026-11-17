"""FastAPI surface: fault-log writes/queries + online index build operations."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from . import index_manager as im
from .db import connect, wait_for_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Wait for the init script, then make tracked state consistent with the
    # actual index before serving traffic.
    wait_for_schema()
    im.reconcile()
    log.info("instance %s ready", im.INSTANCE_ID)
    yield


app = FastAPI(
    title="Device Fault Log Ops",
    version="1.0.0",
    description=(
        "Fault-log ingestion plus coordinated online (CONCURRENTLY) index "
        "builds across multiple stateless API instances."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "instance": im.INSTANCE_ID}


# ---------------------------------------------------------------------------
# Fault logs
# ---------------------------------------------------------------------------

class LogIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4000)
    severity: str = Field(default="error", max_length=16)
    occurred_at: Optional[datetime] = None


@app.post("/logs", status_code=201, tags=["logs"])
def create_log(entry: LogIn):
    with connect() as conn:
        row = conn.execute(
            """
            INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)
            VALUES (%s, COALESCE(%s, now()), %s, %s)
            RETURNING id, occurred_at
            """,
            (entry.device_id, entry.occurred_at, entry.severity, entry.message),
        ).fetchone()
    return {"id": row[0], "occurred_at": row[1]}


@app.get("/logs", tags=["logs"])
def list_logs(
    device_id: str,
    start: datetime,
    end: datetime,
    limit: int = Query(100, ge=1, le=1000),
):
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, device_id, occurred_at, severity, message
            FROM device_fault_logs
            WHERE device_id = %s AND occurred_at >= %s AND occurred_at <= %s
            ORDER BY occurred_at DESC
            LIMIT %s
            """,
            (device_id, start, end, limit),
        ).fetchall()
    return {
        "count": len(rows),
        "items": [
            {"id": r[0], "device_id": r[1], "occurred_at": r[2],
             "severity": r[3], "message": r[4]}
            for r in rows
        ],
    }


@app.get("/logs/plan", tags=["logs"])
def logs_plan(device_id: str, start: datetime, end: datetime):
    """EXPLAIN the device/time-range query (shows whether the index is used)."""
    with connect() as conn:
        rows = conn.execute(
            """
            EXPLAIN (COSTS FALSE)
            SELECT id, device_id, occurred_at, severity, message
            FROM device_fault_logs
            WHERE device_id = %s AND occurred_at >= %s AND occurred_at <= %s
            ORDER BY occurred_at DESC
            LIMIT 100
            """,
            (device_id, start, end),
        ).fetchall()
    return {"plan": [r[0] for r in rows]}


# ---------------------------------------------------------------------------
# Online index build operations
# ---------------------------------------------------------------------------

@app.post("/indexes/build", tags=["index"])
def build_index():
    """Request the (device_id, occurred_at) index build.

    Idempotent: concurrent callers collapse onto a single build; once the
    catalog confirms the index is valid, callers get 'ready'.
    """
    return im.request_build()


@app.get("/indexes/status", tags=["index"])
def index_status():
    return im.get_status()


@app.post("/indexes/cleanup", tags=["index"])
def index_cleanup():
    """Drop invalid leftover indexes (only when no build is active)."""
    return im.cleanup()
