"""Coordination of online (CONCURRENTLY) index builds across API instances.

All coordination state lives in PostgreSQL itself, so any number of stateless
API instances can run concurrently without a leader:

* claim a build          -> INSERT ... ON CONFLICT (index_name) DO NOTHING
                            (unique constraint => exactly one winner)
* serialize decide/clean -> session-level advisory lock around the critical
                            section that inspects the catalog, drops invalid
                            leftovers and claims a new build
* fence late completions -> UPDATE ... WHERE id = <job> AND status = 'building'
                            affects 0 rows once a newer job has replaced the row,
                            so a late old job can never overwrite new results
* ground truth           -> pg_index.indisvalid / indisready, pg_get_indexdef,
                            pg_stat_progress_create_index / pg_stat_activity

"ready" is only ever reported after the database catalog confirms the index
exists, is valid + ready, and has the expected definition.
"""

import logging
import os
import socket
import threading
import uuid

from .db import connect

log = logging.getLogger("index_manager")

INDEX_NAME = "idx_fault_logs_device_occurred"
TABLE_NAME = "device_fault_logs"
EXPECTED_COLUMNS = "(device_id, occurred_at)"

# Fixed advisory-lock key serializing cleanup/claim across all instances.
ADVISORY_LOCK_KEY = 861_227_001

# A 'building' job younger than this is assumed alive even if its CREATE INDEX
# is not visible in pg_stat_activity yet (claim -> statement start race), or
# briefly after its backend died. After the grace period, a 'building' job with
# no active build backend is treated as orphaned.
GRACE_SECONDS = float(os.environ.get("ORPHAN_GRACE_SECONDS", "20"))

INSTANCE_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

CREATE_INDEX_SQL = (
    f'CREATE INDEX CONCURRENTLY "{INDEX_NAME}" '
    f"ON {TABLE_NAME} (device_id, occurred_at)"
)
DROP_INDEX_SQL = f'DROP INDEX CONCURRENTLY IF EXISTS "{INDEX_NAME}"'


# --------------------------------------------------------------------------
# Catalog inspection
# --------------------------------------------------------------------------

def _catalog_state(conn) -> dict:
    row = conn.execute(
        """
        SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid)
        FROM pg_class c
        JOIN pg_index i      ON i.indexrelid = c.oid
        JOIN pg_namespace n  ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = current_schema()
          AND c.relkind = 'i'
        """,
        (INDEX_NAME,),
    ).fetchone()
    if not row:
        return {"exists": False, "valid": False, "ready": False, "definition": None}
    return {"exists": True, "valid": bool(row[0]), "ready": bool(row[1]),
            "definition": row[2]}


def _definition_ok(definition) -> bool:
    if not definition:
        return False
    norm = " ".join(str(definition).split())
    return (
        TABLE_NAME in norm
        and EXPECTED_COLUMNS in norm
        and "USING btree" in norm
    )


def _catalog_ok(cat: dict) -> bool:
    """True only if the catalog proves the index is usable and correct."""
    return (
        cat["exists"]
        and cat["valid"]
        and cat["ready"]
        and _definition_ok(cat["definition"])
    )


def _active_builds(conn) -> int:
    """Number of backends currently running CREATE/DROP INDEX on our index."""
    in_progress = conn.execute(
        """
        SELECT count(*)
        FROM pg_stat_progress_create_index p
        JOIN pg_class c ON c.oid = p.index_relid
        WHERE c.relname = %s
        """,
        (INDEX_NAME,),
    ).fetchone()[0]
    # Only DDL statements count; monitoring queries merely *mention* the index
    # name (psycopg binds parameters client-side, so their text contains it).
    statements = conn.execute(
        """
        SELECT count(*)
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND state = 'active'
          AND query ~* '^\\s*(create|drop)\\s+index\\b'
          AND query ILIKE %s
        """,
        (f"%{INDEX_NAME}%",),
    ).fetchone()[0]
    # The same backend can appear in both views; what matters is "is anyone
    # actively working on this index right now".
    return max(int(in_progress), int(statements))


# --------------------------------------------------------------------------
# Job ledger
# --------------------------------------------------------------------------

def _get_job(conn):
    row = conn.execute(
        """
        SELECT id, status, builder, error, created_at, updated_at,
               EXTRACT(EPOCH FROM (now() - created_at))::float AS age_s
        FROM index_build_jobs
        WHERE index_name = %s
        """,
        (INDEX_NAME,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "status": row[1], "builder": row[2], "error": row[3],
        "created_at": row[4], "updated_at": row[5], "age_s": float(row[6]),
    }


def _is_young(job: dict) -> bool:
    return job["age_s"] < GRACE_SECONDS


def _transition(conn, job_id, to_status, error, from_status) -> int:
    """Fenced state change: only applies if the row is still in from_status."""
    cur = conn.execute(
        """
        UPDATE index_build_jobs
        SET status = %s, error = %s, updated_at = now()
        WHERE id = %s AND status = %s
        """,
        (to_status, error, job_id, from_status),
    )
    return cur.rowcount


def _finish(job_id: uuid.UUID, status: str, error) -> None:
    """Record a build outcome; a replaced job row makes this a no-op."""
    try:
        with connect(autocommit=True) as conn:
            n = _transition(conn, job_id, status, error, "building")
            if n == 0:
                log.info(
                    "stale finish for job %s ignored (row replaced by a newer job)",
                    job_id,
                )
    except Exception:
        log.exception("could not record build result for job %s", job_id)


# --------------------------------------------------------------------------
# Build worker
# --------------------------------------------------------------------------

_build_threads: dict = {}
_build_threads_lock = threading.Lock()


def _start_build_thread(job_id: uuid.UUID) -> None:
    t = threading.Thread(
        target=_run_build, args=(job_id,),
        name=f"index-build-{job_id}", daemon=True,
    )
    with _build_threads_lock:
        _build_threads[str(job_id)] = t
    t.start()


def _run_build(job_id: uuid.UUID) -> None:
    log.info("job %s: starting %s", job_id, CREATE_INDEX_SQL)
    error = None
    try:
        with connect(autocommit=True) as conn:
            conn.execute(CREATE_INDEX_SQL)
    except Exception as exc:  # e.g. canceled, deadlock, connection lost
        error = f"CREATE INDEX CONCURRENTLY failed: {exc}"
        log.warning("job %s: %s", job_id, error)

    if error is None:
        # Report ready only after the catalog confirms validity + definition.
        try:
            with connect(autocommit=True) as conn:
                cat = _catalog_state(conn)
            if _catalog_ok(cat):
                _finish(job_id, "ready", None)
                log.info("job %s: index verified valid in catalog -> ready", job_id)
                return
            error = f"catalog verification failed after build: {cat}"
        except Exception as exc:
            error = f"catalog verification error: {exc}"
        log.warning("job %s: %s", job_id, error)

    _finish(job_id, "failed", error)


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------

def _resp(state, job, active, cat, detail="", **extra):
    r = {
        "state": state,
        "detail": detail,
        "index_name": INDEX_NAME,
        "job_id": str(job["id"]) if job else None,
        "active_build_backends": active,
        "catalog": cat,
        "handled_by": INSTANCE_ID,
    }
    if job:
        r["job"] = {
            "id": str(job.get("id")),
            "status": job.get("status"),
            "builder": job.get("builder"),
            "error": job.get("error"),
            "created_at": (job["created_at"].isoformat()
                           if job.get("created_at") else None),
            "updated_at": (job["updated_at"].isoformat()
                           if job.get("updated_at") else None),
        }
    r.update(extra)
    return r


# --------------------------------------------------------------------------
# Public operations
# --------------------------------------------------------------------------

def request_build() -> dict:
    """Idempotently request the index build.

    Concurrent callers (across all instances) collapse onto a single build:
    the first one claims it, the others observe 'building'.
    """
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        try:
            return _request_build_locked(conn)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))


def _request_build_locked(conn) -> dict:
    job = _get_job(conn)
    cat = _catalog_state(conn)
    active = _active_builds(conn)

    # 1) A build is already claimed.
    if job and job["status"] == "building":
        if active > 0 or _is_young(job):
            return _resp("building", job, active, cat,
                         detail="build already in progress")
        # Dead claim: no backend is building and the grace period has passed.
        _transition(conn, job["id"], "failed",
                    "orphaned build: no active backend", "building")
        job = _get_job(conn)

    # 2) Already built: confirm against the catalog before saying ready.
    if job and job["status"] == "ready":
        if _catalog_ok(cat):
            return _resp("ready", job, active, cat,
                         detail="index verified valid in pg_catalog")
        _transition(conn, job["id"], "failed",
                    "catalog mismatch: index missing/invalid or definition changed",
                    "ready")
        job = _get_job(conn)
        cat = _catalog_state(conn)

    # 3) Clean up invalid leftovers -- but never while a build is active:
    #    an invalid catalog entry may simply be an in-progress CONCURRENTLY
    #    build, which must not be treated as junk.
    dropped_leftover = False
    if cat["exists"] and not cat["valid"]:
        if active > 0:
            return _resp("building", job, active, cat,
                         detail="invalid catalog entry belongs to an in-progress "
                                "build; left untouched")
        conn.execute(DROP_INDEX_SQL)
        dropped_leftover = True
        log.info("dropped invalid leftover index %s", INDEX_NAME)
        cat = _catalog_state(conn)

    # 4) Clear the failed job row so a fresh claim can be inserted.
    if job and job["status"] == "failed":
        conn.execute(
            "DELETE FROM index_build_jobs WHERE id = %s AND status = 'failed'",
            (job["id"],),
        )
        job = None

    # 5) A valid index may already exist (created outside this tool).
    if cat["exists"] and cat["valid"]:
        if _catalog_ok(cat):
            conn.execute(
                """
                INSERT INTO index_build_jobs (id, index_name, status, builder)
                VALUES (%s, %s, 'ready', %s)
                ON CONFLICT (index_name) DO NOTHING
                """,
                (uuid.uuid4(), INDEX_NAME, INSTANCE_ID),
            )
            return _resp("ready", _get_job(conn), active, cat,
                         detail="valid index already present")
        return _resp("conflict", job, active, cat,
                     detail=f"index name {INDEX_NAME} is used by a different "
                            f"definition: {cat['definition']}")

    # 6) Claim the build. The UNIQUE constraint guarantees a single winner.
    new_id = uuid.uuid4()
    cur = conn.execute(
        """
        INSERT INTO index_build_jobs (id, index_name, status, builder)
        VALUES (%s, %s, 'building', %s)
        ON CONFLICT (index_name) DO NOTHING
        """,
        (new_id, INDEX_NAME, INSTANCE_ID),
    )
    if cur.rowcount == 1:
        _start_build_thread(new_id)
        return _resp("started", _get_job(conn), active, cat,
                     detail="build claimed by this instance",
                     dropped_invalid_leftover=dropped_leftover)
    job = _get_job(conn)
    return _resp(job["status"], job, active, cat,
                 detail="build claimed concurrently by another instance")


def get_status() -> dict:
    """Report job + live catalog state; converge orphaned claims to failed."""
    with connect(autocommit=True) as conn:
        job = _get_job(conn)
        cat = _catalog_state(conn)
        active = _active_builds(conn)
        note = ""
        if (job and job["status"] == "building"
                and active == 0 and not _is_young(job)):
            _transition(conn, job["id"], "failed",
                        "orphaned build: no active backend (status check)",
                        "building")
            job = _get_job(conn)
            note = "orphaned build marked failed"

        if job is None:
            if not cat["exists"]:
                state = "not_started"
            elif _catalog_ok(cat):
                state = "untracked_valid"
            else:
                state = "invalid_leftover"
        elif job["status"] == "ready":
            # Never report ready unless the catalog confirms it right now.
            state = "ready" if _catalog_ok(cat) else "inconsistent"
        else:
            state = job["status"]
        return _resp(state, job, active, cat, detail=note)


def cleanup() -> dict:
    """Drop invalid leftover indexes once no build is active; allow retry."""
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        try:
            job = _get_job(conn)
            cat = _catalog_state(conn)
            active = _active_builds(conn)
            if active > 0 or (job and job["status"] == "building"
                              and _is_young(job)):
                return _resp("busy", job, active, cat,
                             detail="active build detected; refusing to clean up",
                             cleaned=False)
            dropped = False
            if cat["exists"] and not cat["valid"]:
                conn.execute(DROP_INDEX_SQL)
                dropped = True
                log.info("cleanup: dropped invalid leftover index %s", INDEX_NAME)
                cat = _catalog_state(conn)
            cleared = False
            if job and not (job["status"] == "ready" and _catalog_ok(cat)):
                conn.execute(
                    "DELETE FROM index_build_jobs WHERE id = %s AND status <> 'ready'",
                    (job["id"],),
                )
                cleared = True
                job = None
            return _resp("cleaned", job, active, cat,
                         detail="invalid leftovers removed" if dropped
                                else "nothing to clean",
                         cleaned=True, dropped_invalid_index=dropped,
                         cleared_job=cleared)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))


def reconcile() -> None:
    """Run at startup: make tracked state consistent with the actual index."""
    try:
        with connect(autocommit=True) as conn:
            job = _get_job(conn)
            if not job:
                log.info("reconcile: no tracked job")
                return
            cat = _catalog_state(conn)
            active = _active_builds(conn)
            if job["status"] == "building":
                if active > 0 or _is_young(job):
                    log.info("reconcile: build %s appears active, keeping state",
                             job["id"])
                    return
                if _catalog_ok(cat):
                    _transition(conn, job["id"], "ready", None, "building")
                    log.info("reconcile: build %s had completed; marked ready",
                             job["id"])
                else:
                    _transition(conn, job["id"], "failed",
                                "orphaned build: instance exited before completion",
                                "building")
                    log.info("reconcile: orphaned build %s marked failed",
                             job["id"])
            elif job["status"] == "ready" and not _catalog_ok(cat):
                _transition(conn, job["id"], "failed",
                            "index missing/invalid at startup", "ready")
                log.info("reconcile: job %s was ready but catalog disagrees; "
                         "marked failed", job["id"])
    except Exception:
        log.exception("reconcile failed")
