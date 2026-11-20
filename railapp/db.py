"""SQLite access helpers and schema."""

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from . import domain

SCHEMA = """
CREATE TABLE IF NOT EXISTS contracts (
    downstream    TEXT PRIMARY KEY,
    columns_json  TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS days (
    day          TEXT PRIMARY KEY,
    current_rev  INTEGER NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publications (
    day                    TEXT NOT NULL,
    rev                    INTEGER NOT NULL,
    original_name          TEXT NOT NULL,
    original_size          INTEGER NOT NULL,
    original_sha256        TEXT NOT NULL,
    settlement_sha256      TEXT NOT NULL,
    settlement_size        INTEGER NOT NULL,
    ridership_sha256       TEXT NOT NULL,
    ridership_size         INTEGER NOT NULL,
    settlement_fingerprint TEXT NOT NULL,
    ridership_fingerprint  TEXT NOT NULL,
    settlement_version     INTEGER NOT NULL,
    ridership_version      INTEGER NOT NULL,
    settlement_columns     TEXT NOT NULL,
    ridership_columns      TEXT NOT NULL,
    row_count              INTEGER NOT NULL,
    created_at             TEXT NOT NULL,
    PRIMARY KEY (day, rev)
);

CREATE TABLE IF NOT EXISTS previews (
    id                     TEXT PRIMARY KEY,
    day                    TEXT NOT NULL,
    base_rev               INTEGER NOT NULL,
    settlement_version     INTEGER NOT NULL,
    ridership_version      INTEGER NOT NULL,
    original_name          TEXT NOT NULL,
    original_size          INTEGER NOT NULL,
    original_sha256        TEXT NOT NULL,
    result_json            TEXT NOT NULL,
    convertible            INTEGER NOT NULL,
    created_at             TEXT NOT NULL,
    expires_at             TEXT NOT NULL,
    consumed_at            TEXT
);

-- Trace (追查) records.  These tables are audit trails: rows are never
-- updated or deleted when a preview is cleaned up, a new revision is
-- published, or the service restarts.
CREATE TABLE IF NOT EXISTS uploads (
    id            TEXT PRIMARY KEY,
    day           TEXT,
    filename      TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    received_at   TEXT NOT NULL,
    status        TEXT NOT NULL,
    detail_json   TEXT NOT NULL,
    preview_id    TEXT,
    preview_json  TEXT
);

CREATE TABLE IF NOT EXISTS confirmations (
    id              TEXT PRIMARY KEY,
    preview_id      TEXT NOT NULL,
    upload_id       TEXT,
    day             TEXT,
    filename        TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    outcome         TEXT,  -- NULL = interrupted, 'published', 'failed'
    failure_kind    TEXT,  -- 'preview_invalid' | 'file_error' | 'publish_failed'
    failure_code    TEXT,
    failure_message TEXT,
    http_status     INTEGER,
    published_rev   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_publications_day ON publications(day, rev);
CREATE INDEX IF NOT EXISTS idx_previews_expiry ON previews(expires_at);
CREATE INDEX IF NOT EXISTS idx_uploads_day ON uploads(day, received_at);
CREATE INDEX IF NOT EXISTS idx_uploads_preview ON uploads(preview_id);
CREATE INDEX IF NOT EXISTS idx_confirmations_day ON confirmations(day, started_at);
CREATE INDEX IF NOT EXISTS idx_confirmations_preview ON confirmations(preview_id);
"""

PREVIEW_TTL_SECONDS = 2 * 60 * 60


def utcnow():
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(moment):
    return moment.isoformat().replace("+00:00", "Z")


def connect(path):
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


@contextmanager
def transaction(conn, *, immediate=False):
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def init_db(conn):
    conn.executescript(SCHEMA)
    now = iso(utcnow())
    for downstream in domain.DOWNSTREAM_ORDER:
        columns = domain.DEFAULT_CONTRACTS[downstream]
        conn.execute(
            "INSERT OR IGNORE INTO contracts VALUES (?, ?, ?, ?, ?)",
            (
                downstream,
                json.dumps(columns, ensure_ascii=False),
                domain.fingerprint(columns),
                1,
                now,
            ),
        )


def get_contract(conn, downstream):
    row = conn.execute(
        """SELECT downstream, columns_json, fingerprint, version, updated_at
           FROM contracts WHERE downstream = ?""",
        (downstream,),
    ).fetchone()
    if row is None:
        return None
    return {
        "downstream": row["downstream"],
        "columns": json.loads(row["columns_json"]),
        "fingerprint": row["fingerprint"],
        "version": int(row["version"]),
        "updated_at": row["updated_at"],
    }


def get_contracts(conn):
    return {name: get_contract(conn, name) for name in domain.DOWNSTREAM_ORDER}


def update_contract(conn, downstream, columns):
    """Replace a contract and bump its version.  Never touches publications.

    A no-op edit (identical canonical columns) does not bump the version, so
    it cannot invalidate previews that are still pending confirmation.
    """
    fp = domain.fingerprint(columns)
    current = get_contract(conn, downstream)
    if current is not None and current["fingerprint"] == fp:
        return current
    conn.execute(
        """UPDATE contracts
           SET columns_json = ?, fingerprint = ?, version = version + 1, updated_at = ?
           WHERE downstream = ?""",
        (json.dumps(columns, ensure_ascii=False), fp, iso(utcnow()), downstream),
    )
    return get_contract(conn, downstream)


def day_revision(conn, day):
    row = conn.execute("SELECT current_rev FROM days WHERE day = ?", (day,)).fetchone()
    return int(row["current_rev"]) if row else 0


def get_publication(conn, day, rev):
    row = conn.execute(
        "SELECT * FROM publications WHERE day = ? AND rev = ?", (day, rev)
    ).fetchone()
    return dict(row) if row else None


def list_publications(conn, day, limit=200):
    rows = conn.execute(
        "SELECT * FROM publications WHERE day = ? ORDER BY rev DESC LIMIT ?", (day, limit)
    ).fetchall()
    return [dict(r) for r in rows]


def list_days(conn, limit=200):
    rows = conn.execute(
        """SELECT d.day, d.current_rev, d.updated_at,
                  (SELECT count(*) FROM publications p WHERE p.day = d.day) AS revision_count
           FROM days d ORDER BY d.day DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def create_preview(conn, *, preview_id, day, base_rev, contract_versions,
                   original_name, original_size, original_sha256, result,
                   convertible):
    now = utcnow()
    expires = now + timedelta(seconds=PREVIEW_TTL_SECONDS)
    conn.execute(
        """INSERT INTO previews (id, day, base_rev, settlement_version,
                                 ridership_version, original_name, original_size,
                                 original_sha256, result_json, convertible,
                                 created_at, expires_at, consumed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
        (
            preview_id,
            day,
            base_rev,
            contract_versions["settlement"],
            contract_versions["ridership"],
            original_name,
            original_size,
            original_sha256,
            json.dumps(result, ensure_ascii=False),
            1 if convertible else 0,
            iso(now),
            iso(expires),
        ),
    )
    return iso(expires)


def get_preview(conn, preview_id):
    row = conn.execute("SELECT * FROM previews WHERE id = ?", (preview_id,)).fetchone()
    if row is None:
        return None
    item = dict(row)
    item["result"] = json.loads(item.pop("result_json"))
    return item


def delete_preview(conn, preview_id):
    conn.execute("DELETE FROM previews WHERE id = ?", (preview_id,))


def purge_expired_previews(conn, now=None):
    now = now or utcnow()
    rows = conn.execute(
        "SELECT id FROM previews WHERE expires_at < ?", (iso(now),)
    ).fetchall()
    ids = [r["id"] for r in rows]
    if ids:
        conn.executemany("DELETE FROM previews WHERE id = ?", [(i,) for i in ids])
    return ids


def wait_for_write_lock(conn, attempts=200, delay=0.05):
    """Best-effort helper used by tests to serialise around a busy database."""
    for _ in range(attempts):
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("ROLLBACK")
            return
        except sqlite3.OperationalError:
            time.sleep(delay)
    raise sqlite3.OperationalError("database is locked")


# ----------------------------------------------------------------------
# trace records (追查): upload attempts and confirmation attempts
# ----------------------------------------------------------------------

def create_upload(conn, *, upload_id, day, filename, size_bytes, received_at,
                  status, detail, preview_id=None, preview=None):
    conn.execute(
        """INSERT INTO uploads (id, day, filename, size_bytes, received_at,
                                status, detail_json, preview_id, preview_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            upload_id,
            day,
            filename,
            size_bytes,
            received_at,
            status,
            json.dumps(detail, ensure_ascii=False),
            preview_id,
            json.dumps(preview, ensure_ascii=False) if preview is not None else None,
        ),
    )


def get_upload_by_preview(conn, preview_id):
    row = conn.execute(
        "SELECT * FROM uploads WHERE preview_id = ? ORDER BY rowid DESC LIMIT 1",
        (preview_id,),
    ).fetchone()
    return dict(row) if row else None


def create_confirmation(conn, *, confirmation_id, preview_id, upload_id, day,
                        filename, started_at):
    conn.execute(
        """INSERT INTO confirmations (id, preview_id, upload_id, day, filename,
                                      started_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (confirmation_id, preview_id, upload_id, day, filename, started_at),
    )


def finish_confirmation(conn, confirmation_id, *, outcome, finished_at,
                        failure_kind=None, failure_code=None,
                        failure_message=None, http_status=None,
                        published_rev=None):
    conn.execute(
        """UPDATE confirmations
           SET outcome = ?, finished_at = ?, failure_kind = ?, failure_code = ?,
               failure_message = ?, http_status = ?, published_rev = ?
           WHERE id = ?""",
        (
            outcome,
            finished_at,
            failure_kind,
            failure_code,
            failure_message,
            http_status,
            published_rev,
            confirmation_id,
        ),
    )


def list_uploads_for_day(conn, day, limit=500):
    rows = conn.execute(
        "SELECT * FROM uploads WHERE day = ? ORDER BY received_at DESC, rowid DESC LIMIT ?",
        (day, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def list_confirmations_for_day(conn, day, limit=1000):
    rows = conn.execute(
        "SELECT * FROM confirmations WHERE day = ? ORDER BY started_at DESC, rowid DESC LIMIT ?",
        (day, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def list_confirmations_without_day(conn, limit=200):
    rows = conn.execute(
        "SELECT * FROM confirmations WHERE day IS NULL ORDER BY started_at DESC, rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def trace_day_summary(conn):
    """Per-day counts of uploads and confirmation outcomes for the trace index."""
    upload_rows = conn.execute(
        """SELECT COALESCE(day, '') AS day, COUNT(*) AS uploads,
                  MAX(received_at) AS last_at
           FROM uploads GROUP BY COALESCE(day, '')"""
    ).fetchall()
    confirmation_rows = conn.execute(
        """SELECT COALESCE(day, '') AS day, COUNT(*) AS confirmations,
                  SUM(CASE WHEN outcome = 'published' THEN 1 ELSE 0 END) AS published,
                  SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) AS failed,
                  SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END) AS interrupted,
                  MAX(started_at) AS last_at
           FROM confirmations GROUP BY COALESCE(day, '')"""
    ).fetchall()
    return upload_rows, confirmation_rows


def preview_exists(conn, preview_id):
    if not preview_id:
        return False
    row = conn.execute(
        "SELECT 1 FROM previews WHERE id = ?", (preview_id,)
    ).fetchone()
    return row is not None
