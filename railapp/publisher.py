"""Preview and publish orchestration.

Consistency model
-----------------
* A ``publication`` row names exactly three immutable blobs (original +
  settlement + ridership) and is inserted in one SQLite transaction together
  with the day's ``current_rev`` pointer.  Readers therefore never observe a
  revision with files from another revision.
* Blobs are written and fsynced *before* the transaction commits.  A crash or
  failed write leaves unreferenced blobs only; the previous revision remains
  current and fully downloadable.
* Confirmations take ``BEGIN IMMEDIATE`` and re-validate the preview snapshot
  (day revision + both contract versions) inside the lock, so out of two
  concurrent confirmations for one day exactly one succeeds.
* Publishing never reads the contract at download time: a revision's column
  contract is frozen into the row and content-addressed files.
"""

import hashlib
import json
import logging
import os
import sqlite3
import uuid

from . import convert, db, domain
from .errors import AppError, Conflict, NotFound, Unprocessable, Unavailable
from .storage import BlobStore

logger = logging.getLogger(__name__)

KINDS = ("original", "settlement", "ridership")

# Trace (追查) labels and classification --------------------------------
UPLOAD_STATUS_LABELS = {
    "preview_ok": "预览通过",
    "preview_incompatible": "预览不通过",
    "batch_rejected": "整批拒绝",
    "file_too_large": "文件超过 1 MB 上限",
    "invalid_day": "日期无效",
    "missing_file": "缺少文件",
    "error": "处理失败",
}

FAILURE_KIND_LABELS = {
    "preview_invalid": "预览失效",
    "file_error": "文件出错",
    "publish_failed": "发布登记失败",
}

# Confirmation failures grouped for the operator: the preview can no longer
# be used, the uploaded file/content is the problem, or the publish itself
# could not be registered (disk / database / internal).
_PREVIEW_INVALID_CODES = {
    "preview_not_found",
    "preview_used",
    "preview_expired",
    "stale_day_revision",
    "stale_contract",
}
_FILE_ERROR_CODES = {
    "incompatible",
    "original_checksum_failed",
    "original_unparseable",
    "result_mismatch",
}


def _classify_confirmation_failure(err):
    if isinstance(err, AppError):
        if err.code in _PREVIEW_INVALID_CODES:
            kind = "preview_invalid"
        elif err.code in _FILE_ERROR_CODES:
            kind = "file_error"
        else:
            kind = "publish_failed"
        return kind, err.code, err.message, err.status
    if isinstance(err, sqlite3.Error):
        return "publish_failed", "database_error", str(err), 503
    return "publish_failed", "internal_error", str(err), 500


def _check_day(day):
    if not isinstance(day, str):
        raise AppError("日期格式必须为 YYYY-MM-DD", code="invalid_day")
    from datetime import date

    if len(day) != 10:
        raise AppError("日期格式必须为 YYYY-MM-DD", code="invalid_day")
    try:
        parsed = date.fromisoformat(day)
    except ValueError:
        raise AppError("日期不是有效日期", code="invalid_day") from None
    if parsed.isoformat() != day:
        raise AppError("日期格式必须为 YYYY-MM-DD", code="invalid_day")
    return day


def _safe_name(name):
    name = (name or "").replace("\\", "/").split("/")[-1].strip()
    name = "".join(ch for ch in name if ch.isprintable() and ch not in "\r\n")
    if not name:
        name = "tickets.csv"
    return name[:255]


def _publication_payload(row):
    return {
        "day": row["day"],
        "rev": row["rev"],
        "created_at": row["created_at"],
        "row_count": row["row_count"],
        "original": {
            "name": row["original_name"],
            "size": row["original_size"],
            "sha256": row["original_sha256"],
        },
        "downstreams": {
            "settlement": {
                "sha256": row["settlement_sha256"],
                "size": row["settlement_size"],
                "fingerprint": row["settlement_fingerprint"],
                "contract_version": row["settlement_version"],
                "columns": json.loads(row["settlement_columns"]),
            },
            "ridership": {
                "sha256": row["ridership_sha256"],
                "size": row["ridership_size"],
                "fingerprint": row["ridership_fingerprint"],
                "contract_version": row["ridership_version"],
                "columns": json.loads(row["ridership_columns"]),
            },
        },
        "downloads": {
            kind: f"/download/{row['day']}/{row['rev']}/{kind}" for kind in KINDS
        },
    }


def _preview_public_payload(preview):
    result = preview["result"]
    return {
        "preview_id": preview["id"],
        "day": preview["day"],
        "base_rev": preview["base_rev"],
        "convertible": bool(preview["convertible"]),
        "original": result["original"],
        "contracts": {
            name: {
                "version": result["contracts"][name]["version"],
                "fingerprint": result["contracts"][name]["fingerprint"],
                "columns": result["contracts"][name]["columns"],
            }
            for name in domain.DOWNSTREAM_ORDER
        },
        "downstreams": result["downstreams"],
        "created_at": preview["created_at"],
        "expires_at": preview["expires_at"],
    }


def _upload_trace_payload(row, preview_alive):
    return {
        "id": row["id"],
        "day": row["day"],
        "filename": row["filename"],
        "size_bytes": row["size_bytes"],
        "received_at": row["received_at"],
        "status": row["status"],
        "status_label": UPLOAD_STATUS_LABELS.get(row["status"], row["status"]),
        "detail": json.loads(row["detail_json"]),
        "preview_id": row["preview_id"],
        "preview": json.loads(row["preview_json"]) if row["preview_json"] else None,
        "preview_alive": preview_alive,
        "confirmations": [],
    }


def _confirmation_trace_payload(row):
    return {
        "id": row["id"],
        "preview_id": row["preview_id"],
        "day": row["day"],
        "filename": row["filename"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        # An attempt that neither published nor recorded a failure (e.g. the
        # process died mid-confirm) is reported as interrupted.
        "outcome": row["outcome"] if row["outcome"] else "interrupted",
        "failure_kind": row["failure_kind"],
        "failure_kind_label": (
            FAILURE_KIND_LABELS.get(row["failure_kind"]) if row["failure_kind"] else None
        ),
        "failure_code": row["failure_code"],
        "failure_message": row["failure_message"],
        "http_status": row["http_status"],
        "published_rev": row["published_rev"],
    }


def _valid_day_string(day):
    if not isinstance(day, str):
        return False
    try:
        _check_day(day)
    except AppError:
        return False
    return True


def _later(first, second):
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second)


def _trace_days_payload(upload_rows, confirmation_rows, orphan_rows):
    buckets = {}

    def bucket(key):
        return buckets.setdefault(
            key,
            {
                "day": key or None,
                "uploads": 0,
                "confirmations": 0,
                "published": 0,
                "failed": 0,
                "interrupted": 0,
                "last_activity": None,
            },
        )

    for row in upload_rows:
        entry = bucket(row["day"])
        entry["uploads"] = int(row["uploads"])
        entry["last_activity"] = _later(entry["last_activity"], row["last_at"])
    for row in confirmation_rows:
        entry = bucket(row["day"])
        entry["confirmations"] = int(row["confirmations"])
        entry["published"] = int(row["published"] or 0)
        entry["failed"] = int(row["failed"] or 0)
        entry["interrupted"] = int(row["interrupted"] or 0)
        entry["last_activity"] = _later(entry["last_activity"], row["last_at"])

    unknown = buckets.pop("", None)
    valid, invalid = [], []
    for entry in buckets.values():
        (valid if _valid_day_string(entry["day"]) else invalid).append(entry)
    valid.sort(key=lambda entry: entry["day"], reverse=True)
    invalid.sort(key=lambda entry: entry["day"] or "")
    return {
        "days": valid,
        "other_days": invalid,
        "unknown_day": unknown,
        "orphan_confirmations": [_confirmation_trace_payload(r) for r in orphan_rows],
    }


class Service:
    def __init__(self, conn_factory, store: BlobStore):
        self.conn_factory = conn_factory
        self.store = store

    # ------------------------------------------------------------------
    # preview
    # ------------------------------------------------------------------
    def preview(self, day, filename, raw_bytes):
        """Run a preview, recording the upload attempt for the trace page.

        Successful previews are recorded in the same transaction as the
        preview row; rejected uploads are recorded best-effort afterwards so
        the operator can see where the daily close got stuck.
        """
        upload_id = uuid.uuid4().hex
        received_at = db.iso(db.utcnow())
        try:
            return self._run_preview(
                day, filename, raw_bytes,
                upload_id=upload_id, received_at=received_at,
            )
        except Exception as err:
            self._record_upload_failure(
                upload_id=upload_id,
                day=day if isinstance(day, str) else None,
                filename=_safe_name(filename),
                size_bytes=len(raw_bytes) if raw_bytes is not None else 0,
                received_at=received_at,
                err=err,
            )
            raise

    def _record_upload_failure(self, *, upload_id, day, filename, size_bytes,
                               received_at, err):
        if isinstance(err, AppError):
            status = err.code if err.code in UPLOAD_STATUS_LABELS else "error"
            detail = {"error": err.code, "message": err.message}
            if err.details is not None:
                detail["problem"] = err.details
        else:
            status = "error"
            detail = {"error": "internal_error", "message": str(err)}
        try:
            conn = self.conn_factory()
            try:
                with db.transaction(conn, immediate=True):
                    db.create_upload(
                        conn,
                        upload_id=upload_id,
                        day=day,
                        filename=filename,
                        size_bytes=size_bytes,
                        received_at=received_at,
                        status=status,
                        detail=detail,
                    )
            finally:
                conn.close()
        except Exception:  # the audit trail must never break the upload flow
            logger.exception("failed to record rejected upload %s", upload_id)

    def _run_preview(self, day, filename, raw_bytes, *, upload_id, received_at):
        day = _check_day(day)
        max_bytes = convert.MAX_FILE_BYTES
        if raw_bytes is None:
            raise AppError("缺少文件", code="missing_file")
        if len(raw_bytes) > max_bytes:
            raise AppError(
                f"文件超过 1 MB 上限（{len(raw_bytes)} 字节）", status=413, code="file_too_large"
            )
        try:
            parsed = convert.parse_csv_bytes(raw_bytes)
        except convert.CsvProblem as problem:
            raise Unprocessable(
                problem.message, code="batch_rejected", details=problem.to_dict()
            ) from None

        conn = self.conn_factory()
        try:
            # One read transaction gives a consistent snapshot of both
            # contracts and the day pointer even if they change mid-preview.
            conn.execute("BEGIN")
            try:
                contracts = db.get_contracts(conn)
                base_rev = db.day_revision(conn, day)
            finally:
                conn.execute("COMMIT")
            db.purge_expired_previews(conn)
            results = {}
            convertible = True
            for name in domain.DOWNSTREAM_ORDER:
                outcome = convert.convert(parsed, contracts[name]["columns"])
                results[name] = outcome
                if outcome["status"] != "ok":
                    convertible = False
            original_name = _safe_name(filename)
            original_sha = self.store.put_bytes(raw_bytes)
            for name in domain.DOWNSTREAM_ORDER:
                outcome = results[name]
                outcome["output_size"] = len(outcome["output_bytes"])
                if outcome["output_bytes"]:
                    self.store.put_bytes(outcome["output_bytes"])
                outcome.pop("output_bytes", None)

            snapshot = {
                "original": {
                    "name": original_name,
                    "size": len(raw_bytes),
                    "sha256": original_sha,
                    "row_count": parsed.row_count,
                    "header": parsed.header,
                },
                "contracts": {
                    name: {
                        "version": contracts[name]["version"],
                        "fingerprint": contracts[name]["fingerprint"],
                        "columns": contracts[name]["columns"],
                    }
                    for name in domain.DOWNSTREAM_ORDER
                },
                "downstreams": results,
                "convertible": convertible,
            }
            preview_id = uuid.uuid4().hex
            # The trace snapshot keeps everything the trace page needs after
            # the preview row itself is cleaned up.
            trace_detail = {
                "downstreams": {
                    name: {
                        "status": results[name]["status"],
                        "missing_columns": results[name]["missing_columns"],
                        "bad_row_count": results[name]["bad_row_count"],
                        "row_count": results[name]["row_count"],
                        "issues": results[name]["issues"][:5],
                        "issues_truncated": results[name]["issues_truncated"]
                        or len(results[name]["issues"]) > 5,
                    }
                    for name in domain.DOWNSTREAM_ORDER
                }
            }
            trace_preview = {
                "base_rev": base_rev,
                "convertible": convertible,
                "original": {
                    "name": original_name,
                    "size": len(raw_bytes),
                    "sha256": original_sha,
                    "row_count": parsed.row_count,
                },
                "contracts": {
                    name: {
                        "version": contracts[name]["version"],
                        "fingerprint": contracts[name]["fingerprint"],
                    }
                    for name in domain.DOWNSTREAM_ORDER
                },
            }
            with db.transaction(conn):
                expires_at = db.create_preview(
                    conn,
                    preview_id=preview_id,
                    day=day,
                    base_rev=base_rev,
                    contract_versions={
                        name: contracts[name]["version"] for name in domain.DOWNSTREAM_ORDER
                    },
                    original_name=original_name,
                    original_size=len(raw_bytes),
                    original_sha256=original_sha,
                    result=snapshot,
                    convertible=convertible,
                )
                trace_preview["expires_at"] = expires_at
                db.create_upload(
                    conn,
                    upload_id=upload_id,
                    day=day,
                    filename=original_name,
                    size_bytes=len(raw_bytes),
                    received_at=received_at,
                    status="preview_ok" if convertible else "preview_incompatible",
                    detail=trace_detail,
                    preview_id=preview_id,
                    preview=trace_preview,
                )
            preview = db.get_preview(conn, preview_id)
            return 201, _preview_public_payload(preview)
        finally:
            conn.close()

    def get_preview(self, preview_id):
        conn = self.conn_factory()
        try:
            preview = db.get_preview(conn, preview_id)
            if preview is None:
                raise NotFound("预览不存在或已失效，请重新上传", code="preview_not_found")
            return _preview_public_payload(preview)
        finally:
            conn.close()

    def discard_preview(self, preview_id):
        conn = self.conn_factory()
        try:
            with db.transaction(conn, immediate=True):
                preview = db.get_preview(conn, preview_id)
                if preview is None:
                    raise NotFound("预览不存在或已失效", code="preview_not_found")
                db.delete_preview(conn, preview_id)
            return 200, {"discarded": preview_id}
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # confirm / publish
    # ------------------------------------------------------------------
    def confirm(self, preview_id):
        conn = self.conn_factory()
        confirmation_id = uuid.uuid4().hex
        try:
            # Register the attempt in its own transaction first: if the
            # process dies mid-confirm the row survives with no outcome and
            # is reported as interrupted (中断).
            self._register_confirmation(conn, confirmation_id, preview_id)
            try:
                with db.transaction(conn, immediate=True):
                    preview = db.get_preview(conn, preview_id)
                    if preview is None:
                        raise NotFound("预览不存在或已失效，请重新上传并预览", code="preview_not_found")
                    if preview["consumed_at"]:
                        raise Conflict("该预览已用于发布，不能重复确认", code="preview_used")
                    now = db.iso(db.utcnow())
                    if preview["expires_at"] < now:
                        db.delete_preview(conn, preview_id)
                        raise Conflict("预览已过期，请重新上传并预览", code="preview_expired")

                    result = preview["result"]
                    day = preview["day"]
                    current_rev = db.day_revision(conn, day)
                    if current_rev != preview["base_rev"]:
                        raise Conflict(
                            f"该日已发布到修订号 {current_rev}，预览基于修订号 "
                            f"{preview['base_rev']}，请重新预览后再确认",
                            code="stale_day_revision",
                        )

                    contracts = db.get_contracts(conn)
                    for name in domain.DOWNSTREAM_ORDER:
                        snapshot = result["contracts"][name]
                        if contracts[name]["version"] != preview[name + "_version"] or (
                            contracts[name]["fingerprint"] != snapshot["fingerprint"]
                        ):
                            raise Conflict(
                                f"{domain.label_for(name)} 契约在预览后已变更，"
                                "请重新预览后再确认",
                                code="stale_contract",
                            )

                    if not preview["convertible"] or not result.get("convertible"):
                        raise Unprocessable(
                            "存在不兼容的列或行，不能发布；请修正 CSV 或契约后重新预览",
                            code="incompatible",
                        )

                    # Re-materialise from the stored original bytes and the frozen
                    # contract snapshot, and verify it matches what was previewed.
                    original_bytes = self.store.read_bytes(preview["original_sha256"])
                    if hashlib.sha256(original_bytes).hexdigest() != preview["original_sha256"]:
                        raise Unavailable(
                            "预览原件校验失败，请重新上传并预览",
                            code="original_checksum_failed",
                        )
                    try:
                        parsed = convert.parse_csv_bytes(original_bytes)
                    except convert.CsvProblem:
                        raise Unavailable(
                            "预览原件无法再次解析，请重新上传并预览",
                            code="original_unparseable",
                        ) from None

                    outputs = {}
                    for name in domain.DOWNSTREAM_ORDER:
                        snapshot = result["contracts"][name]
                        outcome = convert.convert(parsed, snapshot["columns"])
                        if outcome["status"] != "ok":
                            raise Conflict(
                                "当前契约与预览结果不一致，请重新预览", code="stale_contract"
                            )
                        expected = result["downstreams"][name]["output_sha256"]
                        actual = hashlib.sha256(outcome["output_bytes"]).hexdigest()
                        if expected != actual:
                            raise Unavailable(
                                "预览结果校验失败，请重新预览", code="result_mismatch"
                            )
                        outputs[name] = (actual, len(outcome["output_bytes"]), outcome["output_bytes"])
                        if not self.store.exists(actual):
                            self.store.put_bytes(outcome["output_bytes"])

                    # Blobs are durable; now commit the row + pointer atomically.
                    new_rev = current_rev + 1
                    created_at = db.iso(db.utcnow())
                    settlement_cols = json.dumps(result["contracts"]["settlement"]["columns"], ensure_ascii=False)
                    ridership_cols = json.dumps(result["contracts"]["ridership"]["columns"], ensure_ascii=False)
                    conn.execute(
                        """INSERT INTO publications
                           (day, rev, original_name, original_size, original_sha256,
                            settlement_sha256, settlement_size, ridership_sha256, ridership_size,
                            settlement_fingerprint, ridership_fingerprint,
                            settlement_version, ridership_version,
                            settlement_columns, ridership_columns, row_count, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            day,
                            new_rev,
                            preview["original_name"],
                            preview["original_size"],
                            preview["original_sha256"],
                            outputs["settlement"][0],
                            outputs["settlement"][1],
                            outputs["ridership"][0],
                            outputs["ridership"][1],
                            result["contracts"]["settlement"]["fingerprint"],
                            result["contracts"]["ridership"]["fingerprint"],
                            result["contracts"]["settlement"]["version"],
                            result["contracts"]["ridership"]["version"],
                            settlement_cols,
                            ridership_cols,
                            parsed.row_count,
                            created_at,
                        ),
                    )
                    if current_rev == 0:
                        conn.execute(
                            "INSERT INTO days (day, current_rev, updated_at) VALUES (?, ?, ?)",
                            (day, new_rev, created_at),
                        )
                    else:
                        conn.execute(
                            "UPDATE days SET current_rev = ?, updated_at = ? WHERE day = ?",
                            (new_rev, created_at, day),
                        )
                    conn.execute(
                        "UPDATE previews SET consumed_at = ? WHERE id = ?",
                        (created_at, preview_id),
                    )
                    # The preview row is now consumed; publications are the only
                    # thing downloads resolve against, so the preview can go.
                    db.delete_preview(conn, preview_id)
                    # Mark the confirmation published in the SAME transaction as
                    # the publication itself: even if the response is lost or the
                    # site restarts right after COMMIT, the trace record still
                    # shows the revision this confirmation published.
                    db.finish_confirmation(
                        conn,
                        confirmation_id,
                        outcome="published",
                        finished_at=created_at,
                        http_status=201,
                        published_rev=new_rev,
                    )
                    row = db.get_publication(conn, day, new_rev)
                return 201, _publication_payload(row)
            except Exception as err:
                # The publish transaction has rolled back; record why in a
                # separate transaction so the rollback leaves a reason behind.
                self._record_confirmation_failure(conn, confirmation_id, err)
                raise
        finally:
            conn.close()

    def _register_confirmation(self, conn, confirmation_id, preview_id):
        """Insert the confirmation attempt row (outcome NULL until finished).

        Day/filename are denormalised from the preview or its upload record
        (the upload record survives preview cleanup), so the attempt stays
        attributable to a business day even after the preview row is gone.
        """
        try:
            with db.transaction(conn, immediate=True):
                preview = db.get_preview(conn, preview_id)
                upload = db.get_upload_by_preview(conn, preview_id)
                day = preview["day"] if preview else None
                filename = preview["original_name"] if preview else None
                upload_id = None
                if upload is not None:
                    upload_id = upload["id"]
                    day = day or upload["day"]
                    filename = filename or upload["filename"]
                db.create_confirmation(
                    conn,
                    confirmation_id=confirmation_id,
                    preview_id=preview_id,
                    upload_id=upload_id,
                    day=day,
                    filename=filename,
                    started_at=db.iso(db.utcnow()),
                )
        except sqlite3.Error:  # the audit trail must never block publishing
            logger.exception("failed to register confirmation %s", confirmation_id)

    def _record_confirmation_failure(self, conn, confirmation_id, err):
        kind, code, message, status = _classify_confirmation_failure(err)
        try:
            with db.transaction(conn, immediate=True):
                db.finish_confirmation(
                    conn,
                    confirmation_id,
                    outcome="failed",
                    finished_at=db.iso(db.utcnow()),
                    failure_kind=kind,
                    failure_code=code,
                    failure_message=message,
                    http_status=status,
                )
        except sqlite3.Error:
            logger.exception("failed to record confirmation failure %s", confirmation_id)

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def list_days(self):
        conn = self.conn_factory()
        try:
            return db.list_days(conn)
        finally:
            conn.close()

    def day_detail(self, day):
        day = _check_day(day)
        conn = self.conn_factory()
        try:
            revisions = db.list_publications(conn, day)
            return {
                "day": day,
                "current_rev": db.day_revision(conn, day),
                "revisions": [_publication_payload(r) for r in revisions],
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # trace (追查)
    # ------------------------------------------------------------------
    def trace_days(self):
        conn = self.conn_factory()
        try:
            upload_rows, confirmation_rows = db.trace_day_summary(conn)
            orphans = db.list_confirmations_without_day(conn)
            return _trace_days_payload(upload_rows, confirmation_rows, orphans)
        finally:
            conn.close()

    def trace_day(self, day):
        day = _check_day(day)
        conn = self.conn_factory()
        try:
            uploads = [
                _upload_trace_payload(row, db.preview_exists(conn, row["preview_id"]))
                for row in db.list_uploads_for_day(conn, day)
            ]
            by_id = {item["id"]: item for item in uploads}
            by_preview = {
                item["preview_id"]: item for item in uploads if item["preview_id"]
            }
            orphans = []
            for row in db.list_confirmations_for_day(conn, day):
                payload = _confirmation_trace_payload(row)
                target = by_id.get(row["upload_id"]) or by_preview.get(row["preview_id"])
                if target is None:
                    orphans.append(payload)
                else:
                    target["confirmations"].append(payload)
            return {
                "day": day,
                "current_rev": db.day_revision(conn, day),
                "uploads": uploads,
                "orphan_confirmations": orphans,
            }
        finally:
            conn.close()

    def resolve_download(self, day, rev, kind):
        day = _check_day(day)
        if kind not in KINDS:
            raise NotFound("未知的下载类型", code="unknown_kind")
        # Resolve the revision against the database row, then open only the
        # blobs referenced by that row.  Directories are never scanned, so a
        # half-finished publish cannot leak into a reader.
        conn = self.conn_factory()
        try:
            if rev == "latest" or rev is None:
                current = db.day_revision(conn, day)
                if current == 0:
                    raise NotFound("该日尚无可下载版本", code="no_revision")
                row = db.get_publication(conn, day, current)
            else:
                try:
                    rev_int = int(rev)
                except (TypeError, ValueError):
                    raise AppError("修订号必须是数字", code="invalid_rev") from None
                if rev_int < 1:
                    raise AppError("修订号必须大于 0", code="invalid_rev")
                row = db.get_publication(conn, day, rev_int)
            if row is None:
                raise NotFound("该日该修订号不存在", code="revision_not_found")

            # Guard against a damaged revision: never fall back to another
            # revision or serve a partial set.
            for other in ("original_sha256", "settlement_sha256", "ridership_sha256"):
                if not self.store.exists(row[other]):
                    raise Unavailable(
                        "该版本文件缺失，无法提供一致的下载；请联系管理员",
                        code="revision_blob_missing",
                    )
            digest = row[kind + "_sha256"]
            size = row["original_size"] if kind == "original" else row[kind + "_size"]
            name = row["original_name"] if kind == "original" else f"{day}-{kind}.csv"
            path = self.store.path_for(digest)
            # The stored size is what Content-Length will promise; refuse to
            # start a transfer whose blob no longer matches it.
            try:
                if os.path.getsize(path) != size:
                    raise Unavailable(
                        "该版本文件大小与登记不一致，无法提供完整下载；请联系管理员",
                        code="revision_blob_corrupt",
                    )
            except OSError:
                raise Unavailable(
                    "该版本文件缺失，无法提供一致的下载；请联系管理员",
                    code="revision_blob_missing",
                ) from None
            return {
                "path": path,
                "digest": digest,
                "size": size,
                "day": row["day"],
                "rev": row["rev"],
                "kind": kind,
                "name": name,
            }
        finally:
            conn.close()


