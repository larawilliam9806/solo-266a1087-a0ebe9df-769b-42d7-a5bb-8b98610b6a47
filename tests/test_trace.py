"""Tests for the trace (追查) page: upload and confirmation audit records."""

import io
import threading

from conftest import DEFAULT_CSV, preview_and_confirm, upload
from railapp import create_app, db


def confirm(client, preview_id):
    return client.post(
        f"/api/previews/{preview_id}/confirm", headers={"Accept": "application/json"}
    )


def trace_day(client, day="2026-11-01"):
    response = client.get(f"/api/trace/{day}")
    assert response.status_code == 200
    return response.get_json()


def all_confirmations(trace):
    return [c for u in trace["uploads"] for c in u["confirmations"]]


def test_trace_records_successful_upload_and_confirmation(client):
    preview, confirmed = preview_and_confirm(client)
    assert confirmed.status_code == 201

    trace = trace_day(client)
    assert len(trace["uploads"]) == 1
    record = trace["uploads"][0]
    assert record["filename"] == "tickets.csv"
    assert record["received_at"]
    assert record["status"] == "preview_ok"
    assert record["status_label"] == "预览通过"
    assert record["preview_id"] == preview["preview_id"]
    assert record["preview"]["base_rev"] == 0
    assert record["preview"]["original"]["row_count"] == 3

    confirmations = record["confirmations"]
    assert len(confirmations) == 1
    outcome = confirmations[0]
    assert outcome["outcome"] == "published"
    assert outcome["published_rev"] == 1
    assert outcome["started_at"] and outcome["finished_at"]

    summary = client.get("/api/trace/days").get_json()
    entry = next(d for d in summary["days"] if d["day"] == "2026-11-01")
    assert entry["uploads"] == 1
    assert entry["confirmations"] == 1
    assert entry["published"] == 1
    assert entry["failed"] == 0
    assert entry["interrupted"] == 0


def test_trace_shows_batch_rejected_format_problem(client):
    data = (
        "ticket_id,passenger_id,train_no,amount,travel_date,ticket_id\n"
        "T1,P1,G1,10,2026-11-01,T9\n"
    )
    response = upload(client, data=data.encode(), filename="broken.csv")
    assert response.status_code == 422

    trace = trace_day(client)
    assert len(trace["uploads"]) == 1
    record = trace["uploads"][0]
    assert record["filename"] == "broken.csv"
    assert record["status"] == "batch_rejected"
    assert record["status_label"] == "整批拒绝"
    assert record["preview_id"] is None
    assert record["detail"]["problem"]["kind"] == "duplicate_header"
    assert "重复列名" in record["detail"]["message"]
    assert record["confirmations"] == []


def test_trace_records_too_large_upload(client):
    big = b"a,b\n" + b"x,y\n" * (1024 * 1024 // 4 + 10)
    response = client.post(
        "/api/previews",
        data={"day": "2026-11-01", "file": (io.BytesIO(big), "big.csv")},
        content_type="multipart/form-data",
        headers={"Accept": "application/json"},
    )
    assert response.status_code == 413

    record = trace_day(client)["uploads"][0]
    assert record["filename"] == "big.csv"
    assert record["status"] == "file_too_large"
    assert record["preview_id"] is None


def test_trace_invalid_day_listed_under_other_days(client):
    response = upload(client, day="2026-13-01")
    assert response.status_code == 400

    summary = client.get("/api/trace/days").get_json()
    assert summary["days"] == []
    assert any(d["day"] == "2026-13-01" for d in summary["other_days"])


def test_trace_shows_incompatible_downstreams_and_file_error_confirmation(client):
    data = "ticket_id,train_no,amount,travel_date\nT1,G1,10,2026-11-01\n"
    preview = upload(client, data=data.encode()).get_json()
    assert preview["convertible"] is False
    response = confirm(client, preview["preview_id"])
    assert response.status_code == 422

    trace = trace_day(client)
    record = trace["uploads"][0]
    assert record["status"] == "preview_incompatible"
    downstreams = record["detail"]["downstreams"]
    assert downstreams["settlement"]["status"] == "incompatible"
    assert "passenger_id" in downstreams["settlement"]["missing_columns"]
    assert downstreams["ridership"]["status"] == "incompatible"

    outcome = record["confirmations"][0]
    assert outcome["outcome"] == "failed"
    assert outcome["failure_kind"] == "file_error"
    assert outcome["failure_kind_label"] == "文件出错"
    assert outcome["failure_code"] == "incompatible"
    assert outcome["http_status"] == 422


def test_trace_confirmation_preview_invalid_on_stale_contract(client):
    preview = upload(client).get_json()
    client.put(
        "/api/contracts/settlement",
        json={"columns": [{"name": "ticket_id", "type": "text"},
                          {"name": "amount", "type": "integer"}]},
    )
    response = confirm(client, preview["preview_id"])
    assert response.status_code == 409

    outcome = all_confirmations(trace_day(client))[0]
    assert outcome["outcome"] == "failed"
    assert outcome["failure_kind"] == "preview_invalid"
    assert outcome["failure_kind_label"] == "预览失效"
    assert outcome["failure_code"] == "stale_contract"


def test_trace_confirmation_expired_is_preview_invalid(client, app):
    preview = upload(client).get_json()
    conn = db.connect(app.config["DB_PATH"])
    try:
        conn.execute(
            "UPDATE previews SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", preview["preview_id"]),
        )
    finally:
        conn.close()
    response = confirm(client, preview["preview_id"])
    assert response.status_code == 409

    outcome = all_confirmations(trace_day(client))[0]
    assert outcome["outcome"] == "failed"
    assert outcome["failure_kind"] == "preview_invalid"
    assert outcome["failure_code"] == "preview_expired"


def test_trace_publish_failure_records_rollback_reason(client, app, monkeypatch):
    import os

    preview_and_confirm(client)
    service = app.extensions["service"]
    preview = upload(client, data=DEFAULT_CSV.replace("G22", "G23").encode()).get_json()

    def flaky_put(data):
        raise OSError("disk full")

    monkeypatch.setattr(service.store, "put_bytes", flaky_put)
    for name in ("settlement", "ridership"):
        digest = preview["downstreams"][name]["output_sha256"]
        if digest:
            try:
                os.unlink(service.store.path_for(digest))
            except FileNotFoundError:
                pass
    response = confirm(client, preview["preview_id"])
    assert response.status_code == 500

    outcomes = all_confirmations(trace_day(client))
    failed = [c for c in outcomes if c["outcome"] == "failed"]
    assert len(failed) == 1
    outcome = failed[0]
    assert outcome["failure_kind"] == "publish_failed"
    assert outcome["failure_kind_label"] == "发布登记失败"
    assert "disk full" in outcome["failure_message"]


def test_trace_records_rollback_reason_on_registration_failure(client, app):
    """A failure while registering the publication leaves its reason behind."""
    preview_and_confirm(client)
    preview = upload(client, data=DEFAULT_CSV.replace("T1", "T9").encode()).get_json()

    conn = db.connect(app.config["DB_PATH"])
    try:
        conn.execute(
            """CREATE TRIGGER fail_pointer BEFORE UPDATE ON days
               BEGIN SELECT RAISE(ABORT, 'simulated crash before pointer update'); END"""
        )
    finally:
        conn.close()
    try:
        response = confirm(client, preview["preview_id"])
    finally:
        conn = db.connect(app.config["DB_PATH"])
        try:
            conn.execute("DROP TRIGGER fail_pointer")
        finally:
            conn.close()
    assert response.status_code == 503

    outcomes = all_confirmations(trace_day(client))
    failed = [c for c in outcomes if c["outcome"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["failure_kind"] == "publish_failed"
    assert "simulated crash before pointer update" in failed[0]["failure_message"]
    # The failed confirmation did not publish anything.
    assert client.get("/api/days/2026-11-01").get_json()["current_rev"] == 1


def test_trace_survives_preview_cleanup(client):
    preview = upload(client).get_json()
    client.delete(f"/api/previews/{preview['preview_id']}")
    response = confirm(client, preview["preview_id"])
    assert response.status_code == 404

    trace = trace_day(client)
    record = trace["uploads"][0]
    assert record["preview_alive"] is False
    assert record["preview"]["original"]["name"] == "tickets.csv"
    outcome = record["confirmations"][0]
    assert outcome["outcome"] == "failed"
    assert outcome["failure_kind"] == "preview_invalid"
    assert outcome["failure_code"] == "preview_not_found"


def test_trace_marks_unfinished_confirmation_as_interrupted(client, app):
    preview = upload(client).get_json()
    conn = db.connect(app.config["DB_PATH"])
    try:
        db.create_confirmation(
            conn,
            confirmation_id="cafe" * 8,
            preview_id=preview["preview_id"],
            upload_id=None,
            day="2026-11-01",
            filename="tickets.csv",
            started_at=db.iso(db.utcnow()),
        )
    finally:
        conn.close()

    trace = trace_day(client)
    outcome = all_confirmations(trace)[0]
    assert outcome["outcome"] == "interrupted"
    assert outcome["finished_at"] is None

    summary = client.get("/api/trace/days").get_json()
    entry = next(d for d in summary["days"] if d["day"] == "2026-11-01")
    assert entry["interrupted"] == 1

    html = client.get("/trace/2026-11-01").data.decode()
    assert "中断" in html


def test_trace_persists_across_restart(client, app):
    """A published confirmation still shows its revision after a full restart."""
    _, confirmed = preview_and_confirm(client)
    assert confirmed.status_code == 201

    restarted = create_app({"DATA_DIR": app.config["DATA_DIR"], "TESTING": True})
    trace = trace_day(restarted.test_client())
    outcome = all_confirmations(trace)[0]
    assert outcome["outcome"] == "published"
    assert outcome["published_rev"] == 1


def test_concurrent_confirmations_each_leave_record(app):
    client = app.test_client()
    preview = upload(client, day="2026-11-03").get_json()
    results = []
    barrier = threading.Barrier(4)

    def do_confirm():
        local = app.test_client()
        barrier.wait()
        response = local.post(
            f"/api/previews/{preview['preview_id']}/confirm",
            headers={"Accept": "application/json"},
        )
        results.append(response.status_code)

    threads = [threading.Thread(target=do_confirm) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count(201) == 1

    trace = trace_day(client, "2026-11-03")
    outcomes = all_confirmations(trace)
    assert len(outcomes) == 4
    assert sum(1 for c in outcomes if c["outcome"] == "published") == 1
    assert sum(1 for c in outcomes if c["outcome"] == "failed") == 3
    assert all(c["failure_kind"] == "preview_invalid" for c in outcomes if c["outcome"] == "failed")


def test_same_day_republish_keeps_original_results(client):
    _, first = preview_and_confirm(client)
    assert first.get_json()["rev"] == 1
    _, second = preview_and_confirm(client, data=DEFAULT_CSV.replace("G22,88", "G22,99").encode())
    assert second.get_json()["rev"] == 2

    trace = trace_day(client)
    assert len(trace["uploads"]) == 2
    published = sorted(
        (c["published_rev"] for c in all_confirmations(trace) if c["outcome"] == "published")
    )
    assert published == [1, 2]
    # Both revisions still resolve to their own files.
    assert b"G22,88" in client.get("/download/2026-11-01/1/settlement").data
    assert b"G22,99" in client.get("/download/2026-11-01/2/settlement").data


def test_trace_confirmation_of_unknown_preview_is_orphan(client):
    response = confirm(client, "0" * 32)
    assert response.status_code == 404

    summary = client.get("/api/trace/days").get_json()
    assert summary["unknown_day"]["confirmations"] == 1
    orphan = summary["orphan_confirmations"][0]
    assert orphan["outcome"] == "failed"
    assert orphan["failure_kind"] == "preview_invalid"
    assert orphan["failure_code"] == "preview_not_found"


def test_trace_pages_render_and_cross_link(client):
    assert client.get("/trace").status_code == 200
    # A day without records renders an empty page rather than an error.
    assert client.get("/trace/2026-11-01").status_code == 200
    assert client.get("/trace/not-a-day").status_code == 400

    preview_and_confirm(client)
    page = client.get("/trace/2026-11-01")
    assert page.status_code == 200
    html = page.data.decode()
    assert "tickets.csv" in html
    assert "已发布 r1" in html
    # From the record to the published revision details...
    assert "/days/2026-11-01#rev-1" in html

    # ...and back from the revision details to the trace records.
    day_page = client.get("/days/2026-11-01").data.decode()
    assert 'id="rev-1"' in day_page
    assert "/trace/2026-11-01" in day_page

    index = client.get("/").data.decode()
    assert "/trace" in index
