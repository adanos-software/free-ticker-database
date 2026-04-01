import requests

from scripts.run_gemini_review_batches import (
    ACTIVE_STATES,
    RemoteBatchRecord,
    all_batches_terminal,
    build_state_records,
    describe_http_error,
    extract_batch_payload,
    extract_output_file_name,
    is_terminal_state,
    is_retryable_http_error,
    load_state,
    normalize_batch_payload,
    normalize_model_name,
    persist_records,
    submit_batches,
)


def test_extract_output_file_name_supports_multiple_shapes():
    assert extract_output_file_name({"output": {"responsesFile": "files/resp-1"}}) == "files/resp-1"
    assert extract_output_file_name({"response": {"responsesFile": "files/resp-1b"}}) == "files/resp-1b"
    assert extract_output_file_name({"dest": {"fileName": "files/resp-2"}}) == "files/resp-2"
    assert extract_output_file_name({"dest": {"responsesFile": "files/resp-3"}}) == "files/resp-3"
    assert extract_output_file_name({}) is None


def test_extract_batch_payload_handles_wrapped_and_direct_responses():
    payload = {"batch": {"name": "batches/123", "state": "BATCH_STATE_RUNNING"}}
    assert extract_batch_payload(payload) == payload["batch"]
    direct = {"name": "batches/456", "state": "BATCH_STATE_SUCCEEDED"}
    assert extract_batch_payload(direct) == direct


def test_normalize_batch_payload_supports_operation_shape():
    payload = {
        "name": "batches/123",
        "metadata": {
            "state": "JOB_STATE_RUNNING",
            "displayName": "batch-0001",
            "createTime": "2026-03-31T12:00:00Z",
            "updateTime": "2026-03-31T12:01:00Z",
        },
        "response": {"responsesFile": "files/resp-1"},
    }
    normalized = normalize_batch_payload(payload)
    assert normalized["name"] == "batches/123"
    assert normalized["state"] == "JOB_STATE_RUNNING"
    assert normalized["displayName"] == "batch-0001"
    assert normalized["output"] == {"responsesFile": "files/resp-1"}


def test_state_record_roundtrip(tmp_path):
    manifest = {
        "batches": [
            {"batch_id": "batch-0001", "file": "data/gemini_review_jobs/batch-0001.jsonl"},
            {"batch_id": "batch-0002", "file": "data/gemini_review_jobs/batch-0002.jsonl"},
        ]
    }
    state_path = tmp_path / "remote_state.json"
    state = {"_meta": {}, "batches": {"batch-0001": {"batch_name": "batches/1", "state": "BATCH_STATE_RUNNING"}}}
    records = build_state_records(manifest, state)
    assert records["batch-0001"].batch_name == "batches/1"
    assert records["batch-0002"].batch_name is None

    persist_records(state_path=state_path, state=state, records=records, model="models/gemini-2.5-flash")
    stored = load_state(state_path)
    assert stored["_meta"]["model"] == "models/gemini-2.5-flash"
    assert stored["batches"]["batch-0001"]["batch_name"] == "batches/1"


def test_terminal_state_helpers():
    assert is_terminal_state("BATCH_STATE_SUCCEEDED") is True
    assert is_terminal_state("BATCH_STATE_FAILED") is True
    assert is_terminal_state("JOB_STATE_SUCCEEDED") is True
    assert is_terminal_state("BATCH_STATE_RUNNING") is False
    assert "BATCH_STATE_RUNNING" in ACTIVE_STATES
    assert "JOB_STATE_RUNNING" in ACTIVE_STATES
    records = {
        "a": RemoteBatchRecord(batch_id="a", request_file="a.jsonl", batch_name="batches/a", state="BATCH_STATE_SUCCEEDED"),
        "b": RemoteBatchRecord(batch_id="b", request_file="b.jsonl", batch_name="batches/b", state="JOB_STATE_FAILED"),
    }
    assert all_batches_terminal(records) is True
    records["b"].state = "BATCH_STATE_RUNNING"
    assert all_batches_terminal(records) is False


def test_normalize_model_name():
    assert normalize_model_name("models/gemini-2.5-flash") == "gemini-2.5-flash"
    assert normalize_model_name("gemini-2.5-flash") == "gemini-2.5-flash"


def test_retryable_http_error_helpers():
    response = requests.Response()
    response.status_code = 429
    response._content = b'{"error":"rate limited"}'
    response.url = "https://example.com"
    exc = requests.HTTPError("429 Client Error", response=response)
    assert is_retryable_http_error(exc) is True
    assert "rate limited" in describe_http_error(exc)


def test_submit_batches_updates_records_and_persists(tmp_path):
    class FakeApi:
        def upload_jsonl(self, path, *, display_name, mime_type="jsonl"):
            return {"name": f"files/{display_name}", "uri": f"https://files/{display_name}"}

        def create_batch(self, *, file_name, display_name, priority=0):
            return {
                "name": f"batches/{display_name}",
                "state": "JOB_STATE_PENDING",
                "displayName": display_name,
                "createTime": "2026-03-31T12:00:00Z",
                "updateTime": "2026-03-31T12:01:00Z",
                "output": {"responsesFile": f"files/{display_name}-responses"},
            }

    request_path = tmp_path / "batch-0001.jsonl"
    request_path.write_text('{"request":{}}\n', encoding="utf-8")
    records = {
        "batch-0001": RemoteBatchRecord(
            batch_id="batch-0001",
            request_file=str(request_path),
        )
    }
    persisted = []
    submitted = submit_batches(
        api=FakeApi(),
        records=records,
        force=False,
        priority=0,
        submit_delay_seconds=0,
        persist_callback=lambda: persisted.append(records["batch-0001"].batch_name),
    )
    record = records["batch-0001"]
    assert submitted == ["batch-0001"]
    assert record.file_name == "files/batch-0001"
    assert record.batch_name == "batches/batch-0001"
    assert record.state == "JOB_STATE_PENDING"
    assert record.output_file_name == "files/batch-0001-responses"
    assert persisted == ["batches/batch-0001"]
