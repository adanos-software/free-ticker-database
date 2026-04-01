from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_gemini_review_batches import DEFAULT_BATCH_DIR, display_path


DEFAULT_MANIFEST = DEFAULT_BATCH_DIR / "manifest.json"
DEFAULT_STATE_PATH = DEFAULT_BATCH_DIR / "remote_state.json"
DEFAULT_RESPONSES_DIR = DEFAULT_BATCH_DIR / "responses"
BASE_URL = "https://generativelanguage.googleapis.com"
TERMINAL_STATES = {
    "BATCH_STATE_SUCCEEDED",
    "BATCH_STATE_FAILED",
    "BATCH_STATE_CANCELLED",
    "BATCH_STATE_EXPIRED",
    "JOB_STATE_SUCCEEDED",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "JOB_STATE_EXPIRED",
}
ACTIVE_STATES = {
    "BATCH_STATE_PENDING",
    "BATCH_STATE_RUNNING",
    "BATCH_STATE_UNSPECIFIED",
    "JOB_STATE_PENDING",
    "JOB_STATE_RUNNING",
    "JOB_STATE_UNSPECIFIED",
}
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_DELAY_SECONDS = 5.0
DEFAULT_SUBMIT_DELAY_SECONDS = 2.0


@dataclass
class RemoteBatchRecord:
    batch_id: str
    request_file: str
    file_name: str | None = None
    file_uri: str | None = None
    batch_name: str | None = None
    state: str | None = None
    output_file_name: str | None = None
    downloaded_response: str | None = None
    display_name: str | None = None
    update_time: str | None = None
    create_time: str | None = None
    end_time: str | None = None
    last_error: str | None = None


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_model_name(model: str) -> str:
    return model.removeprefix("models/")


def is_terminal_state(state: str | None) -> bool:
    return bool(state in TERMINAL_STATES)


def extract_batch_payload(response_json: dict[str, Any]) -> dict[str, Any]:
    if isinstance(response_json.get("batch"), dict):
        return response_json["batch"]
    return response_json


def normalize_batch_payload(response_json: dict[str, Any]) -> dict[str, Any]:
    payload = extract_batch_payload(response_json)
    metadata = payload.get("metadata")
    response = payload.get("response")

    if not isinstance(metadata, dict) and not isinstance(response, dict):
        return payload

    metadata = metadata if isinstance(metadata, dict) else {}
    response = response if isinstance(response, dict) else {}
    output = payload.get("output")
    dest = payload.get("dest")

    return {
        "name": payload.get("name"),
        "state": payload.get("state") or metadata.get("state"),
        "displayName": payload.get("displayName") or metadata.get("displayName"),
        "createTime": payload.get("createTime") or metadata.get("createTime"),
        "updateTime": payload.get("updateTime") or metadata.get("updateTime"),
        "endTime": payload.get("endTime") or metadata.get("endTime"),
        "output": output if isinstance(output, dict) else response,
        "dest": dest if isinstance(dest, dict) else response,
        "error": payload.get("error"),
        "done": payload.get("done"),
    }


def extract_output_file_name(batch_payload: dict[str, Any]) -> str | None:
    for key in ("output", "response", "dest"):
        value = batch_payload.get(key)
        if not isinstance(value, dict):
            continue
        if isinstance(value.get("responsesFile"), str):
            return value["responsesFile"]
        if isinstance(value.get("fileName"), str):
            return value["fileName"]
        inlined = value.get("inlinedResponses")
        if isinstance(inlined, dict) and isinstance(inlined.get("responsesFile"), str):
            return inlined["responsesFile"]
    return None


def is_retryable_http_error(exc: requests.HTTPError) -> bool:
    response = exc.response
    return bool(response is not None and response.status_code in RETRYABLE_STATUS_CODES)


def describe_http_error(exc: requests.HTTPError) -> str:
    response = exc.response
    if response is None:
        return str(exc)
    details = response.text.strip()
    if details:
        return f"{exc} :: {details}"
    return str(exc)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"_meta": {}, "batches": {}}
    return load_json(path)


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def build_state_records(
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, RemoteBatchRecord]:
    records: dict[str, RemoteBatchRecord] = {}
    existing = state.get("batches", {})
    for batch in manifest.get("batches", []):
        batch_id = batch["batch_id"]
        record_data = existing.get(batch_id, {})
        records[batch_id] = RemoteBatchRecord(
            batch_id=batch_id,
            request_file=batch["file"],
            file_name=record_data.get("file_name"),
            file_uri=record_data.get("file_uri"),
            batch_name=record_data.get("batch_name"),
            state=record_data.get("state"),
            output_file_name=record_data.get("output_file_name"),
            downloaded_response=record_data.get("downloaded_response"),
            display_name=record_data.get("display_name"),
            update_time=record_data.get("update_time"),
            create_time=record_data.get("create_time"),
            end_time=record_data.get("end_time"),
            last_error=record_data.get("last_error"),
        )
    return records


def persist_records(
    *,
    state_path: Path,
    state: dict[str, Any],
    records: dict[str, RemoteBatchRecord],
    model: str,
) -> None:
    state["_meta"] = {
        "model": model,
        "state_path": display_path(state_path),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    state["batches"] = {batch_id: asdict(record) for batch_id, record in sorted(records.items())}
    save_state(state_path, state)


class GeminiBatchApi:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        session: requests.Session | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
    ):
        self.api_key = api_key
        self.model = normalize_model_name(model)
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

    def _headers(self) -> dict[str, str]:
        return {"x-goog-api-key": self.api_key}

    def _request_with_retries(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        delay = self.retry_delay_seconds
        for attempt in range(self.max_retries + 1):
            response = self.session.request(method, url, **kwargs)
            try:
                response.raise_for_status()
                return response
            except requests.HTTPError as exc:
                if not is_retryable_http_error(exc) or attempt >= self.max_retries:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("Retry loop exited unexpectedly.")

    def upload_jsonl(self, path: Path, *, display_name: str, mime_type: str = "jsonl") -> dict[str, Any]:
        size_bytes = path.stat().st_size
        start_response = self._request_with_retries(
            "POST",
            f"{BASE_URL}/upload/v1beta/files",
            headers={
                **self._headers(),
                "X-Goog-Upload-Protocol": "resumable",
                "X-Goog-Upload-Command": "start",
                "X-Goog-Upload-Header-Content-Length": str(size_bytes),
                "X-Goog-Upload-Header-Content-Type": mime_type,
                "Content-Type": "application/json",
            },
            json={"file": {"display_name": display_name}},
        )
        upload_url = start_response.headers.get("x-goog-upload-url")
        if not upload_url:
            raise RuntimeError("Missing x-goog-upload-url header from File API start response.")

        upload_response = self._request_with_retries(
            "POST",
            upload_url,
            headers={
                "Content-Length": str(size_bytes),
                "X-Goog-Upload-Offset": "0",
                "X-Goog-Upload-Command": "upload, finalize",
            },
            data=path.read_bytes(),
        )
        payload = upload_response.json()
        file_payload = payload.get("file")
        if not isinstance(file_payload, dict) or not file_payload.get("name"):
            raise RuntimeError(f"Unexpected upload response: {payload}")
        return file_payload

    def create_batch(self, *, file_name: str, display_name: str, priority: int = 0) -> dict[str, Any]:
        response = self._request_with_retries(
            "POST",
            f"{BASE_URL}/v1beta/models/{self.model}:batchGenerateContent",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "batch": {
                    "display_name": display_name,
                    "input_config": {"file_name": file_name},
                    "priority": str(priority),
                }
            },
        )
        payload = response.json()
        batch_payload = normalize_batch_payload(payload)
        if not batch_payload.get("name"):
            raise RuntimeError(f"Unexpected batch creation response: {payload}")
        return batch_payload

    def get_batch(self, batch_name: str) -> dict[str, Any]:
        response = self._request_with_retries(
            "GET",
            f"{BASE_URL}/v1beta/{batch_name}",
            headers={**self._headers(), "Content-Type": "application/json"},
        )
        payload = response.json()
        return normalize_batch_payload(payload)

    def download_file(self, *, file_name: str, destination: Path) -> None:
        response = self._request_with_retries(
            "GET",
            f"{BASE_URL}/download/v1beta/{file_name}:download?alt=media",
            headers=self._headers(),
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)


def submit_batches(
    *,
    api: GeminiBatchApi,
    records: dict[str, RemoteBatchRecord],
    force: bool,
    priority: int,
    submit_delay_seconds: float,
    persist_callback: Callable[[], None] | None = None,
) -> list[str]:
    submitted: list[str] = []
    pending_records = list(records.values())
    for index, record in enumerate(pending_records):
        if record.batch_name and not force:
            continue

        request_path = ROOT / record.request_file if not Path(record.request_file).is_absolute() else Path(record.request_file)
        display_name = record.display_name or record.batch_id
        try:
            uploaded_file = api.upload_jsonl(request_path, display_name=display_name)
            created_batch = api.create_batch(
                file_name=uploaded_file["name"],
                display_name=display_name,
                priority=priority,
            )
        except requests.HTTPError as exc:
            record.last_error = describe_http_error(exc)
            if persist_callback is not None:
                persist_callback()
            raise

        record.file_name = uploaded_file.get("name")
        record.file_uri = uploaded_file.get("uri")
        record.batch_name = created_batch.get("name")
        record.state = created_batch.get("state")
        record.output_file_name = extract_output_file_name(created_batch)
        record.display_name = created_batch.get("displayName", display_name)
        record.create_time = created_batch.get("createTime")
        record.update_time = created_batch.get("updateTime")
        record.end_time = created_batch.get("endTime")
        record.last_error = None
        if persist_callback is not None:
            persist_callback()
        submitted.append(record.batch_id)
        is_last_record = index == len(pending_records) - 1
        if submit_delay_seconds > 0 and not is_last_record:
            time.sleep(submit_delay_seconds)
    return submitted


def poll_batches(
    *,
    api: GeminiBatchApi,
    records: dict[str, RemoteBatchRecord],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records.values():
        if not record.batch_name or is_terminal_state(record.state):
            if record.state:
                counts[record.state] = counts.get(record.state, 0) + 1
            continue

        try:
            batch_payload = api.get_batch(record.batch_name)
        except requests.HTTPError as exc:
            record.last_error = str(exc)
            counts["POLL_ERROR"] = counts.get("POLL_ERROR", 0) + 1
            continue

        record.state = batch_payload.get("state")
        record.output_file_name = extract_output_file_name(batch_payload)
        record.update_time = batch_payload.get("updateTime")
        record.create_time = batch_payload.get("createTime", record.create_time)
        record.end_time = batch_payload.get("endTime")
        counts[record.state or "UNKNOWN"] = counts.get(record.state or "UNKNOWN", 0) + 1
    return counts


def download_completed_batches(
    *,
    api: GeminiBatchApi,
    records: dict[str, RemoteBatchRecord],
    responses_dir: Path,
    force: bool,
) -> list[str]:
    downloaded: list[str] = []
    for record in records.values():
        if record.state != "BATCH_STATE_SUCCEEDED" or not record.output_file_name:
            continue
        destination = responses_dir / f"{record.batch_id}.jsonl"
        if destination.exists() and record.downloaded_response and not force:
            continue
        api.download_file(file_name=record.output_file_name, destination=destination)
        record.downloaded_response = display_path(destination)
        downloaded.append(record.batch_id)
    return downloaded


def all_batches_terminal(records: dict[str, RemoteBatchRecord]) -> bool:
    relevant = [record for record in records.values() if record.batch_name]
    return bool(relevant) and all(is_terminal_state(record.state) for record in relevant)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload, poll, and download Gemini review batch jobs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_flags(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
        subparser.add_argument("--state-path", type=Path, default=DEFAULT_STATE_PATH)
        subparser.add_argument("--responses-dir", type=Path, default=DEFAULT_RESPONSES_DIR)
        subparser.add_argument("--model", default="gemini-2.5-flash")
        subparser.add_argument("--api-key", default=os.environ.get("GEMINI_API_KEY"))
        subparser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
        subparser.add_argument("--retry-delay-seconds", type=float, default=DEFAULT_RETRY_DELAY_SECONDS)

    submit_parser = subparsers.add_parser("submit")
    add_common_flags(submit_parser)
    submit_parser.add_argument("--force", action="store_true")
    submit_parser.add_argument("--priority", type=int, default=0)
    submit_parser.add_argument("--submit-delay-seconds", type=float, default=DEFAULT_SUBMIT_DELAY_SECONDS)

    poll_parser = subparsers.add_parser("poll")
    add_common_flags(poll_parser)
    poll_parser.add_argument("--download-completed", action="store_true")
    poll_parser.add_argument("--force-download", action="store_true")

    download_parser = subparsers.add_parser("download")
    add_common_flags(download_parser)
    download_parser.add_argument("--force", action="store_true")

    run_parser = subparsers.add_parser("run")
    add_common_flags(run_parser)
    run_parser.add_argument("--force-submit", action="store_true")
    run_parser.add_argument("--force-download", action="store_true")
    run_parser.add_argument("--priority", type=int, default=0)
    run_parser.add_argument("--submit-delay-seconds", type=float, default=DEFAULT_SUBMIT_DELAY_SECONDS)
    run_parser.add_argument("--poll-interval-seconds", type=int, default=30)
    run_parser.add_argument("--timeout-seconds", type=int, default=3600)

    return parser.parse_args()


def ensure_api_key(api_key: str | None) -> str:
    if not api_key:
        raise SystemExit("GEMINI_API_KEY not provided. Pass --api-key or set the environment variable.")
    return api_key


def load_manifest_and_records(manifest_path: Path, state_path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, RemoteBatchRecord]]:
    manifest = load_json(manifest_path)
    state = load_state(state_path)
    records = build_state_records(manifest, state)
    return manifest, state, records


def print_summary(
    *,
    command: str,
    model: str,
    state_path: Path,
    submitted: list[str] | None = None,
    downloaded: list[str] | None = None,
    state_counts: dict[str, int] | None = None,
) -> None:
    print(
        json.dumps(
            {
                "command": command,
                "model": normalize_model_name(model),
                "state_path": display_path(state_path),
                "submitted_batches": submitted or [],
                "downloaded_batches": downloaded or [],
                "state_counts": state_counts or {},
            },
            indent=2,
        )
    )


def main() -> None:
    args = parse_args()
    api_key = ensure_api_key(args.api_key)
    _, state, records = load_manifest_and_records(args.manifest, args.state_path)
    api = GeminiBatchApi(
        api_key=api_key,
        model=args.model,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
    )

    if args.command == "submit":
        submitted = submit_batches(
            api=api,
            records=records,
            force=args.force,
            priority=args.priority,
            submit_delay_seconds=args.submit_delay_seconds,
            persist_callback=lambda: persist_records(
                state_path=args.state_path,
                state=state,
                records=records,
                model=args.model,
            ),
        )
        persist_records(state_path=args.state_path, state=state, records=records, model=args.model)
        print_summary(command="submit", model=args.model, state_path=args.state_path, submitted=submitted)
        return

    if args.command == "poll":
        state_counts = poll_batches(api=api, records=records)
        downloaded: list[str] = []
        if args.download_completed:
            downloaded = download_completed_batches(
                api=api,
                records=records,
                responses_dir=args.responses_dir,
                force=args.force_download,
            )
        persist_records(state_path=args.state_path, state=state, records=records, model=args.model)
        print_summary(
            command="poll",
            model=args.model,
            state_path=args.state_path,
            downloaded=downloaded,
            state_counts=state_counts,
        )
        return

    if args.command == "download":
        downloaded = download_completed_batches(
            api=api,
            records=records,
            responses_dir=args.responses_dir,
            force=args.force,
        )
        persist_records(state_path=args.state_path, state=state, records=records, model=args.model)
        print_summary(command="download", model=args.model, state_path=args.state_path, downloaded=downloaded)
        return

    if args.command == "run":
        submitted = submit_batches(
            api=api,
            records=records,
            force=args.force_submit,
            priority=args.priority,
            submit_delay_seconds=args.submit_delay_seconds,
            persist_callback=lambda: persist_records(
                state_path=args.state_path,
                state=state,
                records=records,
                model=args.model,
            ),
        )
        start = time.time()
        last_counts: dict[str, int] = {}
        while True:
            last_counts = poll_batches(api=api, records=records)
            download_completed_batches(
                api=api,
                records=records,
                responses_dir=args.responses_dir,
                force=args.force_download,
            )
            persist_records(state_path=args.state_path, state=state, records=records, model=args.model)
            if all_batches_terminal(records):
                break
            if time.time() - start > args.timeout_seconds:
                raise SystemExit("Timed out waiting for Gemini batch jobs to reach terminal states.")
            time.sleep(args.poll_interval_seconds)

        downloaded = [
            record.batch_id
            for record in records.values()
            if record.downloaded_response
        ]
        print_summary(
            command="run",
            model=args.model,
            state_path=args.state_path,
            submitted=submitted,
            downloaded=downloaded,
            state_counts=last_counts,
        )


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        raise SystemExit(describe_http_error(exc))
