from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_dataset import DEFAULT_JSON_OUT as DEFAULT_REVIEW_QUEUE_JSON
from scripts.build_gemini_review_batches import DEFAULT_BATCH_DIR, DEFAULT_SCHEMA_PATH


DEFAULT_NORMALIZED_JSON = DEFAULT_BATCH_DIR / "normalized_reviews.json"
DEFAULT_NORMALIZED_CSV = DEFAULT_BATCH_DIR / "normalized_reviews.csv"
DEFAULT_INGEST_ERRORS_JSON = DEFAULT_BATCH_DIR / "ingest_errors.json"


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def build_queue_lookup(queue_payload: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    lookup: dict[tuple[str, str], dict[str, object]] = {}
    for item in queue_payload.get("items", []):
        lookup[(str(item["ticker"]), str(item["exchange"]))] = item
    return lookup


def extract_response_payload(line: dict[str, object]) -> dict[str, object] | None:
    if isinstance(line.get("response"), dict):
        return line["response"]
    if isinstance(line.get("output"), dict) and isinstance(line["output"].get("response"), dict):
        return line["output"]["response"]
    return None


def extract_error_payload(line: dict[str, object]) -> dict[str, object] | None:
    if isinstance(line.get("error"), dict):
        return line["error"]
    if isinstance(line.get("output"), dict) and isinstance(line["output"].get("error"), dict):
        return line["output"]["error"]
    return None


def extract_metadata(line: dict[str, object]) -> dict[str, object]:
    metadata = line.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def extract_response_text(response_payload: dict[str, object]) -> str:
    parts: list[str] = []
    for candidate in response_payload.get("candidates", []):
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        for part in content.get("parts", []):
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                parts.append(part["text"])
    return "\n".join(part for part in parts if part).strip()


def parse_structured_json(text: str) -> dict[str, object]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.removeprefix("json").strip()
    return json.loads(cleaned)


def validate_review_response(payload: dict[str, object], schema: dict[str, object]) -> list[str]:
    errors: list[str] = []
    required = schema.get("required", [])
    properties = schema.get("properties", {})

    for key in required:
        if key not in payload:
            errors.append(f"missing required field: {key}")

    for key, definition in properties.items():
        if key not in payload:
            continue
        value = payload[key]
        if "enum" in definition and value not in definition["enum"]:
            errors.append(f"invalid enum for {key}: {value}")

    confidence = payload.get("confidence")
    if not isinstance(confidence, (int, float)):
        errors.append("confidence must be numeric")
    elif not 0 <= float(confidence) <= 1:
        errors.append("confidence must be between 0 and 1")

    alias_actions = payload.get("alias_actions", [])
    if not isinstance(alias_actions, list):
        errors.append("alias_actions must be an array")
    else:
        for index, action in enumerate(alias_actions):
            if not isinstance(action, dict):
                errors.append(f"alias_actions[{index}] must be an object")
                continue
            for key in ("alias", "decision", "reason"):
                if key not in action:
                    errors.append(f"alias_actions[{index}] missing {key}")
            if action.get("decision") not in {"keep", "remove", "needs_human"}:
                errors.append(f"alias_actions[{index}] invalid decision: {action.get('decision')}")

    metadata_actions = payload.get("metadata_actions", [])
    if not isinstance(metadata_actions, list):
        errors.append("metadata_actions must be an array")
    else:
        valid_fields = {"name", "exchange", "country", "country_code", "isin", "sector", "asset_type"}
        valid_decisions = {"keep", "update", "clear", "needs_human"}
        for index, action in enumerate(metadata_actions):
            if not isinstance(action, dict):
                errors.append(f"metadata_actions[{index}] must be an object")
                continue
            for key in ("field", "decision", "reason"):
                if key not in action:
                    errors.append(f"metadata_actions[{index}] missing {key}")
            if action.get("field") not in valid_fields:
                errors.append(f"metadata_actions[{index}] invalid field: {action.get('field')}")
            if action.get("decision") not in valid_decisions:
                errors.append(f"metadata_actions[{index}] invalid decision: {action.get('decision')}")

    return errors


def normalize_review_result(
    *,
    line: dict[str, object],
    response_path: Path,
    queue_lookup: dict[tuple[str, str], dict[str, object]],
    schema: dict[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    metadata = extract_metadata(line)
    error_payload = extract_error_payload(line)
    if error_payload:
        return None, {
            "source_file": display_path(response_path),
            "key": line.get("key"),
            "metadata": metadata,
            "error_type": "batch_error",
            "error": error_payload,
        }

    response_payload = extract_response_payload(line)
    if not response_payload:
        return None, {
            "source_file": display_path(response_path),
            "key": line.get("key"),
            "metadata": metadata,
            "error_type": "missing_response",
            "error": "Line does not contain response or error payload.",
        }

    response_text = extract_response_text(response_payload)
    if not response_text:
        return None, {
            "source_file": display_path(response_path),
            "key": line.get("key"),
            "metadata": metadata,
            "error_type": "missing_text",
            "error": "No text parts found in GenerateContentResponse.",
        }

    try:
        parsed = parse_structured_json(response_text)
    except json.JSONDecodeError as exc:
        return None, {
            "source_file": display_path(response_path),
            "key": line.get("key"),
            "metadata": metadata,
            "error_type": "invalid_json",
            "error": str(exc),
            "response_text": response_text,
        }

    validation_errors = validate_review_response(parsed, schema)
    if validation_errors:
        return None, {
            "source_file": display_path(response_path),
            "key": line.get("key"),
            "metadata": metadata,
            "error_type": "schema_validation_failed",
            "error": validation_errors,
            "response_text": response_text,
            "parsed_payload": parsed,
        }

    ticker = str(parsed["ticker"])
    exchange = str(parsed["exchange"])
    queue_item = queue_lookup.get((ticker, exchange))

    return (
        {
            "key": line.get("key"),
            "ticker": ticker,
            "exchange": exchange,
            "entry_decision": parsed["entry_decision"],
            "ticker_exists": parsed["ticker_exists"],
            "name_matches_listing": parsed["name_matches_listing"],
            "alias_actions": parsed["alias_actions"],
            "metadata_actions": parsed["metadata_actions"],
            "confidence": parsed["confidence"],
            "summary": parsed["summary"],
            "source_file": display_path(response_path),
            "request_metadata": metadata,
            "queue_total_score": queue_item.get("total_score") if queue_item else None,
            "queue_findings": queue_item.get("findings") if queue_item else None,
            "queue_aliases": queue_item.get("aliases") if queue_item else None,
        },
        None,
    )


def iter_response_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(candidate for candidate in path.glob("*.jsonl") if candidate.is_file())


def build_normalized_payload(
    normalized_items: list[dict[str, object]],
    ingest_errors: list[dict[str, object]],
    *,
    responses_path: Path,
    queue_path: Path,
    schema_path: Path,
) -> dict[str, object]:
    decision_counts = Counter(item["entry_decision"] for item in normalized_items)
    alias_action_counts = Counter(
        action["decision"]
        for item in normalized_items
        for action in item["alias_actions"]
    )
    metadata_action_counts = Counter(
        action["decision"]
        for item in normalized_items
        for action in item["metadata_actions"]
    )
    return {
        "_meta": {
            "responses_path": display_path(responses_path),
            "source_review_queue": display_path(queue_path),
            "response_schema": display_path(schema_path),
            "normalized_reviews": len(normalized_items),
            "ingest_errors": len(ingest_errors),
        },
        "summary": {
            "entry_decisions": dict(sorted(decision_counts.items())),
            "alias_action_decisions": dict(sorted(alias_action_counts.items())),
            "metadata_action_decisions": dict(sorted(metadata_action_counts.items())),
        },
        "items": normalized_items,
    }


def write_normalized_csv(path: Path, normalized_items: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "exchange",
                "entry_decision",
                "ticker_exists",
                "name_matches_listing",
                "confidence",
                "queue_total_score",
                "alias_remove_count",
                "metadata_update_count",
                "needs_human_count",
                "summary",
                "source_file",
            ],
        )
        writer.writeheader()
        for item in normalized_items:
            alias_remove_count = sum(1 for action in item["alias_actions"] if action["decision"] == "remove")
            metadata_update_count = sum(
                1 for action in item["metadata_actions"] if action["decision"] in {"update", "clear"}
            )
            needs_human_count = sum(
                1
                for action in item["alias_actions"] + item["metadata_actions"]
                if action["decision"] == "needs_human"
            )
            writer.writerow(
                {
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "entry_decision": item["entry_decision"],
                    "ticker_exists": item["ticker_exists"],
                    "name_matches_listing": item["name_matches_listing"],
                    "confidence": item["confidence"],
                    "queue_total_score": item["queue_total_score"],
                    "alias_remove_count": alias_remove_count,
                    "metadata_update_count": metadata_update_count,
                    "needs_human_count": needs_human_count,
                    "summary": item["summary"],
                    "source_file": item["source_file"],
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize Gemini batch review responses into validated review decisions.")
    parser.add_argument("--responses-path", type=Path, default=DEFAULT_BATCH_DIR / "responses")
    parser.add_argument("--review-queue-json", type=Path, default=DEFAULT_REVIEW_QUEUE_JSON)
    parser.add_argument("--schema-path", type=Path, default=DEFAULT_SCHEMA_PATH)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_NORMALIZED_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_NORMALIZED_CSV)
    parser.add_argument("--errors-out", type=Path, default=DEFAULT_INGEST_ERRORS_JSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    queue_payload = load_json(args.review_queue_json)
    queue_lookup = build_queue_lookup(queue_payload)
    schema = load_json(args.schema_path)
    response_files = iter_response_files(args.responses_path)

    normalized_items: list[dict[str, object]] = []
    ingest_errors: list[dict[str, object]] = []
    for response_file in response_files:
        for line in load_jsonl(response_file):
            normalized, error = normalize_review_result(
                line=line,
                response_path=response_file,
                queue_lookup=queue_lookup,
                schema=schema,
            )
            if normalized:
                normalized_items.append(normalized)
            if error:
                ingest_errors.append(error)

    normalized_items.sort(key=lambda item: (str(item["ticker"]), str(item["exchange"])))
    payload = build_normalized_payload(
        normalized_items,
        ingest_errors,
        responses_path=args.responses_path,
        queue_path=args.review_queue_json,
        schema_path=args.schema_path,
    )

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_normalized_csv(args.csv_out, normalized_items)
    args.errors_out.write_text(json.dumps(ingest_errors, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "responses_path": display_path(args.responses_path),
                "response_files": [display_path(path) for path in response_files],
                "normalized_reviews": len(normalized_items),
                "ingest_errors": len(ingest_errors),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "errors_out": display_path(args.errors_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
