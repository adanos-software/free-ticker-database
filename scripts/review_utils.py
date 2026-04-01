from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_SCHEMA_PATH = ROOT / "docs" / "review_response.schema.json"
DEFAULT_NORMALIZED_REVIEWS_JSON = ROOT / "data" / "claude_review_jobs" / "normalized_reviews.json"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def build_prompt_text(item: dict[str, object]) -> str:
    return (
        "Review this flagged ticker-database entry.\n\n"
        "Return JSON only.\n"
        "Follow the provided response schema exactly.\n"
        "Be conservative: if evidence is insufficient, use needs_human or unknown.\n"
        "Treat exchange-specific evidence and ISIN-based evidence as stronger than fuzzy name matches.\n"
        "Remove aliases that are obvious products, celebrities, wrapper terms, or generic/common words.\n"
        "Keep aliases that are strong lexical matches to the issuer or fund name, or clear listing identifiers.\n"
        'If ticker existence cannot be validated from the provided evidence, set ticker_exists to "unknown".\n'
        "If the entry looks like a wrapper, receipt, note, warrant, right, or preferred instrument that should not "
        "be in the stock universe, prefer drop_entry.\n\n"
        "Entry:\n"
        f"{json.dumps(item, indent=2, ensure_ascii=True)}\n\n"
        "External evidence:\n"
        "[]\n\n"
        "No external evidence is attached in this batch request. If uncertain, prefer needs_human or unknown.\n"
    )


def parse_structured_json(text: str) -> dict[str, object]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.removeprefix("json").strip()
    return json.loads(cleaned)


def build_queue_lookup(queue_payload: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
    lookup: dict[tuple[str, str], dict[str, object]] = {}
    for item in queue_payload.get("items", []):
        lookup[(str(item["ticker"]), str(item["exchange"]))] = item
    return lookup


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
