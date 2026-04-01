from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_dataset import DEFAULT_JSON_OUT as DEFAULT_REVIEW_QUEUE_JSON
from scripts.build_gemini_review_batches import DEFAULT_SCHEMA_PATH, build_prompt_text
from scripts.ingest_gemini_reviews import (
    build_normalized_payload,
    build_queue_lookup,
    parse_structured_json,
    validate_review_response,
    write_normalized_csv,
)


DEFAULT_OUTPUT_DIR = ROOT / "data" / "claude_review_jobs"
DEFAULT_PROMPT_PATH = ROOT / "docs" / "claude_review_prompt.md"
DEFAULT_RAW_RESPONSES_JSONL = DEFAULT_OUTPUT_DIR / "raw_responses.jsonl"
DEFAULT_NORMALIZED_JSON = DEFAULT_OUTPUT_DIR / "normalized_reviews.json"
DEFAULT_NORMALIZED_CSV = DEFAULT_OUTPUT_DIR / "normalized_reviews.csv"
DEFAULT_ERRORS_JSON = DEFAULT_OUTPUT_DIR / "errors.json"
DEFAULT_BATCH_SIZE = 10


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_existing_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def build_claude_prompt(item: dict[str, object], prompt_template: str) -> str:
    template = prompt_template.strip()
    required_fields = (
        "ticker, exchange, entry_decision, ticker_exists, name_matches_listing, "
        "alias_actions, metadata_actions, confidence, summary"
    )
    schema_rules = (
        "Required top-level fields exactly: "
        f"{required_fields}.\n"
        "Do not rename fields.\n"
        "Use alias_actions objects with keys alias, decision, reason.\n"
        "Use metadata_actions objects with keys field, decision, reason, and optional proposed_value.\n"
        "Use only these enums:\n"
        "- entry_decision: keep, drop_entry, fix_metadata, needs_human\n"
        '- ticker_exists: yes, no, unknown\n'
        '- name_matches_listing: yes, no, unknown\n'
        "- alias decision: keep, remove, needs_human\n"
        "- metadata decision: keep, update, clear, needs_human\n"
        "confidence must be a number between 0 and 1.\n"
        "Do not use alternate keys such as recommended_action, explanation, action, listing_valid, or fields_requiring_verification."
    )
    if template:
        return f"{template}\n\n{schema_rules}\n\n{build_prompt_text(item)}"
    return f"{schema_rules}\n\n{build_prompt_text(item)}"


def build_claude_batch_prompt(items: list[dict[str, object]], prompt_template: str) -> str:
    template = prompt_template.strip()
    schema_rules = (
        "Return one top-level JSON object with exactly one key: reviews.\n"
        "reviews must be an array containing exactly one review object per entry.\n"
        "Every review object must use these exact fields: "
        "ticker, exchange, entry_decision, ticker_exists, name_matches_listing, "
        "alias_actions, metadata_actions, confidence, summary.\n"
        "Do not rename fields.\n"
        "Use alias_actions objects with keys alias, decision, reason.\n"
        "Use metadata_actions objects with keys field, decision, reason, and optional proposed_value.\n"
        "Use only these enums:\n"
        "- entry_decision: keep, drop_entry, fix_metadata, needs_human\n"
        '- ticker_exists: yes, no, unknown\n'
        '- name_matches_listing: yes, no, unknown\n'
        "- alias decision: keep, remove, needs_human\n"
        "- metadata decision: keep, update, clear, needs_human\n"
        "confidence must be a number between 0 and 1.\n"
        "Do not use alternate keys such as recommended_action, explanation, action, listing_valid, or fields_requiring_verification.\n"
        f"Batch size: {len(items)} entries. Return exactly {len(items)} review objects."
    )
    entries = "\n\n".join(
        f"Entry {index}:\n{json.dumps(item, indent=2, ensure_ascii=True)}"
        for index, item in enumerate(items, start=1)
    )
    prompt_body = (
        "Review this batch of flagged ticker-database entries.\n\n"
        "External evidence:\n[]\n\n"
        "No external evidence is attached in this batch request. If uncertain, prefer needs_human or unknown.\n\n"
        f"{entries}\n"
    )
    if template:
        return f"{template}\n\n{schema_rules}\n\n{prompt_body}"
    return f"{schema_rules}\n\n{prompt_body}"


def build_claude_command(
    *,
    prompt: str,
    schema: dict[str, object],
    model: str,
    cwd: Path,
) -> list[str]:
    return [
        "claude",
        "--dangerously-skip-permissions",
        "-p",
        "--output-format",
        "json",
        "--append-system-prompt",
        "Do not use tools. Do not inspect files or browse the web. Use only the user prompt and return structured output that matches the schema.",
        "--json-schema",
        json.dumps(schema, separators=(",", ":")),
        "--model",
        model,
        prompt,
    ]


def build_batch_schema(review_schema: dict[str, object], review_count: int) -> dict[str, object]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["reviews"],
        "properties": {
            "reviews": {
                "type": "array",
                "minItems": review_count,
                "maxItems": review_count,
                "items": review_schema,
            }
        },
    }


def extract_structured_output(output: str, required_fields: set[str]) -> dict[str, object]:
    parsed = json.loads(output)
    if isinstance(parsed, dict):
        candidate = coerce_batch_payload(parsed)
        if required_fields.issubset(candidate.keys()):
            return candidate
        candidate = coerce_review_payload(parsed)
        if required_fields.issubset(candidate.keys()):
            return candidate
        structured_output = parsed.get("structured_output")
        if isinstance(structured_output, dict):
            candidate = coerce_batch_payload(structured_output)
            if required_fields.issubset(candidate.keys()):
                return candidate
            candidate = coerce_review_payload(structured_output)
            if required_fields.issubset(candidate.keys()):
                return candidate

    for key in ("result", "content", "text", "completion"):
        value = parsed.get(key) if isinstance(parsed, dict) else None
        if isinstance(value, str):
            structured_value = parse_structured_json(value)
            candidate = coerce_batch_payload(structured_value)
            if required_fields.issubset(candidate.keys()):
                return candidate
            candidate = coerce_review_payload(structured_value)
            if required_fields.issubset(candidate.keys()):
                return candidate

    raise ValueError("Claude output did not contain a schema-conforming payload.")


def coerce_review_payload(payload: dict[str, object]) -> dict[str, object]:
    candidate = dict(payload)
    if "recommended_action" in candidate and "entry_decision" not in candidate:
        candidate["entry_decision"] = candidate["recommended_action"]
    if "listing_valid" in candidate and "name_matches_listing" not in candidate:
        candidate["name_matches_listing"] = candidate["listing_valid"]
    if "explanation" in candidate and "summary" not in candidate:
        candidate["summary"] = candidate["explanation"]
    if "confidence" not in candidate:
        candidate["confidence"] = 0.3

    alias_actions = candidate.get("alias_actions")
    if isinstance(alias_actions, list):
        coerced_aliases: list[dict[str, object]] = []
        for action in alias_actions:
            if not isinstance(action, dict):
                continue
            coerced_aliases.append(
                {
                    "alias": action.get("alias", ""),
                    "decision": action.get("decision", action.get("action", "needs_human")),
                    "reason": action.get("reason", ""),
                }
            )
        candidate["alias_actions"] = coerced_aliases

    metadata_actions = candidate.get("metadata_actions")
    if not isinstance(metadata_actions, list):
        metadata_actions = []
    fields_requiring_verification = candidate.get("fields_requiring_verification")
    if isinstance(fields_requiring_verification, list):
        for field in fields_requiring_verification:
            if field in {"name", "exchange", "country", "country_code", "isin", "sector", "asset_type"}:
                metadata_actions.append(
                    {
                        "field": field,
                        "decision": "needs_human",
                        "reason": "Claude flagged this field for verification.",
                    }
                )
    candidate["metadata_actions"] = metadata_actions
    return candidate


def coerce_batch_payload(payload: dict[str, object] | list[object]) -> dict[str, object]:
    if isinstance(payload, list):
        return {"reviews": [coerce_review_payload(item) for item in payload if isinstance(item, dict)]}

    if not isinstance(payload, dict):
        return {}

    reviews = payload.get("reviews")
    if isinstance(reviews, list):
        return {"reviews": [coerce_review_payload(item) for item in reviews if isinstance(item, dict)]}

    return {}


def chunk_items(items: list[dict[str, object]], batch_size: int) -> list[list[dict[str, object]]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def build_batches(
    items: list[dict[str, object]],
    batch_size: int,
    *,
    allow_partial_batch: bool,
) -> tuple[list[list[dict[str, object]]], list[dict[str, object]]]:
    full_batch_count = len(items) // batch_size
    full_batch_limit = full_batch_count * batch_size
    batches = chunk_items(items[:full_batch_limit], batch_size)
    deferred_items = items[full_batch_limit:]
    if allow_partial_batch and deferred_items:
        batches.append(deferred_items)
        deferred_items = []
    return batches, deferred_items


def run_claude(
    *,
    prompt: str,
    schema: dict[str, object],
    model: str,
    cwd: Path,
    timeout_seconds: int,
) -> tuple[dict[str, object], str]:
    command = build_claude_command(prompt=prompt, schema=schema, model=model, cwd=cwd)
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip() or f"exit_code={completed.returncode}"
        raise RuntimeError(stderr)

    stdout = completed.stdout.strip()
    if not stdout:
        raise RuntimeError("Claude returned empty output.")

    required_fields = {
        "ticker",
        "exchange",
        "entry_decision",
        "ticker_exists",
        "name_matches_listing",
        "alias_actions",
        "metadata_actions",
        "confidence",
        "summary",
    }
    return extract_structured_output(stdout, required_fields), stdout


def run_claude_batch(
    *,
    items: list[dict[str, object]],
    review_schema: dict[str, object],
    prompt_template: str,
    model: str,
    cwd: Path,
    timeout_seconds: int,
) -> tuple[list[dict[str, object]], str]:
    prompt = build_claude_batch_prompt(items, prompt_template)
    schema = build_batch_schema(review_schema, len(items))
    command = build_claude_command(prompt=prompt, schema=schema, model=model, cwd=cwd)
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip() or f"exit_code={completed.returncode}"
        raise RuntimeError(stderr)

    stdout = completed.stdout.strip()
    if not stdout:
        raise RuntimeError("Claude returned empty output.")

    payload = extract_structured_output(stdout, {"reviews"})
    reviews = payload.get("reviews")
    if not isinstance(reviews, list):
        raise RuntimeError("Claude batch output did not include a reviews array.")
    return reviews, stdout


def normalize_claude_result(
    *,
    item: dict[str, object],
    parsed_payload: dict[str, object],
    raw_output_path: Path,
    queue_lookup: dict[tuple[str, str], dict[str, object]],
    schema: dict[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    validation_errors = validate_review_response(parsed_payload, schema)
    if validation_errors:
        return None, {
            "ticker": item["ticker"],
            "exchange": item["exchange"],
            "source_file": display_path(raw_output_path),
            "error_type": "schema_validation_failed",
            "error": validation_errors,
            "parsed_payload": parsed_payload,
        }

    ticker = str(parsed_payload["ticker"])
    exchange = str(parsed_payload["exchange"])
    queue_item = queue_lookup.get((ticker, exchange))
    return (
        {
            "key": f"claude--{ticker}--{exchange}",
            "ticker": ticker,
            "exchange": exchange,
            "entry_decision": parsed_payload["entry_decision"],
            "ticker_exists": parsed_payload["ticker_exists"],
            "name_matches_listing": parsed_payload["name_matches_listing"],
            "alias_actions": parsed_payload["alias_actions"],
            "metadata_actions": parsed_payload["metadata_actions"],
            "confidence": parsed_payload["confidence"],
            "summary": parsed_payload["summary"],
            "source_file": display_path(raw_output_path),
            "request_metadata": {
                "runner": "claude_local",
                "model": None,
            },
            "queue_total_score": queue_item.get("total_score") if queue_item else None,
            "queue_findings": queue_item.get("findings") if queue_item else None,
            "queue_aliases": queue_item.get("aliases") if queue_item else None,
        },
        None,
    )


def batch_cost_from_output(raw_output: str) -> float | None:
    try:
        payload = json.loads(raw_output)
    except json.JSONDecodeError:
        return None
    total_cost = payload.get("total_cost_usd")
    return float(total_cost) if isinstance(total_cost, (int, float)) else None


def append_jsonl(path: Path, row: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def filter_items(
    items: list[dict[str, object]],
    *,
    min_score: int,
    offset: int,
    limit: int | None,
    include_tickers: set[str],
    processed_pairs: set[tuple[str, str]],
) -> list[dict[str, object]]:
    selected = [
        item
        for item in items
        if int(item["total_score"]) >= min_score
        and (not include_tickers or str(item["ticker"]) in include_tickers)
        and (str(item["ticker"]), str(item["exchange"])) not in processed_pairs
    ]
    selected = selected[offset:]
    if limit is not None:
        selected = selected[:limit]
    return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the flagged review queue through local Claude CLI.")
    parser.add_argument("--review-queue-json", type=Path, default=DEFAULT_REVIEW_QUEUE_JSON)
    parser.add_argument("--prompt-path", type=Path, default=DEFAULT_PROMPT_PATH)
    parser.add_argument("--schema-path", type=Path, default=DEFAULT_SCHEMA_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-out", type=Path, default=DEFAULT_RAW_RESPONSES_JSONL)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_NORMALIZED_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_NORMALIZED_CSV)
    parser.add_argument("--errors-out", type=Path, default=DEFAULT_ERRORS_JSON)
    parser.add_argument("--model", default="sonnet")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--allow-partial-batch", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--min-score", type=int, default=0)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--include-ticker", action="append", default=[])
    parser.add_argument("--skip-existing", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.batch_size < DEFAULT_BATCH_SIZE:
        raise SystemExit(f"--batch-size must be at least {DEFAULT_BATCH_SIZE}.")
    queue_payload = load_json(args.review_queue_json)
    schema = load_json(args.schema_path)
    prompt_template = args.prompt_path.read_text(encoding="utf-8")
    queue_lookup = build_queue_lookup(queue_payload)

    existing_rows = load_existing_jsonl(args.raw_out) if args.skip_existing else []
    processed_pairs = {
        (str(row.get("ticker")), str(row.get("exchange")))
        for row in existing_rows
        if row.get("status") == "ok"
    }
    include_tickers = {ticker.upper() for ticker in args.include_ticker}
    items = filter_items(
        list(queue_payload.get("items", [])),
        min_score=args.min_score,
        offset=args.offset,
        limit=args.limit,
        include_tickers=include_tickers,
        processed_pairs=processed_pairs,
    )

    normalized_items: list[dict[str, object]] = []
    ingest_errors: list[dict[str, object]] = []

    batches, deferred_items = build_batches(
        items,
        args.batch_size,
        allow_partial_batch=args.allow_partial_batch,
    )

    for batch_index, batch_items in enumerate(batches, start=1):
        try:
            parsed_payloads, raw_output = run_claude_batch(
                items=batch_items,
                review_schema=schema,
                prompt_template=prompt_template,
                model=args.model,
                cwd=ROOT,
                timeout_seconds=args.timeout_seconds,
            )
            reviews_by_pair = {
                (str(payload["ticker"]), str(payload["exchange"])): payload
                for payload in parsed_payloads
                if isinstance(payload, dict) and "ticker" in payload and "exchange" in payload
            }
            if len(reviews_by_pair) != len(batch_items):
                raise RuntimeError(
                    f"Claude batch returned {len(reviews_by_pair)} unique reviews for {len(batch_items)} requested items."
                )

            batch_total_cost = batch_cost_from_output(raw_output)
            estimated_cost = batch_total_cost / len(batch_items) if batch_total_cost is not None else None

            for item in batch_items:
                item_key = (str(item["ticker"]), str(item["exchange"]))
                if item_key not in reviews_by_pair:
                    raise RuntimeError(
                        f"Claude batch response missing review for {item['ticker']} on {item['exchange']}."
                    )
                parsed_payload = reviews_by_pair[item_key]
                normalized, error = normalize_claude_result(
                    item=item,
                    parsed_payload=parsed_payload,
                    raw_output_path=args.raw_out,
                    queue_lookup=queue_lookup,
                    schema=schema,
                )
                append_jsonl(
                    args.raw_out,
                    {
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "status": "ok",
                        "model": args.model,
                        "batch_id": f"batch-{batch_index:05d}",
                        "batch_size": len(batch_items),
                        "batch_total_cost_usd": batch_total_cost,
                        "estimated_cost_usd": estimated_cost,
                        "response": parsed_payload,
                    },
                )
                if normalized:
                    normalized["request_metadata"]["model"] = args.model
                    normalized["request_metadata"]["batch_id"] = f"batch-{batch_index:05d}"
                    normalized["request_metadata"]["batch_size"] = len(batch_items)
                    normalized_items.append(normalized)
                if error:
                    ingest_errors.append(error)
        except Exception as exc:  # noqa: BLE001
            for item in batch_items:
                ingest_errors.append(
                    {
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "source_file": display_path(args.raw_out),
                        "error_type": "claude_execution_failed",
                        "error": str(exc),
                    }
                )
                append_jsonl(
                    args.raw_out,
                    {
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "status": "error",
                        "model": args.model,
                        "batch_id": f"batch-{batch_index:05d}",
                        "batch_size": len(batch_items),
                        "error": str(exc),
                    },
                )

        if args.delay_seconds > 0 and batch_index < len(batches):
            time.sleep(args.delay_seconds)

    normalized_items.sort(key=lambda row: (str(row["ticker"]), str(row["exchange"])))
    payload = build_normalized_payload(
        normalized_items,
        ingest_errors,
        responses_path=args.raw_out,
        queue_path=args.review_queue_json,
        schema_path=args.schema_path,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_normalized_csv(args.csv_out, normalized_items)
    args.errors_out.write_text(json.dumps(ingest_errors, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "review_queue": display_path(args.review_queue_json),
                "processed_items": len(items),
                "processed_batches": len(batches),
                "deferred_items": len(deferred_items),
                "normalized_reviews": len(normalized_items),
                "errors": len(ingest_errors),
                "raw_out": display_path(args.raw_out),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "errors_out": display_path(args.errors_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
