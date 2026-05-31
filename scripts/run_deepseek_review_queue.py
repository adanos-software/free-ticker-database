from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = ROOT / "data" / "reports" / "otc_scope_review.csv"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "deepseek_review_jobs"
DEFAULT_RAW_RESPONSES_JSONL = DEFAULT_OUTPUT_DIR / "raw_responses.jsonl"
DEFAULT_NORMALIZED_JSON = DEFAULT_OUTPUT_DIR / "normalized_reviews.json"
DEFAULT_NORMALIZED_CSV = DEFAULT_OUTPUT_DIR / "normalized_reviews.csv"
DEFAULT_ERRORS_JSON = DEFAULT_OUTPUT_DIR / "errors.json"
DEFAULT_MODEL = "deepseek-v4-pro"
DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_BATCH_SIZE = 5
DEFAULT_ROW_LIMIT = 25
DEFAULT_ENV_FILE = ROOT / ".env"

REVIEW_FIELDS_BY_KIND = {
    "otc_scope": [
        "listing_key",
        "ticker",
        "exchange",
        "asset_type",
        "name",
        "instrument_scope",
        "scope_reason",
        "quality_status",
        "issue_types",
        "source_gap_class",
        "source_of_truth_outcome",
        "scope_decision",
        "review_bucket",
        "review_priority",
        "metadata_enrichment_gate",
    ],
    "weak_sector": [
        "listing_key",
        "ticker",
        "exchange",
        "asset_type",
        "name",
        "gap_class",
        "source_of_truth_outcome",
        "core_action",
        "fill_action",
        "review_needed",
        "recommended_next_source",
        "source_gate",
        "official_source_context",
        "official_capability",
        "weak_sector_resolution_queue",
    ],
    "masterfile_collision": [
        "target_listing_key",
        "ticker",
        "target_exchange",
        "official_name",
        "official_asset_type",
        "official_isin",
        "official_sector",
        "official_source_key",
        "existing_listing_keys",
        "existing_exchanges",
        "existing_names",
        "existing_asset_types",
        "existing_isins",
        "same_isin_listing_keys",
        "identity_evidence",
        "collision_decision",
    ],
}

VALID_DECISIONS = {
    "keep_source_gap",
    "needs_official_evidence",
    "candidate_apply_blocked",
    "possible_duplicate_or_cross_listing",
    "out_of_scope_candidate",
    "uncertain",
}

VALID_SAFE_ACTIONS = {
    "needs_official_evidence",
    "likely_same_issuer_review",
    "likely_distinct_issuer_review",
    "source_gap_accept",
    "candidate_for_official_followup",
}

SAFE_ACTION_BY_DECISION = {
    "keep_source_gap": "source_gap_accept",
    "needs_official_evidence": "needs_official_evidence",
    "candidate_apply_blocked": "candidate_for_official_followup",
    "possible_duplicate_or_cross_listing": "likely_same_issuer_review",
    "out_of_scope_candidate": "candidate_for_official_followup",
    "uncertain": "needs_official_evidence",
}

SAFE_ACTIONS_BY_DECISION = {
    decision: {safe_action}
    for decision, safe_action in SAFE_ACTION_BY_DECISION.items()
}
SAFE_ACTIONS_BY_DECISION["possible_duplicate_or_cross_listing"] = {
    "likely_same_issuer_review",
    "likely_distinct_issuer_review",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def trim(value: object, max_length: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key != "DEEPSEEK_API_KEY" or os.environ.get(key):
            continue
        os.environ[key] = value.strip().strip("'\"")


def read_csv_rows(path: Path, *, offset: int = 0, limit: int | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected = rows[offset:]
    if limit is not None:
        selected = selected[:limit]
    return selected


def compact_row(row: dict[str, str], review_kind: str) -> dict[str, str]:
    fields = REVIEW_FIELDS_BY_KIND[review_kind]
    compacted = {field: trim(row.get(field, "")) for field in fields if field in row}
    if "listing_key" not in compacted:
        compacted["listing_key"] = trim(row.get("listing_key") or row.get("target_listing_key", ""))
    if "exchange" not in compacted:
        compacted["exchange"] = trim(row.get("exchange") or row.get("target_exchange", ""))
    return compacted


def chunk_rows(rows: list[dict[str, str]], batch_size: int) -> list[list[dict[str, str]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [rows[index : index + batch_size] for index in range(0, len(rows), batch_size)]


def build_prompt(rows: list[dict[str, str]], *, review_kind: str) -> str:
    compacted_rows = [compact_row(row, review_kind) for row in rows]
    return (
        "You are a conservative review assistant for a truth-sensitive global ticker database.\n"
        "Use ONLY the rows in this prompt. Do not browse, do not infer missing facts, and do not invent ISINs, "
        "sectors, ETF categories, issuer names, aliases, symbol changes, or scope decisions.\n"
        "Your job is to triage review queues and explain what evidence is still needed.\n"
        "Never output a value that should be applied to the database. If official evidence is missing, say so.\n"
        "Return JSON only with this exact top-level shape:\n"
        '{"reviews":[{"listing_key":"...","ticker":"...","exchange":"...","review_kind":"...",'
        '"classification":"...",'
        '"decision_candidate":"keep_source_gap|needs_official_evidence|candidate_apply_blocked|'
        'possible_duplicate_or_cross_listing|out_of_scope_candidate|uncertain",'
        '"safe_action":"needs_official_evidence|likely_same_issuer_review|likely_distinct_issuer_review|'
        'source_gap_accept|candidate_for_official_followup",'
        '"confidence":0.0,"evidence_needed":"...","rationale":"...",'
        '"do_not_apply_reason":"..."}]}\n'
        f"Return exactly {len(compacted_rows)} review objects in the same order as the rows.\n"
        "Set confidence low when the row lacks official evidence. Prefer needs_official_evidence or uncertain over "
        "actionable decisions.\n\n"
        f"Review kind: {review_kind}\n"
        f"Rows:\n{json.dumps(compacted_rows, ensure_ascii=True, indent=2)}"
    )


def parse_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        cleaned = cleaned.removeprefix("json").strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or start >= end:
            raise
        parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("DeepSeek response must be a JSON object.")
    return parsed


def normalize_review(raw: dict[str, Any], source_row: dict[str, str], review_kind: str) -> dict[str, Any]:
    decision = str(raw.get("decision_candidate") or "uncertain")
    if decision not in VALID_DECISIONS:
        decision = "uncertain"
    safe_action = str(raw.get("safe_action") or SAFE_ACTION_BY_DECISION[decision])
    if safe_action not in VALID_SAFE_ACTIONS or safe_action not in SAFE_ACTIONS_BY_DECISION[decision]:
        safe_action = SAFE_ACTION_BY_DECISION[decision]
    confidence = raw.get("confidence", 0)
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        confidence = 0
    confidence = max(0.0, min(1.0, float(confidence)))
    return {
        "listing_key": str(raw.get("listing_key") or source_row.get("listing_key") or source_row.get("target_listing_key", "")),
        "ticker": str(raw.get("ticker") or source_row.get("ticker", "")),
        "exchange": str(raw.get("exchange") or source_row.get("exchange") or source_row.get("target_exchange", "")),
        "review_kind": str(raw.get("review_kind") or review_kind),
        "classification": trim(raw.get("classification") or decision, 120),
        "decision_candidate": decision,
        "safe_action": safe_action,
        "confidence": confidence,
        "evidence_needed": trim(raw.get("evidence_needed", ""), 500),
        "rationale": trim(raw.get("rationale", ""), 500),
        "do_not_apply_reason": trim(raw.get("do_not_apply_reason", ""), 500),
    }


def normalize_payload(payload: dict[str, Any], source_rows: list[dict[str, str]], review_kind: str) -> list[dict[str, Any]]:
    reviews = payload.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError("DeepSeek response is missing reviews array.")
    if len(reviews) != len(source_rows):
        raise ValueError(f"Expected {len(source_rows)} reviews, got {len(reviews)}.")
    normalized = []
    for raw, source_row in zip(reviews, source_rows, strict=True):
        if not isinstance(raw, dict):
            raise ValueError("Each review must be a JSON object.")
        normalized.append(normalize_review(raw, source_row, review_kind))
    return normalized


def call_deepseek(
    *,
    api_key: str,
    prompt: str,
    model: str,
    base_url: str,
    timeout: float,
) -> dict[str, Any]:
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "Return JSON only. Do not browse. Do not invent facts. Do not suggest direct data application.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    content = payload["choices"][0]["message"]["content"]
    return parse_json_object(content)


def write_outputs(
    *,
    normalized_reviews: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    output_json: Path,
    output_csv: Path,
    errors_json: Path,
    input_csv: Path,
    review_kind: str,
    model: str,
    dry_run: bool,
) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_meta": {
            "generated_at": utc_now_iso(),
            "source_csv": display_path(input_csv),
            "review_kind": review_kind,
            "model": model,
            "dry_run": dry_run,
            "rows": len(normalized_reviews),
            "errors": len(errors),
            "policy": (
                "DeepSeek review suggestions are triage only. They do not authorize inferred identifiers, sectors, "
                "categories, names, symbol changes, or scope changes."
            ),
        },
        "items": normalized_reviews,
    }
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "listing_key",
                "ticker",
                "exchange",
                "review_kind",
                "classification",
                "decision_candidate",
                "safe_action",
                "confidence",
                "evidence_needed",
                "rationale",
                "do_not_apply_reason",
            ],
        )
        writer.writeheader()
        writer.writerows(normalized_reviews)
    errors_json.write_text(json.dumps({"errors": errors}, indent=2), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    load_env_file(args.env_file)
    rows = read_csv_rows(args.input_csv, offset=args.offset, limit=args.limit)
    batches = chunk_rows(rows, args.batch_size)
    normalized_reviews: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    args.output_dir.mkdir(parents=True, exist_ok=True)
    with args.raw_responses_jsonl.open("a", encoding="utf-8") as raw_handle:
        for batch_index, batch in enumerate(batches, start=1):
            prompt = build_prompt(batch, review_kind=args.review_kind)
            try:
                if args.dry_run:
                    payload = {
                        "reviews": [
                            {
                                "listing_key": row.get("listing_key") or row.get("target_listing_key", ""),
                                "ticker": row.get("ticker", ""),
                                "exchange": row.get("exchange") or row.get("target_exchange", ""),
                                "review_kind": args.review_kind,
                                "classification": "dry_run_schema_validation",
                                "decision_candidate": "needs_official_evidence",
                                "safe_action": "needs_official_evidence",
                                "confidence": 0.1,
                                "evidence_needed": "Dry run; no DeepSeek API request was made.",
                                "rationale": "Dry run validates batching and output schema only.",
                                "do_not_apply_reason": "No external or official evidence was reviewed.",
                            }
                            for row in batch
                        ]
                    }
                else:
                    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
                    if not api_key:
                        raise RuntimeError("DEEPSEEK_API_KEY is required unless --dry-run is used.")
                    payload = call_deepseek(
                        api_key=api_key,
                        prompt=prompt,
                        model=args.model,
                        base_url=args.base_url,
                        timeout=args.timeout,
                    )
                    time.sleep(args.sleep_seconds)
                raw_handle.write(
                    json.dumps(
                        {
                            "batch_index": batch_index,
                            "review_kind": args.review_kind,
                            "response": payload,
                        },
                        ensure_ascii=True,
                    )
                    + "\n"
                )
                normalized_reviews.extend(normalize_payload(payload, batch, args.review_kind))
            except (RuntimeError, ValueError, KeyError, json.JSONDecodeError, urllib.error.URLError) as exc:
                errors.append({"batch_index": batch_index, "error": str(exc)})

    write_outputs(
        normalized_reviews=normalized_reviews,
        errors=errors,
        output_json=args.normalized_json,
        output_csv=args.normalized_csv,
        errors_json=args.errors_json,
        input_csv=args.input_csv,
        review_kind=args.review_kind,
        model=args.model,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {
                "rows": len(normalized_reviews),
                "errors": len(errors),
                "normalized_json": display_path(args.normalized_json),
                "normalized_csv": display_path(args.normalized_csv),
                "errors_json": display_path(args.errors_json),
            },
            indent=2,
        )
    )
    return 1 if errors else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run conservative DeepSeek review batches over generated review queues.")
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--review-kind", choices=sorted(REVIEW_FIELDS_BY_KIND), default="otc_scope")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-responses-jsonl", type=Path, default=DEFAULT_RAW_RESPONSES_JSONL)
    parser.add_argument("--normalized-json", type=Path, default=DEFAULT_NORMALIZED_JSON)
    parser.add_argument("--normalized-csv", type=Path, default=DEFAULT_NORMALIZED_CSV)
    parser.add_argument("--errors-json", type=Path, default=DEFAULT_ERRORS_JSON)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=DEFAULT_ROW_LIMIT)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Local env file for DEEPSEEK_API_KEY. Defaults to .env, which is git-ignored.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    raise SystemExit(run(parse_args(argv)))


if __name__ == "__main__":
    main()
