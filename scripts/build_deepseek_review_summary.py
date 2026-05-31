from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_RESPONSES_JSONL = ROOT / "data" / "deepseek_review_jobs" / "raw_responses.jsonl"
DEFAULT_JSON_OUT = ROOT / "data" / "reports" / "deepseek_review_summary.json"
DEFAULT_CSV_OUT = ROOT / "data" / "reports" / "deepseek_review_summary.csv"
DEFAULT_MD_OUT = ROOT / "data" / "reports" / "deepseek_review_summary.md"

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


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def trim(value: object, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


def iter_raw_batches(path: Path) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    if not path.exists():
        return batches
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Raw response line {line_number} is not a JSON object.")
            batches.append(payload)
    return batches


def normalize_review(review: dict[str, Any], *, batch_index: int, review_kind: str) -> dict[str, Any]:
    decision = str(review.get("decision_candidate") or "uncertain")
    if decision not in VALID_DECISIONS:
        decision = "uncertain"
    safe_action = str(review.get("safe_action") or SAFE_ACTION_BY_DECISION[decision])
    if safe_action not in VALID_SAFE_ACTIONS:
        safe_action = SAFE_ACTION_BY_DECISION[decision]
    confidence = review.get("confidence", 0)
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        confidence = 0
    confidence = max(0.0, min(1.0, float(confidence)))
    return {
        "batch_index": batch_index,
        "listing_key": str(review.get("listing_key", "")),
        "ticker": str(review.get("ticker", "")),
        "exchange": str(review.get("exchange", "")),
        "review_kind": str(review.get("review_kind") or review_kind),
        "classification": trim(review.get("classification") or decision, 120),
        "decision_candidate": decision,
        "safe_action": safe_action,
        "confidence": confidence,
        "evidence_needed": trim(review.get("evidence_needed", "")),
        "rationale": trim(review.get("rationale", "")),
        "do_not_apply_reason": trim(review.get("do_not_apply_reason", "")),
    }


def normalize_batches(batches: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reviews: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for fallback_index, batch in enumerate(batches, start=1):
        batch_index = int(batch.get("batch_index") or fallback_index)
        review_kind = str(batch.get("review_kind") or "unknown")
        response = batch.get("response")
        if not isinstance(response, dict):
            errors.append({"batch_index": batch_index, "error": "missing response object"})
            continue
        raw_reviews = response.get("reviews")
        if not isinstance(raw_reviews, list):
            errors.append({"batch_index": batch_index, "error": "missing reviews array"})
            continue
        for raw_review in raw_reviews:
            if not isinstance(raw_review, dict):
                errors.append({"batch_index": batch_index, "error": "review is not an object"})
                continue
            reviews.append(normalize_review(raw_review, batch_index=batch_index, review_kind=review_kind))
    return reviews, errors


def summarize_reviews(reviews: list[dict[str, Any]], errors: list[dict[str, Any]]) -> dict[str, Any]:
    by_kind = Counter(str(review.get("review_kind", "")) for review in reviews)
    by_decision = Counter(str(review.get("decision_candidate", "")) for review in reviews)
    by_safe_action = Counter(str(review.get("safe_action", "")) for review in reviews)
    by_kind_decision: dict[str, Counter[str]] = {}
    by_kind_safe_action: dict[str, Counter[str]] = {}
    for review in reviews:
        kind = str(review.get("review_kind", "unknown") or "unknown")
        by_kind_decision.setdefault(kind, Counter())[str(review.get("decision_candidate", "unknown") or "unknown")] += 1
        by_kind_safe_action.setdefault(kind, Counter())[str(review.get("safe_action", "unknown") or "unknown")] += 1
    return {
        "rows": len(reviews),
        "errors": len(errors),
        "review_kind_totals": dict(sorted(by_kind.items())),
        "decision_totals": dict(sorted(by_decision.items())),
        "safe_action_totals": dict(sorted(by_safe_action.items())),
        "review_kind_decision_totals": {
            kind: dict(sorted(counter.items()))
            for kind, counter in sorted(by_kind_decision.items())
        },
        "review_kind_safe_action_totals": {
            kind: dict(sorted(counter.items()))
            for kind, counter in sorted(by_kind_safe_action.items())
        },
    }


def build_payload(raw_path: Path) -> dict[str, Any]:
    batches = iter_raw_batches(raw_path)
    reviews, errors = normalize_batches(batches)
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "raw_responses_jsonl": display_path(raw_path),
            "raw_batches": len(batches),
            "policy": (
                "DeepSeek review suggestions are triage only. They do not authorize inferred identifiers, sectors, "
                "categories, names, symbol changes, scope changes, or data application."
            ),
        },
        "summary": summarize_reviews(reviews, errors),
        "items": reviews,
        "errors": errors,
    }


def write_csv(path: Path, items: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "batch_index",
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
        writer.writerows(items)


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# DeepSeek Review Summary",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: DeepSeek output is triage only and does not authorize data application.",
        "",
        "## Totals",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Raw batches | {meta['raw_batches']} |",
        f"| Review rows | {summary['rows']} |",
        f"| Errors | {summary['errors']} |",
        "",
        "## Decisions By Queue",
        "",
        "| Review kind | Decision | Rows |",
        "| --- | --- | ---: |",
    ]
    for kind, decision_totals in summary["review_kind_decision_totals"].items():
        for decision, count in decision_totals.items():
            lines.append(f"| {kind} | {decision} | {count} |")
    lines.extend(
        [
            "",
            "## Safe Actions By Queue",
            "",
            "| Review kind | Safe action | Rows |",
            "| --- | --- | ---: |",
        ]
    )
    for kind, safe_action_totals in summary["review_kind_safe_action_totals"].items():
        for safe_action, count in safe_action_totals.items():
            lines.append(f"| {kind} | {safe_action} | {count} |")
    lines.extend(
        [
            "",
            "## Next Review",
            "",
            "- `possible_duplicate_or_cross_listing` rows need listing-keyed identity review before any merge/link decision.",
            "- `needs_official_evidence` rows stay source gaps until an official source or reviewed fallback is attached.",
            "- `keep_source_gap` rows remain blocked from data fill unless the underlying official taxonomy mapping is implemented.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a combined summary from DeepSeek raw review responses.")
    parser.add_argument("--raw-responses-jsonl", type=Path, default=DEFAULT_RAW_RESPONSES_JSONL)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    payload = build_payload(args.raw_responses_jsonl)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_csv(args.csv_out, payload["items"])
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"summary": payload["summary"], "json_out": display_path(args.json_out)}, indent=2))


if __name__ == "__main__":
    main()
