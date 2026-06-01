from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEEPSEEK_SUMMARY_JSON = ROOT / "data" / "reports" / "deepseek_review_summary.json"
DEFAULT_COLLISION_REVIEW_CSV = ROOT / "data" / "reports" / "masterfile_collision_review.csv"
DEFAULT_JSON_OUT = ROOT / "data" / "reports" / "deepseek_collision_review_queue.json"
DEFAULT_CSV_OUT = ROOT / "data" / "reports" / "deepseek_collision_review_queue.csv"
DEFAULT_MD_OUT = ROOT / "data" / "reports" / "deepseek_collision_review_queue.md"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def collision_key(row: dict[str, str]) -> str:
    return row.get("target_listing_key") or row.get("listing_key", "")


def build_collision_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {collision_key(row): row for row in rows if collision_key(row)}


def select_deepseek_collision_reviews(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    selected: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("review_kind") != "masterfile_collision":
            continue
        if item.get("decision_candidate") != "possible_duplicate_or_cross_listing":
            continue
        selected.append(item)
    return selected


def build_queue_rows(
    *,
    deepseek_reviews: list[dict[str, Any]],
    collision_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    collision_lookup = build_collision_lookup(collision_rows)
    queue_rows: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for review in deepseek_reviews:
        listing_key = str(review.get("listing_key", ""))
        collision = collision_lookup.get(listing_key)
        if collision is None:
            unmatched.append({"listing_key": listing_key, "reason": "missing_masterfile_collision_review_row"})
            continue
        row = {
            "target_listing_key": collision.get("target_listing_key", listing_key),
            "ticker": collision.get("ticker", review.get("ticker", "")),
            "target_exchange": collision.get("target_exchange", review.get("exchange", "")),
            "official_name": collision.get("official_name", ""),
            "official_asset_type": collision.get("official_asset_type", ""),
            "official_isin": collision.get("official_isin", ""),
            "official_source_key": collision.get("official_source_key", ""),
            "existing_listing_keys": collision.get("existing_listing_keys", ""),
            "existing_exchanges": collision.get("existing_exchanges", ""),
            "existing_names": collision.get("existing_names", ""),
            "existing_asset_types": collision.get("existing_asset_types", ""),
            "existing_isins": collision.get("existing_isins", ""),
            "same_isin_listing_keys": collision.get("same_isin_listing_keys", ""),
            "identity_evidence": collision.get("identity_evidence", ""),
            "collision_risk_flags": collision.get("collision_risk_flags", ""),
            "deepseek_decision_candidate": review.get("decision_candidate", ""),
            "deepseek_confidence": review.get("confidence", 0),
            "deepseek_evidence_needed": review.get("evidence_needed", ""),
            "deepseek_rationale": review.get("rationale", ""),
            "deepseek_do_not_apply_reason": review.get("do_not_apply_reason", ""),
            "review_queue": "manual_cross_listing_identity_review",
            "review_gate": (
                "Do not merge, alias, or dedupe automatically. Confirm listing status, ISIN fungibility, "
                "exchange/MIC, instrument type, and local trading attributes before any data change."
            ),
            "recommended_next_action": (
                "Review as possible cross-listing using official source rows and existing listing keys; keep as "
                "source-gap/collision queue item until reviewer records listing-keyed evidence."
            ),
        }
        queue_rows.append(row)
    return queue_rows, unmatched


def summarize(queue_rows: list[dict[str, Any]], unmatched: list[dict[str, Any]]) -> dict[str, Any]:
    by_exchange = Counter(str(row.get("target_exchange", "")) for row in queue_rows)
    by_source = Counter(str(row.get("official_source_key", "")) for row in queue_rows)
    return {
        "rows": len(queue_rows),
        "unmatched_deepseek_rows": len(unmatched),
        "target_exchange_totals": dict(sorted(by_exchange.items())),
        "official_source_key_totals": dict(sorted(by_source.items())),
    }


def build_payload(deepseek_summary_json: Path, collision_review_csv: Path) -> dict[str, Any]:
    deepseek_payload = load_json(deepseek_summary_json)
    collision_rows = load_csv_rows(collision_review_csv)
    deepseek_reviews = select_deepseek_collision_reviews(deepseek_payload)
    queue_rows, unmatched = build_queue_rows(
        deepseek_reviews=deepseek_reviews,
        collision_rows=collision_rows,
    )
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "deepseek_summary_json": display_path(deepseek_summary_json),
            "masterfile_collision_review_csv": display_path(collision_review_csv),
            "policy": (
                "DeepSeek collision reviews are triage only. They do not authorize automatic merge, dedupe, alias, "
                "identifier, or cross-listing changes."
            ),
        },
        "summary": summarize(queue_rows, unmatched),
        "items": queue_rows,
        "unmatched_deepseek_rows": unmatched,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "target_listing_key",
        "ticker",
        "target_exchange",
        "official_name",
        "official_asset_type",
        "official_isin",
        "official_source_key",
        "existing_listing_keys",
        "existing_exchanges",
        "existing_names",
        "existing_asset_types",
        "existing_isins",
        "same_isin_listing_keys",
        "identity_evidence",
        "collision_risk_flags",
        "deepseek_decision_candidate",
        "deepseek_confidence",
        "deepseek_evidence_needed",
        "deepseek_rationale",
        "deepseek_do_not_apply_reason",
        "review_queue",
        "review_gate",
        "recommended_next_action",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# DeepSeek Collision Review Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: DeepSeek collision reviews are triage only and do not authorize automatic data changes.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Queue rows | {summary['rows']} |",
        f"| Unmatched DeepSeek rows | {summary['unmatched_deepseek_rows']} |",
        "",
        "## Target Exchanges",
        "",
        "| Exchange | Rows |",
        "| --- | ---: |",
    ]
    for exchange, count in summary["target_exchange_totals"].items():
        lines.append(f"| {exchange or 'missing'} | {count} |")
    lines.extend(
        [
            "",
            "## Official Evidence Sources",
            "",
            "| Official source key | Rows |",
            "| --- | ---: |",
        ]
    )
    for source_key, count in summary["official_source_key_totals"].items():
        lines.append(f"| {source_key or 'missing'} | {count} |")
    lines.extend(
        [
            "",
            "## Review Gate",
            "",
            "Do not merge, alias, or dedupe automatically. Each row needs listing-keyed reviewer evidence covering "
            "official listing status, ISIN fungibility, exchange/MIC, instrument type, and local trading attributes.",
            "",
            "Next evidence source: use the row's `official_source_key` first, then verify the existing listing keys "
            "against their official exchange or issuer pages before recording any gated data change.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a manual cross-listing review queue from DeepSeek collision triage.")
    parser.add_argument("--deepseek-summary-json", type=Path, default=DEFAULT_DEEPSEEK_SUMMARY_JSON)
    parser.add_argument("--collision-review-csv", type=Path, default=DEFAULT_COLLISION_REVIEW_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    payload = build_payload(args.deepseek_summary_json, args.collision_review_csv)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_csv(args.csv_out, payload["items"])
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"summary": payload["summary"], "json_out": display_path(args.json_out)}, indent=2))


if __name__ == "__main__":
    main()
