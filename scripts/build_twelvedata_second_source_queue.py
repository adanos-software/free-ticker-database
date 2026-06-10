"""Build second-source validation queues for Twelve Data candidates."""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_RENAME_CSV = Path("data/reports/twelvedata_rename_candidates.csv")
DEFAULT_DEEPSEEK_CSV = Path("data/deepseek_review_jobs/twelvedata_all_batches_normalized_reviews.csv")
DEFAULT_OUTPUT_CSV = Path("data/reports/twelvedata_all_batches_second_source_queue.csv")
DEFAULT_SUMMARY_JSON = Path("data/reports/twelvedata_all_batches_second_source_queue_summary.json")
DEFAULT_SUMMARY_MD = Path("data/reports/twelvedata_all_batches_second_source_queue.md")

FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "mic_code",
    "local_name",
    "twelvedata_name",
    "twelvedata_type",
    "name_score",
    "deepseek_decision_candidate",
    "deepseek_safe_action",
    "deepseek_confidence",
    "provider_queue",
    "validation_status",
    "evidence_required",
    "review_batch",
]


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def provider_queue_for(exchange: str) -> str:
    if exchange in {"BATS", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "OTC"}:
        return "openfigi|alphavantage|fmp"
    if exchange in {"NEO", "TSX", "TSXV"}:
        return "openfigi|fmp"
    return "openfigi|fmp"


def build_queue(rename_rows: list[dict[str, str]], deepseek_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rename_by_key = {row["listing_key"]: row for row in rename_rows}
    rows: list[dict[str, str]] = []
    for review in deepseek_rows:
        key = review.get("listing_key", "")
        source = rename_by_key.get(key)
        if not source:
            continue
        rows.append(
            {
                "listing_key": key,
                "ticker": source.get("ticker", ""),
                "exchange": source.get("exchange", ""),
                "mic_code": source.get("mic_code", ""),
                "local_name": source.get("local_name", ""),
                "twelvedata_name": source.get("twelvedata_name", ""),
                "twelvedata_type": source.get("twelvedata_type", ""),
                "name_score": source.get("name_score", ""),
                "deepseek_decision_candidate": review.get("decision_candidate", ""),
                "deepseek_safe_action": review.get("safe_action", ""),
                "deepseek_confidence": review.get("confidence", ""),
                "provider_queue": provider_queue_for(source.get("exchange", "")),
                "validation_status": "pending_provider_env",
                "evidence_required": (
                    "At least one second source matching ticker, venue, and issuer identity before any name or "
                    "metadata apply. DeepSeek triage alone is not apply evidence."
                ),
                "review_batch": source.get("review_batch", ""),
            }
        )
    return rows


def summarize(rows: list[dict[str, str]]) -> dict[str, object]:
    env_status = {
        key: "set" if os.getenv(key) else "missing"
        for key in ["OPENFIGI_API_KEY", "ALPHAVANTAGE_API_KEY", "FMP_API_KEY"]
    }
    return {
        "generated_at": now_iso(),
        "rows": len(rows),
        "provider_queue_counts": Counter(row["provider_queue"] for row in rows).most_common(),
        "review_batch_counts": Counter(row["review_batch"] for row in rows).most_common(),
        "deepseek_decision_counts": Counter(row["deepseek_decision_candidate"] for row in rows).most_common(),
        "env_status": env_status,
        "policy": (
            "Second-source validation is required before applying Twelve Data-driven name, identifier, alias, "
            "scope, or listing changes."
        ),
    }


def write_markdown(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Twelve Data Batch A Second-Source Queue",
        "",
        str(summary["policy"]),
        "",
        f"- Rows: {summary['rows']:,}",
        "",
        "## Provider Queues",
        "",
        "| Providers | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {providers} | {count:,} |" for providers, count in summary["provider_queue_counts"])
    lines.extend(["", "## Review Batches", "", "| Batch | Rows |", "| --- | ---: |"])
    lines.extend(f"| {batch} | {count:,} |" for batch, count in summary["review_batch_counts"])
    lines.extend(
        [
            "",
            "## DeepSeek Decisions",
            "",
            "| Decision | Rows |",
            "| --- | ---: |",
        ]
    )
    lines.extend(f"| {decision} | {count:,} |" for decision, count in summary["deepseek_decision_counts"])
    lines.extend(
        [
            "",
            "## Environment",
            "",
            "Provider API keys are read from environment variables only. No key values are stored in this report.",
        ]
    )
    for key, status in summary["env_status"].items():
        lines.append(f"- `{key}`: {status}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rename-csv", type=Path, default=DEFAULT_RENAME_CSV)
    parser.add_argument("--deepseek-csv", type=Path, default=DEFAULT_DEEPSEEK_CSV)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY_MD)
    args = parser.parse_args()

    rows = build_queue(read_csv(args.rename_csv), read_csv(args.deepseek_csv))
    write_csv(args.output_csv, rows)
    summary = summarize(rows)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.summary_md, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
