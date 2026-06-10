"""Build curated Twelve Data review queues from comparison reports."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path


REPORT_DIR = Path("data/reports")

def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


ALLOWED_TWELVE_TYPES = {
    "American Depositary Receipt",
    "Common Stock",
    "Depositary Receipt",
    "Global Depositary Receipt",
    "Preferred Stock",
    "REIT",
}

BATCH_A_TWELVE_EXCHANGES = {"CBOE", "NASDAQ", "NYSE", "OTC"}
BATCH_A_LOCAL_EXCHANGES = {"BATS", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "OTC"}
BATCH_B_TWELVE_EXCHANGES = {"NEO", "TSX", "TSXV"}
BATCH_B_LOCAL_EXCHANGES = {"NEO", "TSX", "TSXV"}
BATCH_C_TWELVE_EXCHANGES = {"ASX", "Bovespa", "BSE", "HKEX", "JPX", "LSE", "NSE"}
BATCH_C_LOCAL_EXCHANGES = {"ASX", "B3", "BSE_IN", "HKEX", "LSE", "NSE_IN", "TSE"}

LOW_PRIORITY_TWELVE_EXCHANGES = {"FSX", "Munich", "XDUS", "XHAN", "XSTU"}

COMMON_FIELDS = [
    "reason_code",
    "priority",
    "ticker",
    "exchange",
    "mic_code",
    "local_name",
    "twelvedata_name",
    "twelvedata_type",
    "name_score",
    "candidate_action",
    "validation_status",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def exchange_batch(exchange: str, *, local: bool) -> str:
    if local:
        if exchange in BATCH_A_LOCAL_EXCHANGES:
            return "batch_a_us_core"
        if exchange in BATCH_B_LOCAL_EXCHANGES:
            return "batch_b_canada"
        if exchange in BATCH_C_LOCAL_EXCHANGES:
            return "batch_c_high_value_international"
    else:
        if exchange in BATCH_A_TWELVE_EXCHANGES:
            return "batch_a_us_core"
        if exchange in BATCH_B_TWELVE_EXCHANGES:
            return "batch_b_canada"
        if exchange in BATCH_C_TWELVE_EXCHANGES:
            return "batch_c_high_value_international"
    return "later_global_review"


def priority_for_batch(batch: str, *, score: float | None = None, low_priority_venue: bool = False) -> str:
    if low_priority_venue:
        return "P4"
    if batch == "batch_a_us_core":
        if score is None:
            return "P1"
        if score <= 0.35:
            return "P0"
        if score <= 0.55:
            return "P1"
        return "P2"
    if batch == "batch_b_canada":
        return "P1" if score is None or score <= 0.55 else "P2"
    if batch == "batch_c_high_value_international":
        return "P2"
    return "P3"


def build_gap_candidates(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in read_csv(path):
        twelve_type = row.get("type", "")
        if twelve_type not in ALLOWED_TWELVE_TYPES:
            continue
        exchange = row.get("exchange", "")
        batch = exchange_batch(exchange, local=False)
        same_symbol_exchanges = row.get("same_symbol_local_exchanges", "")
        reason_parts = ["missing_twelvedata_stock_like"]
        if same_symbol_exchanges:
            reason_parts.append("same_symbol_exists_elsewhere")
        if exchange in LOW_PRIORITY_TWELVE_EXCHANGES:
            reason_parts.append("likely_secondary_german_venue")
        if not row.get("figi_code", ""):
            reason_parts.append("missing_twelvedata_figi")
        rows.append(
            {
                **{field: "" for field in COMMON_FIELDS},
                "reason_code": "|".join(reason_parts),
                "priority": priority_for_batch(
                    batch,
                    low_priority_venue=exchange in LOW_PRIORITY_TWELVE_EXCHANGES,
                ),
                "ticker": row.get("symbol", ""),
                "exchange": exchange,
                "mic_code": row.get("mic_code", ""),
                "local_name": "",
                "twelvedata_name": row.get("name", ""),
                "twelvedata_type": twelve_type,
                "name_score": "",
                "candidate_action": "validate_new_listing_with_second_source",
                "validation_status": "pending_second_source",
                "country": row.get("country", ""),
                "currency": row.get("currency", ""),
                "figi_code": row.get("figi_code", ""),
                "same_symbol_local_exchanges": same_symbol_exchanges,
                "review_batch": batch,
            }
        )
    return sorted(rows, key=lambda row: (row["priority"], row["review_batch"], row["exchange"], row["ticker"]))


def build_rename_candidates(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in read_csv(path):
        twelve_type = row.get("twelvedata_type", "")
        if twelve_type not in ALLOWED_TWELVE_TYPES:
            continue
        exchange = row.get("local_exchange", "")
        batch = exchange_batch(exchange, local=True)
        score = float(row.get("name_ratio") or 0)
        reason_parts = ["low_name_similarity"]
        if score <= 0.35:
            reason_parts.append("severe_name_mismatch")
        if exchange in BATCH_A_LOCAL_EXCHANGES:
            reason_parts.append("batch_a_us_core")
        rows.append(
            {
                **{field: "" for field in COMMON_FIELDS},
                "reason_code": "|".join(reason_parts),
                "priority": priority_for_batch(batch, score=score),
                "ticker": row.get("ticker", ""),
                "exchange": exchange,
                "mic_code": row.get("twelvedata_mic", ""),
                "local_name": row.get("local_name", ""),
                "twelvedata_name": row.get("twelvedata_name", ""),
                "twelvedata_type": twelve_type,
                "name_score": row.get("name_ratio", ""),
                "candidate_action": "deepseek_classify_rename_vs_different_security",
                "validation_status": "pending_deepseek_review",
                "listing_key": row.get("listing_key", ""),
                "twelvedata_exchange": row.get("twelvedata_exchange", ""),
                "review_batch": batch,
            }
        )
    return sorted(rows, key=lambda row: (row["priority"], float(row["name_score"] or 0), row["exchange"], row["ticker"]))


def build_stale_candidates(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in read_csv(path):
        exchange = row.get("exchange", "")
        batch = exchange_batch(exchange, local=True)
        reason_parts = ["local_listing_not_seen_in_twelvedata"]
        if exchange in BATCH_A_LOCAL_EXCHANGES:
            reason_parts.append("batch_a_us_core")
        rows.append(
            {
                **{field: "" for field in COMMON_FIELDS},
                "reason_code": "|".join(reason_parts),
                "priority": priority_for_batch(batch),
                "ticker": row.get("ticker", ""),
                "exchange": exchange,
                "mic_code": "",
                "local_name": row.get("name", ""),
                "twelvedata_name": "",
                "twelvedata_type": "",
                "name_score": "",
                "candidate_action": "validate_stale_or_mapping_gap",
                "validation_status": "pending_second_source",
                "listing_key": row.get("listing_key", ""),
                "asset_type": row.get("asset_type", ""),
                "country": row.get("country", ""),
                "isin": row.get("isin", ""),
                "review_batch": batch,
            }
        )
    return sorted(rows, key=lambda row: (row["priority"], row["review_batch"], row["exchange"], row["ticker"]))


def write_markdown(path: Path, counts: dict[str, object]) -> None:
    lines = [
        "# Twelve Data Review Queues",
        "",
        "Generated from local Twelve Data comparison reports. These queues are review inputs, not apply files.",
        "",
        "## Counts",
        "",
        f"- Gap candidates: {counts['gap_total']:,}",
        f"- Rename candidates: {counts['rename_total']:,}",
        f"- Stale local candidates: {counts['stale_total']:,}",
        "",
        "## Priority Distribution",
        "",
        "| Queue | Priority | Rows |",
        "| --- | --- | ---: |",
    ]
    for queue_name in ["gap", "rename", "stale"]:
        for priority, total in counts[f"{queue_name}_priority_counts"]:
            lines.append(f"| {queue_name} | {priority} | {total:,} |")
    lines.extend(
        [
            "",
            "## Batch Distribution",
            "",
            "| Queue | Batch | Rows |",
            "| --- | --- | ---: |",
        ]
    )
    for queue_name in ["gap", "rename", "stale"]:
        for batch, total in counts[f"{queue_name}_batch_counts"]:
            lines.append(f"| {queue_name} | {batch} | {total:,} |")
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "Validate Batch A top candidates with second-source lookups and DeepSeek classification before any apply step.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize(rows: list[dict[str, object]]) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    return (
        sorted(Counter(str(row["priority"]) for row in rows).items()),
        Counter(str(row["review_batch"]) for row in rows).most_common(),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-dir", type=Path, default=REPORT_DIR)
    args = parser.parse_args()

    report_dir = args.report_dir
    gaps = build_gap_candidates(report_dir / "twelvedata_missing_stock_like.csv")
    renames = build_rename_candidates(report_dir / "twelvedata_name_mismatches.csv")
    stale = build_stale_candidates(report_dir / "twelvedata_local_unmatched.csv")

    write_csv(
        report_dir / "twelvedata_gap_candidates.csv",
        gaps,
        COMMON_FIELDS + ["country", "currency", "figi_code", "same_symbol_local_exchanges", "review_batch"],
    )
    write_csv(
        report_dir / "twelvedata_rename_candidates.csv",
        renames,
        COMMON_FIELDS + ["listing_key", "twelvedata_exchange", "review_batch"],
    )
    write_csv(
        report_dir / "twelvedata_stale_local_candidates.csv",
        stale,
        COMMON_FIELDS + ["listing_key", "asset_type", "country", "isin", "review_batch"],
    )

    gap_priority, gap_batch = summarize(gaps)
    rename_priority, rename_batch = summarize(renames)
    stale_priority, stale_batch = summarize(stale)
    summary = {
        "generated_at": now_iso(),
        "gap_total": len(gaps),
        "rename_total": len(renames),
        "stale_total": len(stale),
        "gap_priority_counts": gap_priority,
        "rename_priority_counts": rename_priority,
        "stale_priority_counts": stale_priority,
        "gap_batch_counts": gap_batch,
        "rename_batch_counts": rename_batch,
        "stale_batch_counts": stale_batch,
        "allowed_twelvedata_types": sorted(ALLOWED_TWELVE_TYPES),
    }
    (report_dir / "twelvedata_review_queues_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(report_dir / "twelvedata_review_queues.md", summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
