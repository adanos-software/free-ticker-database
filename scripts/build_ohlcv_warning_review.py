from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_OHLCV_JSON = REPORTS_DIR / "ohlcv_plausibility.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "ohlcv_warning_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "ohlcv_warning_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "ohlcv_warning_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "entry_quality_status",
    "ohlcv_source",
    "ohlcv_symbol",
    "plausibility_status",
    "plausibility_score",
    "bar_count",
    "first_bar_date",
    "last_bar_date",
    "max_price_jump",
    "zero_volume_streak",
    "stagnant_close_streak",
    "invalid_bar_count",
    "issue_count",
    "issue_types",
    "ohlcv_review_bucket",
    "official_review_priority",
    "official_listing_review_status",
    "official_corporate_action_review_status",
    "canonical_data_change_authorization",
    "verification_evidence_required",
    "official_source_url",
    "official_source_locator_status",
    "recommended_next_source",
    "source_gate",
    "review_context",
    "recommended_action",
]

VERIFIED_OFFICIAL_SOURCE_URLS = {
    "LSE::0A3T": "https://www.londonstockexchange.com/market-stock/0A3T/sea-ltd/overview",
    "LSE::0A2V": "https://www.londonstockexchange.com/market-stock/0A2V/sanofi-sa/overview",
    "LSE::0A56": "https://www.londonstockexchange.com/market-stock/0A56/sibanye-stillwater-ltd/overview",
    "LSE::0A6Q": "https://www.londonstockexchange.com/market-stock/0A6Q/aia-group-ltd/overview",
    "LSE::0A6U": "https://www.londonstockexchange.com/market-stock/0A6U/luckin-coffee-inc/overview",
    "LSE::0A6W": "https://www.londonstockexchange.com/market-stock/0A6W/abb-ltd/overview",
    "LSE::0A6X": "https://www.londonstockexchange.com/market-stock/0A6X/ambev-sa/overview",
    "LSE::0A7F": "https://www.londonstockexchange.com/market-stock/0A7F/equinor-asa/overview",
    "LSE::0A8Q": "https://www.londonstockexchange.com/market-stock/0A8Q/zw-data-action-technologies-inc/overview",
    "LSE::0HKY": "https://www.londonstockexchange.com/market-stock/0HKY/byd-co-ltd/overview",
    "LSE::0HL8": "https://www.londonstockexchange.com/market-stock/0HL8/banco-bradesco-sa/trade-recap",
    "LSE::0HLE": "https://www.londonstockexchange.com/market-stock/0HLE/banco-santander-sa/overview",
    "LSE::0HN3": "https://www.londonstockexchange.com/market-stock/0HN3/bhp-billiton-ltd/overview",
}

VERIFIED_OFFICIAL_INSTRUMENT_GROUP_URLS = {
    "LSE::0A2M": "https://www.londonstockexchange.com/market-stock/0LNG/koninklijke-philips-nv/overview",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def issue_types_for(row: dict[str, Any]) -> list[str]:
    issues = row.get("issues")
    if not isinstance(issues, list):
        return []
    return sorted(
        {
            str(issue.get("issue_type", ""))
            for issue in issues
            if isinstance(issue, dict) and issue.get("issue_type")
        }
    )


def official_review_bucket_for(row: dict[str, Any]) -> str:
    issue_types = set(issue_types_for(row))
    if "large_price_jump" in issue_types:
        return "official_corporate_action_and_listing_status_review"
    if "invalid_ohlcv_bar" in issue_types:
        return "official_listing_status_and_market_data_cross_check"
    return "official_market_data_quality_review"


def official_review_priority_for(bucket: str, row: dict[str, Any]) -> str:
    try:
        invalid_bar_count = int(row.get("invalid_bar_count", 0))
    except (TypeError, ValueError):
        invalid_bar_count = 0
    if bucket == "official_corporate_action_and_listing_status_review":
        return "P1"
    if invalid_bar_count >= 10:
        return "P1"
    if invalid_bar_count > 0:
        return "P2"
    return "P3"


def recommended_next_source_for(exchange: str) -> str:
    if exchange == "LSE":
        return "Official LSE instrument page, LSE notices, and issuer corporate-action announcements."
    if exchange == "SZSE":
        return "Official SZSE security page, SZSE announcements, and issuer corporate-action announcements."
    return f"Official {exchange or 'exchange'} listing-status page, exchange notices, and issuer corporate-action announcements."


def official_source_url_for(row: dict[str, Any]) -> str:
    listing_key = str(row.get("listing_key", ""))
    return VERIFIED_OFFICIAL_SOURCE_URLS.get(listing_key, VERIFIED_OFFICIAL_INSTRUMENT_GROUP_URLS.get(listing_key, ""))


def official_source_locator_status_for(row: dict[str, Any]) -> str:
    listing_key = str(row.get("listing_key", ""))
    if listing_key in VERIFIED_OFFICIAL_SOURCE_URLS:
        return "verified_official_exchange_page_seeded"
    if listing_key in VERIFIED_OFFICIAL_INSTRUMENT_GROUP_URLS:
        return "verified_official_exchange_instrument_group_page_seeded"
    return "pending_official_exchange_page_or_notice_lookup"


def review_context_for(row: dict[str, Any], bucket: str, priority: str) -> str:
    return (
        f"listing_key={row.get('listing_key', '') or 'none'};"
        f"ohlcv_symbol={row.get('ohlcv_symbol', '') or 'none'};"
        f"review_bucket={bucket};"
        f"priority={priority};"
        f"plausibility_status={row.get('plausibility_status', '') or 'none'};"
        f"issue_types={'|'.join(issue_types_for(row)) or 'none'};"
        f"official_source_locator_status={official_source_locator_status_for(row)}"
    )


def build_review_rows(ohlcv_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = ohlcv_payload.get("flagged_items", [])
    if not isinstance(rows, list):
        rows = []
    review_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("review_bucket") != "checked_ohlcv_anomaly_requires_listing_review":
            continue
        bucket = official_review_bucket_for(row)
        priority = official_review_priority_for(bucket, row)
        issue_types = issue_types_for(row)
        review_rows.append(
            {
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "exchange": row.get("exchange", ""),
                "asset_type": row.get("asset_type", ""),
                "name": row.get("name", ""),
                "isin": row.get("isin", ""),
                "entry_quality_status": row.get("entry_quality_status", ""),
                "ohlcv_source": row.get("ohlcv_source", ""),
                "ohlcv_symbol": row.get("ohlcv_symbol", ""),
                "plausibility_status": row.get("plausibility_status", ""),
                "plausibility_score": row.get("plausibility_score", 0),
                "bar_count": row.get("bar_count", 0),
                "first_bar_date": row.get("first_bar_date", ""),
                "last_bar_date": row.get("last_bar_date", ""),
                "max_price_jump": row.get("max_price_jump", 0),
                "zero_volume_streak": row.get("zero_volume_streak", 0),
                "stagnant_close_streak": row.get("stagnant_close_streak", 0),
                "invalid_bar_count": row.get("invalid_bar_count", 0),
                "issue_count": len(row.get("issues", [])) if isinstance(row.get("issues"), list) else 0,
                "issue_types": "|".join(issue_types),
                "ohlcv_review_bucket": bucket,
                "official_review_priority": priority,
                "official_listing_review_status": "pending_official_listing_status_review",
                "official_corporate_action_review_status": "pending_official_corporate_action_review",
                "canonical_data_change_authorization": "blocked_until_official_listing_keyed_review",
                "verification_evidence_required": (
                    "official_listing_status_corporate_action_and_independent_market_data_review"
                ),
                "official_source_url": official_source_url_for(row),
                "official_source_locator_status": official_source_locator_status_for(row),
                "recommended_next_source": recommended_next_source_for(str(row.get("exchange", ""))),
                "source_gate": (
                    "OHLCV anomaly is a review signal only; do not change identifiers, names, sectors, "
                    "categories, listings, or symbols without official listing-keyed evidence."
                ),
                "review_context": review_context_for(row, bucket, priority),
                "recommended_action": "perform_official_listing_keyed_review_before_any_canonical_change",
            }
        )
    return sorted(
        review_rows,
        key=lambda row: (
            {"P1": 1, "P2": 2, "P3": 3}.get(str(row["official_review_priority"]), 9),
            str(row["exchange"]),
            str(row["listing_key"]),
        ),
    )


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    exchange_counts = Counter(str(row["exchange"]) for row in rows)
    bucket_counts = Counter(str(row["ohlcv_review_bucket"]) for row in rows)
    priority_counts = Counter(str(row["official_review_priority"]) for row in rows)
    authorization_counts = Counter(str(row["canonical_data_change_authorization"]) for row in rows)
    listing_status_counts = Counter(str(row["official_listing_review_status"]) for row in rows)
    corporate_action_counts = Counter(str(row["official_corporate_action_review_status"]) for row in rows)
    source_locator_counts = Counter(str(row["official_source_locator_status"]) for row in rows)
    issue_counts = Counter(
        issue_type
        for row in rows
        for issue_type in str(row.get("issue_types", "")).split("|")
        if issue_type
    )
    batches = Counter(
        (
            str(row["exchange"]),
            str(row["ohlcv_review_bucket"]),
            str(row["official_review_priority"]),
        )
        for row in rows
    )
    return {
        "review_rows": len(rows),
        "exchange_counts": dict(sorted(exchange_counts.items())),
        "ohlcv_review_bucket_counts": dict(sorted(bucket_counts.items())),
        "official_review_priority_counts": dict(sorted(priority_counts.items())),
        "canonical_data_change_authorization_counts": dict(sorted(authorization_counts.items())),
        "official_listing_review_status_counts": dict(sorted(listing_status_counts.items())),
        "official_corporate_action_review_status_counts": dict(sorted(corporate_action_counts.items())),
        "official_source_locator_status_counts": dict(sorted(source_locator_counts.items())),
        "issue_type_counts": dict(issue_counts.most_common()),
        "top_official_review_batches": [
            {
                "exchange": exchange,
                "ohlcv_review_bucket": bucket,
                "official_review_priority": priority,
                "rows": count,
                "recommended_next_source": recommended_next_source_for(exchange),
            }
            for (exchange, bucket, priority), count in sorted(
                batches.items(),
                key=lambda item: (
                    {"P1": 1, "P2": 2, "P3": 3}.get(item[0][2], 9),
                    -item[1],
                    item[0],
                ),
            )
        ],
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# OHLCV Warning Review",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "This queue turns checked OHLCV anomalies into official listing-keyed review work. It does not authorize canonical data changes.",
        "",
        "## Summary",
        "",
        "| Metric | Rows |",
        "|---|---:|",
        f"| Review rows | {summary.get('review_rows', 0):,} |",
        "",
        "## Review Buckets",
        "",
        "| Bucket | Rows |",
        "|---|---:|",
    ]
    for bucket, count in summary.get("ohlcv_review_bucket_counts", {}).items():
        lines.append(f"| {bucket} | {count:,} |")
    lines.extend(["", "## Official Review Batches", "", "| Exchange | Bucket | Priority | Rows | Next Source |", "|---|---|---|---:|---|"])
    for batch in summary.get("top_official_review_batches", []):
        lines.append(
            f"| {batch['exchange']} | {batch['ohlcv_review_bucket']} | "
            f"{batch['official_review_priority']} | {batch['rows']:,} | {batch['recommended_next_source']} |"
        )
    lines.extend(["", "## Authorization", "", "| Authorization | Rows |", "|---|---:|"])
    for authorization, count in summary.get("canonical_data_change_authorization_counts", {}).items():
        lines.append(f"| {authorization} | {count:,} |")
    lines.extend(["", "## Source Locator Status", "", "| Status | Rows |", "|---|---:|"])
    for status, count in summary.get("official_source_locator_status_counts", {}).items():
        lines.append(f"| {status} | {count:,} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_payload(rows: list[dict[str, Any]], *, source_path: Path, csv_out: Path) -> dict[str, Any]:
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "rows": len(rows),
            "csv_out": display_path(csv_out),
            "source_files": {"ohlcv_plausibility_json": display_path(source_path)},
            "policy": (
                "OHLCV warnings are review signals only. Canonical identifiers, names, sectors, categories, "
                "listings, and symbols remain blocked until official listing-keyed evidence is reviewed."
            ),
        },
        "summary": summarize(rows),
        "review_items": rows,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build official review queue for checked OHLCV warning rows.")
    parser.add_argument("--ohlcv-json", type=Path, default=DEFAULT_OHLCV_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_review_rows(load_json(args.ohlcv_json))
    payload = build_payload(rows, source_path=args.ohlcv_json, csv_out=args.csv_out)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"rows": len(rows), **payload["summary"]}, indent=2))


if __name__ == "__main__":
    main()
