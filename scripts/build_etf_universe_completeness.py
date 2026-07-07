from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_MASTERFILE_SUMMARY_JSON = DATA_DIR / "masterfiles" / "summary.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "etf_universe_completeness.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "etf_universe_completeness.json"
DEFAULT_MD_OUT = REPORTS_DIR / "etf_universe_completeness.md"

MISSING_FIELDNAMES = [
    "source_key",
    "provider",
    "exchange",
    "ticker",
    "name",
    "isin",
    "category",
    "reference_scope",
    "match_status",
    "candidate_action",
    "source_gate",
]
ETF_UNIVERSE_EXCLUDED_REFERENCE_SCOPES = {"corporate_action_daily_list"}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def etf_category(row: dict[str, str]) -> str:
    return row.get("etf_category", "") or row.get("sector", "")


def official_active_etf_master_rows(masterfiles: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, str]] = []
    for row in masterfiles:
        if row.get("official") != "true":
            continue
        if row.get("listing_status") != "active":
            continue
        if row.get("asset_type") != "ETF":
            continue
        if row.get("reference_scope") in ETF_UNIVERSE_EXCLUDED_REFERENCE_SCOPES:
            continue
        key = (row.get("source_key", ""), row.get("exchange", ""), row.get("ticker", ""))
        if not all(key) or key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def match_status_for_master_row(
    row: dict[str, str],
    *,
    dataset_etf_by_exchange_ticker: set[tuple[str, str]],
    dataset_any_by_exchange_ticker: set[tuple[str, str]],
    dataset_etf_exchanges_by_ticker: dict[str, set[str]],
) -> str:
    exchange = row.get("exchange", "")
    ticker = row.get("ticker", "")
    exchange_ticker = (exchange, ticker)
    if exchange_ticker in dataset_etf_by_exchange_ticker:
        return "matched_etf_listing"
    if exchange_ticker in dataset_any_by_exchange_ticker:
        return "local_listing_asset_type_mismatch"
    if dataset_etf_exchanges_by_ticker.get(ticker, set()) - {exchange}:
        return "collision_hidden_by_global_ticker"
    return "missing_from_db"


def source_gate_for(status: str) -> str:
    if status == "matched_etf_listing":
        return "official_etf_directory_match_no_addition_needed"
    if status == "local_listing_asset_type_mismatch":
        return "review_local_asset_type_against_official_etf_directory_then_require_identity_isin_checksum_and_no_collision_before_any_change"
    if status == "collision_hidden_by_global_ticker":
        return "add_only_after_listing_key_identity_isin_checksum_and_no_collision_review"
    return "add_only_after_official_identity_isin_checksum_and_no_collision_review"


def candidate_action_for(status: str) -> str:
    if status == "matched_etf_listing":
        return "none"
    if status == "local_listing_asset_type_mismatch":
        return "review_asset_type"
    if status == "collision_hidden_by_global_ticker":
        return "review_collision_safe_listing_add"
    return "review_official_listing_add"


def build_report(
    listings: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
    *,
    masterfile_summary: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    dataset_etf_by_exchange_ticker = {
        (row.get("exchange", ""), row.get("ticker", ""))
        for row in listings
        if row.get("asset_type") == "ETF" and row.get("exchange") and row.get("ticker")
    }
    dataset_any_by_exchange_ticker = {
        (row.get("exchange", ""), row.get("ticker", ""))
        for row in listings
        if row.get("exchange") and row.get("ticker")
    }
    dataset_etf_exchanges_by_ticker: dict[str, set[str]] = defaultdict(set)
    for row in listings:
        if row.get("asset_type") == "ETF" and row.get("ticker") and row.get("exchange"):
            dataset_etf_exchanges_by_ticker[row["ticker"]].add(row["exchange"])

    official_etfs = official_active_etf_master_rows(masterfiles)
    source_details = (masterfile_summary or {}).get("source_details", {})

    missing_rows: list[dict[str, str]] = []
    by_source: dict[str, Counter[str]] = defaultdict(Counter)
    by_exchange: dict[str, Counter[str]] = defaultdict(Counter)
    source_metadata: dict[str, dict[str, str]] = {}

    for row in official_etfs:
        status = match_status_for_master_row(
            row,
            dataset_etf_by_exchange_ticker=dataset_etf_by_exchange_ticker,
            dataset_any_by_exchange_ticker=dataset_any_by_exchange_ticker,
            dataset_etf_exchanges_by_ticker=dataset_etf_exchanges_by_ticker,
        )
        source_key = row.get("source_key", "")
        exchange = row.get("exchange", "")
        by_source[source_key]["official_etf_rows"] += 1
        by_source[source_key][status] += 1
        by_exchange[exchange]["official_etf_rows"] += 1
        by_exchange[exchange][status] += 1
        source_metadata[source_key] = {
            "source_key": source_key,
            "provider": row.get("provider", ""),
            "reference_scope": row.get("reference_scope", ""),
            "source_mode": str(source_details.get(source_key, {}).get("mode", "")),
            "generated_at": str(source_details.get(source_key, {}).get("generated_at", "")),
        }
        if status == "matched_etf_listing":
            continue
        missing_rows.append(
            {
                "source_key": source_key,
                "provider": row.get("provider", ""),
                "exchange": exchange,
                "ticker": row.get("ticker", ""),
                "name": row.get("name", ""),
                "isin": row.get("isin", ""),
                "category": etf_category(row),
                "reference_scope": row.get("reference_scope", ""),
                "match_status": status,
                "candidate_action": candidate_action_for(status),
                "source_gate": source_gate_for(status),
            }
        )

    def summary_rows(counters: dict[str, Counter[str]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, counts in sorted(counters.items()):
            denominator = counts["official_etf_rows"]
            matched = counts["matched_etf_listing"]
            missing_or_review = denominator - matched
            rows.append(
                {
                    "key": key,
                    "official_etf_rows": denominator,
                    "matched_etf_listings": matched,
                    "missing_or_review_rows": missing_or_review,
                    "missing_from_db": counts["missing_from_db"],
                    "collision_hidden_by_global_ticker": counts["collision_hidden_by_global_ticker"],
                    "local_listing_asset_type_mismatch": counts["local_listing_asset_type_mismatch"],
                    "etf_recall_pct": round(matched / denominator * 100, 2) if denominator else None,
                }
            )
        return rows

    source_rows = []
    for row in summary_rows(by_source):
        source_rows.append({**source_metadata.get(row["key"], {"source_key": row["key"]}), **row})
    exchange_rows = summary_rows(by_exchange)
    for row in exchange_rows:
        row["exchange"] = row.pop("key")

    global_counts = Counter()
    for counts in by_source.values():
        global_counts.update(counts)
    official_rows = global_counts["official_etf_rows"]
    matched_rows = global_counts["matched_etf_listing"]

    return {
        "_meta": {
            "generated_at": generated_at or utc_now_iso(),
            "source_files": {
                "listings_csv": display_path(DEFAULT_LISTINGS_CSV),
                "masterfile_reference_csv": display_path(DEFAULT_MASTERFILE_REFERENCE_CSV),
                "masterfile_summary_json": display_path(DEFAULT_MASTERFILE_SUMMARY_JSON),
            },
            "policy": {
                "official_only": "Only active official ETF masterfile rows are compared; corporate-action daily-list rows are excluded from the ETF universe denominator.",
                "no_auto_add": "Missing ETF rows are review candidates only; this report never adds securities.",
                "identity_gate": "ETF additions require official identity evidence, valid ISIN/checksum when present, listing-key review, and no-collision validation.",
            },
        },
        "summary": {
            "official_etf_rows": official_rows,
            "matched_etf_listings": matched_rows,
            "missing_or_review_rows": official_rows - matched_rows,
            "missing_from_db": global_counts["missing_from_db"],
            "collision_hidden_by_global_ticker": global_counts["collision_hidden_by_global_ticker"],
            "local_listing_asset_type_mismatch": global_counts["local_listing_asset_type_mismatch"],
            "etf_recall_pct": round(matched_rows / official_rows * 100, 2) if official_rows else None,
            "source_count": len(source_rows),
            "exchange_count": len(exchange_rows),
        },
        "by_source": sorted(source_rows, key=lambda row: (-row["missing_or_review_rows"], row["source_key"])),
        "by_exchange": sorted(exchange_rows, key=lambda row: (-row["missing_or_review_rows"], row["exchange"])),
        "missing_rows": sorted(missing_rows, key=lambda row: (row["exchange"], row["ticker"], row["source_key"])),
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MISSING_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in MISSING_FIELDNAMES})


def write_json(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# ETF Universe Completeness",
        "",
        f"Generated at: `{report['_meta']['generated_at']}`",
        "",
        "This report compares active official ETF masterfile rows against the listing-keyed DB universe. Missing rows are review candidates only.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in summary.items():
        lines.append(f"| {key} | {value} |")

    lines.extend(
        [
            "",
            "## By Exchange",
            "",
            "| Exchange | Official ETF Rows | Matched | Missing/Review | Missing From DB | Collision-Hidden | Asset-Type Review | Recall % |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in report["by_exchange"]:
        lines.append(
            f"| {row['exchange']} | {row['official_etf_rows']} | {row['matched_etf_listings']} | "
            f"{row['missing_or_review_rows']} | {row['missing_from_db']} | "
            f"{row['collision_hidden_by_global_ticker']} | {row['local_listing_asset_type_mismatch']} | "
            f"{row['etf_recall_pct'] if row['etf_recall_pct'] is not None else ''} |"
        )

    lines.extend(
        [
            "",
            "## By Source",
            "",
            "| Source | Provider | Scope | Mode | Official ETF Rows | Matched | Missing/Review | Recall % |",
            "|---|---|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in report["by_source"]:
        lines.append(
            f"| {row['source_key']} | {row.get('provider', '')} | {row.get('reference_scope', '')} | "
            f"{row.get('source_mode', '')} | {row['official_etf_rows']} | {row['matched_etf_listings']} | "
            f"{row['missing_or_review_rows']} | {row['etf_recall_pct'] if row['etf_recall_pct'] is not None else ''} |"
        )

    lines.extend(
        [
            "",
            "## Top Missing Or Review Rows",
            "",
            "| Exchange | Ticker | Source | Status | Candidate Action | Source Gate |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in report["missing_rows"][:100]:
        lines.append(
            f"| {row['exchange']} | {row['ticker']} | {row['source_key']} | {row['match_status']} | "
            f"{row['candidate_action']} | {row['source_gate']} |"
        )
    if not report["missing_rows"]:
        lines.append("|  |  |  |  |  |  |")

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Official ETF directory rows do not authorize automatic additions.",
            "- Additions require official identity evidence, valid ISIN/checksum when present, listing-key review, and no-collision validation.",
            "- Collision-hidden rows belong in `core_listings.csv`/`listings.csv` review paths, not the legacy `tickers.csv` global-unique contract.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(report), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare official ETF directories against the DB ETF universe.")
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--masterfile-summary-json", type=Path, default=DEFAULT_MASTERFILE_SUMMARY_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = build_report(
        load_csv(args.listings_csv),
        load_csv(args.masterfile_reference_csv),
        masterfile_summary=load_json(args.masterfile_summary_json),
    )
    write_csv(args.csv_out, report["missing_rows"])
    write_json(args.json_out, report)
    write_markdown(args.md_out, report)
    print(
        json.dumps(
            {
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "md_out": display_path(args.md_out),
                **report["summary"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
