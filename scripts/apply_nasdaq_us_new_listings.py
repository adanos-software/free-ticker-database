from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.rebuild_dataset import should_exclude_stock_row
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import should_exclude_stock_row


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
REPORTS_DIR = DATA_DIR / "reports"
REPORT_JSON = REPORTS_DIR / "nasdaq_us_new_listings_apply.json"
REPORT_MD = REPORTS_DIR / "nasdaq_us_new_listings_apply.md"

NASDAQ_US_SOURCE_KEYS = {"nasdaq_listed", "nasdaq_other_listed"}
US_LISTING_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
DEFAULT_ASSET_TYPES = {"Stock", "ETF"}
US_SUPPLEMENT_COUNTRY = "United States"
US_SUPPLEMENT_COUNTRY_CODE = "US"
SUPPLEMENT_FIELDNAMES = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
    "source_key",
    "source_url",
    "reference_scope",
]
STOCK_LIKE_NAME_PATTERNS = (
    re.compile(r"\bcommon stock\b", re.IGNORECASE),
    re.compile(r"\bcommon shares?\b", re.IGNORECASE),
    re.compile(r"\bordinary shares?\b", re.IGNORECASE),
    re.compile(r"\bamerican depositary receipts?\b", re.IGNORECASE),
    re.compile(r"\bamerican depository receipts?\b", re.IGNORECASE),
    re.compile(r"\bdepositary receipts?\b", re.IGNORECASE),
    re.compile(r"\badr\b", re.IGNORECASE),
    re.compile(r"\bgdr\b", re.IGNORECASE),
)
TEMPORARY_ISSUANCE_PATTERNS = (
    re.compile(r"\bwhen issued\b", re.IGNORECASE),
    re.compile(r"\bwhen-issued\b", re.IGNORECASE),
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def normalize_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def active_reference_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (row.get("source_key", ""), row.get("ticker", ""), row.get("exchange", ""))


def is_stock_like_name(name: str) -> bool:
    return any(pattern.search(name) for pattern in STOCK_LIKE_NAME_PATTERNS)


def is_temporary_issuance_name(name: str) -> bool:
    return any(pattern.search(name) for pattern in TEMPORARY_ISSUANCE_PATTERNS)


def is_supported_reference_row(row: dict[str, str], asset_types: set[str]) -> bool:
    return (
        row.get("source_key") in NASDAQ_US_SOURCE_KEYS
        and row.get("exchange") in US_LISTING_EXCHANGES
        and row.get("asset_type") in asset_types
        and row.get("listing_status") == "active"
        and row.get("reference_scope") == "exchange_directory"
        and normalize_bool(row.get("official", ""))
        and bool(row.get("ticker", "").strip())
        and bool(row.get("name", "").strip())
    )


def dedupe_supplement_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (row.get("ticker", ""), row.get("exchange", ""))
        if key == ("", ""):
            continue
        by_key[key] = {field: row.get(field, "") for field in SUPPLEMENT_FIELDNAMES}
    return sorted(by_key.values(), key=lambda row: (row["exchange"], row["ticker"]))


def reason_for_skip(
    row: dict[str, str],
    *,
    previous_active_keys: set[tuple[str, str, str]],
    existing_listing_keys: set[tuple[str, str]],
    existing_tickers: set[str],
    existing_supplement_keys: set[tuple[str, str]],
    newly_seen_ticker_counts: Counter[str],
    asset_types: set[str],
) -> str:
    if not is_supported_reference_row(row, asset_types):
        return "unsupported_reference_row"
    if active_reference_key(row) in previous_active_keys:
        return "already_active_in_previous_reference"
    ticker = row["ticker"]
    exchange = row["exchange"]
    if (ticker, exchange) in existing_listing_keys:
        return "already_in_listings"
    if (ticker, exchange) in existing_supplement_keys:
        return "already_in_supplements"
    if ticker in existing_tickers:
        return "ticker_collision"
    if newly_seen_ticker_counts[ticker] > 1:
        return "new_feed_ticker_collision"
    if row["asset_type"] == "Stock" and is_temporary_issuance_name(row["name"]):
        return "temporary_when_issued_line"
    if row["asset_type"] == "Stock" and not is_stock_like_name(row["name"]):
        return "not_stock_like_name"
    if row["asset_type"] == "Stock" and should_exclude_stock_row(row):
        return "excluded_non_common_stock"
    return ""


def build_supplement_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "ticker": row["ticker"],
        "name": row["name"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "sector": row.get("sector", ""),
        "country": US_SUPPLEMENT_COUNTRY,
        "country_code": US_SUPPLEMENT_COUNTRY_CODE,
        "isin": row.get("isin", ""),
        "aliases": "",
        "source_key": row.get("source_key", ""),
        "source_url": row.get("source_url", ""),
        "reference_scope": row.get("reference_scope", ""),
    }


def apply_new_listings(
    *,
    previous_reference_csv: Path,
    current_reference_csv: Path = MASTERFILE_REFERENCE_CSV,
    listings_csv: Path = LISTINGS_CSV,
    supplement_csv: Path = MASTERFILE_SUPPLEMENT_CSV,
    report_json: Path = REPORT_JSON,
    report_md: Path = REPORT_MD,
    asset_types: set[str] | None = None,
) -> dict[str, Any]:
    asset_types = asset_types or set(DEFAULT_ASSET_TYPES)
    if not previous_reference_csv.exists():
        raise FileNotFoundError(f"Previous reference snapshot is required: {previous_reference_csv}")

    previous_rows = load_csv(previous_reference_csv)
    current_rows = load_csv(current_reference_csv)
    listings = load_csv(listings_csv)
    existing_supplements = load_csv(supplement_csv)

    previous_active_keys = {
        active_reference_key(row)
        for row in previous_rows
        if is_supported_reference_row(row, asset_types)
    }
    current_supported_rows = [
        row
        for row in current_rows
        if is_supported_reference_row(row, asset_types)
    ]
    new_supported_rows = [
        row
        for row in current_supported_rows
        if active_reference_key(row) not in previous_active_keys
    ]
    newly_seen_ticker_counts = Counter(row["ticker"] for row in new_supported_rows)
    existing_listing_keys = {(row.get("ticker", ""), row.get("exchange", "")) for row in listings}
    existing_tickers = {row.get("ticker", "") for row in listings if row.get("ticker", "")}
    existing_supplement_keys = {
        (row.get("ticker", ""), row.get("exchange", ""))
        for row in existing_supplements
    }

    accepted: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    for row in new_supported_rows:
        reason = reason_for_skip(
            row,
            previous_active_keys=previous_active_keys,
            existing_listing_keys=existing_listing_keys,
            existing_tickers=existing_tickers,
            existing_supplement_keys=existing_supplement_keys,
            newly_seen_ticker_counts=newly_seen_ticker_counts,
            asset_types=asset_types,
        )
        if reason:
            skipped.append(
                {
                    "ticker": row.get("ticker", ""),
                    "exchange": row.get("exchange", ""),
                    "name": row.get("name", ""),
                    "asset_type": row.get("asset_type", ""),
                    "source_key": row.get("source_key", ""),
                    "skip_reason": reason,
                }
            )
            continue
        supplement = build_supplement_row(row)
        accepted.append(supplement)
        existing_supplement_keys.add((supplement["ticker"], supplement["exchange"]))
        existing_tickers.add(supplement["ticker"])

    supplement_rows = dedupe_supplement_rows([*existing_supplements, *accepted])
    write_csv(supplement_csv, SUPPLEMENT_FIELDNAMES, supplement_rows)

    summary = {
        "generated_at": utc_now_iso(),
        "previous_reference_csv": display_path(previous_reference_csv),
        "current_reference_csv": display_path(current_reference_csv),
        "supplement_csv": display_path(supplement_csv),
        "supported_asset_types": sorted(asset_types),
        "new_supported_rows": len(new_supported_rows),
        "accepted_rows": len(accepted),
        "skipped_rows": len(skipped),
        "accepted_by_exchange": dict(sorted(Counter(row["exchange"] for row in accepted).items())),
        "skipped_by_reason": dict(sorted(Counter(row["skip_reason"] for row in skipped).items())),
    }
    report = {"summary": summary, "accepted": accepted, "skipped": skipped}
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown(report_md, report)
    set_github_output("new_listings_applied", "true" if accepted else "false")
    set_github_output("accepted_rows", str(len(accepted)))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return report


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    summary = report["summary"]
    lines = [
        "# Nasdaq US New Listings Apply",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- New supported rows: `{summary['new_supported_rows']}`",
        f"- Accepted rows: `{summary['accepted_rows']}`",
        f"- Skipped rows: `{summary['skipped_rows']}`",
        f"- Supported asset types: `{', '.join(summary['supported_asset_types'])}`",
        "",
        "## Accepted",
        "",
    ]
    if report["accepted"]:
        lines.extend(["| Ticker | Exchange | Name | Asset type | Source |", "|---|---|---|---|---|"])
        for row in report["accepted"]:
            lines.append(
                f"| {row['ticker']} | {row['exchange']} | {row['name']} | "
                f"{row['asset_type']} | {row['source_key']} |"
            )
    else:
        lines.append("No new listings were accepted.")
    lines.extend(["", "## Skipped", ""])
    skipped_by_reason = summary["skipped_by_reason"]
    if skipped_by_reason:
        lines.extend(["| Reason | Rows |", "|---|---:|"])
        for reason, count in skipped_by_reason.items():
            lines.append(f"| {reason} | {count} |")
    else:
        lines.append("No supported new rows were skipped.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def set_github_output(key: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as handle:
        handle.write(f"{key}={value}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply newly active US stock and ETF listings discovered by Nasdaq Trader source refreshes."
    )
    parser.add_argument("--previous-reference", type=Path, required=True)
    parser.add_argument("--current-reference", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--supplement-csv", type=Path, default=MASTERFILE_SUPPLEMENT_CSV)
    parser.add_argument("--report-json", type=Path, default=REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=REPORT_MD)
    parser.add_argument(
        "--asset-type",
        action="append",
        dest="asset_types",
        help="Supported asset type(s). Defaults to Stock. Repeat or comma-separate.",
    )
    return parser.parse_args(argv)


def normalize_asset_types(values: list[str] | None) -> set[str]:
    if not values:
        return set(DEFAULT_ASSET_TYPES)
    normalized: set[str] = set()
    for value in values:
        normalized.update(item.strip() for item in value.split(",") if item.strip())
    return normalized


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    apply_new_listings(
        previous_reference_csv=args.previous_reference,
        current_reference_csv=args.current_reference,
        listings_csv=args.listings_csv,
        supplement_csv=args.supplement_csv,
        report_json=args.report_json,
        report_md=args.report_md,
        asset_types=normalize_asset_types(args.asset_types),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
