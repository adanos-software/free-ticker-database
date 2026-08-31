from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.lib.non_equity_guard import is_blocked_non_common_stock
    from scripts.rebuild_dataset import (
        COUNTRY_TO_ISO,
        alias_matches_company,
        normalized_compact,
        should_exclude_stock_row,
    )
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.non_equity_guard import is_blocked_non_common_stock
    from rebuild_dataset import COUNTRY_TO_ISO, alias_matches_company, normalized_compact, should_exclude_stock_row


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
COVERAGE_EXPANSION_CSV = DATA_DIR / "coverage_expansion_listings.csv"
LISTING_TRANSITIONS_CSV = DATA_DIR / "review_overrides" / "listing_transitions.csv"
DROP_ENTRIES_CSV = DATA_DIR / "review_overrides" / "drop_entries.csv"
REPORTS_DIR = DATA_DIR / "reports"
REPORT_JSON = REPORTS_DIR / "nasdaq_us_new_listings_apply.json"
REPORT_MD = REPORTS_DIR / "nasdaq_us_new_listings_apply.md"

NASDAQ_US_SOURCE_KEYS = {"nasdaq_listed", "nasdaq_other_listed"}
SEC_EXCHANGE_SOURCE_KEY = "sec_company_tickers_exchange"
US_LISTING_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
DEFAULT_ASSET_TYPES = {"Stock", "ETF"}
US_SUPPLEMENT_COUNTRY = "United States"
US_SUPPLEMENT_COUNTRY_CODE = "US"
ISO_TO_COUNTRY = {code: country for country, code in COUNTRY_TO_ISO.items()}
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
COVERAGE_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
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
POTENTIALLY_FOREIGN_SHARE_PATTERNS = (
    re.compile(r"\bordinary shares?\b", re.IGNORECASE),
    re.compile(r"\bcommon shares?\b", re.IGNORECASE),
    re.compile(r"\bdepositary shares?\b", re.IGNORECASE),
    re.compile(r"\bdepository shares?\b", re.IGNORECASE),
    re.compile(r"\bdepositary receipts?\b", re.IGNORECASE),
    re.compile(r"\bdepository receipts?\b", re.IGNORECASE),
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


def append_coverage_rows(path: Path, rows: Iterable[dict[str, str]]) -> int:
    existing_keys = {
        (row.get("ticker", ""), row.get("exchange", ""))
        for row in load_csv(path)
    }
    additions: list[dict[str, str]] = []
    for row in rows:
        key = (row.get("ticker", ""), row.get("exchange", ""))
        if key in existing_keys:
            continue
        additions.append({field: row.get(field, "") for field in COVERAGE_FIELDNAMES})
        existing_keys.add(key)
    if not additions:
        return 0

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COVERAGE_FIELDNAMES, lineterminator="\n")
    writer.writerows(sorted(additions, key=lambda row: row["listing_key"]))
    suffix = buffer.getvalue().encode("utf-8")
    if path.exists():
        original = path.read_bytes()
        if original and not original.endswith((b"\n", b"\r")):
            original += b"\n"
        path.write_bytes(original + suffix)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        header = (",".join(COVERAGE_FIELDNAMES) + "\n").encode("utf-8")
        path.write_bytes(header + suffix)
    return len(additions)


def normalize_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def active_reference_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (row.get("source_key", ""), row.get("ticker", ""), row.get("exchange", ""))


def is_stock_like_name(name: str) -> bool:
    return any(pattern.search(name) for pattern in STOCK_LIKE_NAME_PATTERNS)


def is_temporary_issuance_name(name: str) -> bool:
    return any(pattern.search(name) for pattern in TEMPORARY_ISSUANCE_PATTERNS)


def is_potentially_foreign_share_name(name: str) -> bool:
    return any(pattern.search(name) for pattern in POTENTIALLY_FOREIGN_SHARE_PATTERNS)


def listing_identity_peers(
    row: dict[str, str],
    listings: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    name_key = normalized_compact(row.get("name", ""))
    row_isin = row.get("isin", "").strip().upper()
    peers: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in listings:
        if candidate.get("asset_type") != row.get("asset_type"):
            continue
        matching_isin = bool(
            row_isin
            and row_isin == candidate.get("isin", "").strip().upper()
        )
        exact_name_identity = bool(
            name_key and name_key == normalized_compact(candidate.get("name", ""))
        )
        if not (matching_isin or exact_name_identity):
            continue
        key = (candidate.get("ticker", ""), candidate.get("exchange", ""))
        if key not in seen:
            peers.append(candidate)
            seen.add(key)
    return peers


def is_active_official_sec_row(row: dict[str, str], ticker: str, asset_type: str) -> bool:
    return bool(
        row.get("source_key") == SEC_EXCHANGE_SOURCE_KEY
        and row.get("ticker") == ticker
        and row.get("asset_type") == asset_type
        and row.get("listing_status") == "active"
        and normalize_bool(row.get("official", ""))
    )


def sec_venue_change_identity_peers(
    row: dict[str, str],
    listings: Iterable[dict[str, str]],
    previous_reference_rows: Iterable[dict[str, str]],
    current_reference_rows: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    ticker = row["ticker"]
    asset_type = row["asset_type"]
    current_sec_rows = [
        candidate
        for candidate in current_reference_rows
        if is_active_official_sec_row(candidate, ticker, asset_type)
        and candidate.get("exchange") == row.get("exchange")
        and (
            alias_matches_company(candidate.get("name", ""), row.get("name", ""))
            or alias_matches_company(row.get("name", ""), candidate.get("name", ""))
        )
    ]
    if len(current_sec_rows) != 1:
        return []
    current_sec = current_sec_rows[0]
    peers: list[dict[str, str]] = []
    for listing in listings:
        if listing.get("ticker") != ticker or listing.get("asset_type") != asset_type:
            continue
        previous_sec_rows = [
            candidate
            for candidate in previous_reference_rows
            if is_active_official_sec_row(candidate, ticker, asset_type)
            and candidate.get("exchange") == listing.get("exchange")
            and (
                alias_matches_company(candidate.get("name", ""), current_sec.get("name", ""))
                or alias_matches_company(current_sec.get("name", ""), candidate.get("name", ""))
            )
        ]
        if len(previous_sec_rows) == 1:
            peers.append(listing)
    return peers


def reviewed_symbol_change_identity_peers(
    row: dict[str, str],
    listings: Iterable[dict[str, str]],
    reviewed_transitions: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    new_listing_key = f"{row['exchange']}::{row['ticker']}"
    transitions = [
        transition
        for transition in reviewed_transitions
        if transition.get("new_listing_key") == new_listing_key
        and transition.get("event_type") == "symbol_changed"
        and transition.get("identity_type") == "same_isin"
        and transition.get("identity_value", "").strip()
    ]
    if len(transitions) != 1:
        return []

    transition = transitions[0]
    identity_value = transition["identity_value"].strip().upper()
    peers = [
        listing
        for listing in listings
        if listing.get("listing_key") == transition.get("old_listing_key")
        and listing.get("asset_type") == row.get("asset_type")
        and listing.get("isin", "").strip().upper() == identity_value
    ]
    return peers if len(peers) == 1 else []


def unique_peer_value(peers: Iterable[dict[str, str]], *fields: str) -> str:
    values = {
        next((str(peer.get(field, "")).strip() for field in fields if peer.get(field, "").strip()), "")
        for peer in peers
    }
    values.discard("")
    return next(iter(values)) if len(values) == 1 else ""


def merged_peer_aliases(peers: Iterable[dict[str, str]]) -> str:
    aliases: list[str] = []
    seen: set[str] = set()
    for peer in peers:
        for alias in peer.get("aliases", "").split("|"):
            alias = alias.strip()
            normalized = alias.casefold()
            if alias and normalized not in seen:
                aliases.append(alias)
                seen.add(normalized)
    return "|".join(aliases)


def resolved_peer_country(identity_peers: Iterable[dict[str, str]]) -> tuple[str, str, bool]:
    countries = {peer.get("country", "").strip() for peer in identity_peers if peer.get("country", "").strip()}
    country_codes = {
        peer.get("country_code", "").strip().upper()
        for peer in identity_peers
        if peer.get("country_code", "").strip()
    }
    has_evidence = bool(countries or country_codes)
    if len(countries) > 1 or len(country_codes) > 1:
        return "", "", has_evidence
    country = next(iter(countries), "")
    country_code = next(iter(country_codes), "")
    if country and country_code:
        expected_code = COUNTRY_TO_ISO.get(country, "")
        if expected_code and expected_code != country_code:
            return "", "", True
    elif country:
        country_code = COUNTRY_TO_ISO.get(country, "")
    elif country_code:
        country = ISO_TO_COUNTRY.get(country_code, "")
    return country, country_code, has_evidence


def resolved_listing_metadata(
    row: dict[str, str],
    identity_peers: list[dict[str, str]],
    security_identity_peers: list[dict[str, str]] | None = None,
) -> tuple[str, str, str, str]:
    security_identity_peers = security_identity_peers or []
    sector_fields = ("etf_category", "sector") if row["asset_type"] == "ETF" else ("stock_sector", "sector")
    sector = row.get("sector", "") or unique_peer_value(identity_peers, *sector_fields)
    isin = row.get("isin", "") or unique_peer_value(security_identity_peers, "isin")
    country, country_code, has_country_evidence = resolved_peer_country(identity_peers)
    if not has_country_evidence and not is_potentially_foreign_share_name(row["name"]):
        country = US_SUPPLEMENT_COUNTRY
        country_code = US_SUPPLEMENT_COUNTRY_CODE
    return sector, country, country_code, isin


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
    existing_supplement_keys: set[tuple[str, str]],
    existing_coverage_keys: set[tuple[str, str]],
    newly_seen_ticker_counts: Counter[str],
    reviewed_drop_keys: set[tuple[str, str]],
    asset_types: set[str],
) -> str:
    if not is_supported_reference_row(row, asset_types):
        return "unsupported_reference_row"
    if active_reference_key(row) in previous_active_keys:
        return "already_active_in_previous_reference"
    ticker = row["ticker"]
    exchange = row["exchange"]
    if (ticker, exchange) in reviewed_drop_keys:
        return "reviewed_drop_entry"
    if (ticker, exchange) in existing_listing_keys:
        return "already_in_listings"
    if (ticker, exchange) in existing_supplement_keys:
        return "already_in_supplements"
    if (ticker, exchange) in existing_coverage_keys:
        return "already_in_coverage"
    if newly_seen_ticker_counts[ticker] > 1:
        return "new_feed_ticker_collision"
    if row["asset_type"] == "Stock" and is_temporary_issuance_name(row["name"]):
        return "temporary_when_issued_line"
    if row["asset_type"] == "Stock" and is_blocked_non_common_stock(row):
        return "excluded_non_common_stock"
    if row["asset_type"] == "Stock" and not is_stock_like_name(row["name"]):
        return "not_stock_like_name"
    if row["asset_type"] == "Stock" and should_exclude_stock_row(row):
        return "excluded_non_common_stock"
    return ""


def build_supplement_row(
    row: dict[str, str],
    identity_peers: list[dict[str, str]],
    security_identity_peers: list[dict[str, str]],
) -> dict[str, str]:
    sector, country, country_code, isin = resolved_listing_metadata(
        row,
        identity_peers,
        security_identity_peers,
    )
    return {
        "ticker": row["ticker"],
        "name": row["name"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "sector": sector,
        "country": country,
        "country_code": country_code,
        "isin": isin,
        "aliases": merged_peer_aliases(security_identity_peers),
        "source_key": row.get("source_key", ""),
        "source_url": row.get("source_url", ""),
        "reference_scope": row.get("reference_scope", ""),
    }


def build_coverage_row(
    row: dict[str, str],
    identity_peers: list[dict[str, str]],
    security_identity_peers: list[dict[str, str]],
) -> dict[str, str]:
    sector, country, country_code, isin = resolved_listing_metadata(
        row,
        identity_peers,
        security_identity_peers,
    )
    return {
        "listing_key": f"{row['exchange']}::{row['ticker']}",
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "name": row["name"],
        "asset_type": row["asset_type"],
        "stock_sector": sector if row["asset_type"] == "Stock" else "",
        "etf_category": sector if row["asset_type"] == "ETF" else "",
        "country": country,
        "country_code": country_code,
        "isin": isin,
        "aliases": merged_peer_aliases(security_identity_peers),
    }


def apply_new_listings(
    *,
    previous_reference_csv: Path,
    current_reference_csv: Path = MASTERFILE_REFERENCE_CSV,
    listings_csv: Path = LISTINGS_CSV,
    supplement_csv: Path = MASTERFILE_SUPPLEMENT_CSV,
    coverage_expansion_csv: Path = COVERAGE_EXPANSION_CSV,
    listing_transitions_csv: Path = LISTING_TRANSITIONS_CSV,
    drop_entries_csv: Path = DROP_ENTRIES_CSV,
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
    reviewed_transitions = load_csv(listing_transitions_csv)
    reviewed_drop_keys = {
        (row.get("ticker", ""), row.get("exchange", ""))
        for row in load_csv(drop_entries_csv)
    }

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
    existing_coverage_keys = {
        (row.get("ticker", ""), row.get("exchange", ""))
        for row in load_csv(coverage_expansion_csv)
    }

    accepted: list[dict[str, str]] = []
    accepted_supplements: list[dict[str, str]] = []
    accepted_coverage: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    for row in new_supported_rows:
        reason = reason_for_skip(
            row,
            previous_active_keys=previous_active_keys,
            existing_listing_keys=existing_listing_keys,
            existing_supplement_keys=existing_supplement_keys,
            existing_coverage_keys=existing_coverage_keys,
            newly_seen_ticker_counts=newly_seen_ticker_counts,
            reviewed_drop_keys=reviewed_drop_keys,
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
        identity_peers = listing_identity_peers(row, listings)
        security_identity_peers = [
            peer
            for peer in identity_peers
            if row.get("isin", "").strip()
            and row.get("isin", "").strip().upper() == peer.get("isin", "").strip().upper()
        ]
        security_identity_peers.extend(
            peer
            for peer in sec_venue_change_identity_peers(row, listings, previous_rows, current_rows)
            if peer not in security_identity_peers
        )
        security_identity_peers.extend(
            peer
            for peer in reviewed_symbol_change_identity_peers(row, listings, reviewed_transitions)
            if peer not in security_identity_peers
        )
        identity_peers.extend(peer for peer in security_identity_peers if peer not in identity_peers)
        ticker_occupant_matches = any(peer.get("ticker") == row["ticker"] for peer in security_identity_peers)
        if row["ticker"] in existing_tickers and not ticker_occupant_matches:
            coverage = build_coverage_row(row, identity_peers, security_identity_peers)
            accepted_coverage.append(coverage)
            accepted.append(
                {
                    **coverage,
                    "source_key": row.get("source_key", ""),
                    "source_url": row.get("source_url", ""),
                    "apply_target": "coverage_expansion",
                }
            )
        else:
            supplement = build_supplement_row(row, identity_peers, security_identity_peers)
            accepted_supplements.append(supplement)
            accepted.append({**supplement, "apply_target": "supplement"})
            existing_supplement_keys.add((supplement["ticker"], supplement["exchange"]))

    supplement_rows = dedupe_supplement_rows([*existing_supplements, *accepted_supplements])
    write_csv(supplement_csv, SUPPLEMENT_FIELDNAMES, supplement_rows)
    written_coverage_rows = append_coverage_rows(coverage_expansion_csv, accepted_coverage)
    if written_coverage_rows != len(accepted_coverage):
        raise RuntimeError(
            f"Coverage append wrote {written_coverage_rows} of {len(accepted_coverage)} accepted rows"
        )

    summary = {
        "generated_at": utc_now_iso(),
        "previous_reference_csv": display_path(previous_reference_csv),
        "current_reference_csv": display_path(current_reference_csv),
        "supplement_csv": display_path(supplement_csv),
        "coverage_expansion_csv": display_path(coverage_expansion_csv),
        "listing_transitions_csv": display_path(listing_transitions_csv),
        "drop_entries_csv": display_path(drop_entries_csv),
        "supported_asset_types": sorted(asset_types),
        "new_supported_rows": len(new_supported_rows),
        "accepted_rows": len(accepted),
        "skipped_rows": len(skipped),
        "accepted_by_exchange": dict(sorted(Counter(row["exchange"] for row in accepted).items())),
        "accepted_by_target": dict(sorted(Counter(row["apply_target"] for row in accepted).items())),
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
        lines.extend(["| Ticker | Exchange | Name | Asset type | Target | Source |", "|---|---|---|---|---|---|"])
        for row in report["accepted"]:
            lines.append(
                f"| {row['ticker']} | {row['exchange']} | {row['name']} | "
                f"{row['asset_type']} | {row['apply_target']} | {row.get('source_key', '')} |"
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
    parser.add_argument("--coverage-expansion-csv", type=Path, default=COVERAGE_EXPANSION_CSV)
    parser.add_argument("--listing-transitions-csv", type=Path, default=LISTING_TRANSITIONS_CSV)
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
        coverage_expansion_csv=args.coverage_expansion_csv,
        listing_transitions_csv=args.listing_transitions_csv,
        report_json=args.report_json,
        report_md=args.report_md,
        asset_types=normalize_asset_types(args.asset_types),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
