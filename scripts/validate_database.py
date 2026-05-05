from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import is_common_single_word_alias, normalize_alias_text
from scripts.check_entry_quality_gate import allowed_warn_keys, check_entry_quality_gate
from scripts.rebuild_dataset import COUNTRY_TO_ISO, country_from_isin, is_valid_isin, normalize_sector

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_CORE_LISTINGS_CSV = DATA_DIR / "core_listings.csv"
DEFAULT_LISTING_INDEX_CSV = DATA_DIR / "listing_index.csv"
DEFAULT_IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
DEFAULT_IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
DEFAULT_IDENTIFIER_SUMMARY_JSON = DATA_DIR / "identifier_summary.json"
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_CROSS_LISTINGS_CSV = DATA_DIR / "cross_listings.csv"
DEFAULT_ADANOS_REFERENCE_CSV = DATA_DIR / "adanos" / "ticker_reference.csv"
DEFAULT_ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
DEFAULT_WARN_ALLOWLIST_CSV = REPORTS_DIR / "entry_quality_warn_allowlist.csv"
DEFAULT_ADANOS_ALIAS_AUDIT_CSV = REPORTS_DIR / "adanos_alias_audit.csv"
DEFAULT_REVIEW_REMOVE_ALIASES_CSV = DATA_DIR / "review_overrides" / "remove_aliases.csv"
DEFAULT_REVIEW_METADATA_UPDATES_CSV = DATA_DIR / "review_overrides" / "metadata_updates.csv"
DEFAULT_COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_JSON_OUT = REPORTS_DIR / "validation_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "validation_report.md"

ASSET_TYPES = {"Stock", "ETF"}
PRIMARY_SCOPE_REASONS = {"primary_listing", "primary_listing_missing_isin", "otc_listing"}

TICKERS_COLUMNS = {
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
}
LISTINGS_COLUMNS = {"listing_key", *TICKERS_COLUMNS}
CORE_LISTINGS_COLUMNS = {
    "listing_key",
    *TICKERS_COLUMNS,
    "instrument_group_key",
    "scope_reason",
}
LISTING_INDEX_COLUMNS = {
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "country",
    "country_code",
    "isin",
    "wkn",
    "figi",
    "cik",
    "lei",
}
INSTRUMENT_SCOPE_COLUMNS = {
    "listing_key",
    "ticker",
    "exchange",
    "instrument_scope",
    "scope_reason",
    "primary_listing_key",
}
CROSS_LISTING_COLUMNS = {"isin", "listing_key", "ticker", "exchange", "is_primary"}
ADANOS_COLUMNS = {
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
}
ENTRY_QUALITY_COLUMNS = {"listing_key", "quality_status"}
IDENTIFIERS_COLUMNS = {"ticker", "isin", "wkn"}
IDENTIFIERS_EXTENDED_COLUMNS = {
    "listing_key",
    "ticker",
    "exchange",
    "isin",
    "wkn",
    "figi",
    "cik",
    "lei",
    "figi_source",
    "cik_source",
    "lei_source",
}
METADATA_UPDATE_COLUMNS = {
    "ticker",
    "exchange",
    "field",
    "decision",
    "proposed_value",
    "confidence",
    "reason",
}
MOJIBAKE_RE = re.compile(r"(?:Ã[\u0080-\u00BF]|Â[\u0080-\u00BF]|â[\u0080-\u00BF]{1,2}|�|\\x[0-9a-fA-F]{2})")


@dataclass(frozen=True)
class Gate:
    name: str
    severity: str
    passed: bool
    actual: int
    limit: int | None = 0
    details: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "severity": self.severity,
            "passed": self.passed,
            "actual": self.actual,
            "limit": self.limit,
        }
        if self.details:
            payload["details"] = self.details[:50]
        return payload


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def csv_columns(path: Path) -> set[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return set(reader.fieldnames or [])


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fail_gate(name: str, actual: int, details: list[str] | None = None, limit: int = 0) -> Gate:
    return Gate(name=name, severity="error", passed=actual <= limit, actual=actual, limit=limit, details=details)


def info_gate(name: str, actual: int, details: list[str] | None = None) -> Gate:
    return Gate(name=name, severity="info", passed=True, actual=actual, limit=None, details=details)


def duplicate_values(rows: list[dict[str, str]], field: str) -> list[str]:
    counts = Counter(row.get(field, "") for row in rows if row.get(field, ""))
    return sorted(value for value, count in counts.items() if count > 1)


def duplicate_composite_values(rows: list[dict[str, str]], fields: tuple[str, ...]) -> list[str]:
    counts = Counter(
        tuple(row.get(field, "") for field in fields)
        for row in rows
        if all(row.get(field, "") for field in fields)
    )
    return ["::".join(value) for value, count in sorted(counts.items()) if count > 1]


def duplicate_public_aliases(rows: list[dict[str, str]]) -> list[str]:
    owners_by_alias: dict[str, set[str]] = {}
    for row in rows:
        owner = listing_key(row)
        for alias in row.get("aliases", "").split("|"):
            normalized = normalize_alias_text(alias).lower()
            if not normalized:
                continue
            owners_by_alias.setdefault(normalized, set()).add(owner)
    return [
        f"{alias}:{','.join(sorted(owners))}"
        for alias, owners in sorted(owners_by_alias.items())
        if len(owners) > 1
    ]


def invalid_asset_type_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [row.get(id_field, "") for row in rows if row.get("asset_type") not in ASSET_TYPES]


def adanos_reference_rows_with_untrimmed_names(rows: list[dict[str, str]]) -> list[str]:
    return [
        f"{row.get('exchange', '')}::{row.get('ticker', '')}:{row.get('name', '')!r}"
        for row in rows
        if row.get("name", "") != row.get("name", "").strip()
    ]


def invalid_isin_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        if isin and not is_valid_isin(isin):
            invalid.append(row.get(id_field, ""))
    return invalid


def identifier_summary_mismatches(
    identifiers_extended: list[dict[str, str]],
    identifier_summary: dict[str, Any] | None,
) -> list[str]:
    if not identifiers_extended and identifier_summary is None:
        return []
    if identifier_summary is None:
        return ["missing identifier summary"]
    expected = {
        "rows": len(identifiers_extended),
        "cik_coverage": sum(bool(row.get("cik")) for row in identifiers_extended),
        "figi_coverage": sum(bool(row.get("figi")) for row in identifiers_extended),
        "lei_coverage": sum(bool(row.get("lei")) for row in identifiers_extended),
        "listings_with_any_identifier": sum(
            bool(row.get("cik") or row.get("figi") or row.get("lei")) for row in identifiers_extended
        ),
        "figi_source_counts": dict(
            Counter(row.get("figi_source") or "unknown" for row in identifiers_extended if row.get("figi"))
        ),
        "cik_source_counts": dict(
            Counter(row.get("cik_source") or "unknown" for row in identifiers_extended if row.get("cik"))
        ),
        "lei_source_counts": dict(
            Counter(row.get("lei_source") or "unknown" for row in identifiers_extended if row.get("lei"))
        ),
    }
    return [
        f"{key}: summary={identifier_summary.get(key)} actual={value}"
        for key, value in expected.items()
        if identifier_summary.get(key) != value
    ]


def figi_cross_isin_collisions(rows: list[dict[str, str]]) -> list[str]:
    figi_to_isins: dict[str, set[str]] = {}
    for row in rows:
        figi = row.get("figi", "")
        isin = row.get("isin", "")
        if figi and isin:
            figi_to_isins.setdefault(figi, set()).add(isin)
    return sorted(
        f"{figi}:{','.join(sorted(isins))}"
        for figi, isins in figi_to_isins.items()
        if len(isins) > 1
    )


def invalid_country_code_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        code = row.get("country_code", "").strip()
        if row.get("country") and (len(code) != 2 or not code.isalpha() or code.upper() != code):
            invalid.append(row.get(id_field, ""))
    return invalid


def rows_missing_country_metadata_despite_isin(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        if not isin:
            continue
        if not country_from_isin(isin):
            continue
        country = row.get("country", "").strip()
        country_code = row.get("country_code", "").strip()
        if not country or not country_code:
            invalid.append(row.get(id_field, ""))
    return invalid


def reviewed_country_override_keys(metadata_updates: list[dict[str, str]]) -> set[tuple[str, str]]:
    return {
        (row.get("ticker", ""), row.get("exchange", ""))
        for row in metadata_updates
        if row.get("field") == "country"
        and row.get("decision") == "update"
        and row.get("proposed_value")
        and row.get("reason")
    }


def rows_with_unreviewed_country_isin_prefix_mismatch(
    rows: list[dict[str, str]],
    id_field: str,
    reviewed_country_overrides: set[tuple[str, str]],
) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        country = row.get("country", "").strip()
        if not isin or not country:
            continue
        inferred_country = country_from_isin(isin)
        if not inferred_country or country == inferred_country:
            continue
        if (row.get("ticker", ""), row.get("exchange", "")) in reviewed_country_overrides:
            continue
        invalid.append(row.get(id_field, ""))
    return invalid


def rows_with_country_code_mismatch(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        country = row.get("country", "").strip()
        code = row.get("country_code", "").strip()
        expected_code = COUNTRY_TO_ISO.get(country, "") if country else ""
        if country and code and expected_code and code != expected_code:
            invalid.append(row.get(id_field, ""))
    return invalid


def stock_rows_with_etf_category(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [row.get(id_field, "") for row in rows if row.get("asset_type") == "Stock" and row.get("etf_category")]


def etf_rows_with_stock_sector(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [row.get(id_field, "") for row in rows if row.get("asset_type") == "ETF" and row.get("stock_sector")]


def rows_with_noncanonical_stock_sector(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [
        row.get(id_field, "")
        for row in rows
        if row.get("stock_sector") and normalize_sector(row.get("stock_sector", ""), "Stock") != row.get("stock_sector")
    ]


def rows_with_noncanonical_etf_category(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [
        row.get(id_field, "")
        for row in rows
        if row.get("etf_category") and normalize_sector(row.get("etf_category", ""), "ETF") != row.get("etf_category")
    ]


def metadata_updates_with_noncanonical_typed_values(metadata_updates: list[dict[str, str]]) -> list[str]:
    invalid: list[str] = []
    for row in metadata_updates:
        field = row.get("field", "")
        if row.get("decision") != "update" or field not in {"stock_sector", "etf_category"}:
            continue
        asset_type = "Stock" if field == "stock_sector" else "ETF"
        value = row.get("proposed_value", "")
        if normalize_sector(value, asset_type) != value:
            invalid.append(f"{row.get('exchange', '')}::{row.get('ticker', '')}:{field}={value}")
    return invalid


def metadata_updates_with_typed_leakage(
    metadata_updates: list[dict[str, str]],
    rows: list[dict[str, str]],
) -> list[str]:
    rows_by_key = {(row.get("ticker", ""), row.get("exchange", "")): row for row in rows}
    invalid: list[str] = []
    for row in metadata_updates:
        field = row.get("field", "")
        if field not in {"stock_sector", "etf_category"}:
            continue
        target = rows_by_key.get((row.get("ticker", ""), row.get("exchange", "")))
        if not target:
            continue
        if field == "stock_sector" and target.get("asset_type") == "ETF":
            invalid.append(f"{row.get('exchange', '')}::{row.get('ticker', '')}:stock_sector on ETF")
        elif field == "etf_category" and target.get("asset_type") == "Stock":
            invalid.append(f"{row.get('exchange', '')}::{row.get('ticker', '')}:etf_category on Stock")
    return invalid


def rows_with_mojibake_names(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        name = row.get("name", "")
        if name and MOJIBAKE_RE.search(name):
            invalid.append(row.get(id_field, ""))
    return invalid


def unresolved_review_alias_removals(
    tickers: list[dict[str, str]],
    review_remove_aliases: list[dict[str, str]],
) -> list[str]:
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers}
    unresolved: list[str] = []
    for review in review_remove_aliases:
        row = by_key.get((review.get("ticker", ""), review.get("exchange", "")))
        if not row:
            continue
        aliases = {alias for alias in row.get("aliases", "").split("|") if alias}
        alias = review.get("alias", "")
        if alias and alias in aliases:
            unresolved.append(f"{review.get('exchange', '')}::{review.get('ticker', '')}:{alias}")
    return unresolved


def listing_key(row: dict[str, str]) -> str:
    return f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def rows_with_listing_key_mismatch(rows: list[dict[str, str]]) -> list[str]:
    return [
        f"{row.get('listing_key', '')} expected {listing_key(row)}"
        for row in rows
        if row.get("listing_key") and row.get("listing_key") != listing_key(row)
    ]


def missing_keys(source: set[str], target: set[str]) -> list[str]:
    return sorted(key for key in source if key and key not in target)


def invalid_instrument_scope_rows(rows: list[dict[str, str]]) -> list[str]:
    return [
        row.get("listing_key", "")
        for row in rows
        if row.get("instrument_scope") not in {"core", "extended"}
    ]


def invalid_scope_reason_rows(rows: list[dict[str, str]]) -> list[str]:
    valid_by_scope = {
        "core": {"primary_listing", "primary_listing_missing_isin"},
        "extended": {"secondary_cross_listing", "global_ticker_collision", "otc_listing"},
    }
    return [
        f"{row.get('listing_key', '')}:{row.get('instrument_scope', '')}:{row.get('scope_reason', '')}"
        for row in rows
        if row.get("scope_reason") not in valid_by_scope.get(row.get("instrument_scope"), set())
    ]


def invalid_scope_primary_links(rows: list[dict[str, str]]) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        listing = row.get("listing_key", "")
        primary = row.get("primary_listing_key", "")
        scope = row.get("instrument_scope", "")
        reason = row.get("scope_reason", "")
        if scope == "core" and primary != listing:
            invalid.append(f"{listing}:core primary={primary}")
        elif reason == "secondary_cross_listing" and primary == listing:
            invalid.append(f"{listing}:secondary_cross_listing self-linked")
        elif reason == "global_ticker_collision" and primary != listing:
            invalid.append(f"{listing}:{reason} primary={primary}")
    return invalid


def cross_listing_primary_group_errors(rows: list[dict[str, str]]) -> list[str]:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        isin = row.get("isin", "")
        if isin:
            groups.setdefault(isin, []).append(row)
    return [
        f"{isin}:{sum(1 for row in group if row.get('is_primary') == '1')}"
        for isin, group in sorted(groups.items())
        if sum(1 for row in group if row.get("is_primary") == "1") != 1
    ]


def expected_cross_listing_pairs(listings: list[dict[str, str]]) -> set[tuple[str, str]]:
    isin_groups: dict[str, list[str]] = {}
    for row in listings:
        isin = row.get("isin", "")
        if isin:
            isin_groups.setdefault(isin, []).append(row.get("listing_key", ""))
    return {
        (isin, listing)
        for isin, listing_keys in isin_groups.items()
        if len(listing_keys) > 1
        for listing in listing_keys
        if listing
    }


def actual_cross_listing_pairs(rows: list[dict[str, str]]) -> set[tuple[str, str]]:
    return {
        (row.get("isin", ""), row.get("listing_key", ""))
        for row in rows
        if row.get("isin") and row.get("listing_key")
    }


def parse_adanos_aliases(value: str) -> tuple[list[str], bool]:
    try:
        parsed = json.loads(value or "[]")
    except json.JSONDecodeError:
        return [], False
    if not isinstance(parsed, list):
        return [], False
    aliases = [alias for alias in parsed if isinstance(alias, str) and alias.strip()]
    return aliases, len(aliases) == len(parsed)


def required_column_gates(
    path_to_columns: dict[Path, set[str]],
    required_by_path: dict[Path, set[str]] | None = None,
) -> list[Gate]:
    gates: list[Gate] = []
    required_by_path = required_by_path or {
        DEFAULT_TICKERS_CSV: TICKERS_COLUMNS,
        DEFAULT_LISTINGS_CSV: LISTINGS_COLUMNS,
        DEFAULT_CORE_LISTINGS_CSV: CORE_LISTINGS_COLUMNS,
        DEFAULT_LISTING_INDEX_CSV: LISTING_INDEX_COLUMNS,
        DEFAULT_IDENTIFIERS_CSV: IDENTIFIERS_COLUMNS,
        DEFAULT_IDENTIFIERS_EXTENDED_CSV: IDENTIFIERS_EXTENDED_COLUMNS,
        DEFAULT_INSTRUMENT_SCOPES_CSV: INSTRUMENT_SCOPE_COLUMNS,
        DEFAULT_CROSS_LISTINGS_CSV: CROSS_LISTING_COLUMNS,
        DEFAULT_ADANOS_REFERENCE_CSV: ADANOS_COLUMNS,
        DEFAULT_ENTRY_QUALITY_CSV: ENTRY_QUALITY_COLUMNS,
        DEFAULT_REVIEW_METADATA_UPDATES_CSV: METADATA_UPDATE_COLUMNS,
    }
    for path, required in required_by_path.items():
        columns = path_to_columns.get(path, set())
        missing = sorted(required - columns)
        gates.append(fail_gate(f"required_columns:{display_path(path)}", len(missing), missing))
    return gates


def compare_coverage_report(
    coverage_report: dict[str, Any] | None,
    tickers: list[dict[str, str]],
    listings: list[dict[str, str]],
) -> list[Gate]:
    if not coverage_report:
        return [info_gate("coverage_report_missing", 1, ["coverage report was not provided"])]

    global_metrics = coverage_report.get("global") or coverage_report.get("summary") or {}
    gates: list[Gate] = []
    expected = {
        "tickers": len(tickers),
        "listing_keys": len(listings),
    }
    for metric, actual_count in expected.items():
        report_count = global_metrics.get(metric)
        mismatch = 0 if report_count == actual_count else 1
        gates.append(
            fail_gate(
                f"coverage_report_{metric}_mismatch",
                mismatch,
                [] if not mismatch else [f"report={report_count} actual={actual_count}"],
            )
        )
    return gates


def build_validation_report(
    *,
    tickers: list[dict[str, str]],
    listings: list[dict[str, str]],
    instrument_scopes: list[dict[str, str]],
    adanos_reference: list[dict[str, str]],
    entry_quality: list[dict[str, str]],
    allowed_warns: set[str],
    adanos_alias_findings: list[dict[str, str]],
    identifiers: list[dict[str, str]] | None = None,
    identifiers_extended: list[dict[str, str]] | None = None,
    identifier_summary: dict[str, Any] | None = None,
    core_listings: list[dict[str, str]] | None = None,
    listing_index: list[dict[str, str]] | None = None,
    cross_listings: list[dict[str, str]] | None = None,
    review_remove_aliases: list[dict[str, str]] | None = None,
    review_metadata_updates: list[dict[str, str]] | None = None,
    coverage_report: dict[str, Any] | None = None,
    path_to_columns: dict[Path, set[str]] | None = None,
    required_columns_by_path: dict[Path, set[str]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or utc_now()
    path_to_columns = path_to_columns or {}
    review_remove_aliases = review_remove_aliases or []
    review_metadata_updates = review_metadata_updates or []
    identifiers = identifiers or []
    identifiers_extended = identifiers_extended or []
    has_identifier_inputs = bool(identifiers or identifiers_extended or identifier_summary is not None)
    core_listings = core_listings or []
    listing_index = listing_index or []
    cross_listings = cross_listings or []

    listing_keys = {row["listing_key"] for row in listings if row.get("listing_key")}
    identifiers_extended_keys = {row["listing_key"] for row in identifiers_extended if row.get("listing_key")}
    core_listing_keys = {row["listing_key"] for row in core_listings if row.get("listing_key")}
    listing_index_keys = {row["listing_key"] for row in listing_index if row.get("listing_key")}
    instrument_scope_by_key = {
        row["listing_key"]: row for row in instrument_scopes if row.get("listing_key")
    }
    instrument_scope_keys = set(instrument_scope_by_key)
    scope_core_listing_keys = {
        row["listing_key"]
        for row in instrument_scopes
        if row.get("listing_key") and row.get("instrument_scope") == "core"
    }
    entry_quality_keys = {row["listing_key"] for row in entry_quality if row.get("listing_key")}
    expected_cross_pairs = expected_cross_listing_pairs(listings)
    actual_cross_pairs = actual_cross_listing_pairs(cross_listings)
    ticker_listing_keys = [listing_key(row) for row in tickers]
    entry_gate = check_entry_quality_gate(entry_quality, allowed_warns)
    public_alias_duplicates = duplicate_public_aliases(tickers)
    identifier_summary_diffs = identifier_summary_mismatches(identifiers_extended, identifier_summary)
    figi_collision_rows = figi_cross_isin_collisions(identifiers_extended)
    reviewed_country_overrides = reviewed_country_override_keys(review_metadata_updates)
    typed_rows = tickers + listings + core_listings
    stock_etf_category_rows = (
        stock_rows_with_etf_category(tickers, "ticker")
        + stock_rows_with_etf_category(listings, "listing_key")
        + stock_rows_with_etf_category(core_listings, "listing_key")
    )
    etf_stock_sector_rows = (
        etf_rows_with_stock_sector(tickers, "ticker")
        + etf_rows_with_stock_sector(listings, "listing_key")
        + etf_rows_with_stock_sector(core_listings, "listing_key")
    )
    noncanonical_stock_sector_rows = (
        rows_with_noncanonical_stock_sector(tickers, "ticker")
        + rows_with_noncanonical_stock_sector(listings, "listing_key")
        + rows_with_noncanonical_stock_sector(core_listings, "listing_key")
    )
    noncanonical_etf_category_rows = (
        rows_with_noncanonical_etf_category(tickers, "ticker")
        + rows_with_noncanonical_etf_category(listings, "listing_key")
        + rows_with_noncanonical_etf_category(core_listings, "listing_key")
    )
    unreviewed_country_mismatch_rows = (
        rows_with_unreviewed_country_isin_prefix_mismatch(tickers, "ticker", reviewed_country_overrides)
        + rows_with_unreviewed_country_isin_prefix_mismatch(listings, "listing_key", reviewed_country_overrides)
        + rows_with_unreviewed_country_isin_prefix_mismatch(core_listings, "listing_key", reviewed_country_overrides)
    )
    country_code_mismatch_rows = (
        rows_with_country_code_mismatch(tickers, "ticker")
        + rows_with_country_code_mismatch(listings, "listing_key")
        + rows_with_country_code_mismatch(core_listings, "listing_key")
    )
    noncanonical_metadata_updates = metadata_updates_with_noncanonical_typed_values(review_metadata_updates)
    metadata_typed_leakage = metadata_updates_with_typed_leakage(review_metadata_updates, typed_rows)

    adanos_alias_parse_errors: list[str] = []
    adanos_common_word_aliases: list[str] = []
    for row in adanos_reference:
        aliases, parsed = parse_adanos_aliases(row.get("aliases", ""))
        if not parsed:
            adanos_alias_parse_errors.append(row.get("ticker", ""))
            continue
        for alias in aliases:
            normalized = normalize_alias_text(alias).lower()
            if normalized and is_common_single_word_alias(normalized):
                adanos_common_word_aliases.append(f"{row.get('ticker', '')}:{alias}")

    ticker_scope_rows = [instrument_scope_by_key.get(key, {}) for key in ticker_listing_keys]
    secondary_primary_rows = [
        key
        for key, scope_row in zip(ticker_listing_keys, ticker_scope_rows, strict=False)
        if scope_row.get("scope_reason") == "secondary_cross_listing"
    ]
    invalid_primary_scope_rows = [
        key
        for key, scope_row in zip(ticker_listing_keys, ticker_scope_rows, strict=False)
        if scope_row and scope_row.get("scope_reason") not in PRIMARY_SCOPE_REASONS
    ]

    gates: list[Gate] = []
    if path_to_columns:
        gates.extend(required_column_gates(path_to_columns, required_columns_by_path))
    gates.extend(
        [
            fail_gate("duplicate_primary_ticker_count", len(duplicate_values(tickers, "ticker"))),
            fail_gate("duplicate_listing_key_count", len(duplicate_values(listings, "listing_key"))),
            fail_gate(
                "duplicate_instrument_scope_listing_key_count",
                len(duplicate_values(instrument_scopes, "listing_key")),
                duplicate_values(instrument_scopes, "listing_key"),
            ),
            fail_gate("duplicate_adanos_ticker_count", len(duplicate_values(adanos_reference, "ticker"))),
            fail_gate(
                "duplicate_public_alias_count",
                len(public_alias_duplicates),
                public_alias_duplicates,
            ),
            fail_gate(
                "duplicate_entry_quality_listing_key_count",
                len(duplicate_values(entry_quality, "listing_key")),
                duplicate_values(entry_quality, "listing_key"),
            ),
            fail_gate(
                "invalid_ticker_asset_type_rows",
                len(invalid_asset_type_rows(tickers, "ticker")),
                invalid_asset_type_rows(tickers, "ticker"),
            ),
            fail_gate(
                "invalid_listing_asset_type_rows",
                len(invalid_asset_type_rows(listings, "listing_key")),
                invalid_asset_type_rows(listings, "listing_key"),
            ),
            fail_gate(
                "invalid_core_listing_asset_type_rows",
                len(invalid_asset_type_rows(core_listings, "listing_key")),
                invalid_asset_type_rows(core_listings, "listing_key"),
            ),
            fail_gate(
                "invalid_adanos_asset_type_rows",
                len(invalid_asset_type_rows(adanos_reference, "ticker")),
                invalid_asset_type_rows(adanos_reference, "ticker"),
            ),
            fail_gate(
                "adanos_reference_untrimmed_name_count",
                len(adanos_reference_rows_with_untrimmed_names(adanos_reference)),
                adanos_reference_rows_with_untrimmed_names(adanos_reference),
            ),
            fail_gate(
                "invalid_isin_rows",
                len(invalid_isin_rows(tickers, "ticker"))
                + len(invalid_isin_rows(listings, "listing_key"))
                + len(invalid_isin_rows(core_listings, "listing_key"))
                + len(invalid_isin_rows(identifiers, "ticker"))
                + len(invalid_isin_rows(identifiers_extended, "listing_key")),
                invalid_isin_rows(tickers, "ticker")
                + invalid_isin_rows(listings, "listing_key")
                + invalid_isin_rows(core_listings, "listing_key")
                + invalid_isin_rows(identifiers, "ticker")
                + invalid_isin_rows(identifiers_extended, "listing_key"),
            ),
            fail_gate("duplicate_identifier_ticker_count", len(duplicate_values(identifiers, "ticker"))),
            fail_gate("duplicate_identifier_listing_key_count", len(duplicate_values(identifiers_extended, "listing_key"))),
            fail_gate(
                "identifier_extended_row_count_mismatch",
                0 if not has_identifier_inputs or len(identifiers_extended) == len(listings) else abs(len(identifiers_extended) - len(listings)),
                [] if not has_identifier_inputs or len(identifiers_extended) == len(listings) else [f"identifiers_extended={len(identifiers_extended)} listings={len(listings)}"],
            ),
            fail_gate(
                "identifier_extended_rows_missing_listing",
                0 if not has_identifier_inputs else len([key for key in identifiers_extended_keys if key not in listing_keys]),
                [] if not has_identifier_inputs else [key for key in identifiers_extended_keys if key not in listing_keys],
            ),
            fail_gate(
                "listing_rows_missing_identifier_extended",
                0 if not has_identifier_inputs else len([key for key in listing_keys if key not in identifiers_extended_keys]),
                [] if not has_identifier_inputs else [key for key in listing_keys if key not in identifiers_extended_keys],
            ),
            fail_gate("identifier_summary_mismatch_count", len(identifier_summary_diffs), identifier_summary_diffs),
            fail_gate("figi_cross_isin_collision_count", len(figi_collision_rows), figi_collision_rows),
            fail_gate(
                "invalid_country_code_rows",
                len(invalid_country_code_rows(tickers, "ticker"))
                + len(invalid_country_code_rows(listings, "listing_key"))
                + len(invalid_country_code_rows(core_listings, "listing_key")),
                invalid_country_code_rows(tickers, "ticker")
                + invalid_country_code_rows(listings, "listing_key")
                + invalid_country_code_rows(core_listings, "listing_key"),
            ),
            fail_gate(
                "country_code_mismatch_rows",
                len(country_code_mismatch_rows),
                country_code_mismatch_rows,
            ),
            fail_gate(
                "rows_missing_country_metadata_despite_isin",
                len(rows_missing_country_metadata_despite_isin(tickers, "ticker"))
                + len(rows_missing_country_metadata_despite_isin(listings, "listing_key"))
                + len(rows_missing_country_metadata_despite_isin(core_listings, "listing_key")),
                rows_missing_country_metadata_despite_isin(tickers, "ticker")
                + rows_missing_country_metadata_despite_isin(listings, "listing_key")
                + rows_missing_country_metadata_despite_isin(core_listings, "listing_key"),
            ),
            fail_gate(
                "country_isin_prefix_mismatch_without_review",
                len(unreviewed_country_mismatch_rows),
                unreviewed_country_mismatch_rows,
            ),
            fail_gate(
                "rows_with_mojibake_names",
                len(rows_with_mojibake_names(tickers, "ticker"))
                + len(rows_with_mojibake_names(listings, "listing_key"))
                + len(rows_with_mojibake_names(core_listings, "listing_key")),
                rows_with_mojibake_names(tickers, "ticker")
                + rows_with_mojibake_names(listings, "listing_key")
                + rows_with_mojibake_names(core_listings, "listing_key"),
            ),
            fail_gate(
                "listing_key_format_mismatch_count",
                len(rows_with_listing_key_mismatch(listings))
                + len(rows_with_listing_key_mismatch(instrument_scopes)),
                rows_with_listing_key_mismatch(listings) + rows_with_listing_key_mismatch(instrument_scopes),
            ),
            fail_gate(
                "ticker_rows_missing_listing",
                len([key for key in ticker_listing_keys if key not in listing_keys]),
                [key for key in ticker_listing_keys if key not in listing_keys],
            ),
            fail_gate(
                "listing_rows_missing_instrument_scope",
                len([key for key in listing_keys if key not in instrument_scope_by_key]),
                [key for key in listing_keys if key not in instrument_scope_by_key],
            ),
            fail_gate(
                "instrument_scope_rows_missing_listing",
                len(missing_keys(instrument_scope_keys, listing_keys)),
                missing_keys(instrument_scope_keys, listing_keys),
            ),
            fail_gate(
                "primary_listing_keys_missing_listing",
                len(missing_keys({row.get("primary_listing_key", "") for row in instrument_scopes}, listing_keys)),
                missing_keys({row.get("primary_listing_key", "") for row in instrument_scopes}, listing_keys),
            ),
            fail_gate(
                "invalid_instrument_scope_rows",
                len(invalid_instrument_scope_rows(instrument_scopes)),
                invalid_instrument_scope_rows(instrument_scopes),
            ),
            fail_gate(
                "invalid_scope_reason_rows",
                len(invalid_scope_reason_rows(instrument_scopes)),
                invalid_scope_reason_rows(instrument_scopes),
            ),
            fail_gate(
                "invalid_scope_primary_link_rows",
                len(invalid_scope_primary_links(instrument_scopes)),
                invalid_scope_primary_links(instrument_scopes),
            ),
            fail_gate(
                "listing_rows_missing_entry_quality",
                len([key for key in listing_keys if key not in entry_quality_keys]),
                [key for key in listing_keys if key not in entry_quality_keys],
            ),
            fail_gate(
                "entry_quality_rows_missing_listing",
                len([key for key in entry_quality_keys if key not in listing_keys]),
                [key for key in entry_quality_keys if key not in listing_keys],
            ),
            fail_gate(
                "primary_rows_that_are_known_secondary_cross_listings",
                len(secondary_primary_rows),
                secondary_primary_rows,
            ),
            fail_gate("primary_rows_with_invalid_scope_reason", len(invalid_primary_scope_rows), invalid_primary_scope_rows),
            fail_gate(
                "stock_rows_with_etf_category",
                len(stock_etf_category_rows),
                stock_etf_category_rows,
            ),
            fail_gate(
                "etf_rows_with_stock_sector",
                len(etf_stock_sector_rows),
                etf_stock_sector_rows,
            ),
            fail_gate(
                "noncanonical_stock_sector_rows",
                len(noncanonical_stock_sector_rows),
                noncanonical_stock_sector_rows,
            ),
            fail_gate(
                "noncanonical_etf_category_rows",
                len(noncanonical_etf_category_rows),
                noncanonical_etf_category_rows,
            ),
            fail_gate(
                "metadata_updates_noncanonical_typed_values",
                len(noncanonical_metadata_updates),
                noncanonical_metadata_updates,
            ),
            fail_gate(
                "metadata_updates_typed_leakage",
                len(metadata_typed_leakage),
                metadata_typed_leakage,
            ),
            fail_gate(
                "adanos_reference_row_count_mismatch",
                0 if len(adanos_reference) == len(tickers) else abs(len(adanos_reference) - len(tickers)),
                [] if len(adanos_reference) == len(tickers) else [f"adanos={len(adanos_reference)} tickers={len(tickers)}"],
            ),
            fail_gate(
                "entry_quality_quarantine_count",
                int(entry_gate["quarantine_count"]),
                list(entry_gate.get("quarantined", [])),
            ),
            fail_gate(
                "entry_quality_unexpected_warn_count",
                int(entry_gate["unexpected_warn_count"]),
                list(entry_gate.get("unexpected_warns", [])),
            ),
            fail_gate(
                "adanos_alias_findings",
                len(adanos_alias_findings),
                [
                    f"{row.get('ticker', '')}:{row.get('alias', '')}:{row.get('issue_type', '')}"
                    for row in adanos_alias_findings
                ],
            ),
            fail_gate("adanos_alias_parse_errors", len(adanos_alias_parse_errors), adanos_alias_parse_errors),
            fail_gate("adanos_alias_common_word_count", len(adanos_common_word_aliases), adanos_common_word_aliases),
            fail_gate(
                "review_alias_removals_open_count",
                len(unresolved_review_alias_removals(tickers, review_remove_aliases)),
                unresolved_review_alias_removals(tickers, review_remove_aliases),
            ),
            info_gate(
                "expected_missing_primary_isin",
                sum(1 for row in entry_quality if "expected_missing_primary_isin" in row.get("issue_types", "")),
            ),
            info_gate(
                "missing_stock_sector",
                sum(1 for row in entry_quality if "missing_stock_sector" in row.get("issue_types", "")),
            ),
            info_gate(
                "missing_etf_category",
                sum(1 for row in entry_quality if "missing_etf_category" in row.get("issue_types", "")),
            ),
            info_gate("source_gap_rows", sum(1 for row in entry_quality if row.get("quality_status") == "source_gap")),
            info_gate("allowed_warn_rows", int(entry_gate["allowed_warn_count"])),
        ]
    )
    if core_listings:
        gates.extend(
            [
                fail_gate(
                    "duplicate_core_listing_key_count",
                    len(duplicate_values(core_listings, "listing_key")),
                    duplicate_values(core_listings, "listing_key"),
                ),
                fail_gate(
                    "core_listing_key_format_mismatch_count",
                    len(rows_with_listing_key_mismatch(core_listings)),
                    rows_with_listing_key_mismatch(core_listings),
                ),
                fail_gate(
                    "core_listing_rows_missing_listing",
                    len(missing_keys(core_listing_keys, listing_keys)),
                    missing_keys(core_listing_keys, listing_keys),
                ),
                fail_gate(
                    "core_listing_scope_mismatch_count",
                    len(missing_keys(core_listing_keys, scope_core_listing_keys))
                    + len(missing_keys(scope_core_listing_keys, core_listing_keys)),
                    missing_keys(core_listing_keys, scope_core_listing_keys)
                    + missing_keys(scope_core_listing_keys, core_listing_keys),
                ),
            ]
        )
    if listing_index:
        gates.extend(
            [
                fail_gate(
                    "duplicate_listing_index_key_count",
                    len(duplicate_values(listing_index, "listing_key")),
                    duplicate_values(listing_index, "listing_key"),
                ),
                fail_gate(
                    "listing_index_key_format_mismatch_count",
                    len(rows_with_listing_key_mismatch(listing_index)),
                    rows_with_listing_key_mismatch(listing_index),
                ),
                fail_gate(
                    "listing_index_key_mismatch_count",
                    len(missing_keys(listing_keys, listing_index_keys))
                    + len(missing_keys(listing_index_keys, listing_keys)),
                    missing_keys(listing_keys, listing_index_keys)
                    + missing_keys(listing_index_keys, listing_keys),
                ),
            ]
        )
    if cross_listings:
        cross_keys = {row["listing_key"] for row in cross_listings if row.get("listing_key")}
        missing_cross_pairs = sorted(expected_cross_pairs - actual_cross_pairs)
        extra_cross_pairs = sorted(actual_cross_pairs - expected_cross_pairs)
        gates.extend(
            [
                fail_gate(
                    "duplicate_cross_listing_pair_count",
                    len(duplicate_composite_values(cross_listings, ("isin", "listing_key"))),
                    duplicate_composite_values(cross_listings, ("isin", "listing_key")),
                ),
                fail_gate(
                    "cross_listing_key_format_mismatch_count",
                    len(rows_with_listing_key_mismatch(cross_listings)),
                    rows_with_listing_key_mismatch(cross_listings),
                ),
                fail_gate(
                    "cross_listing_rows_missing_listing",
                    len(missing_keys(cross_keys, listing_keys)),
                    missing_keys(cross_keys, listing_keys),
                ),
                fail_gate(
                    "cross_listing_primary_group_errors",
                    len(cross_listing_primary_group_errors(cross_listings)),
                    cross_listing_primary_group_errors(cross_listings),
                ),
                fail_gate(
                    "cross_listing_pair_mismatch_count",
                    len(missing_cross_pairs) + len(extra_cross_pairs),
                    [f"missing:{isin}:{key}" for isin, key in missing_cross_pairs]
                    + [f"extra:{isin}:{key}" for isin, key in extra_cross_pairs],
                ),
            ]
        )
    gates.extend(compare_coverage_report(coverage_report, tickers, listings))

    failed_error_gates = [gate for gate in gates if gate.severity == "error" and not gate.passed]
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "tickers_csv": display_path(DEFAULT_TICKERS_CSV),
                "listings_csv": display_path(DEFAULT_LISTINGS_CSV),
                "core_listings_csv": display_path(DEFAULT_CORE_LISTINGS_CSV),
                "listing_index_csv": display_path(DEFAULT_LISTING_INDEX_CSV),
                "identifiers_csv": display_path(DEFAULT_IDENTIFIERS_CSV),
                "identifiers_extended_csv": display_path(DEFAULT_IDENTIFIERS_EXTENDED_CSV),
                "identifier_summary_json": display_path(DEFAULT_IDENTIFIER_SUMMARY_JSON),
                "instrument_scopes_csv": display_path(DEFAULT_INSTRUMENT_SCOPES_CSV),
                "cross_listings_csv": display_path(DEFAULT_CROSS_LISTINGS_CSV),
                "adanos_reference_csv": display_path(DEFAULT_ADANOS_REFERENCE_CSV),
                "entry_quality_csv": display_path(DEFAULT_ENTRY_QUALITY_CSV),
                "adanos_alias_audit_csv": display_path(DEFAULT_ADANOS_ALIAS_AUDIT_CSV),
                "review_remove_aliases_csv": display_path(DEFAULT_REVIEW_REMOVE_ALIASES_CSV),
                "review_metadata_updates_csv": display_path(DEFAULT_REVIEW_METADATA_UPDATES_CSV),
                "coverage_report_json": display_path(DEFAULT_COVERAGE_REPORT_JSON),
            },
        },
        "passed": not failed_error_gates,
        "summary": {
            "ticker_rows": len(tickers),
            "listing_rows": len(listings),
            "adanos_reference_rows": len(adanos_reference),
            "entry_quality_rows": len(entry_quality),
            "error_gates": sum(1 for gate in gates if gate.severity == "error"),
            "failed_error_gates": len(failed_error_gates),
            "info_gates": sum(1 for gate in gates if gate.severity == "info"),
        },
        "gates": [gate.to_dict() for gate in gates],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    status = "PASS" if payload["passed"] else "FAIL"
    lines = [
        "# Database Validation Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        f"Status: `{status}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in payload["summary"].items():
        lines.append(f"| {key} | {value:,} |" if isinstance(value, int) else f"| {key} | {value} |")

    lines.extend(["", "## Gates", "", "| Gate | Severity | Status | Actual | Limit |", "|---|---|---|---:|---:|"])
    for gate in payload["gates"]:
        gate_status = "PASS" if gate["passed"] else "FAIL"
        limit = "" if gate["limit"] is None else str(gate["limit"])
        lines.append(f"| {gate['name']} | {gate['severity']} | {gate_status} | {gate['actual']} | {limit} |")

    failed = [gate for gate in payload["gates"] if gate["severity"] == "error" and not gate["passed"]]
    lines.extend(["", "## Failed Gate Details", ""])
    if not failed:
        lines.append("_No failed error gates._")
    for gate in failed:
        lines.extend([f"### {gate['name']}", "", f"- Actual: `{gate['actual']}`"])
        for detail in gate.get("details", [])[:25]:
            lines.append(f"- `{detail}`")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ticker database release gates.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--core-listings-csv", type=Path, default=DEFAULT_CORE_LISTINGS_CSV)
    parser.add_argument("--listing-index-csv", type=Path, default=DEFAULT_LISTING_INDEX_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=DEFAULT_IDENTIFIERS_CSV)
    parser.add_argument("--identifiers-extended-csv", type=Path, default=DEFAULT_IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--identifier-summary-json", type=Path, default=DEFAULT_IDENTIFIER_SUMMARY_JSON)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=DEFAULT_INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--cross-listings-csv", type=Path, default=DEFAULT_CROSS_LISTINGS_CSV)
    parser.add_argument("--adanos-reference-csv", type=Path, default=DEFAULT_ADANOS_REFERENCE_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=DEFAULT_ENTRY_QUALITY_CSV)
    parser.add_argument("--warn-allowlist-csv", type=Path, default=DEFAULT_WARN_ALLOWLIST_CSV)
    parser.add_argument("--adanos-alias-audit-csv", type=Path, default=DEFAULT_ADANOS_ALIAS_AUDIT_CSV)
    parser.add_argument("--review-remove-aliases-csv", type=Path, default=DEFAULT_REVIEW_REMOVE_ALIASES_CSV)
    parser.add_argument("--review-metadata-updates-csv", type=Path, default=DEFAULT_REVIEW_METADATA_UPDATES_CSV)
    parser.add_argument("--coverage-report-json", type=Path, default=DEFAULT_COVERAGE_REPORT_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--no-write", action="store_true", help="Print summary only; do not write report files.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = [
        args.tickers_csv,
        args.listings_csv,
        args.core_listings_csv,
        args.listing_index_csv,
        args.identifiers_csv,
        args.identifiers_extended_csv,
        args.instrument_scopes_csv,
        args.cross_listings_csv,
        args.adanos_reference_csv,
        args.entry_quality_csv,
        args.review_metadata_updates_csv,
    ]
    path_to_columns = {path: csv_columns(path) for path in paths}
    required_columns_by_path = {
        args.tickers_csv: TICKERS_COLUMNS,
        args.listings_csv: LISTINGS_COLUMNS,
        args.core_listings_csv: CORE_LISTINGS_COLUMNS,
        args.listing_index_csv: LISTING_INDEX_COLUMNS,
        args.identifiers_csv: IDENTIFIERS_COLUMNS,
        args.identifiers_extended_csv: IDENTIFIERS_EXTENDED_COLUMNS,
        args.instrument_scopes_csv: INSTRUMENT_SCOPE_COLUMNS,
        args.cross_listings_csv: CROSS_LISTING_COLUMNS,
        args.adanos_reference_csv: ADANOS_COLUMNS,
        args.entry_quality_csv: ENTRY_QUALITY_COLUMNS,
        args.review_metadata_updates_csv: METADATA_UPDATE_COLUMNS,
    }
    coverage_report = load_json(args.coverage_report_json) if args.coverage_report_json.exists() else None
    payload = build_validation_report(
        tickers=load_csv(args.tickers_csv),
        listings=load_csv(args.listings_csv),
        core_listings=load_csv(args.core_listings_csv),
        listing_index=load_csv(args.listing_index_csv),
        identifiers=load_csv(args.identifiers_csv),
        identifiers_extended=load_csv(args.identifiers_extended_csv),
        identifier_summary=load_json(args.identifier_summary_json) if args.identifier_summary_json.exists() else None,
        instrument_scopes=load_csv(args.instrument_scopes_csv),
        cross_listings=load_csv(args.cross_listings_csv),
        adanos_reference=load_csv(args.adanos_reference_csv),
        entry_quality=load_csv(args.entry_quality_csv),
        allowed_warns=allowed_warn_keys(args.warn_allowlist_csv),
        adanos_alias_findings=load_csv(args.adanos_alias_audit_csv) if args.adanos_alias_audit_csv.exists() else [],
        review_remove_aliases=(
            load_csv(args.review_remove_aliases_csv) if args.review_remove_aliases_csv.exists() else []
        ),
        review_metadata_updates=(
            load_csv(args.review_metadata_updates_csv) if args.review_metadata_updates_csv.exists() else []
        ),
        coverage_report=coverage_report,
        path_to_columns=path_to_columns,
        required_columns_by_path=required_columns_by_path,
    )
    if not args.no_write:
        write_json(args.json_out, payload)
        write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "passed": payload["passed"],
                "summary": payload["summary"],
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
