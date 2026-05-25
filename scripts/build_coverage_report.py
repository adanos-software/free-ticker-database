from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.listing_keys import build_listing_key, row_listing_key
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from listing_keys import build_listing_key, row_listing_key


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
TICKERS_CSV = DATA_DIR / "tickers.csv"
LISTINGS_CSV = DATA_DIR / "listings.csv"
CORE_LISTINGS_CSV = DATA_DIR / "core_listings.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"
ALIASES_CSV = DATA_DIR / "aliases.csv"
INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
IDENTIFIER_SUMMARY_JSON = DATA_DIR / "identifier_summary.json"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SOURCES_JSON = DATA_DIR / "masterfiles" / "sources.json"
MASTERFILE_SUMMARY_JSON = DATA_DIR / "masterfiles" / "summary.json"
LISTING_STATUS_HISTORY_CSV = DATA_DIR / "history" / "listing_status_history.csv"
LISTING_EVENTS_CSV = DATA_DIR / "history" / "listing_events.csv"
DAILY_LISTING_SUMMARY_JSON = DATA_DIR / "history" / "daily_listing_summary.json"
STOCK_VERIFICATION_DIR = DATA_DIR / "stock_verification"
ETF_VERIFICATION_DIR = DATA_DIR / "etf_verification"
COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
COVERAGE_REPORT_MD = REPORTS_DIR / "coverage_report.md"
MASTERFILE_COLLISION_REPORT_JSON = REPORTS_DIR / "masterfile_collision_report.json"
MASTERFILE_COLLISION_REVIEW_JSON = REPORTS_DIR / "masterfile_collision_review.json"
SYMBOL_CHANGES_REVIEW_JSON = REPORTS_DIR / "symbol_changes_review.json"
SOURCE_GAP_CLASSIFICATION_JSON = REPORTS_DIR / "source_gap_classification.json"
ENTRY_QUALITY_JSON = REPORTS_DIR / "entry_quality.json"
OHLCV_PLAUSIBILITY_JSON = REPORTS_DIR / "ohlcv_plausibility.json"

COVERAGE_SOURCE_FILES = {
    "tickers_csv": TICKERS_CSV,
    "listings_csv": LISTINGS_CSV,
    "core_listings_csv": CORE_LISTINGS_CSV,
    "aliases_csv": ALIASES_CSV,
    "instrument_scopes_csv": INSTRUMENT_SCOPES_CSV,
    "identifiers_extended_csv": IDENTIFIERS_EXTENDED_CSV,
    "identifier_summary_json": IDENTIFIER_SUMMARY_JSON,
    "masterfile_reference_csv": MASTERFILE_REFERENCE_CSV,
    "masterfile_sources_json": MASTERFILE_SOURCES_JSON,
    "masterfile_summary_json": MASTERFILE_SUMMARY_JSON,
    "listing_status_history_csv": LISTING_STATUS_HISTORY_CSV,
    "listing_events_csv": LISTING_EVENTS_CSV,
    "daily_listing_summary_json": DAILY_LISTING_SUMMARY_JSON,
    "symbol_changes_review_json": SYMBOL_CHANGES_REVIEW_JSON,
    "entry_quality_json": ENTRY_QUALITY_JSON,
    "source_gap_classification_json": SOURCE_GAP_CLASSIFICATION_JSON,
    "masterfile_collision_review_json": MASTERFILE_COLLISION_REVIEW_JSON,
    "ohlcv_plausibility_json": OHLCV_PLAUSIBILITY_JSON,
}

FRESHNESS_REVIEW_SIGNALS = (
    (
        "Dataset build",
        "tickers_built_at",
        "tickers_age_hours",
        "",
        "dataset_age_visibility_no_data_change_authorized",
    ),
    (
        "Masterfiles",
        "masterfiles_generated_at",
        "masterfiles_age_hours",
        "",
        "refresh_old_official_sources_before_identity_or_gap_work",
    ),
    (
        "Identifiers",
        "identifiers_generated_at",
        "identifiers_age_hours",
        "",
        "identifier_age_visibility_no_identifier_backfill_authorized",
    ),
    (
        "Listing history",
        "listing_history_observed_at",
        "listing_history_age_hours",
        "",
        "refresh_listing_history_before_fresh_listing_status_claims",
    ),
    (
        "Stock verification",
        "latest_stock_verification_generated_at",
        "latest_stock_verification_age_hours",
        "",
        "rerun_verification_before_closing_stock_source_gaps",
    ),
    (
        "ETF verification",
        "latest_etf_verification_generated_at",
        "latest_etf_verification_age_hours",
        "",
        "rerun_verification_before_closing_etf_source_gaps",
    ),
    (
        "Symbol changes",
        "symbol_changes_generated_at",
        "symbol_changes_age_hours",
        "symbol_changes_review_rows",
        "symbol_change_age_visibility_no_symbol_change_authorized",
    ),
    (
        "Entry quality",
        "entry_quality_generated_at",
        "entry_quality_age_hours",
        "entry_quality_rows",
        "entry_quality_age_visibility_no_quality_gate_override",
    ),
    (
        "Source gaps",
        "source_gap_classification_generated_at",
        "source_gap_classification_age_hours",
        "source_gap_classification_rows",
        "source_gap_age_visibility_no_gap_fill_authorized",
    ),
    (
        "Masterfile collisions",
        "masterfile_collision_review_generated_at",
        "masterfile_collision_review_age_hours",
        "masterfile_collision_review_rows",
        "collision_review_age_visibility_no_symbol_only_match_authorized",
    ),
    (
        "OHLCV plausibility",
        "ohlcv_plausibility_generated_at",
        "ohlcv_plausibility_age_hours",
        "ohlcv_plausibility_rows",
        "ohlcv_age_visibility_plausibility_only",
    ),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def listing_identity(row: dict[str, str]) -> str:
    return row.get("listing_key") or row_listing_key(row)


def stock_sector_value(row: dict[str, str]) -> str:
    if row.get("asset_type") != "Stock":
        return ""
    return row.get("stock_sector", "") or row.get("sector", "")


def etf_category_value(row: dict[str, str]) -> str:
    if row.get("asset_type") != "ETF":
        return ""
    return row.get("etf_category", "") or row.get("sector", "")


def metadata_sector_value(row: dict[str, str]) -> str:
    if row.get("asset_type") == "Stock":
        return stock_sector_value(row)
    if row.get("asset_type") == "ETF":
        return etf_category_value(row)
    return row.get("sector", "")


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def path_mtime_iso(path: Path) -> str:
    if not path.exists():
        return ""
    timestamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def latest_timestamp_iso(*values: str) -> str:
    latest: datetime | None = None
    latest_value = ""
    for value in values:
        parsed = parse_timestamp(value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
            latest_value = value
    return latest_value


def resolve_identifiers_generated_at(identifier_summary: dict[str, Any]) -> str:
    return latest_timestamp_iso(
        identifier_summary.get("generated_at", ""),
        path_mtime_iso(IDENTIFIERS_CSV),
        path_mtime_iso(IDENTIFIERS_EXTENDED_CSV),
        path_mtime_iso(IDENTIFIER_SUMMARY_JSON),
    )


def payload_generated_at(payload: dict[str, Any], fallback_path: Path) -> str:
    return (
        payload.get("_meta", {}).get("generated_at", "")
        or payload.get("summary", {}).get("generated_at", "")
        or payload.get("generated_at", "")
        or path_mtime_iso(fallback_path)
    )


def payload_row_count(payload: dict[str, Any], *keys: str) -> int:
    for container in (payload.get("summary", {}), payload.get("_meta", {}), payload):
        if not isinstance(container, dict):
            continue
        for key in keys:
            value = container.get(key)
            if value in {"", None}:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    rows = payload.get("rows")
    if isinstance(rows, list):
        return len(rows)
    return 0


def latest_verification_marker_mtime(path: Path) -> float:
    summary_path = path / "summary.json"
    if summary_path.exists():
        return summary_path.stat().st_mtime
    chunk_summaries = list(path.glob("chunk-*-of-*.summary.json"))
    if chunk_summaries:
        return max(candidate.stat().st_mtime for candidate in chunk_summaries)
    return 0.0


def latest_verification_marker_iso(path: Path) -> str:
    summary_path = path / "summary.json"
    if summary_path.exists():
        return path_mtime_iso(summary_path)
    chunk_summaries = list(path.glob("chunk-*-of-*.summary.json"))
    if not chunk_summaries:
        return ""
    latest_path = max(chunk_summaries, key=lambda candidate: candidate.stat().st_mtime)
    return path_mtime_iso(latest_path)


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def age_hours(timestamp: str, *, now: datetime | None = None) -> float | None:
    parsed = parse_timestamp(timestamp)
    if parsed is None:
        return None
    now = now or utc_now()
    return round((now - parsed).total_seconds() / 3600, 2)


def age_bucket_for_hours(value: float | int | None) -> str:
    if value is None:
        return "unknown_age"
    if value <= 48:
        return "age_0_48h"
    if value <= 168:
        return "age_48_168h"
    if value <= 336:
        return "age_168_336h"
    return "age_over_336h"


def build_exchange_reference_catalog(masterfiles: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for row in masterfiles:
        exchange = row.get("exchange", "")
        if not exchange:
            continue
        entry = catalog.setdefault(
            exchange,
            {
                "exchange": exchange,
                "venue_status": "missing",
                "official_source_keys": set(),
                "reference_scopes": set(),
                "manual_source_keys": set(),
            },
        )
        source_key = row.get("source_key", "")
        reference_scope = row.get("reference_scope", "")
        if row.get("official") == "true":
            if source_key:
                entry["official_source_keys"].add(source_key)
            if reference_scope:
                entry["reference_scopes"].add(reference_scope)
            if reference_scope == "exchange_directory":
                entry["venue_status"] = "official_full"
            elif entry["venue_status"] != "official_full":
                entry["venue_status"] = "official_partial"
        elif source_key:
            entry["manual_source_keys"].add(source_key)
            if entry["venue_status"] == "missing":
                entry["venue_status"] = "manual_only"

    normalized: dict[str, dict[str, Any]] = {}
    for exchange, entry in catalog.items():
        normalized[exchange] = {
            "exchange": exchange,
            "venue_status": entry["venue_status"],
            "official_source_count": len(entry["official_source_keys"]),
            "manual_source_count": len(entry["manual_source_keys"]),
            "reference_scopes": sorted(entry["reference_scopes"]),
        }
    return normalized


def build_exchange_report(
    tickers: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
    stock_verification_exchange_rows: list[dict[str, Any]] | None = None,
    etf_verification_exchange_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    identifier_lookup = {
        listing_identity(row): row for row in identifiers_extended
    }
    stock_verification_lookup = {
        row["exchange"]: row for row in (stock_verification_exchange_rows or [])
    }
    etf_verification_lookup = {
        row["exchange"]: row for row in (etf_verification_exchange_rows or [])
    }
    reference_catalog = build_exchange_reference_catalog(masterfiles)
    master_by_exchange: dict[str, set[str]] = defaultdict(set)
    dataset_exchanges_by_ticker: dict[str, set[str]] = defaultdict(set)
    for row in tickers:
        if row["ticker"] and row["exchange"]:
            dataset_exchanges_by_ticker[row["ticker"]].add(row["exchange"])
    for row in masterfiles:
        if row.get("listing_status") == "active" and row.get("reference_scope") == "exchange_directory":
            master_by_exchange[row["exchange"]].add(row["ticker"])

    exchanges = sorted(
        ({row["exchange"] for row in tickers if row["exchange"]})
        | {exchange for exchange in master_by_exchange if exchange}
    )

    rows: list[dict[str, Any]] = []
    for exchange in exchanges:
        exchange_rows = [row for row in tickers if row["exchange"] == exchange]
        identifiers = [identifier_lookup.get(listing_identity(row), {}) for row in exchange_rows]
        dataset_symbols = {row["ticker"] for row in exchange_rows}
        master_symbols = master_by_exchange.get(exchange, set())
        matched = dataset_symbols & master_symbols if master_symbols else set()
        collisions = {
            ticker
            for ticker in (master_symbols - matched)
            if dataset_exchanges_by_ticker.get(ticker, set()) - {exchange}
        }
        missing = master_symbols - matched - collisions
        stock_verification = stock_verification_lookup.get(exchange, {})
        etf_verification = etf_verification_lookup.get(exchange, {})
        catalog_entry = reference_catalog.get(
            exchange,
            {
                "venue_status": "missing",
                "official_source_count": 0,
                "manual_source_count": 0,
                "reference_scopes": [],
            },
        )
        stock_covered_items = int(stock_verification.get("officially_covered_items", 0))
        stock_verified_items = int(stock_verification.get("verified", 0))
        etf_covered_items = int(etf_verification.get("officially_covered_items", 0))
        etf_verified_items = int(etf_verification.get("verified", 0))
        unresolved_count = (
            int(stock_verification.get("reference_gap", 0))
            + int(stock_verification.get("missing_from_official", 0))
            + int(stock_verification.get("name_mismatch", 0))
            + int(stock_verification.get("cross_exchange_collision", 0))
            + int(etf_verification.get("reference_gap", 0))
            + int(etf_verification.get("missing_from_official", 0))
            + int(etf_verification.get("name_mismatch", 0))
            + int(etf_verification.get("cross_exchange_collision", 0))
        )
        rows.append(
            {
                "exchange": exchange,
                "venue_status": catalog_entry["venue_status"],
                "official_source_count": catalog_entry["official_source_count"],
                "manual_source_count": catalog_entry["manual_source_count"],
                "reference_scopes": catalog_entry["reference_scopes"],
                "tickers": len(exchange_rows),
                "isin_coverage": sum(bool(row["isin"]) for row in exchange_rows),
                "sector_coverage": sum(bool(metadata_sector_value(row)) for row in exchange_rows),
                "cik_coverage": sum(bool(row.get("cik")) for row in identifiers),
                "figi_coverage": sum(bool(row.get("figi")) for row in identifiers),
                "lei_coverage": sum(bool(row.get("lei")) for row in identifiers),
                "masterfile_symbols": len(master_symbols),
                "masterfile_matches": len(matched),
                "masterfile_collisions": len(collisions),
                "masterfile_missing": len(missing),
                "masterfile_match_rate": round(len(matched) / len(master_symbols) * 100, 2) if master_symbols else None,
                "masterfile_collision_rate": round(len(collisions) / len(master_symbols) * 100, 2) if master_symbols else None,
                "verification_items": int(stock_verification.get("items", 0)),
                "verification_verified": stock_verified_items,
                "verification_reference_gap": int(stock_verification.get("reference_gap", 0)),
                "verification_missing_from_official": int(stock_verification.get("missing_from_official", 0)),
                "verification_name_mismatch": int(stock_verification.get("name_mismatch", 0)),
                "verification_cross_exchange_collision": int(stock_verification.get("cross_exchange_collision", 0)),
                "verification_verified_rate_on_covered": round(stock_verified_items / stock_covered_items * 100, 2) if stock_covered_items else None,
                "stock_verification_items": int(stock_verification.get("items", 0)),
                "stock_verification_verified": stock_verified_items,
                "stock_verification_reference_gap": int(stock_verification.get("reference_gap", 0)),
                "stock_verification_missing_from_official": int(stock_verification.get("missing_from_official", 0)),
                "stock_verification_name_mismatch": int(stock_verification.get("name_mismatch", 0)),
                "stock_verification_cross_exchange_collision": int(stock_verification.get("cross_exchange_collision", 0)),
                "stock_verification_verified_rate_on_covered": round(stock_verified_items / stock_covered_items * 100, 2) if stock_covered_items else None,
                "etf_verification_items": int(etf_verification.get("items", 0)),
                "etf_verification_verified": etf_verified_items,
                "etf_verification_reference_gap": int(etf_verification.get("reference_gap", 0)),
                "etf_verification_missing_from_official": int(etf_verification.get("missing_from_official", 0)),
                "etf_verification_name_mismatch": int(etf_verification.get("name_mismatch", 0)),
                "etf_verification_cross_exchange_collision": int(etf_verification.get("cross_exchange_collision", 0)),
                "etf_verification_verified_rate_on_covered": round(etf_verified_items / etf_covered_items * 100, 2) if etf_covered_items else None,
                "unresolved_count": unresolved_count,
            }
        )
    return rows


def build_country_report(
    tickers: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
) -> list[dict[str, Any]]:
    identifier_lookup = {
        listing_identity(row): row for row in identifiers_extended
    }
    rows: list[dict[str, Any]] = []
    for country in sorted({row["country"] for row in tickers if row["country"]}):
        country_rows = [row for row in tickers if row["country"] == country]
        identifiers = [identifier_lookup.get(listing_identity(row), {}) for row in country_rows]
        rows.append(
            {
                "country": country,
                "tickers": len(country_rows),
                "isin_coverage": sum(bool(row["isin"]) for row in country_rows),
                "sector_coverage": sum(bool(metadata_sector_value(row)) for row in country_rows),
                "cik_coverage": sum(bool(row.get("cik")) for row in identifiers),
                "figi_coverage": sum(bool(row.get("figi")) for row in identifiers),
                "lei_coverage": sum(bool(row.get("lei")) for row in identifiers),
            }
        )
    return rows


def build_global_summary(
    tickers: list[dict[str, str]],
    listings: list[dict[str, str]],
    aliases: list[dict[str, str]],
    instrument_scopes: list[dict[str, str]] | None,
    identifiers_extended: list[dict[str, str]],
    listing_status_history: list[dict[str, str]],
    listing_events: list[dict[str, str]],
    exchange_coverage: list[dict[str, Any]],
    stock_verification_summary: dict[str, Any] | None = None,
    etf_verification_summary: dict[str, Any] | None = None,
    core_listings: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    venue_status_counts = Counter(row["venue_status"] for row in exchange_coverage)
    instrument_scope_counts = Counter(row["instrument_scope"] for row in (instrument_scopes or []))
    scope_reason_counts = Counter(row["scope_reason"] for row in (instrument_scopes or []))
    core_listings = core_listings or tickers
    legacy_primary_listing_keys = {listing_identity(row) for row in tickers}
    core_listing_keys = {row.get("listing_key") or row_listing_key(row) for row in core_listings}
    summary = {
        "tickers": len(tickers),
        "core_listings": len(core_listings),
        "aliases": len(aliases),
        "stocks": sum(row["asset_type"] == "Stock" for row in tickers),
        "etfs": sum(row["asset_type"] == "ETF" for row in tickers),
        "isin_coverage": sum(bool(row["isin"]) for row in tickers),
        "sector_coverage": sum(bool(metadata_sector_value(row)) for row in tickers),
        "stock_sector_coverage": sum(bool(stock_sector_value(row)) for row in tickers),
        "etf_category_coverage": sum(bool(etf_category_value(row)) for row in tickers),
        "cik_coverage": sum(bool(row.get("cik")) for row in identifiers_extended),
        "figi_coverage": sum(bool(row.get("figi")) for row in identifiers_extended),
        "lei_coverage": sum(bool(row.get("lei")) for row in identifiers_extended),
        "listing_status_rows": len(listing_status_history),
        "listing_status_intervals": len(listing_status_history),
        "listing_events": len(listing_events),
        "listing_keys": len({row_listing_key(row) for row in listings}),
        "instrument_scope_rows": len(instrument_scopes or []),
        "instrument_scope_core": instrument_scope_counts.get("core", 0),
        "instrument_scope_extended": instrument_scope_counts.get("extended", 0),
        "instrument_scope_primary_listing": scope_reason_counts.get("primary_listing", 0),
        "instrument_scope_primary_listing_missing_isin": scope_reason_counts.get("primary_listing_missing_isin", 0),
        "instrument_scope_otc_listing": scope_reason_counts.get("otc_listing", 0),
        "instrument_scope_secondary_cross_listing": scope_reason_counts.get("secondary_cross_listing", 0),
        "legacy_primary_ticker_collision_rows": len(core_listing_keys - legacy_primary_listing_keys),
        "official_masterfile_symbols": sum(row["masterfile_symbols"] for row in exchange_coverage),
        "official_masterfile_matches": sum(row["masterfile_matches"] for row in exchange_coverage),
        "official_masterfile_collisions": sum(row["masterfile_collisions"] for row in exchange_coverage),
        "official_masterfile_missing": sum(row["masterfile_missing"] for row in exchange_coverage),
        "official_full_exchanges": venue_status_counts.get("official_full", 0),
        "official_partial_exchanges": venue_status_counts.get("official_partial", 0),
        "manual_only_exchanges": venue_status_counts.get("manual_only", 0),
        "missing_exchanges": venue_status_counts.get("missing", 0),
    }
    if stock_verification_summary:
        summary.update(
            {
                "stock_verification_items": int(stock_verification_summary.get("items", 0)),
                "stock_verification_verified": int(stock_verification_summary.get("status_counts", {}).get("verified", 0)),
                "stock_verification_reference_gap": int(stock_verification_summary.get("status_counts", {}).get("reference_gap", 0)),
                "stock_verification_missing_from_official": int(stock_verification_summary.get("status_counts", {}).get("missing_from_official", 0)),
                "stock_verification_name_mismatch": int(stock_verification_summary.get("status_counts", {}).get("name_mismatch", 0)),
                "stock_verification_cross_exchange_collision": int(stock_verification_summary.get("status_counts", {}).get("cross_exchange_collision", 0)),
            }
        )
    if etf_verification_summary:
        summary.update(
            {
                "etf_verification_items": int(etf_verification_summary.get("items", 0)),
                "etf_verification_verified": int(etf_verification_summary.get("status_counts", {}).get("verified", 0)),
                "etf_verification_reference_gap": int(etf_verification_summary.get("status_counts", {}).get("reference_gap", 0)),
                "etf_verification_missing_from_official": int(etf_verification_summary.get("status_counts", {}).get("missing_from_official", 0)),
                "etf_verification_name_mismatch": int(etf_verification_summary.get("status_counts", {}).get("name_mismatch", 0)),
                "etf_verification_cross_exchange_collision": int(etf_verification_summary.get("status_counts", {}).get("cross_exchange_collision", 0)),
            }
        )
    return summary


def find_latest_verification_run(base_dir: Path = STOCK_VERIFICATION_DIR) -> Path | None:
    if not base_dir.exists():
        return None
    candidates = [
        path
        for path in base_dir.iterdir()
        if path.is_dir() and ((path / "summary.json").exists() or list(path.glob("chunk-*-of-*.summary.json")))
    ]
    if not candidates:
        return None
    return max(candidates, key=latest_verification_marker_mtime)


def load_verification_report(run_dir: Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {"summary": {}, "exchange_rows": [], "rows": [], "run_dir": ""}

    summary = load_json(run_dir / "summary.json")
    rows: list[dict[str, Any]] = []
    exchange_stats: dict[str, Counter[str]] = defaultdict(Counter)
    exchange_items: Counter[str] = Counter()
    for path in sorted(run_dir.glob("chunk-*-of-*.json")):
        if path.name.endswith(".summary.json"):
            continue
        payload = load_json(path)
        for row in payload:
            rows.append(row)
            exchange = row.get("exchange", "")
            if not exchange:
                continue
            exchange_items[exchange] += 1
            exchange_stats[exchange][row.get("status", "")] += 1

    exchange_rows: list[dict[str, Any]] = []
    for exchange in sorted(exchange_items):
        stats = exchange_stats[exchange]
        covered_items = exchange_items[exchange] - stats.get("reference_gap", 0)
        verified_items = stats.get("verified", 0)
        exchange_rows.append(
            {
                "exchange": exchange,
                "items": exchange_items[exchange],
                "verified": verified_items,
                "reference_gap": stats.get("reference_gap", 0),
                "missing_from_official": stats.get("missing_from_official", 0),
                "name_mismatch": stats.get("name_mismatch", 0),
                "cross_exchange_collision": stats.get("cross_exchange_collision", 0),
                "asset_type_mismatch": stats.get("asset_type_mismatch", 0),
                "non_active_official": stats.get("non_active_official", 0),
                "officially_covered_items": covered_items,
                "verified_rate_on_covered": round(verified_items / covered_items * 100, 2) if covered_items else None,
            }
        )

    if not summary:
        status_counts = Counter(row.get("status", "") for row in rows)
        summary = {
            "items": len(rows),
            "status_counts": dict(sorted(status_counts.items())),
            "finding_examples": [row for row in rows if row.get("status") in {"asset_type_mismatch", "name_mismatch", "missing_from_official", "non_active_official"}][:25],
        }

    return {
        "summary": summary,
        "exchange_rows": exchange_rows,
        "rows": rows,
        "run_dir": display_path(run_dir),
        "generated_at": latest_verification_marker_iso(run_dir),
    }


def classify_b3_gap(row: dict[str, Any]) -> str:
    ticker = row.get("ticker", "")
    if ticker.endswith(("31", "32", "33", "34", "35", "39")):
        return "bdr_or_foreign_receipt"
    if ticker.endswith("F"):
        return "fractional_line"
    if ticker.endswith("11"):
        return "unit_or_fund_line"
    if ticker and ticker[-1:].isdigit():
        return "local_share_line"
    return "other"


def build_b3_gap_breakdown(verification_rows: list[dict[str, Any]]) -> dict[str, Any]:
    relevant = [
        row
        for row in verification_rows
        if row.get("exchange") == "B3" and row.get("status") == "missing_from_official"
    ]
    counts = Counter(classify_b3_gap(row) for row in relevant)
    examples: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in relevant:
        category = classify_b3_gap(row)
        if len(examples[category]) < 10:
            examples[category].append(
                {
                    "ticker": row.get("ticker", ""),
                    "name": row.get("name", ""),
                    "status": row.get("status", ""),
                }
            )
    return {
        "rows": len(relevant),
        "categories": dict(sorted(counts.items())),
        "examples": dict(examples),
    }


def build_b3_masterfile_diagnostics(
    listings: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
) -> dict[str, Any]:
    b3_listings = [
        row
        for row in listings
        if row.get("exchange") == "B3" and row.get("ticker")
    ]
    active_exchange_directory = [
        row
        for row in masterfiles
        if row.get("exchange") == "B3"
        and row.get("listing_status") == "active"
        and row.get("reference_scope") == "exchange_directory"
        and row.get("ticker")
    ]
    all_b3_masterfiles = [
        row
        for row in masterfiles
        if row.get("exchange") == "B3" and row.get("ticker")
    ]
    active_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    all_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in active_exchange_directory:
        active_by_ticker[row["ticker"]].append(row)
    for row in all_b3_masterfiles:
        all_by_ticker[row["ticker"]].append(row)

    matched_rows = [row for row in b3_listings if row["ticker"] in active_by_ticker]
    missing_rows = [row for row in b3_listings if row["ticker"] not in active_by_ticker]
    official_any_source_matched_rows = [row for row in b3_listings if row["ticker"] in all_by_ticker]
    official_any_source_missing_rows = [row for row in b3_listings if row["ticker"] not in all_by_ticker]
    missing_categories = Counter(classify_b3_gap(row) for row in missing_rows)
    missing_asset_types = Counter(row.get("asset_type", "") or "unknown" for row in missing_rows)
    missing_source_presence = Counter(
        "present_only_in_non_exchange_directory_source"
        if all_by_ticker.get(row["ticker"])
        else "absent_from_all_b3_masterfile_sources"
        for row in missing_rows
    )
    examples: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in missing_rows:
        category = classify_b3_gap(row)
        if len(examples[category]) >= 10:
            continue
        candidate_sources = sorted({source.get("source_key", "") for source in all_by_ticker.get(row["ticker"], []) if source.get("source_key")})
        examples[category].append(
            {
                "listing_key": listing_identity(row),
                "ticker": row.get("ticker", ""),
                "asset_type": row.get("asset_type", ""),
                "name": row.get("name", ""),
                "source_presence": (
                    "present_only_in_non_exchange_directory_source"
                    if candidate_sources
                    else "absent_from_all_b3_masterfile_sources"
                ),
                "candidate_sources": "|".join(candidate_sources),
            }
        )

    active_symbols = set(active_by_ticker)
    dataset_symbols = {row["ticker"] for row in b3_listings}
    official_not_in_dataset = active_symbols - dataset_symbols
    return {
        "policy": "B3 diagnostic only. Missing rows are review targets and do not authorize inferred identifiers, sectors, categories, or names.",
        "dataset_rows": len(b3_listings),
        "active_exchange_directory_rows": len(active_exchange_directory),
        "all_b3_masterfile_rows": len(all_b3_masterfiles),
        "matched_dataset_rows": len(matched_rows),
        "missing_dataset_rows": len(missing_rows),
        "official_any_source_matched_dataset_rows": len(official_any_source_matched_rows),
        "official_any_source_missing_dataset_rows": len(official_any_source_missing_rows),
        "official_any_source_match_rate": (
            round(len(official_any_source_matched_rows) / len(b3_listings) * 100, 2) if b3_listings else None
        ),
        "official_active_symbols_not_in_dataset": len(official_not_in_dataset),
        "dataset_match_rate": round(len(matched_rows) / len(b3_listings) * 100, 2) if b3_listings else None,
        "active_source_key_totals": dict(sorted(Counter(row.get("source_key", "") for row in active_exchange_directory).items())),
        "all_source_key_totals": dict(sorted(Counter(row.get("source_key", "") for row in all_b3_masterfiles).items())),
        "missing_category_totals": dict(sorted(missing_categories.items())),
        "missing_asset_type_totals": dict(sorted(missing_asset_types.items())),
        "missing_source_presence_totals": dict(sorted(missing_source_presence.items())),
        "missing_examples": dict(examples),
    }


def build_gap_report(
    exchange_coverage: list[dict[str, Any]],
    stock_verification_exchange_rows: list[dict[str, Any]],
    etf_verification_exchange_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    stock_verification_lookup = {row["exchange"]: row for row in stock_verification_exchange_rows}
    etf_verification_lookup = {row["exchange"]: row for row in etf_verification_exchange_rows}
    gaps: list[dict[str, Any]] = []
    for row in exchange_coverage:
        stock_verification = stock_verification_lookup.get(row["exchange"], {})
        etf_verification = etf_verification_lookup.get(row["exchange"], {})
        unresolved = (
            int(stock_verification.get("reference_gap", 0))
            + int(stock_verification.get("missing_from_official", 0))
            + int(stock_verification.get("name_mismatch", 0))
            + int(stock_verification.get("cross_exchange_collision", 0))
            + int(etf_verification.get("reference_gap", 0))
            + int(etf_verification.get("missing_from_official", 0))
            + int(etf_verification.get("name_mismatch", 0))
            + int(etf_verification.get("cross_exchange_collision", 0))
        )
        if not unresolved:
            continue
        gaps.append(
            {
                "exchange": row["exchange"],
                "venue_status": row["venue_status"],
                "unresolved_findings": unresolved,
                "reference_gap": int(stock_verification.get("reference_gap", 0)) + int(etf_verification.get("reference_gap", 0)),
                "missing_from_official": int(stock_verification.get("missing_from_official", 0)) + int(etf_verification.get("missing_from_official", 0)),
                "name_mismatch": int(stock_verification.get("name_mismatch", 0)) + int(etf_verification.get("name_mismatch", 0)),
                "cross_exchange_collision": int(stock_verification.get("cross_exchange_collision", 0)) + int(etf_verification.get("cross_exchange_collision", 0)),
                "stock_reference_gap": int(stock_verification.get("reference_gap", 0)),
                "stock_missing_from_official": int(stock_verification.get("missing_from_official", 0)),
                "stock_name_mismatch": int(stock_verification.get("name_mismatch", 0)),
                "stock_cross_exchange_collision": int(stock_verification.get("cross_exchange_collision", 0)),
                "etf_reference_gap": int(etf_verification.get("reference_gap", 0)),
                "etf_missing_from_official": int(etf_verification.get("missing_from_official", 0)),
                "etf_name_mismatch": int(etf_verification.get("name_mismatch", 0)),
                "etf_cross_exchange_collision": int(etf_verification.get("cross_exchange_collision", 0)),
                "masterfile_missing": row["masterfile_missing"],
                "masterfile_collisions": row["masterfile_collisions"],
            }
        )
    return sorted(gaps, key=lambda row: (-row["unresolved_findings"], row["exchange"]))


def build_source_report(
    sources: list[dict[str, Any]],
    masterfile_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    now = utc_now()
    source_details = masterfile_summary.get("source_details", {})
    source_counts = masterfile_summary.get("source_counts", {})
    source_modes = masterfile_summary.get("source_modes", {})
    generated_at = masterfile_summary.get("generated_at", path_mtime_iso(MASTERFILE_SUMMARY_JSON))
    rows: list[dict[str, Any]] = []
    for source in sources:
        details = source_details.get(source["key"], {})
        source_generated_at = details.get("generated_at", generated_at)
        source_age_hours = age_hours(source_generated_at, now=now)
        if source_age_hours is None:
            freshness_status = "unknown"
        elif source_age_hours <= 48:
            freshness_status = "fresh"
        elif source_age_hours <= 168:
            freshness_status = "stale"
        else:
            freshness_status = "old"
        official = source.get("official") in {True, "true", "True", "1", 1}
        reference_scope = source["reference_scope"]
        mode = details.get("mode", source_modes.get(source["key"], "unknown"))
        if mode == "unavailable":
            refresh_priority = "P1" if official and reference_scope == "exchange_directory" else "P2"
            recommended_refresh_action = "restore_or_replace_unavailable_source_before_data_fill"
        elif freshness_status == "unknown":
            refresh_priority = "P1" if official else "P2"
            recommended_refresh_action = "capture_source_generated_at_before_refresh_decision"
        elif freshness_status == "old" and official and reference_scope == "exchange_directory":
            refresh_priority = "P1"
            recommended_refresh_action = "refresh_official_exchange_directory_before_identity_or_collision_work"
        elif freshness_status in {"old", "stale"} and official:
            refresh_priority = "P2"
            recommended_refresh_action = "refresh_official_subset_before_gap_enrichment"
        elif freshness_status in {"old", "stale"}:
            refresh_priority = "P3"
            recommended_refresh_action = "review_secondary_source_freshness_or_replace"
        else:
            refresh_priority = "P4"
            recommended_refresh_action = "no_refresh_needed"
        if recommended_refresh_action == "no_refresh_needed":
            refresh_queue = "fresh_no_refresh_needed"
        elif recommended_refresh_action == "refresh_official_exchange_directory_before_identity_or_collision_work":
            refresh_queue = "refresh_official_exchange_directory_before_identity_or_collision_work"
        elif recommended_refresh_action == "refresh_official_subset_before_gap_enrichment":
            refresh_queue = "refresh_official_subset_before_gap_enrichment"
        elif recommended_refresh_action == "restore_or_replace_unavailable_source_before_data_fill":
            refresh_queue = "restore_or_replace_unavailable_source_before_data_fill"
        elif recommended_refresh_action == "capture_source_generated_at_before_refresh_decision":
            refresh_queue = "capture_source_generated_at_before_refresh_decision"
        else:
            refresh_queue = "review_secondary_source_freshness_or_replace"
        review_strategy, evidence_required = refresh_strategy_for(refresh_queue)
        recommended_next_source = refresh_recommended_next_source_for(refresh_queue, reference_scope, mode)
        source_gate = refresh_source_gate_for(refresh_queue)
        row = {
            "key": source["key"],
            "provider": source["provider"],
            "reference_scope": reference_scope,
            "official": source["official"],
            "mode": mode,
            "rows": details.get("rows", source_counts.get(source["key"], 0)),
            "generated_at": source_generated_at,
            "last_error": details.get("last_error", ""),
            "age_hours": source_age_hours,
            "age_bucket": age_bucket_for_hours(source_age_hours),
            "freshness_status": freshness_status,
            "refresh_priority": refresh_priority,
            "refresh_queue": refresh_queue,
            "review_strategy": review_strategy,
            "evidence_required": evidence_required,
            "recommended_next_source": recommended_next_source,
            "source_gate": source_gate,
            "recommended_refresh_action": recommended_refresh_action,
        }
        row["source_artifact_context"] = source_artifact_context_for(row)
        row["freshness_review_context"] = freshness_review_context_for(row)
        row["refresh_gate_context"] = refresh_gate_context_for(row)
        rows.append(row)
    return rows


def source_artifact_context_for(row: dict[str, Any]) -> str:
    return (
        f"key={row.get('key', '') or 'none'};"
        f"provider={row.get('provider', '') or 'none'};"
        f"reference_scope={row.get('reference_scope', '') or 'none'};"
        f"official={str(row.get('official', '')).lower() or 'none'};"
        f"mode={row.get('mode', '') or 'none'};"
        f"rows={row.get('rows', 0)};"
        f"last_error={row.get('last_error', '') or 'none'}"
    )


def freshness_review_context_for(row: dict[str, Any]) -> str:
    return (
        f"generated_at={row.get('generated_at', '') or 'none'};"
        f"age_bucket={row.get('age_bucket', '') or 'none'};"
        f"freshness_status={row.get('freshness_status', '') or 'none'};"
        f"refresh_priority={row.get('refresh_priority', '') or 'none'}"
    )


def refresh_gate_context_for(row: dict[str, Any]) -> str:
    return (
        f"refresh_queue={row.get('refresh_queue', '') or 'none'};"
        f"recommended_refresh_action={row.get('recommended_refresh_action', '') or 'none'};"
        f"review_strategy={row.get('review_strategy', '') or 'none'};"
        f"evidence_required={row.get('evidence_required', '') or 'none'}"
    )


def refresh_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "fresh_no_refresh_needed":
        return ("no_refresh_required", "fresh_source_generated_at_with_age_under_48h")
    if queue == "refresh_official_exchange_directory_before_identity_or_collision_work":
        return (
            "refresh_official_exchange_directory_before_identity_or_collision_work",
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
        )
    if queue == "refresh_official_subset_before_gap_enrichment":
        return (
            "refresh_official_subset_before_gap_enrichment",
            "official_subset_refresh_artifact_with_generated_at_scope_and_row_count",
        )
    if queue == "restore_or_replace_unavailable_source_before_data_fill":
        return (
            "restore_or_replace_unavailable_source_before_data_fill",
            "source_restored_or_replaced_with_official_or_documented_unavailable_decision",
        )
    if queue == "capture_source_generated_at_before_refresh_decision":
        return (
            "capture_source_generated_at_before_refresh_decision",
            "source_generated_at_or_fetch_timestamp_required",
        )
    if queue == "review_secondary_source_freshness_or_replace":
        return (
            "review_secondary_source_freshness_or_replace",
            "reviewed_secondary_source_freshness_or_replacement_decision",
        )
    return ("manual_source_refresh_review_required", "manual_review_required")


def refresh_recommended_next_source_for(queue: str, reference_scope: str, mode: str) -> str:
    if queue == "refresh_official_exchange_directory_before_identity_or_collision_work":
        return f"Refresh the official exchange-directory source for scope {reference_scope} using mode {mode}."
    if queue == "refresh_official_subset_before_gap_enrichment":
        return f"Refresh the official subset source for scope {reference_scope} before identifier or metadata gap work."
    if queue == "restore_or_replace_unavailable_source_before_data_fill":
        return f"Restore the unavailable official source for scope {reference_scope}, or document an official replacement/unavailable decision."
    if queue == "capture_source_generated_at_before_refresh_decision":
        return f"Capture generated_at or fetch timestamp for source scope {reference_scope} before deciding freshness."
    if queue == "review_secondary_source_freshness_or_replace":
        return f"Review secondary-source freshness for scope {reference_scope}, or replace with official evidence."
    if queue == "fresh_no_refresh_needed":
        return f"No refresh needed; retain current fresh source evidence for scope {reference_scope}."
    return f"Manual source refresh review for scope {reference_scope}."


def refresh_source_gate_for(queue: str) -> str:
    if queue == "refresh_official_exchange_directory_before_identity_or_collision_work":
        return "Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated."
    if queue == "refresh_official_subset_before_gap_enrichment":
        return "Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists."
    if queue == "restore_or_replace_unavailable_source_before_data_fill":
        return "Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists."
    if queue == "capture_source_generated_at_before_refresh_decision":
        return "Do not trust freshness status until generated_at or fetch timestamp is captured."
    if queue == "review_secondary_source_freshness_or_replace":
        return "Do not use secondary freshness for canonical data unless reviewed or replaced by official evidence."
    if queue == "fresh_no_refresh_needed":
        return "Freshness evidence is present; no data change is authorized by freshness alone."
    return "Manual review required; stale or unknown freshness cannot authorize data changes."


def build_source_freshness_summary(source_rows: list[dict[str, Any]]) -> dict[str, Any]:
    old_official_exchange_directory = [
        row
        for row in source_rows
        if row.get("official") is True
        and row.get("reference_scope") == "exchange_directory"
        and row.get("freshness_status") == "old"
    ]
    refresh_strategy_totals: dict[str, Counter[str]] = {}
    refresh_evidence_totals: dict[str, Counter[str]] = {}
    refresh_batches: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    priority_order = {"P1": 1, "P2": 2, "P3": 3, "P4": 4}
    for row in source_rows:
        queue = row.get("refresh_queue", "missing") or "missing"
        strategy, evidence_required = refresh_strategy_for(str(queue))
        refresh_strategy_totals.setdefault(str(queue), Counter())[strategy] += 1
        refresh_evidence_totals.setdefault(str(queue), Counter())[evidence_required] += 1
        batch_key = (
            str(queue),
            str(row.get("reference_scope", "missing") or "missing"),
            str(row.get("mode", "missing") or "missing"),
            str(row.get("refresh_priority", "missing") or "missing"),
        )
        batch = refresh_batches.setdefault(
            batch_key,
            {
                "source_count": 0,
                "total_rows": 0,
                "max_age_hours": None,
            },
        )
        batch["source_count"] += 1
        batch["total_rows"] += int(row.get("rows") or 0)
        row_age = row.get("age_hours")
        if isinstance(row_age, (int, float)) and not isinstance(row_age, bool):
            batch["max_age_hours"] = row_age if batch["max_age_hours"] is None else max(batch["max_age_hours"], row_age)
    top_refresh_batches = []
    for (queue, scope, mode, priority), batch in sorted(
        refresh_batches.items(),
        key=lambda item: (
            priority_order.get(item[0][3], 99),
            -item[1]["source_count"],
            -item[1]["total_rows"],
            item[0][0],
            item[0][1],
            item[0][2],
        ),
    )[:25]:
        strategy, evidence_required = refresh_strategy_for(queue)
        top_refresh_batches.append(
            {
                "refresh_queue": queue,
                "reference_scope": scope,
                "mode": mode,
                "refresh_priority": priority,
                "source_count": batch["source_count"],
                "total_rows": batch["total_rows"],
                "max_age_hours": batch["max_age_hours"],
                "review_strategy": strategy,
                "evidence_required": evidence_required,
                "recommended_next_source": refresh_recommended_next_source_for(queue, scope, mode),
                "source_gate": refresh_source_gate_for(queue),
            }
        )
    return {
        "source_count": len(source_rows),
        "freshness_status_totals": dict(sorted(Counter(row.get("freshness_status", "unknown") for row in source_rows).items())),
        "source_age_bucket_totals": dict(sorted(Counter(row.get("age_bucket", "unknown_age") for row in source_rows).items())),
        "refresh_priority_totals": dict(sorted(Counter(row.get("refresh_priority", "missing") for row in source_rows).items())),
        "refresh_queue_totals": dict(sorted(Counter(row.get("refresh_queue", "missing") for row in source_rows).items())),
        "refresh_queue_scope_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row.get("reference_scope", "missing") or "missing"
                        for row in source_rows
                        if row.get("refresh_queue", "missing") == queue
                    ).items()
                )
            )
            for queue in sorted({row.get("refresh_queue", "missing") for row in source_rows})
        },
        "refresh_queue_mode_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row.get("mode", "missing") or "missing"
                        for row in source_rows
                        if row.get("refresh_queue", "missing") == queue
                    ).items()
                )
            )
            for queue in sorted({row.get("refresh_queue", "missing") for row in source_rows})
        },
        "refresh_queue_priority_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row.get("refresh_priority", "missing") or "missing"
                        for row in source_rows
                        if row.get("refresh_queue", "missing") == queue
                    ).items()
                )
            )
            for queue in sorted({row.get("refresh_queue", "missing") for row in source_rows})
        },
        "refresh_queue_age_bucket_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row.get("age_bucket", "unknown_age") or "unknown_age"
                        for row in source_rows
                        if row.get("refresh_queue", "missing") == queue
                    ).items()
                )
            )
            for queue in sorted({row.get("refresh_queue", "missing") for row in source_rows})
        },
        "recommended_refresh_action_totals": dict(sorted(Counter(row.get("recommended_refresh_action", "missing") for row in source_rows).items())),
        "refresh_queue_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(refresh_strategy_totals.items())
        },
        "refresh_queue_evidence_required_totals": {
            queue: dict(sorted(evidence_totals.items()))
            for queue, evidence_totals in sorted(refresh_evidence_totals.items())
        },
        "top_source_refresh_batches": top_refresh_batches,
        "old_official_exchange_directory_count": len(old_official_exchange_directory),
        "top_old_official_exchange_directories": [
            {
                "key": row["key"],
                "provider": row["provider"],
                "mode": row["mode"],
                "rows": row["rows"],
                "age_hours": row["age_hours"],
            }
            for row in sorted(
                old_official_exchange_directory,
                key=lambda item: (-(item.get("rows") or 0), item["key"]),
            )[:25]
        ],
    }


def build_freshness_report(
    masterfile_summary: dict[str, Any],
    identifier_summary: dict[str, Any],
    listing_daily_summary: dict[str, Any],
    stock_verification: dict[str, Any],
    etf_verification: dict[str, Any],
    symbol_changes_review: dict[str, Any] | None = None,
    review_artifacts: dict[str, tuple[dict[str, Any], Path]] | None = None,
) -> dict[str, Any]:
    now = utc_now()
    tickers_meta = load_json(TICKERS_JSON).get("_meta", {})
    stock_verification_generated_at = stock_verification.get("generated_at", "")
    etf_verification_generated_at = etf_verification.get("generated_at", "")
    identifiers_generated_at = resolve_identifiers_generated_at(identifier_summary)
    symbol_changes_review = symbol_changes_review or {}
    symbol_changes_meta = symbol_changes_review.get("_meta", {})
    symbol_changes_summary = symbol_changes_review.get("summary", {})
    symbol_changes_generated_at = (
        symbol_changes_meta.get("generated_at", "") or path_mtime_iso(SYMBOL_CHANGES_REVIEW_JSON)
    )
    freshness = {
        "tickers_built_at": tickers_meta.get("built_at", ""),
        "tickers_age_hours": age_hours(tickers_meta.get("built_at", ""), now=now),
        "masterfiles_generated_at": masterfile_summary.get("generated_at", path_mtime_iso(MASTERFILE_SUMMARY_JSON)),
        "masterfiles_age_hours": age_hours(masterfile_summary.get("generated_at", path_mtime_iso(MASTERFILE_SUMMARY_JSON)), now=now),
        "identifiers_generated_at": identifiers_generated_at,
        "identifiers_age_hours": age_hours(identifiers_generated_at, now=now),
        "listing_history_observed_at": listing_daily_summary.get("observed_at", ""),
        "listing_history_age_hours": age_hours(listing_daily_summary.get("observed_at", ""), now=now),
        "latest_verification_run": stock_verification.get("run_dir", ""),
        "latest_verification_generated_at": stock_verification_generated_at,
        "latest_verification_age_hours": age_hours(stock_verification_generated_at, now=now),
        "latest_stock_verification_run": stock_verification.get("run_dir", ""),
        "latest_stock_verification_generated_at": stock_verification_generated_at,
        "latest_stock_verification_age_hours": age_hours(stock_verification_generated_at, now=now),
        "latest_etf_verification_run": etf_verification.get("run_dir", ""),
        "latest_etf_verification_generated_at": etf_verification_generated_at,
        "latest_etf_verification_age_hours": age_hours(etf_verification_generated_at, now=now),
        "symbol_changes_generated_at": symbol_changes_generated_at,
        "symbol_changes_age_hours": age_hours(symbol_changes_generated_at, now=now),
        "symbol_changes_review_rows": int(symbol_changes_summary.get("review_rows", 0)),
    }
    for prefix, (payload, path) in sorted((review_artifacts or {}).items()):
        generated_at = payload_generated_at(payload, path)
        freshness[f"{prefix}_generated_at"] = generated_at
        freshness[f"{prefix}_age_hours"] = age_hours(generated_at, now=now)
        freshness[f"{prefix}_rows"] = payload_row_count(payload, "review_rows", "rows", "selected_rows")
    return freshness


def freshness_review_signal_rows(freshness: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label, generated_key, age_key, rows_key, gate in FRESHNESS_REVIEW_SIGNALS:
        if generated_key not in freshness and age_key not in freshness:
            continue
        rows.append(
            {
                "signal": label,
                "generated_at": freshness.get(generated_key, ""),
                "age_hours": freshness.get(age_key, ""),
                "rows": freshness.get(rows_key, "") if rows_key else "",
                "source_gate": gate,
            }
        )
    return rows


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Coverage Report",
        "",
        "## Global",
        "",
        "| Metric | Value |",
        "|---|---|",
    ]
    for key, value in report["global"].items():
        lines.append(f"| {key} | {value} |")

    lines.extend(["", "## Freshness", "", "| Metric | Value |", "|---|---|"])
    for key, value in report["freshness"].items():
        lines.append(f"| {key} | {value} |")

    lines.extend(
        [
            "",
            "## Freshness Review Summary",
            "",
            "Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.",
            "",
            "| Signal | Generated At | Age Hours | Rows | Source Gate |",
            "|---|---|---:|---:|---|",
        ]
    )
    for row in freshness_review_signal_rows(report.get("freshness", {})):
        lines.append(
            f"| {row['signal']} | {row['generated_at']} | {row['age_hours']} | {row['rows']} | {row['source_gate']} |"
        )

    source_summary = report.get("source_freshness_summary", {})
    lines.extend(["", "### Source Freshness Totals", "", "| Metric | Value |", "|---|---|"])
    for key in ("freshness_status_totals", "source_age_bucket_totals", "refresh_priority_totals", "refresh_queue_totals"):
        lines.append(f"| {key} | {json.dumps(source_summary.get(key, {}), sort_keys=True)} |")

    lines.extend(
        [
            "",
            "### Highest Priority Source Refresh Batches",
            "",
            "| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |",
            "|---|---|---|---|---:|---:|---:|---|",
        ]
    )
    for batch in source_summary.get("top_source_refresh_batches", [])[:10]:
        lines.append(
            f"| {batch['refresh_queue']} | {batch['reference_scope']} | {batch['mode']} | "
            f"{batch['refresh_priority']} | {batch['source_count']} | {batch['total_rows']} | "
            f"{batch['max_age_hours'] if batch['max_age_hours'] is not None else ''} | {batch['source_gate']} |"
        )

    lines.extend(
        [
            "",
            "## Source Coverage",
            "",
            "| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |",
            "|---|---|---|---|---|---|---:|---|---|---|---|---|---|",
        ]
    )
    for row in report["source_coverage"]:
        lines.append(
            f"| {row['key']} | {row['provider']} | {row['reference_scope']} | {row['mode']} | {row['rows']} | {row['generated_at']} | {row['age_hours'] if row['age_hours'] is not None else ''} | {row['freshness_status']} | {row['refresh_priority']} | {row.get('refresh_queue', '')} | {row['recommended_refresh_action']} | {row.get('recommended_next_source', '')} | {row.get('source_gate', '')} |"
        )
    lines.extend(["", "## Source Refresh Priority", "", "| Priority | Sources |", "|---|---:|"])
    for priority, count in report.get("source_freshness_summary", {}).get("refresh_priority_totals", {}).items():
        lines.append(f"| {priority} | {count} |")

    lines.extend(["", "## Source Refresh Queues", "", "| Queue | Sources |", "|---|---:|"])
    for queue, count in report.get("source_freshness_summary", {}).get("refresh_queue_totals", {}).items():
        lines.append(f"| {queue} | {count} |")

    lines.extend(["", "## Source Refresh Queue By Scope", "", "| Queue | Scope | Sources |", "|---|---|---:|"])
    for queue, scope_totals in report.get("source_freshness_summary", {}).get("refresh_queue_scope_totals", {}).items():
        for scope, count in scope_totals.items():
            lines.append(f"| {queue} | {scope} | {count} |")

    lines.extend(["", "## Source Refresh Queue By Mode", "", "| Queue | Mode | Sources |", "|---|---|---:|"])
    for queue, mode_totals in report.get("source_freshness_summary", {}).get("refresh_queue_mode_totals", {}).items():
        for mode, count in mode_totals.items():
            lines.append(f"| {queue} | {mode} | {count} |")

    lines.extend(["", "## Source Refresh Queue By Priority", "", "| Queue | Priority | Sources |", "|---|---|---:|"])
    for queue, priority_totals in report.get("source_freshness_summary", {}).get("refresh_queue_priority_totals", {}).items():
        for priority, count in priority_totals.items():
            lines.append(f"| {queue} | {priority} | {count} |")

    lines.extend(["", "## Source Age Buckets", "", "| Age bucket | Sources |", "|---|---:|"])
    for age_bucket, count in report.get("source_freshness_summary", {}).get("source_age_bucket_totals", {}).items():
        lines.append(f"| {age_bucket} | {count} |")

    lines.extend(["", "## Source Refresh Queue By Age Bucket", "", "| Queue | Age bucket | Sources |", "|---|---|---:|"])
    for queue, age_bucket_totals in report.get("source_freshness_summary", {}).get("refresh_queue_age_bucket_totals", {}).items():
        for age_bucket, count in age_bucket_totals.items():
            lines.append(f"| {queue} | {age_bucket} | {count} |")

    lines.extend(["", "## Source Refresh Strategies", "", "| Queue | Strategy | Sources |", "|---|---|---:|"])
    for queue, strategy_totals in report.get("source_freshness_summary", {}).get("refresh_queue_review_strategy_totals", {}).items():
        for strategy, count in strategy_totals.items():
            lines.append(f"| {queue} | {strategy} | {count} |")

    lines.extend(["", "## Source Refresh Evidence", "", "| Queue | Evidence required | Sources |", "|---|---|---:|"])
    for queue, evidence_totals in report.get("source_freshness_summary", {}).get("refresh_queue_evidence_required_totals", {}).items():
        for evidence_required, count in evidence_totals.items():
            lines.append(f"| {queue} | {evidence_required} | {count} |")

    lines.extend(
        [
            "",
            "## Top Source Refresh Batches",
            "",
            "| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---:|---:|---:|---|---|---|---|",
        ]
    )
    for batch in report.get("source_freshness_summary", {}).get("top_source_refresh_batches", []):
        lines.append(
            f"| {batch['refresh_queue']} | {batch['reference_scope']} | {batch['mode']} | "
            f"{batch['refresh_priority']} | {batch['source_count']} | {batch['total_rows']} | "
            f"{batch['max_age_hours'] if batch['max_age_hours'] is not None else ''} | "
            f"{batch['review_strategy']} | {batch['evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )

    lines.extend(
        [
            "",
            "## Exchange Coverage",
            "",
            "| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in report["exchange_coverage"]:
        lines.append(
            f"| {row['exchange']} | {row['venue_status']} | {row['tickers']} | {row['isin_coverage']} | {row['sector_coverage']} | {row['cik_coverage']} | {row['figi_coverage']} | {row['lei_coverage']} | {row['masterfile_symbols']} | {row['masterfile_matches']} | {row['masterfile_collisions']} | {row['masterfile_missing']} | {row['masterfile_match_rate'] if row['masterfile_match_rate'] is not None else ''} | {row['verification_verified_rate_on_covered'] if row['verification_verified_rate_on_covered'] is not None else ''} |"
        )

    lines.extend(["", "## Country Coverage", "", "| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |", "|---|---|---|---|---|---|---|"])
    for row in report["country_coverage"]:
        lines.append(
            f"| {row['country']} | {row['tickers']} | {row['isin_coverage']} | {row['sector_coverage']} | {row['cik_coverage']} | {row['figi_coverage']} | {row['lei_coverage']} |"
        )

    if report["gap_report"]:
        lines.extend(["", "## Unresolved Gaps", "", "| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |", "|---|---|---|---|---|---|---|"])
        for row in report["gap_report"][:20]:
            lines.append(
                f"| {row['exchange']} | {row['venue_status']} | {row['unresolved_findings']} | {row['reference_gap']} | {row['missing_from_official']} | {row['name_mismatch']} | {row['cross_exchange_collision']} |"
            )
    b3_diagnostics = report.get("b3_masterfile_diagnostics", {})
    if b3_diagnostics:
        lines.extend(
            [
                "",
                "## B3 Masterfile Diagnostics",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| Dataset rows | {b3_diagnostics['dataset_rows']} |",
                f"| Active exchange-directory rows | {b3_diagnostics['active_exchange_directory_rows']} |",
                f"| Matched dataset rows | {b3_diagnostics['matched_dataset_rows']} |",
                f"| Missing dataset rows | {b3_diagnostics['missing_dataset_rows']} |",
                f"| Dataset match rate | {b3_diagnostics['dataset_match_rate'] if b3_diagnostics['dataset_match_rate'] is not None else ''} |",
                f"| Any official B3 source matched dataset rows | {b3_diagnostics['official_any_source_matched_dataset_rows']} |",
                f"| Any official B3 source missing dataset rows | {b3_diagnostics['official_any_source_missing_dataset_rows']} |",
                f"| Any official B3 source match rate | {b3_diagnostics['official_any_source_match_rate'] if b3_diagnostics['official_any_source_match_rate'] is not None else ''} |",
                f"| Official active symbols not in dataset | {b3_diagnostics['official_active_symbols_not_in_dataset']} |",
                "",
                "### B3 Missing Categories",
                "",
                "| Category | Rows |",
                "|---|---:|",
            ]
        )
        for category, count in b3_diagnostics.get("missing_category_totals", {}).items():
            lines.append(f"| {category} | {count} |")
        lines.extend(["", "### B3 Missing Examples", "", "| Listing key | Category | Asset Type | Source Presence | Name |", "|---|---|---|---|---|"])
        for category, examples in b3_diagnostics.get("missing_examples", {}).items():
            for example in examples[:5]:
                lines.append(
                    f"| {example['listing_key']} | {category} | {example['asset_type']} | {example['source_presence']} | {example['name']} |"
                )
    return "\n".join(lines) + "\n"


def build_masterfile_collision_report(
    tickers: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
) -> dict[str, Any]:
    dataset_keys = {listing_identity(row) for row in tickers if row["ticker"] and row["exchange"]}
    dataset_exchanges_by_ticker: dict[str, set[str]] = defaultdict(set)
    seen_masterfile_keys: set[str] = set()
    for row in tickers:
        if row["ticker"] and row["exchange"]:
            dataset_exchanges_by_ticker[row["ticker"]].add(row["exchange"])

    by_exchange: dict[str, dict[str, Any]] = {}
    for row in masterfiles:
        if row.get("listing_status") != "active" or row.get("reference_scope") != "exchange_directory":
            continue
        exchange = row["exchange"]
        ticker = row["ticker"]
        key = build_listing_key(ticker, exchange)
        if key in seen_masterfile_keys:
            continue
        seen_masterfile_keys.add(key)
        stats = by_exchange.setdefault(
            exchange,
            {
                "exchange": exchange,
                "official_symbols": 0,
                "matched": 0,
                "collisions": 0,
                "missing": 0,
                "collision_examples": [],
                "missing_examples": [],
            },
        )
        stats["official_symbols"] += 1
        if key in dataset_keys:
            stats["matched"] += 1
            continue

        conflicting_exchanges = sorted(dataset_exchanges_by_ticker.get(ticker, set()) - {exchange})
        if conflicting_exchanges:
            stats["collisions"] += 1
            if len(stats["collision_examples"]) < 10:
                stats["collision_examples"].append(
                    {
                        "ticker": ticker,
                        "name": row["name"],
                        "target_exchange": exchange,
                        "existing_exchanges": conflicting_exchanges,
                    }
                )
            continue

        stats["missing"] += 1
        if len(stats["missing_examples"]) < 10:
            stats["missing_examples"].append(
                {
                    "ticker": ticker,
                    "name": row["name"],
                    "exchange": exchange,
                }
            )

    exchanges = sorted(by_exchange.values(), key=lambda row: row["exchange"])
    return {
        "global": {
            "official_symbols": sum(row["official_symbols"] for row in exchanges),
            "matched": sum(row["matched"] for row in exchanges),
            "collisions": sum(row["collisions"] for row in exchanges),
            "missing": sum(row["missing"] for row in exchanges),
        },
        "exchanges": exchanges,
    }


def build_report() -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
    tickers = load_csv(TICKERS_CSV)
    listings = load_csv(LISTINGS_CSV)
    core_listings = load_csv(CORE_LISTINGS_CSV)
    aliases = load_csv(ALIASES_CSV)
    instrument_scopes = load_csv(INSTRUMENT_SCOPES_CSV)
    identifiers_extended = load_csv(IDENTIFIERS_EXTENDED_CSV)
    masterfiles = load_csv(MASTERFILE_REFERENCE_CSV)
    listing_status_history = load_csv(LISTING_STATUS_HISTORY_CSV)
    listing_events = load_csv(LISTING_EVENTS_CSV)
    masterfile_summary = load_json(MASTERFILE_SUMMARY_JSON)
    masterfile_sources = load_json(MASTERFILE_SOURCES_JSON)
    identifier_summary = load_json(IDENTIFIER_SUMMARY_JSON)
    listing_daily_summary = load_json(DAILY_LISTING_SUMMARY_JSON)
    symbol_changes_review = load_json(SYMBOL_CHANGES_REVIEW_JSON)
    review_artifacts = {
        "entry_quality": (load_json(ENTRY_QUALITY_JSON), ENTRY_QUALITY_JSON),
        "source_gap_classification": (load_json(SOURCE_GAP_CLASSIFICATION_JSON), SOURCE_GAP_CLASSIFICATION_JSON),
        "masterfile_collision_review": (load_json(MASTERFILE_COLLISION_REVIEW_JSON), MASTERFILE_COLLISION_REVIEW_JSON),
        "ohlcv_plausibility": (load_json(OHLCV_PLAUSIBILITY_JSON), OHLCV_PLAUSIBILITY_JSON),
    }
    stock_verification = load_verification_report(find_latest_verification_run(STOCK_VERIFICATION_DIR))
    etf_verification = load_verification_report(find_latest_verification_run(ETF_VERIFICATION_DIR))

    exchange_coverage = build_exchange_report(
        listings,
        identifiers_extended,
        masterfiles,
        stock_verification_exchange_rows=stock_verification["exchange_rows"],
        etf_verification_exchange_rows=etf_verification["exchange_rows"],
    )
    masterfile_collision_report = build_masterfile_collision_report(listings, masterfiles)
    source_coverage = build_source_report(masterfile_sources, masterfile_summary)
    report = {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                key: display_path(path)
                for key, path in COVERAGE_SOURCE_FILES.items()
            },
            "policy": "Coverage and freshness report only. It does not authorize inferred identifiers, sectors, categories, names, or symbol changes.",
        },
        "global": build_global_summary(
            tickers,
            listings,
            aliases,
            instrument_scopes,
            identifiers_extended,
            listing_status_history,
            listing_events,
            exchange_coverage,
            stock_verification_summary=stock_verification["summary"],
            etf_verification_summary=etf_verification["summary"],
            core_listings=core_listings,
        ),
        "freshness": build_freshness_report(
            masterfile_summary,
            identifier_summary,
            listing_daily_summary,
            stock_verification,
            etf_verification,
            symbol_changes_review,
            review_artifacts,
        ),
        "source_coverage": source_coverage,
        "source_freshness_summary": build_source_freshness_summary(source_coverage),
        "exchange_coverage": exchange_coverage,
        "by_exchange": exchange_coverage,
        "country_coverage": build_country_report(tickers, identifiers_extended),
        "verification": {
            "run_dir": stock_verification["run_dir"],
            "generated_at": stock_verification["generated_at"],
            "summary": stock_verification["summary"],
            "exchange_coverage": stock_verification["exchange_rows"],
        },
        "stock_verification": {
            "run_dir": stock_verification["run_dir"],
            "generated_at": stock_verification["generated_at"],
            "summary": stock_verification["summary"],
            "exchange_coverage": stock_verification["exchange_rows"],
        },
        "etf_verification": {
            "run_dir": etf_verification["run_dir"],
            "generated_at": etf_verification["generated_at"],
            "summary": etf_verification["summary"],
            "exchange_coverage": etf_verification["exchange_rows"],
        },
        "gap_report": build_gap_report(exchange_coverage, stock_verification["exchange_rows"], etf_verification["exchange_rows"]),
        "b3_gap_breakdown": build_b3_gap_breakdown(stock_verification["rows"]),
        "b3_masterfile_diagnostics": build_b3_masterfile_diagnostics(listings, masterfiles),
    }

    COVERAGE_REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    COVERAGE_REPORT_MD.write_text(render_markdown(report), encoding="utf-8")
    MASTERFILE_COLLISION_REPORT_JSON.write_text(
        json.dumps(masterfile_collision_report, indent=2),
        encoding="utf-8",
    )
    return report


def main() -> None:
    report = build_report()
    print(json.dumps(report["global"], indent=2))


if __name__ == "__main__":
    main()
