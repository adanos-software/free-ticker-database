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
) -> dict[str, Any]:
    venue_status_counts = Counter(row["venue_status"] for row in exchange_coverage)
    instrument_scope_counts = Counter(row["instrument_scope"] for row in (instrument_scopes or []))
    scope_reason_counts = Counter(row["scope_reason"] for row in (instrument_scopes or []))
    summary = {
        "tickers": len(tickers),
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
    source_details = masterfile_summary.get("source_details", {})
    source_counts = masterfile_summary.get("source_counts", {})
    source_modes = masterfile_summary.get("source_modes", {})
    generated_at = masterfile_summary.get("generated_at", path_mtime_iso(MASTERFILE_SUMMARY_JSON))
    rows: list[dict[str, Any]] = []
    for source in sources:
        details = source_details.get(source["key"], {})
        rows.append(
            {
                "key": source["key"],
                "provider": source["provider"],
                "reference_scope": source["reference_scope"],
                "official": source["official"],
                "mode": details.get("mode", source_modes.get(source["key"], "unknown")),
                "rows": details.get("rows", source_counts.get(source["key"], 0)),
                "generated_at": details.get("generated_at", generated_at),
            }
        )
    return rows


def build_freshness_report(
    masterfile_summary: dict[str, Any],
    identifier_summary: dict[str, Any],
    listing_daily_summary: dict[str, Any],
    stock_verification: dict[str, Any],
    etf_verification: dict[str, Any],
) -> dict[str, Any]:
    now = utc_now()
    tickers_meta = load_json(TICKERS_JSON).get("_meta", {})
    stock_verification_generated_at = stock_verification.get("generated_at", "")
    etf_verification_generated_at = etf_verification.get("generated_at", "")
    identifiers_generated_at = resolve_identifiers_generated_at(identifier_summary)
    return {
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
    }


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
            "## Source Coverage",
            "",
            "| Source | Provider | Scope | Mode | Rows | Generated At |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in report["source_coverage"]:
        lines.append(
            f"| {row['key']} | {row['provider']} | {row['reference_scope']} | {row['mode']} | {row['rows']} | {row['generated_at']} |"
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
    tickers = load_csv(TICKERS_CSV)
    listings = load_csv(LISTINGS_CSV)
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
    report = {
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
        ),
        "freshness": build_freshness_report(
            masterfile_summary,
            identifier_summary,
            listing_daily_summary,
            stock_verification,
            etf_verification,
        ),
        "source_coverage": build_source_report(masterfile_sources, masterfile_summary),
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
