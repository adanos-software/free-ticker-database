from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

try:
    from scripts.rebuild_dataset import (
        load_review_overrides,
        normalize_tokens,
        normalized_compact,
        should_exclude_stock_row,
    )
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import load_review_overrides, normalize_tokens, normalized_compact, should_exclude_stock_row

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_SUPPLEMENT_SUMMARY_JSON = DATA_DIR / "masterfiles" / "supplemental_summary.json"
COVERAGE_EXPANSION_CSV = DATA_DIR / "coverage_expansion_listings.csv"
COVERAGE_EXPANSION_FIELDS = [
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

SUPPLEMENT_EXCHANGES: dict[str, dict[str, str]] = {
    "AMS": {
        "country": "Netherlands",
        "country_code": "NL",
    },
    "ASX": {
        "country": "Australia",
        "country_code": "AU",
    },
    "ADX": {
        "country": "United Arab Emirates",
        "country_code": "AE",
    },
    "B3": {
        "country": "Brazil",
        "country_code": "BR",
    },
    "BVB": {
        "country": "Romania",
        "country_code": "RO",
    },
    "BHB": {
        "country": "Bahrain",
        "country_code": "BH",
    },
    "BIST": {
        "country": "Turkey",
        "country_code": "TR",
    },
    "BK": {
        "country": "Kuwait",
        "country_code": "KW",
    },
    "BSE_IN": {
        "country": "India",
        "country_code": "IN",
    },
    "CSE_LK": {
        "country": "Sri Lanka",
        "country_code": "LK",
    },
    "CSE_MA": {
        "country": "Morocco",
        "country_code": "MA",
    },
    "DFM": {
        "country": "United Arab Emirates",
        "country_code": "AE",
    },
    "HKEX": {
        "country": "Hong Kong",
        "country_code": "HK",
    },
    "HNX": {
        "country": "Vietnam",
        "country_code": "VN",
    },
    "MSX": {
        "country": "Oman",
        "country_code": "OM",
    },
    "NSE_IN": {
        "country": "India",
        "country_code": "IN",
    },
    "NSE_KE": {
        "country": "Kenya",
        "country_code": "KE",
    },
    "PSE": {
        "country": "Philippines",
        "country_code": "PH",
    },
    "NZX": {
        "country": "New Zealand",
        "country_code": "NZ",
    },
    "OSL": {
        "country": "Norway",
        "country_code": "NO",
    },
    "QSE": {
        "country": "Qatar",
        "country_code": "QA",
    },
    "SGX": {
        "country": "Singapore",
        "country_code": "SG",
    },
    "TADAWUL": {
        "country": "Saudi Arabia",
        "country_code": "SA",
    },
    "TSX": {
        "country": "Canada",
        "country_code": "CA",
    },
    "TSXV": {
        "country": "Canada",
        "country_code": "CA",
    },
    "TSE": {
        "country": "Japan",
        "country_code": "JP",
    },
    "TWSE": {
        "country": "Taiwan",
        "country_code": "TW",
    },
    "XETRA": {
        "country": "Germany",
        "country_code": "DE",
    },
    "FSX": {
        "country": "Germany",
        "country_code": "DE",
    },
}

# Frankfurt's official T7 directory lists thousands of unique-mnemonic foreign
# dual listings. Keep those rows for coverage/status matching, but do not expand
# the public FSX universe from them.
SUPPLEMENT_REFRESH_ONLY_EXCHANGES = {"FSX"}

SUPPLEMENT_EXCLUDED_STOCK_PATTERNS = [
    re.compile(r"\babs trust\b", re.IGNORECASE),
    re.compile(r"(?:^|[\s-])drs(?:$|[\s-])", re.IGNORECASE),
]

SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE: dict[str, set[str]] = {
    "TSX": {"listed_companies_subset"},
    "TSXV": {"listed_companies_subset"},
    "XETRA": {"exchange_directory", "listed_companies_subset"},
    "FSX": {"exchange_directory"},
}
LOCAL_LANGUAGE_REFRESH_EXCHANGES = {"TWSE", "TPEX"}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def official_isin(row: dict[str, str]) -> str:
    return (row.get("isin") or "").strip().upper()


def append_coverage_expansion_rows(rows: list[dict[str, str]]) -> int:
    if not rows:
        return 0
    existing = load_csv(COVERAGE_EXPANSION_CSV) if COVERAGE_EXPANSION_CSV.exists() else []
    seen = {
        row.get("listing_key") or f"{row['exchange']}::{row['ticker']}"
        for row in existing
    }
    added: list[dict[str, str]] = []
    for row in rows:
        listing_key = row.get("listing_key") or f"{row['exchange']}::{row['ticker']}"
        if listing_key in seen:
            continue
        seen.add(listing_key)
        added.append({field: row.get(field, "") for field in COVERAGE_EXPANSION_FIELDS})
        added[-1]["listing_key"] = listing_key
    if not added:
        return 0
    raw = COVERAGE_EXPANSION_CSV.read_bytes() if COVERAGE_EXPANSION_CSV.exists() else b""
    newline = "\r\n" if raw.endswith(b"\r\n") else "\n"
    prefix = "" if not raw or raw.endswith(newline.encode("utf-8")) else newline
    with COVERAGE_EXPANSION_CSV.open("a", encoding="utf-8", newline="") as handle:
        if prefix:
            handle.write(prefix)
        writer = csv.DictWriter(
            handle,
            fieldnames=COVERAGE_EXPANSION_FIELDS,
            extrasaction="ignore",
            lineterminator=newline,
        )
        if not raw:
            writer.writeheader()
        writer.writerows(added)
    return len(added)


def build_supplement_rows(
    core_rows: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    dropped_keys: set[tuple[str, str]] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    dropped_keys = dropped_keys or set()
    core_exchanges_by_ticker: dict[str, set[str]] = {}
    core_rows_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in core_rows:
        row_key = (row["ticker"], row["exchange"])
        if row_key in dropped_keys:
            continue
        core_exchanges_by_ticker.setdefault(row["ticker"], set()).add(row["exchange"])
        core_rows_by_key[row_key] = row

    supplements: list[dict[str, str]] = []
    coverage_expansion_rows: list[dict[str, str]] = []
    eligible_by_ticker: dict[str, list[dict[str, str]]] = {}
    for row in masterfile_rows:
        exchange = row["exchange"]
        if exchange not in SUPPLEMENT_EXCHANGES:
            continue
        if row.get("listing_status") != "active":
            continue
        allowed_reference_scopes = SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE.get(
            exchange,
            {"exchange_directory"},
        )
        if row.get("reference_scope") not in allowed_reference_scopes:
            continue
        eligible_by_ticker.setdefault(row["ticker"], []).append(row)

    summary: dict[str, Any] = {
        "supplement_rows": 0,
        "safe_missing_rows": 0,
        "refreshable_existing_rows": 0,
        "colliding_rows_skipped": 0,
        "refresh_only_missing_rows_skipped": 0,
        "coverage_expansion_missing_rows": 0,
        "by_exchange": {},
        "coverage_expansion_rows": [],
    }

    seen: set[tuple[str, str]] = set()

    def exchange_stats(exchange: str) -> dict[str, int]:
        return summary["by_exchange"].setdefault(
            exchange,
            {
                "safe_missing_rows": 0,
                "refreshable_existing_rows": 0,
                "colliding_rows_skipped": 0,
            },
        )

    def skip_collision(exchange: str) -> None:
        summary["colliding_rows_skipped"] += 1
        exchange_stats(exchange)["colliding_rows_skipped"] += 1

    def make_candidate(row: dict[str, str]) -> dict[str, str] | None:
        exchange = row["exchange"]
        exchange_meta = SUPPLEMENT_EXCHANGES[exchange]
        candidate = {
            "ticker": row["ticker"],
            "name": row["name"],
            "exchange": exchange,
            "asset_type": row["asset_type"],
            "sector": row.get("sector", ""),
            "country": exchange_meta["country"],
            "country_code": exchange_meta["country_code"],
            "isin": official_isin(row),
            "aliases": "",
            "source_key": row.get("source_key", ""),
            "source_url": row.get("source_url", ""),
            "reference_scope": row.get("reference_scope", ""),
        }
        if candidate["asset_type"] == "Stock" and any(
            pattern.search(candidate["name"]) for pattern in SUPPLEMENT_EXCLUDED_STOCK_PATTERNS
        ):
            return None
        if should_exclude_stock_row(candidate):
            return None
        return candidate

    def as_coverage_row(candidate: dict[str, str]) -> dict[str, str]:
        return {
            "listing_key": f"{candidate['exchange']}::{candidate['ticker']}",
            "ticker": candidate["ticker"],
            "exchange": candidate["exchange"],
            "name": candidate["name"],
            "asset_type": candidate["asset_type"],
            "stock_sector": candidate.get("sector", ""),
            "etf_category": "",
            "country": candidate.get("country", ""),
            "country_code": candidate.get("country_code", ""),
            "isin": candidate.get("isin", ""),
            "aliases": candidate.get("aliases", ""),
        }

    for row in masterfile_rows:
        exchange = row["exchange"]
        if exchange not in SUPPLEMENT_EXCHANGES:
            continue
        if row.get("listing_status") != "active":
            continue
        allowed_reference_scopes = SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE.get(
            exchange,
            {"exchange_directory"},
        )
        if row.get("reference_scope") not in allowed_reference_scopes:
            continue

        ticker = row["ticker"]
        key = (ticker, exchange)
        if key in seen:
            continue
        stats = exchange_stats(exchange)
        core_exchanges = core_exchanges_by_ticker.get(ticker, set())
        existing_row = core_rows_by_key.get(key)

        if exchange in SUPPLEMENT_REFRESH_ONLY_EXCHANGES and existing_row is None:
            summary["refresh_only_missing_rows_skipped"] += 1
            stats["refresh_only_missing_rows_skipped"] = stats.get("refresh_only_missing_rows_skipped", 0) + 1
            continue

        candidate = make_candidate(row)
        if candidate is None:
            skip_collision(exchange)
            continue

        if existing_row:
            if not rows_refer_to_same_entity(existing_row, row):
                skip_collision(exchange)
                continue
            seen.add(key)
            summary["refreshable_existing_rows"] += 1
            stats["refreshable_existing_rows"] += 1
            supplements.append(candidate)
            continue

        if exchange == "B3" and candidate["asset_type"] == "ETF":
            skip_collision(exchange)
            continue

        peers = eligible_by_ticker.get(ticker, [])
        peer_isins = {official_isin(peer) for peer in peers if official_isin(peer)}
        peer_exchanges = {peer["exchange"] for peer in peers}

        if core_exchanges:
            core_isins = {
                official_isin(core_rows_by_key[(ticker, core_exchange)])
                for core_exchange in core_exchanges
                if (ticker, core_exchange) in core_rows_by_key
            }
            core_isins.discard("")
            master_isin = official_isin(row)
            # Only attach a new venue when every existing same-ticker row is the
            # same instrument. Mixed ISINs on one ticker are homonyms, not duals.
            if master_isin and core_isins == {master_isin}:
                seen.add(key)
                summary["safe_missing_rows"] += 1
                stats["safe_missing_rows"] += 1
                supplements.append(candidate)
                continue
            skip_collision(exchange)
            continue

        if len(peer_exchanges) > 1:
            shared_isin = next(iter(peer_isins), "") if len(peer_isins) == 1 else ""
            if shared_isin and all(official_isin(peer) == shared_isin for peer in peers):
                seen.add(key)
                summary["safe_missing_rows"] += 1
                stats["safe_missing_rows"] += 1
                supplements.append(candidate)
                continue
            if len(peer_isins) >= 2:
                seen.add(key)
                coverage_expansion_rows.append(as_coverage_row(candidate))
                continue
            skip_collision(exchange)
            continue

        seen.add(key)
        summary["safe_missing_rows"] += 1
        stats["safe_missing_rows"] += 1
        supplements.append(candidate)

    supplements.sort(key=lambda row: (row["exchange"], row["ticker"]))
    summary["supplement_rows"] = len(supplements)
    summary["coverage_expansion_missing_rows"] = len(coverage_expansion_rows)
    summary["coverage_expansion_rows"] = coverage_expansion_rows
    return supplements, summary


def rows_refer_to_same_entity(core_row: dict[str, str], masterfile_row: dict[str, str]) -> bool:
    core_isin = (core_row.get("isin") or "").strip()
    masterfile_isin = (masterfile_row.get("isin") or "").strip()
    if core_isin and masterfile_isin:
        return core_isin == masterfile_isin

    core_name = (core_row.get("name") or "").strip()
    masterfile_name = (masterfile_row.get("name") or "").strip()
    if not core_name or not masterfile_name:
        return True

    if (
        core_row.get("exchange") == masterfile_row.get("exchange")
        and core_row.get("exchange") in LOCAL_LANGUAGE_REFRESH_EXCHANGES
        and any(ord(character) > 127 and character.isalpha() for character in masterfile_name)
    ):
        return True

    core_compact = normalized_compact(core_name)
    masterfile_compact = normalized_compact(masterfile_name)
    if core_compact and masterfile_compact and (
        core_compact in masterfile_compact or masterfile_compact in core_compact
    ):
        return True

    core_tokens = normalize_tokens(core_name)
    masterfile_tokens = normalize_tokens(masterfile_name)
    if not core_tokens or not masterfile_tokens:
        return False

    overlap = len(core_tokens & masterfile_tokens)
    return overlap >= 2 and overlap / min(len(core_tokens), len(masterfile_tokens)) >= 0.5


def main() -> dict[str, Any]:
    core_rows = load_csv(LISTINGS_CSV)
    masterfile_rows = load_csv(MASTERFILE_REFERENCE_CSV)
    _, _, drop_entries = load_review_overrides()
    supplement_rows, summary = build_supplement_rows(core_rows, masterfile_rows, drop_entries)
    fieldnames = [
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
    write_csv(MASTERFILE_SUPPLEMENT_CSV, fieldnames, supplement_rows)
    expansion_rows = list(summary.pop("coverage_expansion_rows", []))
    summary["coverage_expansion_rows_appended"] = append_coverage_expansion_rows(expansion_rows)
    MASTERFILE_SUPPLEMENT_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    main()
