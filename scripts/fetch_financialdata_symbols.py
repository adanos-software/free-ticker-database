from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import alias_matches_company

DATA_DIR = ROOT / "data"
FINANCIALDATA_DIR = DATA_DIR / "financialdata"
REPORTS_DIR = DATA_DIR / "reports"
LISTINGS_CSV = DATA_DIR / "listings.csv"

DEFAULT_ENDPOINT = "https://financialdata.net/api/v1/international-stock-symbols"
DEFAULT_SYMBOLS_CSV = FINANCIALDATA_DIR / "international_stock_symbols.csv"
DEFAULT_SYMBOLS_JSON = FINANCIALDATA_DIR / "international_stock_symbols.json"
DEFAULT_REPORT_CSV = REPORTS_DIR / "financialdata_symbol_match.csv"
DEFAULT_REPORT_JSON = REPORTS_DIR / "financialdata_symbol_match.json"
DEFAULT_REPORT_MD = REPORTS_DIR / "financialdata_symbol_match.md"
DEFAULT_CURRENT_GAPS_CSV = REPORTS_DIR / "financialdata_current_exchange_gaps.csv"
DEFAULT_GLOBAL_EXPANSION_CSV = REPORTS_DIR / "financialdata_global_expansion_candidates.csv"

PAGE_SIZE = 500
MAX_REQUESTS = 300

SYMBOL_FIELDS = [
    "financialdata_symbol",
    "base_ticker",
    "suffix",
    "mapped_exchange",
    "registrant_name",
    "source_confidence",
    "review_needed",
    "observed_at",
]
MATCH_FIELDS = [
    *SYMBOL_FIELDS,
    "match_status",
    "recommended_action",
    "current_universe",
    "review_scope",
    "quality_gate",
    "listing_keys",
    "matched_names",
    "same_ticker_listing_keys",
    "same_ticker_exchanges",
    "name_match_count",
    "name_mismatch_count",
]

SUFFIX_EXCHANGE_MAP = {
    ".AS": "AMS",
    ".BO": "BSE_IN",
    ".DE": "XETRA",
    ".F": "XETRA",
    ".HK": "HKEX",
    ".JK": "IDX",
    ".KL": "Bursa",
    ".KQ": "KOSDAQ",
    ".KS": "KRX",
    ".L": "LSE",
    ".MX": "BMV",
    ".NE": "NEO",
    ".NS": "NSE_IN",
    ".PA": "Euronext",
    ".SA": "B3",
    ".SI": "SGX",
    ".SS": "SSE",
    ".SZ": "SZSE",
    ".T": "TSE",
    ".TO": "TSX",
    ".V": "TSXV",
}


@dataclass(frozen=True)
class FinancialDataSymbol:
    financialdata_symbol: str
    base_ticker: str
    suffix: str
    mapped_exchange: str
    registrant_name: str
    source_confidence: str
    review_needed: str
    observed_at: str


@dataclass(frozen=True)
class FinancialDataMatch:
    symbol: FinancialDataSymbol
    match_status: str
    recommended_action: str
    current_universe: str
    review_scope: str
    quality_gate: str
    listing_keys: str
    matched_names: str
    same_ticker_listing_keys: str
    same_ticker_exchanges: str
    name_match_count: int
    name_mismatch_count: int


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace(" ", "")


def split_financialdata_symbol(symbol: str) -> tuple[str, str, str]:
    normalized = normalize_symbol(symbol)
    if "." not in normalized:
        return normalized, "", ""
    base, raw_suffix = normalized.rsplit(".", 1)
    suffix = f".{raw_suffix}"
    return base, suffix, SUFFIX_EXCHANGE_MAP.get(suffix, "")


def normalize_api_row(row: dict[str, Any], *, observed_at: str) -> FinancialDataSymbol:
    symbol = str(row.get("trading_symbol", "")).strip()
    base_ticker, suffix, mapped_exchange = split_financialdata_symbol(symbol)
    return FinancialDataSymbol(
        financialdata_symbol=normalize_symbol(symbol),
        base_ticker=base_ticker,
        suffix=suffix,
        mapped_exchange=mapped_exchange,
        registrant_name=" ".join(str(row.get("registrant_name", "")).split()),
        source_confidence="secondary_review",
        review_needed="true",
        observed_at=observed_at,
    )


def extract_api_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "symbols"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    raise ValueError("FinancialData response did not contain a JSON array of symbol rows")


def fetch_page(
    *,
    endpoint: str,
    api_key: str,
    offset: int,
    timeout_seconds: float,
    session: requests.Session,
) -> list[dict[str, Any]]:
    response = session.get(
        endpoint,
        params={"offset": offset, "format": "json", "key": api_key},
        headers={"User-Agent": "free-ticker-database/financialdata-symbol-sync"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return extract_api_rows(response.json())


def fetch_all_symbols(
    *,
    endpoint: str,
    api_key: str,
    page_size: int,
    max_requests: int,
    timeout_seconds: float,
    sleep_seconds: float,
    observed_at: str,
    session: requests.Session | None = None,
) -> tuple[list[FinancialDataSymbol], dict[str, Any]]:
    if page_size != PAGE_SIZE:
        raise ValueError("FinancialData international-stock-symbols pages are fixed at 500 rows; use page_size=500")
    if max_requests > MAX_REQUESTS:
        raise ValueError("max_requests must not exceed the configured daily API safety cap of 300")

    active_session = session or requests.Session()
    symbols: list[FinancialDataSymbol] = []
    requests_used = 0
    truncated_by_request_limit = False

    for request_index in range(max_requests):
        offset = request_index * page_size
        rows = fetch_page(
            endpoint=endpoint,
            api_key=api_key,
            offset=offset,
            timeout_seconds=timeout_seconds,
            session=active_session,
        )
        requests_used += 1
        symbols.extend(normalize_api_row(row, observed_at=observed_at) for row in rows)
        if len(rows) < page_size:
            break
        if request_index == max_requests - 1:
            truncated_by_request_limit = True
            break
        if sleep_seconds:
            time.sleep(sleep_seconds)

    summary = {
        "endpoint": endpoint,
        "page_size": page_size,
        "max_requests": max_requests,
        "requests_used": requests_used,
        "raw_rows": len(symbols),
        "truncated_by_request_limit": truncated_by_request_limit,
    }
    return dedupe_symbols(symbols), summary


def dedupe_symbols(symbols: Iterable[FinancialDataSymbol]) -> list[FinancialDataSymbol]:
    latest: dict[str, FinancialDataSymbol] = {}
    for symbol in symbols:
        if not symbol.financialdata_symbol:
            continue
        latest[symbol.financialdata_symbol] = symbol
    return sorted(latest.values(), key=lambda row: (row.mapped_exchange, row.base_ticker, row.financialdata_symbol))


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def is_stock_listing(row: dict[str, str]) -> bool:
    asset_type = row.get("asset_type", "").strip()
    return not asset_type or asset_type == "Stock"


def listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def build_listing_indexes(
    listings_rows: list[dict[str, str]],
) -> tuple[dict[tuple[str, str], list[dict[str, str]]], dict[str, list[dict[str, str]]], set[str]]:
    by_exchange_ticker: dict[tuple[str, str], list[dict[str, str]]] = {}
    by_ticker: dict[str, list[dict[str, str]]] = {}
    current_exchanges: set[str] = set()
    for row in listings_rows:
        if not is_stock_listing(row):
            continue
        ticker = normalize_symbol(row.get("ticker", ""))
        exchange = row.get("exchange", "").strip()
        if not ticker or not exchange:
            continue
        current_exchanges.add(exchange)
        by_exchange_ticker.setdefault((exchange, ticker), []).append(row)
        by_ticker.setdefault(ticker, []).append(row)
    return by_exchange_ticker, by_ticker, current_exchanges


def name_matches(registrant_name: str, listing_name: str) -> bool:
    if not registrant_name or not listing_name:
        return False
    return alias_matches_company(registrant_name, listing_name) or alias_matches_company(listing_name, registrant_name)


def join_values(values: Iterable[str]) -> str:
    return "|".join(sorted(value for value in values if value))


def review_scope_for(match_status: str, *, current_universe: bool) -> str:
    if match_status == "matched_exchange_name_ok":
        return "secondary_reference"
    if match_status == "matched_exchange_name_mismatch":
        return "name_mismatch_review"
    if match_status == "ticker_present_other_exchange":
        return "current_exchange_mapping_review" if current_universe else "global_expansion_symbol_collision"
    if match_status == "missing_from_database":
        return "current_exchange_gap" if current_universe else "global_expansion_candidate"
    if match_status == "unmapped_suffix":
        return "suffix_mapping_review"
    return "ignored_invalid_symbol"


def quality_gate_for(match_status: str, *, current_universe: bool) -> str:
    if match_status == "matched_exchange_name_ok":
        return "usable_secondary_reference"
    if match_status == "matched_exchange_name_mismatch":
        return "blocked_name_mismatch"
    if match_status == "ticker_present_other_exchange":
        return "review_exchange_mapping_before_use"
    if match_status == "missing_from_database":
        return "needs_official_source_or_isin" if current_universe else "needs_venue_onboarding_and_isin"
    if match_status == "unmapped_suffix":
        return "needs_suffix_mapping"
    return "ignore"


def build_match(
    *,
    symbol: FinancialDataSymbol,
    match_status: str,
    recommended_action: str,
    current_exchanges: set[str],
    listing_keys: str = "",
    matched_names: str = "",
    same_ticker_listing_keys: str = "",
    same_ticker_exchanges: str = "",
    name_match_count: int = 0,
    name_mismatch_count: int = 0,
) -> FinancialDataMatch:
    current_universe = bool(symbol.mapped_exchange and symbol.mapped_exchange in current_exchanges)
    return FinancialDataMatch(
        symbol=symbol,
        match_status=match_status,
        recommended_action=recommended_action,
        current_universe="true" if current_universe else "false",
        review_scope=review_scope_for(match_status, current_universe=current_universe),
        quality_gate=quality_gate_for(match_status, current_universe=current_universe),
        listing_keys=listing_keys,
        matched_names=matched_names,
        same_ticker_listing_keys=same_ticker_listing_keys,
        same_ticker_exchanges=same_ticker_exchanges,
        name_match_count=name_match_count,
        name_mismatch_count=name_mismatch_count,
    )


def evaluate_symbol(
    symbol: FinancialDataSymbol,
    by_exchange_ticker: dict[tuple[str, str], list[dict[str, str]]],
    by_ticker: dict[str, list[dict[str, str]]],
    current_exchanges: set[str],
) -> FinancialDataMatch:
    same_ticker_rows = by_ticker.get(symbol.base_ticker, []) if symbol.base_ticker else []
    same_exchange_rows = (
        by_exchange_ticker.get((symbol.mapped_exchange, symbol.base_ticker), [])
        if symbol.mapped_exchange and symbol.base_ticker
        else []
    )

    if not symbol.base_ticker:
        match_status = "invalid_symbol"
        recommended_action = "ignore_invalid_symbol"
    elif not symbol.mapped_exchange:
        match_status = "unmapped_suffix"
        recommended_action = "extend_suffix_mapping_before_review"
    elif same_exchange_rows:
        name_match_count = sum(name_matches(symbol.registrant_name, row.get("name", "")) for row in same_exchange_rows)
        name_mismatch_count = len(same_exchange_rows) - name_match_count
        if name_match_count:
            match_status = "matched_exchange_name_ok"
            recommended_action = "keep_as_secondary_reference"
        else:
            match_status = "matched_exchange_name_mismatch"
            recommended_action = "review_name_mismatch_before_any_update"
        return build_match(
            symbol=symbol,
            match_status=match_status,
            recommended_action=recommended_action,
            current_exchanges=current_exchanges,
            listing_keys=join_values(listing_key(row) for row in same_exchange_rows),
            matched_names=join_values(row.get("name", "") for row in same_exchange_rows),
            same_ticker_listing_keys=join_values(listing_key(row) for row in same_ticker_rows),
            same_ticker_exchanges=join_values(row.get("exchange", "") for row in same_ticker_rows),
            name_match_count=name_match_count,
            name_mismatch_count=name_mismatch_count,
        )
    elif same_ticker_rows:
        match_status = "ticker_present_other_exchange"
        recommended_action = "review_exchange_mapping_or_cross_listing"
    else:
        match_status = "missing_from_database"
        recommended_action = "review_missing_listing_no_isin_from_secondary_source"

    return build_match(
        symbol=symbol,
        match_status=match_status,
        recommended_action=recommended_action,
        current_exchanges=current_exchanges,
        listing_keys="",
        matched_names="",
        same_ticker_listing_keys=join_values(listing_key(row) for row in same_ticker_rows),
        same_ticker_exchanges=join_values(row.get("exchange", "") for row in same_ticker_rows),
        name_match_count=0,
        name_mismatch_count=0,
    )


def build_match_report(
    symbols: list[FinancialDataSymbol],
    listings_rows: list[dict[str, str]],
) -> list[FinancialDataMatch]:
    by_exchange_ticker, by_ticker, current_exchanges = build_listing_indexes(listings_rows)
    return [evaluate_symbol(symbol, by_exchange_ticker, by_ticker, current_exchanges) for symbol in symbols]


def symbol_to_dict(symbol: FinancialDataSymbol) -> dict[str, str]:
    return asdict(symbol)


def match_to_dict(match: FinancialDataMatch) -> dict[str, Any]:
    return {
        **symbol_to_dict(match.symbol),
        "match_status": match.match_status,
        "recommended_action": match.recommended_action,
        "current_universe": match.current_universe,
        "review_scope": match.review_scope,
        "quality_gate": match.quality_gate,
        "listing_keys": match.listing_keys,
        "matched_names": match.matched_names,
        "same_ticker_listing_keys": match.same_ticker_listing_keys,
        "same_ticker_exchanges": match.same_ticker_exchanges,
        "name_match_count": match.name_match_count,
        "name_mismatch_count": match.name_mismatch_count,
    }


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def summarize(symbols: list[FinancialDataSymbol], matches: list[FinancialDataMatch], fetch_summary: dict[str, Any]) -> dict[str, Any]:
    status_counts = Counter(match.match_status for match in matches)
    action_counts = Counter(match.recommended_action for match in matches)
    scope_counts = Counter(match.review_scope for match in matches)
    gate_counts = Counter(match.quality_gate for match in matches)
    exchange_counts = Counter(symbol.mapped_exchange or "UNMAPPED" for symbol in symbols)
    missing_by_exchange = Counter(
        match.symbol.mapped_exchange or "UNMAPPED"
        for match in matches
        if match.match_status == "missing_from_database"
    )
    current_missing_by_exchange = Counter(
        match.symbol.mapped_exchange
        for match in matches
        if match.review_scope == "current_exchange_gap"
    )
    global_expansion_by_exchange = Counter(
        match.symbol.mapped_exchange
        for match in matches
        if match.review_scope == "global_expansion_candidate"
    )
    return {
        "generated_at": symbols[0].observed_at if symbols else utc_now(),
        **fetch_summary,
        "deduped_rows": len(symbols),
        "mapped_rows": sum(bool(symbol.mapped_exchange) for symbol in symbols),
        "unmapped_rows": sum(not symbol.mapped_exchange for symbol in symbols),
        "match_status_counts": dict(sorted(status_counts.items())),
        "recommended_action_counts": dict(sorted(action_counts.items())),
        "review_scope_counts": dict(sorted(scope_counts.items())),
        "quality_gate_counts": dict(sorted(gate_counts.items())),
        "mapped_exchange_counts": dict(sorted(exchange_counts.items())),
        "missing_from_database_by_exchange": dict(missing_by_exchange.most_common()),
        "current_exchange_gaps": scope_counts["current_exchange_gap"],
        "current_exchange_gaps_by_exchange": dict(current_missing_by_exchange.most_common()),
        "global_expansion_candidates": scope_counts["global_expansion_candidate"],
        "global_expansion_candidates_by_exchange": dict(global_expansion_by_exchange.most_common()),
    }


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# FinancialData Symbol Match",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "FinancialData.net international stock symbols are treated as secondary review signals. ",
        "They do not include ISIN or sector data and must not be applied as official exchange masterfile rows.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Requests used | {summary['requests_used']} |",
        f"| Raw rows | {summary['raw_rows']} |",
        f"| Deduped rows | {summary['deduped_rows']} |",
        f"| Mapped rows | {summary['mapped_rows']} |",
        f"| Unmapped rows | {summary['unmapped_rows']} |",
        f"| Current-exchange gaps | {summary['current_exchange_gaps']} |",
        f"| Global expansion candidates | {summary['global_expansion_candidates']} |",
        f"| Truncated by request limit | {summary['truncated_by_request_limit']} |",
        "",
        "## Match Status",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["match_status_counts"].items():
        lines.append(f"| {status} | {count} |")
    lines.extend(["", "## Review Scope", "", "| Scope | Rows |", "|---|---:|"])
    for scope, count in summary["review_scope_counts"].items():
        lines.append(f"| {scope} | {count} |")
    lines.extend(["", "## Current-Exchange Gaps", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["current_exchange_gaps_by_exchange"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## Global Expansion Candidates", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["global_expansion_candidates_by_exchange"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `matched_exchange_name_ok` rows can be used as secondary coverage evidence only.",
            "- `matched_exchange_name_mismatch` rows need manual/source review before any metadata change.",
            "- `current_exchange_gap` rows need an official source or ISIN-bearing source before core insertion.",
            "- `global_expansion_candidate` rows indicate missing venue coverage and are not current-core gaps.",
            "- Keep the fetch under the daily API cap: page size `500`, max requests `300`.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    observed_at = utc_now()
    api_key = args.api_key or os.environ.get(args.api_key_env, "")
    if not api_key:
        raise SystemExit(f"Missing API key. Set {args.api_key_env} or pass --api-key.")

    symbols, fetch_summary = fetch_all_symbols(
        endpoint=args.endpoint,
        api_key=api_key,
        page_size=args.page_size,
        max_requests=args.max_requests,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        observed_at=observed_at,
    )
    listings_rows = load_csv(args.listings_csv)
    matches = build_match_report(symbols, listings_rows)
    summary = summarize(symbols, matches, fetch_summary)

    symbol_rows = [symbol_to_dict(symbol) for symbol in symbols]
    match_rows = [match_to_dict(match) for match in matches]
    write_csv(args.symbols_csv, SYMBOL_FIELDS, symbol_rows)
    write_json(
        args.symbols_json,
        {
            "_meta": {
                "source": "financialdata_international_stock_symbols",
                "source_confidence": "secondary_review",
                "review_needed": True,
                "generated_at": observed_at,
                "endpoint": args.endpoint,
            },
            "summary": {
                key: summary[key]
                for key in [
                    "requests_used",
                    "raw_rows",
                    "deduped_rows",
                    "mapped_rows",
                    "unmapped_rows",
                    "current_exchange_gaps",
                    "global_expansion_candidates",
                    "truncated_by_request_limit",
                ]
            },
            "symbols": symbol_rows,
        },
    )
    write_csv(args.report_csv, MATCH_FIELDS, match_rows)
    write_csv(
        args.current_gaps_csv,
        MATCH_FIELDS,
        [row for row in match_rows if row["review_scope"] == "current_exchange_gap"],
    )
    write_csv(
        args.global_expansion_csv,
        MATCH_FIELDS,
        [row for row in match_rows if row["review_scope"] == "global_expansion_candidate"],
    )
    write_json(
        args.report_json,
        {
            "_meta": {
                "source": "financialdata_international_stock_symbols",
                "source_confidence": "secondary_review",
                "review_needed": True,
                "generated_at": observed_at,
                "endpoint": args.endpoint,
            },
            "summary": summary,
            "matches": match_rows,
        },
    )
    write_markdown(args.report_md, summary)
    print(json.dumps({"summary": summary}, sort_keys=True))
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and review FinancialData.net international stock symbols.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--api-key", default="", help="FinancialData.net API key. Prefer FINANCIALDATA_API_KEY env var.")
    parser.add_argument("--api-key-env", default="FINANCIALDATA_API_KEY")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--max-requests", type=int, default=MAX_REQUESTS)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--symbols-csv", type=Path, default=DEFAULT_SYMBOLS_CSV)
    parser.add_argument("--symbols-json", type=Path, default=DEFAULT_SYMBOLS_JSON)
    parser.add_argument("--report-csv", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--current-gaps-csv", type=Path, default=DEFAULT_CURRENT_GAPS_CSV)
    parser.add_argument("--global-expansion-csv", type=Path, default=DEFAULT_GLOBAL_EXPANSION_CSV)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
