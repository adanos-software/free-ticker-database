from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.backfill_xtb_omi_isins import names_match
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


DEFAULT_OUTPUT_DIR = ROOT / "data" / "tradingview_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

TRADINGVIEW_SCAN_URL = "https://scanner.tradingview.com/{market}/scan"
USER_AGENT = "Mozilla/5.0"

EXCHANGE_TO_TRADINGVIEW: dict[str, tuple[str, str]] = {
    "ASX": ("australia", "ASX"),
    "B3": ("brazil", "BMFBOVESPA"),
    "BATS": ("america", "CBOE"),
    "BSE_IN": ("india", "BSE"),
    "Bursa": ("malaysia", "MYX"),
    "ATHEX": ("greece", "ATHEX"),
    "BIST": ("turkey", "BIST"),
    "HNX": ("vietnam", "HNX"),
    "HOSE": ("vietnam", "HOSE"),
    "IDX": ("indonesia", "IDX"),
    "JSE": ("rsa", "JSE"),
    "LSE": ("uk", "LSE"),
    "NASDAQ": ("america", "NASDAQ"),
    "NEO": ("canada", "NEO"),
    "NSE_IN": ("india", "NSE"),
    "NYSE": ("america", "NYSE"),
    "NYSE ARCA": ("america", "AMEX"),
    "NYSE MKT": ("america", "AMEX"),
    "PSX": ("pakistan", "PSX"),
    "QSE": ("qatar", "QSE"),
    "SET": ("thailand", "SET"),
    "SSE": ("china", "SSE"),
    "SGX": ("singapore", "SGX"),
    "STO": ("sweden", "OMXSTO"),
    "SZSE": ("china", "SZSE"),
    "TADAWUL": ("ksa", "TADAWUL"),
    "TASE": ("israel", "TASE"),
    "TWSE": ("taiwan", "TWSE"),
    "TSX": ("canada", "TSX"),
    "TSXV": ("canada", "TSXV"),
    "UPCOM": ("vietnam", "UPCOM"),
    "XETRA": ("germany", "XETR"),
}

BLOCKED_ISIN_UPDATES = {
    # Explicit dataset-quality regression guard: this NASDAQ row is a known
    # cross-company collision and must not inherit the unrelated Israeli ISIN.
    ("BMR", "NASDAQ"),
}

SCAN_COLUMNS = [
    "name",
    "description",
    "exchange",
    "type",
    "subtype",
    "isin",
    "sector",
    "industry",
    "country",
    "typespecs",
]

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "tv_symbol",
    "tv_name",
    "tv_exchange",
    "tv_type",
    "tv_subtype",
    "tv_isin",
    "tv_sector",
    "tv_industry",
    "tv_country",
    "name_match",
    "peer_name_match",
    "decision",
]


@dataclass(frozen=True)
class TradingViewRow:
    request_symbol: str
    symbol: str
    name: str
    exchange: str
    instrument_type: str
    subtype: str
    isin: str
    sector: str
    industry: str
    country: str
    typespecs: tuple[str, ...]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def ticker_variants(ticker: str, exchange: str) -> list[str]:
    variants = [ticker.strip().upper()]
    if exchange in {"TSX", "TSXV"}:
        dotted = variants[0].replace("-", ".")
        if dotted not in variants:
            variants.append(dotted)
    if exchange in {"STO"}:
        underscored = variants[0].replace("-", "_")
        if underscored not in variants:
            variants.append(underscored)
    return variants


def tradingview_symbols_for_row(row: dict[str, str]) -> list[str]:
    mapping = EXCHANGE_TO_TRADINGVIEW.get(row.get("exchange", ""))
    if not mapping:
        return []
    _, tv_exchange = mapping
    return [f"{tv_exchange}:{variant}" for variant in ticker_variants(row["ticker"], row["exchange"])]


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace(".", "-")


def load_missing_isin_rows(
    *,
    exchanges: set[str],
    asset_types: set[str],
    tickers_csv: Path = TICKERS_CSV,
) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row.get("exchange") in exchanges
            and row.get("asset_type") in asset_types
            and not row.get("isin", "").strip()
            and row.get("exchange") in EXCHANGE_TO_TRADINGVIEW
        ]


def index_existing_isin_rows(tickers_csv: Path = TICKERS_CSV) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            isin = row.get("isin", "").strip().upper()
            if isin:
                indexed[isin].append(row)
    return indexed


def fetch_tradingview_batch(
    *,
    market: str,
    symbols: list[str],
    session: requests.Session,
    timeout_seconds: float,
) -> dict[str, TradingViewRow]:
    if not symbols:
        return {}
    response = session.post(
        TRADINGVIEW_SCAN_URL.format(market=market),
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        json={"symbols": {"tickers": symbols}, "columns": SCAN_COLUMNS},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    rows: dict[str, TradingViewRow] = {}
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        symbol_key = str(item.get("s") or "")
        values = item.get("d") or []
        if len(values) != len(SCAN_COLUMNS):
            continue
        data = dict(zip(SCAN_COLUMNS, values, strict=True))
        typespecs = data.get("typespecs") if isinstance(data.get("typespecs"), list) else []
        rows[symbol_key.upper()] = TradingViewRow(
            request_symbol=symbol_key.upper(),
            symbol=str(data.get("name") or "").strip().upper(),
            name=str(data.get("description") or "").strip(),
            exchange=str(data.get("exchange") or "").strip().upper(),
            instrument_type=str(data.get("type") or "").strip().lower(),
            subtype=str(data.get("subtype") or "").strip().lower(),
            isin=str(data.get("isin") or "").strip().upper(),
            sector=str(data.get("sector") or "").strip(),
            industry=str(data.get("industry") or "").strip(),
            country=str(data.get("country") or "").strip(),
            typespecs=tuple(str(value).strip().lower() for value in typespecs),
        )
    return rows


def fetch_tradingview_rows(
    target_rows: list[dict[str, str]],
    *,
    batch_size: int,
    delay_seconds: float,
    timeout_seconds: float,
    progress_every: int,
) -> dict[str, TradingViewRow]:
    requests_by_market: dict[str, list[str]] = defaultdict(list)
    for row in target_rows:
        market, _ = EXCHANGE_TO_TRADINGVIEW[row["exchange"]]
        requests_by_market[market].extend(tradingview_symbols_for_row(row))

    session = requests.Session()
    results: dict[str, TradingViewRow] = {}
    checked = 0
    total = sum(len(symbols) for symbols in requests_by_market.values())
    for market, symbols in sorted(requests_by_market.items()):
        unique_symbols = list(dict.fromkeys(symbols))
        for start in range(0, len(unique_symbols), batch_size):
            batch = unique_symbols[start : start + batch_size]
            results.update(
                fetch_tradingview_batch(
                    market=market,
                    symbols=batch,
                    session=session,
                    timeout_seconds=timeout_seconds,
                )
            )
            checked += len(batch)
            if progress_every > 0 and checked % progress_every < batch_size:
                print(f"checked {checked}/{total} TradingView symbols", file=sys.stderr, flush=True)
            if delay_seconds > 0 and start + batch_size < len(unique_symbols):
                time.sleep(delay_seconds)
    return results


def asset_type_matches(target: dict[str, str], source: TradingViewRow) -> bool:
    if target.get("asset_type") == "ETF":
        return source.instrument_type == "fund" and (source.subtype == "etf" or "etf" in source.typespecs)
    if target.get("asset_type") == "Stock":
        return source.instrument_type == "stock"
    return False


def choose_source_for_row(row: dict[str, str], source_rows: dict[str, TradingViewRow]) -> TradingViewRow | None:
    for request_symbol in tradingview_symbols_for_row(row):
        source = source_rows.get(request_symbol.upper())
        if source is not None:
            return source
    return None


def peer_name_matches(isin: str, target_name: str, existing_isin_rows: dict[str, list[dict[str, str]]]) -> bool:
    peers = existing_isin_rows.get(isin, [])
    return not peers or any(names_match(peer.get("name", ""), target_name) for peer in peers)


def has_cjk(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)


def names_compatible_for_isin(row: dict[str, str], source: TradingViewRow) -> bool:
    if names_match(source.name, row["name"]):
        return True
    return (
        row.get("exchange") in {"SSE", "SZSE"}
        and row.get("ticker", "").isdigit()
        and source.isin.startswith("CN")
        and has_cjk(row.get("name", ""))
    )


def evaluate_row(
    row: dict[str, str],
    source: TradingViewRow | None,
    existing_isin_rows: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "tv_symbol": "",
        "tv_name": "",
        "tv_exchange": "",
        "tv_type": "",
        "tv_subtype": "",
        "tv_isin": "",
        "tv_sector": "",
        "tv_industry": "",
        "tv_country": "",
        "name_match": False,
        "peer_name_match": False,
    }
    if source is None:
        return {**base, "decision": "no_tradingview_match"}
    if (row.get("ticker", "").strip().upper(), row.get("exchange", "").strip()) in BLOCKED_ISIN_UPDATES:
        return {**base, "decision": "blocked_known_collision"}

    base.update(
        {
            "tv_symbol": source.symbol,
            "tv_name": source.name,
            "tv_exchange": source.exchange,
            "tv_type": source.instrument_type,
            "tv_subtype": source.subtype,
            "tv_isin": source.isin,
            "tv_sector": source.sector,
            "tv_industry": source.industry,
            "tv_country": source.country,
        }
    )
    _, expected_tv_exchange = EXCHANGE_TO_TRADINGVIEW[row["exchange"]]
    symbol_match = normalize_symbol(source.symbol) == normalize_symbol(row["ticker"])
    name_match = names_match(source.name, row["name"]) if source.name else False
    peer_match = peer_name_matches(source.isin, row["name"], existing_isin_rows) if source.isin else False
    base["name_match"] = name_match
    base["peer_name_match"] = peer_match

    if source.exchange != expected_tv_exchange:
        return {**base, "decision": "exchange_mismatch"}
    if not symbol_match:
        return {**base, "decision": "symbol_mismatch"}
    if not source.isin:
        return {**base, "decision": "missing_isin"}
    if not is_valid_isin(source.isin):
        return {**base, "decision": "invalid_isin"}
    if not asset_type_matches(row, source):
        return {**base, "decision": "asset_type_mismatch"}
    if not names_compatible_for_isin(row, source):
        return {**base, "decision": "name_mismatch"}
    if not peer_match:
        return {**base, "decision": "isin_peer_name_mismatch"}
    return {**base, "decision": "accept"}


def verify_rows(
    target_rows: list[dict[str, str]],
    source_rows: dict[str, TradingViewRow],
    existing_isin_rows: dict[str, list[dict[str, str]]],
) -> list[dict[str, Any]]:
    return [
        evaluate_row(row, choose_source_for_row(row, source_rows), existing_isin_rows)
        for row in target_rows
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "isin",
                "decision": "update",
                "proposed_value": result["tv_isin"],
                "confidence": "0.78",
                "reason": (
                    "TradingView free scanner supplied a valid ISIN for a row without ISIN; accepted only after exact "
                    "scanner exchange, symbol normalization, asset type, issuer/product-name, existing-ISIN peer, "
                    "and ISIN-checksum gates matched."
                ),
            }
        )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing ISINs from the free TradingView scanner.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", choices=sorted(EXCHANGE_TO_TRADINGVIEW))
    parser.add_argument("--asset-type", action="append", choices=["ETF", "Stock"])
    parser.add_argument("--batch-size", type=int, default=150)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or EXCHANGE_TO_TRADINGVIEW)
    asset_types = set(args.asset_type or ["ETF", "Stock"])
    target_rows = load_missing_isin_rows(exchanges=exchanges, asset_types=asset_types)
    if args.offset:
        target_rows = target_rows[args.offset :]
    if args.limit is not None:
        target_rows = target_rows[: args.limit]

    source_rows = fetch_tradingview_rows(
        target_rows,
        batch_size=args.batch_size,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        progress_every=args.progress_every,
    )
    results = verify_rows(target_rows, source_rows, index_existing_isin_rows())
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_report_csv(args.csv_out, results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "accepted_isin_updates": len(updates),
                "applied": args.apply,
                "asset_types": sorted(asset_types),
                "candidates": len(target_rows),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "json_out": display_path(args.json_out),
                "source_rows": len(source_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
