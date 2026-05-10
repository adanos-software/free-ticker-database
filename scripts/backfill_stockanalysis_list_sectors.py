from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, alias_matches_company, is_valid_isin, normalize_sector

DEFAULT_OUTPUT_DIR = ROOT / "data" / "stockanalysis_verification"
DEFAULT_CSV_OUT = DEFAULT_OUTPUT_DIR / "stockanalysis_list_sector_backfill.csv"
DEFAULT_JSON_OUT = DEFAULT_OUTPUT_DIR / "stockanalysis_list_sector_backfill.json"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

STOCKANALYSIS_LISTS = {
    "B3": {
        "exchange_code": "BVMF",
        "slug": "brazil-stock-exchange",
        "symbol_prefix": "bvmf/",
    },
    "ASX": {
        "exchange_code": "ASX",
        "slug": "australian-securities-exchange",
        "symbol_prefix": "asx/",
    },
    "LSE": {
        "exchange_code": "LON",
        "slug": "london-stock-exchange",
        "symbol_prefix": "lon/",
    },
    "OTC": {
        "exchange_code": "OTC",
        "slug": "otc-stocks",
        "symbol_prefix": "otc/",
    },
    "SSE": {
        "exchange_code": "SHA",
        "slug": "shanghai-stock-exchange",
        "symbol_prefix": "sha/",
    },
    "SZSE": {
        "exchange_code": "SHE",
        "slug": "shenzhen-stock-exchange",
        "symbol_prefix": "she/",
    },
    "TSX": {
        "exchange_code": "TSX",
        "slug": "toronto-stock-exchange",
        "symbol_prefix": "tsx/",
    },
    "TSXV": {
        "exchange_code": "TSXV",
        "slug": "tsx-venture-exchange",
        "symbol_prefix": "tsxv/",
    },
}

USER_AGENT = "free-ticker-database/3.0 (+https://github.com/adanos-software/free-ticker-database)"
SCREENER_ENDPOINT = "https://stockanalysis.com/_api/endpoints/screener/table"
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "stockanalysis_symbol",
    "stockanalysis_name",
    "stockanalysis_isin",
    "stockanalysis_sector",
    "stockanalysis_industry",
    "stockanalysis_category",
    "isin_update",
    "sector_update",
    "category_update",
    "decision",
    "source_url",
]


@dataclass(frozen=True)
class StockAnalysisListRow:
    exchange: str
    asset_type: str
    symbol: str
    name: str
    isin: str
    sector: str
    industry: str
    category: str
    source_url: str


def normalize_source_symbol(value: str, prefix: str) -> str:
    symbol = value.strip()
    if symbol.lower().startswith(prefix.lower()):
        symbol = symbol[len(prefix) :]
    return symbol.strip().upper()


def normalize_stockanalysis_sector(value: str) -> str:
    return normalize_sector(value.strip(), "Stock")


def normalize_stockanalysis_etf_category(value: str) -> str:
    normalized = normalize_sector(value.strip(), "ETF")
    if normalized:
        return normalized
    if value.strip().lower() == "commodities":
        return "Commodity"
    return ""


def stockanalysis_screener_url(exchange_code: str, page: int, count: int, asset_type: str = "Stock") -> str:
    if asset_type == "ETF":
        params = {
            "type": "e",
            "m": "aum",
            "s": "desc",
            "c": "no,s,n,isin,assetClass,aum",
            "sc": "aum",
            "cn": count,
            "f": f"exchangeCode-is-{exchange_code},subtype-is-etf",
            "p": page,
            "i": "symbols",
        }
        return f"{SCREENER_ENDPOINT}?{urlencode(params)}"
    params = {
        "type": "a",
        "m": "marketCap",
        "s": "desc",
        "c": "no,s,n,isin,sector,industry,marketCap",
        "sc": "marketCap",
        "cn": count,
        "f": f"exchangeCode-is-{exchange_code},subtype-is-stock",
        "p": page,
        "i": "symbols",
    }
    return f"{SCREENER_ENDPOINT}?{urlencode(params)}"


def fetch_stockanalysis_asset_rows(
    exchange: str,
    asset_type: str,
    session: requests.Session,
    *,
    timeout_seconds: float = 30,
    page_size: int = 1000,
    delay_seconds: float = 0.0,
) -> list[StockAnalysisListRow]:
    config = STOCKANALYSIS_LISTS[exchange]
    symbol_prefix = (
        f"{config['exchange_code']}-"
        if asset_type == "ETF"
        else config["symbol_prefix"]
    )
    rows: list[StockAnalysisListRow] = []
    page = 1
    while True:
        url = stockanalysis_screener_url(config["exchange_code"], page, page_size, asset_type=asset_type)
        response = session.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Referer": f"https://stockanalysis.com/list/{config['slug']}/",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        page_rows = payload.get("data", {}).get("data", [])
        for item in page_rows:
            symbol = normalize_source_symbol(str(item.get("s", "")), symbol_prefix)
            if not symbol:
                continue
            rows.append(
                StockAnalysisListRow(
                    exchange=exchange,
                    asset_type=asset_type,
                    symbol=symbol,
                    name=str(item.get("n", "")).strip(),
                    isin=str(item.get("isin", "")).strip().upper(),
                    sector=str(item.get("sector", "")).strip(),
                    industry=str(item.get("industry", "")).strip(),
                    category=str(item.get("assetClass", "")).strip(),
                    source_url=(
                        f"https://stockanalysis.com/etf/{config['exchange_code']}-{symbol}/"
                        if asset_type == "ETF"
                        else f"https://stockanalysis.com/quote/{config['symbol_prefix']}{symbol}/"
                    ),
                )
            )
        results_count = int(payload.get("data", {}).get("resultsCount") or 0)
        if not page_rows or len(rows) >= results_count:
            break
        page += 1
        if delay_seconds:
            time.sleep(delay_seconds)
    return rows


def fetch_stockanalysis_list_rows(
    exchange: str,
    session: requests.Session,
    *,
    timeout_seconds: float = 30,
    page_size: int = 1000,
    delay_seconds: float = 0.0,
) -> list[StockAnalysisListRow]:
    rows: list[StockAnalysisListRow] = []
    for asset_type in ("Stock", "ETF"):
        rows.extend(
            fetch_stockanalysis_asset_rows(
                exchange,
                asset_type,
                session,
                timeout_seconds=timeout_seconds,
                page_size=page_size,
                delay_seconds=delay_seconds,
            )
        )
    return rows


def load_target_rows(path: Path, exchanges: set[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") in exchanges
        and row.get("asset_type") in {"Stock", "ETF"}
        and (
            not row.get("isin", "").strip()
            or (row.get("asset_type") == "Stock" and not row.get("stock_sector", "").strip())
            or (row.get("asset_type") == "ETF" and not row.get("etf_category", "").strip())
        )
        and row.get("ticker", "").strip()
    ]


def index_source_rows(rows: list[StockAnalysisListRow]) -> dict[tuple[str, str, str], StockAnalysisListRow]:
    return {(row.exchange, row.asset_type, row.symbol): row for row in rows}


def evaluate_row(row: dict[str, str], source_row: StockAnalysisListRow | None) -> dict[str, Any]:
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "stockanalysis_symbol": source_row.symbol if source_row else "",
        "stockanalysis_name": source_row.name if source_row else "",
        "stockanalysis_isin": source_row.isin if source_row else "",
        "stockanalysis_sector": source_row.sector if source_row else "",
        "stockanalysis_industry": source_row.industry if source_row else "",
        "stockanalysis_category": source_row.category if source_row else "",
        "isin_update": "",
        "sector_update": "",
        "category_update": "",
        "source_url": source_row.source_url if source_row else "",
    }
    if source_row is None:
        return {**base, "decision": "no_symbol_match"}

    name = row.get("name", "")
    source_name = source_row.name
    name_match = alias_matches_company(name, source_name) or alias_matches_company(source_name, name)
    if not name_match:
        return {**base, "decision": "name_mismatch"}

    isin_update = source_row.isin if not row.get("isin", "").strip() and is_valid_isin(source_row.isin) else ""
    sector_update = ""
    category_update = ""
    if row.get("asset_type") == "Stock":
        sector = normalize_stockanalysis_sector(source_row.sector)
        sector_update = sector if not row.get("stock_sector", "").strip() and sector else ""
    elif row.get("asset_type") == "ETF":
        category = normalize_stockanalysis_etf_category(source_row.category)
        category_update = category if not row.get("etf_category", "").strip() and category else ""
    if not isin_update and not sector_update and not category_update:
        return {**base, "decision": "no_update"}

    return {
        **base,
        "isin_update": isin_update,
        "sector_update": sector_update,
        "category_update": category_update,
        "decision": "accept",
    }


def evaluate_rows(
    target_rows: list[dict[str, str]],
    source_rows: list[StockAnalysisListRow],
) -> list[dict[str, Any]]:
    indexed = index_source_rows(source_rows)
    return [
        evaluate_row(
            row,
            indexed.get(
                (
                    row.get("exchange", ""),
                    row.get("asset_type", ""),
                    row.get("ticker", "").strip().upper(),
                )
            ),
        )
        for row in target_rows
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        if result.get("isin_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": result["isin_update"],
                    "confidence": "0.76",
                    "reason": (
                        "StockAnalysis exchange list screener supplied a valid ISIN for a row missing ISIN; "
                        "accepted as reviewed secondary metadata after exact exchange/symbol match, issuer-name gate, "
                        f"and ISIN checksum. Source: {result['source_url']}"
                    ),
                }
            )
        if result.get("sector_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "stock_sector",
                    "decision": "update",
                    "proposed_value": result["sector_update"],
                    "confidence": "0.74",
                    "reason": (
                        "StockAnalysis exchange list screener supplied sector metadata for a row missing stock_sector; "
                        "accepted as reviewed secondary metadata after exact exchange/symbol match and issuer-name gate. "
                        f"Source: {result['source_url']}"
                    ),
                }
            )
        if result.get("category_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "etf_category",
                    "decision": "update",
                    "proposed_value": result["category_update"],
                    "confidence": "0.72",
                    "reason": (
                        "StockAnalysis ETF screener supplied asset-class metadata for a row missing etf_category; "
                        "accepted as reviewed secondary metadata after exact exchange/symbol match and issuer-name gate. "
                        f"Source: {result['source_url']}"
                    ),
                }
            )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_report_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill stock sectors from StockAnalysis exchange list data.")
    parser.add_argument("--exchange", action="append", choices=sorted(STOCKANALYSIS_LISTS), default=[])
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or STOCKANALYSIS_LISTS)
    session = requests.Session()

    source_rows: list[StockAnalysisListRow] = []
    errors: list[dict[str, str]] = []
    for exchange in sorted(exchanges):
        try:
            source_rows.extend(
                fetch_stockanalysis_list_rows(
                    exchange,
                    session,
                    timeout_seconds=args.timeout_seconds,
                    page_size=args.page_size,
                    delay_seconds=args.delay_seconds,
                )
            )
        except requests.RequestException as exc:
            errors.append({"exchange": exchange, "error": str(exc)})

    target_rows = load_target_rows(args.listings_csv, exchanges)
    results = evaluate_rows(target_rows, source_rows)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    write_report_csv(args.csv_out, results)
    summary = {
        "source_rows": len(source_rows),
        "targets": len(target_rows),
        "updates": len(updates),
        "applied": args.apply,
        "decision_counts": dict(Counter(row["decision"] for row in results)),
        "errors": errors,
    }
    write_report_json(args.json_out, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
