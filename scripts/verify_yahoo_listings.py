from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_dataset import DEFAULT_JSON_OUT as DEFAULT_REVIEW_QUEUE_JSON
from scripts.rebuild_dataset import alias_matches_company


DEFAULT_OUTPUT_DIR = ROOT / "data" / "yahoo_verification"
DEFAULT_JSON_OUT = DEFAULT_OUTPUT_DIR / "verification.json"
DEFAULT_CSV_OUT = DEFAULT_OUTPUT_DIR / "verification.csv"

YAHOO_SUFFIX_BY_EXCHANGE: dict[str, str] = {
    "AMS": ".AS",
    "ASX": ".AX",
    "BME": ".MC",
    "CPH": ".CO",
    "HEL": ".HE",
    "HKEX": ".HK",
    "IDX": ".JK",
    "LSE": ".L",
    "OSL": ".OL",
    "SET": ".BK",
    "SIX": ".SW",
    "STO": ".ST",
    "TASE": ".TA",
    "TSX": ".TO",
    "TSXV": ".V",
    "WSE": ".WA",
}

EXPECTED_YAHOO_EXCHANGE_CODES: dict[str, set[str]] = {
    "BATS": {"BTS"},
    "NASDAQ": {"NMS", "NGM", "NCM"},
    "NYSE": {"NYQ"},
    "NYSE ARCA": {"PCX"},
    "NYSE MKT": {"ASE"},
    "OTC": {"PNK", "OQB", "OEM"},
    "LSE": {"LSE"},
}

EXPECTED_YAHOO_FULL_EXCHANGES: dict[str, set[str]] = {
    "BATS": {"Cboe US"},
    "NASDAQ": {"NasdaqGS", "NasdaqGM", "NasdaqCM"},
    "NYSE": {"NYSE"},
    "NYSE ARCA": {"NYSEArca"},
    "NYSE MKT": {"NYSEAmerican"},
    "OTC": {"OTC Markets OTCPK", "OTC Markets OTCQB", "OTC Markets OTCQX"},
    "LSE": {"LSE"},
}

EXPECTED_QUOTE_TYPES: dict[str, set[str]] = {
    "Stock": {"EQUITY"},
    "ETF": {"ETF"},
}


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_review_items(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("items", []))


def yahoo_symbol_candidates(ticker: str, exchange: str) -> list[str]:
    if exchange in {"BATS", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "OTC"}:
        return [ticker]
    suffix = YAHOO_SUFFIX_BY_EXCHANGE.get(exchange)
    if suffix:
        return [f"{ticker}{suffix}"]
    return []


def expected_exchange_match(exchange: str, yahoo_exchange: str, yahoo_full_exchange: str) -> bool | None:
    expected_codes = EXPECTED_YAHOO_EXCHANGE_CODES.get(exchange)
    expected_full_names = EXPECTED_YAHOO_FULL_EXCHANGES.get(exchange)
    if not expected_codes and not expected_full_names:
        return None
    return yahoo_exchange in (expected_codes or set()) or yahoo_full_exchange in (expected_full_names or set())


def expected_quote_type_match(asset_type: str, yahoo_quote_type: str) -> bool | None:
    expected = EXPECTED_QUOTE_TYPES.get(asset_type)
    if not expected:
        return None
    return yahoo_quote_type in expected


def choose_yahoo_name(info: dict[str, Any]) -> str:
    return str(info.get("longName") or info.get("shortName") or "").strip()


def normalize_isin(value: Any) -> str:
    if not value:
        return ""
    normalized = str(value).strip()
    return "" if normalized == "-" else normalized


def evaluate_row(row: dict[str, Any], yahoo_result: dict[str, Any]) -> dict[str, Any]:
    yahoo_name = choose_yahoo_name(yahoo_result)
    yahoo_exchange = str(yahoo_result.get("exchange") or "")
    yahoo_full_exchange = str(yahoo_result.get("fullExchangeName") or "")
    yahoo_quote_type = str(yahoo_result.get("quoteType") or "")
    yahoo_country = str(yahoo_result.get("country") or "")
    yahoo_isin = normalize_isin(yahoo_result.get("isin"))
    yahoo_sector = str(yahoo_result.get("sector") or "")
    exchange_match = expected_exchange_match(row["exchange"], yahoo_exchange, yahoo_full_exchange)
    quote_type_match = expected_quote_type_match(row["asset_type"], yahoo_quote_type)
    name_match = alias_matches_company(yahoo_name, row["name"]) if yahoo_name else False
    country_match = yahoo_country == row["country"] if yahoo_country and row.get("country") else None
    isin_match = yahoo_isin == row["isin"] if yahoo_isin and row.get("isin") else None

    if not yahoo_result.get("exists"):
        status = "not_found"
    elif quote_type_match is False or exchange_match is False or name_match is False:
        status = "mismatch"
    else:
        status = "verified"

    return {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "country": row["country"],
        "isin": row["isin"],
        "yahoo_symbol": yahoo_result.get("symbol", ""),
        "yahoo_quote_type": yahoo_quote_type,
        "yahoo_name": yahoo_name,
        "yahoo_exchange": yahoo_exchange,
        "yahoo_full_exchange": yahoo_full_exchange,
        "yahoo_country": yahoo_country,
        "yahoo_isin": yahoo_isin,
        "yahoo_sector": yahoo_sector,
        "history_rows": yahoo_result.get("history_rows", 0),
        "exchange_match": exchange_match,
        "quote_type_match": quote_type_match,
        "name_match": name_match,
        "country_match": country_match,
        "isin_match": isin_match,
        "status": status,
    }


def fetch_yahoo_symbol(symbol: str) -> dict[str, Any]:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - exercised via CLI environment
        raise SystemExit(
            "yfinance is required for Yahoo verification. Install it locally with `pip install yfinance`."
        ) from exc

    ticker = yf.Ticker(symbol)
    info = ticker.info
    history = ticker.history(period="5d")
    exists = bool(info.get("symbol") or len(history) > 0)
    return {
        "exists": exists,
        "symbol": str(info.get("symbol") or symbol),
        "quoteType": info.get("quoteType"),
        "shortName": info.get("shortName"),
        "longName": info.get("longName"),
        "exchange": info.get("exchange"),
        "fullExchangeName": info.get("fullExchangeName"),
        "country": info.get("country"),
        "sector": info.get("sector"),
        "isin": getattr(ticker, "isin", ""),
        "history_rows": len(history),
    }


def verify_items(items: list[dict[str, Any]], *, delay_seconds: float) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        candidates = yahoo_symbol_candidates(str(item["ticker"]), str(item["exchange"]))
        if not candidates:
            results.append(
                {
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "asset_type": item["asset_type"],
                    "name": item["name"],
                    "country": item["country"],
                    "isin": item["isin"],
                    "status": "unsupported_exchange",
                    "yahoo_symbol": "",
                }
            )
            continue

        last_error = None
        matched_result = None
        for symbol in candidates:
            try:
                yahoo_result = fetch_yahoo_symbol(symbol)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                continue
            matched_result = evaluate_row(item, yahoo_result)
            if matched_result["status"] == "verified":
                break

        if matched_result is not None:
            results.append(matched_result)
        else:
            results.append(
                {
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "asset_type": item["asset_type"],
                    "name": item["name"],
                    "country": item["country"],
                    "isin": item["isin"],
                    "status": "error",
                    "yahoo_symbol": candidates[0],
                    "error": last_error or "No Yahoo result.",
                }
            )

        if delay_seconds > 0 and index < len(items) - 1:
            time.sleep(delay_seconds)
    return results


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify suspicious listings against Yahoo Finance via yfinance.")
    parser.add_argument("--review-queue-json", type=Path, default=DEFAULT_REVIEW_QUEUE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--finding-type", action="append", default=[])
    parser.add_argument("--include-ticker", action="append", default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    items = load_review_items(args.review_queue_json)
    finding_types = set(args.finding_type)
    include_tickers = {ticker.upper() for ticker in args.include_ticker}
    if finding_types:
        items = [
            item for item in items
            if any(finding["finding_type"] in finding_types for finding in item.get("findings", []))
        ]
    if include_tickers:
        items = [item for item in items if str(item["ticker"]).upper() in include_tickers]
    if args.limit is not None:
        items = items[: args.limit]

    results = verify_items(items, delay_seconds=args.delay_seconds)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(args.csv_out, results)

    status_counts: dict[str, int] = {}
    for row in results:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    print(
        json.dumps(
            {
                "review_queue": display_path(args.review_queue_json),
                "items": len(items),
                "status_counts": status_counts,
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
