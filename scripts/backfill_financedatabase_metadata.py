from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "financedatabase_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "metadata_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "metadata_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

FINANCEDATABASE_EXCHANGE_CODES: dict[str, set[str]] = {
    "AMS": {"AMS"},
    "ASX": {"ASX"},
    "B3": {"SAO"},
    "BATS": {"BTS"},
    "BME": {"MCE"},
    "BMV": {"MEX"},
    "BSE_HU": {"BUD"},
    "Bursa": {"KLS"},
    "CPH": {"CPH"},
    "Euronext": {"BRU", "LIS", "PAR"},
    "HEL": {"HEL"},
    "HNX": {"HAN"},
    "HOSE": {"HCM"},
    "IDX": {"JKT"},
    "JSE": {"JNB"},
    "KOSDAQ": {"KOE"},
    "KRX": {"KSC"},
    "LSE": {"LSE"},
    "NASDAQ": {"NCM", "NGM", "NMS"},
    "NEO": {"NEO"},
    "NYSE": {"NYQ"},
    "NYSE ARCA": {"PCX"},
    "NYSE MKT": {"ASE"},
    "OSL": {"OSL"},
    "OTC": {"PNK"},
    "SET": {"SET"},
    "SIX": {"EBS"},
    "SSE": {"SHH"},
    "STO": {"STO"},
    "SZSE": {"SHZ"},
    "TASE": {"TLV"},
    "TPEX": {"TWO"},
    "TSE": {"JPX"},
    "TSX": {"TOR"},
    "TSXV": {"VAN"},
    "TWSE": {"TAI"},
    "XETRA": {"FRA", "GER"},
}

EXPECTED_ISIN_PREFIXES: dict[str, tuple[str, ...]] = {
    "ASX": ("AU", "NZ"),
    "B3": ("BR",),
    "BATS": ("US",),
    "BME": ("ES",),
    "BMV": ("MX",),
    "BSE_HU": ("HU",),
    "Bursa": ("MY",),
    "NASDAQ": ("US",),
    "NEO": ("CA",),
    "NYSE": ("US",),
    "NYSE ARCA": ("US",),
    "NYSE MKT": ("US",),
    "SET": ("TH",),
    "SIX": ("CH",),
    "SSE": ("CN",),
    "STO": ("SE",),
    "SZSE": ("CN",),
    "TASE": ("IL",),
    "TPEX": ("TW",),
    "TSE": ("JP",),
    "TSX": ("CA",),
    "TSXV": ("CA",),
    "TWSE": ("TW",),
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "fd_symbol",
    "fd_exchange",
    "fd_name",
    "fd_sector",
    "fd_isin",
    "sector_update",
    "isin_update",
    "decision",
]


@dataclass(frozen=True)
class FinanceDatabaseRow:
    symbol: str
    base_ticker: str
    name: str
    exchange: str
    asset_type: str
    sector: str
    isin: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalized_ticker_key(value: str) -> str:
    return value.strip().upper().replace("-", ".")


def finance_base_ticker(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if "." not in symbol:
        return symbol
    return symbol.rsplit(".", 1)[0]


def expected_isin_prefix_match(exchange: str, isin: str) -> bool:
    prefixes = EXPECTED_ISIN_PREFIXES.get(exchange)
    return prefixes is None or isin.startswith(prefixes)


def load_ticker_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_financedatabase_rows() -> list[FinanceDatabaseRow]:
    try:
        import financedatabase as fd
    except ImportError as exc:  # pragma: no cover - exercised via CLI environment
        raise SystemExit(
            "financedatabase is required for this backfill. Install it locally with `pip install financedatabase`."
        ) from exc

    rows: list[FinanceDatabaseRow] = []

    equities = fd.Equities().select().reset_index().fillna("")
    for record in equities.to_dict("records"):
        symbol = str(record.get("symbol") or "").strip()
        exchange = str(record.get("exchange") or "").strip()
        if not symbol or not exchange:
            continue
        rows.append(
            FinanceDatabaseRow(
                symbol=symbol,
                base_ticker=finance_base_ticker(symbol),
                name=str(record.get("name") or "").strip(),
                exchange=exchange,
                asset_type="Stock",
                sector=str(record.get("sector") or "").strip(),
                isin=str(record.get("isin") or "").strip().upper(),
            )
        )

    etfs = fd.ETFs().select().reset_index().fillna("")
    for record in etfs.to_dict("records"):
        symbol = str(record.get("symbol") or "").strip()
        exchange = str(record.get("exchange") or "").strip()
        if not symbol or not exchange:
            continue
        rows.append(
            FinanceDatabaseRow(
                symbol=symbol,
                base_ticker=finance_base_ticker(symbol),
                name=str(record.get("name") or "").strip(),
                exchange=exchange,
                asset_type="ETF",
                sector=str(record.get("category") or "").strip(),
                isin=str(record.get("isin") or "").strip().upper(),
            )
        )
    return rows


def index_financedatabase_rows(
    rows: list[FinanceDatabaseRow],
) -> dict[tuple[str, str, str], list[FinanceDatabaseRow]]:
    indexed: dict[tuple[str, str, str], list[FinanceDatabaseRow]] = defaultdict(list)
    for row in rows:
        indexed[(normalized_ticker_key(row.base_ticker), row.exchange, row.asset_type)].append(row)
    return indexed


def find_financedatabase_candidates(
    row: dict[str, str],
    indexed_rows: dict[tuple[str, str, str], list[FinanceDatabaseRow]],
) -> list[FinanceDatabaseRow]:
    result: list[FinanceDatabaseRow] = []
    for exchange_code in FINANCEDATABASE_EXCHANGE_CODES.get(row["exchange"], set()):
        result.extend(
            indexed_rows.get(
                (normalized_ticker_key(row["ticker"]), exchange_code, row["asset_type"]),
                [],
            )
        )
    return result


def evaluate_financedatabase_row(
    row: dict[str, str],
    candidates: list[FinanceDatabaseRow],
    *,
    include_sector: bool = True,
    include_isin: bool = True,
) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "fd_symbol": "",
        "fd_exchange": "",
        "fd_name": "",
        "fd_sector": "",
        "fd_isin": "",
        "sector_update": "",
        "isin_update": "",
    }

    if not candidates:
        return {**base, "decision": "no_financedatabase_match"}
    if len(candidates) > 1:
        return {**base, "decision": "ambiguous_financedatabase_match"}

    candidate = candidates[0]
    base.update(
        {
            "fd_symbol": candidate.symbol,
            "fd_exchange": candidate.exchange,
            "fd_name": candidate.name,
            "fd_sector": candidate.sector,
            "fd_isin": candidate.isin,
        }
    )

    if not names_match(candidate.name, row["name"]):
        return {**base, "decision": "name_mismatch"}

    sector_update = ""
    if include_sector and not row.get("sector", "").strip():
        sector_update = normalize_sector(candidate.sector, row["asset_type"])

    isin_update = ""
    if include_isin and not row.get("isin", "").strip() and candidate.isin:
        if not is_valid_isin(candidate.isin):
            return {**base, "decision": "invalid_isin"}
        if not expected_isin_prefix_match(row["exchange"], candidate.isin):
            return {**base, "decision": "isin_country_mismatch"}
        isin_update = candidate.isin

    if not sector_update and not isin_update:
        return {**base, "decision": "no_update"}

    return {
        **base,
        "sector_update": sector_update,
        "isin_update": isin_update,
        "decision": "accept",
    }


def verify_financedatabase_metadata(
    rows: list[dict[str, str]],
    financedatabase_rows: list[FinanceDatabaseRow],
    *,
    include_sector: bool = True,
    include_isin: bool = True,
) -> list[dict[str, Any]]:
    indexed_rows = index_financedatabase_rows(financedatabase_rows)
    return [
        evaluate_financedatabase_row(
            row,
            find_financedatabase_candidates(row, indexed_rows),
            include_sector=include_sector,
            include_isin=include_isin,
        )
        for row in rows
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        if result.get("sector_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": result["sector_update"],
                    "confidence": "0.78",
                    "reason": "FinanceDatabase supplied a normalized sector/category for a row without sector; accepted only after ticker, mapped exchange code, asset type, and issuer/product-name gates matched.",
                }
            )
        if result.get("isin_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": result["isin_update"],
                    "confidence": "0.74",
                    "reason": "FinanceDatabase supplied a valid ISIN for a row without ISIN; accepted only after ticker, mapped exchange code, asset type, issuer/product-name, expected ISIN country prefix, and checksum gates matched.",
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
    parser = argparse.ArgumentParser(description="Backfill missing sector/ISIN metadata from FinanceDatabase.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--asset-type", action="append", choices=["ETF", "Stock"], help="Restrict to one or more asset types.")
    parser.add_argument("--disable-sector", action="store_true")
    parser.add_argument(
        "--enable-isin",
        action="store_true",
        help="Also emit ISIN candidates. Keep disabled by default because FinanceDatabase ISINs are secondary and can collide with stale same-ISIN rows.",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or FINANCEDATABASE_EXCHANGE_CODES)
    asset_types = set(args.asset_type or ["ETF", "Stock"])
    rows = [
        row
        for row in load_ticker_rows()
        if row["exchange"] in exchanges
        and row["asset_type"] in asset_types
        and (
            (not args.disable_sector and not row.get("sector", "").strip())
            or (args.enable_isin and not row.get("isin", "").strip())
        )
    ]
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results = verify_financedatabase_metadata(
        rows,
        load_financedatabase_rows(),
        include_sector=not args.disable_sector,
        include_isin=args.enable_isin,
    )
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "asset_types": sorted(asset_types),
                "candidates": len(rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "accepted_sector_updates": sum(1 for update in updates if update["field"] == "sector"),
                "accepted_isin_updates": sum(1 for update in updates if update["field"] == "isin"),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
