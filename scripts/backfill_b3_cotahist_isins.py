from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from collections import Counter
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


B3_COTAHIST_URL_TEMPLATE = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.ZIP"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "b3_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "cotahist_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "cotahist_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

SPOT_MARKET_TYPES = {"010", "020"}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "cotahist_ticker",
    "cotahist_name",
    "cotahist_bdi_code",
    "cotahist_market_type",
    "cotahist_date",
    "cotahist_isin",
    "decision",
]


@dataclass(frozen=True)
class CotahistRow:
    ticker: str
    name: str
    bdi_code: str
    market_type: str
    date: str
    isin: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def download_cotahist_zip(year: int, *, timeout_seconds: float) -> bytes:
    url = B3_COTAHIST_URL_TEMPLATE.format(year=year)
    request = urllib.request.Request(url, headers={"User-Agent": "free-ticker-database/3.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read()


def read_cotahist_zip(zip_bytes: bytes) -> str:
    with ZipFile(BytesIO(zip_bytes)) as archive:
        txt_names = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if len(txt_names) != 1:
            raise ValueError(f"Expected one TXT file in COTAHIST zip, found {txt_names!r}")
        return archive.read(txt_names[0]).decode("latin1")


def parse_cotahist_line(line: str) -> CotahistRow | None:
    if len(line) < 245 or line[:2] != "01":
        return None
    ticker = line[12:24].strip().upper()
    name = line[27:39].strip()
    bdi_code = line[10:12].strip()
    market_type = line[24:27].strip()
    isin = line[230:242].strip().upper()
    if not ticker or market_type not in SPOT_MARKET_TYPES or not is_valid_isin(isin) or not isin.startswith("BR"):
        return None
    return CotahistRow(
        ticker=ticker,
        name=name,
        bdi_code=bdi_code,
        market_type=market_type,
        date=line[2:10].strip(),
        isin=isin,
    )


def parse_cotahist_text(text: str) -> list[CotahistRow]:
    rows: list[CotahistRow] = []
    for line in text.splitlines():
        parsed = parse_cotahist_line(line)
        if parsed is not None:
            rows.append(parsed)
    return rows


def latest_rows_by_ticker(rows: list[CotahistRow]) -> dict[str, list[CotahistRow]]:
    grouped: dict[str, dict[tuple[str, str, str], CotahistRow]] = {}
    for row in rows:
        by_ticker = grouped.setdefault(row.ticker, {})
        by_ticker[(row.isin, row.market_type, row.bdi_code)] = row
    latest: dict[str, list[CotahistRow]] = {}
    for ticker, values in grouped.items():
        max_date = max(row.date for row in values.values())
        latest[ticker] = sorted(
            [row for row in values.values() if row.date == max_date],
            key=lambda row: (row.isin, row.market_type, row.bdi_code),
        )
    return latest


def load_b3_missing_isin_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if row.get("exchange") == "B3" and not row.get("isin", "").strip()]


def evaluate_b3_row(target: dict[str, str], source_rows: list[CotahistRow]) -> dict[str, Any]:
    base = {
        "ticker": target["ticker"],
        "exchange": target["exchange"],
        "asset_type": target["asset_type"],
        "name": target["name"],
        "cotahist_ticker": "",
        "cotahist_name": "",
        "cotahist_bdi_code": "",
        "cotahist_market_type": "",
        "cotahist_date": "",
        "cotahist_isin": "",
    }
    if not source_rows:
        return {**base, "decision": "no_cotahist_match"}

    isins = {row.isin for row in source_rows}
    if len(isins) > 1:
        return {**base, "decision": "ambiguous_cotahist_isin"}

    source = source_rows[0]
    base.update(
        {
            "cotahist_ticker": source.ticker,
            "cotahist_name": source.name,
            "cotahist_bdi_code": source.bdi_code,
            "cotahist_market_type": source.market_type,
            "cotahist_date": source.date,
            "cotahist_isin": source.isin,
        }
    )
    return {**base, "decision": "accept"}


def verify_b3_missing_isins(target_rows: list[dict[str, str]], source_rows: list[CotahistRow]) -> list[dict[str, Any]]:
    source_by_ticker = latest_rows_by_ticker(source_rows)
    return [evaluate_b3_row(row, source_by_ticker.get(row["ticker"].strip().upper(), [])) for row in target_rows]


def build_metadata_updates(results: list[dict[str, Any]], years: list[int]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    source_note = ", ".join(str(year) for year in sorted(years, reverse=True))
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "isin",
                "decision": "update",
                "proposed_value": result["cotahist_isin"],
                "confidence": "0.95",
                "reason": (
                    "Official B3 COTAHIST annual files supplied a valid BR ISIN for an exact B3 trading code; "
                    "accepted only for spot/fractional market records after ticker, BR-prefix, and ISIN-checksum gates. "
                    f"Years checked: {source_note}."
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


def load_source_rows(years: list[int], zip_paths: list[Path], timeout_seconds: float) -> list[CotahistRow]:
    rows: list[CotahistRow] = []
    for zip_path in zip_paths:
        rows.extend(parse_cotahist_text(read_cotahist_zip(zip_path.read_bytes())))
    for year in years:
        rows.extend(parse_cotahist_text(read_cotahist_zip(download_cotahist_zip(year, timeout_seconds=timeout_seconds))))
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill B3 missing ISINs from official annual COTAHIST files.")
    parser.add_argument("--year", type=int, action="append", help="Annual COTAHIST year to download. Can be repeated.")
    parser.add_argument("--zip-path", type=Path, action="append", default=[], help="Local COTAHIST_A*.ZIP path. Can be repeated.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    years = args.year or [2026, 2025]
    source_rows = load_source_rows(years, args.zip_path, args.timeout_seconds)
    target_rows = load_b3_missing_isin_rows(args.tickers_csv)
    results = verify_b3_missing_isins(target_rows, source_rows)
    updates = build_metadata_updates(results, years)

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
                "accepted_isin_updates": len(updates),
                "applied": args.apply,
                "candidates": len(target_rows),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "json_out": display_path(args.json_out),
                "source_rows": len(source_rows),
                "years": years,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
