from __future__ import annotations

import argparse
import csv
import html
import json
import sys
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin

SEC_THAILAND_ISIN_URL = "https://market.sec.or.th/public/idisc/th/ISIN"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "set_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sec_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sec_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "sec_security_type",
    "sec_name",
    "sec_symbol",
    "sec_isin_local",
    "decision",
]


class FormAndTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.inputs: dict[str, str] = {}
        self.in_cell = False
        self.current_cell: list[str] = []
        self.current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {key: value or "" for key, value in attrs}
        if tag == "input" and attr.get("name"):
            self.inputs[attr["name"]] = attr.get("value", "")
        if tag in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []
        if tag == "tr":
            self.current_row = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.in_cell:
            self.current_row.append(" ".join("".join(self.current_cell).split()))
            self.in_cell = False
        if tag == "tr" and self.current_row:
            self.rows.append(self.current_row)

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.current_cell.append(html.unescape(data))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def parse_sec_isin_rows(page_html: str) -> list[dict[str, str]]:
    parser = FormAndTableParser()
    parser.feed(page_html)
    parsed: list[dict[str, str]] = []
    for index, row in enumerate(parser.rows):
        if {"ชื่อย่อหลักทรัพย์", "ISIN Local"} <= set(row):
            headers = row
            for values in parser.rows[index + 1 :]:
                if len(values) < len(headers):
                    continue
                record = {header: values[pos] for pos, header in enumerate(headers)}
                ticker = record.get("ชื่อย่อหลักทรัพย์", "").strip().upper()
                isin = record.get("ISIN Local", "").strip().upper()
                if ticker and isin:
                    parsed.append(
                        {
                            "sec_security_type": record.get("ประเภทหลักทรัพย์", "").strip(),
                            "sec_name": record.get("ชื่อหลักทรัพย์", "").strip(),
                            "sec_symbol": ticker,
                            "sec_isin_local": isin,
                        }
                    )
            break
    return parsed


def extract_form_inputs(page_html: str) -> dict[str, str]:
    parser = FormAndTableParser()
    parser.feed(page_html)
    return parser.inputs


def fetch_sec_isin_rows_for_symbol(
    symbol: str,
    *,
    session: requests.Session,
    timeout_seconds: float,
) -> list[dict[str, str]]:
    response = session.get(SEC_THAILAND_ISIN_URL, timeout=timeout_seconds)
    response.raise_for_status()
    data = extract_form_inputs(response.text)
    data.update(
        {
            "ctl00$CPH$ddlSecuTypeCode": "CS",
            "ctl00$CPH$BsCompany": "",
            "ctl00$CPH$BsCompany_t": "",
            "ctl00$CPH$BsCompany_v": "",
            "ctl00$CPH$BsCompany_d": "",
            "ctl00$CPH$txSecuAbbrName": symbol,
            "ctl00$CPH$txISINCode": "",
            "ctl00$CPH$btSearch": "ค้นหา",
        }
    )
    search_response = session.post(SEC_THAILAND_ISIN_URL, data=data, timeout=timeout_seconds)
    search_response.raise_for_status()
    return parse_sec_isin_rows(search_response.text)


def load_set_missing_isin_rows(path: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "SET"
        and row.get("asset_type") == "Stock"
        and not row.get("isin", "").strip()
    ]


def evaluate_row(target: dict[str, str], sec_rows: list[dict[str, str]]) -> dict[str, str]:
    base = {
        "ticker": target.get("ticker", ""),
        "exchange": target.get("exchange", ""),
        "asset_type": target.get("asset_type", ""),
        "name": target.get("name", ""),
        "sec_security_type": "",
        "sec_name": "",
        "sec_symbol": "",
        "sec_isin_local": "",
    }
    matches = [row for row in sec_rows if row.get("sec_symbol") == target.get("ticker", "").upper()]
    if not matches:
        return {**base, "decision": "no_sec_isin_match"}
    unique_isins = {row.get("sec_isin_local", "") for row in matches if row.get("sec_isin_local", "")}
    if len(unique_isins) != 1:
        return {**base, "decision": "ambiguous_sec_isin"}
    match = matches[0]
    base.update(match)
    if not is_valid_isin(match["sec_isin_local"]) or not match["sec_isin_local"].startswith("TH"):
        return {**base, "decision": "invalid_or_non_th_isin"}
    return {**base, "decision": "accept"}


def build_metadata_updates(results: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "isin",
            "decision": "update",
            "proposed_value": result["sec_isin_local"],
            "confidence": "0.97",
            "reason": (
                "Official Thailand SEC ISIN search returned one common-stock ISIN Local for this exact SET symbol; "
                "accepted only after exact symbol, common-stock security type, TH country prefix, and ISIN checksum gates matched."
            ),
        }
        for result in results
        if result.get("decision") == "accept"
    ]


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill SET missing ISINs from the official Thailand SEC ISIN search.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    targets = load_set_missing_isin_rows(args.tickers_csv)
    if args.limit is not None:
        targets = targets[: args.limit]

    session = requests.Session()
    results = [
        evaluate_row(target, fetch_sec_isin_rows_for_symbol(target["ticker"], session=session, timeout_seconds=args.timeout_seconds))
        for target in targets
    ]
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row.get("decision") == "accept"], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    print(
        json.dumps(
            {
                "accepted_isin_updates": len(updates),
                "applied": args.apply,
                "candidates": len(targets),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(row["decision"] for row in results)),
                "json_out": display_path(args.json_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
