from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_primary_same_issuer_sectors import issuer_key, key_usable
from scripts.fetch_exchange_masterfiles import (
    fetch_jpx_tse_stock_detail_payload,
    normalize_jpx_33_industry_sector,
)
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin

JPX_LISTED_ISSUES_URL = (
    "https://www.jpx.co.jp/english/markets/statistics-equities/misc/"
    "tvdivq0000001vg2-att/data_e.xlsx"
)
DEFAULT_OUTPUT_DIR = ROOT / "data" / "jpx_verification"
DEFAULT_XLSX_PATH = DEFAULT_OUTPUT_DIR / "listed_issues_e.xlsx"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_detail_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
REASON = (
    "Copied stock_sector from the official JPX listed-issues 33-industry after an exact "
    "issuer key match whose local code came from that workbook, then an exact ISIN match "
    "on the official JPX stock_detail quote."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "jpx_code",
    "jpx_name",
    "jpx_industry",
    "jpx_detail_isin",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalize_local_code(value: Any) -> str:
    code = str(value or "").strip()
    if code.endswith(".0"):
        code = code[:-2]
    return code


def normalize_industry_label(value: str) -> str:
    return " ".join((value or "").replace("&", " and ").split())


def parse_listed_issue_rows(xls_bytes: bytes) -> list[dict[str, str]]:
    import pandas as pd

    frame = pd.read_excel(BytesIO(xls_bytes)).fillna("")
    rows: list[dict[str, str]] = []
    for record in frame.to_dict("records"):
        code = normalize_local_code(record.get("Local Code") or record.get("コード"))
        name = str(record.get("Name (English)") or record.get("銘柄名") or "").strip()
        industry = str(record.get("33 Sector(name)") or record.get("33業種区分") or "").strip()
        section = str(record.get("Section/Products") or record.get("市場・商品区分") or "").strip()
        if not code or not name:
            continue
        if "etf" in section.lower() or "etn" in section.lower():
            continue
        key = issuer_key(name)
        if not key_usable(key):
            continue
        rows.append(
            {
                "local_code": code,
                "name": name,
                "industry": industry,
                "section": section,
                "issuer_key": key,
            }
        )
    return rows


def index_by_issuer_key(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[row["issuer_key"]].append(row)
    return indexed


def extract_stock_detail(payload: dict[str, Any]) -> dict[str, str]:
    section = payload.get("section1", {})
    data = section.get("data") if isinstance(section, dict) else None
    if not isinstance(data, dict) or not data:
        return {}
    detail = next(iter(data.values()))
    if not isinstance(detail, dict):
        return {}
    isin = str(detail.get("ISIN") or "").strip().upper()
    industry = str(detail.get("JSECE_CNV") or detail.get("JSEC_CNV") or "").strip()
    code = normalize_local_code(detail.get("TTCODE2"))
    name = str(detail.get("FLLNE") or detail.get("NAMEE") or "").strip()
    if not is_valid_isin(isin):
        return {}
    return {"isin": isin, "industry": industry, "local_code": code, "name": name}


def load_missing_rows(path: Path = LISTINGS_CSV, *, exchanges: set[str] | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected: list[dict[str, str]] = []
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        if exchanges is not None and row.get("exchange") not in exchanges:
            continue
        if (row.get("stock_sector") or row.get("sector") or "").strip():
            continue
        selected.append(row)
    return selected


def evaluate_row(
    row: dict[str, str],
    by_key: dict[str, list[dict[str, str]]],
    details_by_code: dict[str, dict[str, str]],
) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "jpx_code": "",
        "jpx_name": "",
        "jpx_industry": "",
        "jpx_detail_isin": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin) or not isin.startswith("JP"):
        return {**base, "decision": "not_jp_isin"}
    key = issuer_key(row.get("name") or "")
    if not key_usable(key):
        return {**base, "decision": "short_issuer_key"}
    matches = by_key.get(key, [])
    if not matches:
        return {**base, "decision": "no_jpx_name_match"}
    if len(matches) > 1:
        return {**base, "decision": "ambiguous_jpx_name"}
    issue = matches[0]
    base["jpx_code"] = issue.get("local_code", "")
    base["jpx_name"] = issue.get("name", "")
    detail = details_by_code.get(issue.get("local_code", ""), {})
    if not detail:
        return {**base, "decision": "missing_stock_detail"}
    detail_isin = (detail.get("isin") or "").strip().upper()
    base["jpx_detail_isin"] = detail_isin
    if detail_isin != isin:
        return {**base, "decision": "isin_mismatch"}
    industry = normalize_industry_label(detail.get("industry") or issue.get("industry") or "")
    base["jpx_industry"] = industry
    sector = normalize_jpx_33_industry_sector(industry, "Stock")
    if not sector:
        return {**base, "decision": "unmapped_jpx_industry"}
    return {**base, "sector_update": sector, "decision": "accept"}


def candidate_codes(rows: list[dict[str, str]], by_key: dict[str, list[dict[str, str]]]) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if not is_valid_isin(isin) or not isin.startswith("JP"):
            continue
        key = issuer_key(row.get("name") or "")
        if not key_usable(key):
            continue
        matches = by_key.get(key, [])
        if len(matches) != 1:
            continue
        code = matches[0].get("local_code", "")
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def download_listed_issues(url: str, *, timeout_seconds: float = 60.0) -> bytes:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read()


def load_details_capture(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    details: dict[str, dict[str, str]] = {}
    for code, item in payload.items():
        if isinstance(item, dict) and item.get("isin"):
            details[str(code)] = {
                "isin": str(item.get("isin") or "").strip().upper(),
                "industry": str(item.get("industry") or "").strip(),
                "local_code": str(item.get("local_code") or code).strip(),
                "name": str(item.get("name") or "").strip(),
            }
    return details


def fetch_details(codes: list[str], existing: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    details = dict(existing)
    for code in codes:
        if code in details:
            continue
        try:
            parsed = extract_stock_detail(fetch_jpx_tse_stock_detail_payload(code))
        except Exception as exc:
            print(f"JPX stock_detail skip {code}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        if parsed:
            details[code] = parsed
    return details


def verify_rows(
    rows: list[dict[str, str]],
    listed_rows: list[dict[str, str]],
    details_by_code: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    indexed = index_by_issuer_key(listed_rows)
    return [evaluate_row(row, indexed, details_by_code) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source local code {result['jpx_code']}.",
        }
        for result in results
        if result.get("decision") == "accept"
    ]


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill missing JP-ISIN stock sectors from official JPX listed-issues "
            "codes after exact stock_detail ISIN confirmation."
        )
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--xls-path", type=Path, default=DEFAULT_XLSX_PATH)
    parser.add_argument("--xls-url", default=JPX_LISTED_ISSUES_URL)
    parser.add_argument("--capture-json", type=Path, default=DEFAULT_CAPTURE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else {"FSX"}
    if args.refresh or not args.xls_path.exists():
        content = download_listed_issues(args.xls_url)
        args.xls_path.parent.mkdir(parents=True, exist_ok=True)
        args.xls_path.write_bytes(content)
    else:
        content = args.xls_path.read_bytes()
    listed_rows = parse_listed_issue_rows(content)
    missing = load_missing_rows(args.listings_csv, exchanges=exchanges)
    by_key = index_by_issuer_key(listed_rows)
    codes = candidate_codes(missing, by_key)
    details = load_details_capture(args.capture_json)
    if args.refresh or any(code not in details for code in codes):
        details = fetch_details(codes, details)
        args.capture_json.parent.mkdir(parents=True, exist_ok=True)
        args.capture_json.write_text(json.dumps(details, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    results = verify_rows(missing, listed_rows, details)
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
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "detail_codes": len(codes),
                "json_out": display_path(args.json_out),
                "listed_rows": len(listed_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
