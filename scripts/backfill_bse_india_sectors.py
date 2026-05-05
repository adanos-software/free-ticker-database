from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin, normalize_sector


BSE_LIST_SCRIPS_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
BSE_COM_HEADER_URL = "https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "bse_india_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

BSE_SECTOR_MAP = {
    "Commodities": "Materials",
    "Consumer Discretionary": "Consumer Discretionary",
    "Energy": "Energy",
    "Fast Moving Consumer Goods": "Consumer Staples",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Information Technology": "Information Technology",
    "Telecommunication": "Communication Services",
    "Utilities": "Utilities",
}
BSE_SERVICES_GROUP_MAP = {
    "Commercial Services & Supplies": "Industrials",
    "Transport Services": "Industrials",
    "Leisure Services": "Consumer Discretionary",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "bse_scrip_code",
    "bse_security_id",
    "bse_name",
    "bse_isin",
    "bse_sector",
    "bse_industry",
    "bse_group",
    "bse_subgroup",
    "sector_update",
    "name_match",
    "isin_match",
    "decision",
    "error",
]


@dataclass(frozen=True)
class BseScrip:
    symbol: str
    scrip_code: str
    name: str
    issuer_name: str
    isin: str
    url: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fetch_json(url: str, *, referer: str, timeout_seconds: float) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "Referer": referer,
            "User-Agent": "Mozilla/5.0 free-ticker-database/3.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.load(response)


def fetch_bse_scrips(timeout_seconds: float) -> list[BseScrip]:
    params = urllib.parse.urlencode(
        {
            "Group": "",
            "Scripcode": "",
            "industry": "",
            "segment": "Equity",
            "status": "Active",
        }
    )
    payload = fetch_json(f"{BSE_LIST_SCRIPS_URL}?{params}", referer="https://www.bseindia.com/corporates/List_Scrips.html", timeout_seconds=timeout_seconds)
    if not isinstance(payload, list):
        raise ValueError(f"BSE ListofScripData returned non-list payload: {payload!r}")
    return [
        BseScrip(
            symbol=str(row.get("scrip_id") or "").strip().upper(),
            scrip_code=str(row.get("SCRIP_CD") or "").strip(),
            name=str(row.get("Scrip_Name") or "").strip(),
            issuer_name=str(row.get("Issuer_Name") or "").strip(),
            isin=str(row.get("ISIN_NUMBER") or "").strip().upper(),
            url=str(row.get("NSURL") or "").strip(),
        )
        for row in payload
        if str(row.get("scrip_id") or "").strip() and str(row.get("SCRIP_CD") or "").strip()
    ]


def fetch_bse_com_header(scrip: BseScrip, timeout_seconds: float) -> dict[str, Any]:
    params = urllib.parse.urlencode({"quotetype": "EQ", "scripcode": scrip.scrip_code, "seriesid": ""})
    payload = fetch_json(
        f"{BSE_COM_HEADER_URL}?{params}",
        referer=scrip.url or "https://www.bseindia.com/",
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(payload, dict):
        raise ValueError(f"BSE ComHeader returned non-dict payload for {scrip.symbol}: {payload!r}")
    return payload


def bse_sector_to_canonical(sector: str, group: str) -> str:
    sector = sector.strip()
    group = group.strip()
    if sector == "Services":
        return normalize_sector(BSE_SERVICES_GROUP_MAP.get(group, ""), "Stock")
    return normalize_sector(BSE_SECTOR_MAP.get(sector, ""), "Stock")


def load_targets(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "BSE_IN"
        and row.get("asset_type") == "Stock"
        and not row.get("stock_sector", "").strip()
    ]


def evaluate_row(row: dict[str, str], scrip: BseScrip | None, header: dict[str, Any] | None, error: str = "") -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "bse_scrip_code": scrip.scrip_code if scrip else "",
        "bse_security_id": "",
        "bse_name": scrip.name if scrip else "",
        "bse_isin": "",
        "bse_sector": "",
        "bse_industry": "",
        "bse_group": "",
        "bse_subgroup": "",
        "sector_update": "",
        "name_match": False,
        "isin_match": False,
        "error": error,
    }
    if scrip is None:
        return {**base, "decision": "no_bse_scrip_match"}
    if header is None:
        return {**base, "decision": "fetch_error"}

    bse_isin = str(header.get("ISIN") or scrip.isin or "").strip().upper()
    bse_sector = str(header.get("Sector") or "").strip()
    bse_industry = str(header.get("IndustryNew") or "").strip()
    bse_group = str(header.get("IGroup") or "").strip()
    bse_subgroup = str(header.get("ISubGroup") or "").strip()
    bse_security_id = str(header.get("SecurityId") or "").strip().upper()
    name_match = names_match(scrip.name or scrip.issuer_name, row["name"]) or names_match(scrip.issuer_name, row["name"])
    isin_match = bool(row.get("isin", "").strip()) and bse_isin == row.get("isin", "").strip().upper()
    sector_update = bse_sector_to_canonical(bse_sector, bse_group)
    base.update(
        {
            "bse_security_id": bse_security_id,
            "bse_isin": bse_isin,
            "bse_sector": bse_sector,
            "bse_industry": bse_industry,
            "bse_group": bse_group,
            "bse_subgroup": bse_subgroup,
            "sector_update": sector_update,
            "name_match": name_match,
            "isin_match": isin_match,
        }
    )
    if bse_security_id and bse_security_id != row["ticker"].upper():
        return {**base, "decision": "security_id_mismatch"}
    if row.get("isin", "").strip() and (not bse_isin or not is_valid_isin(bse_isin) or not isin_match):
        return {**base, "decision": "isin_mismatch"}
    if not name_match and not isin_match:
        return {**base, "decision": "name_mismatch"}
    if not sector_update:
        return {**base, "decision": "unsupported_bse_sector"}
    return {**base, "decision": "accept"}


def verify_rows(
    targets: list[dict[str, str]],
    scrips: list[BseScrip],
    *,
    timeout_seconds: float,
    workers: int,
) -> list[dict[str, Any]]:
    scrip_by_symbol = {scrip.symbol: scrip for scrip in scrips}
    results: list[dict[str, Any]] = []
    futures = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for row in targets:
            scrip = scrip_by_symbol.get(row["ticker"].strip().upper())
            if scrip is None:
                results.append(evaluate_row(row, None, None))
                continue
            futures[executor.submit(fetch_bse_com_header, scrip, timeout_seconds)] = (row, scrip)
        for future in as_completed(futures):
            row, scrip = futures[future]
            try:
                header = future.result()
                results.append(evaluate_row(row, scrip, header))
            except Exception as exc:  # noqa: BLE001
                results.append(evaluate_row(row, scrip, None, str(exc)))
    return sorted(results, key=lambda result: result["ticker"])


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.88",
            "reason": (
                "BSE India ComHeader returned official Sector/Industry metadata for a row missing stock_sector; "
                "accepted only after BSE symbol, scrip-code, ISIN/name, and canonical sector gates matched. "
                f"Source: {BSE_COM_HEADER_URL}"
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
    parser = argparse.ArgumentParser(description="Backfill BSE India stock sectors from official BSE ComHeader metadata.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    targets = load_targets(args.tickers_csv)
    if args.limit is not None:
        targets = targets[: args.limit]
    scrips = fetch_bse_scrips(args.timeout_seconds)
    results = verify_rows(targets, scrips, timeout_seconds=args.timeout_seconds, workers=args.workers)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"] == "accept"], indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    print(
        json.dumps(
            {
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "bse_scrips": len(scrips),
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
