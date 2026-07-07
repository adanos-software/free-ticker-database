from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pdfplumber
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.normalize import names_match
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


CBOE_LMM_CSV_URL = "https://www.cboe.com/us/equities/listings/trade_on_cboe/lmm/securities/csv/"
CBOE_GLOBAL_FUND_LISTINGS_PDF_URL = "https://cdn.cboe.com/resources/listings/Cboe_USL_GlobalFundListings.pdf"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "cboe_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "us_lmm_metadata_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "us_lmm_metadata_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
DEFAULT_ENTRY_QUALITY_CSV = ROOT / "data" / "reports" / "entry_quality.csv"
DEFAULT_EXISTING_ISINS_CSV = ROOT / "data" / "listings.csv"

ASSET_CLASS_CATEGORY_MAP = {
    "Fixed Income": "Fixed Income",
    "International Equity": "Equity",
    "Outcome-Based": "Alternative",
    "Other": "Other",
    "Single Stock": "Other",
    "US Equity": "Equity",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "cboe_name",
    "cboe_asset_class",
    "cboe_isin",
    "category_update",
    "isin_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fetch_text(url: str, timeout_seconds: float) -> str:
    response = requests.get(url, headers={"User-Agent": "free-ticker-database/3.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def fetch_bytes(url: str, timeout_seconds: float) -> bytes:
    response = requests.get(url, headers={"User-Agent": "free-ticker-database/3.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def parse_lmm_csv(text: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for row in csv.DictReader(io.StringIO(text)):
        ticker = str(row.get("symbol") or "").strip().upper()
        name = str(row.get("security_name") or "").strip()
        asset_class = str(row.get("asset_class") or "").strip()
        if not ticker or not name or asset_class not in ASSET_CLASS_CATEGORY_MAP:
            continue
        rows[ticker] = {"name": name, "asset_class": asset_class}
    return rows


def parse_global_fund_listing_pdf(content: bytes) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    pattern = re.compile(
        r"^US: Cboe BZX\s+(?P<ticker>\S+)\s+(?P<name>.+?)\s+(?P<isin>US[A-Z0-9]{10})\s+\d{4}-\d{2}-\d{2}\s+",
        re.MULTILINE,
    )
    for match in pattern.finditer(text):
        ticker = match.group("ticker").strip().upper()
        isin = match.group("isin").strip().upper()
        if ticker and is_valid_isin(isin):
            rows[ticker] = {"name": match.group("name").strip(), "isin": isin}
    return rows


def load_target_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "BATS"
        and row.get("asset_type") == "ETF"
        and (not row.get("etf_category", "").strip() or not row.get("isin", "").strip())
        ]


def load_entry_quality_bats_etf_residual_rows(path: Path = DEFAULT_ENTRY_QUALITY_CSV) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "BATS"
        and row.get("asset_type") == "ETF"
        and (
            "missing_etf_category" in row.get("issue_types", "").split("|")
            or "expected_missing_primary_isin" in row.get("issue_types", "").split("|")
        )
    ]


def target_missing_category(target: dict[str, str]) -> bool:
    issue_types = target.get("issue_types", "")
    if issue_types:
        return "missing_etf_category" in issue_types.split("|")
    return not target.get("etf_category", "").strip()


def target_missing_isin(target: dict[str, str]) -> bool:
    issue_types = target.get("issue_types", "")
    if issue_types:
        return "expected_missing_primary_isin" in issue_types.split("|")
    return not target.get("isin", "").strip()


def normalize_cboe_product_name(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.lower()))


def cboe_etf_names_match(source_name: str, target_name: str) -> bool:
    if names_match(source_name, target_name):
        return True
    if " - " not in target_name:
        return False
    wrapper, product_name = target_name.rsplit(" - ", 1)
    wrapper_tokens = normalize_cboe_product_name(wrapper).split()
    if not any(token in {"trust", "series", "fund"} for token in wrapper_tokens):
        return False
    return normalize_cboe_product_name(source_name) == normalize_cboe_product_name(product_name)


def evaluate_rows(
    targets: list[dict[str, str]],
    lmm_rows: dict[str, dict[str, str]],
    fund_listing_rows: dict[str, dict[str, str]],
    *,
    existing_isins: set[str] | None = None,
) -> list[dict[str, Any]]:
    existing_isins = existing_isins or set()
    results: list[dict[str, Any]] = []
    for target in targets:
        ticker = target["ticker"].strip().upper()
        base = {
            "ticker": target["ticker"],
            "exchange": target["exchange"],
            "asset_type": target["asset_type"],
            "name": target["name"],
            "cboe_name": "",
            "cboe_asset_class": "",
            "cboe_isin": "",
            "category_update": "",
            "isin_update": "",
        }
        lmm = lmm_rows.get(ticker)
        fund = fund_listing_rows.get(ticker)
        if not lmm and not fund:
            results.append({**base, "decision": "no_cboe_match"})
            continue

        accepted_fields: list[str] = []
        matched_name = False
        rejected_duplicate_isin = False
        lmm_name_match = False
        if lmm:
            base["cboe_name"] = lmm["name"]
            base["cboe_asset_class"] = lmm["asset_class"]
            lmm_name_match = cboe_etf_names_match(lmm["name"], target["name"])
            matched_name = matched_name or lmm_name_match
            if target_missing_category(target) and lmm_name_match:
                base["category_update"] = ASSET_CLASS_CATEGORY_MAP[lmm["asset_class"]]
                accepted_fields.append("etf_category")

        if fund:
            if not base["cboe_name"]:
                base["cboe_name"] = fund["name"]
            base["cboe_isin"] = fund["isin"]
            fund_name_match = cboe_etf_names_match(fund["name"], target["name"]) or (
                lmm_name_match
                and bool(lmm)
                and normalize_cboe_product_name(fund["name"]).startswith(normalize_cboe_product_name(lmm["name"]))
            )
            matched_name = matched_name or fund_name_match
            if target_missing_isin(target) and fund_name_match:
                if fund["isin"] in existing_isins:
                    rejected_duplicate_isin = True
                else:
                    base["isin_update"] = fund["isin"]
                    accepted_fields.append("isin")

        if accepted_fields:
            results.append({**base, "decision": "accept_" + "_".join(accepted_fields)})
        elif rejected_duplicate_isin:
            results.append({**base, "decision": "isin_already_present"})
        elif matched_name:
            results.append({**base, "decision": "no_update"})
        else:
            results.append({**base, "decision": "name_mismatch"})
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("category_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "etf_category",
                    "decision": "update",
                    "proposed_value": result["category_update"],
                    "confidence": "0.86",
                    "reason": (
                        "Cboe U.S. LMM Securities CSV supplied an official asset-class label for a BATS ETF; "
                        "accepted only after exact ticker and ETF-name gates matched. "
                        f"Source: {CBOE_LMM_CSV_URL}"
                    ),
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
                    "confidence": "0.84",
                    "reason": (
                        "Cboe U.S. Global Fund Listings PDF supplied a valid ISIN for a BATS ETF; "
                        "accepted only after exact ticker, ETF-name, US-prefix, and checksum gates matched. "
                        f"Source: {CBOE_GLOBAL_FUND_LISTINGS_PDF_URL}"
                    ),
                }
            )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill BATS ETF metadata from official Cboe U.S. listings files.")
    parser.add_argument("--entry-quality-csv", type=Path, default=DEFAULT_ENTRY_QUALITY_CSV)
    parser.add_argument(
        "--tickers-csv",
        type=Path,
        help="Optional broader source CSV probe; defaults to entry_quality BATS ETF residual rows.",
    )
    parser.add_argument("--lmm-csv", type=Path)
    parser.add_argument("--fund-listings-pdf", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument(
        "--existing-isins-csv",
        type=Path,
        default=DEFAULT_EXISTING_ISINS_CSV,
        help="CSV used for duplicate-ISIN protection before applying Cboe ISIN updates.",
    )
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    lmm_text = args.lmm_csv.read_text(encoding="utf-8") if args.lmm_csv else fetch_text(CBOE_LMM_CSV_URL, args.timeout_seconds)
    fund_pdf = (
        args.fund_listings_pdf.read_bytes()
        if args.fund_listings_pdf
        else fetch_bytes(CBOE_GLOBAL_FUND_LISTINGS_PDF_URL, args.timeout_seconds)
    )
    target_rows = (
        load_target_rows(args.tickers_csv)
        if args.tickers_csv is not None
        else load_entry_quality_bats_etf_residual_rows(args.entry_quality_csv)
    )
    with args.existing_isins_csv.open(newline="", encoding="utf-8") as handle:
        existing_isins = {
            row["isin"].strip().upper()
            for row in csv.DictReader(handle)
            if row.get("isin", "").strip()
        }
    results = evaluate_rows(
        target_rows,
        parse_lmm_csv(lmm_text),
        parse_global_fund_listing_pdf(fund_pdf),
        existing_isins=existing_isins,
    )
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"].startswith("accept")], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    print(
        json.dumps(
            {
                "accepted_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
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
