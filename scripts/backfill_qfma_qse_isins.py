from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.lib.normalize import names_match
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


QFMA_MAIN_MARKET_URL = (
    "https://www.qfma.org.qa/English/Securities/MainMarketShares/"
    "_vti_bin/QFMA/Service.svc/GetAllCompaniesMainMarket"
)
QFMA_SECOND_MARKET_URL = (
    "https://www.qfma.org.qa/English/Securities/SecondMarketShares/"
    "_vti_bin/QFMA/Service.svc/GetAllCompaniesSecondMarket"
)
QFMA_ETFS_URL = "https://www.qfma.org.qa/English/Securities/ETFs/_vti_bin/QFMA/Service.svc/GetAllListedETFS"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "qfma_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "qse_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "qse_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "qfma_company_code",
    "qfma_company_name",
    "qfma_sector",
    "qfma_isin",
    "name_match",
    "decision",
    "source_url",
    "verification_evidence_required",
    "identity_gate_context",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_missing_qse_rows(path: Path) -> list[dict[str, str]]:
    return [
        row
        for row in load_csv(path)
        if row.get("exchange") == "QSE"
        and row.get("asset_type") in {"ETF", "Stock"}
        and not row.get("isin", "").strip()
    ]


def clean_isin(value: str) -> str:
    match = re.search(r"\b([A-Z]{2}[A-Z0-9]{10})\b", value.upper())
    return match.group(1) if match else ""


def fetch_qfma_payload(url: str, component_title: str, timeout_seconds: float) -> dict[str, Any]:
    body = json.dumps({"ComponentTitle": component_title}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "free-ticker-database/3.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_qfma_main_market_rows(payload: dict[str, Any], source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for group in payload.get("results") or []:
        for item in group.get("Items") or []:
            ticker = str(item.get("CompanyCode") or "").strip().upper()
            isin = clean_isin(str(item.get("ISIN") or ""))
            if not ticker:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "name": str(item.get("CompanyName") or "").strip(),
                    "sector": str(item.get("Sector") or group.get("Sector") or "").strip(),
                    "isin": isin,
                    "active": "true" if item.get("Active") is True else "false",
                    "source_url": source_url,
                }
            )
    return rows


def identity_gate_context(row: dict[str, Any]) -> str:
    return (
        f"ticker_exact_match={'true' if row.get('ticker') == row.get('qfma_company_code') else 'false'};"
        f"valid_isin_checksum={'true' if is_valid_isin(str(row.get('qfma_isin') or '')) else 'false'};"
        f"qfma_active_source={row.get('qfma_active', '') or 'false'};"
        f"name_match={'true' if row.get('name_match') else 'false'}"
    )


def evaluate_rows(target_rows: list[dict[str, str]], qfma_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_ticker = {row["ticker"]: row for row in qfma_rows if row.get("active") == "true"}
    results: list[dict[str, Any]] = []
    for target in target_rows:
        ticker = target.get("ticker", "").strip().upper()
        source = by_ticker.get(ticker)
        base: dict[str, Any] = {
            "ticker": target.get("ticker", ""),
            "exchange": target.get("exchange", ""),
            "asset_type": target.get("asset_type", ""),
            "name": target.get("name", ""),
            "qfma_company_code": source.get("ticker", "") if source else "",
            "qfma_company_name": source.get("name", "") if source else "",
            "qfma_sector": source.get("sector", "") if source else "",
            "qfma_isin": source.get("isin", "") if source else "",
            "qfma_active": source.get("active", "") if source else "",
            "source_url": source.get("source_url", QFMA_MAIN_MARKET_URL) if source else QFMA_MAIN_MARKET_URL,
            "verification_evidence_required": "official_qfma_market_row_with_exact_company_code_and_valid_isin_checksum",
        }
        base["name_match"] = bool(
            base["qfma_company_name"] and names_match(base["qfma_company_name"], target.get("name", ""))
        )
        if not source:
            base["decision"] = "no_qfma_company_code_match"
        elif not base["qfma_isin"]:
            base["decision"] = "missing_qfma_isin"
        elif not is_valid_isin(base["qfma_isin"]):
            base["decision"] = "invalid_qfma_isin"
        else:
            base["decision"] = "accept"
        base["identity_gate_context"] = identity_gate_context(base)
        results.append(base)
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "isin",
            "decision": "update",
            "proposed_value": result["qfma_isin"],
            "confidence": "0.92",
            "reason": (
                "Official QFMA security source supplied a valid ISIN for the exact QSE company code; "
                "accepted only after exact ticker/company-code match and repo ISIN checksum gate. "
                f"Source: {result['source_url']}"
            ),
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
        description="Backfill QSE stock and ETF ISINs from official QFMA market lists."
    )
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--source-json", type=Path)
    parser.add_argument("--second-source-json", type=Path)
    parser.add_argument("--etfs-source-json", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.source_json:
        main_payload = json.loads(args.source_json.read_text(encoding="utf-8"))
    else:
        main_payload = fetch_qfma_payload(QFMA_MAIN_MARKET_URL, "CompaniesMainMarket", args.timeout_seconds)
    if args.second_source_json:
        second_payload = json.loads(args.second_source_json.read_text(encoding="utf-8"))
    else:
        second_payload = fetch_qfma_payload(QFMA_SECOND_MARKET_URL, "CompaniesSecondMarket", args.timeout_seconds)
    if args.etfs_source_json:
        etfs_payload = json.loads(args.etfs_source_json.read_text(encoding="utf-8"))
    else:
        etfs_payload = fetch_qfma_payload(QFMA_ETFS_URL, "CompaniesEtfs", args.timeout_seconds)
    qfma_rows = [
        *parse_qfma_main_market_rows(main_payload, QFMA_MAIN_MARKET_URL),
        *parse_qfma_main_market_rows(second_payload, QFMA_SECOND_MARKET_URL),
        *parse_qfma_main_market_rows(etfs_payload, QFMA_ETFS_URL),
    ]
    target_rows = load_missing_qse_rows(args.tickers_csv)
    results = evaluate_rows(target_rows, qfma_rows)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    summary = {
        "accepted_isin_updates": len(updates),
        "applied": args.apply,
        "candidates": len(target_rows),
        "csv_out": display_path(args.csv_out),
        "decision_counts": dict(Counter(row["decision"] for row in results)),
        "generated_at": utc_now_iso(),
        "json_out": display_path(args.json_out),
        "qfma_rows": len(qfma_rows),
        "source_urls": [QFMA_MAIN_MARKET_URL, QFMA_SECOND_MARKET_URL, QFMA_ETFS_URL],
        "policy": {
            "official_source": "QFMA main-market, venture-market, and ETF service responses.",
            "no_guessing": "ISIN updates require exact QSE ticker to QFMA CompanyCode match and a valid checksum.",
            "traceability": "Every target row is written to the probe CSV with source and identity gate context.",
        },
    }
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": results}, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
