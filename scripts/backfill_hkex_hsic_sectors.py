from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.fetch_exchange_masterfiles import HKEX_SECURITIES_LIST_URL
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "hkex_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "hsic_browser_capture.json"
DEFAULT_CAPTURE_CSV = DEFAULT_OUTPUT_DIR / "hsic_browser_capture.csv"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "hsic_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "hsic_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
DEFAULT_CHROME_EXECUTABLE = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

HKEX_QUOTE_URL = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym={sym}&sc_lang=en"
HKEX_HSIC_TOP_LEVELS = {
    "Conglomerates",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Healthcare",
    "Industrials",
    "Information Technology",
    "Materials",
    "Properties & Construction",
    "Telecommunications",
    "Utilities",
}
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "hkex_quote_url",
    "hkex_heading",
    "hkex_industry",
    "sector_update",
    "decision",
    "error",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def quote_symbol(ticker: str) -> str:
    return str(int(ticker)) if ticker.isdigit() else ticker.strip().upper()


def parse_hsic_industry(text: str) -> str:
    text = " ".join(text.split())
    if "(HSIC)" not in text:
        return ""
    before_hsic = text.rsplit("(HSIC)", 1)[0]
    if " Industry " in before_hsic:
        return before_hsic.rsplit(" Industry ", 1)[1].strip()
    match = re.search(r"\bIndustry\s+(.+)$", before_hsic)
    return match.group(1).strip() if match else ""


def is_plausible_hsic_industry(industry: str) -> bool:
    top = industry.split(" - ", 1)[0].strip()
    return top in HKEX_HSIC_TOP_LEVELS


def hsic_to_canonical(industry: str) -> str:
    parts = [part.strip() for part in industry.split(" - ") if part.strip()]
    if not parts:
        return ""
    top = parts[0]
    second = parts[1] if len(parts) > 1 else ""
    if top == "Properties & Construction":
        if second == "Properties":
            return "Real Estate"
        if second == "Construction":
            return "Industrials"
        return ""
    if top == "Healthcare":
        return "Health Care"
    if top == "Telecommunications":
        return "Communication Services"
    if top == "Conglomerates":
        return ""
    return normalize_sector(top, "Stock")


def load_hkex_targets(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "HKEX"
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip()
        and not row.get("stock_sector", "").strip()
    ]


def load_hkex_official_isins(url: str = HKEX_SECURITIES_LIST_URL) -> dict[str, str]:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"}, timeout=30)
    response.raise_for_status()
    dataframe = pd.read_excel(BytesIO(response.content), sheet_name=0, header=2, dtype=str)
    result: dict[str, str] = {}
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Stock Code") or "").strip().zfill(5)
        category = str(record.get("Category") or "").strip()
        isin = str(record.get("ISIN") or "").strip().upper()
        if ticker and category in {"Equity", "Real Estate Investment Trusts"} and is_valid_isin(isin):
            result[ticker] = isin
    return result


def capture_hkex_hsic(
    targets: list[dict[str, str]],
    *,
    chrome_executable: str,
    output_json: Path,
    output_csv: Path,
    limit: int | None = None,
    timeout_ms: int = 25_000,
) -> list[dict[str, Any]]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    selected = targets[:limit] if limit is not None else targets
    output_json.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict[str, Any]] = []
    if output_json.exists():
        existing = json.loads(output_json.read_text(encoding="utf-8"))
    captured_by_ticker = {str(row.get("ticker", "")): row for row in existing if row.get("ticker")}

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=chrome_executable)
        page = browser.new_page(user_agent="Mozilla/5.0 free-ticker-database/3.0")
        for index, target in enumerate(selected, start=1):
            ticker = target["ticker"]
            if (
                ticker in captured_by_ticker
                and is_plausible_hsic_industry(str(captured_by_ticker[ticker].get("hkex_industry") or ""))
                and not captured_by_ticker[ticker].get("error")
            ):
                continue
            url = HKEX_QUOTE_URL.format(sym=quote_symbol(ticker))
            result: dict[str, Any] = {
                "ticker": ticker,
                "exchange": target["exchange"],
                "asset_type": target["asset_type"],
                "name": target["name"],
                "isin": target.get("isin", ""),
                "hkex_quote_url": url,
                "hkex_heading": "",
                "hkex_industry": "",
                "error": "",
            }
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.get_by_text("Industry", exact=True).wait_for(timeout=timeout_ms)
                body_text = page.locator("body").inner_text(timeout=timeout_ms)
                result["hkex_industry"] = parse_hsic_industry(body_text)
                headings = page.locator("h1").all_text_contents()
                result["hkex_heading"] = headings[0].strip() if headings else ""
                if not result["hkex_industry"]:
                    result["error"] = "missing_hsic_industry"
            except PlaywrightTimeoutError as exc:
                result["error"] = f"timeout: {exc}"
            except Exception as exc:  # noqa: BLE001
                result["error"] = str(exc)
            captured_by_ticker[ticker] = result
            if index % 25 == 0:
                write_capture(output_json, output_csv, list(captured_by_ticker.values()))
        browser.close()

    rows = [captured_by_ticker[target["ticker"]] for target in selected if target["ticker"] in captured_by_ticker]
    write_capture(output_json, output_csv, rows)
    return rows


def write_capture(json_path: Path, csv_path: Path, rows: list[dict[str, Any]]) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES[:-2] + ["error"])
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in writer.fieldnames or []})


def evaluate_rows(
    targets: list[dict[str, str]],
    captures: list[dict[str, Any]],
    official_isins: dict[str, str],
) -> list[dict[str, Any]]:
    capture_by_ticker = {str(row.get("ticker", "")): row for row in captures}
    results: list[dict[str, Any]] = []
    for target in targets:
        capture = capture_by_ticker.get(target["ticker"], {})
        industry = str(capture.get("hkex_industry") or "").strip()
        sector_update = hsic_to_canonical(industry)
        official_isin = official_isins.get(target["ticker"], "")
        isin = target.get("isin", "").strip().upper()
        base = {
            "ticker": target["ticker"],
            "exchange": target["exchange"],
            "asset_type": target["asset_type"],
            "name": target["name"],
            "isin": isin,
            "hkex_quote_url": capture.get("hkex_quote_url") or HKEX_QUOTE_URL.format(sym=quote_symbol(target["ticker"])),
            "hkex_heading": capture.get("hkex_heading", ""),
            "hkex_industry": industry,
            "sector_update": sector_update,
            "error": capture.get("error", ""),
        }
        if not capture:
            results.append({**base, "decision": "missing_capture"})
        elif capture.get("error") and not industry:
            results.append({**base, "decision": "capture_error"})
        elif not official_isin or official_isin != isin:
            results.append({**base, "decision": "official_isin_mismatch"})
        elif not industry:
            results.append({**base, "decision": "missing_hsic_industry"})
        elif not sector_update:
            results.append({**base, "decision": "unsupported_hsic_sector"})
        else:
            results.append({**base, "decision": "accept"})
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": (
                "HKEX official quote page exposed HSIC industry metadata for a stock missing stock_sector; "
                "accepted only after ticker and ISIN matched the official HKEX ListOfSecurities workbook. "
                f"Source: {result['hkex_quote_url']}"
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
    parser = argparse.ArgumentParser(description="Backfill HKEX stock sectors from HKEX quote-page HSIC metadata.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--capture-json", type=Path, default=DEFAULT_CAPTURE_JSON)
    parser.add_argument("--capture-csv", type=Path, default=DEFAULT_CAPTURE_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--chrome-executable", default=DEFAULT_CHROME_EXECUTABLE)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--timeout-ms", type=int, default=25_000)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    targets = load_hkex_targets(args.tickers_csv)
    if args.limit is not None:
        targets = targets[: args.limit]
    if args.capture:
        captures = capture_hkex_hsic(
            targets,
            chrome_executable=args.chrome_executable,
            output_json=args.capture_json,
            output_csv=args.capture_csv,
            timeout_ms=args.timeout_ms,
        )
    else:
        captures = json.loads(args.capture_json.read_text(encoding="utf-8"))

    official_isins = load_hkex_official_isins()
    results = evaluate_rows(targets, captures, official_isins)
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
                "capture_rows": len(captures),
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
