from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from html import unescape
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, alias_matches_company, is_valid_isin, normalize_sector

DEFAULT_OUTPUT_DIR = ROOT / "data" / "stockanalysis_verification"
DEFAULT_CSV_OUT = DEFAULT_OUTPUT_DIR / "stockanalysis_metadata_backfill.csv"
DEFAULT_JSON_OUT = DEFAULT_OUTPUT_DIR / "stockanalysis_metadata_backfill.json"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

EXCHANGE_SLUGS = {
    "HOSE": "hose",
    "TPEX": "tpex",
}

USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"


@dataclass(frozen=True)
class StockAnalysisProfile:
    url: str
    name: str
    isin: str
    sector: str
    industry: str


def strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", unescape(value)).strip()


def extract_company_title(html: str) -> str:
    meta_match = re.search(r'<meta property="og:title" content="([^"]+)"', html)
    if meta_match:
        title = unescape(meta_match.group(1))
    else:
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
        title = strip_tags(title_match.group(1)) if title_match else ""
    title = re.sub(r"\s*\([^)]*\)\s*Company Profile.*$", "", title).strip()
    title = re.sub(r"\s+Company Description$", "", title).strip()
    return title


def extract_profile_table_value(html: str, label: str) -> str:
    pattern = rf">{re.escape(label)}</td><td[^>]*>(.*?)</td>"
    match = re.search(pattern, html, re.S)
    if match is None:
        return ""
    return " ".join(strip_tags(match.group(1)).split())


def parse_stockanalysis_company_profile(html: str, url: str) -> StockAnalysisProfile:
    return StockAnalysisProfile(
        url=url,
        name=extract_company_title(html),
        isin=extract_profile_table_value(html, "ISIN Number").upper(),
        sector=extract_profile_table_value(html, "Sector"),
        industry=extract_profile_table_value(html, "Industry"),
    )


def stockanalysis_company_url(exchange: str, ticker: str) -> str:
    slug = EXCHANGE_SLUGS.get(exchange)
    if not slug:
        raise ValueError(f"Unsupported StockAnalysis exchange mapping: {exchange}")
    return f"https://stockanalysis.com/quote/{slug}/{ticker}/company/"


def fetch_stockanalysis_company_profile(exchange: str, ticker: str, session: requests.Session) -> StockAnalysisProfile:
    url = stockanalysis_company_url(exchange, ticker)
    response = session.get(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"}, timeout=30)
    response.raise_for_status()
    return parse_stockanalysis_company_profile(response.text, url)


def stockanalysis_sector_to_canonical(profile: StockAnalysisProfile) -> str:
    sector = normalize_sector(profile.sector, "Stock")
    if sector:
        return sector
    industry = profile.industry.lower()
    if "construction" in industry or "engineering" in industry:
        return "Industrials"
    if "real estate" in industry:
        return "Real Estate"
    if "bank" in industry or "financial" in industry:
        return "Financials"
    if "health" in industry or "pharma" in industry or "biotech" in industry:
        return "Health Care"
    if "software" in industry or "technology" in industry:
        return "Information Technology"
    return ""


def load_targets(path: Path, exchanges: set[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") in exchanges
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip()
        and (not row.get("isin", "").strip() or not row.get("stock_sector", "").strip())
    ]


def evaluate_target(row: dict[str, str], profile: StockAnalysisProfile) -> dict[str, Any]:
    name_match = alias_matches_company(row.get("name", ""), profile.name) or alias_matches_company(
        profile.name,
        row.get("name", ""),
    )
    accepted_isin = profile.isin if not row.get("isin", "").strip() and is_valid_isin(profile.isin) and name_match else ""
    sector = stockanalysis_sector_to_canonical(profile)
    accepted_sector = sector if not row.get("stock_sector", "").strip() and sector and name_match else ""
    if not profile.name:
        decision = "no_profile_name"
    elif not name_match:
        decision = "name_mismatch"
    elif accepted_isin or accepted_sector:
        decision = "accept"
    else:
        decision = "no_update"
    return {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "current_name": row.get("name", ""),
        "stockanalysis_name": profile.name,
        "stockanalysis_url": profile.url,
        "stockanalysis_isin": profile.isin,
        "stockanalysis_sector": profile.sector,
        "stockanalysis_industry": profile.industry,
        "accepted_isin": accepted_isin,
        "accepted_stock_sector": accepted_sector,
        "decision": decision,
    }


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        if result["accepted_isin"]:
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": result["accepted_isin"],
                    "confidence": "0.78",
                    "reason": (
                        "StockAnalysis company page provided a valid ISIN for a row missing ISIN; "
                        "accepted as reviewed secondary metadata after exact exchange URL, issuer-name gate, "
                        f"and ISIN checksum. Source: {result['stockanalysis_url']}"
                    ),
                }
            )
        if result["accepted_stock_sector"]:
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "stock_sector",
                    "decision": "update",
                    "proposed_value": result["accepted_stock_sector"],
                    "confidence": "0.72",
                    "reason": (
                        "StockAnalysis company page provided sector/industry metadata for a row missing stock_sector; "
                        "accepted as reviewed secondary metadata after exact exchange URL and issuer-name gate. "
                        f"Source: {result['stockanalysis_url']}"
                    ),
                }
            )
    return updates


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ticker",
        "exchange",
        "current_name",
        "stockanalysis_name",
        "stockanalysis_url",
        "stockanalysis_isin",
        "stockanalysis_sector",
        "stockanalysis_industry",
        "accepted_isin",
        "accepted_stock_sector",
        "decision",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reviewed StockAnalysis metadata backfill for residual gaps.")
    parser.add_argument("--exchange", action="append", choices=sorted(EXCHANGE_SLUGS), default=[])
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or EXCHANGE_SLUGS)
    ticker_filter = {ticker.strip().upper() for ticker in args.ticker if ticker.strip()}
    targets = [
        row
        for row in load_targets(args.listings_csv, exchanges)
        if not ticker_filter or row.get("ticker", "").strip().upper() in ticker_filter
    ]

    session = requests.Session()
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for row in targets:
        try:
            profile = fetch_stockanalysis_company_profile(row["exchange"], row["ticker"], session)
            results.append(evaluate_target(row, profile))
        except requests.RequestException as exc:
            errors.append({"ticker": row["ticker"], "exchange": row["exchange"], "error": str(exc)})

    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    write_csv(args.csv_out, results)
    summary = {
        "targets": len(targets),
        "results": len(results),
        "updates": len(updates),
        "applied": args.apply,
        "decision_counts": dict(Counter(row["decision"] for row in results)),
        "errors": errors,
    }
    write_json(args.json_out, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
