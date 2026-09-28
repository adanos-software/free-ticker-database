#!/usr/bin/env python3
"""DivvyDiary sitemap discovery funnel.

Sitemap URLs are discovery only. Names, tickers, and ISINs are taken from official
masterfile cache rows. Does not recode existing identities.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_masterfile_supplements import SUPPLEMENT_EXCHANGES
from scripts.lib.non_equity_guard import is_blocked_non_common_stock
from scripts.lib.normalize import names_match

LISTINGS_CSV = ROOT / "data" / "listings.csv"
COVERAGE_CSV = ROOT / "data" / "coverage_expansion_listings.csv"
CACHE_DIR = ROOT / "data" / "masterfiles" / "cache"
OUT_DIR = ROOT / "data" / "divvydiary_verification"
USER_AGENT = "free-ticker-database-maintenance/1.0 (sitemap discovery; no ingest of DivvyDiary content)"
SITEMAP_INDEX = "https://divvydiary.com/sitemap.xml"
ISIN_RE = re.compile(r"([A-Z]{2}[A-Z0-9]{9}[0-9])(?:/)?$")
SKIP_SLUG_RE = re.compile(
    r"(wikifolio|ucits|\betf\b|\bfonds\b|\bfund\b|sicav|anleihe|\bbond\b|"
    r"zertifikat|knock-out|knockout|optionsschein|faktor-zertifikat|"
    r"accumulat|distributing|class-[a-z0-9]+-eur)",
    re.IGNORECASE,
)
GICS = {
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
}
SKIP_EXCHANGES = {"BVB", "OTC", "FSX", "XETRA", "LSE", "SIX", "TXSE"}
LISTING_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_url(url: str) -> str:
    lower = url.lower()
    if "-etf-" in lower or lower.endswith("-etf"):
        return "etf"
    if "-fonds-" in lower or "-fund-" in lower:
        return "fund"
    if "-bond-" in lower or re.search(r"-\d+-\d{2}-\d{2}-[a-z]{2}", lower):
        return "bond"
    if "-stock-" in lower or "-aktie-" in lower:
        return "stock"
    slug = lower.rsplit("/", 1)[-1]
    slug = ISIN_RE.sub("", slug)
    if SKIP_SLUG_RE.search(slug.replace("-", " ")):
        return "non_equity"
    if slug.startswith("wikifolio"):
        return "non_equity"
    return "stock"


def parse_sitemap_locs(xml_text: str) -> list[str]:
    return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml_text)


def fetch_sitemaps(session: requests.Session) -> dict[str, str]:
    index = session.get(SITEMAP_INDEX, timeout=60)
    index.raise_for_status()
    child_urls = parse_sitemap_locs(index.text)
    mapping: dict[str, str] = {}
    for child in child_urls:
        response = session.get(child, timeout=120)
        response.raise_for_status()
        for loc in parse_sitemap_locs(response.text):
            url = unquote(loc)
            match = ISIN_RE.search(url)
            if not match:
                continue
            isin = match.group(1)
            if isin.startswith("XF"):
                continue
            kind = classify_url(url)
            if kind != "stock":
                continue
            mapping[isin] = url
    return mapping


def load_listings() -> tuple[list[dict[str, str]], set[str], set[str], set[str], set[tuple[str, str]]]:
    rows: list[dict[str, str]] = []
    with LISTINGS_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    isins = {(row.get("isin") or "").strip().upper() for row in rows if len((row.get("isin") or "").strip()) == 12}
    listing_keys = {row["listing_key"] for row in rows if row.get("listing_key")}
    global_tickers = {row["ticker"].upper() for row in rows if row.get("ticker")}
    exchange_tickers = {(row["ticker"].upper(), row["exchange"]) for row in rows}
    return rows, isins, listing_keys, global_tickers, exchange_tickers


def load_official_by_isin() -> dict[str, list[dict[str, str]]]:
    by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for path in sorted(CACHE_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
            continue
        if "ticker" not in payload[0] or "exchange" not in payload[0]:
            continue
        for row in payload:
            if row.get("asset_type") != "Stock":
                continue
            if row.get("listing_status") != "active":
                continue
            if str(row.get("official", "")).lower() not in {"true", "1", ""}:
                continue
            isin = (row.get("isin") or "").strip().upper()
            ticker = (row.get("ticker") or "").strip()
            exchange = (row.get("exchange") or "").strip()
            if len(isin) != 12 or not ticker or not exchange:
                continue
            if exchange in SKIP_EXCHANGES:
                continue
            by_isin[isin].append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "name": (row.get("name") or "").strip(),
                    "isin": isin,
                    "sector": (row.get("sector") or "").strip(),
                    "source_key": row.get("source_key") or "",
                    "source_url": row.get("source_url") or "",
                    "reference_scope": row.get("reference_scope") or "",
                    "cache_file": path.name,
                }
            )
    return by_isin


def alias_from_name(name: str) -> str:
    alias = name.lower()
    alias = re.sub(r"\s*\b(limited|ltd|inc|corporation|corp|plc|company|co|the|ag|sa|nv|oyj|ab|asa)\b\.?", " ", alias)
    alias = re.sub(r"\s+", " ", alias).strip(" .,-")
    return alias


def gics_or_blank(value: str) -> str:
    return value if value in GICS else ""


def decide_row(
    official: dict[str, str],
    listing_keys: set[str],
    global_tickers: set[str],
    exchange_tickers: set[tuple[str, str]],
    coverage_keys: set[str],
) -> str:
    exchange = official["exchange"]
    ticker = official["ticker"]
    listing_key = f"{exchange}::{ticker}"
    if listing_key in listing_keys or listing_key in coverage_keys:
        return "already_listed"
    if (ticker.upper(), exchange) in exchange_tickers:
        return "already_listed"
    if is_blocked_non_common_stock({"name": official["name"], "ticker": ticker, "asset_type": "Stock"}):
        return "blocked_non_equity"
    if exchange == "B3" and (ticker.endswith("11") or "UNT" in official["isin"] or "TRV" in official["isin"]):
        return "blocked_non_equity"
    if re.search(r"\b(?:sdr|cedear|bdr|nvdr)\b", official["name"], re.IGNORECASE):
        return "blocked_non_equity"
    if exchange not in SUPPLEMENT_EXCHANGES:
        return "exchange_not_in_supplement_allowlist"
    if ticker.upper() in global_tickers:
        return "ticker_collision"
    return "land_free"


def make_listing_row(official: dict[str, str]) -> dict[str, str]:
    exchange = official["exchange"]
    meta = SUPPLEMENT_EXCHANGES.get(exchange, {"country": "", "country_code": ""})
    return {
        "listing_key": f"{exchange}::{official['ticker']}",
        "ticker": official["ticker"],
        "exchange": exchange,
        "name": official["name"],
        "asset_type": "Stock",
        "stock_sector": gics_or_blank(official.get("sector") or ""),
        "etf_category": "",
        "country": meta.get("country") or "",
        "country_code": meta.get("country_code") or "",
        "isin": official["isin"],
        "aliases": alias_from_name(official["name"]),
    }


def append_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    newline = "\n"
    raw = path.read_bytes()
    if raw.endswith(b"\r\n"):
        newline = "\r\n"
    elif raw.endswith(b"\n"):
        newline = "\n"
    prefix = b"" if raw.endswith(newline.encode("utf-8")) else newline.encode("utf-8")
    with path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(prefix.decode("utf-8") if prefix else "")
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator=newline)
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply-free", action="store_true", help="Append globally-free official matches to listings.csv")
    parser.add_argument("--apply-collisions", action="store_true", help="Append colliding official matches to coverage_expansion_listings.csv")
    parser.add_argument("--skip-fetch", action="store_true", help="Reuse saved sitemap map")
    args = parser.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    map_path = OUT_DIR / "sitemap_stock_isins.json"

    if args.skip_fetch and map_path.exists():
        sitemap = json.loads(map_path.read_text(encoding="utf-8"))
        print(f"reused sitemap map {len(sitemap)}", flush=True)
    else:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/xml,text/xml,*/*"})
        print("fetching sitemaps", flush=True)
        sitemap = fetch_sitemaps(session)
        map_path.write_text(json.dumps(sitemap, indent=2) + "\n", encoding="utf-8")
        print(f"sitemap stock ISINs {len(sitemap)}", flush=True)

    listings, db_isins, listing_keys, global_tickers, exchange_tickers = load_listings()
    coverage_rows: list[dict[str, str]] = []
    if COVERAGE_CSV.exists():
        with COVERAGE_CSV.open(newline="", encoding="utf-8") as handle:
            coverage_rows = list(csv.DictReader(handle))
    coverage_keys = {row.get("listing_key") or f"{row['exchange']}::{row['ticker']}" for row in coverage_rows}
    official_by_isin = load_official_by_isin()

    missing = sorted(set(sitemap) - db_isins)
    decisions: list[dict[str, Any]] = []
    for isin in missing:
        officials = official_by_isin.get(isin) or []
        if not officials:
            decisions.append(
                {
                    "isin": isin,
                    "sitemap_url": sitemap[isin],
                    "decision": "no_official_master",
                }
            )
            continue
        # Prefer home-market rows: unique exchanges, first by exchange name
        officials.sort(key=lambda row: (row["exchange"], row["ticker"]))
        seen_exchanges: set[str] = set()
        for official in officials:
            if official["exchange"] in seen_exchanges:
                continue
            seen_exchanges.add(official["exchange"])
            decision = decide_row(official, listing_keys, global_tickers, exchange_tickers, coverage_keys)
            listing = make_listing_row(official) if decision in {"land_free", "ticker_collision"} else {}
            decisions.append(
                {
                    "isin": isin,
                    "sitemap_url": sitemap[isin],
                    "decision": decision,
                    **official,
                    **listing,
                }
            )

    counts = Counter(row["decision"] for row in decisions)
    report = {
        "generated_at": utc_now(),
        "sitemap_stock_isins": len(sitemap),
        "db_isins": len(db_isins),
        "missing_from_db": len(missing),
        "decisions": dict(counts),
        "note": "DivvyDiary is discovery only. Landed rows use official master ticker/name/ISIN.",
        "rows": decisions,
    }
    (OUT_DIR / "sitemap_funnel.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    with (OUT_DIR / "sitemap_funnel.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = [
            "decision",
            "isin",
            "exchange",
            "ticker",
            "name",
            "listing_key",
            "source_key",
            "cache_file",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(decisions)

    free_rows = [make_listing_row(row) for row in decisions if row["decision"] == "land_free"]
    collision_rows = [make_listing_row(row) for row in decisions if row["decision"] == "ticker_collision"]
    md = [
        "# DivvyDiary sitemap funnel",
        "",
        f"Generated: {report['generated_at']}",
        "",
        f"- Sitemap stock ISINs: {len(sitemap)}",
        f"- Missing from DB: {len(missing)}",
        "",
        "## Decisions",
        "",
    ]
    for key, count in counts.most_common():
        md.append(f"- `{key}`: {count}")
    md.extend(["", f"## Land-free ({len(free_rows)})", ""])
    for row in free_rows[:80]:
        md.append(f"- `{row['listing_key']}` {row['name']} `{row['isin']}`")
    if len(free_rows) > 80:
        md.append(f"- … {len(free_rows) - 80} more")
    (OUT_DIR / "sitemap_funnel.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print("decisions", dict(counts), flush=True)
    print(f"land_free={len(free_rows)} collisions={len(collision_rows)}", flush=True)

    if args.apply_free and free_rows:
        append_csv(LISTINGS_CSV, LISTING_FIELDS, free_rows)
        print(f"appended {len(free_rows)} rows to listings.csv", flush=True)
    if args.apply_collisions and collision_rows:
        coverage_fields = list(csv.DictReader(COVERAGE_CSV.open(newline="", encoding="utf-8")).fieldnames or LISTING_FIELDS)
        append_csv(COVERAGE_CSV, coverage_fields, collision_rows)
        print(f"appended {len(collision_rows)} rows to coverage_expansion_listings.csv", flush=True)
    print("DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
