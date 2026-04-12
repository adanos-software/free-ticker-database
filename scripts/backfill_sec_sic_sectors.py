from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, normalize_sector


SEC_COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
DEFAULT_USER_AGENT = "adanos-software free-ticker-database research contact@adanos-software.com"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "sec_verification"
DEFAULT_CACHE_DIR = DEFAULT_OUTPUT_DIR / "submissions_cache"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sic_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sic_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

SEC_EXCHANGE_CODES: dict[str, set[str]] = {
    "NASDAQ": {"NASDAQ", "NASDAQ GLOBAL MARKET", "NASDAQ CAPITAL MARKET"},
    "NYSE": {"NYSE"},
    "OTC": {"OTC"},
}
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "sec_cik",
    "sec_ticker",
    "sec_exchange",
    "sec_name",
    "sec_sic",
    "sec_sic_description",
    "sector_update",
    "number_tokens_match",
    "name_match",
    "decision",
]


@dataclass(frozen=True)
class SecTicker:
    cik: int
    name: str
    ticker: str
    exchange: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalized_ticker_key(value: str) -> str:
    return value.strip().upper().replace("-", ".").replace("/", ".")


def significant_numbers(value: str) -> set[str]:
    return set(re.findall(r"\d+", value or ""))


def normalize_sec_exchange(value: str) -> str:
    return str(value or "").strip().upper()


def map_sec_sic_to_sector(sic: str) -> str:
    if not sic or not sic.isdigit():
        return ""
    code = int(sic)

    exact: dict[int, str] = {
        1311: "Energy",
        2833: "Health Care",
        2834: "Health Care",
        2835: "Health Care",
        2836: "Health Care",
        3570: "Information Technology",
        3571: "Information Technology",
        3572: "Information Technology",
        3575: "Information Technology",
        3576: "Information Technology",
        3577: "Information Technology",
        3578: "Information Technology",
        3661: "Information Technology",
        3663: "Information Technology",
        3669: "Information Technology",
        3670: "Information Technology",
        3672: "Information Technology",
        3674: "Information Technology",
        3677: "Information Technology",
        3678: "Information Technology",
        3679: "Information Technology",
        3720: "Industrials",
        3721: "Industrials",
        3724: "Industrials",
        3728: "Industrials",
        3841: "Health Care",
        3842: "Health Care",
        3843: "Health Care",
        3844: "Health Care",
        3845: "Health Care",
        3851: "Health Care",
        4812: "Communication Services",
        4813: "Communication Services",
        4822: "Communication Services",
        4832: "Communication Services",
        4833: "Communication Services",
        4841: "Communication Services",
        4899: "Communication Services",
        6500: "Real Estate",
        6510: "Real Estate",
        6512: "Real Estate",
        6513: "Real Estate",
        6519: "Real Estate",
        6531: "Real Estate",
        6798: "Real Estate",
        7310: "Communication Services",
        7311: "Communication Services",
        7370: "Information Technology",
        7371: "Information Technology",
        7372: "Information Technology",
        7373: "Information Technology",
        7374: "Information Technology",
        7377: "Information Technology",
        7378: "Information Technology",
        7379: "Information Technology",
        7389: "Industrials",
        7812: "Communication Services",
        7830: "Communication Services",
        7948: "Communication Services",
        8000: "Health Care",
        8011: "Health Care",
        8050: "Health Care",
        8060: "Health Care",
        8071: "Health Care",
        8090: "Health Care",
        8093: "Health Care",
        8742: "Industrials",
    }
    if code in exact:
        return exact[code]

    ranges: list[tuple[int, int, str]] = [
        (100, 999, "Consumer Staples"),
        (1000, 1299, "Materials"),
        (1300, 1399, "Energy"),
        (1400, 1499, "Materials"),
        (1500, 1799, "Industrials"),
        (2000, 2199, "Consumer Staples"),
        (2200, 2399, "Consumer Discretionary"),
        (2400, 2499, "Materials"),
        (2500, 2599, "Consumer Discretionary"),
        (2600, 2699, "Materials"),
        (2700, 2799, "Communication Services"),
        (2800, 2899, "Materials"),
        (2900, 2999, "Energy"),
        (3000, 3099, "Materials"),
        (3100, 3199, "Consumer Discretionary"),
        (3200, 3399, "Materials"),
        (3400, 3499, "Industrials"),
        (3500, 3569, "Industrials"),
        (3580, 3599, "Industrials"),
        (3600, 3699, "Information Technology"),
        (3700, 3719, "Consumer Discretionary"),
        (3730, 3799, "Industrials"),
        (3800, 3899, "Health Care"),
        (3900, 3999, "Consumer Discretionary"),
        (4900, 4999, "Utilities"),
        (5000, 5199, "Industrials"),
        (5200, 5999, "Consumer Discretionary"),
        (6000, 6499, "Financials"),
        (6600, 6799, "Financials"),
    ]
    for start, end, sector in ranges:
        if start <= code <= end:
            return normalize_sector(sector, "Stock")
    return ""


def fetch_json(url: str, *, user_agent: str, timeout_seconds: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object payload from {url}")
    return payload


def load_sec_tickers(*, user_agent: str, timeout_seconds: float) -> list[SecTicker]:
    payload = fetch_json(SEC_COMPANY_TICKERS_EXCHANGE_URL, user_agent=user_agent, timeout_seconds=timeout_seconds)
    fields = payload.get("fields") or []
    data = payload.get("data") or []
    result: list[SecTicker] = []
    for item in data:
        record = dict(zip(fields, item, strict=False))
        try:
            cik = int(record.get("cik"))
        except (TypeError, ValueError):
            continue
        result.append(
            SecTicker(
                cik=cik,
                name=str(record.get("name") or "").strip(),
                ticker=str(record.get("ticker") or "").strip().upper(),
                exchange=normalize_sec_exchange(record.get("exchange") or ""),
            )
        )
    return result


def load_missing_sector_rows(
    *,
    exchanges: set[str],
    tickers_csv: Path = TICKERS_CSV,
) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] in exchanges
        and row["asset_type"] == "Stock"
        and not (row.get("stock_sector", "") or row.get("sector", "")).strip()
    ]


def index_sec_tickers(sec_tickers: list[SecTicker]) -> dict[tuple[str, str], list[SecTicker]]:
    indexed: dict[tuple[str, str], list[SecTicker]] = {}
    for row in sec_tickers:
        indexed.setdefault((normalized_ticker_key(row.ticker), row.exchange), []).append(row)
    return indexed


def find_sec_candidates(
    row: dict[str, str],
    indexed_sec_tickers: dict[tuple[str, str], list[SecTicker]],
) -> list[SecTicker]:
    candidates: list[SecTicker] = []
    for exchange_code in SEC_EXCHANGE_CODES.get(row["exchange"], set()):
        candidates.extend(indexed_sec_tickers.get((normalized_ticker_key(row["ticker"]), exchange_code), []))
    return candidates


def cache_path_for_cik(cache_dir: Path, cik: int) -> Path:
    return cache_dir / f"CIK{cik:010d}.json"


def fetch_sec_submission(
    cik: int,
    *,
    cache_dir: Path,
    user_agent: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], bool]:
    cache_path = cache_path_for_cik(cache_dir, cik)
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8")), True
    payload = fetch_json(SEC_SUBMISSIONS_URL.format(cik=cik), user_agent=user_agent, timeout_seconds=timeout_seconds)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload, False


def evaluate_sec_sic_row(
    row: dict[str, str],
    candidates: list[SecTicker],
    submission: dict[str, Any] | None,
) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "sec_cik": candidates[0].cik if candidates else "",
        "sec_ticker": candidates[0].ticker if candidates else "",
        "sec_exchange": candidates[0].exchange if candidates else "",
        "sec_name": candidates[0].name if candidates else "",
        "sec_sic": "",
        "sec_sic_description": "",
        "sector_update": "",
        "number_tokens_match": False,
        "name_match": False,
    }
    if not candidates:
        return {**base, "decision": "no_sec_match"}
    if len(candidates) > 1:
        return {**base, "decision": "ambiguous_sec_match"}

    candidate = candidates[0]
    number_tokens_match = significant_numbers(candidate.name) == significant_numbers(row["name"])
    name_match = names_match(candidate.name, row["name"])
    base["number_tokens_match"] = number_tokens_match
    base["name_match"] = name_match
    if not number_tokens_match:
        return {**base, "decision": "number_token_mismatch"}
    if not name_match:
        return {**base, "decision": "name_mismatch"}
    if submission is None:
        return {**base, "decision": "missing_submission"}

    sic = str(submission.get("sic") or "").strip()
    sic_description = str(submission.get("sicDescription") or "").strip()
    base["sec_sic"] = sic
    base["sec_sic_description"] = sic_description
    sector_update = map_sec_sic_to_sector(sic)
    if not sic:
        return {**base, "decision": "missing_sic"}
    if not sector_update:
        return {**base, "decision": "unmapped_sic"}
    return {**base, "sector_update": sector_update, "decision": "accept"}


def verify_sec_sic_sectors(
    rows: list[dict[str, str]],
    sec_tickers: list[SecTicker],
    *,
    cache_dir: Path,
    user_agent: str,
    timeout_seconds: float,
    rate_limit_seconds: float,
    max_requests: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    indexed = index_sec_tickers(sec_tickers)
    results: list[dict[str, Any]] = []
    requests_made = 0
    for row in rows:
        candidates = find_sec_candidates(row, indexed)
        if len(candidates) != 1:
            results.append(evaluate_sec_sic_row(row, candidates, None))
            continue
        if max_requests is not None and requests_made >= max_requests and not cache_path_for_cik(cache_dir, candidates[0].cik).exists():
            results.append({**evaluate_sec_sic_row(row, candidates, None), "decision": "max_requests_reached"})
            continue
        submission, cache_hit = fetch_sec_submission(
            candidates[0].cik,
            cache_dir=cache_dir,
            user_agent=user_agent,
            timeout_seconds=timeout_seconds,
        )
        if not cache_hit:
            requests_made += 1
            if rate_limit_seconds > 0:
                time.sleep(rate_limit_seconds)
        results.append(evaluate_sec_sic_row(row, candidates, submission))
    return results, requests_made


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.72",
                "reason": f"SEC submissions data lists SIC {result['sec_sic']} ({result['sec_sic_description']}) for the exact ticker/exchange CIK match; SIC was conservatively mapped to a canonical stock sector.",
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
    parser = argparse.ArgumentParser(description="Backfill missing US stock sectors from SEC SIC classifications.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--rate-limit-seconds", type=float, default=0.12)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or ["NASDAQ", "NYSE"])
    unsupported = sorted(exchanges - set(SEC_EXCHANGE_CODES))
    if unsupported:
        raise SystemExit(f"Unsupported SEC exchange(s): {', '.join(unsupported)}")

    rows = load_missing_sector_rows(exchanges=exchanges)
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results, requests_made = verify_sec_sic_sectors(
        rows,
        load_sec_tickers(user_agent=args.user_agent, timeout_seconds=args.timeout_seconds),
        cache_dir=args.cache_dir,
        user_agent=args.user_agent,
        timeout_seconds=args.timeout_seconds,
        rate_limit_seconds=args.rate_limit_seconds,
        max_requests=args.max_requests,
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
                "candidates": len(rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "requests_made": requests_made,
                "accepted_sector_updates": len(updates),
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
