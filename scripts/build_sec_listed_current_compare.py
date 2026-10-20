"""Compare the SEC listed-current denominator without applying its identities."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_sec_sic_sectors import SEC_EXCHANGE_CODES, normalized_ticker_key, normalize_sec_exchange
from scripts.lib.dataio import load_csv
from scripts.lib.non_equity_guard import classify_non_equity_leakage
from scripts.lib.reference_audit import fetch_snapshot, sha256, snapshot_metadata, write_report

SEC_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
US_VENUES = {"NASDAQ", "NYSE", "NYSE MKT", "NYSE ARCA", "BATS"}
FIELDS = ["listing_key", "sec_ticker", "sec_exchange", "sec_cik", "status", "reason"]


def compare(listings: list[dict[str, str]], payload: dict) -> list[dict[str, str]]:
    if not {"ticker", "exchange", "cik", "name"} <= set(payload.get("fields", [])):
        raise ValueError("SEC snapshot lacks required fields")
    index = defaultdict(list)
    for row in listings:
        if row["exchange"] in US_VENUES:
            index[(normalized_ticker_key(row["ticker"]), row["exchange"])].append(row)
    matched = set()
    result = []
    for values in payload["data"]:
        record = dict(zip(payload["fields"], values, strict=True))
        ticker = str(record["ticker"] or "")
        exchange = normalize_sec_exchange(record["exchange"])
        candidates = [row for local, sec in SEC_EXCHANGE_CODES.items()
                      if local in US_VENUES and exchange in sec
                      for row in index.get((normalized_ticker_key(ticker), local), [])]
        guard = classify_non_equity_leakage({"ticker": ticker, "name": record["name"], "asset_type": "Stock"})
        reason = ""
        if len(candidates) == 1:
            status = "matched_listed_current"
            matched.add(candidates[0]["listing_key"])
        elif len(candidates) > 1:
            status, reason = "ambiguous_venue_review", "SEC venue label resolves to multiple local listings"
        elif exchange not in {value for local, codes in SEC_EXCHANGE_CODES.items() if local in US_VENUES for value in codes}:
            status, reason = "sec_out_of_scope_venue", "SEC OTC and unmapped venue rows are outside this denominator"
        elif guard["guard_decision"] != "accepted_or_not_applicable":
            status, reason = "sec_only_scope_review", guard["leakage_class"]
        else:
            status, reason = "sec_only_missing_candidate", "SEC lacks security type; confirm common-equity scope before ingestion"
        result.append({"listing_key": "|".join(sorted(r["listing_key"] for r in candidates)),
                       "sec_ticker": ticker, "sec_exchange": exchange, "sec_cik": str(record["cik"]),
                       "status": status, "reason": reason})
    for row in listings:
        if row["exchange"] in US_VENUES and row["listing_key"] not in matched:
            result.append({"listing_key": row["listing_key"], "status": "adanos_only_us_listed",
                           "reason": "SEC is not a complete ETP master or delisting source"})
    return result


def run(listings_csv: Path, sec_json: Path, out_dir: Path) -> dict:
    before = sha256(listings_csv)
    rows = compare(load_csv(listings_csv), json.loads(sec_json.read_text()))
    report = write_report(out_dir / "sec_listed_current_compare", "SEC listed-current comparison",
                          dict(sorted(Counter(row["status"] for row in rows).items())), rows, FIELDS,
                          [snapshot_metadata(sec_json, SEC_URL)],
                          ["SEC NYSE may mean NYSE American; ambiguous ticker-plus-venue matches stay open.",
                           "Scope candidates require review. No bare-ticker join, ISIN claims, identity writes, or delisting inference.",
                           "SEC citation and access policy: https://www.sec.gov/os/accessing-edgar-data"], inputs=(listings_csv,))
    if sha256(listings_csv) != before:
        raise RuntimeError("Comparison mutated listings")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sec-json", type=Path, default=ROOT / "data/masterfiles/cache/sec_company_tickers_exchange.json")
    parser.add_argument("--listings-csv", type=Path, default=ROOT / "data/listings.csv")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data/reports")
    parser.add_argument("--fetch", action="store_true")
    args = parser.parse_args()
    if args.fetch:
        fetch_snapshot(SEC_URL, args.sec_json)
    print(json.dumps(run(args.listings_csv, args.sec_json, args.out_dir)["summary"], indent=2))


if __name__ == "__main__":
    main()
