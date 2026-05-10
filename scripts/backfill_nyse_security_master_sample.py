from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


NYSE_SECURITY_MASTER_SAMPLE_URL = (
    "https://ftp.nyse.com/Reference%20Data%20Samples/NYSE%20GROUP%20SECURITY%20MASTER/"
    "NYSEGROUP_US_REF_SECURITYMASTER_EQUITY_4.0_20241203.txt"
)
DEFAULT_OUTPUT_DIR = ROOT / "data" / "nyse_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "security_master_sample_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "security_master_sample_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

TARGET_EXCHANGES = {"NYSE", "NYSE ARCA", "NYSE MKT", "BATS", "NASDAQ"}
ASSET_CLASS_CATEGORY_MAP = {
    "COMMODITIES_AND_FUTURES": "Commodity",
    "CURRENCY": "Currency",
    "EQUITY": "Equity",
    "FIXED_INCOME": "Fixed Income",
    "HYBRID": "Multi-Asset",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "nyse_symbol",
    "nyse_issuer_name",
    "nyse_issue_name",
    "nyse_instrument_type",
    "nyse_isin",
    "nyse_asset_class",
    "nyse_strategy",
    "isin_update",
    "category_update",
    "decision",
]


@dataclass(frozen=True)
class NyseSecurityMasterRow:
    symbol: str
    issuer_name: str
    issue_name: str
    instrument_type: str
    isin: str
    asset_class: str
    strategy: str

    @property
    def combined_name(self) -> str:
        return " ".join(part for part in [self.issuer_name, self.issue_name] if part).strip()


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fetch_text(url: str, timeout_seconds: float) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "free-ticker-database/3.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", "replace")


def parse_security_master_line(line: str) -> NyseSecurityMasterRow | None:
    parts = line.rstrip("\n").split("|")
    if len(parts) < 97:
        return None
    symbol = parts[1].strip().upper()
    isin = parts[4].strip().upper()
    if not symbol or not is_valid_isin(isin):
        return None
    return NyseSecurityMasterRow(
        symbol=symbol,
        issuer_name=parts[5].strip(),
        issue_name=parts[6].strip(),
        instrument_type=parts[8].strip(),
        isin=isin,
        asset_class=parts[95].strip().upper(),
        strategy=parts[96].strip().upper(),
    )


def parse_security_master_text(text: str) -> list[NyseSecurityMasterRow]:
    rows: list[NyseSecurityMasterRow] = []
    for line in text.splitlines():
        row = parse_security_master_line(line)
        if row is not None:
            rows.append(row)
    return rows


def index_security_master_rows(rows: list[NyseSecurityMasterRow]) -> dict[str, list[NyseSecurityMasterRow]]:
    indexed: dict[str, dict[tuple[str, str, str, str], NyseSecurityMasterRow]] = defaultdict(dict)
    for row in rows:
        indexed[row.symbol][(row.isin, row.issuer_name, row.issue_name, row.asset_class)] = row
    return {symbol: list(values.values()) for symbol, values in indexed.items()}


def load_target_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") in TARGET_EXCHANGES
        and (
            (row.get("asset_type") == "ETF" and not row.get("etf_category", "").strip())
            or not row.get("isin", "").strip()
        )
    ]


def target_matches_source_name(target: dict[str, str], source: NyseSecurityMasterRow) -> bool:
    return names_match(source.issue_name, target["name"]) or names_match(source.combined_name, target["name"])


def evaluate_row(target: dict[str, str], sources: list[NyseSecurityMasterRow]) -> dict[str, Any]:
    base = {
        "ticker": target["ticker"],
        "exchange": target["exchange"],
        "asset_type": target["asset_type"],
        "name": target["name"],
        "nyse_symbol": "",
        "nyse_issuer_name": "",
        "nyse_issue_name": "",
        "nyse_instrument_type": "",
        "nyse_isin": "",
        "nyse_asset_class": "",
        "nyse_strategy": "",
        "isin_update": "",
        "category_update": "",
    }
    if not sources:
        return {**base, "decision": "no_nyse_sample_match"}

    matching_sources = [source for source in sources if target_matches_source_name(target, source)]
    if len(matching_sources) != 1:
        return {**base, "decision": "name_mismatch" if not matching_sources else "ambiguous_nyse_sample_match"}

    source = matching_sources[0]
    base.update(
        {
            "nyse_symbol": source.symbol,
            "nyse_issuer_name": source.issuer_name,
            "nyse_issue_name": source.issue_name,
            "nyse_instrument_type": source.instrument_type,
            "nyse_isin": source.isin,
            "nyse_asset_class": source.asset_class,
            "nyse_strategy": source.strategy,
        }
    )

    accepted: list[str] = []
    if not target.get("isin", "").strip() and source.isin.startswith("US"):
        base["isin_update"] = source.isin
        accepted.append("isin")
    if target.get("asset_type") == "ETF" and not target.get("etf_category", "").strip():
        category = ASSET_CLASS_CATEGORY_MAP.get(source.asset_class, "")
        if category:
            base["category_update"] = category
            accepted.append("etf_category")

    if not accepted:
        return {**base, "decision": "no_update"}
    return {**base, "decision": "accept_" + "_".join(accepted)}


def verify_rows(targets: list[dict[str, str]], sources: list[NyseSecurityMasterRow]) -> list[dict[str, Any]]:
    indexed_sources = index_security_master_rows(sources)
    return [evaluate_row(target, indexed_sources.get(target["ticker"].strip().upper(), [])) for target in targets]


def build_metadata_updates(results: list[dict[str, Any]], source_url: str) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if not result["decision"].startswith("accept"):
            continue
        if result.get("isin_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": result["isin_update"],
                    "confidence": "0.78",
                    "reason": (
                        "NYSE Group Security Master public sample supplied a valid US ISIN; accepted only after exact "
                        f"ticker and security-name gates matched. Sample source: {source_url}"
                    ),
                }
            )
        if result.get("category_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "etf_category",
                    "decision": "update",
                    "proposed_value": result["category_update"],
                    "confidence": "0.76",
                    "reason": (
                        "NYSE Group Security Master public sample supplied an ETF asset-class label; accepted only after "
                        f"exact ticker and security-name gates matched. Sample source: {source_url}"
                    ),
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
    parser = argparse.ArgumentParser(description="Backfill US ETF metadata from the public NYSE Security Master sample.")
    parser.add_argument("--source-url", default=NYSE_SECURITY_MASTER_SAMPLE_URL)
    parser.add_argument("--source-path", type=Path)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    source_text = args.source_path.read_text(encoding="utf-8") if args.source_path else fetch_text(
        args.source_url,
        args.timeout_seconds,
    )
    source_rows = parse_security_master_text(source_text)
    results = verify_rows(load_target_rows(args.tickers_csv), source_rows)
    updates = build_metadata_updates(results, args.source_url)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"].startswith("accept")], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "accepted_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(row["decision"] for row in results)),
                "json_out": display_path(args.json_out),
                "source_rows": len(source_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
