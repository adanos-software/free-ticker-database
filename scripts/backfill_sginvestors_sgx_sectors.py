from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import urllib.request
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, normalize_sector


SGINVESTORS_SECTOR_URL = "https://sginvestors.io/sgx/stock-listing/sector"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "sgx_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sginvestors_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sginvestors_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "sginvestors_name",
    "sginvestors_sector",
    "sector_update",
    "decision",
]


@dataclass(frozen=True)
class SgInvestorsSectorRow:
    ticker: str
    name: str
    sector: str


class TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = " ".join(data.split()).strip()
        if text:
            self.parts.append(text)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fetch_sginvestors_sector_page(url: str, timeout_seconds: float) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "free-ticker-database/3.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_sginvestors_sector_page(text: str) -> list[SgInvestorsSectorRow]:
    parser = TextCollector()
    parser.feed(text)
    current_sector = ""
    pending_name = ""
    rows: list[SgInvestorsSectorRow] = []
    seen: set[str] = set()

    for part in parser.parts:
        heading = re.fullmatch(r"SGX Listed (.+?) Sector Companies", part)
        if heading:
            current_sector = normalize_sector(heading.group(1), "Stock")
            pending_name = ""
            continue
        if not current_sector:
            continue

        inline_match = re.search(r"^(?P<name>.+?)\s*\(\s*SGX:\s*(?P<ticker>[A-Z0-9]+)\s*\)", part)
        split_match = re.fullmatch(r"SGX:\s*(?P<ticker>[A-Z0-9]+)", part)
        if inline_match:
            name = inline_match.group("name").strip()
            ticker = inline_match.group("ticker").strip().upper()
        elif split_match and pending_name:
            name = pending_name
            ticker = split_match.group("ticker").strip().upper()
        else:
            if part not in {"(", ")"} and not part.isdigit() and not part.startswith("SGX:"):
                pending_name = part
            continue
        if ticker in seen:
            pending_name = ""
            continue
        rows.append(
            SgInvestorsSectorRow(
                ticker=ticker,
                name=name,
                sector=current_sector,
            )
        )
        seen.add(ticker)
        pending_name = ""
    return rows


def load_target_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "SGX"
        and row.get("asset_type") == "Stock"
        and not row.get("stock_sector", "").strip()
    ]


def evaluate_rows(targets: list[dict[str, str]], sector_rows: list[SgInvestorsSectorRow]) -> list[dict[str, Any]]:
    by_ticker = {row.ticker: row for row in sector_rows}
    results: list[dict[str, Any]] = []
    for target in targets:
        base = {
            "ticker": target["ticker"],
            "exchange": target["exchange"],
            "asset_type": target["asset_type"],
            "name": target["name"],
            "sginvestors_name": "",
            "sginvestors_sector": "",
            "sector_update": "",
        }
        candidate = by_ticker.get(target["ticker"].strip().upper())
        if not candidate:
            results.append({**base, "decision": "no_sginvestors_match"})
            continue
        base.update({"sginvestors_name": candidate.name, "sginvestors_sector": candidate.sector})
        if not names_match(candidate.name, target["name"]):
            results.append({**base, "decision": "name_mismatch"})
            continue
        results.append({**base, "sector_update": candidate.sector, "decision": "accept"})
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.76",
            "reason": (
                "SGinvestors SGX sector page supplied a GICS sector for an SGX stock without stock_sector; "
                "accepted only after exact SGX ticker and issuer-name gates matched. "
                f"Source: {SGINVESTORS_SECTOR_URL}"
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
    parser = argparse.ArgumentParser(description="Backfill SGX stock sectors from SGinvestors sector lists.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--source-html", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    html = (
        args.source_html.read_text(encoding="utf-8")
        if args.source_html
        else fetch_sginvestors_sector_page(SGINVESTORS_SECTOR_URL, args.timeout_seconds)
    )
    sector_rows = parse_sginvestors_sector_page(html)
    results = evaluate_rows(load_target_rows(args.tickers_csv), sector_rows)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    print(
        json.dumps(
            {
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(row["decision"] for row in results)),
                "json_out": display_path(args.json_out),
                "sginvestors_sector_rows": len(sector_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
