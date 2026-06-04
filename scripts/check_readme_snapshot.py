#!/usr/bin/env python3
"""Validate README snapshot metrics against generated report artifacts."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
COVERAGE_REPORT_JSON = ROOT / "data" / "reports" / "coverage_report.json"
SOURCE_INVENTORY_JSON = ROOT / "data" / "reports" / "source_inventory_gap.json"
ENTRY_QUALITY_JSON = ROOT / "data" / "reports" / "entry_quality.json"


SNAPSHOT_METRICS = {
    "Core listings": "core_listings",
    "Primary tickers": "tickers",
    "Full listing rows": "listing_keys",
    "Stocks": "stocks",
    "ETFs": "etfs",
    "Aliases": "aliases",
    "ISIN coverage": "isin_coverage",
    "FIGI coverage": "figi_coverage",
    "Sector/category coverage": "sector_coverage",
    "Stock sector coverage": "stock_sector_coverage",
    "ETF category coverage": "etf_category_coverage",
    "Core listing-scope rows": "instrument_scope_core",
    "Core primary rows with ISIN": "instrument_scope_primary_listing",
    "Core primary rows missing ISIN": "instrument_scope_primary_listing_missing_isin",
    "Extended listing-scope rows": "instrument_scope_extended",
    "Official full exchanges": "official_full_exchanges",
    "Official partial exchanges": "official_partial_exchanges",
    "Missing current-scope exchanges": "missing_exchanges",
    "Entry quality source-gap rows": None,
    "Entry quality warn rows": None,
}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_snapshot_table(readme: str) -> dict[str, str]:
    rows: dict[str, str] = {}
    in_snapshot = False
    for line in readme.splitlines():
        if line.strip() == "## Snapshot":
            in_snapshot = True
            continue
        if in_snapshot and line.startswith("## "):
            break
        if not in_snapshot or not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 2 or cells[0] in {"Metric", "---"}:
            continue
        rows[cells[0]] = cells[1]
    return rows


def first_int(value: str) -> int | None:
    match = re.search(r"\d[\d,]*", value)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def expected_snapshot_values(
    coverage: dict[str, Any],
    source_inventory: dict[str, Any],
    entry_quality: dict[str, Any],
) -> dict[str, int]:
    global_metrics = coverage["global"]
    source_counts = source_inventory["summary"]["current_status_counts"]
    entry_status_counts = entry_quality["summary"]["status_counts"]

    expected = {
        metric: int(global_metrics[key])
        for metric, key in SNAPSHOT_METRICS.items()
        if key is not None
    }
    expected["Missing current-scope exchanges"] = int(source_counts.get("missing", 0))
    expected["Entry quality source-gap rows"] = int(entry_status_counts.get("source_gap", 0))
    expected["Entry quality warn rows"] = int(entry_status_counts.get("warn", 0))
    return expected


def check_readme_snapshot(
    *,
    readme_path: Path = README,
    coverage_report_json: Path = COVERAGE_REPORT_JSON,
    source_inventory_json: Path = SOURCE_INVENTORY_JSON,
    entry_quality_json: Path = ENTRY_QUALITY_JSON,
) -> list[str]:
    readme = readme_path.read_text(encoding="utf-8")
    snapshot = parse_snapshot_table(readme)
    expected = expected_snapshot_values(
        load_json(coverage_report_json),
        load_json(source_inventory_json),
        load_json(entry_quality_json),
    )

    errors: list[str] = []
    for metric, expected_value in expected.items():
        if metric not in snapshot:
            errors.append(f"README snapshot is missing metric: {metric}")
            continue
        actual_value = first_int(snapshot[metric])
        if actual_value != expected_value:
            errors.append(
                f"README snapshot metric {metric!r} is stale: "
                f"README={snapshot[metric]!r}, expected={expected_value:,}"
            )

    required_claims = [
        "`tickers.csv` is a compatibility export",
        "`listings.csv` and `listing_key` are the venue-level source of truth",
        "`source_inventory_gap.md` is authoritative for current-scope source gaps",
    ]
    for claim in required_claims:
        if claim not in readme:
            errors.append(f"README is missing required claim: {claim}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readme", type=Path, default=README)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--source-inventory-json", type=Path, default=SOURCE_INVENTORY_JSON)
    parser.add_argument("--entry-quality-json", type=Path, default=ENTRY_QUALITY_JSON)
    args = parser.parse_args()

    errors = check_readme_snapshot(
        readme_path=args.readme,
        coverage_report_json=args.coverage_report_json,
        source_inventory_json=args.source_inventory_json,
        entry_quality_json=args.entry_quality_json,
    )
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("README snapshot matches generated reports.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
