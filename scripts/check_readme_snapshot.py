#!/usr/bin/env python3
"""Validate README snapshot metrics against generated report artifacts."""

from __future__ import annotations

import argparse
import csv
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
TICKERS_CSV = ROOT / "data" / "tickers.csv"
ALIASES_CSV = ROOT / "data" / "aliases.csv"


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
    "Official full exchanges": None,
    "Official partial exchanges": None,
    "Missing current-scope exchanges": None,
    "Entry quality source-gap rows": None,
    "Entry quality warn rows": None,
}

SOURCE_STATUS_PATTERN = re.compile(
    r"Current source (?:inventory|coverage) status:\s*"
    r"`?(?P<missing>\d[\d,]*)`?\s+missing current-scope (?:sources|exchanges),\s*"
    r"`?(?P<todo>\d[\d,]*)`?\s+parser todo rows?,\s*"
    r"`?(?P<global_expansion>\d[\d,]*)`?\s+real global-expansion candidates,\s*"
    r"`?(?P<official_full>\d[\d,]*)`?\s+official-full (?:rows?|exchanges),\s+and\s+"
    r"`?(?P<official_partial>\d[\d,]*)`?\s+official-partial (?:rows?|exchanges)",
    re.IGNORECASE,
)


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
    source_counts = coverage_venue_status_counts(coverage)
    entry_status_counts = entry_quality["summary"]["status_counts"]

    expected = {
        metric: int(global_metrics[key])
        for metric, key in SNAPSHOT_METRICS.items()
        if key is not None
    }
    expected["Official full exchanges"] = int(source_counts.get("official_full", 0))
    expected["Official partial exchanges"] = int(source_counts.get("official_partial", 0))
    expected["Missing current-scope exchanges"] = int(source_counts.get("missing", 0))
    expected["Entry quality source-gap rows"] = int(entry_status_counts.get("source_gap", 0))
    expected["Entry quality warn rows"] = int(entry_status_counts.get("warn", 0))
    return expected


def coverage_venue_status_counts(coverage: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in coverage.get("by_exchange", []):
        status = str(row.get("venue_status", ""))
        if status:
            counts[status] = counts.get(status, 0) + 1
    return counts


def expected_source_status_values(
    coverage: dict[str, Any], source_inventory: dict[str, Any]
) -> dict[str, int]:
    summary = source_inventory["summary"]
    source_counts = coverage_venue_status_counts(coverage)
    return {
        "missing": int(source_counts.get("missing", 0)),
        "todo": int(summary.get("todo_rows", 0)),
        "global_expansion": int(summary.get("global_expansion_candidates", 0)),
        "official_full": int(source_counts.get("official_full", 0)),
        "official_partial": int(source_counts.get("official_partial", 0)),
    }


def generated_primary_snapshot_values(tickers_csv: Path, aliases_csv: Path) -> dict[str, int]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    with aliases_csv.open(newline="", encoding="utf-8") as handle:
        alias_count = sum(1 for _ in csv.DictReader(handle))
    return {
        "Primary tickers": len(rows),
        "Stocks": sum(row["asset_type"] == "Stock" for row in rows),
        "ETFs": sum(row["asset_type"] == "ETF" for row in rows),
        "Exchanges": len({row["exchange"] for row in rows if row.get("exchange")}),
        "Countries": len({row["country"] for row in rows if row.get("country")}),
        "Aliases": alias_count,
        "ISIN coverage": sum(bool(row.get("isin")) for row in rows),
        "Sector/category coverage": sum(
            bool(row.get("stock_sector") or row.get("etf_category")) for row in rows
        ),
        "Stock sector coverage": sum(
            row["asset_type"] == "Stock" and bool(row.get("stock_sector")) for row in rows
        ),
        "ETF category coverage": sum(
            row["asset_type"] == "ETF" and bool(row.get("etf_category")) for row in rows
        ),
    }


def parse_source_status_values(readme: str) -> dict[str, int] | None:
    match = SOURCE_STATUS_PATTERN.search(readme)
    if not match:
        return None
    return {
        key: int(value.replace(",", ""))
        for key, value in match.groupdict().items()
    }


def check_readme_snapshot(
    *,
    readme_path: Path = README,
    coverage_report_json: Path = COVERAGE_REPORT_JSON,
    source_inventory_json: Path = SOURCE_INVENTORY_JSON,
    entry_quality_json: Path = ENTRY_QUALITY_JSON,
    tickers_csv: Path = TICKERS_CSV,
    aliases_csv: Path = ALIASES_CSV,
) -> list[str]:
    readme = readme_path.read_text(encoding="utf-8")
    snapshot = parse_snapshot_table(readme)
    coverage = load_json(coverage_report_json)
    source_inventory = load_json(source_inventory_json)
    expected = expected_snapshot_values(
        coverage,
        source_inventory,
        load_json(entry_quality_json),
    )
    expected.update(generated_primary_snapshot_values(tickers_csv, aliases_csv))
    expected_source_status = expected_source_status_values(coverage, source_inventory)

    errors: list[str] = []
    classified_exchange_count = sum(
        expected_source_status[key]
        for key in ("missing", "official_full", "official_partial")
    )
    if classified_exchange_count != expected["Exchanges"]:
        errors.append(
            "Coverage venue statuses do not classify every ticker exchange: "
            f"classified={classified_exchange_count:,}, exchanges={expected['Exchanges']:,}"
        )
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
    source_status = parse_source_status_values(readme)
    if source_status is None:
        errors.append("README Sources section is missing the generated current source inventory status paragraph.")
    else:
        for key, expected_value in expected_source_status.items():
            actual_value = source_status[key]
            if actual_value != expected_value:
                errors.append(
                    f"README Sources status {key!r} is stale: "
                    f"README={actual_value:,}, expected={expected_value:,}"
                )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readme", type=Path, default=README)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--source-inventory-json", type=Path, default=SOURCE_INVENTORY_JSON)
    parser.add_argument("--entry-quality-json", type=Path, default=ENTRY_QUALITY_JSON)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    args = parser.parse_args()

    errors = check_readme_snapshot(
        readme_path=args.readme,
        coverage_report_json=args.coverage_report_json,
        source_inventory_json=args.source_inventory_json,
        entry_quality_json=args.entry_quality_json,
        tickers_csv=args.tickers_csv,
        aliases_csv=args.aliases_csv,
    )
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("README snapshot matches generated reports.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
