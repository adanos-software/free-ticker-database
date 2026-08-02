from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from scripts.check_readme_snapshot import (
        SOURCE_STATUS_PATTERN,
        expected_snapshot_values,
        expected_source_status_values,
        load_json,
        parse_snapshot_table,
    )
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from check_readme_snapshot import (
        SOURCE_STATUS_PATTERN,
        expected_snapshot_values,
        expected_source_status_values,
        load_json,
        parse_snapshot_table,
    )


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
COVERAGE_REPORT_JSON = ROOT / "data" / "reports" / "coverage_report.json"
SOURCE_INVENTORY_JSON = ROOT / "data" / "reports" / "source_inventory_gap.json"
ENTRY_QUALITY_JSON = ROOT / "data" / "reports" / "entry_quality.json"
TICKERS_CSV = ROOT / "data" / "tickers.csv"
ALIASES_CSV = ROOT / "data" / "aliases.csv"


def format_count(value: int) -> str:
    return f"{value:,}"


def replace_first_int(value: str, expected: int) -> str:
    return re.sub(r"\d[\d,]*", format_count(expected), value, count=1)


def replace_first_percent(value: str, numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return value
    percent = numerator / denominator * 100
    return re.sub(r"\(\d+(?:\.\d+)?%\)", f"({percent:.1f}%)", value, count=1)


def updated_metric_value(metric: str, value: str, expected: dict[str, int]) -> str:
    updated = replace_first_int(value, expected[metric])
    denominator_by_metric = {
        "ISIN coverage": "Primary tickers",
        "Sector/category coverage": "Primary tickers",
    }
    denominator_metric = denominator_by_metric.get(metric)
    if denominator_metric:
        updated = replace_first_percent(updated, expected[metric], expected[denominator_metric])
    return updated


def update_snapshot_table(readme: str, expected: dict[str, int]) -> str:
    snapshot = parse_snapshot_table(readme)
    lines: list[str] = []
    in_snapshot = False
    for line in readme.splitlines():
        if line.strip() == "## Snapshot":
            in_snapshot = True
            lines.append(line)
            continue
        if in_snapshot and line.startswith("## "):
            in_snapshot = False
        if in_snapshot and line.startswith("|"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) >= 3 and cells[0] in expected and cells[0] in snapshot:
                cells[1] = updated_metric_value(cells[0], cells[1], expected)
                line = "| " + " | ".join(cells) + " |"
        lines.append(line)
    return "\n".join(lines) + "\n"


def load_exchange_counts(tickers_csv: Path) -> Counter[str]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return Counter(row["exchange"] for row in csv.DictReader(handle))


def load_primary_metrics(tickers_csv: Path) -> dict[str, int]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {
        "Primary tickers": len(rows),
        "Stocks": sum(row["asset_type"] == "Stock" for row in rows),
        "ETFs": sum(row["asset_type"] == "ETF" for row in rows),
        "Exchanges": len({row["exchange"] for row in rows if row.get("exchange")}),
        "Countries": len({row["country"] for row in rows if row.get("country")}),
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


def load_row_count(csv_path: Path) -> int:
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def update_top_exchange_table(readme: str, exchange_counts: Counter[str]) -> str:
    lines: list[str] = []
    in_top_exchange_table = False
    for line in readme.splitlines():
        if line.strip() == "Top exchanges by primary ticker count:":
            in_top_exchange_table = True
            lines.append(line)
            continue
        if in_top_exchange_table and line.startswith("## "):
            in_top_exchange_table = False
        if in_top_exchange_table and line.startswith("|"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) >= 2 and cells[0] in exchange_counts:
                cells[1] = format_count(exchange_counts[cells[0]])
                line = "| " + " | ".join(cells) + " |"
        lines.append(line)
    return "\n".join(lines) + "\n"


def source_status_sentence(
    coverage: dict[str, Any], source_inventory: dict[str, Any]
) -> str:
    values = expected_source_status_values(coverage, source_inventory)
    return (
        "Current source coverage status: "
        f"`{format_count(values['missing'])}` missing current-scope exchanges, "
        f"`{format_count(values['todo'])}` parser todo rows, "
        f"`{format_count(values['global_expansion'])}` real global-expansion candidates, "
        f"`{format_count(values['official_full'])}` official-full exchanges, and "
        f"`{format_count(values['official_partial'])}` official-partial exchanges."
    )


def update_sources_status_paragraph(
    readme: str, coverage: dict[str, Any], source_inventory: dict[str, Any]
) -> str:
    replacement = (
        "Official source candidates and reconciled source gaps are tracked in "
        "[`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) "
        "and summarized by "
        "[`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md). "
        f"{source_status_sentence(coverage, source_inventory)} "
        "Remaining work includes source-parser backlog plus field-completion and taxonomy coverage."
    )
    if SOURCE_STATUS_PATTERN.search(readme):
        paragraph_pattern = re.compile(
            r"Official source candidates and reconciled source gaps are tracked in .*?"
            r"Remaining work .*?(?=\n\n)",
            re.DOTALL,
        )
        updated, count = paragraph_pattern.subn(replacement, readme, count=1)
        if count:
            return updated
        return SOURCE_STATUS_PATTERN.sub(
            source_status_sentence(coverage, source_inventory), readme, count=1
        )

    marker = "## Sources"
    marker_index = readme.find(marker)
    if marker_index == -1:
        return readme
    next_paragraph_index = readme.find("\n\n", marker_index + len(marker))
    if next_paragraph_index == -1:
        return readme
    insert_index = readme.find("\n\n", next_paragraph_index + 2)
    if insert_index == -1:
        return readme + "\n\n" + replacement + "\n"
    return readme[:insert_index] + "\n\n" + replacement + readme[insert_index:]


def expected_values(
    coverage_report_json: Path,
    source_inventory_json: Path,
    entry_quality_json: Path,
) -> dict[str, int]:
    return expected_snapshot_values(
        load_json(coverage_report_json),
        load_json(source_inventory_json),
        load_json(entry_quality_json),
    )


def update_readme_snapshot(
    *,
    readme_path: Path = README,
    coverage_report_json: Path = COVERAGE_REPORT_JSON,
    source_inventory_json: Path = SOURCE_INVENTORY_JSON,
    entry_quality_json: Path = ENTRY_QUALITY_JSON,
    tickers_csv: Path = TICKERS_CSV,
    aliases_csv: Path = ALIASES_CSV,
) -> dict[str, Any]:
    coverage = load_json(coverage_report_json)
    source_inventory = load_json(source_inventory_json)
    expected = expected_values(coverage_report_json, source_inventory_json, entry_quality_json)
    expected.update(load_primary_metrics(tickers_csv))
    expected["Aliases"] = load_row_count(aliases_csv)
    original = readme_path.read_text(encoding="utf-8")
    updated = update_snapshot_table(original, expected)
    updated = update_top_exchange_table(updated, load_exchange_counts(tickers_csv))
    updated = update_sources_status_paragraph(updated, coverage, source_inventory)
    readme_path.write_text(updated, encoding="utf-8")
    changed = original != updated
    summary = {"readme": str(readme_path.relative_to(ROOT)), "changed": changed, "metrics": expected}
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update README snapshot counts from generated reports.")
    parser.add_argument("--readme", type=Path, default=README)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--source-inventory-json", type=Path, default=SOURCE_INVENTORY_JSON)
    parser.add_argument("--entry-quality-json", type=Path, default=ENTRY_QUALITY_JSON)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    update_readme_snapshot(
        readme_path=args.readme,
        coverage_report_json=args.coverage_report_json,
        source_inventory_json=args.source_inventory_json,
        entry_quality_json=args.entry_quality_json,
        tickers_csv=args.tickers_csv,
        aliases_csv=args.aliases_csv,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
