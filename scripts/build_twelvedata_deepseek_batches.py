"""Split Twelve Data rename candidates into DeepSeek review batch CSVs."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


DEFAULT_INPUT_CSV = Path("data/reports/twelvedata_rename_candidates.csv")
DEFAULT_OUTPUT_DIR = Path("data/deepseek_review_jobs")
DEFAULT_SUMMARY_JSON = Path("data/reports/twelvedata_deepseek_batches_summary.json")
DEFAULT_SUMMARY_MD = Path("data/reports/twelvedata_deepseek_batches.md")

BATCH_FILES = {
    "batch_a_us_core": "twelvedata_batch_a_rename_candidates.csv",
    "batch_b_canada": "twelvedata_batch_b_rename_candidates.csv",
    "batch_c_high_value_international": "twelvedata_batch_c_rename_candidates.csv",
    "later_global_review": "twelvedata_global_rest_rename_candidates.csv",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def split_rows(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped = {batch: [] for batch in BATCH_FILES}
    for row in rows:
        grouped.setdefault(row.get("review_batch", "later_global_review"), []).append(row)
    return grouped


def summarize(grouped: dict[str, list[dict[str, str]]], output_dir: Path) -> dict[str, object]:
    batches = []
    for batch, filename in BATCH_FILES.items():
        rows = grouped.get(batch, [])
        batches.append(
            {
                "review_batch": batch,
                "csv": str(output_dir / filename),
                "rows": len(rows),
                "priority_counts": Counter(row.get("priority", "") for row in rows).most_common(),
                "exchange_counts": Counter(row.get("exchange", "") for row in rows).most_common(20),
                "deepseek_status": "pending_segmented_review",
            }
        )
    return {
        "total_rows": sum(batch["rows"] for batch in batches),
        "batches": batches,
        "policy": (
            "Each CSV is a bounded DeepSeek review input. Run segment-by-segment; do not use one monolithic "
            "prompt for all global candidates."
        ),
    }


def write_markdown(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Twelve Data DeepSeek Review Batches",
        "",
        str(summary["policy"]),
        "",
        f"- Total rename candidates: {summary['total_rows']:,}",
        "",
        "| Review batch | Rows | CSV |",
        "| --- | ---: | --- |",
    ]
    for batch in summary["batches"]:
        lines.append(f"| {batch['review_batch']} | {batch['rows']:,} | `{batch['csv']}` |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY_MD)
    args = parser.parse_args()

    rows = read_csv(args.input_csv)
    grouped = split_rows(rows)
    fieldnames = list(rows[0]) if rows else []
    for batch, filename in BATCH_FILES.items():
        write_csv(args.output_dir / filename, grouped.get(batch, []), fieldnames)
    summary = summarize(grouped, args.output_dir)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.summary_md, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
