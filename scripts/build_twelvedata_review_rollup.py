"""Build a rollup summary for the segmented Twelve Data review workflow."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_OUTPUT_JSON = Path("data/reports/twelvedata_all_batches_review_rollup.json")
DEFAULT_OUTPUT_MD = Path("data/reports/twelvedata_all_batches_review_rollup.md")
DEFAULT_ADJUDICATION_CSV = Path("data/reports/twelvedata_source_adjudication.csv")

def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


SEGMENTS = [
    {
        "segment": "batch_a_us_core",
        "rename_csv": Path("data/deepseek_review_jobs/twelvedata_batch_a_rename_candidates.csv"),
        "deepseek_csv": Path("data/deepseek_review_jobs/twelvedata_batch_a_normalized_reviews.csv"),
        "deepseek_errors": Path("data/deepseek_review_jobs/twelvedata_batch_a_errors.json"),
        "second_source_queue": Path("data/reports/twelvedata_batch_a_second_source_queue.csv"),
        "second_source_validation": Path("data/reports/twelvedata_batch_a_second_source_validation.csv"),
        "manual_apply": Path("data/reports/twelvedata_batch_a_manual_apply_candidates.csv"),
    },
    {
        "segment": "batch_b_canada",
        "rename_csv": Path("data/deepseek_review_jobs/twelvedata_batch_b_rename_candidates.csv"),
        "deepseek_csv": Path("data/deepseek_review_jobs/twelvedata_batch_b_normalized_reviews.csv"),
        "deepseek_errors": Path("data/deepseek_review_jobs/twelvedata_batch_b_errors.json"),
        "second_source_queue": Path("data/reports/twelvedata_batch_b_second_source_queue.csv"),
        "second_source_validation": Path("data/reports/twelvedata_batch_b_second_source_validation.csv"),
        "manual_apply": Path("data/reports/twelvedata_batch_b_manual_apply_candidates.csv"),
    },
    {
        "segment": "batch_c_high_value_international",
        "rename_csv": Path("data/deepseek_review_jobs/twelvedata_batch_c_rename_candidates.csv"),
        "deepseek_csv": Path("data/deepseek_review_jobs/twelvedata_batch_c_normalized_reviews.csv"),
        "deepseek_errors": Path("data/deepseek_review_jobs/twelvedata_batch_c_errors.json"),
        "second_source_queue": Path("data/reports/twelvedata_batch_c_second_source_queue.csv"),
        "second_source_validation": Path("data/reports/twelvedata_batch_c_second_source_validation.csv"),
        "manual_apply": Path("data/reports/twelvedata_batch_c_manual_apply_candidates.csv"),
    },
    {
        "segment": "later_global_review",
        "rename_csv": Path("data/deepseek_review_jobs/twelvedata_global_rest_rename_candidates.csv"),
        "deepseek_csv": Path("data/deepseek_review_jobs/twelvedata_global_rest_normalized_reviews.csv"),
        "deepseek_errors": Path("data/deepseek_review_jobs/twelvedata_global_rest_errors.json"),
        "second_source_queue": Path("data/reports/twelvedata_global_rest_second_source_queue.csv"),
        "second_source_validation": Path("data/reports/twelvedata_global_rest_second_source_validation.csv"),
        "manual_apply": Path("data/reports/twelvedata_global_rest_manual_apply_candidates.csv"),
    },
]


def csv_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def csv_counter(path: Path, field: str) -> list[tuple[str, int]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return Counter(row.get(field, "") for row in csv.DictReader(handle)).most_common()


def error_count(path: Path) -> int:
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    return len(payload.get("errors", []))


def segment_summary(segment: dict[str, Path | str]) -> dict[str, object]:
    rename_rows = csv_count(segment["rename_csv"])
    deepseek_rows = csv_count(segment["deepseek_csv"])
    second_source_queue_rows = csv_count(segment["second_source_queue"])
    second_source_validation_rows = csv_count(segment["second_source_validation"])
    manual_apply_rows = csv_count(segment["manual_apply"])
    return {
        "segment": segment["segment"],
        "rename_candidates": rename_rows,
        "deepseek_reviews": deepseek_rows,
        "deepseek_errors": error_count(segment["deepseek_errors"]),
        "deepseek_coverage_status": "complete" if rename_rows and deepseek_rows == rename_rows else "incomplete",
        "deepseek_decision_counts": csv_counter(segment["deepseek_csv"], "decision_candidate"),
        "second_source_queue_rows": second_source_queue_rows,
        "second_source_validation_rows": second_source_validation_rows,
        "second_source_status": (
            "validated"
            if second_source_validation_rows
            else "queued_pending_provider_validation"
            if second_source_queue_rows
            else "not_queued"
        ),
        "validation_status_counts": csv_counter(segment["second_source_validation"], "validation_status"),
        "manual_apply_candidates": manual_apply_rows,
    }


def build_summary() -> dict[str, object]:
    segments = [segment_summary(segment) for segment in SEGMENTS]
    adjudication_rows = csv_count(DEFAULT_ADJUDICATION_CSV)
    adjudication_apply_rows = sum(
        count
        for eligibility, count in csv_counter(DEFAULT_ADJUDICATION_CSV, "apply_eligibility")
        if eligibility == "apply_ready"
    )
    return {
        "generated_at": now_iso(),
        "segments": segments,
        "totals": {
            "rename_candidates": sum(int(segment["rename_candidates"]) for segment in segments),
            "deepseek_reviews": sum(int(segment["deepseek_reviews"]) for segment in segments),
            "deepseek_errors": sum(int(segment["deepseek_errors"]) for segment in segments),
            "second_source_queue_rows": sum(int(segment["second_source_queue_rows"]) for segment in segments),
            "second_source_validation_rows": sum(int(segment["second_source_validation_rows"]) for segment in segments),
            "manual_apply_candidates": sum(int(segment["manual_apply_candidates"]) for segment in segments),
            "source_adjudication_rows": adjudication_rows,
            "source_adjudication_apply_ready_rows": adjudication_apply_rows,
        },
        "source_adjudication": {
            "rows": adjudication_rows,
            "apply_ready_rows": adjudication_apply_rows,
            "decision_counts": csv_counter(DEFAULT_ADJUDICATION_CSV, "adjudication_decision"),
            "apply_eligibility_counts": csv_counter(DEFAULT_ADJUDICATION_CSV, "apply_eligibility"),
        },
        "policy": (
            "Twelve Data is a challenger source. DeepSeek reviews are advisory; apply-ready rows come from the "
            "source-adjudication report after provider, identifier, reviewed-override, and source-inventory gates. "
            "No database changes are applied by these reports."
        ),
    }


def write_markdown(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Twelve Data All-Batch Review Rollup",
        "",
        str(summary["policy"]),
        "",
        "## Totals",
        "",
    ]
    totals = summary["totals"]
    for key in [
        "rename_candidates",
        "deepseek_reviews",
        "deepseek_errors",
        "second_source_queue_rows",
        "second_source_validation_rows",
        "manual_apply_candidates",
        "source_adjudication_rows",
        "source_adjudication_apply_ready_rows",
    ]:
        lines.append(f"- {key}: {totals[key]:,}")
    adjudication = summary["source_adjudication"]
    lines.extend(["", "## Source Adjudication", "", "| Decision | Rows |", "| --- | ---: |"])
    lines.extend(f"| {decision} | {count:,} |" for decision, count in adjudication["decision_counts"])
    lines.extend(
        [
            "",
            "## Segments",
            "",
            "| Segment | Rename rows | DeepSeek rows | Errors | Second-source queue | Second-source validated | Manual candidates | Status |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for segment in summary["segments"]:
        lines.append(
            "| {segment} | {rename_candidates:,} | {deepseek_reviews:,} | {deepseek_errors:,} | "
            "{second_source_queue_rows:,} | {second_source_validation_rows:,} | {manual_apply_candidates:,} | "
            "{second_source_status} |".format(**segment)
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    args = parser.parse_args()

    summary = build_summary()
    args.output_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.output_md, summary)
    print(json.dumps(summary["totals"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
