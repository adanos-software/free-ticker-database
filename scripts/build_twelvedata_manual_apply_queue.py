"""Build a conservative manual apply queue from validated Twelve Data evidence."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


DEFAULT_VALIDATION_CSV = Path("data/reports/twelvedata_batch_a_second_source_validation.csv")
DEFAULT_APPLY_CSV = Path("data/reports/twelvedata_batch_a_manual_apply_candidates.csv")
DEFAULT_REJECTED_CSV = Path("data/reports/twelvedata_batch_a_manual_apply_rejected.csv")
DEFAULT_SUMMARY_JSON = Path("data/reports/twelvedata_batch_a_manual_apply_summary.json")
DEFAULT_SUMMARY_MD = Path("data/reports/twelvedata_batch_a_manual_apply.md")

APPLY_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "current_name",
    "proposed_name",
    "twelvedata_type",
    "name_score",
    "supporting_providers",
    "supporting_provider_names",
    "figi_evidence",
    "deepseek_decision_candidate",
    "deepseek_safe_action",
    "apply_status",
    "apply_gate",
    "recommended_operation",
]

REJECTED_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "current_name",
    "twelvedata_name",
    "validation_status",
    "recommended_next_action",
    "rejection_reason",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def provider_support(row: dict[str, str]) -> tuple[list[str], list[str]]:
    providers: list[str] = []
    names: list[str] = []
    for provider, name_field, match_field in [
        ("OpenFIGI", "openfigi_name", "openfigi_match"),
        ("AlphaVantage", "alphavantage_name", "alphavantage_match"),
        ("FMP", "fmp_name", "fmp_match"),
    ]:
        if row.get(match_field) == "supports_twelvedata":
            providers.append(provider)
            provider_name = row.get(name_field, "")
            if provider_name:
                names.append(f"{provider}:{provider_name}")
    return providers, names


def rejection_reason(row: dict[str, str]) -> str:
    status = row.get("validation_status", "")
    if status == "second_source_supports_local_name":
        return "second_source_supports_current_local_name"
    if status == "conflicting_second_source_evidence":
        return "provider_evidence_conflicts_between_local_and_twelvedata"
    if status == "provider_found_different_name":
        return "provider_name_supports_neither_current_nor_twelvedata"
    if status == "ambiguous_second_source_evidence":
        return "provider_name_similarity_is_ambiguous"
    if status == "no_second_source_name_match":
        return "no_provider_supplied_usable_matching_name"
    return "not_eligible_for_manual_apply_candidate"


def build_queues(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    apply_rows: list[dict[str, str]] = []
    rejected_rows: list[dict[str, str]] = []
    for row in rows:
        providers, provider_names = provider_support(row)
        if row.get("validation_status") == "second_source_supports_twelvedata_name" and providers:
            apply_rows.append(
                {
                    "listing_key": row.get("listing_key", ""),
                    "ticker": row.get("ticker", ""),
                    "exchange": row.get("exchange", ""),
                    "current_name": row.get("local_name", ""),
                    "proposed_name": row.get("twelvedata_name", ""),
                    "twelvedata_type": row.get("twelvedata_type", ""),
                    "name_score": row.get("name_score", ""),
                    "supporting_providers": "|".join(providers),
                    "supporting_provider_names": "|".join(provider_names),
                    "figi_evidence": row.get("openfigi_figi", ""),
                    "deepseek_decision_candidate": row.get("deepseek_decision_candidate", ""),
                    "deepseek_safe_action": row.get("deepseek_safe_action", ""),
                    "apply_status": "manual_review_required",
                    "apply_gate": (
                        "Do not apply automatically. Confirm exact listing identity with official or identifier "
                        "evidence, then apply in a small reviewed batch with rebuild and validation gates."
                    ),
                    "recommended_operation": "review_name_update",
                }
            )
        else:
            rejected_rows.append(
                {
                    "listing_key": row.get("listing_key", ""),
                    "ticker": row.get("ticker", ""),
                    "exchange": row.get("exchange", ""),
                    "current_name": row.get("local_name", ""),
                    "twelvedata_name": row.get("twelvedata_name", ""),
                    "validation_status": row.get("validation_status", ""),
                    "recommended_next_action": row.get("recommended_next_action", ""),
                    "rejection_reason": rejection_reason(row),
                }
            )
    return apply_rows, rejected_rows


def summarize(apply_rows: list[dict[str, str]], rejected_rows: list[dict[str, str]]) -> dict[str, object]:
    return {
        "manual_apply_candidates": len(apply_rows),
        "rejected_or_followup_rows": len(rejected_rows),
        "candidate_exchange_counts": Counter(row["exchange"] for row in apply_rows).most_common(),
        "supporting_provider_counts": Counter(
            provider
            for row in apply_rows
            for provider in row["supporting_providers"].split("|")
            if provider
        ).most_common(),
        "rejection_reason_counts": Counter(row["rejection_reason"] for row in rejected_rows).most_common(),
        "policy": (
            "This is a manual apply-candidate queue only. It authorizes no automatic data changes; official or "
            "identifier evidence and dataset gates are still required before applying."
        ),
    }


def write_markdown(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Twelve Data Batch A Manual Apply Candidates",
        "",
        str(summary["policy"]),
        "",
        "No `data/review_overrides/metadata_updates.csv` rows are generated by this script. The next step is a "
        "human-reviewed micro-batch that converts selected rows into repo-standard metadata overrides, followed by "
        "dataset rebuild and validation gates.",
        "",
        f"- Manual apply candidates: {summary['manual_apply_candidates']:,}",
        f"- Rejected/follow-up rows: {summary['rejected_or_followup_rows']:,}",
        "",
        "## Candidate Exchanges",
        "",
        "| Exchange | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {exchange} | {count:,} |" for exchange, count in summary["candidate_exchange_counts"])
    lines.extend(["", "## Supporting Providers", "", "| Provider | Rows |", "| --- | ---: |"])
    lines.extend(f"| {provider} | {count:,} |" for provider, count in summary["supporting_provider_counts"])
    lines.extend(["", "## Rejection Reasons", "", "| Reason | Rows |", "| --- | ---: |"])
    lines.extend(f"| {reason} | {count:,} |" for reason, count in summary["rejection_reason_counts"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-csv", type=Path, default=DEFAULT_VALIDATION_CSV)
    parser.add_argument("--apply-csv", type=Path, default=DEFAULT_APPLY_CSV)
    parser.add_argument("--rejected-csv", type=Path, default=DEFAULT_REJECTED_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY_MD)
    args = parser.parse_args()

    apply_rows, rejected_rows = build_queues(read_csv(args.validation_csv))
    write_csv(args.apply_csv, apply_rows, APPLY_FIELDS)
    write_csv(args.rejected_csv, rejected_rows, REJECTED_FIELDS)
    summary = summarize(apply_rows, rejected_rows)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.summary_md, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
