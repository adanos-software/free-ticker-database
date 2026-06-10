"""Adjudicate Twelve Data name mismatches against repo and provider evidence.

This script does not mutate the ticker database. It turns the existing Twelve
Data mismatch, DeepSeek, provider-validation, identifier, and source-inventory
artifacts into deterministic decision classes that can later feed apply PRs.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_RENAME_CSV = Path("data/reports/twelvedata_rename_candidates.csv")
DEFAULT_CORE_LISTINGS_CSV = Path("data/core_listings.csv")
DEFAULT_IDENTIFIERS_CSV = Path("data/identifiers_extended.csv")
DEFAULT_METADATA_UPDATES_CSV = Path("data/review_overrides/metadata_updates.csv")
DEFAULT_SOURCE_TRUTH_CSV = Path("data/reports/source_of_truth_decisions.csv")
DEFAULT_COVERAGE_JSON = Path("data/reports/coverage_report.json")
DEFAULT_OUTPUT_CSV = Path("data/reports/twelvedata_source_adjudication.csv")
DEFAULT_APPLY_CSV = Path("data/reports/twelvedata_source_adjudication_apply_candidates.csv")
DEFAULT_JSON = Path("data/reports/twelvedata_source_adjudication_summary.json")
DEFAULT_MD = Path("data/reports/twelvedata_source_adjudication.md")

DEEPSEEK_CSVS = [
    Path("data/deepseek_review_jobs/twelvedata_batch_a_normalized_reviews.csv"),
    Path("data/deepseek_review_jobs/twelvedata_batch_b_normalized_reviews.csv"),
    Path("data/deepseek_review_jobs/twelvedata_batch_c_normalized_reviews.csv"),
    Path("data/deepseek_review_jobs/twelvedata_global_rest_normalized_reviews.csv"),
]

VALIDATION_CSVS = [
    Path("data/reports/twelvedata_batch_a_second_source_validation.csv"),
    Path("data/reports/twelvedata_batch_b_second_source_validation.csv"),
    Path("data/reports/twelvedata_batch_c_second_source_validation.csv"),
    Path("data/reports/twelvedata_global_rest_second_source_validation.csv"),
]

QUEUE_CSVS = [
    Path("data/reports/twelvedata_batch_a_second_source_queue.csv"),
    Path("data/reports/twelvedata_batch_b_second_source_queue.csv"),
    Path("data/reports/twelvedata_batch_c_second_source_queue.csv"),
    Path("data/reports/twelvedata_global_rest_second_source_queue.csv"),
]

SUPPORTED_TWELVEDATA_TYPES = {
    "American Depositary Receipt",
    "Common Stock",
    "Depositary Receipt",
    "Preferred Stock",
    "REIT",
}

OUTPUT_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "current_name",
    "twelvedata_name",
    "twelvedata_type",
    "type_scope",
    "review_batch",
    "name_score",
    "adjudication_decision",
    "apply_eligibility",
    "evidence_tier",
    "confidence",
    "recommended_operation",
    "source_gate",
    "evidence_summary",
    "provider_validation_status",
    "provider_supports_twelvedata",
    "provider_supports_local",
    "provider_conflicts",
    "openfigi_status",
    "openfigi_figi",
    "local_figi",
    "figi_relation",
    "venue_status",
    "official_source_count",
    "reference_scopes",
    "verification_name_mismatch",
    "masterfile_matches",
    "reviewed_name_override",
    "source_of_truth_outcome",
    "source_of_truth_gate",
    "deepseek_decision_candidate",
    "deepseek_safe_action",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def by_listing_key(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in rows if row.get("listing_key")}


def load_deepseek_rows(paths: list[Path]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for path in paths:
        rows.update(by_listing_key(read_csv(path)))
    return rows


def load_validation_rows(paths: list[Path]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for path in paths:
        rows.update(by_listing_key(read_csv(path)))
    return rows


def load_queue_rows(paths: list[Path]) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for path in paths:
        rows.update(by_listing_key(read_csv(path)))
    return rows


def load_exchange_coverage(coverage: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = coverage.get("by_exchange") or coverage.get("exchange_coverage") or []
    return {str(row.get("exchange", "")): row for row in rows if row.get("exchange")}


def load_reviewed_name_overrides(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    overrides: dict[str, dict[str, str]] = {}
    for row in rows:
        if row.get("field") != "name" or row.get("decision") != "update":
            continue
        key = f"{row.get('exchange', '')}::{row.get('ticker', '')}"
        overrides[key] = row
    return overrides


def load_source_truth(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("listing_key"):
            grouped[row["listing_key"]].append(row)
    return grouped


def provider_support(row: dict[str, str], target: str) -> list[str]:
    suffix = "supports_twelvedata" if target == "twelvedata" else "supports_local"
    providers: list[str] = []
    for provider, field in [
        ("OpenFIGI", "openfigi_match"),
        ("AlphaVantage", "alphavantage_match"),
        ("FMP", "fmp_match"),
    ]:
        if row.get(field) == suffix:
            providers.append(provider)
    return providers


def figi_relation(local_figi: str, openfigi_figi: str) -> str:
    if local_figi and openfigi_figi and local_figi == openfigi_figi:
        return "local_figi_match"
    if local_figi and openfigi_figi and local_figi != openfigi_figi:
        return "figi_mismatch"
    if local_figi:
        return "local_figi_only"
    if openfigi_figi:
        return "openfigi_only"
    return "no_figi"


def reviewed_override_status(row: dict[str, str] | None, current_name: str, twelvedata_name: str) -> str:
    if not row:
        return ""
    value = row.get("proposed_value", "")
    if value == current_name:
        return "supports_local"
    if value == twelvedata_name:
        return "supports_twelvedata"
    return "other_reviewed_name"


def source_truth_summary(rows: list[dict[str, str]]) -> tuple[str, str]:
    if not rows:
        return "", ""
    outcomes = sorted({row.get("source_of_truth_outcome", "") for row in rows if row.get("source_of_truth_outcome")})
    gates = sorted({row.get("source_gate", "") for row in rows if row.get("source_gate")})
    return "|".join(outcomes), " | ".join(gates)


def classify_row(
    row: dict[str, str],
    *,
    deepseek: dict[str, dict[str, str]],
    validation: dict[str, dict[str, str]],
    queues: dict[str, dict[str, str]],
    identifiers: dict[str, dict[str, str]],
    coverage: dict[str, dict[str, Any]],
    name_overrides: dict[str, dict[str, str]],
    source_truth: dict[str, list[dict[str, str]]],
) -> dict[str, str]:
    listing_key = row["listing_key"]
    exchange = row["exchange"]
    current_name = row.get("local_name", "")
    twelvedata_name = row.get("twelvedata_name", "")
    twelvedata_type = row.get("twelvedata_type", "")
    type_scope = "supported_core_stock_type" if twelvedata_type in SUPPORTED_TWELVEDATA_TYPES else "type_out_of_scope"

    deepseek_row = deepseek.get(listing_key, {})
    validation_row = validation.get(listing_key, {})
    queue_row = queues.get(listing_key, {})
    identifier_row = identifiers.get(listing_key, {})
    coverage_row = coverage.get(exchange, {})
    override_status = reviewed_override_status(name_overrides.get(listing_key), current_name, twelvedata_name)
    source_outcome, source_gate = source_truth_summary(source_truth.get(listing_key, []))

    openfigi_figi = validation_row.get("openfigi_figi", "")
    local_figi = identifier_row.get("figi", "")
    relation = figi_relation(local_figi, openfigi_figi)
    supports_twelve = provider_support(validation_row, "twelvedata")
    supports_local = provider_support(validation_row, "local")
    validation_status = validation_row.get("validation_status") or queue_row.get("validation_status", "")
    provider_conflicts = "true" if validation_status == "conflicting_second_source_evidence" else "false"
    official_name_mismatches = int(coverage_row.get("verification_name_mismatch") or 0)
    venue_status = str(coverage_row.get("venue_status", ""))

    decision = "source_gap_needs_primary_evidence"
    eligibility = "blocked"
    tier = "none"
    confidence = "0.00"
    recommended = "collect_primary_or_identifier_evidence"
    gate = "Do not apply until official or identifier evidence resolves the name mismatch."
    evidence = "No decisive repo or provider evidence supports applying the Twelve Data name."

    if type_scope == "type_out_of_scope":
        decision = "type_out_of_scope"
        recommended = "exclude_from_core_stock_name_apply"
        gate = "Only Common Stock, REIT, ADR/DR, and Preferred Stock are eligible for this adjudication."
        evidence = f"Twelve Data type {twelvedata_type!r} is outside the requested stock-type scope."
    elif override_status == "supports_local":
        decision = "keep_local_name_reviewed_override"
        tier = "reviewed_override"
        confidence = "0.96"
        recommended = "keep_local_name"
        gate = "Reviewed repo name override wins over secondary-source mismatch evidence."
        evidence = "Existing reviewed metadata name override supports the current local name."
    elif override_status == "supports_twelvedata":
        decision = "apply_twelvedata_name_reviewed_override"
        eligibility = "apply_ready"
        tier = "reviewed_override"
        confidence = "0.96"
        recommended = "build_metadata_name_update"
        gate = "Existing reviewed metadata name override supports the Twelve Data name."
        evidence = "Existing reviewed metadata name override matches the Twelve Data name."
    elif relation == "figi_mismatch":
        decision = "conflict_blocked_figi_mismatch"
        tier = "identifier_conflict"
        recommended = "resolve_identifier_conflict_before_name_change"
        gate = "OpenFIGI returned a different FIGI than the local listing-keyed identifier."
        evidence = f"Local FIGI {local_figi} conflicts with OpenFIGI FIGI {openfigi_figi}."
    elif provider_conflicts == "true":
        decision = "conflict_blocked_provider_disagreement"
        tier = "provider_conflict"
        recommended = "resolve_provider_disagreement"
        gate = "Provider evidence supports both local and Twelve Data names."
        evidence = validation_row.get("evidence_summary", "Provider evidence conflicts.")
    elif supports_local:
        decision = "keep_local_name_provider_supported"
        tier = "provider"
        confidence = "0.88"
        recommended = "keep_local_name"
        gate = "Do not apply Twelve Data name while provider evidence supports the current local name."
        evidence = f"{'|'.join(supports_local)} supports the current local name."
    elif "core_exclusion_candidate" in source_outcome.split("|"):
        decision = "scope_review_blocked"
        tier = "source_of_truth_scope_gate"
        recommended = "resolve_core_scope_before_name_apply"
        gate = source_gate or "Core exclusion candidates require scope review before name or metadata changes."
        evidence = "Existing source-of-truth decision marks this listing as a core-exclusion candidate."
    elif supports_twelve:
        if "OpenFIGI" in supports_twelve and relation in {"local_figi_match", "openfigi_only"}:
            decision = "apply_twelvedata_name_identifier_supported"
            eligibility = "apply_ready"
            tier = "identifier"
            confidence = "0.91" if relation == "local_figi_match" else "0.88"
            recommended = "build_metadata_name_update"
            gate = "OpenFIGI supports the Twelve Data name without conflicting with the local listing FIGI."
            evidence = f"OpenFIGI supports Twelve Data name; FIGI relation={relation}."
        elif len(supports_twelve) >= 2:
            decision = "apply_twelvedata_name_multi_provider_supported"
            eligibility = "apply_ready"
            tier = "multi_provider"
            confidence = "0.87"
            recommended = "build_metadata_name_update"
            gate = "Multiple providers support the Twelve Data name and none support the local name."
            evidence = f"{'|'.join(supports_twelve)} support the Twelve Data name."
        else:
            decision = "candidate_needs_primary_source"
            tier = "single_provider"
            confidence = "0.70"
            recommended = "validate_against_official_source_before_apply"
            gate = "Single non-conflicting provider support is not enough without identifier or official-source evidence."
            evidence = f"{'|'.join(supports_twelve)} supports the Twelve Data name, but no strong identifier gate passed."
    elif validation_status == "provider_found_different_name":
        decision = "conflict_blocked_provider_third_name"
        tier = "provider_conflict"
        recommended = "resolve_third_name_before_apply"
        gate = "Provider evidence supports neither local nor Twelve Data clearly."
        evidence = validation_row.get("evidence_summary", "Provider found a third name.")
    elif validation_status == "ambiguous_second_source_evidence":
        decision = "ambiguous_blocked"
        tier = "provider_ambiguous"
        recommended = "collect_stronger_name_evidence"
        gate = "Provider name similarity is ambiguous."
        evidence = validation_row.get("evidence_summary", "Provider evidence is ambiguous.")
    elif validation_status == "no_second_source_name_match":
        decision = "source_gap_no_provider_match"
        recommended = "collect_primary_or_identifier_evidence"
        evidence = validation_row.get("evidence_summary", "Providers returned no usable name match.")
    elif validation_status == "pending_provider_env":
        decision = "provider_validation_pending"
        recommended = "run_provider_validation_for_segment"
        gate = queue_row.get("evidence_required", gate)
        evidence = "DeepSeek reviewed this row, but provider validation has not been run for this segment."
    elif deepseek_row.get("safe_action") == "source_gap_accept":
        decision = "source_gap_keep_local"
        tier = "deepseek_triage"
        recommended = "keep_local_until_primary_source_available"
        gate = "DeepSeek classified this as a source gap; it is not apply evidence."
        evidence = deepseek_row.get("rationale", "DeepSeek source-gap triage.")
    elif venue_status == "official_full" and official_name_mismatches == 0:
        decision = "keep_local_name_official_exchange_context"
        tier = "official_exchange_context"
        confidence = "0.82"
        recommended = "keep_local_until_row_level_official_conflict"
        gate = "The exchange has official-full coverage and no current official name-mismatch findings."
        evidence = "Repo source inventory favors current local name absent stronger row-level contrary evidence."

    return {
        "listing_key": listing_key,
        "ticker": row.get("ticker", ""),
        "exchange": exchange,
        "current_name": current_name,
        "twelvedata_name": twelvedata_name,
        "twelvedata_type": twelvedata_type,
        "type_scope": type_scope,
        "review_batch": row.get("review_batch", ""),
        "name_score": row.get("name_score", ""),
        "adjudication_decision": decision,
        "apply_eligibility": eligibility,
        "evidence_tier": tier,
        "confidence": confidence,
        "recommended_operation": recommended,
        "source_gate": gate,
        "evidence_summary": evidence,
        "provider_validation_status": validation_status,
        "provider_supports_twelvedata": "|".join(supports_twelve),
        "provider_supports_local": "|".join(supports_local),
        "provider_conflicts": provider_conflicts,
        "openfigi_status": validation_row.get("openfigi_status", ""),
        "openfigi_figi": openfigi_figi,
        "local_figi": local_figi,
        "figi_relation": relation,
        "venue_status": venue_status,
        "official_source_count": str(coverage_row.get("official_source_count", "")),
        "reference_scopes": "|".join(coverage_row.get("reference_scopes", []) or []),
        "verification_name_mismatch": str(coverage_row.get("verification_name_mismatch", "")),
        "masterfile_matches": str(coverage_row.get("masterfile_matches", "")),
        "reviewed_name_override": override_status,
        "source_of_truth_outcome": source_outcome,
        "source_of_truth_gate": source_gate,
        "deepseek_decision_candidate": deepseek_row.get("decision_candidate", ""),
        "deepseek_safe_action": deepseek_row.get("safe_action", ""),
    }


def build_adjudication(
    rename_rows: list[dict[str, str]],
    *,
    deepseek: dict[str, dict[str, str]],
    validation: dict[str, dict[str, str]],
    queues: dict[str, dict[str, str]],
    identifiers: dict[str, dict[str, str]],
    coverage: dict[str, dict[str, Any]],
    name_overrides: dict[str, dict[str, str]],
    source_truth: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    rows = [
        classify_row(
            row,
            deepseek=deepseek,
            validation=validation,
            queues=queues,
            identifiers=identifiers,
            coverage=coverage,
            name_overrides=name_overrides,
            source_truth=source_truth,
        )
        for row in rename_rows
    ]
    return sorted(rows, key=lambda row: (row["apply_eligibility"] != "apply_ready", row["exchange"], row["ticker"]))


def summarize(rows: list[dict[str, str]]) -> dict[str, Any]:
    apply_rows = [row for row in rows if row["apply_eligibility"] == "apply_ready"]
    return {
        "rows": len(rows),
        "apply_ready_rows": len(apply_rows),
        "decision_counts": Counter(row["adjudication_decision"] for row in rows).most_common(),
        "apply_decision_counts": Counter(row["adjudication_decision"] for row in apply_rows).most_common(),
        "batch_counts": Counter(row["review_batch"] for row in rows).most_common(),
        "apply_batch_counts": Counter(row["review_batch"] for row in apply_rows).most_common(),
        "evidence_tier_counts": Counter(row["evidence_tier"] for row in rows).most_common(),
        "type_scope_counts": Counter(row["type_scope"] for row in rows).most_common(),
        "policy": (
            "Twelve Data is treated as a challenger source. Apply-ready rows require supported stock type scope and "
            "non-conflicting provider, identifier, or reviewed-override evidence. Conflicts, source gaps, and pending "
            "provider validation are blocked from database mutation."
        ),
    }


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Twelve Data Source Adjudication",
        "",
        str(summary["policy"]),
        "",
        "## Totals",
        "",
        f"- Rows adjudicated: {summary['rows']:,}",
        f"- Apply-ready rows: {summary['apply_ready_rows']:,}",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {decision} | {count:,} |" for decision, count in summary["decision_counts"])
    lines.extend(["", "## Apply-Ready Decisions", "", "| Decision | Rows |", "| --- | ---: |"])
    lines.extend(f"| {decision} | {count:,} |" for decision, count in summary["apply_decision_counts"])
    lines.extend(["", "## Apply-Ready Batches", "", "| Batch | Rows |", "| --- | ---: |"])
    lines.extend(f"| {batch} | {count:,} |" for batch, count in summary["apply_batch_counts"])
    lines.extend(["", "## Evidence Tiers", "", "| Tier | Rows |", "| --- | ---: |"])
    lines.extend(f"| {tier or 'none'} | {count:,} |" for tier, count in summary["evidence_tier_counts"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rename-csv", type=Path, default=DEFAULT_RENAME_CSV)
    parser.add_argument("--core-listings-csv", type=Path, default=DEFAULT_CORE_LISTINGS_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=DEFAULT_IDENTIFIERS_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--source-truth-csv", type=Path, default=DEFAULT_SOURCE_TRUTH_CSV)
    parser.add_argument("--coverage-json", type=Path, default=DEFAULT_COVERAGE_JSON)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--apply-csv", type=Path, default=DEFAULT_APPLY_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()

    rows = build_adjudication(
        read_csv(args.rename_csv),
        deepseek=load_deepseek_rows(DEEPSEEK_CSVS),
        validation=load_validation_rows(VALIDATION_CSVS),
        queues=load_queue_rows(QUEUE_CSVS),
        identifiers=by_listing_key(read_csv(args.identifiers_csv)),
        coverage=load_exchange_coverage(load_json(args.coverage_json)),
        name_overrides=load_reviewed_name_overrides(read_csv(args.metadata_updates_csv)),
        source_truth=load_source_truth(read_csv(args.source_truth_csv)),
    )
    apply_rows = [row for row in rows if row["apply_eligibility"] == "apply_ready"]
    write_csv(args.output_csv, rows, OUTPUT_FIELDS)
    write_csv(args.apply_csv, apply_rows, OUTPUT_FIELDS)
    summary = summarize(rows)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.summary_md, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
