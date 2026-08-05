from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_REVIEW_GATE = "entry_quality_unexpected_warn_count"
CORRELATED_REVIEW_GATES = {"country_isin_prefix_mismatch_without_review"}


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def gate_details_match_unexpected_warnings(
    details: object,
    unexpected_warns: set[str],
) -> bool:
    if not isinstance(details, list) or not details or not unexpected_warns:
        return False

    ticker_matches: dict[str, set[str]] = {}
    for listing_key in unexpected_warns:
        if "::" not in listing_key:
            continue
        ticker_matches.setdefault(listing_key.rsplit("::", 1)[1], set()).add(listing_key)

    for detail in details:
        value = str(detail)
        if value in unexpected_warns:
            continue
        if "::" not in value and len(ticker_matches.get(value, set())) == 1:
            continue
        return False
    return True


def classify_gate_results(
    entry_quality_gate: dict[str, Any],
    validation_report: dict[str, Any],
    masterfile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    unexpected_warn_count = int(entry_quality_gate.get("unexpected_warn_count") or 0)
    unexpected_warns = {
        str(value) for value in entry_quality_gate.get("unexpected_warns", []) if value
    }
    quarantine_count = int(entry_quality_gate.get("quarantine_count") or 0)
    failed_gate_rows = [
        gate
        for gate in validation_report.get("gates", [])
        if gate.get("severity") == "error" and not gate.get("passed") and gate.get("name")
    ]
    failed_error_gates = sorted(str(gate.get("name", "")) for gate in failed_gate_rows)
    correlated_review_gates = {
        str(gate.get("name", ""))
        for gate in failed_gate_rows
        if gate.get("name") in CORRELATED_REVIEW_GATES
        and gate_details_match_unexpected_warnings(gate.get("details"), unexpected_warns)
    }
    hard_failures = sorted(
        set(failed_error_gates) - {EXPECTED_REVIEW_GATE} - correlated_review_gates
    )

    if quarantine_count:
        hard_failures.append("entry_quality_quarantine")
    if unexpected_warn_count and EXPECTED_REVIEW_GATE not in failed_error_gates:
        hard_failures.append("entry_quality_validation_report_mismatch")
    if not unexpected_warn_count and EXPECTED_REVIEW_GATE in failed_error_gates:
        hard_failures.append("entry_quality_gate_report_mismatch")
    if bool(entry_quality_gate.get("passed")) == bool(unexpected_warn_count or quarantine_count):
        hard_failures.append("entry_quality_gate_report_inconsistent")
    if not entry_quality_gate.get("passed") and not unexpected_warn_count and not quarantine_count:
        hard_failures.append("unclassified_entry_quality_failure")
    if bool(validation_report.get("passed")) == bool(failed_error_gates):
        hard_failures.append("database_validation_report_inconsistent")
    if not validation_report.get("passed") and not failed_error_gates:
        hard_failures.append("unclassified_database_validation_failure")

    masterfile_summary = masterfile_summary or {}
    source_details = masterfile_summary.get("source_details", {})
    last_refresh = masterfile_summary.get("last_refresh", {})
    selected_source_keys = last_refresh.get("selected_source_keys", [])
    refresh_modes = last_refresh.get("source_modes", {})
    source_review_keys = sorted(
        source_key
        for source_key in selected_source_keys
        if refresh_modes.get(source_key) == "unavailable"
        or (
            source_details.get(source_key, {}).get("official")
            and source_details.get(source_key, {}).get("reference_scope") == "exchange_directory"
            and refresh_modes.get(source_key) != "network"
        )
    )

    hard_failures = sorted(set(hard_failures))
    review_required = bool((unexpected_warn_count or source_review_keys) and not hard_failures)
    return {
        "passed": not hard_failures,
        "review_required": review_required,
        "unexpected_warn_count": unexpected_warn_count,
        "quarantine_count": quarantine_count,
        "failed_error_gates": failed_error_gates,
        "hard_failures": hard_failures,
        "source_review_count": len(source_review_keys),
        "source_review_keys": source_review_keys,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify masterfile gate failures as manual data review or hard workflow failures."
    )
    parser.add_argument("--entry-quality-gate", type=Path, required=True)
    parser.add_argument("--validation-report", type=Path, required=True)
    parser.add_argument("--masterfile-summary", type=Path)
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = classify_gate_results(
        load_json(args.entry_quality_gate),
        load_json(args.validation_report),
        load_json(args.masterfile_summary) if args.masterfile_summary else None,
    )
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as handle:
            handle.write(f"review_required={str(result['review_required']).lower()}\n")
            handle.write(f"unexpected_warn_count={result['unexpected_warn_count']}\n")
            handle.write(f"quarantine_count={result['quarantine_count']}\n")
            handle.write(f"source_review_count={result['source_review_count']}\n")
            handle.write(f"source_review_keys={','.join(result['source_review_keys'])}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
