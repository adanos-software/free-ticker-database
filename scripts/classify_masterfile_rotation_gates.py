from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


EXPECTED_REVIEW_GATE = "entry_quality_unexpected_warn_count"
ENTRY_QUALITY_REVIEW_POLICY = "entry_quality_warning"
REVIEW_REQUIRED_MASTERFILE_FIELDS = {
    "asset_type",
    "isin",
    "listing_status",
    "official",
    "reference_scope",
    "sector",
}
MASTERFILE_COMPARE_FIELDS = REVIEW_REQUIRED_MASTERFILE_FIELDS | {"name"}


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def parse_warning_subjects(value: object) -> set[tuple[str, str]] | None:
    if not isinstance(value, list) or not value:
        return None

    subjects: set[tuple[str, str]] = set()
    for item in value:
        if not isinstance(item, dict):
            return None
        listing_key = item.get("listing_key")
        issue_type = item.get("issue_type")
        if (
            not isinstance(listing_key, str)
            or "::" not in listing_key
            or not isinstance(issue_type, str)
            or not issue_type
        ):
            return None
        subjects.add((listing_key, issue_type))
    return subjects


def parse_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def has_valid_failure_count(gate: dict[str, Any], *, minimum_actual: int = 1) -> bool:
    actual = parse_nonnegative_int(gate.get("actual"))
    limit = parse_nonnegative_int(gate.get("limit"))
    return (
        actual is not None
        and limit is not None
        and actual >= minimum_actual
        and actual > limit
    )


def classify_critical_rotation_changes(
    rotation_diff: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], bool]:
    if rotation_diff is None:
        return [], False
    changed_rows = rotation_diff.get("changed")
    if not isinstance(changed_rows, list):
        return [], True

    critical_changes: list[dict[str, Any]] = []
    malformed = False
    for row in changed_rows:
        if not isinstance(row, dict) or not isinstance(row.get("changes"), dict):
            malformed = True
            continue
        changes = row["changes"]
        identity = [row.get(field) for field in ("source_key", "exchange", "ticker")]
        if (
            not changes
            or any(not isinstance(value, str) or not value for value in identity)
            or any(
                field not in MASTERFILE_COMPARE_FIELDS
                or not isinstance(change, dict)
                or not isinstance(change.get("before"), str)
                or not isinstance(change.get("after"), str)
                or change["before"] == change["after"]
                for field, change in changes.items()
            )
        ):
            malformed = True
            continue
        fields = sorted(REVIEW_REQUIRED_MASTERFILE_FIELDS.intersection(changes))
        if not fields:
            continue
        critical_changes.append(
            {
                "source_key": str(row.get("source_key", "")),
                "exchange": str(row.get("exchange", "")),
                "ticker": str(row.get("ticker", "")),
                "fields": fields,
            }
        )
    return critical_changes, malformed


def classify_gate_results(
    entry_quality_gate: dict[str, Any],
    validation_report: dict[str, Any],
    masterfile_summary: dict[str, Any] | None = None,
    rotation_diff: dict[str, Any] | None = None,
    entry_quality_outcome: str | None = None,
    database_outcome: str | None = None,
) -> dict[str, Any]:
    parsed_unexpected_warn_count = parse_nonnegative_int(
        entry_quality_gate.get("unexpected_warn_count")
    )
    parsed_quarantine_count = parse_nonnegative_int(entry_quality_gate.get("quarantine_count"))
    count_report_inconsistent = (
        parsed_unexpected_warn_count is None or parsed_quarantine_count is None
    )
    unexpected_warn_count = parsed_unexpected_warn_count or 0
    quarantine_count = parsed_quarantine_count or 0
    raw_unexpected_warning_subjects = entry_quality_gate.get("unexpected_warning_subjects")
    parsed_unexpected_warning_subjects = parse_warning_subjects(raw_unexpected_warning_subjects)
    unexpected_warning_subjects = parsed_unexpected_warning_subjects or set()
    warning_subject_payload_malformed = (
        raw_unexpected_warning_subjects is not None
        and not isinstance(raw_unexpected_warning_subjects, list)
    ) or (
        isinstance(raw_unexpected_warning_subjects, list)
        and bool(raw_unexpected_warning_subjects)
        and parsed_unexpected_warning_subjects is None
    )
    unexpected_warning_subject_keys = {
        listing_key for listing_key, _issue_type in unexpected_warning_subjects
    }
    warning_subject_report_inconsistent = warning_subject_payload_malformed or (
        unexpected_warn_count > 0
        and len(unexpected_warning_subject_keys) != unexpected_warn_count
    ) or (unexpected_warn_count == 0 and bool(unexpected_warning_subjects))
    failed_gate_rows = [
        gate
        for gate in validation_report.get("gates", [])
        if gate.get("severity") == "error" and not gate.get("passed") and gate.get("name")
    ]
    failed_error_gates = sorted(str(gate.get("name", "")) for gate in failed_gate_rows)
    duplicate_failed_gate_names = {
        name for name, count in Counter(failed_error_gates).items() if count > 1
    }
    expected_review_gate_rows = [
        gate for gate in failed_gate_rows if gate.get("name") == EXPECTED_REVIEW_GATE
    ]
    validation_warning_report_mismatch = not count_report_inconsistent and (
        (
            unexpected_warn_count > 0
            and (
                len(expected_review_gate_rows) != 1
                or parse_nonnegative_int(expected_review_gate_rows[0].get("actual"))
                != unexpected_warn_count
                or not has_valid_failure_count(expected_review_gate_rows[0])
            )
        )
        or (unexpected_warn_count == 0 and bool(expected_review_gate_rows))
    )
    hard_failures: list[str] = []
    for gate in failed_gate_rows:
        gate_name = str(gate.get("name", ""))
        if gate_name == EXPECTED_REVIEW_GATE:
            continue
        gate_subjects = parse_warning_subjects(gate.get("review_subjects"))
        if (
            gate.get("review_policy") == ENTRY_QUALITY_REVIEW_POLICY
            and not count_report_inconsistent
            and not validation_warning_report_mismatch
            and not warning_subject_report_inconsistent
            and gate.get("review_subjects_complete") is True
            and gate_subjects is not None
            and has_valid_failure_count(
                gate,
                minimum_actual=len(
                    {listing_key for listing_key, _issue_type in gate_subjects}
                ),
            )
            and gate_subjects <= unexpected_warning_subjects
        ):
            continue
        hard_failures.append(gate_name)

    if quarantine_count:
        hard_failures.append("entry_quality_quarantine")
    if count_report_inconsistent:
        hard_failures.append("entry_quality_count_report_inconsistent")
    if warning_subject_report_inconsistent:
        hard_failures.append("entry_quality_warning_subject_report_inconsistent")
    if validation_warning_report_mismatch:
        hard_failures.append("entry_quality_validation_report_mismatch")
    if duplicate_failed_gate_names:
        hard_failures.append("duplicate_validation_gate_names")
    if not count_report_inconsistent:
        if bool(entry_quality_gate.get("passed")) == bool(
            unexpected_warn_count or quarantine_count
        ):
            hard_failures.append("entry_quality_gate_report_inconsistent")
        if not entry_quality_gate.get("passed") and not unexpected_warn_count and not quarantine_count:
            hard_failures.append("unclassified_entry_quality_failure")
    if bool(validation_report.get("passed")) == bool(failed_error_gates):
        hard_failures.append("database_validation_report_inconsistent")
    if not validation_report.get("passed") and not failed_error_gates:
        hard_failures.append("unclassified_database_validation_failure")
    if entry_quality_outcome is not None:
        expected_outcome = "success" if entry_quality_gate.get("passed") else "failure"
        if entry_quality_outcome != expected_outcome:
            hard_failures.append("entry_quality_step_outcome_mismatch")
    if database_outcome is not None:
        expected_outcome = "success" if validation_report.get("passed") else "failure"
        if database_outcome != expected_outcome:
            hard_failures.append("database_validation_step_outcome_mismatch")

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

    critical_rotation_changes, rotation_diff_malformed = classify_critical_rotation_changes(
        rotation_diff
    )
    if rotation_diff_malformed:
        hard_failures.append("masterfile_rotation_diff_malformed")

    hard_failures = sorted(set(hard_failures))
    review_required = bool(
        (unexpected_warn_count or source_review_keys or critical_rotation_changes)
        and not hard_failures
    )
    return {
        "passed": not hard_failures,
        "review_required": review_required,
        "unexpected_warn_count": unexpected_warn_count,
        "quarantine_count": quarantine_count,
        "failed_error_gates": failed_error_gates,
        "hard_failures": hard_failures,
        "source_review_count": len(source_review_keys),
        "source_review_keys": source_review_keys,
        "critical_rotation_change_count": len(critical_rotation_changes),
        "critical_rotation_changes": critical_rotation_changes,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify masterfile gate failures as manual data review or hard workflow failures."
    )
    parser.add_argument("--entry-quality-gate", type=Path, required=True)
    parser.add_argument("--validation-report", type=Path, required=True)
    parser.add_argument("--masterfile-summary", type=Path)
    parser.add_argument("--rotation-diff", type=Path)
    parser.add_argument("--entry-quality-outcome", choices=("success", "failure"))
    parser.add_argument("--database-outcome", choices=("success", "failure"))
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = classify_gate_results(
        load_json(args.entry_quality_gate),
        load_json(args.validation_report),
        load_json(args.masterfile_summary) if args.masterfile_summary else None,
        load_json(args.rotation_diff) if args.rotation_diff else None,
        args.entry_quality_outcome,
        args.database_outcome,
    )
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as handle:
            handle.write(f"review_required={str(result['review_required']).lower()}\n")
            handle.write(f"unexpected_warn_count={result['unexpected_warn_count']}\n")
            handle.write(f"quarantine_count={result['quarantine_count']}\n")
            handle.write(f"source_review_count={result['source_review_count']}\n")
            handle.write(f"source_review_keys={','.join(result['source_review_keys'])}\n")
            handle.write(
                f"critical_rotation_change_count={result['critical_rotation_change_count']}\n"
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
