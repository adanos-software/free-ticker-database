from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


EXPECTED_REVIEW_GATE = "entry_quality_unexpected_warn_count"
ENTRY_QUALITY_REVIEW_POLICY = "entry_quality_warning"
FETCH_ISSUE_MODES = {"unavailable", "cache"}
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
                "after": {
                    field: str(changes[field]["after"])
                    for field in fields
                },
            }
        )
    return critical_changes, malformed


def _listing_lookup(listings: list[dict[str, str]] | None) -> dict[str, dict[str, str]] | None:
    if listings is None:
        return None
    by_key: dict[str, dict[str, str]] = {}
    for row in listings:
        key = str(row.get("listing_key") or "").strip()
        if not key:
            exchange = str(row.get("exchange") or "").strip()
            ticker = str(row.get("ticker") or "").strip()
            if exchange and ticker:
                key = f"{exchange}::{ticker}"
        if key:
            by_key[key] = row
    return by_key


def _authorized_metadata_values(
    metadata_updates: list[dict[str, str]] | None,
) -> set[tuple[str, str, str, str]]:
    authorized: set[tuple[str, str, str, str]] = set()
    for row in metadata_updates or []:
        if str(row.get("decision") or "").strip() != "update":
            continue
        ticker = str(row.get("ticker") or "").strip()
        exchange = str(row.get("exchange") or "").strip()
        field = str(row.get("field") or "").strip()
        value = str(row.get("proposed_value") or "").strip()
        if not ticker or not exchange or not field:
            continue
        authorized.add((ticker, exchange, field, value))
        if field in {"stock_sector", "etf_category"}:
            authorized.add((ticker, exchange, "sector", value))
    return authorized


def _listing_field_value(listing: dict[str, str], field: str) -> str | None:
    if field == "isin":
        return str(listing.get("isin") or "").strip()
    if field == "asset_type":
        return str(listing.get("asset_type") or "").strip()
    if field == "sector":
        asset_type = str(listing.get("asset_type") or "").strip()
        if asset_type == "ETF":
            return str(listing.get("etf_category") or "").strip()
        return str(listing.get("stock_sector") or "").strip()
    return None


def identity_review_changes(
    critical_changes: list[dict[str, Any]],
    listings: list[dict[str, str]] | None,
    metadata_updates: list[dict[str, str]] | None,
) -> list[dict[str, Any]]:
    """Keep only official critical diffs that would recode an existing listing without evidence.

    Unknown listing snapshot stays fail-closed (all critical diffs remain identity review).
    Official-directory rows with no listing are ops notes, not identity review.
    """
    listing_by_key = _listing_lookup(listings)
    if listing_by_key is None:
        return list(critical_changes)
    authorized = _authorized_metadata_values(metadata_updates)
    kept: list[dict[str, Any]] = []
    for change in critical_changes:
        exchange = str(change.get("exchange") or "")
        ticker = str(change.get("ticker") or "")
        listing = listing_by_key.get(f"{exchange}::{ticker}")
        if listing is None:
            continue
        remaining: list[str] = []
        after_values = change.get("after") if isinstance(change.get("after"), dict) else {}
        for field in change.get("fields") or []:
            proposed = str(after_values.get(field) or "").strip()
            current = _listing_field_value(listing, field)
            if current is None:
                remaining.append(field)
                continue
            if proposed and current == proposed:
                continue
            metadata_field = "stock_sector" if field == "sector" and listing.get("asset_type") != "ETF" else field
            if listing.get("asset_type") == "ETF" and field == "sector":
                metadata_field = "etf_category"
            if proposed and (
                (ticker, exchange, field, proposed) in authorized
                or (ticker, exchange, metadata_field, proposed) in authorized
            ):
                continue
            remaining.append(field)
        if remaining:
            kept.append({**change, "fields": remaining})
    return kept


def classify_safe_merge(
    safe_merge: dict[str, Any] | None,
    safe_merge_outcome: str | None,
) -> dict[str, Any]:
    if safe_merge is None and safe_merge_outcome is None:
        return {
            "hard_failures": [],
            "unevidenced_field_change_count": 0,
        }
    if not isinstance(safe_merge, dict):
        return {
            "hard_failures": ["safe_merge_report_malformed"],
            "unevidenced_field_change_count": 0,
        }
    summary = safe_merge.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    status = safe_merge.get("status")
    unevidenced_removed = parse_nonnegative_int(summary.get("unevidenced_removed_rows"))
    unevidenced_fields = parse_nonnegative_int(summary.get("unevidenced_critical_field_changes"))
    hard_failures: list[str] = []
    if status not in {"pass", "fail"} or unevidenced_removed is None or unevidenced_fields is None:
        hard_failures.append("safe_merge_report_malformed")
        return {
            "hard_failures": hard_failures,
            "unevidenced_field_change_count": 0,
        }
    expected_outcome = "success" if status == "pass" else "failure"
    if safe_merge_outcome is not None and safe_merge_outcome != expected_outcome:
        hard_failures.append("safe_merge_step_outcome_mismatch")
    if unevidenced_removed:
        hard_failures.append("unevidenced_listing_removals")
    if status == "fail" and not unevidenced_removed and not unevidenced_fields:
        hard_failures.append("unclassified_safe_merge_failure")
    return {
        "hard_failures": hard_failures,
        "unevidenced_field_change_count": 0 if unevidenced_removed else unevidenced_fields,
    }


def classify_gate_results(
    entry_quality_gate: dict[str, Any],
    validation_report: dict[str, Any],
    masterfile_summary: dict[str, Any] | None = None,
    rotation_diff: dict[str, Any] | None = None,
    entry_quality_outcome: str | None = None,
    database_outcome: str | None = None,
    safe_merge: dict[str, Any] | None = None,
    safe_merge_outcome: str | None = None,
    listings: list[dict[str, str]] | None = None,
    metadata_updates: list[dict[str, str]] | None = None,
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
    # Fetch misses preserve last committed rows; they are ops notes, not identity review.
    fetch_issue_keys = sorted(
        {
            source_key
            for source_key in selected_source_keys
            if refresh_modes.get(source_key) in FETCH_ISSUE_MODES
            or (
                source_details.get(source_key, {}).get("official")
                and source_details.get(source_key, {}).get("reference_scope")
                == "exchange_directory"
                and refresh_modes.get(source_key) not in {"network", None, ""}
            )
        }
    )
    source_review_keys: list[str] = []

    critical_rotation_changes, rotation_diff_malformed = classify_critical_rotation_changes(
        rotation_diff
    )
    if rotation_diff_malformed:
        hard_failures.append("masterfile_rotation_diff_malformed")
    identity_changes = identity_review_changes(
        critical_rotation_changes, listings, metadata_updates
    )
    safe_merge_classification = classify_safe_merge(safe_merge, safe_merge_outcome)
    hard_failures.extend(safe_merge_classification["hard_failures"])
    unevidenced_field_change_count = safe_merge_classification["unevidenced_field_change_count"]

    hard_failures = sorted(set(hard_failures))
    review_required = bool(
        (
            unexpected_warn_count
            or identity_changes
            or unevidenced_field_change_count
        )
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
        "fetch_issue_count": len(fetch_issue_keys),
        "fetch_issue_keys": fetch_issue_keys,
        "critical_rotation_change_count": len(critical_rotation_changes),
        "critical_rotation_changes": critical_rotation_changes,
        "identity_review_change_count": len(identity_changes),
        "identity_review_changes": identity_changes,
        "unevidenced_listing_field_change_count": unevidenced_field_change_count,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify masterfile gate failures as manual data review or hard workflow failures."
    )
    parser.add_argument("--entry-quality-gate", type=Path, required=True)
    parser.add_argument("--validation-report", type=Path, required=True)
    parser.add_argument("--masterfile-summary", type=Path)
    parser.add_argument("--rotation-diff", type=Path)
    parser.add_argument("--safe-merge", type=Path)
    parser.add_argument("--listings", type=Path)
    parser.add_argument("--metadata-updates", type=Path)
    parser.add_argument("--entry-quality-outcome", choices=("success", "failure"))
    parser.add_argument("--database-outcome", choices=("success", "failure"))
    parser.add_argument("--safe-merge-outcome", choices=("success", "failure"))
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def _load_csv(path: Path | None) -> list[dict[str, str]] | None:
    if path is None:
        return None
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = classify_gate_results(
        load_json(args.entry_quality_gate),
        load_json(args.validation_report),
        load_json(args.masterfile_summary) if args.masterfile_summary else None,
        load_json(args.rotation_diff) if args.rotation_diff else None,
        args.entry_quality_outcome,
        args.database_outcome,
        load_json(args.safe_merge) if args.safe_merge else None,
        args.safe_merge_outcome,
        listings=_load_csv(args.listings),
        metadata_updates=_load_csv(args.metadata_updates),
    )
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as handle:
            handle.write(f"review_required={str(result['review_required']).lower()}\n")
            handle.write(f"unexpected_warn_count={result['unexpected_warn_count']}\n")
            handle.write(f"quarantine_count={result['quarantine_count']}\n")
            handle.write(f"source_review_count={result['source_review_count']}\n")
            handle.write(f"source_review_keys={','.join(result['source_review_keys'])}\n")
            handle.write(f"fetch_issue_count={result['fetch_issue_count']}\n")
            handle.write(f"fetch_issue_keys={','.join(result['fetch_issue_keys'])}\n")
            handle.write(
                f"critical_rotation_change_count={result['critical_rotation_change_count']}\n"
            )
            handle.write(
                f"identity_review_change_count={result['identity_review_change_count']}\n"
            )
            handle.write(
                "unevidenced_listing_field_change_count="
                f"{result['unevidenced_listing_field_change_count']}\n"
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
