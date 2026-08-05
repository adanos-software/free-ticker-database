from __future__ import annotations

import json

import pytest

from scripts.classify_masterfile_rotation_gates import classify_gate_results, main


def entry_gate(
    *,
    unexpected: int = 0,
    quarantine: int = 0,
    unexpected_warns: list[str] | None = None,
    unexpected_warning_subjects: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "passed": unexpected == 0 and quarantine == 0,
        "unexpected_warn_count": unexpected,
        "unexpected_warns": unexpected_warns or [],
        "unexpected_warning_subjects": unexpected_warning_subjects or [],
        "quarantine_count": quarantine,
    }


def validation_report(
    *failed_names: str,
    unexpected_warn_count: object | None = None,
) -> dict[str, object]:
    return {
        "passed": not failed_names,
        "gates": [
            {
                "name": name,
                "severity": "error",
                "passed": False,
                "limit": 0,
                "actual": (
                    unexpected_warn_count
                    if name == "entry_quality_unexpected_warn_count"
                    else 1
                ),
            }
            for name in failed_names
        ],
    }


def test_classify_gate_results_routes_only_new_warnings_to_manual_review() -> None:
    subjects = [subject(f"LSE::T{index}", "official_name_mismatch") for index in range(3)]
    result = classify_gate_results(
        entry_gate(unexpected=3, unexpected_warning_subjects=subjects),
        validation_report("entry_quality_unexpected_warn_count", unexpected_warn_count=3),
    )

    assert result["passed"] is True
    assert result["review_required"] is True
    assert result["unexpected_warn_count"] == 3
    assert result["hard_failures"] == []


def subject(listing_key: str, issue_type: str = "country_isin_mismatch") -> dict[str, str]:
    return {"listing_key": listing_key, "issue_type": issue_type}


def test_classify_gate_results_routes_structurally_correlated_gate_to_review() -> None:
    report = validation_report(
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=1,
            unexpected_warns=["LSE::0I4T"],
            unexpected_warning_subjects=[subject("LSE::0I4T")],
        ),
        report,
    )

    assert result["passed"] is True
    assert result["review_required"] is True
    assert result["hard_failures"] == []


def test_classify_gate_results_keeps_uncorrelated_review_subject_fatal() -> None:
    report = validation_report(
        "country_isin_prefix_mismatch_without_review",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T"), subject("NASDAQ::OTHER")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=1,
            unexpected_warns=["LSE::0I4T"],
            unexpected_warning_subjects=[subject("LSE::0I4T")],
        ),
        report,
    )

    assert result["passed"] is False
    assert result["review_required"] is False
    assert result["hard_failures"] == ["country_isin_prefix_mismatch_without_review"]


def test_classify_gate_results_keeps_issue_type_mismatch_fatal() -> None:
    report = validation_report(
        "country_isin_prefix_mismatch_without_review",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=1,
            unexpected_warns=["LSE::0I4T"],
            unexpected_warning_subjects=[subject("LSE::0I4T", "official_name_mismatch")],
        ),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["country_isin_prefix_mismatch_without_review"]


def test_classify_gate_results_keeps_incomplete_review_subjects_fatal() -> None:
    report = validation_report(
        "country_isin_prefix_mismatch_without_review",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T")],
        review_subjects_complete=False,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=1,
            unexpected_warning_subjects=[subject("LSE::0I4T")],
        ),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["country_isin_prefix_mismatch_without_review"]


def test_classify_gate_results_checks_subjects_beyond_display_limit() -> None:
    warning_subjects = [subject(f"LSE::T{index:02d}") for index in range(51)]
    report = validation_report(
        "country_isin_prefix_mismatch_without_review",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=51,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[*warning_subjects, subject("NASDAQ::OTHER")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=51,
            unexpected_warns=[item["listing_key"] for item in warning_subjects[:50]],
            unexpected_warning_subjects=warning_subjects,
        ),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["country_isin_prefix_mismatch_without_review"]


def test_classify_gate_results_rejects_subjects_when_warning_count_is_zero() -> None:
    report = validation_report("future_identity_consistency_gate")
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(
            unexpected=0,
            unexpected_warning_subjects=[subject("LSE::0I4T")],
        ),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == [
        "entry_quality_warning_subject_report_inconsistent",
        "future_identity_consistency_gate",
    ]


def test_classify_gate_results_rejects_more_subject_keys_than_warnings() -> None:
    report = validation_report(
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    subjects = [subject("LSE::0I4T"), subject("NASDAQ::OTHER")]
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=subjects,
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(unexpected=1, unexpected_warning_subjects=subjects),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == [
        "entry_quality_warning_subject_report_inconsistent",
        "future_identity_consistency_gate",
    ]


@pytest.mark.parametrize("validation_count", [1, True, "2", None])
def test_classify_gate_results_rejects_mismatched_validation_warning_count(
    validation_count: object,
) -> None:
    subjects = [subject("LSE::ONE"), subject("LSE::TWO")]

    result = classify_gate_results(
        entry_gate(unexpected=2, unexpected_warning_subjects=subjects),
        validation_report(
            "entry_quality_unexpected_warn_count",
            unexpected_warn_count=validation_count,
        ),
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["entry_quality_validation_report_mismatch"]


def test_classify_gate_results_rejects_duplicate_validation_warning_gates() -> None:
    subjects = [subject("LSE::0I4T")]
    report = validation_report(
        "entry_quality_unexpected_warn_count",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )

    result = classify_gate_results(
        entry_gate(unexpected=1, unexpected_warning_subjects=subjects),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == [
        "duplicate_validation_gate_names",
        "entry_quality_validation_report_mismatch",
    ]


def test_classify_gate_results_rejects_duplicate_correlated_gate_names() -> None:
    subjects = [subject("LSE::0I4T")]
    report = validation_report(
        "future_identity_consistency_gate",
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        actual=1,
        review_policy="entry_quality_warning",
        review_subjects=subjects,
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(unexpected=1, unexpected_warning_subjects=subjects),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == [
        "duplicate_validation_gate_names",
        "future_identity_consistency_gate",
    ]


@pytest.mark.parametrize("invalid_actual", [None, True, "1", -1, 0])
def test_classify_gate_results_rejects_invalid_correlated_gate_count(
    invalid_actual: object,
) -> None:
    subjects = [subject("LSE::0I4T")]
    report = validation_report(
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        actual=invalid_actual,
        review_policy="entry_quality_warning",
        review_subjects=subjects,
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(unexpected=1, unexpected_warning_subjects=subjects),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["future_identity_consistency_gate"]


def test_classify_gate_results_rejects_correlated_count_below_subject_count() -> None:
    subjects = [subject("LSE::ONE"), subject("LSE::TWO")]
    report = validation_report(
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=2,
    )
    report["gates"][0].update(
        actual=1,
        review_policy="entry_quality_warning",
        review_subjects=subjects,
        review_subjects_complete=True,
    )

    result = classify_gate_results(
        entry_gate(unexpected=2, unexpected_warning_subjects=subjects),
        report,
    )

    assert result["passed"] is False
    assert result["hard_failures"] == ["future_identity_consistency_gate"]


@pytest.mark.parametrize("subject_state", ["missing", "empty", "short"])
def test_classify_gate_results_rejects_incomplete_warning_subjects(subject_state: str) -> None:
    subjects = [] if subject_state != "short" else [subject("LSE::ONLY")]
    gate = entry_gate(unexpected=2, unexpected_warning_subjects=subjects)
    if subject_state == "missing":
        gate.pop("unexpected_warning_subjects")

    result = classify_gate_results(
        gate,
        validation_report("entry_quality_unexpected_warn_count", unexpected_warn_count=2),
    )

    assert result["passed"] is False
    assert result["hard_failures"] == [
        "entry_quality_warning_subject_report_inconsistent"
    ]


@pytest.mark.parametrize("invalid_count", [-1, True, "1", None])
def test_classify_gate_results_rejects_invalid_warning_counts(invalid_count: object) -> None:
    gate = entry_gate(
        unexpected=1,
        unexpected_warning_subjects=[subject("LSE::0I4T")],
    )
    gate["unexpected_warn_count"] = invalid_count
    report = validation_report(
        "future_identity_consistency_gate",
        "entry_quality_unexpected_warn_count",
        unexpected_warn_count=1,
    )
    report["gates"][0].update(
        review_policy="entry_quality_warning",
        review_subjects=[subject("LSE::0I4T")],
        review_subjects_complete=True,
    )

    result = classify_gate_results(gate, report)

    assert result["passed"] is False
    assert "entry_quality_count_report_inconsistent" in result["hard_failures"]
    assert "future_identity_consistency_gate" in result["hard_failures"]


def test_classify_gate_results_rejects_invalid_quarantine_count() -> None:
    gate = entry_gate()
    gate["quarantine_count"] = -1

    result = classify_gate_results(gate, validation_report())

    assert result["passed"] is False
    assert result["hard_failures"] == ["entry_quality_count_report_inconsistent"]


def test_classify_gate_results_keeps_quarantine_and_unrelated_gates_fatal() -> None:
    subjects = [subject(f"LSE::T{index}", "official_name_mismatch") for index in range(3)]
    result = classify_gate_results(
        entry_gate(
            unexpected=3,
            quarantine=1,
            unexpected_warning_subjects=subjects,
        ),
        validation_report(
            "entry_quality_unexpected_warn_count",
            "duplicate_listing_key_count",
            unexpected_warn_count=3,
        ),
    )

    assert result["passed"] is False
    assert result["review_required"] is False
    assert result["hard_failures"] == ["duplicate_listing_key_count", "entry_quality_quarantine"]


def test_classify_gate_results_routes_failed_selected_sources_to_manual_review() -> None:
    result = classify_gate_results(
        entry_gate(),
        validation_report(),
        {
            "source_details": {
                "fresh_directory": {
                    "official": True,
                    "reference_scope": "exchange_directory",
                },
                "failed_directory": {
                    "official": True,
                    "reference_scope": "exchange_directory",
                },
                "cached_subset": {
                    "official": True,
                    "reference_scope": "listed_companies_subset",
                },
            },
            "last_refresh": {
                "selected_source_keys": [
                    "fresh_directory",
                    "failed_directory",
                    "cached_subset",
                ],
                "source_modes": {
                    "fresh_directory": "network",
                    "failed_directory": "unavailable",
                    "cached_subset": "cache",
                },
            },
        },
    )

    assert result["passed"] is True
    assert result["review_required"] is True
    assert result["source_review_keys"] == ["failed_directory"]


def test_classify_gate_cli_writes_github_outputs_for_review(tmp_path) -> None:
    entry_path = tmp_path / "entry.json"
    validation_path = tmp_path / "validation.json"
    output_path = tmp_path / "github-output"
    entry_path.write_text(
        json.dumps(
            entry_gate(
                unexpected=2,
                unexpected_warning_subjects=[
                    subject("LSE::ONE", "official_name_mismatch"),
                    subject("LSE::TWO", "official_name_mismatch"),
                ],
            )
        ),
        encoding="utf-8",
    )
    validation_path.write_text(
        json.dumps(
            validation_report("entry_quality_unexpected_warn_count", unexpected_warn_count=2)
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--entry-quality-gate",
            str(entry_path),
            "--validation-report",
            str(validation_path),
            "--github-output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "review_required=true",
        "unexpected_warn_count=2",
        "quarantine_count=0",
        "source_review_count=0",
        "source_review_keys=",
    ]
