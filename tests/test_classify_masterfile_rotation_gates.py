from __future__ import annotations

import json

from scripts.classify_masterfile_rotation_gates import classify_gate_results, main


def entry_gate(*, unexpected: int = 0, quarantine: int = 0) -> dict[str, object]:
    return {
        "passed": unexpected == 0 and quarantine == 0,
        "unexpected_warn_count": unexpected,
        "quarantine_count": quarantine,
    }


def validation_report(*failed_names: str) -> dict[str, object]:
    return {
        "passed": not failed_names,
        "gates": [
            {
                "name": name,
                "severity": "error",
                "passed": False,
            }
            for name in failed_names
        ],
    }


def test_classify_gate_results_routes_only_new_warnings_to_manual_review() -> None:
    result = classify_gate_results(
        entry_gate(unexpected=3),
        validation_report("entry_quality_unexpected_warn_count"),
    )

    assert result["passed"] is True
    assert result["review_required"] is True
    assert result["unexpected_warn_count"] == 3
    assert result["hard_failures"] == []


def test_classify_gate_results_keeps_quarantine_and_unrelated_gates_fatal() -> None:
    result = classify_gate_results(
        entry_gate(unexpected=3, quarantine=1),
        validation_report("entry_quality_unexpected_warn_count", "duplicate_listing_key_count"),
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
    entry_path.write_text(json.dumps(entry_gate(unexpected=2)), encoding="utf-8")
    validation_path.write_text(
        json.dumps(validation_report("entry_quality_unexpected_warn_count")),
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
