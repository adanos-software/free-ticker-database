from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github" / "workflows"
AUTOMATION_BRANCHES = {
    "delisting-report.yml": "automation/delisting-report",
    "freshness.yml": "automation/drift-report",
    "isin-validation.yml": "automation/isin-validation",
    "masterfile-rotation.yml": "automation/masterfile-rotation",
    "nasdaq-us-new-listings.yml": "automation/nasdaq-us-new-listings",
    "symbol-changes.yml": "automation/symbol-changes",
}
REPORT_REBUILD_WORKFLOWS = (
    "delisting-report.yml",
    "masterfile-rotation.yml",
    "nasdaq-us-new-listings.yml",
    "release.yml",
    "symbol-changes.yml",
)


def test_ci_supports_dispatch_and_aggregate_branch_protection_status() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "statuses: write" in workflow
    assert "\n  compatibility:\n" in workflow
    assert "\n  canonical-v4:\n" in workflow
    assert "\n  test:\n" in workflow
    assert "name: test" in workflow
    assert "if: ${{ always() }}" in workflow
    assert "needs:\n      - compatibility\n      - canonical-v4" in workflow
    assert 'TARGET_SHA: ${{ github.event.pull_request.head.sha || github.sha }}' in workflow
    assert "COMPATIBILITY_RESULT: ${{ needs.compatibility.result }}" in workflow
    assert "CANONICAL_RESULT: ${{ needs.canonical-v4.result }}" in workflow
    assert 'context="test"' in workflow
    assert 'description="Aggregate PR CI: $CI_STATE"' in workflow


def test_ci_rebuilds_entry_quality_from_current_data_before_the_gate() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    dataset_rebuild = workflow.index("python scripts/rebuild_canonical.py")
    entry_quality_rebuild = workflow.index("python scripts/build_entry_quality_report.py")
    test_suite = workflow.index("python -m pytest tests/ -q")
    entry_quality_gate = workflow.index("python scripts/check_entry_quality_gate.py")

    assert dataset_rebuild < entry_quality_rebuild < test_suite < entry_quality_gate
    assert "--csv-out /tmp/entry-quality.csv" in workflow
    assert "--entry-quality-csv /tmp/entry-quality.csv" in workflow
    assert "--no-json-out" in workflow


def test_ci_rebuilds_coverage_before_exchange_scope_audit_and_gate() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    coverage_rebuild = workflow.index("python scripts/build_coverage_report.py")
    audit_rebuild = workflow.index("python scripts/build_exchange_source_audit.py")
    scope_gate = workflow.index("python scripts/check_exchange_scope_decisions.py")

    assert coverage_rebuild < audit_rebuild < scope_gate


def test_automation_workflows_use_scoped_token_and_dispatch_real_ci() -> None:
    for filename, branch in AUTOMATION_BRANCHES.items():
        workflow = (WORKFLOW_DIR / filename).read_text(encoding="utf-8")

        assert "actions: write" in workflow
        assert "token: ${{ github.token }}" in workflow
        assert "GH_TOKEN: ${{ github.token }}" in workflow
        assert f"gh workflow run ci.yml --ref {branch}" in workflow
        assert "AUTOMATION_PR_TOKEN" not in workflow


def test_masterfile_html_parser_dependencies_are_declared() -> None:
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "html5lib>=" in requirements
    assert "beautifulsoup4>=" in requirements


def test_report_rebuilds_follow_their_data_dependencies() -> None:
    for filename in REPORT_REBUILD_WORKFLOWS:
        workflow = (WORKFLOW_DIR / filename).read_text(encoding="utf-8")

        entry_quality = workflow.index("python scripts/build_entry_quality_report.py")
        source_gaps = workflow.index("python scripts/build_source_gap_classification.py")
        source_decisions = workflow.index("python scripts/build_source_of_truth_decisions.py")
        completion_backlog = workflow.index("python scripts/build_completion_backlog.py")

        assert entry_quality < source_gaps < source_decisions < completion_backlog


def test_operational_rebuilds_use_canonical_entrypoint_and_safe_merge() -> None:
    for filename in REPORT_REBUILD_WORKFLOWS:
        workflow = (WORKFLOW_DIR / filename).read_text(encoding="utf-8")
        assert "python scripts/rebuild_canonical.py" in workflow
        assert "python scripts/check_safe_merge.py" in workflow
        if filename != "release.yml":
            assert "python scripts/rebuild_dataset.py" not in workflow
            assert "python scripts/rebuild_canonical.py --apply-identity-fixes" in workflow
