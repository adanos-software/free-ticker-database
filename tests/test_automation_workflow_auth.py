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


def test_ci_supports_explicit_dispatch() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "statuses: write" in workflow
    assert "always() && github.event_name == 'workflow_dispatch'" in workflow
    assert 'context="test"' in workflow
    assert 'description="Full dispatched CI: $CI_STATE"' in workflow


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
