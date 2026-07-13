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


def test_ci_supports_explicit_dispatch() -> None:
    workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow


def test_automation_workflows_use_scoped_token_and_dispatch_real_ci() -> None:
    for filename, branch in AUTOMATION_BRANCHES.items():
        workflow = (WORKFLOW_DIR / filename).read_text(encoding="utf-8")

        assert "actions: write" in workflow
        assert "token: ${{ github.token }}" in workflow
        assert "GH_TOKEN: ${{ github.token }}" in workflow
        assert f"gh workflow run ci.yml --ref {branch}" in workflow
        assert "AUTOMATION_PR_TOKEN" not in workflow
