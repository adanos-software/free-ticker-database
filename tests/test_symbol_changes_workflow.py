from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "symbol-changes.yml"


def test_symbol_changes_workflow_avoids_duplicate_github_auth_headers():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "uses: actions/checkout@v6" in workflow
    assert "persist-credentials: false" in workflow


def test_symbol_changes_workflow_uses_node24_create_pull_request_action():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "uses: peter-evans/create-pull-request@v8.1.1" in workflow
    assert "uses: peter-evans/create-pull-request@v6" not in workflow
