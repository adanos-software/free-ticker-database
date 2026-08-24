from __future__ import annotations

from pathlib import Path

from scripts.check_workflow_source_policy import check_repository


ROOT = Path(__file__).resolve().parents[1]


def write(path: Path, text: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_rejects_bootstrap_and_compressed_patch_payload(tmp_path: Path) -> None:
    write(tmp_path / ".bootstrap" / "patch.00", "payload")
    write(tmp_path / "hidden.patch.xz.b64", "payload")
    rules = {item.rule for item in check_repository(tmp_path)}
    assert rules == {"no_bootstrap_source_transport", "no_compressed_source_payloads"}


def test_rejects_self_pushing_workflow(tmp_path: Path) -> None:
    write(tmp_path / ".github/workflows/ci.yml", "jobs:\n  test:\n    steps:\n      - run: git push origin HEAD\n")
    assert [item.rule for item in check_repository(tmp_path)] == ["no_workflow_self_push"]


def test_rejects_unreviewed_contents_write_workflow(tmp_path: Path) -> None:
    write(tmp_path / ".github/workflows/new.yml", "permissions:\n  contents: write\n")
    assert [item.rule for item in check_repository(tmp_path)] == ["contents_write_allowlist"]


def test_allows_reviewed_existing_maintenance_workflow(tmp_path: Path) -> None:
    write(tmp_path / ".github/workflows/release.yml", "permissions:\n  contents: write\n")
    assert check_repository(tmp_path) == []


def test_historical_safe_merge_workflows_preserve_matching_reference_baseline() -> None:
    for workflow_name in ("ci.yml", "release.yml"):
        workflow = (ROOT / ".github" / "workflows" / workflow_name).read_text(
            encoding="utf-8"
        )
        assert 'data/masterfiles/reference.csv" > /tmp/reference-before-safe-merge.csv' in workflow
        assert "cp data/masterfiles/reference.csv /tmp/reference-before-safe-merge.csv" in workflow
        assert "test -s /tmp/reference-before-safe-merge.csv" in workflow
        assert "--previous-official-reference /tmp/reference-before-safe-merge.csv" in workflow
