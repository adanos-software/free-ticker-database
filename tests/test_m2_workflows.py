from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_masterfile_rotation_workflow_batches_and_reports_diffs() -> None:
    workflow = (ROOT / ".github" / "workflows" / "masterfile-rotation.yml").read_text(encoding="utf-8")

    assert "--rotation-batch-size 20" in workflow
    assert "scripts/build_masterfile_diff_report.py" in workflow
    assert "scripts/apply_nasdaq_us_new_listings.py" in workflow
    assert "--asset-type Stock,ETF" in workflow
    assert 'diff_count" = "0"' in workflow
    assert "timestamp-only churn discarded" in workflow
    assert "AUTOMATION_PR_TOKEN" in workflow


def test_release_workflow_builds_release_assets_and_guarded_publish_hooks() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")

    assert "tags:" in workflow
    assert "scripts/build_release_artifacts.py" in workflow
    assert "scripts/build_evidence_archive.py" in workflow
    assert "softprops/action-gh-release@v2" in workflow
    assert "PUBLISH_KAGGLE" in workflow
    assert "PUBLISH_HUGGINGFACE" in workflow


def test_symbol_change_workflow_skips_timestamp_only_apply_report_churn() -> None:
    workflow = (ROOT / ".github" / "workflows" / "symbol-changes.yml").read_text(encoding="utf-8")

    assert "data/corporate_actions/symbol_changes.csv" in workflow
    assert "steps.apply.outputs.symbol_changes_applied" in workflow
    assert "timestamp-only churn discarded" in workflow


def test_pipeline_manifest_keeps_active_workflows_out_of_archive() -> None:
    manifest = json.loads((ROOT / "scripts" / "pipeline_manifest.json").read_text(encoding="utf-8"))

    living = set(manifest["living_pipeline"])
    assert ".github/workflows/ci.yml" in living
    assert "scripts/lib/dataio.py" in living
    assert "scripts/archive/backfill_yahoo_generic_etf_names.py" in manifest["archive_after_bundle"]
    assert "data/deepseek_review_jobs/*" in manifest["archive_after_bundle"]
    assert not any(path.startswith("scripts/archive/") for path in living)
    for workflow in (ROOT / ".github" / "workflows").glob("*.yml"):
        assert "scripts/archive/" not in workflow.read_text(encoding="utf-8")
