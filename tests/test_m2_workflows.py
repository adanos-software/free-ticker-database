from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_masterfile_rotation_workflow_batches_and_reports_diffs() -> None:
    workflow = (ROOT / ".github" / "workflows" / "masterfile-rotation.yml").read_text(encoding="utf-8")

    assert "--rotation-batch-size 20" in workflow
    assert "scripts/build_masterfile_diff_report.py" in workflow
    assert "scripts/build_masterfile_vanished_delisting_review.py" in workflow
    assert "scripts/apply_nasdaq_us_new_listings.py" in workflow
    assert "--asset-type Stock,ETF" in workflow
    rebuild_step = workflow.split("- name: Rebuild derived exports and reports", 1)[1].split(
        "- name: Validate technical release gates", 1
    )[0]
    validation_step = workflow.split("- name: Validate technical release gates", 1)[1].split(
        "- name: Detect substantive changes", 1
    )[0]
    assert "if:" not in rebuild_step
    assert "continue-on-error: true" in validation_step
    assert "scripts/classify_masterfile_rotation_gates.py" in workflow
    assert "steps.release.outputs.review_required != 'true'" in workflow
    assert "draft: ${{ steps.release.outputs.review_required == 'true' }}" in workflow
    assert 'gh pr ready "${{ steps.cpr.outputs.pull-request-number }}" --undo' in workflow
    assert 'diff_count" = "0"' in workflow
    assert "timestamp-only churn discarded" in workflow
    assert "gh workflow run ci.yml --ref automation/masterfile-rotation" in workflow
    assert "AUTOMATION_PR_TOKEN" not in workflow


def test_release_workflow_builds_release_assets_and_guarded_publish_hooks() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")

    assert "tags:" in workflow
    assert "scripts/build_release_artifacts.py" in workflow
    assert "scripts/build_evidence_archive.py" in workflow
    assert "softprops/action-gh-release@v2" in workflow
    assert "output/evidence_archive/*" not in workflow
    assert "output/evidence_archive/m2-campaign-evidence.zip" in workflow
    assert "output/evidence_archive/evidence-manifest.json" in workflow
    assert "PUBLISH_KAGGLE" in workflow
    assert "PUBLISH_HUGGINGFACE" in workflow
    assert "git rev-parse HEAD^ >/dev/null 2>&1" in workflow
    assert "cp data/listings.csv /tmp/listings-before.csv" in workflow

    validation = workflow.index("python scripts/validate_database.py")
    b3_review = workflow.index("python scripts/build_b3_masterfile_gap_review.py")
    campaigns = workflow.index("python scripts/build_improvement_campaign_report.py")
    acceptance = workflow.index("python scripts/build_release_acceptance_report.py --fail-on-failure")
    release_assets = workflow.index("python scripts/build_release_artifacts.py")

    assert validation < b3_review < campaigns < acceptance < release_assets
    assert "--profile merge" in workflow
    assert "--profile stable" in workflow
    assert "--profile complete" in workflow
    assert re.search(r"--profile merge\s+\\\s+--strict", workflow)
    assert not re.search(r"--profile stable\s+\\\s+--strict", workflow)


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
