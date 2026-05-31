import json

from scripts.build_pr_review_summary import main, render_markdown


def test_render_pr_review_summary_uses_generated_report_values() -> None:
    markdown = render_markdown(
        coverage={
            "global": {
                "tickers": 10,
                "listing_keys": 12,
                "official_masterfile_symbols": 20,
                "official_masterfile_matches": 8,
                "official_masterfile_collisions": 3,
                "official_masterfile_missing": 9,
            },
            "source_freshness_summary": {
                "freshness_status_totals": {"fresh": 4, "old": 2},
                "old_official_exchange_directory_count": 1,
                "top_old_official_exchange_directories": [{"key": "lse_price_explorer"}],
            },
        },
        campaigns={
            "next_review_batches": [
                {
                    "campaign_key": "masterfile_collisions",
                    "artifact_rows": 3,
                    "status": "review_queue_only_no_data_apply",
                }
            ]
        },
        entry_quality_gate={"warn_count": 5, "unexpected_warn_count": 0, "quarantine_count": 0},
        release_acceptance={"passed": True, "summary": {"criteria": 41, "passed_criteria": 41}},
        source_gap_classification={"summary": {"rows": 7}},
        validation={"passed": True, "summary": {"failed_error_gates": 0}},
        generated_at="2026-05-25T12:00:00Z",
    )

    assert "Generated: `2026-05-25T12:00:00Z`" in markdown
    assert "| Source gaps | 7 |" in markdown
    assert "| Entry-quality warnings | 5 |" in markdown
    assert "- `lse_price_explorer`" in markdown
    assert "| Masterfile Collisions | 3 | review_queue_only_no_data_apply |" in markdown
    assert "`41/41`" in markdown
    assert "`unexpected_warn_count=0`" in markdown
    assert "`quarantine_count=0`" in markdown
    assert "1246 passed" not in markdown
    assert "run before release; not captured by generated report JSON" in markdown


def test_build_pr_review_summary_writes_markdown(tmp_path) -> None:
    coverage_json = tmp_path / "coverage.json"
    campaigns_json = tmp_path / "campaigns.json"
    entry_gate_json = tmp_path / "entry_gate.json"
    release_json = tmp_path / "release.json"
    source_gap_json = tmp_path / "source_gap.json"
    validation_json = tmp_path / "validation.json"
    md_out = tmp_path / "summary.md"

    coverage_json.write_text(
        json.dumps(
            {
                "global": {"tickers": 1},
                "source_freshness_summary": {
                    "freshness_status_totals": {"fresh": 1, "old": 0},
                    "top_old_official_exchange_directories": [],
                },
            }
        ),
        encoding="utf-8",
    )
    campaigns_json.write_text(json.dumps({"next_review_batches": []}), encoding="utf-8")
    entry_gate_json.write_text(json.dumps({"warn_count": 0, "unexpected_warn_count": 0, "quarantine_count": 0}), encoding="utf-8")
    release_json.write_text(json.dumps({"passed": True, "summary": {"criteria": 1, "passed_criteria": 1}}), encoding="utf-8")
    source_gap_json.write_text(json.dumps({"summary": {"rows": 0}}), encoding="utf-8")
    validation_json.write_text(json.dumps({"passed": True, "summary": {"failed_error_gates": 0}}), encoding="utf-8")

    main(
        [
            "--coverage-json",
            str(coverage_json),
            "--improvement-campaigns-json",
            str(campaigns_json),
            "--entry-quality-gate-json",
            str(entry_gate_json),
            "--release-acceptance-json",
            str(release_json),
            "--source-gap-classification-json",
            str(source_gap_json),
            "--validation-json",
            str(validation_json),
            "--md-out",
            str(md_out),
        ]
    )

    assert md_out.exists()
    assert "# PR Review Summary" in md_out.read_text(encoding="utf-8")
