from __future__ import annotations

from scripts.run_enrichment_pipeline import PipelineOptions, build_pipeline_commands


def test_pipeline_default_stages_are_safe_and_ordered():
    stages = build_pipeline_commands(PipelineOptions(python="python3"))
    names = [stage.name for stage in stages]

    assert names[:3] == ["fetch_masterfiles", "fetch_symbol_changes", "completion_backlog_before"]
    assert "same_isin_sector_peer_backfill" in names
    assert "financedatabase_sector_backfill" in names
    assert names[-11:] == [
        "build_entry_quality_report",
        "check_entry_quality_gate",
        "build_ohlcv_plausibility_report",
        "audit_dataset",
        "completion_backlog_after",
        "build_source_gap_classification",
        "build_source_of_truth_decisions",
        "validate_database",
        "build_improvement_campaign_report",
        "build_release_acceptance_report",
        "build_pr_review_summary",
    ]
    assert names[names.index("build_entry_quality_report") + 1] == "check_entry_quality_gate"
    assert names[names.index("completion_backlog_after") + 1] == "build_source_gap_classification"
    assert names[names.index("build_source_gap_classification") + 1] == "build_source_of_truth_decisions"
    assert names[names.index("build_source_of_truth_decisions") + 1] == "validate_database"
    assert names[names.index("validate_database") + 1] == "build_improvement_campaign_report"
    assert names[names.index("build_improvement_campaign_report") + 1] == "build_release_acceptance_report"
    assert names[names.index("build_release_acceptance_report") + 1] == "build_pr_review_summary"
    assert names[names.index("build_coverage_report") + 1] == "build_source_inventory"
    assert "eodhd_reviewed_isin_backfill" not in names
    assert all("--apply" not in stage.command for stage in stages)


def test_pipeline_can_include_secondary_network_and_apply_flag():
    stages = build_pipeline_commands(
        PipelineOptions(
            python="python3",
            include_fetch=False,
            include_secondary_network=True,
            apply_reviewed_backfills=True,
        )
    )
    names = [stage.name for stage in stages]

    assert "fetch_masterfiles" not in names
    assert "eodhd_reviewed_isin_backfill" in names
    assert "yahoo_reviewed_etf_isin_backfill" in names
    assert "financialdata_symbol_match" in names
    assert names[names.index("financialdata_symbol_match") + 1] == "financialdata_official_isin_supplements"
    assert names[names.index("financialdata_official_isin_supplements") + 1] == "set_sec_reviewed_isin_backfill"
    assert any(stage.name == "eodhd_reviewed_isin_backfill" and "--apply" in stage.command for stage in stages)
    assert any(stage.name == "set_sec_reviewed_isin_backfill" and "--apply" in stage.command for stage in stages)
    assert any(stage.name == "same_isin_sector_peer_backfill" and stage.mutates_data for stage in stages)


def test_pipeline_only_stage_filters_commands():
    stages = build_pipeline_commands(
        PipelineOptions(
            python="python3",
            only_stages=(
                "check_entry_quality_gate",
                "completion_backlog_after",
                "build_source_of_truth_decisions",
                "validate_database",
                "build_pr_review_summary",
            ),
        )
    )

    assert [stage.name for stage in stages] == [
        "check_entry_quality_gate",
        "completion_backlog_after",
        "build_source_of_truth_decisions",
        "validate_database",
        "build_pr_review_summary",
    ]
