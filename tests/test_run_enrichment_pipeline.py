from __future__ import annotations

from scripts.run_enrichment_pipeline import PipelineOptions, build_pipeline_commands


def test_pipeline_default_stages_are_safe_and_ordered():
    stages = build_pipeline_commands(PipelineOptions(python="python3"))
    names = [stage.name for stage in stages]

    assert names[:2] == ["fetch_masterfiles", "completion_backlog_before"]
    assert "same_isin_sector_peer_backfill" in names
    assert "financedatabase_sector_backfill" in names
    assert names[-4:] == ["build_listing_history", "build_coverage_report", "audit_dataset", "completion_backlog_after"]
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
    assert any(stage.name == "eodhd_reviewed_isin_backfill" and "--apply" in stage.command for stage in stages)
    assert any(stage.name == "same_isin_sector_peer_backfill" and stage.mutates_data for stage in stages)


def test_pipeline_only_stage_filters_commands():
    stages = build_pipeline_commands(
        PipelineOptions(
            python="python3",
            only_stages=("completion_backlog_after",),
        )
    )

    assert [stage.name for stage in stages] == ["completion_backlog_after"]
