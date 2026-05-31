from pathlib import Path

from scripts.build_improvement_campaign_report import (
    ARTIFACT_NAMES,
    acceptance_matrix,
    artifact_entry,
    artifact_context_for,
    blocker_context_for,
    before_after_context_for,
    build_campaigns,
    campaign_context_for,
    campaign_review_policy,
    closure_blockers,
    closure_context_for,
    closure_readiness_summary,
    command_metadata_for_blocker,
    command_mutation_risk_for,
    command_readiness_context_for,
    command_safety_context_for,
    command_safety_summary_context_for,
    command_script_paths,
    delta_evidence,
    delta_review_context_for,
    evidence_command_for_blocker,
    execution_summary_context_for,
    execution_plan_context_for,
    execution_ranking_context_for,
    execution_ranking_reason,
    freshness_age_bucket,
    freshness_snapshot,
    freshness_snapshot_age_bucket_totals,
    next_review_batches,
    next_review_execution_plan,
    next_review_execution_summary,
    next_review_command_safety_summary,
    next_review_workload_context,
    next_review_workload_summary,
    next_review_recommended_source,
    next_review_source_gate,
    render_markdown,
    review_required_command_context_for,
    review_required_preflight_checks,
    review_required_preflight_context_for,
    risky_command_scripts,
    row_count,
    scoped_exchange_delta_totals,
    summary_context_for,
    top_counts,
    top_freshness_snapshot_ages,
)


def test_row_count_reads_common_report_shapes() -> None:
    assert row_count({"summary": {"review_rows": 12}}) == 12
    assert row_count({"_meta": {"rows": 34}}) == 34
    assert row_count({"rows": [{"a": 1}, {"a": 2}]}) == 2
    assert row_count({"review_items": [{"a": 1}, {"a": 2}, {"a": 3}]}) == 3
    assert row_count({}) == 0


def test_campaign_source_artifacts_are_declared_for_meta_traceability() -> None:
    assert "coverage_report" in ARTIFACT_NAMES
    assert "improvement_deltas" in ARTIFACT_NAMES
    assert len(ARTIFACT_NAMES) == len(set(ARTIFACT_NAMES))


def test_top_counts_sorts_numeric_values_and_ignores_non_numeric() -> None:
    assert top_counts({"b": "2", "a": 3, "ignored": "x"}, limit=2) == {"a": 3, "b": 2}


def test_artifact_entry_extracts_generated_at_and_rows() -> None:
    entry = artifact_entry(
        Path("/tmp/report.json"),
        {"_meta": {"generated_at": "2026-05-24T00:00:00Z"}, "summary": {"rows": 5}},
    )

    assert entry["generated_at"] == "2026-05-24T00:00:00Z"
    assert entry["rows"] == 5


def test_freshness_snapshot_exposes_dataset_source_and_symbol_change_ages() -> None:
    assert freshness_age_bucket(24) == "age_0_48h"
    assert freshness_age_bucket(200) == "age_168_336h"

    snapshot = freshness_snapshot(
        {
            "global": {"tickers": 10, "official_masterfile_symbols": 20, "isin_coverage": 8},
            "freshness": {
                "tickers_built_at": "2026-05-24T00:00:00Z",
                "tickers_age_hours": 24.4,
                "masterfiles_generated_at": "2026-05-20T00:00:00Z",
                "masterfiles_age_hours": 120,
                "identifiers_generated_at": "2026-05-24T01:00:00Z",
                "identifiers_age_hours": 23,
                "entry_quality_generated_at": "2026-05-24T02:00:00Z",
                "entry_quality_age_hours": 22,
                "entry_quality_rows": 10,
                "source_gap_classification_generated_at": "2026-05-24T03:00:00Z",
                "source_gap_classification_age_hours": 21,
                "source_gap_classification_rows": 2,
                "symbol_changes_generated_at": "2026-05-24T04:00:00Z",
                "symbol_changes_age_hours": 20,
                "symbol_changes_review_rows": 3,
                "masterfile_collision_review_generated_at": "2026-05-24T05:00:00Z",
                "masterfile_collision_review_age_hours": 19,
                "masterfile_collision_review_rows": 4,
                "ohlcv_plausibility_generated_at": "2026-05-24T06:00:00Z",
                "ohlcv_plausibility_age_hours": 18,
                "ohlcv_plausibility_rows": 5,
            },
        }
    )

    by_key = {row["key"]: row for row in snapshot}
    assert {"tickers", "masterfiles", "identifiers", "symbol_changes", "source_gap_classification"} <= set(by_key)
    assert by_key["tickers"]["rows"] == 10
    assert by_key["tickers"]["age_hours"] == 24.4
    assert by_key["masterfiles"]["age_bucket"] == "age_48_168h"
    assert by_key["symbol_changes"]["source_type"] == "workflow_report"
    assert "does not authorize canonical data changes" in by_key["symbol_changes"]["source_gate"]
    assert freshness_snapshot_age_bucket_totals(snapshot) == {"age_0_48h": 7, "age_48_168h": 1}
    assert top_freshness_snapshot_ages(snapshot, limit=2) == [
        {
            "key": "masterfiles",
            "source_type": "source_inventory",
            "generated_at": "2026-05-20T00:00:00Z",
            "age_hours": 120.0,
            "age_bucket": "age_48_168h",
            "rows": 20,
            "recommended_next_source": "Fresh generated_at and row-count evidence for masterfiles.",
            "source_gate": (
                "Freshness is visibility evidence only; it does not authorize canonical data changes without "
                "listing-keyed source review."
            ),
        },
        {
            "key": "tickers",
            "source_type": "dataset",
            "generated_at": "2026-05-24T00:00:00Z",
            "age_hours": 24.4,
            "age_bucket": "age_0_48h",
            "rows": 10,
            "recommended_next_source": "Fresh generated_at and row-count evidence for tickers.",
            "source_gate": (
                "Freshness is visibility evidence only; it does not authorize canonical data changes without "
                "listing-keyed source review."
            ),
        },
    ]


def test_delta_evidence_has_stable_shape() -> None:
    evidence = delta_evidence("partial", {"rows": 1}, ["missing_snapshot"])

    assert evidence == {
        "status": "partial",
        "known_deltas": {"rows": 1},
        "missing_deltas": ["missing_snapshot"],
    }


def test_scoped_exchange_delta_totals_sums_requested_exchanges() -> None:
    result = scoped_exchange_delta_totals(
        {
            "exchange_acceptance_delta_matrix": {
                "B3": {
                    "isin_delta": {"delta": 2},
                    "sector_delta": {"delta": 3},
                    "source_gap_delta": {"delta": -1},
                    "warn_delta": {"delta": 0},
                    "quarantine_delta": {"delta": 0},
                },
                "ASX": {
                    "isin_delta": {"delta": 10},
                    "sector_delta": {"delta": 0},
                    "source_gap_delta": {"delta": 0},
                    "warn_delta": {"delta": 1},
                    "quarantine_delta": {"delta": 1},
                },
            }
        },
        ["B3"],
    )

    assert result["exchange_count"] == 1
    assert result["changed_exchange_rows"] == 1
    assert result["delta_totals"]["isin_delta"] == 2
    assert result["delta_totals"]["sector_delta"] == 3
    assert result["delta_totals"]["category_delta"] == 3
    assert result["delta_totals"]["source_gap_delta"] == -1
    assert result["delta_totals"]["quarantine_delta"] == 0


def test_acceptance_matrix_documents_required_delta_shape() -> None:
    result = acceptance_matrix(
        campaign_key="b3",
        exchanges=["B3"],
        artifacts=[{"rows": 2}, {"rows": 3}],
        deltas={
            "acceptance_delta_matrix": {"isin_delta": {"delta": 0}},
            "campaign_deltas": {"b3": {"children": {"missing_isin_residual_rows": {"delta": -1}}}},
            "exchange_acceptance_delta_matrix": {"B3": {"isin_delta": {"delta": 0}}},
        },
    )

    assert result["campaign_key"] == "b3"
    assert result["exchange_scope"] == ["B3"]
    assert result["affected_artifact_rows"] == 5
    assert "isin_delta" in result["required_metrics"]
    assert result["campaign_metric_deltas"]["missing_isin_residual_rows"]["delta"] == -1


def test_campaign_review_policy_documents_no_guessing_source_authority_and_traceability() -> None:
    policy = campaign_review_policy("b3")

    assert set(policy) == {
        "campaign_scope",
        "no_guessing",
        "source_authority",
        "traceability",
        "uncertain_gap_handling",
    }
    assert "No inferred" in policy["no_guessing"]
    assert "Official exchange" in policy["source_authority"]
    assert "blank" in policy["uncertain_gap_handling"]
    assert "listing-keyed" in policy["traceability"]


def test_campaign_contexts_are_derived_from_campaign_fields() -> None:
    campaign = {
        "priority": 1,
        "campaign_key": "b3",
        "status": "partial",
        "next_action": "review B3 gaps",
        "artifacts": [
            {"path": "data/reports/a.json", "rows": 2},
            {"path": "data/reports/b.json", "rows": 5},
        ],
        "acceptance_matrix": {"exchange_scope": ["B3"], "affected_artifact_rows": 7},
        "delta_evidence": {"status": "partial", "missing_deltas": ["source_gap_delta"]},
        "before_after_summary": {"exchange_scope": ["B3"]},
    }

    assert campaign_context_for(campaign) == "priority=1;campaign_key=b3;status=partial;exchange_scope=B3"
    assert (
        artifact_context_for(campaign)
        == "artifact_count=2;affected_artifact_rows=7;primary_artifact=data/reports/b.json;primary_artifact_rows=5"
    )
    assert (
        delta_review_context_for(campaign)
        == "delta_status=partial;missing_delta_count=1;before_after_present=true;next_action_present=true"
    )
    assert (
        closure_context_for(campaign)
        == "closure_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure;artifact_rows=7;missing_delta_count=1;closure_status=blocked_until_closure_gate_resolved"
    )


def test_before_after_context_is_derived_from_summary_fields() -> None:
    summary = {
        "exchange_scope": ["B3"],
        "affected_artifact_rows": 7,
        "global_before_after": {
            "isin_delta": {"before": 1, "after": 2, "delta": 1},
            "warn_delta": {"before": 3, "after": 3, "delta": 0},
        },
        "exchange_scope_delta_totals": {"isin_delta": 1, "warn_delta": 0},
        "warn_quarantine_delta": {
            "warn_delta": {"before": 3, "after": 3, "delta": 0},
            "quarantine_delta": {"before": 0, "after": 0, "delta": 0},
        },
    }

    assert before_after_context_for(summary) == (
        "exchange_scope=B3;affected_artifact_rows=7;global_delta_count=2;"
        "nonzero_global_delta_count=1;nonzero_exchange_scope_delta_count=1;"
        "warn_delta=0;quarantine_delta=0"
    )


def test_summary_context_is_derived_from_summary_fields() -> None:
    summary = {
        "campaigns": 10,
        "complete_campaigns": 0,
        "next_review_batches": 10,
        "next_review_batch_rows": 28722,
        "closure_ready_campaigns": 9,
        "closure_blocked_campaigns": 1,
        "closure_blockers": 1,
        "validation_failed_error_gates": 0,
    }

    assert summary_context_for(summary) == (
        "campaigns=10;complete_campaigns=0;next_review_batches=10;"
        "next_review_batch_rows=28722;closure_ready_campaigns=9;"
        "closure_blocked_campaigns=1;closure_blockers=1;validation_failed_error_gates=0"
    )


def test_next_review_batches_select_primary_artifact_and_closure_gate() -> None:
    batches = next_review_batches(
        [
            {
                "priority": 2,
                "campaign_key": "otc",
                "status": "partial",
                "delta_evidence": {"status": "partial", "missing_deltas": ["scope_snapshot"]},
                "artifacts": [
                    {"path": "data/reports/otc_scope_review.json", "rows": 10},
                    {"path": "data/reports/otc_name_mismatch_review.json", "rows": 3},
                ],
                "next_action": "review issuer evidence",
            },
            {
                "priority": 10,
                "campaign_key": "baseline",
                "status": "baseline_only",
                "delta_evidence": {"status": "baseline_only", "missing_deltas": ["next_campaign"]},
                "artifacts": [{"path": "data/reports/improvement_baseline.json", "rows": 0}],
                "next_action": "compare future deltas",
            },
            {
                "priority": 11,
                "campaign_key": "done",
                "status": "complete",
                "delta_evidence": {"status": "complete", "missing_deltas": []},
                "artifacts": [{"path": "data/reports/done.json", "rows": 0}],
                "next_action": "",
            },
        ]
    )

    assert [batch["campaign_key"] for batch in batches] == ["otc", "baseline"]
    assert batches[0]["artifact_rows"] == 13
    assert batches[0]["primary_artifact"] == "data/reports/otc_scope_review.json"
    assert batches[0]["primary_artifact_rows"] == 10
    assert batches[0]["closure_gate"] == "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure"
    assert batches[0]["closure_context"] == (
        "closure_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure;"
        "artifact_rows=13;missing_delta_count=1;closure_status=blocked_until_closure_gate_resolved"
    )
    assert batches[0]["recommended_next_source"] == (
        "OTC Markets profile, SEC/issuer filing, product taxonomy, or reviewed issuer/product source matching the OTC listing key."
    )
    assert batches[0]["source_gate"] == (
        "Do not enrich OTC rows from symbol/name shape; decide scope and require issuer/product evidence before metadata changes."
    )
    assert batches[1]["closure_gate"] == "future_campaign_delta_comparison_required_before_release_closure"
    assert next_review_recommended_source("symbol_changes").startswith("Official exchange notice")
    assert "OHLCV only as plausibility evidence" in next_review_source_gate("ohlcv")


def test_closure_readiness_summary_counts_ready_and_blocked_campaigns() -> None:
    summary = closure_readiness_summary(
        [
            {
                "campaign_key": "b3",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
            },
            {
                "campaign_key": "otc",
                "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
            },
            {
                "campaign_key": "baseline",
                "closure_gate": "future_campaign_delta_comparison_required_before_release_closure",
            },
        ]
    )

    assert summary["ready_campaigns"] == 1
    assert summary["blocked_campaigns"] == 2
    assert summary["ready_campaign_keys"] == ["otc"]
    assert summary["blocked_campaign_keys"] == ["b3", "baseline"]
    assert summary["closure_gate_counts"] == {
        "future_campaign_delta_comparison_required_before_release_closure": 1,
        "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure": 1,
        "review_artifact_rows_must_be_processed_before_campaign_closure": 1,
    }
    assert "not closure-ready" in summary["policy"]


def test_next_review_workload_summary_is_derived_from_batches() -> None:
    batches = [
        {
            "priority": 1,
            "campaign_key": "b3",
            "artifact_rows": 5,
            "primary_artifact": "data/reports/b3.json",
            "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
        },
        {
            "priority": 2,
            "campaign_key": "otc",
            "artifact_rows": 11,
            "primary_artifact": "data/reports/otc.json",
            "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
        },
    ]

    workload = next_review_workload_summary(batches)

    assert workload["total_batches"] == 2
    assert workload["total_rows"] == 16
    assert workload["blocked_batches"] == 1
    assert workload["rows_by_campaign_key"] == {"b3": 5, "otc": 11}
    assert workload["rows_by_closure_gate"] == {
        "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure": 11,
        "review_artifact_rows_must_be_processed_before_campaign_closure": 5,
    }
    assert workload["largest_batch"] == {
        "priority": 2,
        "campaign_key": "otc",
        "artifact_rows": 11,
        "primary_artifact": "data/reports/otc.json",
        "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
    }
    assert next_review_workload_context(workload) == (
        "batches=2;rows=16;blocked_batches=1;top_campaign=otc;top_rows=11;"
        "top_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure"
    )


def test_next_review_execution_plan_is_source_gated_and_non_authorizing() -> None:
    plan = next_review_execution_plan(
        [
            {
                "priority": 9,
                "campaign_key": "freshness",
                "artifact_rows": 4,
                "primary_artifact": "data/reports/coverage_report.json",
                "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
                "missing_delta_evidence": [],
                "next_action": "refresh stale official sources",
                "recommended_next_source": "Fresh official source.",
                "source_gate": "Refresh first.",
            },
            {
                "priority": 2,
                "campaign_key": "otc",
                "artifact_rows": 10,
                "primary_artifact": "data/reports/otc_scope_review.json",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                "missing_delta_evidence": ["scope_snapshot"],
                "next_action": "review OTC source gaps",
                "recommended_next_source": "OTC Markets profile.",
                "source_gate": "No metadata enrichment from symbol shape.",
            },
        ]
    )

    assert [row["campaign_key"] for row in plan] == ["otc", "freshness"]
    assert plan[0]["command_mode"] == "local_report_rebuild"
    assert plan[0]["execution_order"] == 1
    assert plan[0]["ranking_reason"] == "objective_priority_order_with_local_review_artifact_processing"
    assert plan[0]["command_scripts"] == [
        "scripts/build_otc_scope_review.py",
        "scripts/build_otc_name_mismatch_review.py",
        "scripts/build_improvement_delta_report.py",
    ]
    assert plan[0]["missing_command_scripts"] == []
    assert plan[0]["command_readiness_context"] == (
        "campaign_key=otc;script_count=3;missing_script_count=0;all_scripts_exist=true"
    )
    assert plan[0]["command_mutation_risk"] == "report_or_fetch_only"
    assert plan[0]["manual_review_required_before_run"] is False
    assert plan[0]["data_change_authorized"] is False
    assert plan[0]["source_gate"] == "No metadata enrichment from symbol shape."
    assert plan[1]["command_mode"] == "network_evidence_refresh"
    assert plan[1]["execution_order"] == 2
    assert plan[1]["ranking_reason"] == "objective_priority_order_with_external_source_refresh_required"
    assert plan[1]["network_required"] is True
    assert plan[1]["data_change_authorized"] is False
    assert execution_plan_context_for(plan[1]) == (
        "priority=9;campaign_key=freshness;artifact_rows=4;"
        "command_mode=network_evidence_refresh;network_required=true;data_change_authorized=false"
    )
    assert execution_ranking_reason({"artifact_rows": 0, "network_required": False}) == (
        "objective_priority_order_future_delta_baseline_gate"
    )
    assert execution_ranking_context_for(plan[1]) == (
        "execution_order=2;priority=9;campaign_key=freshness;artifact_rows=4;"
        "network_required=true;ranking_reason=objective_priority_order_with_external_source_refresh_required"
    )
    assert command_script_paths("python scripts/a.py && python scripts/b-name.py --flag") == [
        "scripts/a.py",
        "scripts/b-name.py",
    ]
    assert command_readiness_context_for(
        {"campaign_key": "x", "command_scripts": ["scripts/a.py"], "missing_command_scripts": ["scripts/a.py"]}
    ) == "campaign_key=x;script_count=1;missing_script_count=1;all_scripts_exist=false"
    assert command_mutation_risk_for(["scripts/apply_x.py"]) == "review_required"
    assert command_mutation_risk_for(["scripts/build_x.py", "scripts/fetch_y.py"]) == "report_or_fetch_only"
    assert risky_command_scripts(["scripts/build_x.py", "scripts/backfill_x.py", "scripts/enrich_y.py"]) == [
        "scripts/backfill_x.py",
        "scripts/enrich_y.py",
    ]
    assert command_safety_context_for(
        {
            "campaign_key": "b3",
            "command_mutation_risk": "review_required",
            "manual_review_required_before_run": True,
            "data_change_authorized": False,
        }
    ) == (
        "campaign_key=b3;command_mutation_risk=review_required;"
        "manual_review_required_before_run=true;data_change_authorized=false"
    )
    assert review_required_command_context_for(
        {
            "campaign_key": "b3",
            "risky_command_scripts": ["scripts/apply_x.py"],
            "manual_review_required_before_run": True,
            "data_change_authorized": False,
        }
    ) == (
        "campaign_key=b3;risky_script_count=1;"
        "manual_review_required_before_run=true;data_change_authorized=false"
    )
    assert review_required_preflight_checks({"command_mutation_risk": "report_or_fetch_only"}) == []
    checks = review_required_preflight_checks({"command_mutation_risk": "review_required"})
    assert checks == [
        "inspect_risky_scripts_before_execution",
        "confirm_listing_keyed_source_review_for_any_write",
        "rerun_quality_validation_and_release_acceptance_after_execution",
    ]
    assert review_required_preflight_context_for(
        {
            "campaign_key": "b3",
            "review_required_preflight_checks": checks,
            "manual_review_required_before_run": True,
            "data_change_authorized": False,
        }
    ) == (
        "campaign_key=b3;preflight_check_count=3;"
        "manual_review_required_before_run=true;data_change_authorized=false"
    )


def test_next_review_execution_summary_counts_local_network_and_authorization() -> None:
    plan = [
        {
            "campaign_key": "b3",
            "artifact_rows": 5,
            "command_mode": "local_report_rebuild",
            "network_required": False,
            "data_change_authorized": False,
        },
        {
            "campaign_key": "freshness",
            "artifact_rows": 7,
            "command_mode": "network_evidence_refresh",
            "network_required": True,
            "data_change_authorized": False,
        },
    ]

    summary = next_review_execution_summary(plan)

    assert summary["total_actions"] == 2
    assert summary["local_report_rebuild_actions"] == 1
    assert summary["network_evidence_refresh_actions"] == 1
    assert summary["network_required_rows"] == 7
    assert summary["local_report_rebuild_rows"] == 5
    assert summary["rows_by_command_mode"] == {
        "local_report_rebuild": 5,
        "network_evidence_refresh": 7,
    }
    assert summary["network_campaign_keys"] == ["freshness"]
    assert summary["local_campaign_keys"] == ["b3"]
    assert summary["data_change_authorized_actions"] == 0
    assert execution_summary_context_for(summary) == (
        "actions=2;local_actions=1;network_actions=1;network_rows=7;data_change_authorized_actions=0"
    )


def test_next_review_command_safety_summary_counts_review_required_actions() -> None:
    plan = [
        {
            "campaign_key": "b3",
            "command_mutation_risk": "review_required",
            "risky_command_scripts": ["scripts/apply_b3.py"],
            "manual_review_required_before_run": True,
            "data_change_authorized": False,
            "review_required_command_context": (
                "campaign_key=b3;risky_script_count=1;"
                "manual_review_required_before_run=true;data_change_authorized=false"
            ),
            "review_required_preflight_checks": [
                "inspect_risky_scripts_before_execution",
                "confirm_listing_keyed_source_review_for_any_write",
                "rerun_quality_validation_and_release_acceptance_after_execution",
            ],
            "review_required_preflight_context": (
                "campaign_key=b3;preflight_check_count=3;"
                "manual_review_required_before_run=true;data_change_authorized=false"
            ),
        },
        {
            "campaign_key": "otc",
            "command_mutation_risk": "report_or_fetch_only",
            "manual_review_required_before_run": False,
            "data_change_authorized": False,
        },
    ]

    summary = next_review_command_safety_summary(plan)

    assert summary["total_actions"] == 2
    assert summary["risk_counts"] == {"report_or_fetch_only": 1, "review_required": 1}
    assert summary["review_required_actions"] == 1
    assert summary["report_or_fetch_only_actions"] == 1
    assert summary["manual_review_required_actions"] == 1
    assert summary["review_required_campaign_keys"] == ["b3"]
    assert summary["preflight_complete_actions"] == 1
    assert summary["preflight_gap_campaign_keys"] == []
    assert summary["execution_ready_without_manual_review"] is False
    assert summary["execution_blocking_gate"] == "manual_review_required_before_execution"
    assert summary["execution_blocking_campaign_keys"] == ["b3"]
    assert summary["review_required_command_rows"] == [
        {
            "campaign_key": "b3",
            "risky_command_scripts": ["scripts/apply_b3.py"],
            "manual_review_required_before_run": True,
            "data_change_authorized": False,
            "review_required_command_context": (
                "campaign_key=b3;risky_script_count=1;"
                "manual_review_required_before_run=true;data_change_authorized=false"
            ),
            "review_required_preflight_checks": [
                "inspect_risky_scripts_before_execution",
                "confirm_listing_keyed_source_review_for_any_write",
                "rerun_quality_validation_and_release_acceptance_after_execution",
            ],
            "review_required_preflight_context": (
                "campaign_key=b3;preflight_check_count=3;"
                "manual_review_required_before_run=true;data_change_authorized=false"
            ),
        }
    ]
    assert summary["data_change_authorized_actions"] == 0
    assert command_safety_summary_context_for(summary) == (
        "actions=2;review_required_actions=1;report_or_fetch_only_actions=1;"
        "manual_review_required_actions=1;preflight_complete_actions=1;data_change_authorized_actions=0;"
        "execution_ready_without_manual_review=false;"
        "execution_blocking_gate=manual_review_required_before_execution"
    )


def test_evidence_command_for_blocker_uses_review_or_report_scripts() -> None:
    assert evidence_command_for_blocker("b3", "source_gap_delta") == (
        "python scripts/build_b3_masterfile_gap_review.py && "
        "python scripts/apply_b3_etf_category_review.py && "
        "python scripts/backfill_b3_sector_classification.py && "
        "python scripts/build_b3_residual_isin_review.py && "
        "python scripts/build_b3_residual_sector_review.py && "
        "python scripts/build_coverage_report.py && "
        "python scripts/build_improvement_delta_report.py"
    )
    assert evidence_command_for_blocker("baseline", "next_campaign_after_snapshot") == (
        "python scripts/build_improvement_delta_report.py"
    )
    assert evidence_command_for_blocker("unknown", "missing_delta") == (
        "python scripts/build_improvement_delta_report.py # resolve missing_delta"
    )


def test_command_metadata_for_blocker_marks_network_without_authorizing_changes() -> None:
    local = command_metadata_for_blocker("b3", "python scripts/build_b3_residual_isin_review.py")
    network = command_metadata_for_blocker("freshness", "python scripts/fetch_exchange_masterfiles.py")

    assert local["command_mode"] == "local_report_rebuild"
    assert local["network_required"] is False
    assert local["data_change_authorized"] is False
    assert network["command_mode"] == "network_evidence_refresh"
    assert network["network_required"] is True
    assert network["data_change_authorized"] is False
    assert "do not authorize" in network["safety_policy"]


def test_closure_blockers_rank_missing_delta_before_future_baseline() -> None:
    blockers = closure_blockers(
        [
            {
                "priority": 10,
                "campaign_key": "baseline",
                "closure_gate": "future_campaign_delta_comparison_required_before_release_closure",
                "artifact_rows": 0,
                "primary_artifact": "",
                "missing_delta_evidence": ["next_campaign_after_snapshot"],
                "next_action": "compare future deltas",
            },
            {
                "priority": 2,
                "campaign_key": "otc",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                "artifact_rows": 10,
                "primary_artifact": "data/reports/otc_scope_review.json",
                "missing_delta_evidence": ["scope_snapshot", "warn_delta"],
                "next_action": "review issuer evidence",
            },
            {
                "priority": 1,
                "campaign_key": "done",
                "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
                "artifact_rows": 3,
                "primary_artifact": "data/reports/done.json",
                "missing_delta_evidence": [],
                "next_action": "process rows",
            },
        ]
    )

    assert [row["campaign_key"] for row in blockers] == ["otc", "baseline"]
    assert blockers[0]["blocker_type"] == "missing_delta_evidence"
    assert blockers[0]["first_missing_delta"] == "scope_snapshot"
    assert blockers[0]["evidence_command"] == (
        "python scripts/build_otc_scope_review.py && "
        "python scripts/build_otc_name_mismatch_review.py && "
        "python scripts/build_improvement_delta_report.py"
    )
    assert blockers[0]["command_mode"] == "local_report_rebuild"
    assert blockers[0]["network_required"] is False
    assert blockers[0]["data_change_authorized"] is False
    assert blockers[0]["recommended_next_source"] == (
        "OTC Markets profile, SEC/issuer filing, product taxonomy, or reviewed issuer/product source matching the OTC listing key."
    )
    assert blockers[0]["source_gate"] == (
        "Do not enrich OTC rows from symbol/name shape; decide scope and require issuer/product evidence before metadata changes."
    )
    assert blockers[0]["blocker_context"] == blocker_context_for(blockers[0])
    assert blockers[1]["blocker_type"] == "future_baseline_comparison"


def test_render_markdown_includes_next_review_batches() -> None:
    payload = {
        "_meta": {
            "generated_at": "2026-05-24T00:00:00Z",
            "source_files": {
                "coverage_report": "data/reports/coverage_report.json",
                "improvement_deltas": "data/reports/improvement_deltas.json",
            },
        },
        "summary_context": "campaigns=1;complete_campaigns=0;next_review_batches=1;next_review_batch_rows=2;closure_ready_campaigns=0;closure_blocked_campaigns=1;closure_blockers=1;validation_failed_error_gates=0",
        "campaigns": [
            {
                "priority": 1,
                "name": "B3",
                "campaign_key": "b3",
                "status": "partial",
                "delta_evidence": {"status": "partial", "missing_deltas": ["source_gap_delta"]},
                "artifacts": [{"path": "data/reports/b3_residual_isin_review.json", "rows": 2}],
                "next_action": "review official B3 source",
                "evidence": {
                    "rows": 2,
                    "top_source_gap_review_batches": [
                        {
                            "field": "missing_sector_stock",
                            "gap_class": "official_industry_taxonomy_unavailable_gap",
                            "exchange": "B3",
                            "rows": 2,
                            "recommended_next_source": "Official taxonomy source.",
                            "source_gate": "Exact listing-keyed taxonomy evidence.",
                        }
                    ],
                    "top_b3_isin_review_batches": [
                        {
                            "review_priority": "P1",
                            "review_bucket": "scope_review_before_identifier_fill",
                            "rows": 2,
                            "review_strategy": "decide_b3_core_extended_or_exclude_before_identifier_work",
                            "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_fill",
                            "recommended_next_source": "Current B3 source plus reviewed scope decision.",
                            "source_gate": "No ISIN fill before scope review.",
                        }
                    ],
                },
                "review_policy": campaign_review_policy("b3"),
                "acceptance_matrix": {},
            }
        ],
        "next_review_batches": [
            {
                "execution_order": 1,
                "priority": 1,
                "campaign_key": "b3",
                "artifact_rows": 2,
                "primary_artifact": "data/reports/b3_residual_isin_review.json",
                "missing_delta_evidence": ["source_gap_delta"],
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                "closure_context": (
                    "closure_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure;"
                    "artifact_rows=2;missing_delta_count=1;closure_status=blocked_until_closure_gate_resolved"
                ),
                "next_action": "review official B3 source",
                "recommended_next_source": "Official taxonomy source.",
                "source_gate": "Exact listing-keyed taxonomy evidence.",
            }
        ],
        "next_review_workload": {
            "total_batches": 1,
            "total_rows": 2,
            "blocked_batches": 1,
            "rows_by_campaign_key": {"b3": 2},
            "rows_by_closure_gate": {
                "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure": 2
            },
            "largest_batch": {
                "priority": 1,
                "campaign_key": "b3",
                "artifact_rows": 2,
                "primary_artifact": "data/reports/b3_residual_isin_review.json",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
            },
            "workload_context": (
                "batches=1;rows=2;blocked_batches=1;top_campaign=b3;top_rows=2;"
                "top_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure"
            ),
        },
        "next_review_execution_plan": [
            {
                "execution_order": 1,
                "priority": 1,
                "campaign_key": "b3",
                "artifact_rows": 2,
                "primary_artifact": "data/reports/b3_residual_isin_review.json",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                "evidence_command": "python scripts/build_b3_masterfile_gap_review.py && python scripts/apply_b3_etf_category_review.py && python scripts/backfill_b3_sector_classification.py && python scripts/build_b3_residual_isin_review.py && python scripts/build_b3_residual_sector_review.py && python scripts/build_coverage_report.py && python scripts/build_improvement_delta_report.py",
                "command_mode": "network_evidence_refresh",
                "network_required": True,
                "data_change_authorized": False,
                "recommended_next_source": "Official taxonomy source.",
                "source_gate": "Exact listing-keyed taxonomy evidence.",
                "next_action": "review official B3 source",
                "ranking_reason": "objective_priority_order_with_external_source_refresh_required",
                "ranking_context": (
                    "execution_order=1;priority=1;campaign_key=b3;artifact_rows=2;"
                    "network_required=true;ranking_reason=objective_priority_order_with_external_source_refresh_required"
                ),
                "command_scripts": [
                    "scripts/build_b3_masterfile_gap_review.py",
                    "scripts/apply_b3_etf_category_review.py",
                ],
                "missing_command_scripts": [],
                "command_readiness_context": (
                    "campaign_key=b3;script_count=2;missing_script_count=0;all_scripts_exist=true"
                ),
                "command_mutation_risk": "review_required",
                "risky_command_scripts": ["scripts/apply_b3_etf_category_review.py"],
                "manual_review_required_before_run": True,
                "command_safety_context": (
                    "campaign_key=b3;command_mutation_risk=review_required;"
                    "manual_review_required_before_run=true;data_change_authorized=false"
                ),
                "review_required_command_context": (
                    "campaign_key=b3;risky_script_count=1;"
                    "manual_review_required_before_run=true;data_change_authorized=false"
                ),
                "review_required_preflight_checks": [
                    "inspect_risky_scripts_before_execution",
                    "confirm_listing_keyed_source_review_for_any_write",
                    "rerun_quality_validation_and_release_acceptance_after_execution",
                ],
                "review_required_preflight_context": (
                    "campaign_key=b3;preflight_check_count=3;"
                    "manual_review_required_before_run=true;data_change_authorized=false"
                ),
                "execution_context": (
                    "priority=1;campaign_key=b3;artifact_rows=2;"
                    "command_mode=network_evidence_refresh;network_required=true;data_change_authorized=false"
                ),
            }
        ],
        "next_review_execution_summary": {
            "total_actions": 1,
            "local_report_rebuild_actions": 0,
            "network_evidence_refresh_actions": 1,
            "network_required_rows": 2,
            "local_report_rebuild_rows": 0,
            "rows_by_command_mode": {"network_evidence_refresh": 2},
            "network_campaign_keys": ["b3"],
            "local_campaign_keys": [],
            "data_change_authorized_actions": 0,
            "execution_summary_context": (
                "actions=1;local_actions=0;network_actions=1;"
                "network_rows=2;data_change_authorized_actions=0"
            ),
        },
        "next_review_command_safety_summary": {
            "total_actions": 1,
            "risk_counts": {"review_required": 1},
            "review_required_actions": 1,
            "report_or_fetch_only_actions": 0,
            "manual_review_required_actions": 1,
            "review_required_campaign_keys": ["b3"],
            "preflight_complete_actions": 1,
            "preflight_gap_campaign_keys": [],
            "review_required_command_rows": [
                {
                    "campaign_key": "b3",
                    "risky_command_scripts": ["scripts/apply_b3_etf_category_review.py"],
                    "manual_review_required_before_run": True,
                    "data_change_authorized": False,
                    "review_required_command_context": (
                        "campaign_key=b3;risky_script_count=1;"
                        "manual_review_required_before_run=true;data_change_authorized=false"
                    ),
                    "review_required_preflight_checks": [
                        "inspect_risky_scripts_before_execution",
                        "confirm_listing_keyed_source_review_for_any_write",
                        "rerun_quality_validation_and_release_acceptance_after_execution",
                    ],
                    "review_required_preflight_context": (
                        "campaign_key=b3;preflight_check_count=3;"
                        "manual_review_required_before_run=true;data_change_authorized=false"
                    ),
                }
            ],
            "data_change_authorized_actions": 0,
            "execution_ready_without_manual_review": False,
            "execution_blocking_gate": "manual_review_required_before_execution",
            "execution_blocking_campaign_keys": ["b3"],
            "command_safety_summary_context": (
                "actions=1;review_required_actions=1;report_or_fetch_only_actions=0;"
                "manual_review_required_actions=1;preflight_complete_actions=1;data_change_authorized_actions=0;"
                "execution_ready_without_manual_review=false;"
                "execution_blocking_gate=manual_review_required_before_execution"
            ),
        },
        "closure_readiness": {
            "ready_campaigns": 0,
            "blocked_campaigns": 1,
            "closure_gate_counts": {
                "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure": 1
            },
            "blocked_campaign_keys": ["b3"],
            "policy": "Campaigns are not closure-ready while missing delta evidence remains.",
        },
        "closure_blockers": [
            {
                "priority": 1,
                "campaign_key": "b3",
                "blocker_type": "missing_delta_evidence",
                "artifact_rows": 2,
                "primary_artifact": "data/reports/b3_residual_isin_review.json",
                "first_missing_delta": "source_gap_delta",
                "evidence_command": "python scripts/build_b3_masterfile_gap_review.py && python scripts/apply_b3_etf_category_review.py && python scripts/backfill_b3_sector_classification.py && python scripts/build_b3_residual_isin_review.py && python scripts/build_b3_residual_sector_review.py && python scripts/build_coverage_report.py && python scripts/build_improvement_delta_report.py",
                "command_mode": "network_evidence_refresh",
                "network_required": True,
                "data_change_authorized": False,
                "next_action": "review official B3 source",
                "recommended_next_source": "Official taxonomy source.",
                "source_gate": "Exact listing-keyed taxonomy evidence.",
                "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                "blocker_context": (
                    "campaign_key=b3;blocker_type=missing_delta_evidence;"
                    "closure_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure;"
                    "command_mode=network_evidence_refresh;network_required=true;data_change_authorized=false"
                ),
            }
        ],
    }

    markdown = render_markdown(payload)

    assert "## Source Files" in markdown
    assert "Summary context: `campaigns=1;complete_campaigns=0;next_review_batches=1" in markdown
    assert "| `coverage_report` | `data/reports/coverage_report.json` |" in markdown
    assert "| `improvement_deltas` | `data/reports/improvement_deltas.json` |" in markdown
    assert "## Next Review Batches" in markdown
    assert "| 1 | `b3` | 2 | `data/reports/b3_residual_isin_review.json` | source_gap_delta |" in markdown
    assert "`missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure`" in markdown
    assert "closure_status=blocked_until_closure_gate_resolved" in markdown
    assert "## Next Review Workload" in markdown
    assert (
        "Workload context: `batches=1;rows=2;blocked_batches=1;top_campaign=b3;top_rows=2;"
        "top_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure`"
    ) in markdown
    assert "| `largest_campaign` | `b3` |" in markdown
    assert "| `b3` | 2 |" in markdown
    assert "## Next Review Execution Plan" in markdown
    assert "| 1 | `b3` | 2 | `network_evidence_refresh` | True | False |" in markdown
    assert "priority=1;campaign_key=b3;artifact_rows=2;command_mode=network_evidence_refresh" in markdown
    assert "| Order | Campaign | Ranking Reason | Ranking Context |" in markdown
    assert (
        "| 1 | `b3` | `objective_priority_order_with_external_source_refresh_required` | "
        "`execution_order=1;priority=1;campaign_key=b3;artifact_rows=2;"
        "network_required=true;ranking_reason=objective_priority_order_with_external_source_refresh_required` |"
    ) in markdown
    assert "| Campaign | Scripts | Missing Scripts | Command Readiness Context |" in markdown
    assert "| `b3` | 2 | 0 | `campaign_key=b3;script_count=2;missing_script_count=0;all_scripts_exist=true` |" in markdown
    assert "| Campaign | Mutation Risk | Manual Review Before Run | Command Safety Context |" in markdown
    assert (
        "| `b3` | `review_required` | True | `campaign_key=b3;command_mutation_risk=review_required;"
        "manual_review_required_before_run=true;data_change_authorized=false` |"
    ) in markdown
    assert "## Next Review Command Safety Summary" in markdown
    assert (
        "Command safety summary context: `actions=1;review_required_actions=1;report_or_fetch_only_actions=0;"
        "manual_review_required_actions=1;preflight_complete_actions=1;data_change_authorized_actions=0;"
        "execution_ready_without_manual_review=false;"
        "execution_blocking_gate=manual_review_required_before_execution`"
    ) in markdown
    assert "| `preflight_complete_actions` | `1` |" in markdown
    assert "| `preflight_gap_campaign_keys` | `[]` |" in markdown
    assert "| `execution_ready_without_manual_review` | `False` |" in markdown
    assert "| `execution_blocking_gate` | `manual_review_required_before_execution` |" in markdown
    assert "| `execution_blocking_campaign_keys` | `[\"b3\"]` |" in markdown
    assert "| `review_required_campaign_keys` | `[\"b3\"]` |" in markdown
    assert "| Campaign | Risky Scripts | Manual Review | Data Change Authorized | Preflight Checks | Review-Required Context | Preflight Context |" in markdown
    assert (
        "| `b3` | `[\"scripts/apply_b3_etf_category_review.py\"]` | True | False | "
        "`[\"inspect_risky_scripts_before_execution\", \"confirm_listing_keyed_source_review_for_any_write\", "
        "\"rerun_quality_validation_and_release_acceptance_after_execution\"]` | "
        "`campaign_key=b3;risky_script_count=1;manual_review_required_before_run=true;data_change_authorized=false` | "
        "`campaign_key=b3;preflight_check_count=3;manual_review_required_before_run=true;data_change_authorized=false` |"
    ) in markdown
    assert "## Next Review Execution Summary" in markdown
    assert (
        "Execution summary context: `actions=1;local_actions=0;network_actions=1;"
        "network_rows=2;data_change_authorized_actions=0`"
    ) in markdown
    assert "| `network_campaign_keys` | `[\"b3\"]` |" in markdown
    assert "data_change_authorized=false" in markdown
    assert "## Closure Readiness" in markdown
    assert "| `blocked_campaigns` | `1` |" in markdown
    assert "## Closure Blockers" in markdown
    assert "| 1 | `b3` | `missing_delta_evidence` | 2 | `data/reports/b3_residual_isin_review.json` | `source_gap_delta` |" in markdown
    assert "| `b3` | `network_evidence_refresh` | True | False | `campaign_key=b3;blocker_type=missing_delta_evidence;" in markdown
    assert "`python scripts/build_b3_masterfile_gap_review.py && python scripts/apply_b3_etf_category_review.py && python scripts/backfill_b3_sector_classification.py && python scripts/build_b3_residual_isin_review.py && python scripts/build_b3_residual_sector_review.py && python scripts/build_coverage_report.py && python scripts/build_improvement_delta_report.py` |" in markdown
    assert "| Batch | Priority | Rows | Strategy | Evidence | Recommended Next Source | Source Gate |" in markdown
    assert (
        "| `field=missing_sector_stock` | `` | 2 |  |  | Official taxonomy source. | Exact listing-keyed taxonomy evidence. |"
        in markdown
    )
    assert (
        "| `review_bucket=scope_review_before_identifier_fill` | `P1` | 2 | decide_b3_core_extended_or_exclude_before_identifier_work | scope_decision_for_core_extended_or_exclude_before_identifier_fill | Current B3 source plus reviewed scope decision. | No ISIN fill before scope review. |"
        in markdown
    )
    assert '`top_b3_isin_review_batches`: `[{"recommended_next_source"' not in markdown


def test_build_campaigns_summarizes_all_priorities() -> None:
    campaigns = build_campaigns(
        {
            "coverage_report": {
                "global": {"official_masterfile_symbols": 10},
                "b3_masterfile_diagnostics": {
                    "dataset_rows": 3,
                    "active_exchange_directory_rows": 4,
                    "all_b3_masterfile_rows": 5,
                    "matched_dataset_rows": 2,
                    "missing_dataset_rows": 1,
                    "dataset_match_rate": 66.67,
                    "official_any_source_matched_dataset_rows": 3,
                    "official_any_source_missing_dataset_rows": 0,
                    "official_any_source_match_rate": 100.0,
                    "missing_category_totals": {"unit_or_fund_line": 1},
                    "missing_asset_type_totals": {"ETF": 1},
                    "missing_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                    "missing_examples": {
                        "unit_or_fund_line": [
                            {
                                "listing_key": "B3::AFOF11",
                                "ticker": "AFOF11",
                                "asset_type": "ETF",
                                "name": "Alianza FOFII",
                                "source_presence": "present_only_in_non_exchange_directory_source",
                                "candidate_sources": "b3_listed_etfs",
                            }
                        ]
                    },
                },
                "freshness": {"tickers_built_at": "2026-05-24T00:00:00Z"},
                "source_coverage": [
                    {"freshness_status": "old", "age_bucket": "age_168_336h"},
                    {"freshness_status": "fresh", "age_bucket": "age_0_48h"},
                ],
                "source_freshness_summary": {
                    "source_age_bucket_totals": {"age_0_48h": 1, "age_168_336h": 1},
                    "refresh_priority_totals": {"P1": 1, "P4": 1},
                    "refresh_queue_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
                        "fresh_no_refresh_needed": 1,
                    },
                    "refresh_queue_scope_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {
                            "exchange_directory": 1
                        },
                        "fresh_no_refresh_needed": {"exchange_directory": 1},
                    },
                    "refresh_queue_mode_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 1},
                        "fresh_no_refresh_needed": {"network": 1},
                    },
                    "refresh_queue_priority_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 1},
                        "fresh_no_refresh_needed": {"P4": 1},
                    },
                    "refresh_queue_age_bucket_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {
                            "age_168_336h": 1
                        },
                        "fresh_no_refresh_needed": {"age_0_48h": 1},
                    },
                    "recommended_refresh_action_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
                        "no_refresh_needed": 1,
                    },
                    "refresh_queue_review_strategy_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {
                            "refresh_official_exchange_directory_before_identity_or_collision_work": 1
                        },
                        "fresh_no_refresh_needed": {"no_refresh_required": 1},
                    },
                    "refresh_queue_evidence_required_totals": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": {
                            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1
                        },
                        "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
                    },
                    "top_source_refresh_batches": [
                        {
                            "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
                            "reference_scope": "exchange_directory",
                            "mode": "cache",
                            "refresh_priority": "P1",
                            "source_count": 1,
                            "total_rows": 50,
                            "max_age_hours": 200.0,
                            "review_strategy": "refresh_official_exchange_directory_before_identity_or_collision_work",
                            "evidence_required": "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
                            "recommended_next_source": (
                                "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
                            ),
                            "source_gate": (
                                "Do not perform identity, collision, or listing-add work until the official exchange "
                                "directory is freshly regenerated."
                            ),
                        }
                    ],
                    "old_official_exchange_directory_count": 1,
                    "top_old_official_exchange_directories": [
                        {"key": "old_source", "provider": "Official", "age_hours": 200.0}
                    ],
                },
            },
            "source_gap_classification": {
                "summary": {
                    "rows": 3,
                    "class_totals": {"official_gap": 2},
                    "top_source_gap_review_batches": [
                        {
                            "field": "missing_sector_stock",
                            "gap_class": "official_gap",
                            "exchange": "B3",
                            "rows": 2,
                            "recommended_next_source": "Official taxonomy source.",
                            "source_gate": "Exact listing-keyed taxonomy evidence.",
                        }
                    ],
                }
            },
            "b3_masterfile_gap_review": {
                "summary": {
                    "rows": 1,
                    "open_review_rows": 1,
                    "closed_no_data_change_rows": 0,
                    "source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                    "open_review_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                    "review_bucket_totals": {"official_b3_non_directory_source_review": 1},
                    "b3_resolution_queue_totals": {
                        "official_subset_category_requires_review": 1
                    },
                    "open_review_resolution_queue_totals": {
                        "official_subset_category_requires_review": 1
                    },
                    "review_strategy_totals": {
                        "review_official_subset_category_and_scope_before_apply_gate": 1
                    },
                    "apply_eligibility_totals": {
                        "review_scope_or_parser_before_any_data_change": 1
                    },
                    "b3_resolution_queue_asset_type_totals": {
                        "official_subset_category_requires_review": {"ETF": 1}
                    },
                    "b3_resolution_queue_gap_category_totals": {
                        "official_subset_category_requires_review": {"unit_or_fund_line": 1}
                    },
                    "candidate_source_totals": {"b3_listed_etfs": 1},
                    "candidate_sector_present_rows": 1,
                    "candidate_isin_present_rows": 0,
                    "candidate_category_review_decision_totals": {
                        "official_candidate_category_differs_from_current_requires_review": 1
                    },
                    "official_subset_review_decision_totals": {
                        "official_subset_category_mismatch_requires_apply_gate": 1
                    },
                    "official_subset_closure_eligibility_totals": {
                        "blocked_until_category_apply_gate": 1
                    },
                    "official_subset_closure_ready_rows": 0,
                    "candidate_category_mismatch_rows": 1,
                    "candidate_category_mismatch_examples": [
                        {
                            "listing_key": "B3::AFOF11",
                            "ticker": "AFOF11",
                            "current_etf_category": "Equity",
                            "candidate_sectors": "Fixed Income",
                            "candidate_sources": "b3_listed_etfs",
                        }
                    ],
                    "coverage_diagnosis": {
                        "status": "active_directory_coverage_has_official_subset_parser_or_scope_gap",
                        "dataset_rows": 3,
                        "active_directory_match_rate": 66.67,
                        "active_directory_missing_dataset_rows": 1,
                        "open_review_rows": 1,
                        "closed_no_data_change_rows": 0,
                        "official_non_directory_gap_rows": 1,
                        "absent_from_all_b3_source_gap_rows": 0,
                        "official_subset_candidate_isin_rows": 0,
                        "official_subset_candidate_sector_rows": 1,
                        "rows_requiring_parser_or_scope_review": 1,
                        "rows_requiring_external_active_evidence": 0,
                        "data_change_authorized": False,
                        "root_cause": "official subset parser/scope review",
                        "source_gate": "No B3 ISIN, sector, category, name, symbol, or scope change is authorized.",
                    },
                    "top_b3_masterfile_gap_review_batches": [
                        {
                            "review_priority": "P2",
                            "b3_resolution_queue": "official_subset_category_requires_review",
                            "asset_type": "ETF",
                            "b3_gap_category": "unit_or_fund_line",
                            "source_presence": "present_only_in_non_exchange_directory_source",
                            "rows": 1,
                            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                            "verification_evidence_required": (
                                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                            ),
                            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                            "recommended_next_source": (
                                "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                            ),
                            "source_gate": (
                                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                            ),
                        }
                    ],
                    "top_open_b3_masterfile_review_batches": [
                        {
                            "review_priority": "P2",
                            "b3_resolution_queue": "official_subset_category_requires_review",
                            "asset_type": "ETF",
                            "b3_gap_category": "unit_or_fund_line",
                            "source_presence": "present_only_in_non_exchange_directory_source",
                            "rows": 1,
                            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                            "verification_evidence_required": (
                                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                            ),
                            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                            "recommended_next_source": (
                                "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                            ),
                            "source_gate": (
                                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                            ),
                        }
                    ],
                    "open_review_next_source_totals": {
                        "Official B3 subset source plus category taxonomy evidence with exact listing-key match.": 1
                    },
                    "open_review_evidence_path_totals": {
                        "official_b3_subset_category_apply_evidence": 1,
                    },
                    "source_gap_resolution_gate_totals": {
                        "apply_only_after_listing_keyed_category_review": 1,
                    },
                    "top_open_b3_masterfile_review_rows": [
                        {
                            "listing_key": "B3::AFOF11",
                            "ticker": "AFOF11",
                            "asset_type": "ETF",
                            "name": "Alianza Fofii Fundo De Investimento Imobiliario",
                            "b3_gap_category": "unit_or_fund_line",
                            "b3_resolution_queue": "official_subset_category_requires_review",
                            "review_priority": "P2",
                            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                            "verification_evidence_required": (
                                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                            ),
                            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                            "recommended_next_source": (
                                "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                            ),
                            "source_gate": (
                                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                            ),
                        }
                    ],
                }
            },
            "b3_etf_category_apply_report": {
                "summary": {
                    "rows": 2,
                    "written_updates": 1,
                    "decision_totals": {"apply": 1, "skip": 1},
                    "category_update_totals": {"Fixed Income": 1},
                }
            },
            "b3_residual_isin_review": {
                "summary": {
                    "rows": 1,
                    "review_bucket_totals": {"scope_review_before_identifier_fill": 1},
                    "review_priority_totals": {"P1": 1},
                    "apply_eligibility_totals": {"blocked_until_core_or_extended_scope_decision": 1},
                    "verification_evidence_required_totals": {"scope_decision_for_core_extended_or_exclude_before_identifier_fill": 1},
                    "b3_official_source_identifier_exposure": {
                        "b3_instruments_equities": {
                            "rows": 1,
                            "isin_present_rows": 1,
                            "isin_missing_rows": 0,
                        },
                        "b3_listed_etfs": {
                            "rows": 1,
                            "isin_present_rows": 0,
                            "isin_missing_rows": 1,
                        },
                    },
                }
            },
            "b3_residual_sector_review": {
                "summary": {
                    "rows": 2,
                    "review_bucket_totals": {"no_b3_classification_code_match_source_gap": 2},
                    "review_priority_totals": {"P3": 2},
                    "apply_eligibility_totals": {"source_gap_keep_blank_until_official_taxonomy_evidence": 2},
                    "verification_evidence_required_totals": {"stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match": 2},
                    "b3_code_shape_totals": {"alpha_b3_code": 1, "alphanumeric_b3_code": 1},
                    "alphanumeric_b3_code_rows": 1,
                    "alphanumeric_b3_code_examples": [
                        {"listing_key": "B3::A6OP3", "ticker": "A6OP3", "b3_code": "A6OP"}
                    ],
                }
            },
            "otc_scope_review": {
                "summary": {
                    "rows": 4,
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 1},
                    "source_gap_field_totals": {"missing_sector_stock": 1},
                    "source_gap_class_totals": {"otc_sector_source_gap": 1},
                    "drop_override_rows_still_present": 0,
                    "otc_review_decision_active_name_mismatch_rows": 0,
                    "otc_name_mismatch_unreviewed_active_rows": 2,
                    "otc_review_decision_resolution_totals": {
                        "pending_active_name_mismatch_review": 2,
                        "reviewed_decision_not_in_current_otc_scope": 1,
                    },
                    "otc_review_decision_current_listing_suppressed_rows": 0,
                    "otc_review_decision_not_current_scope_rows": 1,
                    "otc_review_decision_stale_rows": 1,
                    "review_bucket_totals": {"documented_otc_sector_source_gap": 1},
                    "review_bucket_asset_type_totals": {"documented_otc_sector_source_gap": {"Stock": 1}},
                    "review_bucket_metadata_gate_totals": {
                        "documented_otc_sector_source_gap": {
                            "reviewed_issuer_sector_source_required_keep_blank": 1
                        }
                    },
                    "review_priority_totals": {"P3": 1},
                    "review_strategy_totals": {"keep_sector_blank_until_reviewed_issuer_sector_source": 1},
                    "verification_evidence_required_totals": {
                        "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 1
                    },
                    "scope_apply_eligibility_totals": {"already_extended_no_scope_change_required": 1},
                    "otc_scope_completion": {
                        "status": "complete_extended_scope_no_core_candidates",
                        "rows": 4,
                        "extended_otc_rows": 4,
                        "otc_listing_scope_reason_rows": 4,
                        "already_extended_scope_decision_rows": 4,
                        "core_exclusion_candidate_rows": 0,
                        "unexpected_core_scope_rows": 0,
                        "blocked_scope_decision_rows": 0,
                        "scope_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "OTC scope is complete only when every current OTC row is extended/otc_listing and "
                            "no core or core-exclusion scope decision remains open; metadata still requires "
                            "listing-keyed evidence."
                        ),
                    },
                    "post_scope_metadata_backlog": {
                        "status": "metadata_review_backlog_open",
                        "rows": 1,
                        "metadata_enrichment_authorized": False,
                        "scope_blocked_rows": 0,
                        "source_gate": (
                            "Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, "
                            "issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed."
                        ),
                    },
                    "post_scope_metadata_backlog_bucket_totals": {"documented_otc_sector_source_gap": 1},
                    "post_scope_metadata_backlog_gate_totals": {
                        "reviewed_issuer_sector_source_required_keep_blank": 1
                    },
                    "post_scope_metadata_backlog_examples": [
                        {
                            "listing_key": "OTC::AAAA",
                            "ticker": "AAAA",
                            "asset_type": "Stock",
                            "name": "AAAA Corp",
                            "review_bucket": "documented_otc_sector_source_gap",
                            "quality_status": "source_gap",
                            "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
                            "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
                            "verification_evidence_required": (
                                "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
                            ),
                            "recommended_next_source": (
                                "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
                            ),
                            "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
                        }
                    ],
                    "metadata_enrichment_gate_totals": {"reviewed_issuer_sector_source_required_keep_blank": 1},
                }
            },
            "otc_name_mismatch_review": {
                "summary": {
                    "rows": 2,
                    "review_class_counts": {"probable_otc_rename_or_symbol_reuse": 1, "hold_unresolved": 1},
                    "review_priority_counts": {"high": 1, "held": 1},
                    "apply_eligibility_counts": {
                        "blocked_until_isin_anchored_issuer_history_review": 1,
                        "keep_current_until_stronger_issuer_history_source": 1,
                    },
                    "verification_evidence_required_counts": {
                        "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name": 1,
                        "stronger_official_or_reviewed_issuer_history_source_before_any_name_change": 1,
                    },
                }
            },
            "canada_residual_review": {
                "summary": {
                    "rows": 5,
                    "missing_isin_rows": 2,
                    "missing_figi_rows": 3,
                    "canada_identifier_backlog": {
                        "status": "figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps",
                        "rows": 5,
                        "scope_decision_required_rows": 1,
                        "official_isin_source_required_rows": 2,
                        "figi_blocked_until_isin_rows": 2,
                        "reviewed_openfigi_source_gap_rows": 2,
                        "openfigi_candidate_rows": 0,
                        "direct_identifier_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "Canadian identifier work remains blocked unless a listing-keyed official CSD, issuer, prospectus, "
                            "transfer-agent, OpenFIGI-by-ISIN, or reviewed stronger source proves the value; no symbol/name inference."
                        ),
                    },
                    "canada_identifier_backlog_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1,
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
                        "reviewed_openfigi_no_match_source_gap": 2,
                    },
                    "canada_identifier_backlog_evidence_required_totals": {
                        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
                        "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment": 1,
                        "stronger_figi_source_required_openfigi_no_match_reviewed": 2,
                    },
                    "core_exclusion_candidate_rows": 1,
                    "core_exclusion_candidate_exchange_totals": {"TSXV": 1},
                    "core_exclusion_candidate_asset_type_totals": {"Stock": 1},
                    "core_exclusion_candidate_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1
                    },
                    "core_exclusion_candidate_official_source_totals": {"tmx_listed_issuers": 1},
                    "core_exclusion_candidate_source_gap_class_totals": {
                        "capital_pool_or_halted_identifier_gap": 1
                    },
                    "exchange_totals": {"TSX": 3, "TSXV": 2},
                    "official_masterfile_source_totals": {"tmx_listed_issuers": 5},
                    "canada_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1,
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
                        "reviewed_openfigi_no_match_source_gap": 2,
                    },
                    "canada_resolution_queue_exchange_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {"TSXV": 1},
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 2},
                        "reviewed_openfigi_no_match_source_gap": {"TSX": 1, "TSXV": 1},
                    },
                    "canada_resolution_queue_asset_type_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {"Stock": 1},
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"Stock": 2},
                        "reviewed_openfigi_no_match_source_gap": {"ETF": 1, "Stock": 1},
                    },
                    "canada_resolution_queue_source_gap_class_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "capital_pool_or_halted_identifier_gap": 1
                        },
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                            "official_identifier_not_exposed_source_gap": 2
                        },
                        "reviewed_openfigi_no_match_source_gap": {"none": 2},
                    },
                    "canada_resolution_queue_official_source_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {"tmx_listed_issuers": 1},
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"tmx_listed_issuers": 2},
                        "reviewed_openfigi_no_match_source_gap": {"tmx_listed_issuers": 2},
                    },
                    "canada_resolution_queue_review_strategy_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "scope_review_before_canada_identifier_enrichment": 1
                        },
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                            "seek_official_canada_isin_source": 2
                        },
                        "reviewed_openfigi_no_match_source_gap": {
                            "keep_figi_blank_after_reviewed_openfigi_no_match": 2
                        },
                    },
                    "canada_resolution_queue_evidence_required_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "official_listing_scope_decision_for_core_extended_or_exclude": 1
                        },
                        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2
                        },
                        "reviewed_openfigi_no_match_source_gap": {
                            "stronger_figi_source_required_openfigi_no_match_reviewed": 2
                        },
                    },
                    "top_canada_resolution_review_batches": [
                        {
                            "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                            "exchange": "TSX",
                            "official_source": "tmx_listed_issuers",
                            "rows": 2,
                            "review_strategy": "seek_official_canada_isin_source",
                            "evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
                        }
                    ],
                    "source_gap_field_totals": {"missing_isin_primary": 2, "missing_sector_stock": 1},
                    "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 2},
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                    "openfigi_review_status_totals": {"accepted_source_gap_no_openfigi_match": 2},
                    "openfigi_review_decision_totals": {"no_openfigi_match": 2},
                    "isin_apply_eligibility_totals": {"keep_blank_until_official_isin_source": 2},
                    "figi_apply_eligibility_totals": {"blocked_until_isin_resolved": 2, "keep_blank_as_reviewed_openfigi_source_gap": 1},
                    "verification_evidence_required_totals": {
                        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
                        "stronger_figi_source_required_openfigi_no_match_reviewed": 1,
                    },
                }
            },
            "canada_figi_queue": {
                "summary": {
                    "rows": 0,
                    "apply_eligibility_totals": {"no_active_openfigi_probe_rows": 1},
                    "verification_evidence_required_totals": {
                        "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1
                    },
                }
            },
            "canada_figi_apply_report": {"summary": {"applied_rows": 4, "gap_rows_added": 1}},
            "asx_residual_review": {
                "summary": {
                    "rows": 6,
                    "field_totals": {"missing_isin_primary": 4, "missing_etf_category": 2},
                    "asset_type_totals": {"Stock": 4, "ETF": 2},
                    "core_exclusion_candidate_rows": 4,
                    "core_exclusion_candidate_field_totals": {"missing_isin_primary": 4},
                    "core_exclusion_candidate_asset_type_totals": {"Stock": 4},
                    "core_exclusion_candidate_gap_class_totals": {"official_current_directory_absent_identifier_gap": 4},
                    "core_exclusion_candidate_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 4
                    },
                    "core_exclusion_candidate_official_source_totals": {"asx_listed_companies": 4},
                    "core_exclusion_candidate_official_capability_totals": {
                        "masterfile_exposes_isin=false": 4,
                        "masterfile_exposes_sector=false": 4,
                        "masterfile_match=true": 4,
                    },
                    "gap_class_totals": {"official_current_directory_absent_identifier_gap": 4, "official_product_taxonomy_unavailable_gap": 2},
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 2, "core_exclusion_candidate": 4},
                    "asx_residual_backlog": {
                        "status": "review_only_scope_identifier_or_product_taxonomy_source_gaps",
                        "rows": 6,
                        "scope_decision_required_rows": 4,
                        "identity_review_required_rows": 0,
                        "official_identifier_source_required_rows": 0,
                        "official_product_taxonomy_required_rows": 2,
                        "official_isin_apply_candidate_rows": 0,
                        "direct_data_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
                            "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
                        ),
                    },
                    "asx_residual_backlog_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 4,
                        "missing_etf_category_requires_official_product_taxonomy": 2,
                    },
                    "asx_residual_backlog_evidence_required_totals": {
                        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2,
                        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4,
                    },
                    "asx_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 4,
                        "missing_etf_category_requires_official_product_taxonomy": 2,
                    },
                    "asx_resolution_queue_field_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {"missing_isin_primary": 4},
                        "missing_etf_category_requires_official_product_taxonomy": {"missing_etf_category": 2},
                    },
                    "asx_resolution_queue_gap_class_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "official_current_directory_absent_identifier_gap": 4
                        },
                        "missing_etf_category_requires_official_product_taxonomy": {
                            "official_product_taxonomy_unavailable_gap": 2
                        },
                    },
                    "asx_resolution_queue_official_source_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {"asx_listed_companies": 4},
                        "missing_etf_category_requires_official_product_taxonomy": {"asx_listed_companies": 2},
                    },
                    "asx_resolution_queue_review_strategy_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "scope_review_before_asx_identifier_enrichment": 4
                        },
                        "missing_etf_category_requires_official_product_taxonomy": {
                            "seek_official_or_reviewed_asx_product_taxonomy": 2
                        },
                    },
                    "asx_resolution_queue_evidence_required_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4
                        },
                        "missing_etf_category_requires_official_product_taxonomy": {
                            "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2
                        },
                    },
                    "asx_resolution_queue_official_capability_totals": {
                        "core_exclusion_candidate_identifier_scope_review": {
                            "masterfile_exposes_isin=false": 4,
                            "masterfile_exposes_sector=false": 4,
                            "masterfile_match=true": 4,
                        },
                        "missing_etf_category_requires_official_product_taxonomy": {
                            "masterfile_exposes_isin=false": 2,
                            "masterfile_exposes_sector=false": 2,
                            "masterfile_match=true": 2,
                        },
                    },
                    "top_asx_resolution_review_batches": [
                        {
                            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
                            "field": "missing_isin_primary",
                            "official_source": "asx_listed_companies",
                            "rows": 4,
                            "review_strategy": "scope_review_before_asx_identifier_enrichment",
                            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
                        }
                    ],
                    "residual_decision_totals": {"gap": 6},
                    "review_bucket_totals": {"scope_review_before_any_data_fill": 4},
                    "review_priority_totals": {"P1": 4},
                    "apply_eligibility_totals": {"blocked_until_core_or_extended_scope_decision": 4},
                    "verification_evidence_required_totals": {
                        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4
                    },
                    "asx_isin_probe_decision_totals": {"no_asx_match": 4},
                    "official_masterfile_match_totals": {"true": 6},
                    "official_masterfile_exposes_isin_totals": {"false": 6},
                    "official_masterfile_exposes_sector_totals": {"false": 6},
                    "official_masterfile_source_totals": {"asx_listed_companies": 6},
                }
            },
            "weak_sector_residual_review": {
                "summary": {
                    "rows": 7,
                    "exchanges": ["BK", "PSE"],
                    "exchange_totals": {"BK": 4, "PSE": 3},
                    "official_sector_candidate_rows": 2,
                    "official_sector_candidate_exchange_totals": {"PSE": 2},
                    "official_sector_candidate_value_totals": {"SERVICES": 2},
                    "scope_review_rows": 2,
                    "scope_review_exchange_totals": {"BK": 2},
                    "scope_review_gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 2},
                    "masterfile_without_sector_rows": 2,
                    "masterfile_without_sector_exchange_totals": {"BK": 2},
                    "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 7},
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 5, "core_exclusion_candidate": 2},
                    "weak_sector_backlog": {
                        "status": "venue_specific_review_queue_open",
                        "rows": 7,
                        "official_sector_candidate_rows": 2,
                        "scope_decision_required_rows": 2,
                        "masterfile_without_sector_rows": 2,
                        "venue_taxonomy_source_required_rows": 1,
                        "direct_sector_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "Weak-sector enrichment remains blocked unless venue-official masterfile, issuer, or reviewed taxonomy "
                            "evidence maps the exact listing to a canonical sector; no global symbol/name/peer inference is allowed."
                        ),
                    },
                    "weak_sector_backlog_queue_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": 2,
                        "official_masterfile_without_sector_source_gap": 2,
                        "official_sector_candidate_normalization_review": 2,
                        "venue_official_taxonomy_unavailable_source_gap": 1,
                    },
                    "weak_sector_backlog_evidence_required_totals": {
                        "new_or_restored_official_venue_industry_taxonomy_source": 1,
                        "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 2,
                        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2,
                        "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 2,
                    },
                    "weak_sector_resolution_queue_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": 2,
                        "official_masterfile_without_sector_source_gap": 2,
                        "official_sector_candidate_normalization_review": 2,
                        "venue_official_taxonomy_unavailable_source_gap": 1,
                    },
                    "weak_sector_resolution_queue_exchange_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": {"BK": 2},
                        "official_masterfile_without_sector_source_gap": {"BK": 2},
                        "official_sector_candidate_normalization_review": {"PSE": 2},
                        "venue_official_taxonomy_unavailable_source_gap": {"PSE": 1},
                    },
                    "weak_sector_resolution_queue_gap_class_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": {
                            "official_industry_taxonomy_unavailable_gap": 2
                        },
                        "official_masterfile_without_sector_source_gap": {
                            "official_industry_taxonomy_unavailable_gap": 2
                        },
                        "official_sector_candidate_normalization_review": {
                            "official_industry_taxonomy_unavailable_gap": 2
                        },
                        "venue_official_taxonomy_unavailable_source_gap": {
                            "official_industry_taxonomy_unavailable_gap": 1
                        },
                    },
                    "weak_sector_resolution_queue_official_source_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": {
                            "pse_listed_company_directory": 2
                        },
                        "official_masterfile_without_sector_source_gap": {
                            "pse_listed_company_directory": 2
                        },
                        "official_sector_candidate_normalization_review": {
                            "pse_listed_company_directory": 2
                        },
                        "venue_official_taxonomy_unavailable_source_gap": {
                            "pse_listed_company_directory": 1
                        },
                    },
                    "weak_sector_resolution_queue_review_strategy_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": {
                            "scope_review_before_weak_sector_enrichment": 2
                        },
                        "official_masterfile_without_sector_source_gap": {
                            "keep_blank_until_official_masterfile_or_issuer_sector_source": 2
                        },
                        "official_sector_candidate_normalization_review": {
                            "normalize_official_sector_candidate_before_apply": 2
                        },
                        "venue_official_taxonomy_unavailable_source_gap": {
                            "restore_or_add_venue_official_taxonomy_parser": 1
                        },
                    },
                    "weak_sector_resolution_queue_official_capability_totals": {
                        "core_exclusion_candidate_scope_review_before_sector_fill": {
                            "masterfile_exposes_sector=false": 2,
                            "masterfile_match=true": 2,
                        },
                        "official_masterfile_without_sector_source_gap": {
                            "masterfile_exposes_sector=false": 2,
                            "masterfile_match=true": 2,
                        },
                        "official_sector_candidate_normalization_review": {
                            "masterfile_exposes_sector=true": 2,
                            "masterfile_match=true": 2,
                        },
                        "venue_official_taxonomy_unavailable_source_gap": {
                            "masterfile_exposes_sector=false": 1,
                            "masterfile_match=true": 1,
                        },
                    },
                    "venue_backlog_exchange_queue_totals": {
                        "BK": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": 2,
                            "official_masterfile_without_sector_source_gap": 2,
                        },
                        "PSE": {
                            "official_sector_candidate_normalization_review": 2,
                            "venue_official_taxonomy_unavailable_source_gap": 1,
                        },
                    },
                    "venue_backlog_exchange_official_capability_totals": {
                        "BK": {"masterfile_exposes_sector=false": 4, "masterfile_match=true": 4},
                        "PSE": {
                            "masterfile_exposes_sector=false": 1,
                            "masterfile_exposes_sector=true": 2,
                            "masterfile_match=true": 3,
                        },
                    },
                    "top_venue_backlog_batches": [
                        {
                            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                            "exchange": "BK",
                            "official_source": "pse_listed_company_directory",
                            "rows": 2,
                            "review_strategy": "scope_review_before_weak_sector_enrichment",
                            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
                        }
                    ],
                    "top_weak_sector_resolution_review_batches": [
                        {
                            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                            "exchange": "BK",
                            "official_source": "pse_listed_company_directory",
                            "rows": 2,
                            "review_strategy": "scope_review_before_weak_sector_enrichment",
                            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
                        }
                    ],
                    "residual_decision_totals": {"official_sector_available_review_apply": 2, "accepted_source_gap_no_official_sector_taxonomy": 5},
                    "review_bucket_totals": {"official_sector_candidate_requires_normalization_gate": 2},
                    "review_priority_totals": {"P1": 2},
                    "apply_eligibility_totals": {"blocked_until_canonical_sector_normalization_and_listing_key_gate": 2},
                    "verification_evidence_required_totals": {
                        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2
                    },
                    "official_masterfile_match_totals": {"true": 7},
                    "official_masterfile_exposes_sector_totals": {"false": 5, "true": 2},
                    "official_masterfile_source_totals": {"pse_listed_company_directory": 7},
                    "official_sector_value_totals": {"SERVICES": 2},
                }
            },
            "ngx_official_sector_apply_report": {"summary": {"rows": 2, "written_updates": 0}},
            "masterfile_collision_review": {
                "summary": {
                    "rows": 8,
                    "decision_totals": {"same_isin": 5},
                    "review_bucket_totals": {"same_isin_exact_name_cross_listing_candidate": 2},
                    "review_priority_totals": {"P1": 2},
                    "collision_risk_flag_totals": {"same_isin_existing_listing": 2},
                    "identity_resolution_queue_totals": {"review_cross_listing_same_isin_exact_name": 2},
                    "identity_resolution_backlog": {
                        "status": "identity_resolution_review_queue_open",
                        "rows": 8,
                        "same_isin_exact_name_scope_review_rows": 2,
                        "same_isin_name_or_scope_reconciliation_rows": 0,
                        "distinct_official_isin_listing_add_review_rows": 0,
                        "asset_type_conflict_blocked_rows": 0,
                        "symbol_only_non_symbol_identity_required_rows": 0,
                        "direct_listing_add_allowed_rows": 0,
                        "symbol_only_resolution_authorized": False,
                        "source_gate": (
                            "Masterfile collision rows remain review queues only; listing additions, merges, renames, "
                            "or enrichments require official non-symbol identity evidence for the target listing."
                        ),
                    },
                    "identity_resolution_risk_flag_totals": {
                        "review_cross_listing_same_isin_exact_name": {"same_isin_existing_listing": 2}
                    },
                    "identity_resolution_exchange_totals": {"review_cross_listing_same_isin_exact_name": {"NYSE": 2}},
                    "identity_resolution_asset_type_totals": {"review_cross_listing_same_isin_exact_name": {"Stock": 2}},
                    "identity_resolution_official_source_totals": {
                        "review_cross_listing_same_isin_exact_name": {"sec_company_tickers_exchange": 2}
                    },
                    "identity_resolution_existing_exchange_pair_totals": {
                        "review_cross_listing_same_isin_exact_name": {"NYSE::NASDAQ": 2}
                    },
                    "identity_resolution_pair_review_strategy_totals": {
                        "review_cross_listing_same_isin_exact_name": {
                            "batch_review_same_isin_exact_name_cross_listing_scope": 2
                        }
                    },
                    "identity_resolution_review_strategy_totals": {
                        "review_cross_listing_same_isin_exact_name": {
                            "batch_review_same_isin_exact_name_cross_listing_scope": 2
                        }
                    },
                    "identity_resolution_evidence_required_totals": {
                        "review_cross_listing_same_isin_exact_name": {
                            "official_target_and_existing_exchange_directories_confirm_active_same_instrument": 2
                        }
                    },
                    "identity_resolution_identity_evidence_totals": {
                        "review_cross_listing_same_isin_exact_name": {
                            "asset_type_consistent": 2,
                            "exact_name_match": 2,
                            "official_isin": 2,
                            "same_isin_existing_listing": 2,
                        }
                    },
                    "top_identity_resolution_pair_review_batches": [
                        {
                            "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
                            "exchange_pair": "NYSE::NASDAQ",
                            "rows": 2,
                            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
                        }
                    ],
                    "same_isin_exact_name_scope_review_rows": 2,
                    "top_same_isin_exact_name_scope_review_batches": [
                        {
                            "exchange_pair": "NYSE::NASDAQ",
                            "official_source_key": "sec_company_tickers_exchange",
                            "official_asset_type": "Stock",
                            "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
                            "rows": 2,
                            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
                            "recommended_next_source": (
                                "Official active-listing directories for both exchanges in NYSE::NASDAQ."
                            ),
                            "source_gate": (
                                "Do not add or merge until both official exchange directories confirm the same active instrument."
                            ),
                        }
                    ],
                    "clearance_evidence_totals": {"official_target_exchange_listing_status_mic_name_instrument_type": 2},
                    "exchange_totals": {"NYSE": 5, "TSX": 3},
                    "official_asset_type_totals": {"Stock": 7, "ETF": 1},
                    "asset_type_mismatch_totals": {"false": 7, "true": 1},
                    "official_source_totals": {"tmx_listed_issuers": 3, "sec_company_tickers_exchange": 5},
                }
            },
            "symbol_changes_review": {
                "summary": {
                    "review_rows": 9,
                    "match_status_counts": {"old_symbol_present_new_symbol_missing": 2},
                    "symbol_change_workflow_queue_counts": {"review_verified_rename_or_delisting": 2},
                    "symbol_change_backlog": {
                        "status": "listing_keyed_symbol_change_review_queue_open",
                        "rows": 9,
                        "verified_rename_or_delisting_review_rows": 2,
                        "duplicate_or_cross_listing_review_rows": 0,
                        "already_reflected_audit_rows": 0,
                        "out_of_scope_collision_blocked_rows": 0,
                        "missing_source_scope_mapping_rows": 0,
                        "no_dataset_match_documentation_rows": 0,
                        "time_sensitive_review_rows": 1,
                        "direct_symbol_change_apply_allowed_rows": 0,
                        "secondary_feed_apply_authorized": False,
                        "source_gate": (
                            "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes "
                            "require listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
                        ),
                    },
                    "review_bucket_counts": {"action_required_possible_rename_or_delisting": 2},
                    "review_priority_counts": {"P1": 2},
                    "review_bucket_priorities": {"action_required_possible_rename_or_delisting": "P1"},
                    "recency_bucket_counts": {"recent_7d": 1, "older_than_90d": 1},
                    "review_priority_recency_counts": {"P1:recent_7d": 1, "P4:older_than_90d": 1},
                    "workflow_queue_recency_counts": {"review_verified_rename_or_delisting:recent_7d": 2},
                    "workflow_queue_priority_counts": {"review_verified_rename_or_delisting:P1": 2},
                    "workflow_queue_scope_counts": {
                        "review_verified_rename_or_delisting:matches_within_source_scope": 2
                    },
                    "workflow_queue_match_status_counts": {
                        "review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing": 2
                    },
                    "workflow_queue_source_hint_counts": {
                        "review_verified_rename_or_delisting": {"US_LISTED": 2}
                    },
                    "workflow_queue_source_confidence_counts": {
                        "review_verified_rename_or_delisting": {"secondary_review": 2}
                    },
                    "workflow_queue_listing_key_review_counts": {
                        "review_verified_rename_or_delisting": {"old_scoped_listing_key_only": 2}
                    },
                    "workflow_queue_review_strategy_counts": {
                        "review_verified_rename_or_delisting": {
                            "verify_rename_or_delisting_with_official_venue_or_issuer_evidence": 2
                        }
                    },
                    "top_symbol_change_workflow_batches": [
                        {
                            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                            "review_priority": "P1",
                            "recency_bucket": "recent_7d",
                            "exchange_scope_status": "matches_within_source_scope",
                            "rows": 2,
                            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                            "recommended_next_source": (
                                "Official exchange notice, issuer notice, or current exchange directory proving "
                                "old/new symbols for the same issuer."
                            ),
                            "source_gate": (
                                "Do not rename until official listing-keyed evidence proves old inactive and new "
                                "active for the same issuer."
                            ),
                        }
                    ],
                    "apply_eligibility_counts": {"requires_official_venue_confirmation": 1, "audit_only_no_apply": 1},
                    "symbol_change_apply_readiness_counts": {
                        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
                        "audit_only_no_canonical_change": 1,
                    },
                    "verification_evidence_required_counts": {"official_exchange_notice": 2},
                    "recommended_action_counts": {"review_possible_rename_or_delisting_in_source_scope": 2},
                    "time_sensitive_review_rows": 1,
                    "time_sensitive_workflow_queue_counts": {"review_verified_rename_or_delisting": 1},
                    "time_sensitive_recency_counts": {"recent_7d": 1},
                    "time_sensitive_apply_readiness_counts": {
                        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
                    },
                    "top_time_sensitive_symbol_change_batches": [
                        {
                            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                            "recency_bucket": "recent_7d",
                            "exchange_scope_status": "matches_within_source_scope",
                            "match_status": "old_symbol_present_new_symbol_missing",
                            "listing_key_review_status": "old_scoped_listing_key_only",
                            "rows": 1,
                            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                            "recommended_next_source": (
                                "Official exchange notice, issuer notice, or current exchange directory proving "
                                "old/new symbols for the same issuer."
                            ),
                            "source_gate": (
                                "Do not rename until official listing-keyed evidence proves old inactive and new "
                                "active for the same issuer."
                            ),
                        }
                    ],
                    "exchange_scope_status_counts": {"matches_within_source_scope": 8},
                }
            },
            "ohlcv_plausibility": {
                "_meta": {"rows": 10},
                "summary": {
                    "status_counts": {"not_checked": 10},
                    "sampling_coverage": {
                        "selected_rows": 10,
                        "report_rows": 10,
                        "checked_rows": 0,
                        "not_checked_rows": 10,
                        "skipped_not_checked_rows": 0,
                        "local_sample_rows": 0,
                        "yahoo_sample_rows": 0,
                        "warn_or_source_gap_signal_rows": 0,
                    },
                    "issue_counts": {"no_ohlcv_sample": 10},
                    "selection_bucket_counts": {"warn": 10},
                    "selection_bucket_exchange_counts": {"warn": {"OTC": 10}},
                    "selection_bucket_status_counts": {"warn": {"not_checked": 10}},
                    "review_bucket_counts": {"not_checked_entry_quality_warn_sample": 10},
                    "review_bucket_selection_bucket_counts": {"not_checked_entry_quality_warn_sample": {"warn": 10}},
                    "review_bucket_exchange_counts": {"not_checked_entry_quality_warn_sample": {"OTC": 10}},
                    "review_bucket_sampling_strategy_counts": {
                        "not_checked_entry_quality_warn_sample": {
                            "collect_ohlcv_sample_then_existing_entry_quality_review": 10
                        }
                    },
                    "review_bucket_sampling_readiness_counts": {
                        "not_checked_entry_quality_warn_sample": {"needs_ohlcv_sample": 10}
                    },
                    "ohlcv_sampling_backlog": {
                        "status": "sampling_queue_enabled_plausibility_only",
                        "selected_rows": 10,
                        "report_rows": 10,
                        "checked_rows": 0,
                        "not_checked_rows": 10,
                        "source_gap_cluster_sample_rows": 0,
                        "entry_quality_warn_sample_rows": 0,
                        "large_exchange_baseline_sample_rows": 0,
                        "warn_or_source_gap_signal_rows": 0,
                        "direct_canonical_data_change_allowed_rows": 0,
                        "plausibility_signal_only": True,
                        "source_gate": (
                            "OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, "
                            "listings, and symbols remain blocked until official listing-keyed review evidence is available."
                        ),
                    },
                    "top_ohlcv_sampling_batches": [
                        {
                            "review_bucket": "not_checked_entry_quality_warn_sample",
                            "selection_bucket": "warn",
                            "exchange": "OTC",
                            "plausibility_status": "not_checked",
                            "rows": 10,
                            "review_priority": "P2",
                            "sampling_strategy": "collect_ohlcv_sample_then_existing_entry_quality_review",
                            "evidence_required": "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
                            "recommended_next_source": (
                                "Collect a local or bounded-network OHLCV sample for OTC, then review the existing "
                                "entry-quality warning."
                            ),
                            "source_gate": (
                                "Sampling can prioritize review, but entry-quality changes still require the existing "
                                "official evidence gates."
                            ),
                        }
                    ],
                    "review_priority_counts": {"P2": 10},
                    "plausibility_use_counts": {"sampling_queue_for_existing_entry_quality_warn": 10},
                    "canonical_data_change_authorization_counts": {"no_canonical_data_change_authorized": 10},
                    "verification_evidence_required_counts": {"local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review": 10},
                    "source_gap_class_counts": {},
                    "top_flagged_exchanges": [{"exchange": "OTC", "not_checked": 10}],
                },
            },
            "ohlcv_warning_review": {
                "_meta": {"rows": 2},
                "summary": {
                    "review_rows": 2,
                    "ohlcv_review_bucket_counts": {
                        "official_listing_status_and_market_data_cross_check": 2,
                    },
                    "official_review_priority_counts": {"P2": 2},
                    "canonical_data_change_authorization_counts": {
                        "blocked_until_official_listing_keyed_review": 2,
                    },
                    "official_listing_review_status_counts": {
                        "pending_official_listing_status_review": 2,
                    },
                    "official_source_locator_status_counts": {
                        "verified_official_exchange_page_seeded": 2,
                    },
                },
            },
            "financialdata_isin_supplements_review": {
                "summary": {
                    "supplement_rows": 11,
                    "apply_eligibility_counts": {
                        "preserve_existing_reviewed_supplement_no_new_apply": 10,
                        "keep_absent_until_name_gated_official_isin_match": 1,
                    },
                    "verification_evidence_required_counts": {
                        "existing_reviewed_supplement_retained_with_original_official_source": 10,
                        "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 1,
                    },
                    "top_financialdata_supplement_review_batches": [
                        {
                            "review_priority": "P2",
                            "financialdata_review_queue": "keep_absent_until_official_name_gated_match",
                            "decision": "reject",
                            "reason": "no_name_gated_official_isin_match",
                            "financialdata_exchange": "NSE_IN",
                            "financialdata_review_scope": "global_expansion_candidate",
                            "official_source_key": "missing_official_source",
                            "review_strategy": (
                                "keep_absent_until_official_name_gated_identifier_evidence_exists"
                            ),
                            "apply_eligibility": "keep_absent_until_name_gated_official_isin_match",
                            "verification_evidence_required": (
                                "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing"
                            ),
                            "rows": 1,
                            "recommended_next_source": (
                                "Official active masterfile, registry, or issuer source matching FinancialData name and listing identity."
                            ),
                            "source_gate": (
                                "Keep absent until an official active source satisfies exact name/listing identity and ISIN gates."
                            ),
                        }
                    ],
                }
            },
            "improvement_baseline": {
                "_meta": {"generated_at": "2026-05-24T00:00:00Z"},
                "global_baseline": {"isin_coverage": 1},
                "campaign_baseline": {"b3": {}, "canada": {}},
            },
            "improvement_deltas": {
                "summary": {"changed_numeric_delta_rows": 0},
                "acceptance_delta_matrix": {
                    "source_gap_delta": {"baseline": 1, "current": 1, "delta": 0},
                    "warn_delta": {"baseline": 1, "current": 1, "delta": 0},
                    "quarantine_delta": {"baseline": 0, "current": 0, "delta": 0},
                },
                "campaign_deltas": {
                    "asx": {
                        "children": {
                            "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
                        }
                    },
                    "weak_sector": {
                        "children": {
                            "residual_rows": {"baseline": 7, "current": 7, "delta": 0},
                        }
                    }
                },
            },
        }
    )

    assert [campaign["priority"] for campaign in campaigns] == list(range(1, 11))
    assert all(campaign["review_policy"]["source_authority"].startswith("Official exchange") for campaign in campaigns)
    assert campaigns[0]["evidence"]["b3_missing_sector_residual_rows"] == 2
    assert campaigns[0]["evidence"]["b3_dataset_rows"] == 3
    assert campaigns[0]["evidence"]["b3_masterfile_missing_dataset_rows"] == 1
    assert campaigns[0]["evidence"]["b3_masterfile_dataset_match_rate"] == 66.67
    assert campaigns[0]["evidence"]["b3_official_any_source_missing_dataset_rows"] == 0
    assert campaigns[0]["evidence"]["b3_official_any_source_match_rate"] == 100.0
    assert campaigns[0]["evidence"]["b3_missing_source_presence_totals"] == {
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert campaigns[0]["evidence"]["b3_missing_examples"]["unit_or_fund_line"][0]["listing_key"] == "B3::AFOF11"
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_rows"] == 1
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_open_rows"] == 1
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_closed_no_data_change_rows"] == 0
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_source_presence_totals"] == {
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_open_source_presence_totals"] == {
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_resolution_queue_totals"] == {
        "official_subset_category_requires_review": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_open_resolution_queue_totals"] == {
        "official_subset_category_requires_review": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_open_next_source_totals"] == {
        "Official B3 subset source plus category taxonomy evidence with exact listing-key match.": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_open_evidence_path_totals"] == {
        "official_b3_subset_category_apply_evidence": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_source_gap_resolution_gate_totals"] == {
        "apply_only_after_listing_keyed_category_review": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_strategy_totals"] == {
        "review_official_subset_category_and_scope_before_apply_gate": 1,
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_resolution_queue_asset_type_totals"] == {
        "official_subset_category_requires_review": {"ETF": 1},
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_resolution_queue_gap_category_totals"] == {
        "official_subset_category_requires_review": {"unit_or_fund_line": 1},
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_candidate_source_totals"] == {"b3_listed_etfs": 1}
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_candidate_sector_present_rows"] == 1
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_candidate_isin_present_rows"] == 0
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_candidate_category_mismatch_rows"] == 1
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_official_subset_review_decision_totals"] == {
        "official_subset_category_mismatch_requires_apply_gate": 1
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_official_subset_closure_eligibility_totals"] == {
        "blocked_until_category_apply_gate": 1
    }
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_official_subset_closure_ready_rows"] == 0
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_candidate_category_mismatch_examples"][0][
        "listing_key"
    ] == "B3::AFOF11"
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_coverage_diagnosis"]["data_change_authorized"] is False
    assert campaigns[0]["evidence"]["b3_masterfile_gap_review_coverage_diagnosis"][
        "rows_requiring_parser_or_scope_review"
    ] == 1
    assert campaigns[0]["evidence"]["top_b3_masterfile_gap_review_batches"] == [
        {
            "review_priority": "P2",
            "b3_resolution_queue": "official_subset_category_requires_review",
            "asset_type": "ETF",
            "b3_gap_category": "unit_or_fund_line",
            "source_presence": "present_only_in_non_exchange_directory_source",
            "rows": 1,
            "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
            "verification_evidence_required": (
                "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
            ),
            "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
            "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
            "recommended_next_source": (
                "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
            ),
            "source_gate": (
                "Apply category only after official subset category, listing key, and current dataset category are reviewed."
            ),
        }
    ]
    assert campaigns[0]["evidence"]["top_open_b3_masterfile_review_batches"] == campaigns[0]["evidence"][
        "top_b3_masterfile_gap_review_batches"
    ]
    assert campaigns[0]["evidence"]["top_open_b3_masterfile_review_rows"][0]["listing_key"] == "B3::AFOF11"
    assert campaigns[0]["evidence"]["top_open_b3_masterfile_review_rows"][0]["source_gate"].startswith(
        "Apply category only after official subset category"
    )
    assert campaigns[0]["evidence"]["top_open_b3_masterfile_review_rows"][0]["b3_source_gap_evidence_path"] == (
        "official_b3_subset_category_apply_evidence"
    )
    assert campaigns[0]["evidence"]["b3_etf_category_apply_rows"] == 2
    assert campaigns[0]["evidence"]["b3_etf_category_written_updates"] == 1
    assert campaigns[0]["evidence"]["b3_etf_category_apply_decision_totals"] == {"apply": 1, "skip": 1}
    assert campaigns[0]["evidence"]["b3_etf_category_update_totals"] == {"Fixed Income": 1}
    assert campaigns[0]["evidence"]["isin_review_bucket_totals"] == {"scope_review_before_identifier_fill": 1}
    assert campaigns[0]["evidence"]["isin_apply_eligibility_totals"] == {"blocked_until_core_or_extended_scope_decision": 1}
    assert campaigns[0]["evidence"]["isin_verification_evidence_required_totals"] == {"scope_decision_for_core_extended_or_exclude_before_identifier_fill": 1}
    assert campaigns[0]["evidence"]["b3_isin_official_source_identifier_exposure"] == {
        "b3_instruments_equities": {
            "rows": 1,
            "isin_present_rows": 1,
            "isin_missing_rows": 0,
        },
        "b3_listed_etfs": {
            "rows": 1,
            "isin_present_rows": 0,
            "isin_missing_rows": 1,
        },
    }
    assert campaigns[0]["evidence"]["sector_review_priority_totals"] == {"P3": 2}
    assert campaigns[0]["evidence"]["sector_apply_eligibility_totals"] == {"source_gap_keep_blank_until_official_taxonomy_evidence": 2}
    assert campaigns[0]["evidence"]["sector_verification_evidence_required_totals"] == {"stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match": 2}
    assert campaigns[0]["evidence"]["sector_b3_code_shape_totals"] == {"alpha_b3_code": 1, "alphanumeric_b3_code": 1}
    assert campaigns[0]["evidence"]["sector_alphanumeric_b3_code_rows"] == 1
    assert campaigns[0]["evidence"]["sector_alphanumeric_b3_code_examples"][0]["b3_code"] == "A6OP"
    assert campaigns[0]["evidence"]["b3_residual_workstream_rows"] == {
        "masterfile_active_directory_gap": 1,
        "missing_isin_residual": 1,
        "missing_sector_residual": 2,
    }
    assert campaigns[0]["evidence"]["b3_residual_workstream_priority_totals"] == {
        "masterfile_active_directory_gap": {},
        "missing_isin_residual": {"P1": 1},
        "missing_sector_residual": {"P3": 2},
    }
    assert campaigns[0]["evidence"]["b3_residual_workstream_readiness_totals"] == {
        "masterfile_active_directory_gap": {"review_scope_or_parser_before_any_data_change": 1},
        "missing_isin_residual": {"blocked_until_core_or_extended_scope_decision": 1},
        "missing_sector_residual": {"source_gap_keep_blank_until_official_taxonomy_evidence": 2},
    }
    assert campaigns[0]["delta_evidence"]["status"] == "partial"
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_masterfile_missing_dataset_rows"] == 1
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_masterfile_gap_review_rows"] == 1
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_masterfile_gap_review_open_rows"] == 1
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_etf_category_written_updates"] == 1
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_official_any_source_missing_dataset_rows"] == 0
    assert campaigns[0]["delta_evidence"]["known_deltas"]["current_b3_sector_alphanumeric_code_rows"] == 1
    assert campaigns[0]["delta_evidence"]["known_deltas"]["campaign_start_coverage_snapshot"] == {}
    assert campaigns[0]["delta_evidence"]["known_deltas"]["source_gap_delta"] == {
        "baseline": 1,
        "current": 1,
        "delta": 0,
    }
    assert campaigns[0]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[1]["evidence"]["review_bucket_totals"] == {"documented_otc_sector_source_gap": 1}
    assert campaigns[1]["evidence"]["review_priority_totals"] == {"P3": 1}
    assert campaigns[1]["evidence"]["review_bucket_asset_type_totals"] == {
        "documented_otc_sector_source_gap": {"Stock": 1}
    }
    assert campaigns[1]["evidence"]["review_bucket_metadata_gate_totals"] == {
        "documented_otc_sector_source_gap": {"reviewed_issuer_sector_source_required_keep_blank": 1}
    }
    assert campaigns[1]["evidence"]["scope_apply_eligibility_totals"] == {"already_extended_no_scope_change_required": 1}
    assert campaigns[1]["evidence"]["otc_scope_completion"]["status"] == "complete_extended_scope_no_core_candidates"
    assert campaigns[1]["evidence"]["otc_scope_completion"]["metadata_enrichment_authorized"] is False
    assert campaigns[1]["evidence"]["post_scope_metadata_backlog"]["rows"] == 1
    assert campaigns[1]["evidence"]["post_scope_metadata_backlog"]["metadata_enrichment_authorized"] is False
    assert campaigns[1]["evidence"]["post_scope_metadata_backlog_bucket_totals"] == {
        "documented_otc_sector_source_gap": 1
    }
    assert campaigns[1]["evidence"]["post_scope_metadata_backlog_gate_totals"] == {
        "reviewed_issuer_sector_source_required_keep_blank": 1
    }
    assert campaigns[1]["evidence"]["metadata_enrichment_gate_totals"] == {"reviewed_issuer_sector_source_required_keep_blank": 1}
    assert campaigns[1]["evidence"]["scope_verification_evidence_required_totals"] == {
        "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 1
    }
    assert campaigns[1]["evidence"]["source_gap_field_totals"] == {"missing_sector_stock": 1}
    assert campaigns[1]["evidence"]["source_gap_class_totals"] == {"otc_sector_source_gap": 1}
    assert campaigns[1]["evidence"]["source_of_truth_outcome_totals"] == {"accepted_source_gap": 1}
    assert campaigns[1]["evidence"]["otc_review_decision_active_name_mismatch_rows"] == 0
    assert campaigns[1]["evidence"]["otc_name_mismatch_unreviewed_active_rows"] == 2
    assert campaigns[1]["evidence"]["otc_review_decision_resolution_totals"] == {
        "pending_active_name_mismatch_review": 2,
        "reviewed_decision_not_in_current_otc_scope": 1,
    }
    assert campaigns[1]["evidence"]["otc_review_decision_current_listing_suppressed_rows"] == 0
    assert campaigns[1]["evidence"]["otc_review_decision_not_current_scope_rows"] == 1
    assert campaigns[1]["evidence"]["otc_review_decision_stale_rows"] == 1
    assert campaigns[1]["evidence"]["name_mismatch_review_rows"] == 2
    assert campaigns[1]["evidence"]["name_mismatch_class_counts"] == {
        "probable_otc_rename_or_symbol_reuse": 1,
        "hold_unresolved": 1,
    }
    assert campaigns[1]["evidence"]["name_mismatch_apply_eligibility_counts"] == {
        "blocked_until_isin_anchored_issuer_history_review": 1,
        "keep_current_until_stronger_issuer_history_source": 1,
    }
    assert campaigns[1]["evidence"]["name_mismatch_verification_evidence_required_counts"] == {
        "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name": 1,
        "stronger_official_or_reviewed_issuer_history_source_before_any_name_change": 1,
    }
    assert campaigns[1]["delta_evidence"]["known_deltas"]["active_name_mismatch_review_rows"] == 2
    assert campaigns[1]["delta_evidence"]["known_deltas"]["otc_name_mismatch_unreviewed_active_rows"] == 2
    assert campaigns[1]["delta_evidence"]["known_deltas"]["otc_review_decision_current_listing_suppressed_rows"] == 0
    assert campaigns[1]["delta_evidence"]["known_deltas"]["otc_review_decision_not_current_scope_rows"] == 1
    assert campaigns[1]["delta_evidence"]["known_deltas"]["otc_review_decision_stale_rows"] == 1
    assert campaigns[1]["delta_evidence"]["known_deltas"]["campaign_start_scope_snapshot"] == {}
    assert campaigns[1]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[2]["delta_evidence"]["known_deltas"]["applied_figi_rows"] == 4
    assert campaigns[2]["evidence"]["reviewed_openfigi_source_gap_rows"] == 2
    assert campaigns[2]["evidence"]["canada_identifier_backlog"]["status"] == (
        "figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps"
    )
    assert campaigns[2]["evidence"]["canada_identifier_backlog"]["direct_identifier_apply_allowed_rows"] == 0
    assert campaigns[2]["evidence"]["canada_identifier_backlog"]["metadata_enrichment_authorized"] is False
    assert campaigns[2]["evidence"]["canada_identifier_backlog_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
        "reviewed_openfigi_no_match_source_gap": 2,
    }
    assert campaigns[2]["evidence"]["canada_identifier_backlog_evidence_required_totals"] == {
        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
        "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment": 1,
        "stronger_figi_source_required_openfigi_no_match_reviewed": 2,
    }
    assert campaigns[2]["evidence"]["exchange_totals"] == {"TSX": 3, "TSXV": 2}
    assert campaigns[2]["evidence"]["official_masterfile_source_totals"] == {"tmx_listed_issuers": 5}
    assert campaigns[2]["evidence"]["canada_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
        "reviewed_openfigi_no_match_source_gap": 2,
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_exchange_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"TSXV": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 2},
        "reviewed_openfigi_no_match_source_gap": {"TSX": 1, "TSXV": 1},
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_asset_type_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"Stock": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"Stock": 2},
        "reviewed_openfigi_no_match_source_gap": {"ETF": 1, "Stock": 1},
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_source_gap_class_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"capital_pool_or_halted_identifier_gap": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "official_identifier_not_exposed_source_gap": 2
        },
        "reviewed_openfigi_no_match_source_gap": {"none": 2},
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"tmx_listed_issuers": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"tmx_listed_issuers": 2},
        "reviewed_openfigi_no_match_source_gap": {"tmx_listed_issuers": 2},
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_review_before_canada_identifier_enrichment": 1
        },
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "seek_official_canada_isin_source": 2
        },
        "reviewed_openfigi_no_match_source_gap": {
            "keep_figi_blank_after_reviewed_openfigi_no_match": 2
        },
    }
    assert campaigns[2]["evidence"]["canada_resolution_queue_evidence_required_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "official_listing_scope_decision_for_core_extended_or_exclude": 1
        },
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2
        },
        "reviewed_openfigi_no_match_source_gap": {
            "stronger_figi_source_required_openfigi_no_match_reviewed": 2
        },
    }
    assert campaigns[2]["evidence"]["top_canada_resolution_review_batches"] == [
        {
            "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
            "exchange": "TSX",
            "official_source": "tmx_listed_issuers",
            "rows": 2,
            "review_strategy": "seek_official_canada_isin_source",
            "evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
        }
    ]
    assert campaigns[2]["evidence"]["source_gap_field_totals"] == {"missing_isin_primary": 2, "missing_sector_stock": 1}
    assert campaigns[2]["evidence"]["source_gap_class_totals"] == {"official_identifier_not_exposed_source_gap": 2}
    assert campaigns[2]["evidence"]["source_of_truth_outcome_totals"] == {"accepted_source_gap": 2}
    assert campaigns[2]["evidence"]["openfigi_review_decision_totals"] == {"no_openfigi_match": 2}
    assert campaigns[2]["evidence"]["isin_apply_eligibility_totals"] == {"keep_blank_until_official_isin_source": 2}
    assert campaigns[2]["evidence"]["figi_apply_eligibility_totals"] == {
        "blocked_until_isin_resolved": 2,
        "keep_blank_as_reviewed_openfigi_source_gap": 1,
    }
    assert campaigns[2]["evidence"]["verification_evidence_required_totals"] == {
        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
        "stronger_figi_source_required_openfigi_no_match_reviewed": 1,
    }
    assert campaigns[2]["evidence"]["figi_queue_apply_eligibility_totals"] == {"no_active_openfigi_probe_rows": 1}
    assert campaigns[2]["evidence"]["figi_queue_verification_evidence_required_totals"] == {
        "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1
    }
    assert campaigns[2]["delta_evidence"]["known_deltas"]["reviewed_openfigi_source_gap_rows"] == 2
    assert campaigns[2]["delta_evidence"]["known_deltas"]["canada_core_exclusion_candidate_rows"] == 1
    assert campaigns[2]["delta_evidence"]["known_deltas"]["isin_delta"] == {}
    assert campaigns[2]["delta_evidence"]["known_deltas"]["sector_category_delta"] == {
        "sector_delta": {},
        "category_delta": {},
    }
    assert campaigns[2]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_rows"] == 1
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_exchange_totals"] == {"TSXV": 1}
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_asset_type_totals"] == {"Stock": 1}
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1
    }
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_official_source_totals"] == {
        "tmx_listed_issuers": 1
    }
    assert campaigns[2]["evidence"]["canada_core_exclusion_candidate_source_gap_class_totals"] == {
        "capital_pool_or_halted_identifier_gap": 1
    }
    assert campaigns[2]["evidence"]["applied_figi_rows"] == 4
    assert campaigns[2]["evidence"]["openfigi_gap_rows_added"] == 1
    assert [artifact["path"] for artifact in campaigns[2]["artifacts"]] == [
        "data/reports/canada_residual_review.json",
        "data/reports/canada_figi_queue.json",
        "data/reports/canada_figi_apply_report.json",
    ]
    assert campaigns[3]["evidence"]["review_bucket_totals"] == {"scope_review_before_any_data_fill": 4}
    assert campaigns[3]["evidence"]["review_priority_totals"] == {"P1": 4}
    assert campaigns[3]["evidence"]["field_totals"] == {"missing_isin_primary": 4, "missing_etf_category": 2}
    assert campaigns[3]["evidence"]["asset_type_totals"] == {"Stock": 4, "ETF": 2}
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_rows"] == 4
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_field_totals"] == {"missing_isin_primary": 4}
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_asset_type_totals"] == {"Stock": 4}
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_gap_class_totals"] == {
        "official_current_directory_absent_identifier_gap": 4
    }
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 4
    }
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_official_source_totals"] == {
        "asx_listed_companies": 4
    }
    assert campaigns[3]["evidence"]["asx_core_exclusion_candidate_official_capability_totals"] == {
        "masterfile_exposes_isin=false": 4,
        "masterfile_exposes_sector=false": 4,
        "masterfile_match=true": 4,
    }
    assert campaigns[3]["evidence"]["gap_class_totals"] == {
        "official_current_directory_absent_identifier_gap": 4,
        "official_product_taxonomy_unavailable_gap": 2,
    }
    assert campaigns[3]["evidence"]["source_of_truth_outcome_totals"] == {
        "accepted_source_gap": 2,
        "core_exclusion_candidate": 4,
    }
    assert campaigns[3]["evidence"]["asx_residual_backlog"]["rows"] == 6
    assert campaigns[3]["evidence"]["asx_residual_backlog"]["direct_data_apply_allowed_rows"] == 0
    assert campaigns[3]["evidence"]["asx_residual_backlog"]["metadata_enrichment_authorized"] is False
    assert campaigns[3]["evidence"]["asx_residual_backlog_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 4,
        "missing_etf_category_requires_official_product_taxonomy": 2,
    }
    assert campaigns[3]["evidence"]["asx_residual_backlog_evidence_required_totals"] == {
        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2,
        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4,
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 4,
        "missing_etf_category_requires_official_product_taxonomy": 2,
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_field_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"missing_isin_primary": 4},
        "missing_etf_category_requires_official_product_taxonomy": {"missing_etf_category": 2},
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_gap_class_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "official_current_directory_absent_identifier_gap": 4
        },
        "missing_etf_category_requires_official_product_taxonomy": {
            "official_product_taxonomy_unavailable_gap": 2
        },
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"asx_listed_companies": 4},
        "missing_etf_category_requires_official_product_taxonomy": {"asx_listed_companies": 2},
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_review_before_asx_identifier_enrichment": 4
        },
        "missing_etf_category_requires_official_product_taxonomy": {
            "seek_official_or_reviewed_asx_product_taxonomy": 2
        },
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_evidence_required_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4
        },
        "missing_etf_category_requires_official_product_taxonomy": {
            "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2
        },
    }
    assert campaigns[3]["evidence"]["asx_resolution_queue_official_capability_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "masterfile_exposes_isin=false": 4,
            "masterfile_exposes_sector=false": 4,
            "masterfile_match=true": 4,
        },
        "missing_etf_category_requires_official_product_taxonomy": {
            "masterfile_exposes_isin=false": 2,
            "masterfile_exposes_sector=false": 2,
            "masterfile_match=true": 2,
        },
    }
    assert campaigns[3]["evidence"]["top_asx_resolution_review_batches"] == [
        {
            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "field": "missing_isin_primary",
            "official_source": "asx_listed_companies",
            "rows": 4,
            "review_strategy": "scope_review_before_asx_identifier_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
        }
    ]
    assert campaigns[3]["evidence"]["asx_isin_probe_decision_totals"] == {"no_asx_match": 4}
    assert campaigns[3]["evidence"]["official_masterfile_match_totals"] == {"true": 6}
    assert campaigns[3]["evidence"]["official_masterfile_exposes_isin_totals"] == {"false": 6}
    assert campaigns[3]["evidence"]["official_masterfile_exposes_sector_totals"] == {"false": 6}
    assert campaigns[3]["evidence"]["official_masterfile_source_totals"] == {"asx_listed_companies": 6}
    assert campaigns[3]["evidence"]["apply_eligibility_totals"] == {"blocked_until_core_or_extended_scope_decision": 4}
    assert campaigns[3]["evidence"]["verification_evidence_required_totals"] == {
        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 4
    }
    assert campaigns[3]["delta_evidence"]["known_deltas"]["asx_core_exclusion_candidate_rows"] == 4
    assert campaigns[3]["delta_evidence"]["known_deltas"]["campaign_start_residual_snapshot"] == {
        "baseline": 6,
        "current": 6,
        "delta": 0,
    }
    assert campaigns[3]["delta_evidence"]["known_deltas"]["source_gap_delta"] == {
        "baseline": 1,
        "current": 1,
        "delta": 0,
    }
    assert campaigns[3]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[3]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[4]["evidence"]["review_bucket_totals"] == {"official_sector_candidate_requires_normalization_gate": 2}
    assert campaigns[4]["evidence"]["review_priority_totals"] == {"P1": 2}
    assert campaigns[4]["evidence"]["exchanges"] == ["BK", "PSE"]
    assert campaigns[4]["evidence"]["exchange_totals"] == {"BK": 4, "PSE": 3}
    assert campaigns[4]["evidence"]["official_sector_candidate_rows"] == 2
    assert campaigns[4]["evidence"]["official_sector_candidate_exchange_totals"] == {"PSE": 2}
    assert campaigns[4]["evidence"]["official_sector_candidate_value_totals"] == {"SERVICES": 2}
    assert campaigns[4]["evidence"]["scope_review_rows"] == 2
    assert campaigns[4]["evidence"]["scope_review_exchange_totals"] == {"BK": 2}
    assert campaigns[4]["evidence"]["scope_review_gap_class_totals"] == {
        "official_industry_taxonomy_unavailable_gap": 2
    }
    assert campaigns[4]["evidence"]["masterfile_without_sector_rows"] == 2
    assert campaigns[4]["evidence"]["masterfile_without_sector_exchange_totals"] == {"BK": 2}
    assert campaigns[4]["evidence"]["gap_class_totals"] == {"official_industry_taxonomy_unavailable_gap": 7}
    assert campaigns[4]["evidence"]["source_of_truth_outcome_totals"] == {
        "accepted_source_gap": 5,
        "core_exclusion_candidate": 2,
    }
    assert campaigns[4]["evidence"]["weak_sector_backlog"]["rows"] == 7
    assert campaigns[4]["evidence"]["weak_sector_backlog"]["direct_sector_apply_allowed_rows"] == 0
    assert campaigns[4]["evidence"]["weak_sector_backlog"]["metadata_enrichment_authorized"] is False
    assert campaigns[4]["evidence"]["weak_sector_backlog_queue_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": 2,
        "official_masterfile_without_sector_source_gap": 2,
        "official_sector_candidate_normalization_review": 2,
        "venue_official_taxonomy_unavailable_source_gap": 1,
    }
    assert campaigns[4]["evidence"]["weak_sector_backlog_evidence_required_totals"] == {
        "new_or_restored_official_venue_industry_taxonomy_source": 1,
        "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 2,
        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2,
        "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 2,
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": 2,
        "official_masterfile_without_sector_source_gap": 2,
        "official_sector_candidate_normalization_review": 2,
        "venue_official_taxonomy_unavailable_source_gap": 1,
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_exchange_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {"BK": 2},
        "official_masterfile_without_sector_source_gap": {"BK": 2},
        "official_sector_candidate_normalization_review": {"PSE": 2},
        "venue_official_taxonomy_unavailable_source_gap": {"PSE": 1},
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_gap_class_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {
            "official_industry_taxonomy_unavailable_gap": 2
        },
        "official_masterfile_without_sector_source_gap": {
            "official_industry_taxonomy_unavailable_gap": 2
        },
        "official_sector_candidate_normalization_review": {
            "official_industry_taxonomy_unavailable_gap": 2
        },
        "venue_official_taxonomy_unavailable_source_gap": {
            "official_industry_taxonomy_unavailable_gap": 1
        },
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {"pse_listed_company_directory": 2},
        "official_masterfile_without_sector_source_gap": {"pse_listed_company_directory": 2},
        "official_sector_candidate_normalization_review": {"pse_listed_company_directory": 2},
        "venue_official_taxonomy_unavailable_source_gap": {"pse_listed_company_directory": 1},
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {
            "scope_review_before_weak_sector_enrichment": 2
        },
        "official_masterfile_without_sector_source_gap": {
            "keep_blank_until_official_masterfile_or_issuer_sector_source": 2
        },
        "official_sector_candidate_normalization_review": {
            "normalize_official_sector_candidate_before_apply": 2
        },
        "venue_official_taxonomy_unavailable_source_gap": {
            "restore_or_add_venue_official_taxonomy_parser": 1
        },
    }
    assert campaigns[4]["evidence"]["weak_sector_resolution_queue_official_capability_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {
            "masterfile_exposes_sector=false": 2,
            "masterfile_match=true": 2,
        },
        "official_masterfile_without_sector_source_gap": {
            "masterfile_exposes_sector=false": 2,
            "masterfile_match=true": 2,
        },
        "official_sector_candidate_normalization_review": {
            "masterfile_exposes_sector=true": 2,
            "masterfile_match=true": 2,
        },
        "venue_official_taxonomy_unavailable_source_gap": {
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
    }
    assert campaigns[4]["evidence"]["venue_backlog_exchange_queue_totals"] == {
        "BK": {
            "core_exclusion_candidate_scope_review_before_sector_fill": 2,
            "official_masterfile_without_sector_source_gap": 2,
        },
        "PSE": {
            "official_sector_candidate_normalization_review": 2,
            "venue_official_taxonomy_unavailable_source_gap": 1,
        },
    }
    assert campaigns[4]["evidence"]["venue_backlog_exchange_official_capability_totals"] == {
        "BK": {"masterfile_exposes_sector=false": 4, "masterfile_match=true": 4},
        "PSE": {
            "masterfile_exposes_sector=false": 1,
            "masterfile_exposes_sector=true": 2,
            "masterfile_match=true": 3,
        },
    }
    assert campaigns[4]["evidence"]["top_venue_backlog_batches"] == [
        {
            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
            "exchange": "BK",
            "official_source": "pse_listed_company_directory",
            "rows": 2,
            "review_strategy": "scope_review_before_weak_sector_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
        }
    ]
    assert campaigns[4]["evidence"]["top_weak_sector_resolution_review_batches"] == [
        {
            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
            "exchange": "BK",
            "official_source": "pse_listed_company_directory",
            "rows": 2,
            "review_strategy": "scope_review_before_weak_sector_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
        }
    ]
    assert campaigns[4]["evidence"]["residual_decision_totals"] == {
        "official_sector_available_review_apply": 2,
        "accepted_source_gap_no_official_sector_taxonomy": 5,
    }
    assert campaigns[4]["evidence"]["apply_eligibility_totals"] == {
        "blocked_until_canonical_sector_normalization_and_listing_key_gate": 2
    }
    assert campaigns[4]["evidence"]["verification_evidence_required_totals"] == {
        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2
    }
    assert campaigns[4]["evidence"]["official_masterfile_match_totals"] == {"true": 7}
    assert campaigns[4]["evidence"]["official_masterfile_exposes_sector_totals"] == {"false": 5, "true": 2}
    assert campaigns[4]["evidence"]["official_masterfile_source_totals"] == {"pse_listed_company_directory": 7}
    assert campaigns[4]["evidence"]["official_sector_value_totals"] == {"SERVICES": 2}
    assert campaigns[4]["evidence"]["ngx_written_updates"] == 0
    assert campaigns[4]["delta_evidence"]["known_deltas"]["official_sector_candidate_rows"] == 2
    assert campaigns[4]["delta_evidence"]["known_deltas"]["scope_review_rows"] == 2
    assert campaigns[4]["delta_evidence"]["known_deltas"]["masterfile_without_sector_rows"] == 2
    assert campaigns[4]["delta_evidence"]["known_deltas"]["campaign_start_sector_coverage_snapshot"] == {
        "baseline": 7,
        "current": 7,
        "delta": 0,
    }
    assert campaigns[4]["delta_evidence"]["known_deltas"]["source_gap_delta"] == {
        "baseline": 1,
        "current": 1,
        "delta": 0,
    }
    assert campaigns[4]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[4]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[5]["evidence"]["collision_review_rows"] == 8
    assert campaigns[5]["evidence"]["review_bucket_totals"] == {"same_isin_exact_name_cross_listing_candidate": 2}
    assert campaigns[5]["evidence"]["review_priority_totals"] == {"P1": 2}
    assert campaigns[5]["evidence"]["collision_risk_flag_totals"] == {"same_isin_existing_listing": 2}
    assert campaigns[5]["evidence"]["identity_resolution_backlog"] == {
        "status": "identity_resolution_review_queue_open",
        "rows": 8,
        "same_isin_exact_name_scope_review_rows": 2,
        "same_isin_name_or_scope_reconciliation_rows": 0,
        "distinct_official_isin_listing_add_review_rows": 0,
        "asset_type_conflict_blocked_rows": 0,
        "symbol_only_non_symbol_identity_required_rows": 0,
        "direct_listing_add_allowed_rows": 0,
        "symbol_only_resolution_authorized": False,
        "source_gate": (
            "Masterfile collision rows remain review queues only; listing additions, merges, renames, "
            "or enrichments require official non-symbol identity evidence for the target listing."
        ),
    }
    assert campaigns[5]["evidence"]["identity_resolution_risk_flag_totals"] == {
        "review_cross_listing_same_isin_exact_name": {"same_isin_existing_listing": 2}
    }
    assert campaigns[5]["evidence"]["identity_resolution_official_source_totals"] == {
        "review_cross_listing_same_isin_exact_name": {"sec_company_tickers_exchange": 2}
    }
    assert campaigns[5]["evidence"]["identity_resolution_existing_exchange_pair_totals"] == {
        "review_cross_listing_same_isin_exact_name": {"NYSE::NASDAQ": 2}
    }
    assert campaigns[5]["evidence"]["identity_resolution_pair_review_strategy_totals"] == {
        "review_cross_listing_same_isin_exact_name": {
            "batch_review_same_isin_exact_name_cross_listing_scope": 2
        }
    }
    assert campaigns[5]["evidence"]["identity_resolution_review_strategy_totals"] == {
        "review_cross_listing_same_isin_exact_name": {
            "batch_review_same_isin_exact_name_cross_listing_scope": 2
        }
    }
    assert campaigns[5]["evidence"]["identity_resolution_evidence_required_totals"] == {
        "review_cross_listing_same_isin_exact_name": {
            "official_target_and_existing_exchange_directories_confirm_active_same_instrument": 2
        }
    }
    assert campaigns[5]["evidence"]["identity_resolution_identity_evidence_totals"] == {
        "review_cross_listing_same_isin_exact_name": {
            "asset_type_consistent": 2,
            "exact_name_match": 2,
            "official_isin": 2,
            "same_isin_existing_listing": 2,
        }
    }
    assert campaigns[5]["evidence"]["top_identity_resolution_pair_review_batches"] == [
        {
            "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
            "exchange_pair": "NYSE::NASDAQ",
            "rows": 2,
            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
        }
    ]
    assert campaigns[5]["evidence"]["same_isin_exact_name_scope_review_rows"] == 2
    assert campaigns[5]["evidence"]["top_same_isin_exact_name_scope_review_batches"] == [
        {
            "exchange_pair": "NYSE::NASDAQ",
            "official_source_key": "sec_company_tickers_exchange",
            "official_asset_type": "Stock",
            "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
            "rows": 2,
            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
            "recommended_next_source": (
                "Official active-listing directories for both exchanges in NYSE::NASDAQ."
            ),
            "source_gate": (
                "Do not add or merge until both official exchange directories confirm the same active instrument."
            ),
        }
    ]
    assert campaigns[5]["evidence"]["clearance_evidence_totals"] == {"official_target_exchange_listing_status_mic_name_instrument_type": 2}
    assert campaigns[5]["evidence"]["exchange_totals"] == {"NYSE": 5, "TSX": 3}
    assert campaigns[5]["evidence"]["official_asset_type_totals"] == {"Stock": 7, "ETF": 1}
    assert campaigns[5]["evidence"]["asset_type_mismatch_totals"] == {"false": 7, "true": 1}
    assert campaigns[5]["evidence"]["official_source_totals"] == {
        "tmx_listed_issuers": 3,
        "sec_company_tickers_exchange": 5,
    }
    assert campaigns[5]["evidence"]["asset_type_mismatches"] == 1
    assert campaigns[5]["delta_evidence"]["status"] == "review_queue_only_no_data_apply"
    assert campaigns[5]["delta_evidence"]["known_deltas"]["current_collision_review_rows"] == 8
    assert campaigns[5]["delta_evidence"]["known_deltas"]["collision_resolution_delta"] == 0
    assert campaigns[5]["delta_evidence"]["known_deltas"]["listing_addition_delta"] == 0
    assert campaigns[5]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[5]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[6]["evidence"]["review_bucket_counts"] == {"action_required_possible_rename_or_delisting": 2}
    assert campaigns[6]["evidence"]["match_status_counts"] == {"old_symbol_present_new_symbol_missing": 2}
    assert campaigns[6]["evidence"]["symbol_change_workflow_queue_counts"] == {
        "review_verified_rename_or_delisting": 2
    }
    assert campaigns[6]["evidence"]["symbol_change_backlog"] == {
        "status": "listing_keyed_symbol_change_review_queue_open",
        "rows": 9,
        "verified_rename_or_delisting_review_rows": 2,
        "duplicate_or_cross_listing_review_rows": 0,
        "already_reflected_audit_rows": 0,
        "out_of_scope_collision_blocked_rows": 0,
        "missing_source_scope_mapping_rows": 0,
        "no_dataset_match_documentation_rows": 0,
        "time_sensitive_review_rows": 1,
        "direct_symbol_change_apply_allowed_rows": 0,
        "secondary_feed_apply_authorized": False,
        "source_gate": (
            "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes require "
            "listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
        ),
    }
    assert campaigns[6]["evidence"]["review_priority_counts"] == {"P1": 2}
    assert campaigns[6]["evidence"]["review_bucket_priorities"] == {"action_required_possible_rename_or_delisting": "P1"}
    assert campaigns[6]["evidence"]["recency_bucket_counts"] == {"recent_7d": 1, "older_than_90d": 1}
    assert campaigns[6]["evidence"]["review_priority_recency_counts"] == {"P1:recent_7d": 1, "P4:older_than_90d": 1}
    assert campaigns[6]["evidence"]["workflow_queue_recency_counts"] == {
        "review_verified_rename_or_delisting:recent_7d": 2
    }
    assert campaigns[6]["evidence"]["workflow_queue_priority_counts"] == {
        "review_verified_rename_or_delisting:P1": 2
    }
    assert campaigns[6]["evidence"]["workflow_queue_scope_counts"] == {
        "review_verified_rename_or_delisting:matches_within_source_scope": 2
    }
    assert campaigns[6]["evidence"]["workflow_queue_match_status_counts"] == {
        "review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing": 2
    }
    assert campaigns[6]["evidence"]["workflow_queue_source_hint_counts"] == {
        "review_verified_rename_or_delisting": {"US_LISTED": 2}
    }
    assert campaigns[6]["evidence"]["workflow_queue_source_confidence_counts"] == {
        "review_verified_rename_or_delisting": {"secondary_review": 2}
    }
    assert campaigns[6]["evidence"]["workflow_queue_listing_key_review_counts"] == {
        "review_verified_rename_or_delisting": {"old_scoped_listing_key_only": 2}
    }
    assert campaigns[6]["evidence"]["workflow_queue_review_strategy_counts"] == {
        "review_verified_rename_or_delisting": {
            "verify_rename_or_delisting_with_official_venue_or_issuer_evidence": 2
        }
    }
    assert campaigns[6]["evidence"]["top_symbol_change_workflow_batches"] == [
        {
            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
            "review_priority": "P1",
            "recency_bucket": "recent_7d",
            "exchange_scope_status": "matches_within_source_scope",
            "rows": 2,
            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
            "recommended_next_source": (
                "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols "
                "for the same issuer."
            ),
            "source_gate": (
                "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
            ),
        }
    ]
    assert campaigns[6]["evidence"]["apply_eligibility_counts"] == {"requires_official_venue_confirmation": 1, "audit_only_no_apply": 1}
    assert campaigns[6]["evidence"]["symbol_change_apply_readiness_counts"] == {
        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
        "audit_only_no_canonical_change": 1,
    }
    assert campaigns[6]["evidence"]["verification_evidence_required_counts"] == {"official_exchange_notice": 2}
    assert campaigns[6]["evidence"]["recommended_action_counts"] == {
        "review_possible_rename_or_delisting_in_source_scope": 2
    }
    assert campaigns[6]["evidence"]["time_sensitive_review_rows"] == 1
    assert campaigns[6]["evidence"]["time_sensitive_workflow_queue_counts"] == {
        "review_verified_rename_or_delisting": 1
    }
    assert campaigns[6]["evidence"]["time_sensitive_recency_counts"] == {"recent_7d": 1}
    assert campaigns[6]["evidence"]["time_sensitive_apply_readiness_counts"] == {
        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
    }
    assert campaigns[6]["evidence"]["top_time_sensitive_symbol_change_batches"] == [
        {
            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
            "recency_bucket": "recent_7d",
            "exchange_scope_status": "matches_within_source_scope",
            "match_status": "old_symbol_present_new_symbol_missing",
            "listing_key_review_status": "old_scoped_listing_key_only",
            "rows": 1,
            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
            "recommended_next_source": (
                "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols "
                "for the same issuer."
            ),
            "source_gate": (
                "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
            ),
        }
    ]
    assert campaigns[6]["delta_evidence"]["known_deltas"]["source_scope_outside_collision_rows"] == 0
    assert campaigns[6]["delta_evidence"]["known_deltas"]["verified_rename_delta"] == 0
    assert campaigns[6]["delta_evidence"]["known_deltas"]["duplicate_resolution_delta"] == 0
    assert campaigns[6]["delta_evidence"]["known_deltas"]["warn_quarantine_delta"] == {
        "warn_delta": 0,
        "quarantine_delta": 0,
    }
    assert campaigns[6]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[7]["evidence"]["review_bucket_counts"] == {"not_checked_entry_quality_warn_sample": 10}
    assert campaigns[7]["evidence"]["status_counts"] == {"not_checked": 10}
    assert campaigns[7]["evidence"]["selected_sample_rows"] == 10
    assert campaigns[7]["evidence"]["checked_sample_rows"] == 0
    assert campaigns[7]["evidence"]["not_checked_sample_rows"] == 10
    assert campaigns[7]["evidence"]["sampling_coverage"]["report_rows"] == 10
    assert campaigns[7]["evidence"]["ohlcv_sampling_backlog"] == {
        "status": "sampling_queue_enabled_plausibility_only",
        "selected_rows": 10,
        "report_rows": 10,
        "checked_rows": 0,
        "not_checked_rows": 10,
        "source_gap_cluster_sample_rows": 0,
        "entry_quality_warn_sample_rows": 0,
        "large_exchange_baseline_sample_rows": 0,
        "warn_or_source_gap_signal_rows": 0,
        "direct_canonical_data_change_allowed_rows": 0,
        "plausibility_signal_only": True,
        "source_gate": (
            "OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, "
            "and symbols remain blocked until official listing-keyed review evidence is available."
        ),
    }
    assert campaigns[7]["delta_evidence"]["known_deltas"]["selected_sample_rows"] == 10
    assert campaigns[7]["delta_evidence"]["known_deltas"]["checked_sample_rows"] == 0
    assert campaigns[7]["delta_evidence"]["known_deltas"]["warn_or_source_gap_signal_rows"] == 0
    assert campaigns[7]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[7]["evidence"]["issue_counts"] == {"no_ohlcv_sample": 10}
    assert campaigns[7]["evidence"]["selection_bucket_counts"] == {"warn": 10}
    assert campaigns[7]["evidence"]["selection_bucket_exchange_counts"] == {"warn": {"OTC": 10}}
    assert campaigns[7]["evidence"]["selection_bucket_status_counts"] == {"warn": {"not_checked": 10}}
    assert campaigns[7]["evidence"]["review_bucket_selection_bucket_counts"] == {
        "not_checked_entry_quality_warn_sample": {"warn": 10}
    }
    assert campaigns[7]["evidence"]["review_bucket_exchange_counts"] == {
        "not_checked_entry_quality_warn_sample": {"OTC": 10}
    }
    assert campaigns[7]["evidence"]["review_bucket_sampling_strategy_counts"] == {
        "not_checked_entry_quality_warn_sample": {
            "collect_ohlcv_sample_then_existing_entry_quality_review": 10
        }
    }
    assert campaigns[7]["evidence"]["review_bucket_sampling_readiness_counts"] == {
        "not_checked_entry_quality_warn_sample": {"needs_ohlcv_sample": 10}
    }
    assert campaigns[7]["evidence"]["top_ohlcv_sampling_batches"] == [
        {
            "review_bucket": "not_checked_entry_quality_warn_sample",
            "selection_bucket": "warn",
            "exchange": "OTC",
            "plausibility_status": "not_checked",
            "rows": 10,
            "review_priority": "P2",
            "sampling_strategy": "collect_ohlcv_sample_then_existing_entry_quality_review",
            "evidence_required": "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
            "recommended_next_source": (
                "Collect a local or bounded-network OHLCV sample for OTC, then review the existing entry-quality warning."
            ),
            "source_gate": (
                "Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates."
            ),
        }
    ]
    assert campaigns[7]["evidence"]["review_priority_counts"] == {"P2": 10}
    assert campaigns[7]["evidence"]["plausibility_use_counts"] == {"sampling_queue_for_existing_entry_quality_warn": 10}
    assert campaigns[7]["evidence"]["canonical_data_change_authorization_counts"] == {
        "no_canonical_data_change_authorized": 10
    }
    assert campaigns[7]["evidence"]["verification_evidence_required_counts"] == {"local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review": 10}
    assert campaigns[7]["evidence"]["source_gap_class_counts"] == {}
    assert campaigns[7]["evidence"]["top_flagged_exchanges"] == [{"exchange": "OTC", "not_checked": 10}]
    assert campaigns[7]["evidence"]["ohlcv_warning_review_rows"] == 2
    assert campaigns[7]["evidence"]["ohlcv_warning_review_bucket_counts"] == {
        "official_listing_status_and_market_data_cross_check": 2
    }
    assert campaigns[7]["evidence"]["ohlcv_warning_review_priority_counts"] == {"P2": 2}
    assert campaigns[7]["evidence"]["ohlcv_warning_review_authorization_counts"] == {
        "blocked_until_official_listing_keyed_review": 2
    }
    assert campaigns[7]["evidence"]["ohlcv_warning_review_status_counts"] == {
        "pending_official_listing_status_review": 2
    }
    assert campaigns[7]["evidence"]["ohlcv_warning_review_source_locator_counts"] == {
        "verified_official_exchange_page_seeded": 2
    }
    assert campaigns[8]["evidence"]["source_freshness_status_totals"] == {"fresh": 1, "old": 1}
    freshness_by_key = {row["key"]: row for row in campaigns[8]["evidence"]["freshness_snapshot"]}
    assert {"tickers", "masterfiles", "identifiers", "symbol_changes", "source_gap_classification"} <= set(freshness_by_key)
    assert freshness_by_key["tickers"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert freshness_by_key["tickers"]["source_type"] == "dataset"
    assert freshness_by_key["symbol_changes"]["source_type"] == "workflow_report"
    assert campaigns[8]["evidence"]["freshness_snapshot_age_bucket_totals"] == {
        "unknown_age": 8,
    }
    assert campaigns[8]["evidence"]["top_freshness_snapshot_ages"] == []
    assert campaigns[8]["evidence"]["source_age_bucket_totals"] == {"age_0_48h": 1, "age_168_336h": 1}
    assert campaigns[8]["evidence"]["source_refresh_priority_totals"] == {"P1": 1, "P4": 1}
    assert campaigns[8]["evidence"]["source_refresh_queue_totals"] == {
        "fresh_no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert campaigns[8]["evidence"]["source_refresh_queue_scope_totals"] == {
        "fresh_no_refresh_needed": {"exchange_directory": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"exchange_directory": 1},
    }
    assert campaigns[8]["evidence"]["source_refresh_queue_mode_totals"] == {
        "fresh_no_refresh_needed": {"network": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 1},
    }
    assert campaigns[8]["evidence"]["source_refresh_queue_priority_totals"] == {
        "fresh_no_refresh_needed": {"P4": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 1},
    }
    assert campaigns[8]["evidence"]["source_refresh_queue_age_bucket_totals"] == {
        "fresh_no_refresh_needed": {"age_0_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"age_168_336h": 1},
    }
    assert campaigns[8]["evidence"]["source_refresh_action_totals"] == {
        "no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert campaigns[8]["delta_evidence"]["known_deltas"]["source_refresh_action_totals"] == {
        "no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert campaigns[8]["delta_evidence"]["known_deltas"]["freshness_snapshot_age_bucket_totals"] == {
        "unknown_age": 8,
    }
    assert campaigns[8]["delta_evidence"]["known_deltas"]["top_freshness_snapshot_ages"] == campaigns[8]["evidence"][
        "top_freshness_snapshot_ages"
    ]
    assert campaigns[8]["delta_evidence"]["missing_deltas"] == []
    assert campaigns[8]["evidence"]["source_refresh_queue_review_strategy_totals"] == {
        "fresh_no_refresh_needed": {"no_refresh_required": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "refresh_official_exchange_directory_before_identity_or_collision_work": 1
        },
    }
    assert campaigns[8]["evidence"]["source_refresh_queue_evidence_required_totals"] == {
        "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1
        },
    }
    assert campaigns[8]["delta_evidence"]["known_deltas"]["source_refresh_queue_evidence_required_totals"] == {
        "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1
        },
    }
    assert campaigns[8]["evidence"]["top_source_refresh_batches"] == [
        {
            "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
            "reference_scope": "exchange_directory",
            "mode": "cache",
            "refresh_priority": "P1",
            "source_count": 1,
            "total_rows": 50,
            "max_age_hours": 200.0,
            "review_strategy": "refresh_official_exchange_directory_before_identity_or_collision_work",
            "evidence_required": "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
            "recommended_next_source": (
                "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
            ),
            "source_gate": (
                "Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated."
            ),
        }
    ]
    assert campaigns[8]["evidence"]["old_official_exchange_directory_count"] == 1
    assert campaigns[8]["evidence"]["top_old_official_exchange_directories"] == [
        {"key": "old_source", "provider": "Official", "age_hours": 200.0}
    ]
    assert campaigns[8]["evidence"]["source_gap_class_totals"] == {"official_gap": 2}
    assert campaigns[8]["evidence"]["top_source_gap_review_batches"] == [
        {
            "field": "missing_sector_stock",
            "gap_class": "official_gap",
            "exchange": "B3",
            "rows": 2,
            "recommended_next_source": "Official taxonomy source.",
            "source_gate": "Exact listing-keyed taxonomy evidence.",
        }
    ]
    assert campaigns[8]["delta_evidence"]["known_deltas"]["top_source_gap_review_batches"] == [
        {
            "field": "missing_sector_stock",
            "gap_class": "official_gap",
            "exchange": "B3",
            "rows": 2,
            "recommended_next_source": "Official taxonomy source.",
            "source_gate": "Exact listing-keyed taxonomy evidence.",
        }
    ]
    assert campaigns[8]["evidence"]["financialdata_supplement_rows"] == 11
    assert campaigns[8]["evidence"]["financialdata_apply_eligibility_counts"] == {
        "preserve_existing_reviewed_supplement_no_new_apply": 10,
        "keep_absent_until_name_gated_official_isin_match": 1,
    }
    assert campaigns[8]["evidence"]["financialdata_verification_evidence_required_counts"] == {
        "existing_reviewed_supplement_retained_with_original_official_source": 10,
        "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 1,
    }
    assert campaigns[8]["evidence"]["top_financialdata_supplement_review_batches"] == [
        {
            "review_priority": "P2",
            "financialdata_review_queue": "keep_absent_until_official_name_gated_match",
            "decision": "reject",
            "reason": "no_name_gated_official_isin_match",
            "financialdata_exchange": "NSE_IN",
            "financialdata_review_scope": "global_expansion_candidate",
            "official_source_key": "missing_official_source",
            "review_strategy": "keep_absent_until_official_name_gated_identifier_evidence_exists",
            "apply_eligibility": "keep_absent_until_name_gated_official_isin_match",
            "verification_evidence_required": (
                "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing"
            ),
            "rows": 1,
            "recommended_next_source": (
                "Official active masterfile, registry, or issuer source matching FinancialData name and listing identity."
            ),
            "source_gate": (
                "Keep absent until an official active source satisfies exact name/listing identity and ISIN gates."
            ),
        }
    ]
    assert campaigns[9]["delta_evidence"]["status"] == "baseline_only"
    assert campaigns[9]["evidence"]["baseline_global_metrics"] == 1
    assert campaigns[9]["evidence"]["baseline_campaigns"] == 2
    assert campaigns[9]["evidence"]["changed_numeric_delta_rows"] == 0
    assert campaigns[0]["campaign_key"] == "b3"
    assert campaigns[0]["acceptance_matrix"]["exchange_scope"] == ["B3"]
    assert campaigns[0]["acceptance_matrix"]["affected_artifact_rows"] == 6
    assert campaigns[0]["before_after_summary"]["exchange_scope"] == ["B3"]
    assert campaigns[0]["before_after_summary"]["affected_artifact_rows"] == 6
    assert campaigns[0]["before_after_summary"]["before_after_context"] == before_after_context_for(
        campaigns[0]["before_after_summary"]
    )
    assert campaigns[0]["campaign_context"] == campaign_context_for(campaigns[0])
    assert campaigns[0]["artifact_context"] == artifact_context_for(campaigns[0])
    assert campaigns[0]["delta_review_context"] == delta_review_context_for(campaigns[0])
    assert campaigns[0]["closure_context"] == closure_context_for(campaigns[0])
    assert sorted(campaigns[0]["before_after_summary"]["global_before_after"]) == [
        "category_delta",
        "isin_delta",
        "quarantine_delta",
        "sector_delta",
        "source_gap_delta",
        "warn_delta",
    ]
    assert campaigns[4]["acceptance_matrix"]["exchange_scope"] == ["BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext"]
