# Release Acceptance

Generated: `2026-10-20T10:15:46Z`

Overall passed: `False`

Summary context: `passed=false;criteria=67;passed_criteria=66;failed_criteria=1;validation_failed_error_gates=0`

## Summary

| Metric | Value |
|---|---:|
| `criteria` | `67` |
| `passed_criteria` | `66` |
| `failed_criteria` | `1` |
| `validation_failed_error_gates` | `0` |

| Criterion | Passed |
|---|---:|
| `no_invalid_isins` | True |
| `no_duplicate_primary_tickers` | True |
| `no_duplicate_listing_keys` | True |
| `no_duplicate_public_aliases` | True |
| `no_mojibake_names` | True |
| `no_unreviewed_country_isin_conflicts` | True |
| `adanos_alias_safety` | True |
| `source_gap_review_integrity` | True |
| `entry_quality_release_gate` | True |
| `release_metadata_consistency` | True |
| `release_source_report_integrity` | False |
| `deepseek_advisory_integrity` | True |
| `deepseek_queue_advisory_policies` | True |
| `deepseek_isin_collision_validation_gate` | True |
| `isin_validation_residual_triage_gate` | True |
| `m3_correctness_campaigns_gate` | True |
| `completion_backlog_next_actions` | True |
| `progress_markdown_traceability` | True |
| `adanos_detection_simulation` | True |
| `entry_quality_command_report` | True |
| `coverage_freshness_visibility` | True |
| `official_recall_exception_decisions` | True |
| `official_name_mismatch_backfill_gate` | True |
| `source_inventory_gap_gate` | True |
| `primary_isin_completeness_gate` | True |
| `etf_universe_completeness_gate` | True |
| `cfi_code_review_gate` | True |
| `source_refresh_queue_gate` | True |
| `source_gap_traceability` | True |
| `symbol_change_review_gate` | True |
| `otc_name_mismatch_review_gate` | True |
| `otc_name_mismatch_action_queue_gate` | True |
| `ohlcv_plausibility_gate` | True |
| `ohlcv_warning_review_gate` | True |
| `masterfile_collision_gate` | True |
| `isin_identity_collision_gate` | True |
| `financialdata_supplement_review_gate` | True |
| `otc_scope_gate` | True |
| `canada_figi_gate` | True |
| `canada_scope_review_queue_gate` | True |
| `canada_improvement_action_queue_gate` | True |
| `b3_masterfile_gap_review_gate` | True |
| `b3_core_scope_review_queue_gate` | True |
| `b3_improvement_action_queue_gate` | True |
| `b3_residual_gate` | True |
| `asx_scope_review_queue_gate` | True |
| `asx_residual_gate` | True |
| `weak_sector_residual_gate` | True |
| `weak_sector_venue_action_queue_gate` | True |
| `before_after_delta_matrix` | True |
| `improvement_baseline_integrity` | True |
| `campaign_reviewability` | True |
| `next_review_batch_visibility` | True |
| `next_review_command_safety_gate` | True |
| `campaign_closure_readiness_visibility` | True |
| `campaign_closure_blocker_visibility` | True |
| `campaign_review_policies` | True |
| `campaign_baseline_alignment` | True |
| `campaign_acceptance_matrices` | True |
| `campaign_artifact_integrity` | True |
| `review_artifact_siblings` | True |
| `review_artifact_gates` | True |
| `review_artifact_policy` | True |
| `review_row_traceability` | True |
| `review_row_evidence` | True |
| `apply_artifact_traceability` | True |
| `supplement_artifact_traceability` | True |

## Failed Criteria

| Criterion | Details |
|---|---|
| `release_source_report_integrity` | `{"checked": {"adanos_alias_audit": {"exists": true, "generated_at": "2026-07-08T04:36:11Z", "path": "data/reports/adanos_alias_audit.json"}, "adanos_detection_simulation": {"exists": true, "generated_at": "2026-08-01T16:50:36Z", "path": "data/reports/adanos_detection_simulation.json"}, "asx_residual_review": {"exists": true, "generated_at": "2026-06-02T19:09:19Z", "path": "data/reports/asx_residual_review.json"}, "asx_scope_review_queue": {"exists": true, "generated_at": "2026-06-02T20:18:32Z", "path": "data/reports/asx_scope_review_queue.json"}, "b3_core_scope_review_queue": {"exists": true, "generated_at": "2026-06-02T20:18:31Z", "path": "data/reports/b3_core_scope_review_queue.json"}, "b3_improvement_action_queue": {"exists": true, "generated_at": "2026-06-02T20:18:32Z", "path": "data/reports/b3_improvement_action_queue.json"}, "b3_masterfile_gap_review": {"exists": true, "generated_at": "2026-10-20T10:15:07Z", "path": "data/reports/b3_masterfile_gap_review.json"}, "b3_residual_isin_review": {"exists": true, "generated_at": "2026-07-01T05:20:19Z", "path": "data/reports/b3_residual_isin_review.json"}, "b3_residual_sector_review": {"exists": true, "generated_at": "2026-07-08T04:36:06Z", "path": "data/reports/b3_residual_sector_review.json"}, "canada_figi_apply_report": {"exists": true, "generated_at": "2026-05-25T05:46:48Z", "path": "data/reports/canada_figi_apply_report.json"}, "canada_figi_queue": {"exists": true, "generated_at": "2026-06-03T04:26:51Z", "path": "data/reports/canada_figi_queue.json"}, "canada_improvement_action_queue": {"exists": true, "generated_at": "2026-06-02T20:18:32Z", "path": "data/reports/canada_improvement_action_queue.json"}, "canada_residual_review": {"exists": true, "generated_at": "2026-06-03T04:26:51Z", "path": "data/reports/canada_residual_review.json"}, "canada_scope_review_queue": {"exists": true, "generated_at": "2026-06-02T20:18:32Z", "path": "data/reports/canada_scope_review_queue.json"}, "cfi_code_review": {"exists": true, "generated_at": "2026-07-07T10:09:52Z", "path": "data/reports/cfi_code_review.json"}, "completion_backlog": {"exists": true, "generated_at": "2026-10-11T12:47:07Z", "path": "data/reports/completion_backlog.json"}, "deepseek_batch_plan": {"exists": true, "generated_at": "2026-06-02T23:05:27Z", "path": "data/reports/deepseek_batch_plan.json"}, "deepseek_collision_review_queue": {"exists": true, "generated_at": "2026-06-03T00:48:32Z", "path": "data/reports/deepseek_collision_review_queue.json"}, "deepseek_isin_collision_validation": {"exists": true, "generated_at": "2026-05-30T07:30:59Z", "path": "data/reports/deepseek_isin_collision_validation.json"}, "deepseek_otc_review_queue": {"exists": true, "generated_at": "2026-06-03T00:42:25Z", "path": "data/reports/deepseek_otc_review_queue.json"}, "deepseek_review_summary": {"exists": true, "generated_at": "2026-06-02T21:15:57Z", "path": "data/reports/deepseek_review_summary.json"}, "deepseek_weak_sector_review_queue": {"exists": true, "generated_at": "2026-06-03T00:37:41Z", "path": "data/reports/deepseek_weak_sector_review_queue.json"}, "entry_quality_gate": {"exists": true, "generated_at": "2026-10-20T10:14:11Z", "path": "data/reports/entry_quality_gate.json"}, "etf_universe_completeness": {"exists": true, "generated_at": "2026-07-07T10:09:48Z", "path": "data/reports/etf_universe_completeness.json"}, "finance_database_isin_coverage_review": {"exists": true, "generated_at": "2026-08-03T16:31:52.200680Z", "path": "data/reports/finance_database_isin_coverage_review.json"}, "financialdata_isin_supplements_review": {"exists": true, "generated_at": "2026-06-02T20:31:19Z", "path": "data/reports/financialdata_isin_supplements_review.json"}, "firds_listing_compare": {"exists": true, "generated_at": "2026-10-20T08:32:01.785388+00:00", "path": "data/reports/firds_listing_compare.json"}, "gleif_isin_lei_compare": {"exists": true, "generated_at": "2026-10-20T08:30:57.514503+00:00", "path": "data/reports/gleif_isin_lei_compare.json"}, "improvement_baseline": {"exists": true, "generated_at": "2026-08-06T14:13:29Z", "path": "data/reports/improvement_baseline.json"}, "improvement_campaigns": {"exists": true, "generated_at": "2026-10-20T10:15:08Z", "path": "data/reports/improvement_campaigns.json"}, "improvement_deltas": {"exists": true, "generated_at": "2026-08-06T14:13:29Z", "path": "data/reports/improvement_deltas.json"}, "isin_identity_collision_review_queue": {"exists": true, "generated_at": "2026-10-20T10:12:31Z", "path": "data/reports/isin_identity_collision_review_queue.json"}, "isin_identity_issue449_review": {"exists": true, "generated_at": "2026-10-20T10:15:44.542530+00:00", "path": "data/reports/isin_identity_issue449_review.json"}, "isin_validation_report": {"exists": true, "generated_at": "2026-10-19T15:08:14Z", "path": "data/reports/isin_validation_report.json"}, "m3_correctness_audit": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_correctness_audit.json"}, "m3_correctness_campaigns": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_correctness_campaigns.json"}, "m3_identity_residual_campaign": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_identity_residual_campaign.json"}, "m3_name_freshness_campaign": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_name_freshness_campaign.json"}, "m3_non_equity_leakage_guard": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_non_equity_leakage_guard.json"}, "m3_sector_category_campaign": {"exists": true, "generated_at": "2026-10-20T10:15:44Z", "path": "data/reports/m3_sector_category_campaign.json"}, "masterfile_collision_review": {"exists": true, "generated_at": "2026-06-02T19:18:19Z", "path": "data/reports/masterfile_collision_review.json"}, "masterfile_vanished_delisting_review": {"exists": true, "generated_at": "2026-10-11T12:41:19Z", "path": "data/reports/masterfile_vanished_delisting_review.json"}, "official_full_recall_action_queue": {"exists": true, "generated_at": "2026-10-20T10:12:25Z", "path": "data/reports/official_full_recall_action_queue.json"}, "official_name_mismatch_backfill": {"exists": true, "generated_at": "2026-10-11T12:44:11Z", "path": "data/reports/official_name_mismatch_backfill.json"}, "ohlcv_plausibility": {"exists": true, "generated_at": "2026-08-01T16:53:59Z", "path": "data/reports/ohlcv_plausibility.json"}, "ohlcv_warning_review": {"exists": true, "generated_at": "2026-06-02T20:39:44Z", "path": "data/reports/ohlcv_warning_review.json"}, "openfigi_isin_mic_remap": {"exists": true, "generated_at": "2026-10-20T08:32:07.451231+00:00", "path": "data/reports/openfigi_isin_mic_remap.json"}, "otc_name_mismatch_action_queue": {"exists": true, "generated_at": "2026-08-06T14:18:42Z", "path": "data/reports/otc_name_mismatch_action_queue.json"}, "otc_name_mismatch_review": {"exists": true, "generated_at": "2026-08-06T14:18:41Z", "path": "data/reports/otc_name_mismatch_review.json"}, "otc_scope_review": {"exists": true, "generated_at": "2026-08-06T14:18:41Z", "path": "data/reports/otc_scope_review.json"}, "pending_renames_manual_review": {"exists": true, "generated_at": "2026-10-12T16:51:42Z", "path": "data/reports/pending_renames_manual_review.json"}, "primary_isin_completeness": {"exists": true, "generated_at": "2026-07-08T04:36:03Z", "path": "data/reports/primary_isin_completeness.json"}, "sec_listed_current_compare": {"exists": true, "generated_at": "2026-10-20T08:30:42.315228+00:00", "path": "data/reports/sec_listed_current_compare.json"}, "sec_sic_sector_backfill": {"exists": true, "generated_at": "2026-10-09T06:24:13Z", "path": "data/reports/sec_sic_sector_backfill.json"}, "source_gap_classification": {"exists": true, "generated_at": "2026-10-20T10:12:33Z", "path": "data/reports/source_gap_classification.json"}, "source_inventory_gap": {"exists": true, "generated_at": "2026-10-11T12:47:03Z", "path": "data/reports/source_inventory_gap.json"}, "source_refresh_queue": {"exists": true, "generated_at": "2026-06-03T03:12:31Z", "path": "data/reports/source_refresh_queue.json"}, "symbol_changes_review": {"exists": true, "generated_at": "2026-10-16T11:18:49Z", "path": "data/reports/symbol_changes_review.json"}, "tse_sector_backfill": {"exists": true, "generated_at": "2026-06-02T23:50:41Z", "path": "data/reports/tse_sector_backfill.json"}, "twelvedata_all_batches_review_rollup": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_all_batches_review_rollup.json"}, "twelvedata_batch_a_second_source_queue": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_batch_a_second_source_queue_summary.json"}, "twelvedata_batch_b_second_source_queue": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_batch_b_second_source_queue_summary.json"}, "twelvedata_batch_c_second_source_queue": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_batch_c_second_source_queue_summary.json"}, "twelvedata_global_rest_second_source_queue": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_global_rest_second_source_queue_summary.json"}, "twelvedata_review_queues": {"exists": true, "generated_at": "2026-06-10T13:33:18Z", "path": "data/reports/twelvedata_review_queues_summary.json"}, "validation_report": {"exists": true, "generated_at": "2026-10-20T08:30:53Z", "path": "data/reports/validation_report.json"}, "weak_sector_residual_review": {"exists": true, "generated_at": "2026-06-03T04:26:52Z", "path": "data/reports/weak_sector_residual_review.json"}, "weak_sector_venue_action_queue": {"exists": true, "generated_at": "2026-06-02T20:27:12Z", "path": "data/reports/weak_sector_venue_action_queue.json"}}, "invalid_generated_at": {"firds_listing_compare": "2026-10-20T08:32:01.785388+00:00", "gleif_isin_lei_compare": "2026-10-20T08:30:57.514503+00:00", "isin_identity_issue449_review": "2026-10-20T10:15:44.542530+00:00", "openfigi_isin_mic_remap": "2026-10-20T08:32:07.451231+00:00", "sec_listed_current_compare": "2026-10-20T08:30:42.315228+00:00"}, "release_generated_at": "2026-10-20T10:15:46Z", "release_generated_at_invalid": false, "review_source_report_name_markers": ["queue", "review", "gap", "backlog"]}` |

## Acceptance Delta Matrix

| Metric | Baseline | Current | Delta |
|---|---:|---:|---:|
| `isin_delta` | 62309 | 62309 | 0 |
| `sector_delta` | 45881 | 45881 | 0 |
| `category_delta` | 15601 | 15601 | 0 |
| `source_gap_delta` | 9847 | 9847 | 0 |
| `warn_delta` | 278 | 278 | 0 |
| `quarantine_delta` | 0 | 0 | 0 |

## Campaign Status

| Priority | Campaign | Status | Delta Status | Next Action |
|---:|---|---|---|---|
| 1 | `b3` | `partially_improved_with_residual_source_gaps` | `partial` | refresh official B3 sources and only apply residual ISIN/sector values with exact official identifier or taxonomy evidence |
| 2 | `otc` | `scoped_as_extended_with_source_gaps_documented` | `partial` | keep OTC enrichment gated by issuer/product evidence; do not sector-fill extended OTC rows from symbol or name shape |
| 3 | `canada` | `figi_queue_drained_remaining_isin_first_gaps` | `present_for_figi_apply` | resolve remaining Canada identifiers through official ISIN-capable sources before any additional FIGI probing |
| 4 | `asx` | `official_probe_reviewed_residuals_documented` | `review_only_no_data_apply` | only apply ASX identifiers or categories after exact ASX workbook/listing/name gates pass |
| 5 | `weak_sector` | `venue_specific_review_queue_with_safe_ngx_apply` | `partial` | continue venue-specific official taxonomy work; skip broad NGX labels unless they map cleanly to canonical sectors |
| 6 | `masterfile_collisions` | `listing_keyed_review_queue_ready_no_symbol_only_additions` | `review_queue_only_no_data_apply` | process same-ISIN cross-listing candidates first, with exchange/MIC/name/instrument-type review gates |
| 7 | `symbol_changes` | `source_scope_aware_review_queue` | `review_queue_only_no_symbol_changes_applied` | review scoped rename candidates against official exchange notices before changing any canonical symbol |
| 8 | `ohlcv` | `sampling_queue_enabled_plausibility_only` | `sampling_queue_only_no_data_apply` | run bounded Yahoo/local samples for selected buckets and use results only as review signals |
| 9 | `freshness` | `global_and_source_freshness_visible` | `present_for_report_visibility` | refresh old official masterfile sources and rerun verification reports |
| 10 | `baseline` | `baseline_snapshot_available_for_future_campaign_deltas` | `baseline_only` | compare future campaign reports against this baseline before applying or closing a PR |

## Campaign Evidence Gates

| Guard | Gap Count |
|---|---:|
| `asx_campaign_evidence_gaps` | 0 |
| `b3_campaign_evidence_gaps` | 0 |
| `baseline_campaign_evidence_gaps` | 0 |
| `canada_campaign_evidence_gaps` | 0 |
| `freshness_campaign_evidence_gaps` | 0 |
| `masterfile_collision_campaign_evidence_gaps` | 0 |
| `ohlcv_campaign_evidence_gaps` | 0 |
| `otc_campaign_evidence_gaps` | 0 |
| `symbol_change_campaign_evidence_gaps` | 0 |
| `weak_sector_campaign_evidence_gaps` | 0 |
