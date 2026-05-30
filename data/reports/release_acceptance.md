# Release Acceptance

Generated: `2026-05-30T05:19:55Z`

Overall passed: `True`

Summary context: `passed=true;criteria=41;passed_criteria=41;failed_criteria=0;validation_failed_error_gates=0`

## Summary

| Metric | Value |
|---|---:|
| `criteria` | `41` |
| `passed_criteria` | `41` |
| `failed_criteria` | `0` |
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
| `release_source_report_integrity` | True |
| `progress_markdown_traceability` | True |
| `adanos_detection_simulation` | True |
| `entry_quality_command_report` | True |
| `coverage_freshness_visibility` | True |
| `source_gap_traceability` | True |
| `symbol_change_review_gate` | True |
| `ohlcv_plausibility_gate` | True |
| `masterfile_collision_gate` | True |
| `otc_scope_gate` | True |
| `canada_figi_gate` | True |
| `b3_residual_gate` | True |
| `asx_residual_gate` | True |
| `weak_sector_residual_gate` | True |
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

## Acceptance Delta Matrix

| Metric | Baseline | Current | Delta |
|---|---:|---:|---:|
| `isin_delta` | 59847 | 59847 | 0 |
| `sector_delta` | 43310 | 43310 | 0 |
| `category_delta` | 15453 | 15453 | 0 |
| `source_gap_delta` | 3548 | 3548 | 0 |
| `warn_delta` | 217 | 217 | 0 |
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
