# Improvement Deltas

Generated: `2026-05-25T12:54:49Z`
Baseline: `2026-05-25T12:54:49Z`

This report compares current campaign metrics against `data/reports/improvement_baseline.json`.

## Summary

| Metric | Value |
|---|---:|
| numeric_delta_rows | 926 |
| changed_numeric_delta_rows | 0 |
| global_changed_numeric_delta_rows | 0 |
| campaign_changed_numeric_delta_rows | 0 |
| exchange_changed_numeric_delta_rows | 0 |
| changed_exchange_rows | 0 |

## Acceptance Delta Matrix

| Acceptance Metric | Baseline | Current | Delta | Review Context |
|---|---:|---:|---:|---|
| `isin_delta` | 59847 | 59847 | 0 | `scope=global;metric=isin_delta;baseline=59847;current=59847;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `sector_delta` | 43310 | 43310 | 0 | `scope=global;metric=sector_delta;baseline=43310;current=43310;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `category_delta` | 15453 | 15453 | 0 | `scope=global;metric=category_delta;baseline=15453;current=15453;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `source_gap_delta` | 3548 | 3548 | 0 | `scope=global;metric=source_gap_delta;baseline=3548;current=3548;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `warn_delta` | 217 | 217 | 0 | `scope=global;metric=warn_delta;baseline=217;current=217;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `quarantine_delta` | 0 | 0 | 0 | `scope=global;metric=quarantine_delta;baseline=0;current=0;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |

## Changed Exchange Acceptance Deltas

| Exchange | Changed Metrics |
|---|---|
| none | `{}` |

## Changed Numeric Deltas

| Path | Baseline | Current | Delta |
|---|---:|---:|---:|
| none |  |  | 0 |

## Source Files

| Key | Path |
|---|---|
| `baseline_snapshot` | `data/reports/improvement_baseline.json` |
| `current_snapshot_asx_residual_review` | `data/reports/asx_residual_review.json` |
| `current_snapshot_b3_residual_isin_review` | `data/reports/b3_residual_isin_review.json` |
| `current_snapshot_b3_residual_sector_review` | `data/reports/b3_residual_sector_review.json` |
| `current_snapshot_canada_figi_queue` | `data/reports/canada_figi_queue.json` |
| `current_snapshot_canada_residual_review` | `data/reports/canada_residual_review.json` |
| `current_snapshot_coverage_report` | `data/reports/coverage_report.json` |
| `current_snapshot_entry_quality_csv` | `data/reports/entry_quality.csv` |
| `current_snapshot_entry_quality_json` | `data/reports/entry_quality.json` |
| `current_snapshot_financialdata_isin_supplements_review` | `data/reports/financialdata_isin_supplements_review.json` |
| `current_snapshot_masterfile_collision_review` | `data/reports/masterfile_collision_review.json` |
| `current_snapshot_ohlcv_plausibility` | `data/reports/ohlcv_plausibility.json` |
| `current_snapshot_ohlcv_warning_review` | `data/reports/ohlcv_warning_review.json` |
| `current_snapshot_otc_scope_review` | `data/reports/otc_scope_review.json` |
| `current_snapshot_source_gap_classification_csv` | `data/reports/source_gap_classification.csv` |
| `current_snapshot_source_gap_classification_json` | `data/reports/source_gap_classification.json` |
| `current_snapshot_symbol_changes_review` | `data/reports/symbol_changes_review.json` |
| `current_snapshot_validation_report` | `data/reports/validation_report.json` |
| `current_snapshot_weak_sector_residual_review` | `data/reports/weak_sector_residual_review.json` |
