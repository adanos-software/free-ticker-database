# Improvement Deltas

Generated: `2026-07-01T05:20:49Z`
Baseline: `2026-07-01T05:20:49Z`

This report compares current campaign metrics against `data/reports/improvement_baseline.json`.

## Summary

| Metric | Value |
|---|---:|
| numeric_delta_rows | 932 |
| changed_numeric_delta_rows | 0 |
| global_changed_numeric_delta_rows | 0 |
| campaign_changed_numeric_delta_rows | 0 |
| exchange_changed_numeric_delta_rows | 0 |
| changed_exchange_rows | 0 |

## Acceptance Delta Matrix

| Acceptance Metric | Baseline | Current | Delta | Review Context |
|---|---:|---:|---:|---|
| `isin_delta` | 61477 | 61477 | 0 | `scope=global;metric=isin_delta;baseline=61477;current=61477;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `sector_delta` | 47389 | 47389 | 0 | `scope=global;metric=sector_delta;baseline=47389;current=47389;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `category_delta` | 15581 | 15581 | 0 | `scope=global;metric=category_delta;baseline=15581;current=15581;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `source_gap_delta` | 1090 | 1090 | 0 | `scope=global;metric=source_gap_delta;baseline=1090;current=1090;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
| `warn_delta` | 75 | 75 | 0 | `scope=global;metric=warn_delta;baseline=75;current=75;delta=0;direction=unchanged;review_policy=no_data_change_inferred` |
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
