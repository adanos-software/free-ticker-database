# Twelve Data All-Batch Review Rollup

Twelve Data is a challenger source. DeepSeek reviews are advisory; apply-ready rows come from the source-adjudication report after provider, identifier, reviewed-override, and source-inventory gates. No database changes are applied by these reports.

## Totals

- rename_candidates: 4,946
- deepseek_reviews: 4,946
- deepseek_errors: 0
- second_source_queue_rows: 4,946
- second_source_validation_rows: 1,231
- manual_apply_candidates: 429
- source_adjudication_rows: 4,946
- source_adjudication_apply_ready_rows: 344

## Source Adjudication

| Decision | Rows |
| --- | ---: |
| provider_validation_pending | 3,622 |
| apply_twelvedata_name_identifier_supported | 344 |
| keep_local_name_provider_supported | 244 |
| conflict_blocked_provider_disagreement | 182 |
| conflict_blocked_figi_mismatch | 150 |
| keep_local_name_reviewed_override | 125 |
| source_gap_no_provider_match | 105 |
| ambiguous_blocked | 68 |
| conflict_blocked_provider_third_name | 65 |
| scope_review_blocked | 38 |
| candidate_needs_primary_source | 3 |

## Segments

| Segment | Rename rows | DeepSeek rows | Errors | Second-source queue | Second-source validated | Manual candidates | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| batch_a_us_core | 1,175 | 1,175 | 0 | 1,175 | 1,175 | 409 | validated |
| batch_b_canada | 56 | 56 | 0 | 56 | 56 | 20 | validated |
| batch_c_high_value_international | 1,291 | 1,291 | 0 | 1,291 | 0 | 0 | queued_pending_provider_validation |
| later_global_review | 2,424 | 2,424 | 0 | 2,424 | 0 | 0 | queued_pending_provider_validation |
