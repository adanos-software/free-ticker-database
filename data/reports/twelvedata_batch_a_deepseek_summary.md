# Twelve Data Batch A DeepSeek Triage

DeepSeek triage is advisory only and authorizes no name, identifier, alias, scope, or listing change without second-source evidence.

## Counts

- Rows reviewed: 100
- Errors: 0

## Decision Distribution

| Decision | Rows |
| --- | ---: |
| needs_official_evidence | 76 |
| uncertain | 20 |
| out_of_scope_candidate | 2 |
| candidate_apply_blocked | 1 |
| possible_duplicate_or_cross_listing | 1 |

## Safe Action Distribution

| Safe action | Rows |
| --- | ---: |
| needs_official_evidence | 96 |
| candidate_for_official_followup | 3 |
| likely_same_issuer_review | 1 |

## Confidence Buckets

| Bucket | Rows |
| --- | ---: |
| <0.4 | 97 |
| >=0.7 | 3 |

## External Validation Status

OpenFIGI, AlphaVantage, and FMP live checks require environment variables before running. No external API keys are stored in this report.

## Outputs

- `data/deepseek_review_jobs/twelvedata_batch_a_normalized_reviews.csv`
- `data/deepseek_review_jobs/twelvedata_batch_a_normalized_reviews.json`
- `data/deepseek_review_jobs/twelvedata_batch_a_raw_responses.jsonl`
- `data/reports/twelvedata_batch_a_deepseek_summary.json`
