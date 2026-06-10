# Twelve Data Batch A Second-Source Queue

Second-source validation is required before applying Twelve Data-driven name, identifier, alias, scope, or listing changes.

- Rows: 100

## Provider Queues

| Providers | Rows |
| --- | ---: |
| openfigi|alphavantage|fmp | 100 |

## DeepSeek Decisions

| Decision | Rows |
| --- | ---: |
| needs_official_evidence | 76 |
| uncertain | 20 |
| out_of_scope_candidate | 2 |
| candidate_apply_blocked | 1 |
| possible_duplicate_or_cross_listing | 1 |

## Environment

Provider API keys are read from environment variables only. No key values are stored in this report.
- `OPENFIGI_API_KEY`: missing
- `ALPHAVANTAGE_API_KEY`: missing
- `FMP_API_KEY`: missing
