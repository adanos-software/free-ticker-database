# Twelve Data Batch A Second-Source Validation

Provider evidence is advisory validation only. It does not authorize applying Twelve Data-driven changes without a separate apply queue and dataset gates.

- Rows validated: 56
- Dry run: False

## Validation Status

| Status | Rows |
| --- | ---: |
| provider_found_different_name | 29 |
| second_source_supports_twelvedata_name | 20 |
| second_source_supports_local_name | 3 |
| no_second_source_name_match | 2 |
| ambiguous_second_source_evidence | 2 |

## Review Batches

| Batch | Rows |
| --- | ---: |
| batch_b_canada | 56 |

## Recommended Next Actions

| Action | Rows |
| --- | ---: |
| send_to_manual_or_deepseek_followup_with_additional_evidence | 31 |
| build_manual_apply_candidate_for_name_update_after_official_or_identifier_gate | 20 |
| keep_local_name_and_record_twelvedata_as_non_authoritative_mismatch | 3 |
| needs_more_second_source_evidence | 2 |

## Openfigi Status

| Status | Rows |
| --- | ---: |
| ok | 53 |
| no_match | 3 |

## Alphavantage Status

| Status | Rows |
| --- | ---: |
| no_match | 53 |
| ok | 3 |

## Fmp Status

| Status | Rows |
| --- | ---: |
| rate_limited_or_unavailable | 56 |

## Environment

API keys are read from environment variables only. No key values are stored in this report.
- `OPENFIGI_API_KEY`: set
- `ALPHAVANTAGE_API_KEY`: set
- `FMP_API_KEY`: set
