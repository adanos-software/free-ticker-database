# Twelve Data Batch A Second-Source Validation

Provider evidence is advisory validation only. It does not authorize applying Twelve Data-driven changes without a separate apply queue and dataset gates.

- Rows validated: 1,175
- Dry run: False

## Validation Status

| Status | Rows |
| --- | ---: |
| second_source_supports_twelvedata_name | 409 |
| second_source_supports_local_name | 286 |
| conflicting_second_source_evidence | 216 |
| no_second_source_name_match | 105 |
| ambiguous_second_source_evidence | 87 |
| provider_found_different_name | 72 |

## Review Batches

| Batch | Rows |
| --- | ---: |
| batch_a_us_core | 1,175 |

## Recommended Next Actions

| Action | Rows |
| --- | ---: |
| build_manual_apply_candidate_for_name_update_after_official_or_identifier_gate | 409 |
| keep_local_name_and_record_twelvedata_as_non_authoritative_mismatch | 286 |
| manual_identity_review_required_before_any_apply | 216 |
| send_to_manual_or_deepseek_followup_with_additional_evidence | 159 |
| needs_more_second_source_evidence | 105 |

## Openfigi Status

| Status | Rows |
| --- | ---: |
| ok | 988 |
| no_match | 187 |

## Alphavantage Status

| Status | Rows |
| --- | ---: |
| no_match | 718 |
| ok | 457 |

## Fmp Status

| Status | Rows |
| --- | ---: |
| rate_limited_or_unavailable | 1,175 |

## Environment

API keys are read from environment variables only. No key values are stored in this report.
- `OPENFIGI_API_KEY`: set
- `ALPHAVANTAGE_API_KEY`: set
- `FMP_API_KEY`: set
