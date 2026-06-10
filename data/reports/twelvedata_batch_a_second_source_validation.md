# Twelve Data Batch A Second-Source Validation

Provider evidence is advisory validation only. It does not authorize applying Twelve Data-driven changes without a separate apply queue and dataset gates.

- Rows validated: 100
- Dry run: False

## Validation Status

| Status | Rows |
| --- | ---: |
| second_source_supports_twelvedata_name | 39 |
| second_source_supports_local_name | 28 |
| provider_found_different_name | 12 |
| no_second_source_name_match | 12 |
| conflicting_second_source_evidence | 6 |
| ambiguous_second_source_evidence | 3 |

## Recommended Next Actions

| Action | Rows |
| --- | ---: |
| build_manual_apply_candidate_for_name_update_after_official_or_identifier_gate | 39 |
| keep_local_name_and_record_twelvedata_as_non_authoritative_mismatch | 28 |
| send_to_manual_or_deepseek_followup_with_additional_evidence | 15 |
| needs_more_second_source_evidence | 11 |
| manual_identity_review_required_before_any_apply | 6 |
| official_followup_required | 1 |

## Openfigi Status

| Status | Rows |
| --- | ---: |
| ok | 80 |
| no_match | 20 |

## Alphavantage Status

| Status | Rows |
| --- | ---: |
| no_match | 76 |
| ok | 24 |

## Fmp Status

| Status | Rows |
| --- | ---: |
| provider_error | 80 |
| ok | 19 |
| no_match | 1 |

## Environment

API keys are read from environment variables only. No key values are stored in this report.
- `OPENFIGI_API_KEY`: set
- `ALPHAVANTAGE_API_KEY`: set
- `FMP_API_KEY`: set
