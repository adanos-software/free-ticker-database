# DeepSeek OTC Review Queue

Generated: `2026-06-02T20:15:06Z`

Policy: DeepSeek OTC reviews are triage only and do not authorize names, sectors, aliases, or scope changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 8996 |
| Unmatched DeepSeek rows | 2 |

## Review Queues

| Queue | Rows |
| --- | ---: |
| official_name_mismatch_evidence_review | 136 |
| otc_scope_evidence_review | 8708 |
| otc_source_gap_evidence_review | 152 |

## Issue Types

| Issue type | Rows |
| --- | ---: |
| missing | 6129 |
| official_name_mismatch | 136 |
| official_reference_gap | 2731 |

## Unmatched DeepSeek Rows

These advisory rows no longer match the current OTC scope review and are excluded from the active queue.

| Listing key | Reason |
| --- | --- |
| OTC::DUKR | missing_otc_scope_review_row |
| OTC::FNRN | missing_otc_scope_review_row |

## Review Gate

Do not change OTC names, aliases, sectors, identifiers, or scope from DeepSeek output. Resolve each row with listing-keyed OTC Markets, SEC, issuer, or ISIN-anchored issuer-history evidence.
