# DeepSeek OTC Review Queue

Generated: `2026-06-02T20:38:59Z`

Policy: DeepSeek OTC reviews are triage only and do not authorize names, sectors, aliases, or scope changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 8996 |
| Unmatched DeepSeek rows | 2 |

## Review Queues

| Queue | Rows |
| --- | ---: |
| official_name_mismatch_evidence_review | 21 |
| otc_scope_evidence_review | 8804 |
| otc_source_gap_evidence_review | 171 |

## Issue Types

| Issue type | Rows |
| --- | ---: |
| missing | 6217 |
| official_isin_mismatch|official_name_mismatch | 9 |
| official_name_mismatch | 12 |
| official_reference_gap | 2758 |

## Unmatched DeepSeek Rows

These advisory rows no longer match the current OTC scope review and are excluded from the active queue.

| Listing key | Reason |
| --- | --- |
| OTC::DUKR | missing_otc_scope_review_row |
| OTC::FNRN | missing_otc_scope_review_row |

## Review Gate

Do not change OTC names, aliases, sectors, identifiers, or scope from DeepSeek output. Resolve each row with listing-keyed OTC Markets, SEC, issuer, or ISIN-anchored issuer-history evidence.
