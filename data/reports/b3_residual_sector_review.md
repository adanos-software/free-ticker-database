# B3 Residual Sector Review

Generated at: `2026-07-08T04:36:06Z`

This report tracks remaining B3 stock-sector gaps after the official B3 sector-classification probe. It does not fill values.

## Summary

- Residual B3 stock-sector gaps: `1`

## Residual Decisions

| Decision | Rows |
|---|---:|
| accepted_source_gap_no_b3_classification_code_match | 1 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P3 | 1 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| no_b3_classification_code_match_source_gap | 1 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| source_gap_keep_blank_until_official_taxonomy_evidence | 1 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match | 1 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_blank_until_stronger_official_b3_or_issuer_taxonomy | 1 |

## Top Review Batches

| Priority | Bucket | Gap class | B3 code shape | Asset type | Rows | Strategy | Evidence gate | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P3 | no_b3_classification_code_match_source_gap | official_industry_taxonomy_unavailable_gap | alpha_b3_code | Stock | 1 | keep_blank_until_stronger_official_b3_or_issuer_taxonomy | stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match | Stronger official B3 or issuer taxonomy source exposing sector for the exact listing. | Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing. |

## B3 Probe Decisions

| Probe decision | Rows |
|---|---:|
| no_b3_code_match | 1 |

## B3 Code Shape Diagnostics

| Shape | Rows |
|---|---:|
| alpha_b3_code | 1 |

### Alphanumeric B3 Code Examples

| Listing key | Ticker | B3 code | Probe decision |
|---|---|---|---|

## Rows

| Listing key | Priority | Bucket | Name | B3 code | Probe | Decision |
|---|---|---|---|---|---|---|
| B3::RJSA3 | P3 | no_b3_classification_code_match_source_gap | RJS S.A. | RJSA | no_b3_code_match | accepted_source_gap_no_b3_classification_code_match |

## Policy

- No sector is inferred from ticker root, issuer-name shape, or peer rows in this report.
- Fill only after a direct official B3 taxonomy row or reviewed issuer source satisfies the row-level source gate.
