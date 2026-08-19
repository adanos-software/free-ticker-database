# Source-of-Truth Decisions

Generated at: `2026-08-19T08:50:35Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 6103 |
| official_fill_required | 2260 |
| core_exclusion_candidate | 441 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5307 |
| official_reference_symbol_collision_gap | 1304 |
| official_industry_taxonomy_unavailable_gap | 844 |
| otc_sector_source_gap | 553 |
| official_identifier_not_exposed_source_gap | 270 |
| fund_or_trust_identifier_gap | 213 |
| debt_or_securitized_identifier_gap | 76 |
| exchange_industry_source_gap | 62 |
| adr_cdr_or_depositary_identifier_gap | 44 |
| capital_pool_or_halted_identifier_gap | 33 |
| shell_or_cpc_sector_gap | 31 |
| inactive_or_legacy_identifier_gap | 22 |
| adr_cdr_or_depositary_sector_gap | 16 |
| official_current_directory_absent_identifier_gap | 9 |
| official_identifier_reference_unmatched_gap | 7 |
| fundlike_stock_sector_gap | 6 |
| official_product_taxonomy_unavailable_gap | 6 |
| official_product_reference_unmatched_category_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
