# Source-of-Truth Decisions

Generated at: `2026-11-04T17:22:53Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 5111 |
| official_fill_required | 2594 |
| core_exclusion_candidate | 480 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5646 |
| official_reference_symbol_collision_gap | 1181 |
| official_industry_taxonomy_unavailable_gap | 461 |
| fund_or_trust_identifier_gap | 279 |
| official_identifier_not_exposed_source_gap | 158 |
| otc_sector_source_gap | 92 |
| debt_or_securitized_identifier_gap | 91 |
| official_product_taxonomy_unavailable_gap | 75 |
| exchange_industry_source_gap | 51 |
| adr_cdr_or_depositary_identifier_gap | 32 |
| capital_pool_or_halted_identifier_gap | 31 |
| fundlike_stock_sector_gap | 18 |
| inactive_or_legacy_identifier_gap | 17 |
| official_identifier_reference_unmatched_gap | 15 |
| equity_etf_category_gap | 13 |
| shell_or_cpc_sector_gap | 7 |
| commodity_etf_category_gap | 6 |
| adr_cdr_or_depositary_sector_gap | 5 |
| fixed_income_etf_category_gap | 2 |
| official_current_directory_absent_identifier_gap | 2 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
