# Source-of-Truth Decisions

Generated at: `2026-11-11T18:37:13Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 5101 |
| official_fill_required | 2574 |
| core_exclusion_candidate | 495 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5624 |
| official_reference_symbol_collision_gap | 1182 |
| official_industry_taxonomy_unavailable_gap | 450 |
| fund_or_trust_identifier_gap | 290 |
| official_identifier_not_exposed_source_gap | 146 |
| debt_or_securitized_identifier_gap | 91 |
| otc_sector_source_gap | 91 |
| official_product_taxonomy_unavailable_gap | 89 |
| exchange_industry_source_gap | 48 |
| fundlike_stock_sector_gap | 33 |
| capital_pool_or_halted_identifier_gap | 31 |
| adr_cdr_or_depositary_identifier_gap | 20 |
| equity_etf_category_gap | 17 |
| inactive_or_legacy_identifier_gap | 17 |
| official_identifier_reference_unmatched_gap | 15 |
| shell_or_cpc_sector_gap | 8 |
| commodity_etf_category_gap | 6 |
| adr_cdr_or_depositary_sector_gap | 5 |
| fixed_income_etf_category_gap | 3 |
| official_current_directory_absent_identifier_gap | 2 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
