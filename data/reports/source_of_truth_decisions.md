# Source-of-Truth Decisions

Generated at: `2026-08-31T14:01:36Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 6033 |
| official_fill_required | 2152 |
| core_exclusion_candidate | 514 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5198 |
| official_reference_symbol_collision_gap | 1191 |
| official_industry_taxonomy_unavailable_gap | 848 |
| otc_sector_source_gap | 554 |
| official_identifier_not_exposed_source_gap | 286 |
| fund_or_trust_identifier_gap | 273 |
| debt_or_securitized_identifier_gap | 84 |
| exchange_industry_source_gap | 62 |
| adr_cdr_or_depositary_identifier_gap | 44 |
| shell_or_cpc_sector_gap | 37 |
| capital_pool_or_halted_identifier_gap | 33 |
| official_product_taxonomy_unavailable_gap | 29 |
| inactive_or_legacy_identifier_gap | 22 |
| adr_cdr_or_depositary_sector_gap | 16 |
| official_identifier_reference_unmatched_gap | 8 |
| equity_etf_category_gap | 5 |
| fundlike_stock_sector_gap | 5 |
| official_current_directory_absent_identifier_gap | 2 |
| digital_asset_etf_category_gap | 1 |
| official_product_reference_unmatched_category_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
