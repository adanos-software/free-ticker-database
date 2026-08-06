# Source-of-Truth Decisions

Generated at: `2026-08-06T14:07:50Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 4645 |
| official_fill_required | 2079 |
| core_exclusion_candidate | 641 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5131 |
| official_reference_symbol_collision_gap | 792 |
| official_identifier_not_exposed_source_gap | 510 |
| fund_or_trust_identifier_gap | 366 |
| official_industry_taxonomy_unavailable_gap | 116 |
| official_product_taxonomy_unavailable_gap | 115 |
| inactive_or_legacy_identifier_gap | 104 |
| debt_or_securitized_identifier_gap | 76 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| otc_sector_source_gap | 22 |
| equity_etf_category_gap | 20 |
| shell_or_cpc_sector_gap | 18 |
| official_identifier_reference_unmatched_gap | 10 |
| digital_asset_etf_category_gap | 3 |
| commodity_etf_category_gap | 2 |
| official_current_directory_absent_identifier_gap | 2 |
| fixed_income_etf_category_gap | 1 |
| fundlike_stock_sector_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
