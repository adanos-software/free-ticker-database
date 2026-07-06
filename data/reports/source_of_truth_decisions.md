# Source-of-Truth Decisions

Generated at: `2026-07-06T11:17:03Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 647 |
| core_exclusion_candidate | 491 |

## Top Classes

| Value | Rows |
|---|---:|
| official_identifier_not_exposed_source_gap | 418 |
| fund_or_trust_identifier_gap | 300 |
| debt_or_securitized_identifier_gap | 81 |
| official_product_taxonomy_unavailable_gap | 63 |
| official_identifier_reference_unmatched_gap | 49 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| otc_sector_source_gap | 43 |
| official_industry_taxonomy_unavailable_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_product_reference_unmatched_category_gap | 20 |
| inactive_or_legacy_identifier_gap | 19 |
| shell_or_cpc_sector_gap | 11 |
| equity_etf_category_gap | 6 |
| commodity_etf_category_gap | 3 |
| fixed_income_etf_category_gap | 3 |
| official_current_directory_absent_identifier_gap | 2 |
| adr_cdr_or_depositary_sector_gap | 1 |
| digital_asset_etf_category_gap | 1 |
| fundlike_stock_sector_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
