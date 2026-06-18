# Source-of-Truth Decisions

Generated at: `2026-06-18T12:10:47Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 792 |
| core_exclusion_candidate | 635 |

## Top Classes

| Value | Rows |
|---|---:|
| official_identifier_not_exposed_source_gap | 523 |
| fund_or_trust_identifier_gap | 450 |
| official_identifier_reference_unmatched_gap | 104 |
| debt_or_securitized_identifier_gap | 83 |
| official_product_taxonomy_unavailable_gap | 58 |
| adr_cdr_or_depositary_identifier_gap | 46 |
| otc_sector_source_gap | 42 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_industry_taxonomy_unavailable_gap | 22 |
| official_product_reference_unmatched_category_gap | 20 |
| inactive_or_legacy_identifier_gap | 18 |
| official_current_directory_absent_identifier_gap | 12 |
| equity_etf_category_gap | 4 |
| commodity_etf_category_gap | 3 |
| fixed_income_etf_category_gap | 3 |
| adr_cdr_or_depositary_sector_gap | 1 |
| digital_asset_etf_category_gap | 1 |
| fundlike_stock_sector_gap | 1 |
| shell_or_cpc_sector_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
