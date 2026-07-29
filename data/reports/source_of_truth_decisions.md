# Source-of-Truth Decisions

Generated at: `2026-07-29T09:25:57Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| official_fill_required | 5861 |
| accepted_source_gap | 1113 |
| core_exclusion_candidate | 536 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5861 |
| official_reference_symbol_collision_gap | 709 |
| fund_or_trust_identifier_gap | 354 |
| official_identifier_not_exposed_source_gap | 255 |
| official_product_taxonomy_unavailable_gap | 97 |
| debt_or_securitized_identifier_gap | 79 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| official_industry_taxonomy_unavailable_gap | 25 |
| inactive_or_legacy_identifier_gap | 17 |
| equity_etf_category_gap | 14 |
| shell_or_cpc_sector_gap | 10 |
| official_identifier_reference_unmatched_gap | 5 |
| digital_asset_etf_category_gap | 3 |
| commodity_etf_category_gap | 2 |
| official_current_directory_absent_identifier_gap | 2 |
| fixed_income_etf_category_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
