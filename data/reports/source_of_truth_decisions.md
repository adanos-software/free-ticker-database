# Source-of-Truth Decisions

Generated at: `2026-05-16T17:23:12Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 2945 |
| core_exclusion_candidate | 817 |

## Top Classes

| Value | Rows |
|---|---:|
| official_industry_taxonomy_unavailable_gap | 1579 |
| otc_sector_source_gap | 817 |
| official_identifier_not_exposed_source_gap | 315 |
| fund_or_trust_identifier_gap | 294 |
| inactive_or_legacy_identifier_gap | 158 |
| shell_or_cpc_sector_gap | 90 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 73 |
| official_product_taxonomy_unavailable_gap | 72 |
| official_identifier_reference_unmatched_gap | 65 |
| official_current_directory_absent_identifier_gap | 64 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| official_product_reference_unmatched_category_gap | 24 |
| commodity_etf_category_gap | 3 |
| digital_asset_etf_category_gap | 2 |
| equity_etf_category_gap | 2 |
| fixed_income_etf_category_gap | 2 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
