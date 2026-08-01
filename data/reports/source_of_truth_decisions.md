# Source-of-Truth Decisions

Generated at: `2026-08-01T08:56:22Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| official_fill_required | 5964 |
| accepted_source_gap | 1142 |
| core_exclusion_candidate | 542 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5964 |
| official_reference_symbol_collision_gap | 716 |
| fund_or_trust_identifier_gap | 357 |
| official_identifier_not_exposed_source_gap | 263 |
| official_product_taxonomy_unavailable_gap | 101 |
| debt_or_securitized_identifier_gap | 79 |
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| official_industry_taxonomy_unavailable_gap | 33 |
| inactive_or_legacy_identifier_gap | 17 |
| equity_etf_category_gap | 15 |
| shell_or_cpc_sector_gap | 13 |
| official_identifier_reference_unmatched_gap | 5 |
| digital_asset_etf_category_gap | 3 |
| commodity_etf_category_gap | 2 |
| official_current_directory_absent_identifier_gap | 2 |
| fixed_income_etf_category_gap | 1 |
| otc_sector_source_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
