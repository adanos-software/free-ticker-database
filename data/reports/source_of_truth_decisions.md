# Source-of-Truth Decisions

Generated at: `2026-10-27T16:09:25Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 5614 |
| official_fill_required | 2564 |
| core_exclusion_candidate | 514 |

## Top Classes

| Value | Rows |
|---|---:|
| official_reference_unmatched_source_gap | 5609 |
| official_reference_symbol_collision_gap | 1171 |
| otc_sector_source_gap | 552 |
| official_industry_taxonomy_unavailable_gap | 450 |
| fund_or_trust_identifier_gap | 299 |
| official_identifier_not_exposed_source_gap | 181 |
| debt_or_securitized_identifier_gap | 92 |
| official_product_taxonomy_unavailable_gap | 68 |
| exchange_industry_source_gap | 59 |
| equity_etf_category_gap | 43 |
| adr_cdr_or_depositary_identifier_gap | 42 |
| capital_pool_or_halted_identifier_gap | 32 |
| fixed_income_etf_category_gap | 20 |
| inactive_or_legacy_identifier_gap | 20 |
| fundlike_stock_sector_gap | 18 |
| official_identifier_reference_unmatched_gap | 11 |
| commodity_etf_category_gap | 8 |
| shell_or_cpc_sector_gap | 6 |
| adr_cdr_or_depositary_sector_gap | 5 |
| digital_asset_etf_category_gap | 2 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- Extended OTC official-source gaps are accepted only as blank, review-gated residuals; this does not authorize a metadata fill.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
