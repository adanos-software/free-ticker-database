# Source-of-Truth Decisions

Generated at: `2026-05-11T06:24:51Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| accepted_source_gap | 2597 |
| core_exclusion_candidate | 818 |
| official_fill_required | 445 |

## Top Classes

| Value | Rows |
|---|---:|
| official_industry_taxonomy_unavailable_gap | 1664 |
| otc_sector_source_gap | 840 |
| official_identifier_source_gap | 354 |
| fund_or_trust_identifier_gap | 294 |
| inactive_or_legacy_identifier_gap | 158 |
| generic_etf_category_source_gap | 91 |
| official_identifier_not_exposed_source_gap | 90 |
| shell_or_cpc_sector_gap | 90 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 74 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| adr_cdr_or_depositary_sector_gap | 39 |
| capital_pool_or_halted_identifier_gap | 35 |
| commodity_etf_category_gap | 1 |
| equity_etf_category_gap | 1 |
| fixed_income_etf_category_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
