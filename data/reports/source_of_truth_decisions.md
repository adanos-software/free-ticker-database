# Source-of-Truth Decisions

Generated at: `2026-05-11T05:57:38Z`

This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.

## Outcomes

| Value | Rows |
|---|---:|
| official_fill_required | 2343 |
| accepted_source_gap | 843 |
| core_exclusion_candidate | 714 |

## Top Classes

| Value | Rows |
|---|---:|
| exchange_industry_source_gap | 1769 |
| otc_sector_source_gap | 840 |
| official_identifier_source_gap | 483 |
| fund_or_trust_identifier_gap | 295 |
| inactive_or_legacy_identifier_gap | 158 |
| generic_etf_category_source_gap | 91 |
| debt_or_securitized_identifier_gap | 83 |
| fundlike_stock_sector_gap | 76 |
| adr_cdr_or_depositary_identifier_gap | 45 |
| capital_pool_or_halted_identifier_gap | 35 |
| shell_or_cpc_sector_gap | 22 |
| commodity_etf_category_gap | 1 |
| equity_etf_category_gap | 1 |
| fixed_income_etf_category_gap | 1 |

## Policy

- `official_fill_required`: get a source/parser or reviewed override before filling.
- `accepted_source_gap`: keep the blank value as a documented source gap.
- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.
- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.
