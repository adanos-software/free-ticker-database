# B3 Core Scope Review Queue

Generated at: `2026-05-29T12:10:58Z`

This queue isolates B3 `core_exclusion_candidate` rows that must be decided as core, extended, or exclude before identifier or category work. It does not apply data changes.

## Summary

- Scope review rows: `10`

## Scope Review Queues

| Queue | Rows |
|---|---:|
| b3_fund_or_trust_core_scope_review | 7 |
| b3_inactive_or_legacy_core_scope_review | 3 |

## Gap Classes

| Gap class | Rows |
|---|---:|
| fund_or_trust_identifier_gap | 7 |
| inactive_or_legacy_identifier_gap | 3 |

## Evidence Status

| Signal | Value | Rows |
|---|---|---:|
| masterfile_source_presence | absent_from_all_b3_masterfile_sources | 9 |
| masterfile_source_presence | present_only_in_non_exchange_directory_source | 1 |
| listing_history_status | active | 10 |
| ohlcv_plausibility_status | not_checked | 1 |
| ohlcv_plausibility_status | not_sampled | 9 |

## Rows

| Listing key | Queue | Asset type | Gap class | Name | Evidence required | Source gate |
|---|---|---|---|---|---|---|
| B3::AFOF11 | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | Alianza Fofii Fundo De Investimento Imobiliario | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::AGCX11 | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::AQLL11 | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | ÁQUILLA FDO INV IMOB - FII | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::CPTS11B | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | Capitania Securities II Fundo Investimento Imobiliario FII | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::MBRF11 | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::NLFA11 | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | Nu Letras Financeiras Anbima Classe De Índice - Responsabilidade Limitada | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::OULG11B | b3_fund_or_trust_core_scope_review | ETF | fund_or_trust_identifier_gap | Pedra Dourada Fundo De Investimento Imobiliário - FII | current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision | No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence. |
| B3::C3RP3 | b3_inactive_or_legacy_core_scope_review | Stock | inactive_or_legacy_identifier_gap | COTRASA PARTICIPACOES S.A. | current_active_b3_directory_or_official_inactive_delisting_evidence_plus_scope_decision | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| B3::ICBR3 | b3_inactive_or_legacy_core_scope_review | Stock | inactive_or_legacy_identifier_gap | INTERCEMENT BRASIL S.A. | current_active_b3_directory_or_official_inactive_delisting_evidence_plus_scope_decision | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| B3::PASS5 | b3_inactive_or_legacy_core_scope_review | Stock | inactive_or_legacy_identifier_gap | COMPASS GAS E ENERGIA S.A. | current_active_b3_directory_or_official_inactive_delisting_evidence_plus_scope_decision | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |

## Policy

- Do not fill B3 ISINs, categories, names, symbols, listing status, or scope from this queue alone.
- Close a row only with exact listing-keyed official evidence and an explicit scope decision.
