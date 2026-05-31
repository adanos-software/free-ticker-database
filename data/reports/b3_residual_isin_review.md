# B3 Residual ISIN Review

Generated at: `2026-05-25T12:54:45Z`

This report tracks the remaining B3 primary-listing ISIN gaps after the official B3 InstrumentsEquities refresh and dataset rebuild. It does not fill values.

## Summary

- Residual B3 ISIN gaps: `12`

## Residual Decisions

| Decision | Rows |
|---|---:|
| accepted_source_gap_not_in_current_b3_directory | 2 |
| core_exclusion_candidate_requires_scope_review | 10 |

## Current Scope Context

| Instrument scope | Rows |
|---|---:|
| core | 12 |

| Scope reason | Rows |
|---|---:|
| primary_listing_missing_isin | 12 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 10 |
| P3 | 2 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| not_in_current_b3_directory_source_gap | 2 |
| scope_review_before_identifier_fill | 10 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_core_or_extended_scope_decision | 10 |
| source_gap_keep_blank_until_official_identifier_evidence | 2 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| new_current_b3_directory_or_official_delisting_inactive_evidence | 2 |
| scope_decision_for_core_extended_or_exclude_before_identifier_fill | 10 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| decide_b3_core_extended_or_exclude_before_identifier_work | 10 |
| keep_blank_until_current_b3_directory_or_registry_evidence | 2 |

## Top Review Batches

| Priority | Bucket | Gap class | Asset type | Rows | Strategy | Evidence gate | Recommended next source | Source gate |
|---|---|---|---|---:|---|---|---|---|
| P1 | scope_review_before_identifier_fill | fund_or_trust_identifier_gap | ETF | 7 | decide_b3_core_extended_or_exclude_before_identifier_work | scope_decision_for_core_extended_or_exclude_before_identifier_fill | Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work. | No ISIN fill until the B3 listing is reviewed as core, extended, or excluded. |
| P1 | scope_review_before_identifier_fill | inactive_or_legacy_identifier_gap | Stock | 3 | decide_b3_core_extended_or_exclude_before_identifier_work | scope_decision_for_core_extended_or_exclude_before_identifier_fill | Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work. | No ISIN fill until the B3 listing is reviewed as core, extended, or excluded. |
| P3 | not_in_current_b3_directory_source_gap | official_current_directory_absent_identifier_gap | Stock | 2 | keep_blank_until_current_b3_directory_or_registry_evidence | new_current_b3_directory_or_official_delisting_inactive_evidence | Current official B3 directory, CSD/security registry, or official delisting/inactive evidence. | Keep ISIN blank unless the listing reappears in a current official directory or registry evidence. |

## Gap Classes

| Class | Rows |
|---|---:|
| fund_or_trust_identifier_gap | 7 |
| inactive_or_legacy_identifier_gap | 3 |
| official_current_directory_absent_identifier_gap | 2 |

## Official B3 Source Identifier Exposure

| Source | Rows | ISIN present | ISIN missing |
|---|---:|---:|---:|
| b3_bdr_etfs | 305 | 0 | 305 |
| b3_instruments_equities | 1941 | 1941 | 0 |
| b3_listed_etfs | 188 | 0 | 188 |

## Rows

| Listing key | Priority | Bucket | Current scope | Scope reason | Name | Class | COTAHIST | Decision |
|---|---|---|---|---|---|---|---|---|
| B3::AFOF11 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | Alianza Fofii Fundo De Investimento Imobiliario | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::AGCX11 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::AQLL11 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | ÁQUILLA FDO INV IMOB - FII | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::CPTS11B | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | Capitania Securities II Fundo Investimento Imobiliario FII | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::MBRF11 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | Fundo De Investimento Imobiliário Mercantil Do Brasil-fii -Inicio | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::NLFA11 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | Nu Letras Financeiras Anbima Classe De Índice - Responsabilidade Limitada | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::OULG11B | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | Pedra Dourada Fundo De Investimento Imobiliário - FII | fund_or_trust_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::C3RP3 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | COTRASA PARTICIPACOES S.A. | inactive_or_legacy_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::ICBR3 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | INTERCEMENT BRASIL S.A. | inactive_or_legacy_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::PASS5 | P1 | scope_review_before_identifier_fill | core | primary_listing_missing_isin | COMPASS GAS E ENERGIA S.A. | inactive_or_legacy_identifier_gap | no_cotahist_match | core_exclusion_candidate_requires_scope_review |
| B3::P5RD3 | P3 | not_in_current_b3_directory_source_gap | core | primary_listing_missing_isin | MATRIZ COMPANHIA METALURGICA PRADA | official_current_directory_absent_identifier_gap | no_cotahist_match | accepted_source_gap_not_in_current_b3_directory |
| B3::VRGL3 | P3 | not_in_current_b3_directory_source_gap | core | primary_listing_missing_isin | GOL LINHAS AEREAS S.A | official_current_directory_absent_identifier_gap | no_cotahist_match | accepted_source_gap_not_in_current_b3_directory |

## Policy

- No value in this report is inferred from ticker or issuer-name shape.
- Fill only after a direct official B3/CSD/fund source satisfies the row-level source gate.
