# ASX Scope Review Queue

Generated at: `2026-05-29T12:20:12Z`

This queue isolates ASX `core_exclusion_candidate` rows that must be decided as core, extended, or exclude before ASX ISIN or ETF-category work. It does not apply data changes.

## Summary

- Scope review rows: `94`

## Scope Review Queues

| Queue | Rows |
|---|---:|
| asx_debt_or_securitized_scope_review | 59 |
| asx_fund_or_trust_scope_review | 26 |
| asx_inactive_or_legacy_scope_review | 9 |

## Evidence Status

| Signal | Value | Rows |
|---|---|---:|
| listing_history_status | active | 94 |
| current_scope_reason | primary_listing_missing_isin | 94 |
| ohlcv_plausibility_status | not_checked | 6 |
| ohlcv_plausibility_status | not_sampled | 88 |

## Top Scope Review Batches

| Queue | Official source | Rows | Evidence required | Recommended next source | Source gate |
|---|---|---:|---|---|---|
| asx_debt_or_securitized_scope_review | asx_listed_companies | 58 | official_asx_registry_issuer_trustee_or_prospectus_evidence_plus_scope_decision | asx_listed_companies, trustee page, prospectus, registry, or official product evidence for the exact listing. | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| asx_fund_or_trust_scope_review | asx_listed_companies | 25 | official_asx_investment_product_issuer_pds_or_registry_evidence_plus_scope_decision | asx_listed_companies, issuer/sponsor product page, PDS, or registry evidence for the exact listing. | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| asx_inactive_or_legacy_scope_review | asx_listed_companies | 9 | current_active_or_inactive_official_asx_listing_evidence_plus_scope_decision | asx_listed_companies, issuer notice, ASX announcement, or official inactive/delisting evidence. | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| asx_debt_or_securitized_scope_review | none | 1 | official_asx_registry_issuer_trustee_or_prospectus_evidence_plus_scope_decision | current official ASX, registry, or issuer source, trustee page, prospectus, registry, or official product evidence for the exact listing. | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| asx_fund_or_trust_scope_review | none | 1 | official_asx_investment_product_issuer_pds_or_registry_evidence_plus_scope_decision | current official ASX, registry, or issuer source, issuer/sponsor product page, PDS, or registry evidence for the exact listing. | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |

## Rows

| Listing key | Queue | Asset type | Gap class | Name | Source gate |
|---|---|---|---|---|---|
| ASX::AC2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | ALLIED CREDIT ABS TRUST 2025-1P | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AC3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | ALLIED CREDIT ABS TRUST 2025-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AF2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | ANGLE ASSET FINANCE - RADIAN TRUST 2025-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AF4 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | AFG 2024-1 TRUST IN RESPECT OF SERIES 2024-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AF5 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | AFG 2025-1NC TRUST IN RESPECT OF SERIES 2025-1NC | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AF6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | AFG 2025-1 TRUST IN RESPECT OF SERIES 2025-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AF7 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | ANGLE ASSET FINANCE - RADIAN TRUST 2025-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AO2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | APOLLO SERIES 2017-2 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::AO3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | APOLLO SERIES 2018-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::BA2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2019-1 REDS TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::BW6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | BLACKWATTLE SERIES RMBS TRUST NO.6 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::FM1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | FIRSTMAC MORTGAGE FUNDING TRUST NO.4 SERIES 4-2019 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::FM2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | FIRSTMAC MORTGAGE FUNDING TRUST NO.4 SERIES 2-2019 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::FM3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | FIRSTMAC MORTGAGE FUNDING TRUST NO. 4 SERIES 1-2020 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::FM5 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | FIRSTMAC MORTGAGE FUNDING TRUST NO.4 SERIES 2-2020 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::KI1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | KINGFISHER TRUST 2019-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::KIG | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | KINGFISHER TRUST 2016-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LI8 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LION SERIES 2020-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LN1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LION SERIES 2022-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LN2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LION SERIES 2023-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LN3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LION SERIES 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LP1 | asx_debt_or_securitized_scope_review | Stock | debt_or_securitized_identifier_gap | LIBERTY PRIME SERIES 2021-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LR1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2023-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LR3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2024-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LR4 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2023-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LR5 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2024-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LR6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2024-3 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::LT9 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | LA TROBE FINANCIAL CAPITAL MARKETS TRUST 2022-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::ML2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MA MONEY RESIDENTIAL SECURITISATION TRUST 2025-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::MM2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MME AUTOPAY ABS 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::MZ1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MEDALLION TRUST SERIES 2017-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::MZ2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MEDALLION TRUST SERIES 2017-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::MZF | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MEDALLION TRUST SERIES 2016-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::MZT | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | MEDALLION TRUST SERIES 2016-2 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::NW1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | NOW TRUST 2024-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::OR2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | ORDE SERIES 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2023-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2024-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA3 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2023-3 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA4 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2024-3 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA5 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2025-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PA6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PANORAMA AUTO TRUST 2025-3 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PS2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PUMA SERIES 2024-2 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PUT | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PUMA SERIES 2023-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PUU | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PUMA SERIES 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::PUV | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | PUMA SERIES 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::RA2 | asx_debt_or_securitized_scope_review | Stock | debt_or_securitized_identifier_gap | RESIMAC PREMIER SERIES 2020-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::RA3 | asx_debt_or_securitized_scope_review | Stock | debt_or_securitized_identifier_gap | RESIMAC SERIES 2024-1NC | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::RM1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | NATIONAL RMBS TRUST 2024-1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::SP4 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SAPPHIRE XXIX SERIES 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::SP6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SAPPHIRE XXXI SERIES 2024-3 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::TT1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | THINK TANK RESIDENTIAL SERIES 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::TT2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | THINK TANK RESIDENTIAL SERIES 2025-2 TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::TT6 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | TRITON BOND TRUST 2021-1 IN RESPECT OF SERIES 1 | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::WB1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2020-1 WST TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::WE1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2019-1 WST TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::WS1 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2024-1 WST TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::WS2 | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2024-2 WST TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::WSE | asx_debt_or_securitized_scope_review | ETF | debt_or_securitized_identifier_gap | SERIES 2021-1 WST TRUST | No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed. |
| ASX::BS1 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | BLUESTONE PRIME 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::D10 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | DRIVER AUSTRALIA TEN TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::DA8 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | DRIVER AUSTRALIA EIGHT TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::DA9 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | DRIVER AUSTRALIA NINE TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::EBTC | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | Global X 21Shares Bitcoin ETF | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::HC1 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | HOUSEHOLD CAPITAL 2025-1 RMBS TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::MF3 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | METRO FINANCE 2023-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::MF4 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | METRO FINANCE 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::MF5 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | METRO FINANCE 2023-2 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::MF6 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | METRO FINANCE 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::MM4 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | MME PL 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::OY1 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | OLYMPUS 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::OYS | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | OLYMPUS 2024-2 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POB | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2021-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POC | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2022-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POE | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2023-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POF | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2022-2 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POG | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2024-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POH | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2023-2 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::POI | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PROGRESS 2025-1 TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::PR4 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PEPPER RESIDENTIAL SECURITIES TRUST NO. 40 | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::PU6 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PEPPER SPARKZ TRUST NO.6 | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::PU8 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PEPPER SPARKZ TRUST NO.8 | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::PU9 | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | PEPPER SPARKZ TRUST NO.9 | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::SCA | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | SCENTRE GROUP TRUST 1 AND SCENTRE GROUP TRUST 2 | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::VCD | asx_fund_or_trust_scope_review | ETF | fund_or_trust_identifier_gap | VICINITY CENTRES TRUST | No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed. |
| ASX::AN3 | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | AUSTRALIA AND NEW ZEALAND BANKING GROUP LIMITED. | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::DMN | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | DOMINION INVESTMENT GROUP LIMITED | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::IF1 | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | IBERDROLA FINANZAS, S.A.U. | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::MA2 | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | MA CREDIT PORTFOLIO HOLDINGS LIMITED | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::MBL | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | MACQUARIE BANK LIMITED | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::QNB | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | QNB FINANCE LTD | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::SC1 | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | SHINHAN CARD CO., LTD. | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::SFV | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | SANTOS FINANCE LIMITED | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |
| ASX::TA1 | asx_inactive_or_legacy_scope_review | Stock | inactive_or_legacy_identifier_gap | TRANSURBAN FINANCE COMPANY PTY LTD | Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence. |

## Policy

- Do not fill ASX ISINs, ETF categories, sectors, names, symbols, listing status, or scope from this queue alone.
- Close a row only with exact listing-keyed official evidence and an explicit scope decision.
