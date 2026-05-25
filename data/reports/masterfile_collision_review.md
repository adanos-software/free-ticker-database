# Masterfile Collision Review

Generated: 2026-05-25T12:53:29Z

This is a review queue only. It does not authorize symbol-only additions.

## Summary

- Rows: 11107
- Asset-type mismatches: 1612
- Direct listing add allowed rows: 0
- Symbol-only resolution authorized: `false`

## Identity Resolution Backlog

- Status: `identity_resolution_review_queue_open`
- Rows: `11107`
- Same-ISIN exact-name scope review rows: `329`
- Same-ISIN name/scope reconciliation rows: `3805`
- Distinct official-ISIN listing-add review rows: `2177`
- Asset-type conflict blocked rows: `1612`
- Symbol-only non-symbol identity required rows: `3184`
- Source gate: Masterfile collision rows remain review queues only; listing additions, merges, renames, or enrichments require official non-symbol identity evidence for the target listing.

## Identity Resolution Queues

| Queue | Rows |
| --- | ---: |
| blocked_asset_type_conflict | 1612 |
| blocked_symbol_only_missing_non_symbol_identity | 3184 |
| review_cross_listing_same_isin_exact_name | 329 |
| review_cross_listing_same_isin_name_or_scope_gap | 3805 |
| review_distinct_official_isin_new_listing | 2177 |

## Identity Resolution Clearance Readiness

| Readiness | Rows |
| --- | ---: |
| blocked_symbol_only_non_symbol_identity_required | 3184 |
| blocked_until_asset_type_conflict_resolved | 1612 |
| needs_official_listing_add_review | 2177 |
| needs_official_name_or_scope_reconciliation | 3805 |
| review_ready_same_isin_exact_name_scope_check | 329 |

## Top Clearance Batches

| Queue | Readiness | Rows | Review Strategy | Evidence Required | Source Gate |
| --- | --- | ---: | --- | --- | --- |
| review_cross_listing_same_isin_name_or_scope_gap | needs_official_name_or_scope_reconciliation | 3805 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| blocked_symbol_only_missing_non_symbol_identity | blocked_symbol_only_non_symbol_identity_required | 3184 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Keep absent; ticker equality alone is not identity evidence. |
| review_distinct_official_isin_new_listing | needs_official_listing_add_review | 2177 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| blocked_asset_type_conflict | blocked_until_asset_type_conflict_resolved | 1612 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Block identity resolution until official instrument-type evidence resolves the conflict. |
| review_cross_listing_same_isin_exact_name | review_ready_same_isin_exact_name_scope_check | 329 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Do not add or merge until both official exchange directories confirm the same active instrument. |

## Same-ISIN Exact-Name Scope Review Batches

Rows: 329

| Exchange Pair | Official Source | Asset Type | Clearance Evidence | Rows | Recommended Next Source | Source Gate |
| --- | --- | --- | --- | ---: | --- | --- |
| Euronext::LSE | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 135 | Official active-listing directories for both exchanges in Euronext::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::XETRA | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 70 | Official active-listing directories for both exchanges in Euronext::XETRA. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::AMS | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 42 | Official active-listing directories for both exchanges in Euronext::AMS. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| AMS::LSE | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 36 | Official active-listing directories for both exchanges in AMS::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| BSE_IN::NSE_IN | bse_india_scrips | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 10 | Official active-listing directories for both exchanges in BSE_IN::NSE_IN. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::SIX | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 10 | Official active-listing directories for both exchanges in Euronext::SIX. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| LSE::NYSE | lse_price_explorer | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 9 | Official active-listing directories for both exchanges in LSE::NYSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| AMS::XETRA | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 5 | Official active-listing directories for both exchanges in AMS::XETRA. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| LSE::NASDAQ | lse_price_explorer | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 2 | Official active-listing directories for both exchanges in LSE::NASDAQ. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| ADX::NASDAQ | adx_market_watch | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in ADX::NASDAQ. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| CSE_MA::Euronext | cse_ma_listed_companies | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in CSE_MA::Euronext. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::ISE | euronext_equities | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in Euronext::ISE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::LSE | euronext_equities | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in Euronext::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::NASDAQ | euronext_equities | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in Euronext::NASDAQ. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::OSL | euronext_etfs | ETF | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in Euronext::OSL. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| Euronext::OTC | euronext_equities | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in Euronext::OTC. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| ISE::LSE | euronext_equities | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in ISE::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| LSE::TSX | lse_price_explorer | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in LSE::TSX. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| PSE::NYSE | pse_listed_company_directory | Stock | official_target_exchange_listing_status_mic_name_instrument_type | 1 | Official active-listing directories for both exchanges in PSE::NYSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |

## Identity Resolution By Asset Type

| Queue | Asset Type | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | ETF | 639 |
| blocked_asset_type_conflict | Stock | 973 |
| blocked_symbol_only_missing_non_symbol_identity | ETF | 154 |
| blocked_symbol_only_missing_non_symbol_identity | Stock | 3030 |
| review_cross_listing_same_isin_exact_name | ETF | 300 |
| review_cross_listing_same_isin_exact_name | Stock | 29 |
| review_cross_listing_same_isin_name_or_scope_gap | ETF | 2627 |
| review_cross_listing_same_isin_name_or_scope_gap | Stock | 1178 |
| review_distinct_official_isin_new_listing | ETF | 240 |
| review_distinct_official_isin_new_listing | Stock | 1937 |

## Identity Resolution By Official Source

| Queue | Official Source | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | adx_market_watch | 7 |
| blocked_asset_type_conflict | bahrain_bourse_listed_companies | 1 |
| blocked_asset_type_conflict | bist_kap_mkk_listed_securities | 2 |
| blocked_asset_type_conflict | boursa_kuwait_stocks | 3 |
| blocked_asset_type_conflict | bse_india_scrips | 43 |
| blocked_asset_type_conflict | bvb_shares_directory | 22 |
| blocked_asset_type_conflict | cboe_canada_listing_directory | 23 |
| blocked_asset_type_conflict | cse_ma_listed_companies | 1 |
| blocked_asset_type_conflict | deutsche_boerse_xetra_all_tradable_equities | 110 |
| blocked_asset_type_conflict | dfm_listed_securities | 1 |
| blocked_asset_type_conflict | euronext_equities | 84 |
| blocked_asset_type_conflict | euronext_etfs | 176 |
| blocked_asset_type_conflict | hkex_securities_list | 82 |
| blocked_asset_type_conflict | hnx_listed_securities | 29 |
| blocked_asset_type_conflict | idx_company_profiles | 77 |
| blocked_asset_type_conflict | jpx_listed_issues | 100 |
| blocked_asset_type_conflict | lse_price_explorer | 235 |
| blocked_asset_type_conflict | muscat_securities_companies | 2 |
| blocked_asset_type_conflict | nasdaq_listed | 29 |
| blocked_asset_type_conflict | nasdaq_other_listed | 45 |
| blocked_asset_type_conflict | nse_india_securities_available | 28 |
| blocked_asset_type_conflict | nse_ke_listed_companies | 5 |
| blocked_asset_type_conflict | nzx_instruments | 26 |
| blocked_asset_type_conflict | otc_markets_stock_screener | 9 |
| blocked_asset_type_conflict | pse_listed_company_directory | 28 |
| blocked_asset_type_conflict | psx_dps_symbols | 24 |
| blocked_asset_type_conflict | sec_company_tickers_exchange | 75 |
| blocked_asset_type_conflict | set_stock_search | 51 |
| blocked_asset_type_conflict | sgx_securities_prices | 45 |
| blocked_asset_type_conflict | tadawul_main_market_watch | 19 |
| blocked_asset_type_conflict | tmx_listed_issuers | 148 |
| blocked_asset_type_conflict | twse_listed_companies | 3 |
| blocked_asset_type_conflict | upcom_registered_securities | 79 |
| blocked_symbol_only_missing_non_symbol_identity | cboe_canada_listing_directory | 62 |
| blocked_symbol_only_missing_non_symbol_identity | idx_company_profiles | 166 |
| blocked_symbol_only_missing_non_symbol_identity | jpx_listed_issues | 1087 |
| blocked_symbol_only_missing_non_symbol_identity | muscat_securities_companies | 12 |
| blocked_symbol_only_missing_non_symbol_identity | nasdaq_listed | 46 |
| blocked_symbol_only_missing_non_symbol_identity | nasdaq_other_listed | 171 |
| blocked_symbol_only_missing_non_symbol_identity | otc_markets_stock_screener | 27 |
| blocked_symbol_only_missing_non_symbol_identity | psx_dps_symbols | 128 |
| blocked_symbol_only_missing_non_symbol_identity | qse_market_watch | 2 |
| blocked_symbol_only_missing_non_symbol_identity | sec_company_tickers_exchange | 309 |
| blocked_symbol_only_missing_non_symbol_identity | set_stock_search | 298 |
| blocked_symbol_only_missing_non_symbol_identity | tmx_listed_issuers | 868 |
| blocked_symbol_only_missing_non_symbol_identity | twse_listed_companies | 8 |
| review_cross_listing_same_isin_exact_name | adx_market_watch | 1 |
| review_cross_listing_same_isin_exact_name | bse_india_scrips | 10 |
| review_cross_listing_same_isin_exact_name | cse_ma_listed_companies | 1 |
| review_cross_listing_same_isin_exact_name | euronext_equities | 5 |
| review_cross_listing_same_isin_exact_name | euronext_etfs | 299 |
| review_cross_listing_same_isin_exact_name | lse_price_explorer | 12 |
| review_cross_listing_same_isin_exact_name | pse_listed_company_directory | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | adx_market_watch | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | bse_india_scrips | 474 |
| review_cross_listing_same_isin_name_or_scope_gap | bvb_shares_directory | 18 |
| review_cross_listing_same_isin_name_or_scope_gap | cse_ma_listed_companies | 27 |
| review_cross_listing_same_isin_name_or_scope_gap | deutsche_boerse_xetra_all_tradable_equities | 592 |
| review_cross_listing_same_isin_name_or_scope_gap | dfm_listed_securities | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | euronext_equities | 200 |
| review_cross_listing_same_isin_name_or_scope_gap | euronext_etfs | 1591 |
| review_cross_listing_same_isin_name_or_scope_gap | hnx_listed_securities | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | lse_price_explorer | 704 |
| review_cross_listing_same_isin_name_or_scope_gap | nse_india_securities_available | 112 |
| review_cross_listing_same_isin_name_or_scope_gap | nse_ke_listed_companies | 10 |
| review_cross_listing_same_isin_name_or_scope_gap | nzx_instruments | 38 |
| review_cross_listing_same_isin_name_or_scope_gap | pse_listed_company_directory | 20 |
| review_cross_listing_same_isin_name_or_scope_gap | sgx_securities_prices | 16 |
| review_distinct_official_isin_new_listing | adx_market_watch | 24 |
| review_distinct_official_isin_new_listing | bahrain_bourse_listed_companies | 8 |
| review_distinct_official_isin_new_listing | bist_kap_mkk_listed_securities | 18 |
| review_distinct_official_isin_new_listing | boursa_kuwait_stocks | 24 |
| review_distinct_official_isin_new_listing | bse_india_scrips | 225 |
| review_distinct_official_isin_new_listing | bvb_shares_directory | 81 |
| review_distinct_official_isin_new_listing | cse_ma_listed_companies | 8 |
| review_distinct_official_isin_new_listing | deutsche_boerse_xetra_all_tradable_equities | 94 |
| review_distinct_official_isin_new_listing | dfm_listed_securities | 14 |
| review_distinct_official_isin_new_listing | euronext_equities | 209 |
| review_distinct_official_isin_new_listing | euronext_etfs | 60 |
| review_distinct_official_isin_new_listing | hnx_listed_securities | 126 |
| review_distinct_official_isin_new_listing | krx_listed_companies | 3 |
| review_distinct_official_isin_new_listing | lse_price_explorer | 233 |
| review_distinct_official_isin_new_listing | nse_india_securities_available | 150 |
| review_distinct_official_isin_new_listing | nse_ke_listed_companies | 9 |
| review_distinct_official_isin_new_listing | nzx_instruments | 62 |
| review_distinct_official_isin_new_listing | pse_listed_company_directory | 136 |
| review_distinct_official_isin_new_listing | sgx_securities_prices | 81 |
| review_distinct_official_isin_new_listing | tadawul_main_market_watch | 198 |
| review_distinct_official_isin_new_listing | twse_listed_companies | 25 |
| review_distinct_official_isin_new_listing | upcom_registered_securities | 389 |

## Identity Resolution By Target/Existing Exchange Pair

| Queue | Target/Existing Pair | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | LSE::NASDAQ | 82 |
| blocked_asset_type_conflict | HKEX::TWSE | 77 |
| blocked_asset_type_conflict | TSE::TWSE | 63 |
| blocked_asset_type_conflict | LSE::NYSE ARCA | 61 |
| blocked_asset_type_conflict | Euronext::NASDAQ | 56 |
| blocked_asset_type_conflict | NYSE::NYSE ARCA | 50 |
| blocked_asset_type_conflict | TSXV::NYSE ARCA | 46 |
| blocked_asset_type_conflict | XETRA::NASDAQ | 40 |
| blocked_asset_type_conflict | Euronext::NYSE | 39 |
| blocked_asset_type_conflict | LSE::OTC | 34 |
| blocked_asset_type_conflict | Euronext::NYSE ARCA | 31 |
| blocked_asset_type_conflict | TSX::NYSE ARCA | 30 |
| blocked_asset_type_conflict | UPCOM::NYSE ARCA | 30 |
| blocked_asset_type_conflict | TSE::TPEX | 28 |
| blocked_asset_type_conflict | XETRA::OTC | 27 |
| blocked_asset_type_conflict | Euronext::OTC | 24 |
| blocked_asset_type_conflict | IDX::LSE | 22 |
| blocked_asset_type_conflict | IDX::NYSE ARCA | 22 |
| blocked_asset_type_conflict | SET::NYSE ARCA | 20 |
| blocked_asset_type_conflict | UPCOM::TSX | 20 |
| blocked_asset_type_conflict | TSXV::NASDAQ | 19 |
| blocked_asset_type_conflict | TADAWUL::TSE | 17 |
| blocked_asset_type_conflict | LSE::NYSE | 16 |
| blocked_asset_type_conflict | NYSE::NYSE MKT | 14 |
| blocked_asset_type_conflict | AMS::NASDAQ | 13 |
| blocked_asset_type_conflict | BSE_IN::NYSE ARCA | 13 |
| blocked_asset_type_conflict | TSXV::BATS | 13 |
| blocked_asset_type_conflict | XETRA::NYSE ARCA | 13 |
| blocked_asset_type_conflict | Euronext::LSE | 12 |
| blocked_asset_type_conflict | LSE::BATS | 12 |
| blocked_asset_type_conflict | Euronext::BATS | 11 |
| blocked_asset_type_conflict | HNX::NYSE ARCA | 11 |
| blocked_asset_type_conflict | PSE::NYSE ARCA | 10 |
| blocked_asset_type_conflict | IDX::NASDAQ | 9 |
| blocked_asset_type_conflict | NASDAQ::LSE | 9 |
| blocked_asset_type_conflict | TSE::Bursa | 9 |
| blocked_asset_type_conflict | TSX::NASDAQ | 9 |
| blocked_asset_type_conflict | UPCOM::NASDAQ | 9 |
| blocked_asset_type_conflict | XETRA::LSE | 9 |
| blocked_asset_type_conflict | Euronext::XETRA | 8 |
| blocked_asset_type_conflict | HNX::NASDAQ | 8 |
| blocked_asset_type_conflict | NYSE::LSE | 8 |
| blocked_asset_type_conflict | OSL::NYSE ARCA | 8 |
| blocked_asset_type_conflict | SET::NASDAQ | 8 |
| blocked_asset_type_conflict | TSXV::LSE | 8 |
| blocked_asset_type_conflict | BVB::NYSE ARCA | 7 |
| blocked_asset_type_conflict | IDX::BATS | 7 |
| blocked_asset_type_conflict | NEO::NYSE | 7 |
| blocked_asset_type_conflict | NZX::LSE | 7 |
| blocked_asset_type_conflict | PSX::LSE | 7 |
| blocked_symbol_only_missing_non_symbol_identity | TSE::TPEX | 476 |
| blocked_symbol_only_missing_non_symbol_identity | TSE::TWSE | 347 |
| blocked_symbol_only_missing_non_symbol_identity | TSE::Bursa | 264 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::NYSE MKT | 218 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::NYSE | 191 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::NYSE | 130 |
| blocked_symbol_only_missing_non_symbol_identity | SET::NYSE | 111 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::LSE | 101 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::NASDAQ | 98 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::NASDAQ | 96 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::NYSE ARCA | 83 |
| blocked_symbol_only_missing_non_symbol_identity | IDX::NASDAQ | 60 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::ASX | 41 |
| blocked_symbol_only_missing_non_symbol_identity | SET::NASDAQ | 39 |
| blocked_symbol_only_missing_non_symbol_identity | SET::LSE | 32 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::LSE | 30 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::WSE | 25 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::XETRA | 25 |
| blocked_symbol_only_missing_non_symbol_identity | IDX::OTC | 24 |
| blocked_symbol_only_missing_non_symbol_identity | PSX::NASDAQ | 24 |
| blocked_symbol_only_missing_non_symbol_identity | IDX::LSE | 22 |
| blocked_symbol_only_missing_non_symbol_identity | PSX::NYSE | 20 |
| blocked_symbol_only_missing_non_symbol_identity | SET::ASX | 20 |
| blocked_symbol_only_missing_non_symbol_identity | SET::TSXV | 20 |
| blocked_symbol_only_missing_non_symbol_identity | IDX::NYSE | 19 |
| blocked_symbol_only_missing_non_symbol_identity | OTC::NASDAQ | 19 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::NYSE MKT | 19 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::TSXV | 18 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::Euronext | 17 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::NYSE MKT | 17 |
| blocked_symbol_only_missing_non_symbol_identity | SET::Euronext | 16 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::WSE | 15 |
| blocked_symbol_only_missing_non_symbol_identity | TSX::XETRA | 14 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::XETRA | 13 |
| blocked_symbol_only_missing_non_symbol_identity | PSX::LSE | 13 |
| blocked_symbol_only_missing_non_symbol_identity | NEO::BATS | 11 |
| blocked_symbol_only_missing_non_symbol_identity | NEO::NASDAQ | 11 |
| blocked_symbol_only_missing_non_symbol_identity | NEO::NYSE ARCA | 11 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::LSE | 10 |
| blocked_symbol_only_missing_non_symbol_identity | SET::WSE | 10 |
| blocked_symbol_only_missing_non_symbol_identity | IDX::TSXV | 9 |
| blocked_symbol_only_missing_non_symbol_identity | NASDAQ::ASX | 9 |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::TSX | 9 |
| blocked_symbol_only_missing_non_symbol_identity | PSX::ASX | 9 |
| blocked_symbol_only_missing_non_symbol_identity | SET::NYSE MKT | 9 |
| blocked_symbol_only_missing_non_symbol_identity | SET::TSX | 9 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::BME | 9 |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::OTC | 9 |
| blocked_symbol_only_missing_non_symbol_identity | NEO::XETRA | 8 |
| blocked_symbol_only_missing_non_symbol_identity | PSX::TSX | 8 |
| review_cross_listing_same_isin_exact_name | Euronext::LSE | 136 |
| review_cross_listing_same_isin_exact_name | Euronext::XETRA | 70 |
| review_cross_listing_same_isin_exact_name | Euronext::AMS | 42 |
| review_cross_listing_same_isin_exact_name | AMS::LSE | 36 |
| review_cross_listing_same_isin_exact_name | BSE_IN::NSE_IN | 10 |
| review_cross_listing_same_isin_exact_name | Euronext::SIX | 10 |
| review_cross_listing_same_isin_exact_name | LSE::NYSE | 9 |
| review_cross_listing_same_isin_exact_name | AMS::XETRA | 5 |
| review_cross_listing_same_isin_exact_name | LSE::NASDAQ | 2 |
| review_cross_listing_same_isin_exact_name | ADX::NASDAQ | 1 |
| review_cross_listing_same_isin_exact_name | CSE_MA::Euronext | 1 |
| review_cross_listing_same_isin_exact_name | Euronext::ISE | 1 |
| review_cross_listing_same_isin_exact_name | Euronext::NASDAQ | 1 |
| review_cross_listing_same_isin_exact_name | Euronext::OSL | 1 |
| review_cross_listing_same_isin_exact_name | Euronext::OTC | 1 |
| review_cross_listing_same_isin_exact_name | ISE::LSE | 1 |
| review_cross_listing_same_isin_exact_name | LSE::TSX | 1 |
| review_cross_listing_same_isin_exact_name | PSE::NYSE | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::LSE | 661 |
| review_cross_listing_same_isin_name_or_scope_gap | BSE_IN::NSE_IN | 473 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::XETRA | 379 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::LSE | 379 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::XETRA | 212 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NASDAQ | 136 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NYSE ARCA | 130 |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::LSE | 118 |
| review_cross_listing_same_isin_name_or_scope_gap | NSE_IN::BSE_IN | 112 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::SIX | 100 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::NYSE ARCA | 91 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::NASDAQ | 88 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::NYSE | 64 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::NYSE | 62 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::AMS | 59 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::AMS | 59 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NYSE | 58 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::BATS | 57 |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::XETRA | 54 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::NYSE ARCA | 43 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::NASDAQ | 38 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::BATS | 34 |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::NYSE ARCA | 23 |
| review_cross_listing_same_isin_name_or_scope_gap | NZX::ASX | 21 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::BATS | 16 |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::NASDAQ | 14 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::TSX | 14 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::ASX | 14 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::TSX | 11 |
| review_cross_listing_same_isin_name_or_scope_gap | PSE::NYSE | 11 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::ASX | 10 |
| review_cross_listing_same_isin_name_or_scope_gap | OSL::NYSE | 10 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::OTC | 9 |
| review_cross_listing_same_isin_name_or_scope_gap | OSL::NASDAQ | 9 |
| review_cross_listing_same_isin_name_or_scope_gap | BVB::XETRA | 8 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::OTC | 8 |
| review_cross_listing_same_isin_name_or_scope_gap | OSL::LSE | 8 |
| review_cross_listing_same_isin_name_or_scope_gap | BVB::NYSE | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | CSE_MA::ASX | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | CSE_MA::NYSE | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NYSE MKT | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::SIX | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | NZX::NYSE | 7 |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::BATS | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::ASX | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::Euronext | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::Euronext | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::TSX | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::WSE | 6 |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::TSXV | 5 |
| review_distinct_official_isin_new_listing | TADAWUL::TSE | 126 |
| review_distinct_official_isin_new_listing | UPCOM::NYSE | 86 |
| review_distinct_official_isin_new_listing | LSE::NASDAQ | 75 |
| review_distinct_official_isin_new_listing | UPCOM::ASX | 58 |
| review_distinct_official_isin_new_listing | LSE::NYSE | 57 |
| review_distinct_official_isin_new_listing | UPCOM::LSE | 51 |
| review_distinct_official_isin_new_listing | Euronext::NYSE | 42 |
| review_distinct_official_isin_new_listing | PSE::NYSE | 41 |
| review_distinct_official_isin_new_listing | Euronext::NASDAQ | 40 |
| review_distinct_official_isin_new_listing | BSE_IN::NYSE | 36 |
| review_distinct_official_isin_new_listing | HNX::NYSE | 34 |
| review_distinct_official_isin_new_listing | TADAWUL::TWSE | 34 |
| review_distinct_official_isin_new_listing | UPCOM::WSE | 29 |
| review_distinct_official_isin_new_listing | LSE::NYSE ARCA | 28 |
| review_distinct_official_isin_new_listing | XETRA::NYSE ARCA | 27 |
| review_distinct_official_isin_new_listing | BSE_IN::ASX | 26 |
| review_distinct_official_isin_new_listing | Euronext::LSE | 26 |
| review_distinct_official_isin_new_listing | NSE_IN::NYSE | 25 |
| review_distinct_official_isin_new_listing | UPCOM::XETRA | 24 |
| review_distinct_official_isin_new_listing | UPCOM::TSXV | 23 |
| review_distinct_official_isin_new_listing | UPCOM::SET | 22 |
| review_distinct_official_isin_new_listing | LSE::BATS | 21 |
| review_distinct_official_isin_new_listing | TWSE::TSE | 21 |
| review_distinct_official_isin_new_listing | BVB::NYSE | 20 |
| review_distinct_official_isin_new_listing | TADAWUL::Bursa | 20 |
| review_distinct_official_isin_new_listing | BSE_IN::LSE | 19 |
| review_distinct_official_isin_new_listing | BSE_IN::NASDAQ | 19 |
| review_distinct_official_isin_new_listing | Euronext::ASX | 19 |
| review_distinct_official_isin_new_listing | XETRA::NASDAQ | 19 |
| review_distinct_official_isin_new_listing | NSE_IN::NASDAQ | 18 |
| review_distinct_official_isin_new_listing | NSE_IN::ASX | 17 |
| review_distinct_official_isin_new_listing | TADAWUL::TPEX | 17 |
| review_distinct_official_isin_new_listing | PSE::NASDAQ | 16 |
| review_distinct_official_isin_new_listing | UPCOM::NASDAQ | 16 |
| review_distinct_official_isin_new_listing | UPCOM::TSX | 16 |
| review_distinct_official_isin_new_listing | HNX::NASDAQ | 15 |
| review_distinct_official_isin_new_listing | PSE::LSE | 15 |
| review_distinct_official_isin_new_listing | SGX::NYSE | 15 |
| review_distinct_official_isin_new_listing | XETRA::BATS | 15 |
| review_distinct_official_isin_new_listing | XETRA::LSE | 15 |
| review_distinct_official_isin_new_listing | HNX::ASX | 14 |
| review_distinct_official_isin_new_listing | BSE_IN::PSX | 13 |
| review_distinct_official_isin_new_listing | BSE_IN::TSXV | 13 |
| review_distinct_official_isin_new_listing | Euronext::NYSE ARCA | 13 |
| review_distinct_official_isin_new_listing | BSE_IN::WSE | 12 |
| review_distinct_official_isin_new_listing | BVB::NASDAQ | 12 |
| review_distinct_official_isin_new_listing | Euronext::TSXV | 12 |
| review_distinct_official_isin_new_listing | Euronext::XETRA | 12 |
| review_distinct_official_isin_new_listing | Euronext::BATS | 11 |
| review_distinct_official_isin_new_listing | HNX::WSE | 11 |

## Top Pair Review Batches

| Queue | Exchange Pair | Rows | Review Strategy | Evidence Required | Recommended Next Source | Source Gate |
| --- | --- | ---: | --- | --- | --- | --- |
| blocked_asset_type_conflict | LSE::NASDAQ | 82 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for LSE::NASDAQ. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | HKEX::TWSE | 77 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for HKEX::TWSE. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | TSE::TWSE | 63 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for TSE::TWSE. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | LSE::NYSE ARCA | 61 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for LSE::NYSE ARCA. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | Euronext::NASDAQ | 56 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for Euronext::NASDAQ. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | NYSE::NYSE ARCA | 50 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for NYSE::NYSE ARCA. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | TSXV::NYSE ARCA | 46 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for TSXV::NYSE ARCA. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | XETRA::NASDAQ | 40 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for XETRA::NASDAQ. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | Euronext::NYSE | 39 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for Euronext::NYSE. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_asset_type_conflict | LSE::OTC | 34 | batch_block_instrument_type_conflict_until_official_resolution | official_instrument_type_resolution_before_listing_identity_review | Official instrument-type evidence resolving the asset-type conflict for LSE::OTC. | Block identity resolution until official instrument-type evidence resolves the conflict. |
| blocked_symbol_only_missing_non_symbol_identity | TSE::TPEX | 476 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSE::TPEX, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSE::TWSE | 347 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSE::TWSE, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSE::Bursa | 264 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSE::Bursa, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | NYSE::NYSE MKT | 218 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for NYSE::NYSE MKT, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSX::NYSE | 191 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSX::NYSE, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::NYSE | 130 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSXV::NYSE, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | SET::NYSE | 111 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for SET::NYSE, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::LSE | 101 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSXV::LSE, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSXV::NASDAQ | 98 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSXV::NASDAQ, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| blocked_symbol_only_missing_non_symbol_identity | TSX::NASDAQ | 96 | batch_hold_symbol_reuse_until_non_symbol_identity_source | official_non_symbol_identifier_or_keep_absent | Official non-symbol identifier evidence for TSX::NASDAQ, or keep the target listing absent. | Keep absent; ticker equality alone is not identity evidence. |
| review_cross_listing_same_isin_exact_name | Euronext::LSE | 136 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in Euronext::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | Euronext::XETRA | 70 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in Euronext::XETRA. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | Euronext::AMS | 42 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in Euronext::AMS. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | AMS::LSE | 36 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in AMS::LSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | BSE_IN::NSE_IN | 10 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in BSE_IN::NSE_IN. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | Euronext::SIX | 10 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in Euronext::SIX. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | LSE::NYSE | 9 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in LSE::NYSE. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | AMS::XETRA | 5 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in AMS::XETRA. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | LSE::NASDAQ | 2 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in LSE::NASDAQ. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_exact_name | ADX::NASDAQ | 1 | batch_review_same_isin_exact_name_cross_listing_scope | official_target_and_existing_exchange_directories_confirm_active_same_instrument | Official active-listing directories for both exchanges in ADX::NASDAQ. | Do not add or merge until both official exchange directories confirm the same active instrument. |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::LSE | 661 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for Euronext::LSE. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | BSE_IN::NSE_IN | 473 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for BSE_IN::NSE_IN. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::XETRA | 379 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for Euronext::XETRA. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | XETRA::LSE | 379 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for XETRA::LSE. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::XETRA | 212 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for LSE::XETRA. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NASDAQ | 136 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for LSE::NASDAQ. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | LSE::NYSE ARCA | 130 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for LSE::NYSE ARCA. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | AMS::LSE | 118 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for AMS::LSE. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | NSE_IN::BSE_IN | 112 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for NSE_IN::BSE_IN. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_cross_listing_same_isin_name_or_scope_gap | Euronext::SIX | 100 | batch_review_same_isin_name_or_scope_reconciliation | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | Official target-exchange status plus issuer/name or scope reconciliation for Euronext::SIX. | Do not resolve identity until official listing status and issuer/name or scope differences are reconciled. |
| review_distinct_official_isin_new_listing | TADAWUL::TSE | 126 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for TADAWUL::TSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | UPCOM::NYSE | 86 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for UPCOM::NYSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | LSE::NASDAQ | 75 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for LSE::NASDAQ with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | UPCOM::ASX | 58 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for UPCOM::ASX with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | LSE::NYSE | 57 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for LSE::NYSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | UPCOM::LSE | 51 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for UPCOM::LSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | Euronext::NYSE | 42 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for Euronext::NYSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | PSE::NYSE | 41 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for PSE::NYSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | Euronext::NASDAQ | 40 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for Euronext::NASDAQ with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |
| review_distinct_official_isin_new_listing | BSE_IN::NYSE | 36 | batch_review_distinct_isin_new_listing_candidates | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | Official target-exchange listing record for BSE_IN::NYSE with listing key, ISIN, name, type, and status. | Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status. |

## Identity Resolution Review Strategies

| Queue | Strategy | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | batch_block_instrument_type_conflict_until_official_resolution | 1612 |
| blocked_symbol_only_missing_non_symbol_identity | batch_hold_symbol_reuse_until_non_symbol_identity_source | 3184 |
| review_cross_listing_same_isin_exact_name | batch_review_same_isin_exact_name_cross_listing_scope | 329 |
| review_cross_listing_same_isin_name_or_scope_gap | batch_review_same_isin_name_or_scope_reconciliation | 3805 |
| review_distinct_official_isin_new_listing | batch_review_distinct_isin_new_listing_candidates | 2177 |

## Identity Resolution Evidence Required

| Queue | Evidence Required | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | official_instrument_type_resolution_before_listing_identity_review | 1612 |
| blocked_symbol_only_missing_non_symbol_identity | official_non_symbol_identifier_or_keep_absent | 3184 |
| review_cross_listing_same_isin_exact_name | official_target_and_existing_exchange_directories_confirm_active_same_instrument | 329 |
| review_cross_listing_same_isin_name_or_scope_gap | official_target_exchange_status_plus_issuer_or_name_scope_reconciliation | 3805 |
| review_distinct_official_isin_new_listing | official_target_exchange_listing_key_isin_name_instrument_type_listing_status | 2177 |

## Identity Resolution Identity Evidence

| Queue | Identity Evidence | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | asset_type_conflict | 1612 |
| blocked_asset_type_conflict | exact_name_match | 4 |
| blocked_asset_type_conflict | official_isin | 1029 |
| blocked_asset_type_conflict | same_isin_existing_listing | 444 |
| blocked_symbol_only_missing_non_symbol_identity | asset_type_consistent | 3184 |
| blocked_symbol_only_missing_non_symbol_identity | exact_name_match | 166 |
| review_cross_listing_same_isin_exact_name | asset_type_consistent | 329 |
| review_cross_listing_same_isin_exact_name | exact_name_match | 329 |
| review_cross_listing_same_isin_exact_name | official_isin | 329 |
| review_cross_listing_same_isin_exact_name | same_isin_existing_listing | 329 |
| review_cross_listing_same_isin_name_or_scope_gap | asset_type_consistent | 3805 |
| review_cross_listing_same_isin_name_or_scope_gap | official_isin | 3805 |
| review_cross_listing_same_isin_name_or_scope_gap | same_isin_existing_listing | 3805 |
| review_distinct_official_isin_new_listing | asset_type_consistent | 2177 |
| review_distinct_official_isin_new_listing | exact_name_match | 3 |
| review_distinct_official_isin_new_listing | official_isin | 2177 |

## Identity Resolution Risk Flags

| Queue | Risk Flag | Rows |
| --- | --- | ---: |
| blocked_asset_type_conflict | asset_type_mismatch | 1612 |
| blocked_asset_type_conflict | exact_name_match | 4 |
| blocked_asset_type_conflict | existing_isin_conflict | 863 |
| blocked_asset_type_conflict | missing_official_isin | 583 |
| blocked_asset_type_conflict | no_exact_name_match | 1608 |
| blocked_asset_type_conflict | same_isin_existing_listing | 444 |
| blocked_asset_type_conflict | ticker_reused_on_other_exchange | 1612 |
| blocked_symbol_only_missing_non_symbol_identity | exact_name_match | 166 |
| blocked_symbol_only_missing_non_symbol_identity | missing_official_isin | 3184 |
| blocked_symbol_only_missing_non_symbol_identity | multiple_existing_symbol_rows | 1 |
| blocked_symbol_only_missing_non_symbol_identity | no_exact_name_match | 3018 |
| blocked_symbol_only_missing_non_symbol_identity | ticker_reused_on_other_exchange | 3184 |
| review_cross_listing_same_isin_exact_name | exact_name_match | 329 |
| review_cross_listing_same_isin_exact_name | existing_isin_conflict | 2 |
| review_cross_listing_same_isin_exact_name | same_isin_existing_listing | 329 |
| review_cross_listing_same_isin_exact_name | ticker_reused_on_other_exchange | 329 |
| review_cross_listing_same_isin_name_or_scope_gap | existing_isin_conflict | 757 |
| review_cross_listing_same_isin_name_or_scope_gap | multiple_existing_symbol_rows | 1 |
| review_cross_listing_same_isin_name_or_scope_gap | no_exact_name_match | 3805 |
| review_cross_listing_same_isin_name_or_scope_gap | same_isin_existing_listing | 3805 |
| review_cross_listing_same_isin_name_or_scope_gap | ticker_reused_on_other_exchange | 3805 |
| review_distinct_official_isin_new_listing | exact_name_match | 3 |
| review_distinct_official_isin_new_listing | existing_isin_conflict | 2152 |
| review_distinct_official_isin_new_listing | no_exact_name_match | 2174 |
| review_distinct_official_isin_new_listing | ticker_reused_on_other_exchange | 2177 |

## Decisions

| Decision | Rows |
| --- | ---: |
| new_listing_candidate_requires_official_listing_add_review | 2762 |
| same_isin_cross_listing_candidate_requires_exchange_scope_review | 4578 |
| symbol_collision_requires_non_symbol_identity_source | 3767 |

## Review Buckets

| Priority | Bucket | Rows |
| --- | --- | ---: |
| P2 | distinct_official_isin_new_listing_candidate | 2177 |
| P3 | hold_symbol_only_collision_needs_non_symbol_identity | 3184 |
| P2 | resolve_asset_type_conflict_before_identity_review | 1612 |
| P2 | same_isin_cross_listing_needs_name_or_scope_review | 3805 |
| P1 | same_isin_exact_name_cross_listing_candidate | 329 |

## Clearance Evidence

| Evidence Gate | Rows |
| --- | ---: |
| official_instrument_type_resolution_before_listing_identity_review | 1612 |
| official_non_symbol_identifier_or_keep_absent | 3184 |
| official_target_exchange_listing_key_isin_name_instrument_type_listing_status | 2177 |
| official_target_exchange_listing_status_mic_name_instrument_type | 329 |
| official_target_exchange_listing_status_plus_name_or_scope_reconciliation | 3805 |

## Risk Flags

| Flag | Rows |
| --- | ---: |
| asset_type_mismatch | 1612 |
| exact_name_match | 502 |
| existing_isin_conflict | 3774 |
| missing_official_isin | 3767 |
| multiple_existing_symbol_rows | 2 |
| no_exact_name_match | 10605 |
| same_isin_existing_listing | 4578 |
| ticker_reused_on_other_exchange | 11107 |

## Top Exchanges

| Exchange | Rows |
| --- | ---: |
| Euronext | 2251 |
| TSE | 1187 |
| LSE | 1184 |
| XETRA | 796 |
| BSE_IN | 752 |
| TSXV | 556 |
| NYSE | 533 |
| UPCOM | 468 |
| TSX | 460 |
| SET | 349 |
| AMS | 301 |
| NSE_IN | 290 |
| IDX | 243 |
| TADAWUL | 217 |
| PSE | 185 |
| HNX | 156 |
| PSX | 152 |
| SGX | 142 |
| NZX | 126 |
| BVB | 121 |
| NEO | 85 |
| HKEX | 82 |
| NASDAQ | 75 |
| OSL | 66 |
| CSE_MA | 37 |

## Policy

- No ticker-only evidence may create, rename, merge, or enrich a listing.
- Official target-exchange evidence is required before any listing addition.
- Ambiguous or conflicting rows stay absent and remain review gaps.
