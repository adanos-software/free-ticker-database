# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 61466 |
| core_listings | 54030 |
| aliases | 121652 |
| stocks | 45911 |
| etfs | 15555 |
| isin_coverage | 59848 |
| sector_coverage | 58764 |
| stock_sector_coverage | 43311 |
| etf_category_coverage | 15453 |
| cik_coverage | 7740 |
| figi_coverage | 64316 |
| lei_coverage | 919 |
| listing_status_rows | 96060 |
| listing_status_intervals | 96060 |
| listing_events | 39701 |
| listing_keys | 71043 |
| instrument_scope_rows | 71043 |
| instrument_scope_core | 54030 |
| instrument_scope_extended | 17013 |
| instrument_scope_primary_listing | 53184 |
| instrument_scope_primary_listing_missing_isin | 846 |
| instrument_scope_otc_listing | 11054 |
| instrument_scope_secondary_cross_listing | 5959 |
| legacy_primary_ticker_collision_rows | 1 |
| official_masterfile_symbols | 78887 |
| official_masterfile_matches | 50981 |
| official_masterfile_collisions | 11170 |
| official_masterfile_missing | 16736 |
| official_full_exchanges | 47 |
| official_partial_exchanges | 32 |
| manual_only_exchanges | 0 |
| missing_exchanges | 1 |
| stock_verification_items | 52170 |
| stock_verification_verified | 46506 |
| stock_verification_reference_gap | 4281 |
| stock_verification_missing_from_official | 361 |
| stock_verification_name_mismatch | 974 |
| stock_verification_cross_exchange_collision | 7 |
| etf_verification_items | 18873 |
| etf_verification_verified | 17451 |
| etf_verification_reference_gap | 1359 |
| etf_verification_missing_from_official | 49 |
| etf_verification_name_mismatch | 7 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-05-31T03:58:05Z |
| tickers_age_hours | 48.24 |
| masterfiles_generated_at | 2026-06-02T04:11:58Z |
| masterfiles_age_hours | 0.01 |
| identifiers_generated_at | 2026-05-31T04:05:20Z |
| identifiers_age_hours | 48.12 |
| listing_history_observed_at | 2026-05-31T03:58:05Z |
| listing_history_age_hours | 48.24 |
| latest_verification_run | data/stock_verification/run-20260516-source-refresh |
| latest_verification_generated_at | 2026-05-16T17:24:24Z |
| latest_verification_age_hours | 394.8 |
| latest_stock_verification_run | data/stock_verification/run-20260516-source-refresh |
| latest_stock_verification_generated_at | 2026-05-16T17:24:24Z |
| latest_stock_verification_age_hours | 394.8 |
| latest_etf_verification_run | data/etf_verification/run-20260516-source-refresh |
| latest_etf_verification_generated_at | 2026-05-16T17:24:24Z |
| latest_etf_verification_age_hours | 394.8 |
| symbol_changes_generated_at | 2026-05-30T16:24:00Z |
| symbol_changes_age_hours | 59.81 |
| symbol_changes_review_rows | 271 |
| entry_quality_generated_at | 2026-05-31T04:00:45Z |
| entry_quality_age_hours | 48.2 |
| entry_quality_rows | 71043 |
| masterfile_collision_review_generated_at | 2026-05-25T12:53:29Z |
| masterfile_collision_review_age_hours | 183.32 |
| masterfile_collision_review_rows | 11107 |
| ohlcv_plausibility_generated_at | 2026-05-31T04:01:17Z |
| ohlcv_plausibility_age_hours | 48.19 |
| ohlcv_plausibility_rows | 350 |
| source_gap_classification_generated_at | 2026-05-31T04:00:47Z |
| source_gap_classification_age_hours | 48.2 |
| source_gap_classification_rows | 3548 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-05-31T03:58:05Z | 48.24 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-06-02T04:11:58Z | 0.01 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-05-31T04:05:20Z | 48.12 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-05-31T03:58:05Z | 48.24 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-16T17:24:24Z | 394.8 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-16T17:24:24Z | 394.8 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-05-30T16:24:00Z | 59.81 | 271 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-05-31T04:00:45Z | 48.2 | 71043 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-05-31T04:00:47Z | 48.2 | 3548 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-05-25T12:53:29Z | 183.32 | 11107 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-05-31T04:01:17Z | 48.19 | 350 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 36, "old": 100} |
| source_age_bucket_totals | {"age_0_48h": 36, "age_168_336h": 28, "age_over_336h": 72} |
| refresh_priority_totals | {"P1": 25, "P2": 75, "P4": 36} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 36, "refresh_official_exchange_directory_before_identity_or_collision_work": 25, "refresh_official_subset_before_gap_enrichment": 68, "restore_or_replace_unavailable_source_before_data_fill": 7} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 23 | 26908 | 184.26 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 184.9 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 45 | 8525 | 396.22 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 396.23 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 396.21 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 396.23 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 396.23 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 18 | 29021 | 0.85 | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 16 | 35624 | 1.68 | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 3205 | 0.64 | Freshness evidence is present; no data change is authorized by freshness alone. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5485 | 2026-06-02T02:41:57Z | 1.51 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7228 | 2026-06-02T02:41:57Z | 1.51 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 2026-05-16T15:58:33Z | 396.23 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-05-16T15:58:34Z | 396.23 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-05-16T15:58:37Z | 396.23 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11013 | 2026-05-25T13:43:01Z | 182.49 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_listed_companies | ASX | listed_companies_subset | network | 1978 | 2026-06-02T03:21:38Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-06-02T03:10:54Z | 1.03 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_investment_products | ASX | listed_companies_subset | network | 446 | 2026-06-02T03:24:01Z | 0.81 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-06-02T03:49:54Z | 0.38 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-06-02T03:06:34Z | 1.1 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-06-02T03:52:09Z | 0.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_dr_search | SET | listed_companies_subset | network | 378 | 2026-06-02T03:53:47Z | 0.31 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 2026-05-16T15:59:06Z | 396.22 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | cache | 1746 | 2026-05-16T15:59:07Z | 396.22 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 268 | 2026-06-02T04:09:08Z | 0.06 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_equities | Euronext | exchange_directory | network | 3864 | 2026-06-02T02:52:48Z | 1.33 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 3566 | 2026-06-02T03:26:20Z | 0.77 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_listed_issues | JPX | exchange_directory | network | 4449 | 2026-06-02T02:50:43Z | 1.36 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 3205 | 2026-06-02T03:34:18Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 472 | 2026-06-02T04:02:32Z | 0.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3565 | 2026-06-02T03:28:53Z | 0.73 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 4544 | 2026-06-02T02:48:38Z | 1.4 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-06-02T04:04:34Z | 0.13 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-05-16T15:59:27Z | 396.22 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8710 | 2026-06-02T03:31:51Z | 0.68 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_etp_products | SIX | listed_companies_subset | network | 830 | 2026-06-02T04:06:49Z | 0.09 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_instruments_equities | B3 | exchange_directory | network | 1315 | 2026-06-02T02:44:04Z | 1.47 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 188 | 2026-05-25T07:49:09Z | 188.39 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 305 | 2026-05-25T07:49:09Z | 188.39 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | cache | 134 | 2026-05-16T15:59:41Z | 396.21 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 2026-05-16T15:59:42Z | 396.21 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_instrument_search | JSE | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:43Z | 396.21 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_listed_companies | BME | listed_companies_subset | cache | 78 | 2026-05-16T15:59:46Z | 396.21 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:48Z | 396.21 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:49Z | 396.21 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 10 | 2026-06-02T02:31:56Z | 1.68 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:52Z | 396.21 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | cache | 91 | 2026-05-16T16:00:26Z | 396.2 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1127 | 2026-06-02T03:48:02Z | 0.41 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 2026-06-02T03:43:13Z | 0.49 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | cache | 26 | 2026-05-16T16:01:01Z | 396.19 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 2 | 2026-05-25T12:19:11Z | 183.89 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | cache | 190 | 2026-05-16T16:01:03Z | 396.19 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | cache | 31 | 2026-05-16T16:01:04Z | 396.19 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 2026-05-25T11:56:39Z | 184.26 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 2026-05-25T12:37:38Z | 183.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 315 | 2026-05-25T12:37:48Z | 183.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | cache | 17 | 2026-05-16T16:02:09Z | 396.17 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | cache | 3 | 2026-05-16T16:02:10Z | 396.17 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| byma_equity_details | BYMA | security_lookup_subset | cache | 63 | 2026-05-16T16:02:11Z | 396.17 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 0 | 2026-05-16T17:15:33Z | 394.95 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 2026-05-25T12:07:16Z | 184.09 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3016 | 2026-05-25T13:40:05Z | 182.54 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | network | 5019 | 2026-05-25T13:33:23Z | 182.65 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| hkex_securities_list | HKEX | exchange_directory | network | 3154 | 2026-05-25T13:36:00Z | 182.61 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| sgx_securities_prices | SGX | exchange_directory | network | 738 | 2026-05-25T12:53:14Z | 183.32 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-05-25T12:22:20Z | 183.84 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-05-25T12:25:59Z | 183.78 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-05-25T12:15:51Z | 183.94 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 637 | 2026-05-25T12:50:40Z | 183.36 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 2026-05-25T12:47:25Z | 183.42 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| adx_market_watch | ADX | exchange_directory | network | 122 | 2026-05-25T12:23:57Z | 183.81 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-05-25T12:12:50Z | 183.99 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | network | 108 | 2026-05-25T12:27:25Z | 183.75 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-05-16T16:02:31Z | 396.17 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-05-16T17:14:29Z | 394.97 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-05-16T16:02:33Z | 396.17 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 111 | 2026-05-25T12:32:07Z | 183.67 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| sem_isin | SEM | exchange_directory | network | 47 | 2026-05-25T12:10:16Z | 184.04 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | cache | 7 | 2026-05-16T16:02:37Z | 396.16 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | network | 173 | 2026-05-25T12:29:41Z | 183.71 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-05-16T16:02:40Z | 396.16 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | cache | 27 | 2026-05-16T16:02:41Z | 396.16 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 348 | 2026-05-25T12:35:04Z | 183.62 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bvb_fund_units_directory | BVB | listed_companies_subset | cache | 9 | 2026-05-16T16:02:44Z | 396.16 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-05-16T16:02:45Z | 396.16 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 2026-05-25T12:00:28Z | 184.2 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 148 | 2026-05-16T16:03:18Z | 396.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 7 | 2026-06-02T03:57:46Z | 0.25 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_etf_search | BMV | listed_companies_subset | network | 7 | 2026-06-02T03:55:56Z | 0.28 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 125 | 2026-05-16T16:37:25Z | 395.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 76 | 2026-05-16T16:14:15Z | 395.97 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | network | 746 | 2026-05-16T16:15:37Z | 395.95 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:15:41Z | 395.95 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | network | 191 | 2026-05-16T16:15:42Z | 395.95 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:15:45Z | 395.95 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-05-16T16:15:50Z | 395.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 134 | 2026-05-16T16:45:27Z | 395.45 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-05-16T16:21:55Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-05-16T16:21:56Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | network | 30 | 2026-05-16T16:22:00Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | network | 143 | 2026-05-16T16:22:02Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:22:04Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | network | 33 | 2026-05-16T16:22:06Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | network | 2 | 2026-05-16T16:22:07Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-05-16T16:22:08Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:22:10Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | network | 6 | 2026-05-16T16:22:11Z | 395.84 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1090 | 2026-06-02T03:00:46Z | 1.2 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| twse_etf_list | TWSE | listed_companies_subset | network | 220 | 2026-06-02T04:11:58Z | 0.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2356 | 2026-06-02T03:36:57Z | 0.59 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sse_etf_list | SSE | listed_companies_subset | unavailable | 0 | 2026-05-16T16:22:54Z | 395.83 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2893 | 2026-06-02T03:40:14Z | 0.54 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-05-16T16:24:14Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 658 | 2026-05-16T16:24:19Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | unavailable | 0 | 2026-05-16T16:24:25Z | 395.8 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 113 | 2026-05-16T16:24:27Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 887 | 2026-05-16T16:24:31Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 349 | 2026-05-16T16:24:37Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2764 | 2026-06-02T02:54:46Z | 1.3 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1136 | 2026-06-02T02:57:21Z | 1.25 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 562 | 2026-05-16T16:24:54Z | 395.79 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 368 | 2026-05-16T16:25:23Z | 395.79 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 716 | 2026-06-02T03:08:46Z | 1.06 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-06-02T03:13:13Z | 0.99 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 63 | 2026-05-16T16:25:33Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 957 | 2026-05-16T16:25:40Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 958 | 2026-06-02T03:03:33Z | 1.15 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_listed_companies | GPW | listed_companies_subset | network | 400 | 2026-05-16T16:25:44Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 364 | 2026-05-16T16:25:49Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | network | 27 | 2026-05-16T16:25:52Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 523 | 2026-05-16T16:25:59Z | 395.78 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 463 | 2026-05-16T16:26:32Z | 395.77 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 0 | 2026-05-16T16:26:44Z | 395.76 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 0 | 2026-05-16T16:26:45Z | 395.76 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 402 | 2026-05-16T16:26:46Z | 395.76 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_etf_list | HOSE | listed_companies_subset | network | 18 | 2026-05-16T16:26:54Z | 395.76 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-05-16T16:26:57Z | 395.76 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | network | 300 | 2026-06-02T03:15:22Z | 0.95 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| upcom_registered_securities | HNX | exchange_directory | network | 830 | 2026-05-25T13:30:27Z | 182.7 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 22 | 2026-05-16T16:31:06Z | 395.69 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 74 | 2026-05-16T16:31:09Z | 395.69 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 2026-05-25T11:20:18Z | 184.87 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 746 | 2026-05-16T16:52:15Z | 395.34 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 2026-05-25T11:18:18Z | 184.9 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 25 |
| P2 | 75 |
| P4 | 36 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 25 |
| refresh_official_subset_before_gap_enrichment | 68 |
| restore_or_replace_unavailable_source_before_data_fill | 7 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 16 |
| fresh_no_refresh_needed | interlisted_subset | 1 |
| fresh_no_refresh_needed | listed_companies_subset | 18 |
| fresh_no_refresh_needed | security_identifier_registry_subset | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 25 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 62 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 6 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 7 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 2 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 23 |
| refresh_official_subset_before_gap_enrichment | cache | 21 |
| refresh_official_subset_before_gap_enrichment | network | 47 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 7 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 25 |
| refresh_official_subset_before_gap_enrichment | P2 | 68 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 7 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 36 |
| age_168_336h | 28 |
| age_over_336h | 72 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_168_336h | 25 |
| refresh_official_subset_before_gap_enrichment | age_168_336h | 3 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 65 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 7 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 25 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 68 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 7 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 25 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 68 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 7 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 23 | 26908 | 184.26 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 2 | 22047 | 184.9 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 45 | 8525 | 396.22 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 17 | 18897 | 396.23 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 7 | 0 | 396.21 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 396.23 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 746 | 396.23 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 18 | 29021 | 0.85 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 16 | 35624 | 1.68 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 3205 | 0.64 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | interlisted_subset | network | P4 | 1 | 268 | 0.06 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 0 | 122 | 85 | 33 | 4 | 69.67 | 100.0 |
| AMS | official_full | 314 | 310 | 226 | 0 | 307 | 0 | 583 | 238 | 300 | 45 | 40.82 | 100.0 |
| ASX | official_partial | 1298 | 1193 | 1253 | 30 | 1161 | 24 | 0 | 0 | 0 | 0 |  | 99.89 |
| ATHEX | official_partial | 117 | 109 | 117 | 0 | 95 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| B3 | official_full | 1584 | 1572 | 1389 | 0 | 1252 | 0 | 1315 | 1245 | 0 | 70 | 94.68 | 100.0 |
| BATS | official_full | 1241 | 1219 | 1220 | 0 | 1115 | 0 | 1336 | 1180 | 20 | 136 | 88.32 | 100.0 |
| BCBA | official_partial | 64 | 61 | 50 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BHB | official_full | 29 | 29 | 2 | 0 | 27 | 0 | 41 | 29 | 9 | 3 | 70.73 | 100.0 |
| BIST | official_full | 614 | 614 | 608 | 0 | 614 | 0 | 637 | 614 | 20 | 3 | 96.39 | 100.0 |
| BK | official_full | 104 | 104 | 1 | 0 | 104 | 0 | 140 | 104 | 27 | 9 | 74.29 | 100.0 |
| BME | official_full | 169 | 169 | 160 | 3 | 169 | 0 | 10 | 3 | 0 | 7 | 30.0 | 100.0 |
| BMV | official_partial | 179 | 160 | 174 | 0 | 159 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 28 | 0 | 37 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_HU | official_partial | 31 | 23 | 13 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_IN | official_full | 2642 | 2642 | 2599 | 0 | 2626 | 0 | 5019 | 2611 | 765 | 1643 | 52.02 | 93.74 |
| BVB | official_full | 80 | 80 | 76 | 0 | 80 | 0 | 348 | 75 | 121 | 152 | 21.55 | 100.0 |
| BVC | official_partial | 3 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BVL | official_partial | 33 | 31 | 3 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Bursa | official_partial | 936 | 936 | 934 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CPH | official_partial | 131 | 131 | 130 | 0 | 131 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 164 | 0 | 305 | 0 | 315 | 307 | 0 | 8 | 97.46 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 2 | 0 | 62 | 0 | 50 | 1 | 37 | 12 | 2.0 | 59.09 |
| DFM | official_full | 46 | 46 | 45 | 0 | 46 | 0 | 71 | 46 | 16 | 9 | 64.79 | 100.0 |
| DSE_TZ | official_partial | 17 | 15 | 2 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| EGX | official_partial | 225 | 225 | 196 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Euronext | official_full | 975 | 972 | 755 | 7 | 965 | 65 | 4421 | 935 | 2263 | 1223 | 21.15 | 100.0 |
| GSE | official_partial | 19 | 18 | 2 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HEL | official_partial | 188 | 188 | 185 | 1 | 188 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HKEX | official_full | 3044 | 3044 | 3005 | 0 | 3035 | 0 | 3154 | 3038 | 82 | 34 | 96.32 | 99.89 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 300 | 105 | 156 | 39 | 35.0 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 16 | 1 | 18 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| IDX | official_full | 694 | 689 | 694 | 1 | 578 | 0 | 958 | 694 | 243 | 21 | 72.44 | 99.71 |
| ISE | official_full | 14 | 14 | 13 | 0 | 14 | 0 | 19 | 9 | 6 | 4 | 47.37 | 100.0 |
| JSE | official_partial | 212 | 204 | 210 | 2 | 167 | 0 | 0 | 0 | 0 | 0 |  |  |
| KOSDAQ | official_full | 1583 | 1578 | 1580 | 0 | 1578 | 0 | 1819 | 1574 | 0 | 245 | 86.53 | 99.49 |
| KRX | official_full | 1796 | 1794 | 1794 | 0 | 1793 | 0 | 2081 | 1781 | 3 | 297 | 85.58 | 99.53 |
| LSE | official_full | 6415 | 6404 | 5701 | 16 | 6386 | 5 | 11013 | 6317 | 1186 | 3510 | 57.36 | 99.02 |
| LUSE | official_partial | 22 | 22 | 2 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| MSE_MW | missing | 8 | 8 | 0 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSX | official_full | 91 | 1 | 91 | 0 | 0 | 0 | 108 | 91 | 14 | 3 | 84.26 | 100.0 |
| NASDAQ | official_full | 4637 | 4597 | 4614 | 3441 | 4022 | 399 | 5524 | 4490 | 80 | 954 | 81.28 | 99.47 |
| NEO | official_full | 197 | 152 | 164 | 0 | 151 | 0 | 440 | 190 | 85 | 165 | 43.18 | 100.0 |
| NGX | official_full | 145 | 143 | 114 | 0 | 134 | 0 | 133 | 133 | 0 | 0 | 100.0 | 100.0 |
| NMFQS | official_partial | 7 | 7 | 6 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_IN | official_full | 1234 | 1234 | 1228 | 0 | 1232 | 0 | 3016 | 1212 | 314 | 1490 | 40.19 | 98.34 |
| NSE_KE | official_full | 46 | 46 | 13 | 0 | 43 | 0 | 66 | 10 | 24 | 32 | 15.15 | 100.0 |
| NYSE | official_full | 2082 | 2043 | 2069 | 1997 | 1954 | 372 | 3872 | 2041 | 533 | 1298 | 52.71 | 99.95 |
| NYSE ARCA | official_full | 2653 | 2601 | 2600 | 126 | 2369 | 3 | 2646 | 2567 | 25 | 54 | 97.01 | 100.0 |
| NYSE MKT | official_full | 236 | 235 | 236 | 219 | 208 | 26 | 313 | 229 | 23 | 61 | 73.16 | 100.0 |
| NZX | official_full | 45 | 45 | 23 | 0 | 45 | 0 | 173 | 45 | 126 | 2 | 26.01 | 100.0 |
| OSL | official_full | 241 | 237 | 178 | 2 | 233 | 0 | 297 | 230 | 66 | 1 | 77.44 | 100.0 |
| OTC | official_full | 11054 | 10282 | 9909 | 1835 | 9215 | 0 | 11925 | 7640 | 36 | 4249 | 64.07 | 87.31 |
| PSE | official_full | 90 | 90 | 13 | 1 | 90 | 0 | 381 | 90 | 185 | 106 | 23.62 | 100.0 |
| PSE_CZ | official_partial | 24 | 23 | 12 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| PSX | official_full | 373 | 338 | 373 | 3 | 266 | 0 | 716 | 371 | 152 | 193 | 51.82 | 99.18 |
| QSE | official_full | 54 | 27 | 47 | 0 | 0 | 0 | 57 | 54 | 2 | 1 | 94.74 | 100.0 |
| RSE | official_partial | 2 | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SEM | official_full | 53 | 53 | 4 | 1 | 50 | 0 | 47 | 47 | 0 | 0 | 100.0 | 90.2 |
| SET | official_full | 547 | 541 | 547 | 4 | 342 | 0 | 944 | 545 | 349 | 50 | 57.73 | 99.63 |
| SGX | official_full | 594 | 591 | 515 | 0 | 8 | 0 | 738 | 589 | 142 | 7 | 79.81 | 99.63 |
| SIX | official_partial | 743 | 743 | 743 | 2 | 743 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE | official_partial | 2789 | 2747 | 2789 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | official_full | 116 | 87 | 101 | 0 | 85 | 0 | 111 | 111 | 0 | 0 | 100.0 | 98.97 |
| STO | official_partial | 725 | 725 | 669 | 2 | 723 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SZSE | official_partial | 3083 | 3069 | 3083 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TADAWUL | official_full | 191 | 191 | 188 | 0 | 191 | 0 | 412 | 191 | 217 | 4 | 46.36 | 100.0 |
| TASE | official_partial | 673 | 673 | 647 | 0 | 672 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TPEX | official_partial | 1118 | 1118 | 1118 | 0 | 917 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3216 | 3212 | 3178 | 0 | 3212 | 0 | 4449 | 3206 | 1187 | 56 | 72.06 | 99.68 |
| TSX | official_full | 1904 | 1802 | 1663 | 12 | 1661 | 23 | 785 | 324 | 460 | 1 | 41.27 | 99.32 |
| TSXV | official_full | 1066 | 984 | 964 | 17 | 921 | 0 | 1518 | 961 | 556 | 1 | 63.31 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1190 | 0 | 1165 | 0 | 1090 | 973 | 38 | 79 | 89.27 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 830 | 2 | 468 | 360 | 0.24 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 0 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| VSE | official_partial | 36 | 34 | 32 | 0 | 34 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| WSE | official_partial | 348 | 348 | 322 | 7 | 347 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| XETRA | official_full | 3779 | 3776 | 3109 | 8 | 3767 | 2 | 4544 | 3648 | 801 | 95 | 80.28 | 99.42 |
| ZSE | official_partial | 23 | 23 | 1 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 6 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 60 | 57 | 47 | 0 | 56 | 0 |
| Australia | 1774 | 1669 | 1729 | 230 | 1585 | 62 |
| Austria | 58 | 56 | 53 | 8 | 48 | 8 |
| Bahrain | 30 | 30 | 2 | 0 | 28 | 0 |
| Belgium | 121 | 120 | 106 | 7 | 120 | 1 |
| Bermuda | 533 | 533 | 522 | 58 | 509 | 14 |
| Botswana | 24 | 24 | 20 | 0 | 24 | 0 |
| Brazil | 1586 | 1574 | 1392 | 0 | 1253 | 0 |
| Bulgaria | 14 | 14 | 14 | 3 | 14 | 0 |
| Canada | 4826 | 4599 | 4494 | 648 | 4182 | 65 |
| Cayman Islands | 2104 | 2103 | 2084 | 482 | 1943 | 19 |
| Chile | 115 | 86 | 101 | 0 | 84 | 0 |
| China | 6345 | 6289 | 6344 | 1 | 5239 | 0 |
| Colombia | 3 | 0 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 1 | 0 | 23 | 0 |
| Cyprus | 19 | 19 | 16 | 2 | 14 | 0 |
| Czech Republic | 22 | 21 | 10 | 0 | 21 | 0 |
| Denmark | 142 | 142 | 139 | 4 | 139 | 0 |
| Egypt | 242 | 242 | 212 | 6 | 209 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 0 |
| Finland | 192 | 192 | 189 | 1 | 192 | 0 |
| France | 683 | 681 | 580 | 18 | 670 | 59 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 0 |
| Germany | 767 | 764 | 729 | 13 | 757 | 5 |
| Ghana | 20 | 19 | 4 | 0 | 18 | 0 |
| Greece | 124 | 116 | 123 | 4 | 106 | 0 |
| Guernsey | 69 | 69 | 66 | 7 | 65 | 0 |
| Hong Kong | 469 | 469 | 467 | 1 | 468 | 0 |
| Hungary | 22 | 15 | 8 | 0 | 15 | 0 |
| Iceland | 18 | 18 | 16 | 1 | 18 | 0 |
| India | 3875 | 3875 | 3827 | 0 | 3857 | 0 |
| Indonesia | 744 | 739 | 744 | 21 | 628 | 1 |
| Ireland | 2589 | 2589 | 2585 | 67 | 2581 | 7 |
| Isle of Man | 15 | 15 | 15 | 3 | 14 | 0 |
| Israel | 773 | 772 | 760 | 94 | 764 | 1 |
| Italy | 126 | 126 | 117 | 2 | 122 | 0 |
| Japan | 3316 | 3312 | 3269 | 15 | 3307 | 0 |
| Jersey | 173 | 173 | 167 | 19 | 171 | 4 |
| Kazakhstan | 1 | 1 | 0 | 0 | 1 | 0 |
| Kenya | 45 | 45 | 13 | 0 | 42 | 0 |
| Kuwait | 102 | 102 | 1 | 0 | 102 | 0 |
| Liechtenstein | 4 | 4 | 3 | 0 | 4 | 0 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 0 |
| Luxembourg | 1030 | 1030 | 1023 | 28 | 1027 | 5 |
| Malawi | 8 | 8 | 0 | 0 | 7 | 0 |
| Malaysia | 939 | 939 | 937 | 0 | 932 | 0 |
| Malta | 6 | 6 | 6 | 0 | 6 | 0 |
| Marshall Islands | 38 | 38 | 37 | 31 | 30 | 0 |
| Mauritius | 69 | 69 | 21 | 6 | 64 | 0 |
| Mexico | 136 | 117 | 129 | 5 | 114 | 0 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 62 | 0 |
| Netherlands | 193 | 189 | 172 | 28 | 186 | 1 |
| New Zealand | 80 | 80 | 61 | 5 | 77 | 1 |
| Nigeria | 146 | 144 | 116 | 0 | 135 | 0 |
| Norway | 244 | 240 | 194 | 6 | 233 | 0 |
| Oman | 90 | 0 | 90 | 0 | 0 | 0 |
| Pakistan | 378 | 343 | 376 | 9 | 271 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 31 | 29 | 2 | 0 | 29 | 0 |
| Philippines | 110 | 110 | 33 | 11 | 109 | 1 |
| Poland | 350 | 350 | 323 | 21 | 349 | 5 |
| Portugal | 34 | 34 | 31 | 0 | 34 | 1 |
| Qatar | 54 | 27 | 47 | 0 | 0 | 0 |
| Romania | 83 | 83 | 78 | 3 | 83 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 188 | 0 | 191 | 0 |
| Singapore | 548 | 543 | 506 | 15 | 51 | 2 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 0 |
| South Africa | 230 | 222 | 224 | 5 | 183 | 0 |
| South Korea | 3369 | 3362 | 3364 | 0 | 3361 | 0 |
| Spain | 223 | 223 | 202 | 9 | 222 | 3 |
| Sri Lanka | 307 | 307 | 164 | 0 | 305 | 0 |
| Sweden | 758 | 758 | 698 | 11 | 753 | 1 |
| Switzerland | 390 | 390 | 387 | 21 | 387 | 5 |
| Taiwan | 2273 | 2273 | 2272 | 0 | 2055 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 13 | 0 |
| Thailand | 565 | 559 | 564 | 28 | 358 | 3 |
| Turkey | 614 | 614 | 608 | 0 | 614 | 0 |
| Uganda | 7 | 7 | 0 | 0 | 7 | 0 |
| United Arab Emirates | 123 | 123 | 122 | 0 | 123 | 0 |
| United Kingdom | 1350 | 1339 | 1279 | 96 | 1320 | 19 |
| United States | 13910 | 13002 | 13238 | 5070 | 11715 | 610 |
| Vietnam | 261 | 261 | 260 | 2 | 260 | 0 |
| Zambia | 22 | 22 | 2 | 0 | 21 | 0 |
| Zimbabwe | 28 | 28 | 7 | 0 | 25 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 4010 | 3055 | 0 | 954 | 1 |
| B3 | official_full | 1205 | 1205 | 0 | 0 | 0 |
| SSE | official_partial | 534 | 534 | 0 | 0 | 0 |
| BSE_IN | official_full | 165 | 0 | 165 | 0 | 0 |
| NASDAQ | official_full | 126 | 108 | 0 | 18 | 0 |
| BME | official_full | 102 | 102 | 0 | 0 | 0 |
| JSE | official_partial | 90 | 87 | 0 | 3 | 0 |
| TSXV | official_full | 84 | 8 | 76 | 0 | 0 |
| NYSE ARCA | official_full | 77 | 77 | 0 | 0 | 0 |
| Euronext | official_full | 64 | 64 | 0 | 0 | 0 |
| BATS | official_full | 62 | 61 | 0 | 1 | 0 |
| LSE | official_full | 59 | 15 | 43 | 0 | 1 |
| TASE | official_partial | 39 | 39 | 0 | 0 | 0 |
| NYSE | official_full | 36 | 35 | 0 | 0 | 1 |
| EGX | official_partial | 34 | 34 | 0 | 0 | 0 |
| ASX | official_partial | 32 | 31 | 0 | 1 | 0 |
| BSE_HU | official_partial | 29 | 29 | 0 | 0 | 0 |
| CSE_MA | official_full | 27 | 0 | 27 | 0 | 0 |
| ATHEX | official_partial | 26 | 26 | 0 | 0 | 0 |
| NSE_IN | official_full | 22 | 2 | 16 | 0 | 4 |

## B3 Masterfile Diagnostics

| Metric | Value |
|---|---:|
| Dataset rows | 1584 |
| Active exchange-directory rows | 1315 |
| Matched dataset rows | 1245 |
| Missing dataset rows | 339 |
| Dataset match rate | 78.6 |
| Any official B3 source matched dataset rows | 1262 |
| Any official B3 source missing dataset rows | 322 |
| Any official B3 source match rate | 79.67 |
| Official active symbols not in dataset | 70 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 2 |
| local_share_line | 268 |
| other | 18 |
| unit_or_fund_line | 51 |

### B3 Missing Examples

| Listing key | Category | Asset Type | Source Presence | Name |
|---|---|---|---|---|
| B3::2WAV3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | 2W ECOBANK S.A. |
| B3::A6OP3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ACESSOPAR INVESTIMENTOS E PARTICIPAÇÕES S.A. |
| B3::AALR12 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR13 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::ABCB3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | BCO ABC BRASIL S.A. |
| B3::AFOF11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | Alianza Fofii Fundo De Investimento Imobiliario |
| B3::AGCX11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | FDO INV IMOB RIO BRAVO RENDA VAREJO - FII |
| B3::AGPL11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | MAGNETIS TEVA AÇÕES AGRONEGOCIO ETF FDO IND |
| B3::AQLL11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | ÁQUILLA FDO INV IMOB - FII |
| B3::AURB11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | ALIANZA URBAN HUB RENDA FII RESP LIM |
| B3::BIAU39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | Ishares Gold Trust |
| B3::BSLV39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | Ishares Silver Trust |
| B3::CPTS11B | other | ETF | absent_from_all_b3_masterfile_sources | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DNEN3B | other | Stock | absent_from_all_b3_masterfile_sources | DINAMICA ENERGIA S.A. |
| B3::EQMA5B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | other | Stock | absent_from_all_b3_masterfile_sources | INVITEL LEGACY S.A. |
