# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63812 |
| core_listings | 61702 |
| aliases | 125529 |
| stocks | 47780 |
| etfs | 16032 |
| isin_coverage | 62564 |
| sector_coverage | 62268 |
| stock_sector_coverage | 46267 |
| etf_category_coverage | 16001 |
| cik_coverage | 7785 |
| figi_coverage | 65408 |
| lei_coverage | 17790 |
| listing_status_rows | 107914 |
| listing_status_intervals | 107914 |
| listing_events | 82630 |
| listing_keys | 92006 |
| instrument_scope_rows | 92006 |
| instrument_scope_core | 61702 |
| instrument_scope_extended | 30304 |
| instrument_scope_primary_listing | 61002 |
| instrument_scope_primary_listing_missing_isin | 700 |
| instrument_scope_otc_listing | 11753 |
| instrument_scope_secondary_cross_listing | 18551 |
| legacy_primary_ticker_collision_rows | 4784 |
| official_masterfile_symbols | 98866 |
| official_masterfile_matches | 64811 |
| official_masterfile_collisions | 13806 |
| official_masterfile_missing | 20249 |
| official_recall_denominator | 98866 |
| official_recall_matches | 64811 |
| official_recall_missing | 34055 |
| official_recall_pct | 65.55 |
| collision_adjusted_recall_denominator | 85060 |
| collision_adjusted_recall_missing | 20249 |
| collision_adjusted_recall_pct | 76.19 |
| collision_adjusted_recall_gap_rate | 23.81 |
| official_full_recall_target_exchanges | 49 |
| official_full_recall_passing_exchanges | 3 |
| official_full_recall_exception_exchanges | 46 |
| collision_adjusted_full_recall_passing_exchanges | 6 |
| collision_adjusted_full_recall_exception_exchanges | 43 |
| official_recall_decision_counts | {'fixed': 3, 'mostly_collision_hidden': 8, 'out_of_current_scope': 33, 'source_unavailable': 5, 'still_actionable': 38} |
| official_recall_exception_decision_counts | {'mostly_collision_hidden': 8, 'still_actionable': 38} |
| official_recall_unclassified_exception_exchanges | 0 |
| official_full_exchanges | 49 |
| official_partial_exchanges | 33 |
| manual_only_exchanges | 0 |
| missing_exchanges | 5 |
| stock_verification_items | 52232 |
| stock_verification_verified | 47042 |
| stock_verification_reference_gap | 4190 |
| stock_verification_missing_from_official | 125 |
| stock_verification_name_mismatch | 864 |
| stock_verification_cross_exchange_collision | 2 |
| etf_verification_items | 18860 |
| etf_verification_verified | 18288 |
| etf_verification_reference_gap | 521 |
| etf_verification_missing_from_official | 34 |
| etf_verification_name_mismatch | 7 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-08-19T15:26:28Z |
| tickers_age_hours | 0.08 |
| masterfiles_generated_at | 2026-08-19T15:20:47Z |
| masterfiles_age_hours | 0.18 |
| identifiers_generated_at | 2026-08-19T15:31:16Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-08-19T15:26:28Z |
| listing_history_age_hours | 0.08 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 2575.09 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 2575.09 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 2575.09 |
| symbol_changes_generated_at | 2026-08-19T07:03:27Z |
| symbol_changes_age_hours | 8.47 |
| symbol_changes_review_rows | 327 |
| entry_quality_generated_at | 2026-08-19T15:26:44Z |
| entry_quality_age_hours | 0.08 |
| entry_quality_rows | 92006 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 1868.22 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 430.62 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-08-19T15:26:45Z |
| source_gap_classification_age_hours | 0.08 |
| source_gap_classification_rows | 8815 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-08-19T15:26:28Z | 0.08 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-08-19T15:20:47Z | 0.18 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-08-19T15:31:16Z | 0.0 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-08-19T15:26:28Z | 0.08 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 2575.09 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 2575.09 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-08-19T07:03:27Z | 8.47 | 327 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-08-19T15:26:44Z | 0.08 | 92006 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-08-19T15:26:45Z | 0.08 | 8815 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 1868.22 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 430.62 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 112, "old": 26} |
| source_age_bucket_totals | {"age_0_48h": 112, "age_over_336h": 26} |
| refresh_priority_totals | {"P1": 6, "P2": 20, "P4": 112} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 112, "refresh_official_subset_before_gap_enrichment": 2, "restore_or_replace_unavailable_source_before_data_fill": 24} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 6 | 12822 | 1867.87 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 16 | 20337 | 1867.87 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 534.4 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1867.87 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 1 | 17 | 557.42 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 1867.87 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 69 | 39158 | 32.55 | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 36 | 91197 | 32.55 | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_lookup_subset | network | P4 | 5 | 1011 | 6.87 | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | interlisted_subset | network | P4 | 1 | 269 | 0.64 | Freshness evidence is present; no data change is authorized by freshness alone. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5602 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7558 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 24 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope corporate_action_daily_list. | Freshness evidence is present; no data change is authorized by freshness alone. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11107 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 749.62 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 444 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_investment_products | ASX | listed_companies_subset | network | 457 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_listed_companies | SET | listed_companies_subset | network | 930 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_stock_search | SET | exchange_directory | network | 943 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_dr_search | SET | listed_companies_subset | network | 512 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3700 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1780 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 269 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_equities | Euronext | exchange_directory | network | 3849 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4086 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | unavailable | 4030 | 2026-07-28T09:07:29Z | 534.4 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 464 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3674 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5093 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_frankfurt_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 18025 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 1 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_etf_products | SIX | listed_companies_subset | network | 8950 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_etp_products | SIX | listed_companies_subset | network | 850 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_instruments_equities | B3 | exchange_directory | network | 1294 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 216 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 321 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_etf_list | JSE | listed_companies_subset | network | 140 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 123 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | unavailable | 50 | 2026-08-02T08:35:09Z | 414.94 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 126 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1127 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | network | 26 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 20 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | unavailable | 82 | 2026-07-27T10:06:05Z | 557.42 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | unavailable | 307 | 2026-07-27T10:06:05Z | 557.42 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | unavailable | 318 | 2026-07-27T10:06:05Z | 557.42 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-07-27T10:06:05Z | 557.42 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 92 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 8 | 2026-07-07T09:07:40Z | 1038.4 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 68 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3202 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_india_scrips | BSE India | exchange_directory | network | 4970 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hkex_securities_list | HKEX | exchange_directory | network | 3194 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sgx_securities_prices | SGX | exchange_directory | network | 746 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | unavailable | 140 | 2026-07-27T10:06:05Z | 557.42 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 652 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 413 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| adx_market_watch | ADX | exchange_directory | network | 123 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| muscat_securities_companies | MSX | exchange_directory | network | 108 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| rse_listed_companies | RSE | listed_companies_subset | network | 1 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| luse_listed_companies | LuSE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 122 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sem_isin | SEM | exchange_directory | network | 46 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nzx_instruments | NZX | exchange_directory | network | 172 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | network | 6 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_shares_directory | BVB | exchange_directory | network | 350 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 10 | 2026-08-19T15:20:47Z | 0.18 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| ngx_equities_price_list | NGX | listed_companies_subset | network | 130 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| ngx_company_profile_directory | NGX | exchange_directory | network | 130 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_stock_search | BMV | listed_companies_subset | unavailable | 10 | 2026-08-02T08:35:09Z | 414.94 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | unavailable | 5 | 2026-08-02T08:35:09Z | 414.94 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 5 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_market_data_securities | BMV | listed_companies_subset | unavailable | 9 | 2026-08-02T08:35:09Z | 414.94 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 76 | 2026-08-19T15:05:31Z | 0.43 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | network | 744 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | network | 194 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 129 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| ngm_market_data_equities | NGM | listed_companies_subset | unavailable | 30 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | network | 145 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | network | 35 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | network | 2 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | network | 6 | 2026-08-19T08:39:18Z | 6.87 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| twse_listed_companies | TWSE | exchange_directory | network | 1095 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| twse_etf_list | TWSE | listed_companies_subset | unavailable | 231 | 2026-08-01T08:32:15Z | 438.99 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2355 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sse_etf_list | SSE | listed_companies_subset | network | 918 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| szse_etf_list | SZSE | listed_companies_subset | network | 714 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 896 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 117 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 890 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 360 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_listed_companies | KRX | exchange_directory | network | 2759 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1163 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 565 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 383 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_dps_symbols | PSX | exchange_directory | network | 720 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_listed_company_directory | PSE | exchange_directory | network | 385 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 62 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| idx_listed_companies | IDX | listed_companies_subset | network | 962 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| idx_company_profiles | IDX | exchange_directory | network | 962 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_listed_companies | GPW | listed_companies_subset | network | 403 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 347 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_etf_list | GPW | listed_companies_subset | network | 38 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 532 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 467 | 2026-08-19T14:30:55Z | 1.01 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 1867.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 405 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 3 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-08-18T06:58:26Z | 32.55 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| upcom_registered_securities | HNX | exchange_directory | network | 824 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 66 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 73 | 2026-08-19T14:53:11Z | 0.64 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sec_company_tickers_exchange | SEC | exchange_directory | network | 10166 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 882 | 2026-08-19T14:11:11Z | 1.34 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | unavailable | 11925 | 2026-06-02T19:38:59Z | 1867.87 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 6 |
| P2 | 20 |
| P4 | 112 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 112 |
| refresh_official_subset_before_gap_enrichment | 2 |
| restore_or_replace_unavailable_source_before_data_fill | 24 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | corporate_action_daily_list | 1 |
| fresh_no_refresh_needed | exchange_directory | 36 |
| fresh_no_refresh_needed | interlisted_subset | 1 |
| fresh_no_refresh_needed | listed_companies_subset | 69 |
| fresh_no_refresh_needed | security_lookup_subset | 5 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 2 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 6 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 16 |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | 1 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 1 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 112 |
| refresh_official_subset_before_gap_enrichment | cache | 1 |
| refresh_official_subset_before_gap_enrichment | network | 1 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 24 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 112 |
| refresh_official_subset_before_gap_enrichment | P2 | 2 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 6 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 18 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 112 |
| age_over_336h | 26 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 112 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 2 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 24 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 112 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 2 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 24 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 112 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 2 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 24 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 6 | 12822 | 1867.87 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 16 | 20337 | 1867.87 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 534.4 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1867.87 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 1 | 17 | 557.42 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 1867.87 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 69 | 39158 | 32.55 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 36 | 91197 | 32.55 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_lookup_subset | network | P4 | 5 | 1011 | 6.87 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | interlisted_subset | network | P4 | 1 | 269 | 0.64 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | corporate_action_daily_list | network | P4 | 1 | 24 | 1.34 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope corporate_action_daily_list. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 123 | 85 | 32 | 6 | 69.11 | 30.89 | 93.41 | 6 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 | 100.0 |
| AMS | official_full | 546 | 546 | 544 | 0 | 322 | 153 | 604 | 374 | 177 | 53 | 61.92 | 38.08 | 87.59 | 53 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=177 | 100.0 |
| ASX | official_partial | 2259 | 2160 | 2254 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 163 | 163 | 163 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1580 | 0 | 1252 | 0 | 1294 | 1212 | 0 | 82 | 93.66 | 6.34 | 93.66 | 82 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=82;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1357 | 1320 | 1340 | 0 | 1048 | 243 | 1603 | 1262 | 58 | 283 | 78.73 | 21.27 | 81.68 | 283 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=341;symbol_collisions=58 | 100.0 |
| BCBA | official_partial | 92 | 92 | 69 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 28 | 28 | 28 | 0 | 26 | 7 | 41 | 28 | 9 | 4 | 68.29 | 31.71 | 87.5 | 4 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 652 | 612 | 21 | 19 | 93.87 | 6.13 | 96.99 | 19 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=40;symbol_collisions=21 | 100.0 |
| BK | official_full | 104 | 104 | 104 | 0 | 104 | 0 | 140 | 102 | 27 | 11 | 72.86 | 27.14 | 90.27 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 | 100.0 |
| BME | official_full | 276 | 276 | 266 | 3 | 220 | 212 | 50 | 18 | 0 | 32 | 36.0 | 64.0 | 36.0 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=0 | 100.0 |
| BMV | official_partial | 344 | 327 | 335 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 50 | 0 | 41 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2732 | 2732 | 2732 | 0 | 2613 | 0 | 4970 | 2535 | 1845 | 590 | 51.01 | 48.99 | 81.12 | 590 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2435;symbol_collisions=1845 | 100.0 |
| BVB | official_full | 92 | 92 | 91 | 0 | 80 | 76 | 350 | 87 | 119 | 144 | 24.86 | 75.14 | 37.66 | 144 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=263;symbol_collisions=119 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 276 | 275 | 2903 | 250 | 1856 | 797 | 8.61 | 91.39 | 23.88 | 797 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2653;symbol_collisions=1856 |  |
| Bursa | official_partial | 1039 | 1039 | 1036 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 153 | 153 | 150 | 0 | 144 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 318 | 307 | 0 | 11 | 96.54 | 3.46 | 96.54 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 82 | 1 | 64 | 17 | 1.22 | 98.78 | 5.56 | 17 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=64 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 45 | 17 | 9 | 63.38 | 36.62 | 83.33 | 9 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 17 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 223 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1477 | 1477 | 1457 | 7 | 1071 | 844 | 2014 | 1341 | 365 | 308 | 66.58 | 33.42 | 81.32 | 308 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=673;symbol_collisions=365 | 100.0 |
| FSX | official_full | 8143 | 8143 | 6551 | 0 | 0 | 0 | 18025 | 8006 | 3945 | 6074 | 44.42 | 55.58 | 56.86 | 6074 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10019;symbol_collisions=3945 |  |
| GSE | official_partial | 19 | 18 | 19 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 200 | 200 | 200 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3058 | 3058 | 3043 | 0 | 3004 | 268 | 3194 | 3038 | 70 | 86 | 95.12 | 4.88 | 97.25 | 86 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=156;symbol_collisions=70 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 299 | 104 | 163 | 32 | 34.78 | 65.22 | 76.47 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=163 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 756 | 694 | 756 | 0 | 577 | 0 | 962 | 756 | 187 | 19 | 78.59 | 21.41 | 97.55 | 19 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=187 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 15 | 9 | 6 | 0 | 60.0 | 40.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1605 | 1605 | 1605 | 0 | 1578 | 0 | 1818 | 1595 | 3 | 220 | 87.73 | 12.27 | 87.88 | 220 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=223;symbol_collisions=3 | 99.62 |
| KRX | official_full | 1991 | 1990 | 1988 | 0 | 1793 | 0 | 2104 | 1959 | 14 | 131 | 93.11 | 6.89 | 93.73 | 131 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=145;symbol_collisions=14 | 99.76 |
| LSE | official_full | 7030 | 7029 | 7014 | 16 | 6491 | 4338 | 11107 | 6842 | 779 | 3486 | 61.6 | 38.4 | 66.25 | 3486 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4265;symbol_collisions=779 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 13 | 4 | 84.26 | 15.74 | 95.79 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 | 100.0 |
| Munich | missing | 223 | 223 | 185 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| NASDAQ | official_full | 4767 | 4718 | 4729 | 3469 | 3390 | 1382 | 5608 | 4571 | 60 | 977 | 81.51 | 18.49 | 82.39 | 977 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1037;symbol_collisions=60 | 99.56 |
| NEO | official_full | 247 | 204 | 230 | 0 | 149 | 1 | 444 | 211 | 66 | 167 | 47.52 | 52.48 | 55.82 | 167 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=66 | 100.0 |
| NGX | official_full | 145 | 145 | 145 | 0 | 133 | 76 | 130 | 130 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2489 | 0 | 3202 | 2331 | 390 | 481 | 72.8 | 27.2 | 82.89 | 481 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=871;symbol_collisions=390 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 46 | 0 | 42 | 1 | 68 | 11 | 25 | 32 | 16.18 | 83.82 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=57;symbol_collisions=25 | 100.0 |
| NYSE | official_full | 2022 | 1969 | 2008 | 1936 | 1440 | 994 | 3891 | 1983 | 572 | 1336 | 50.96 | 49.04 | 59.75 | 1336 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1908;symbol_collisions=572 | 100.0 |
| NYSE ARCA | official_full | 2729 | 2681 | 2719 | 113 | 2099 | 368 | 2701 | 2580 | 29 | 92 | 95.52 | 4.48 | 96.56 | 92 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=29 | 100.0 |
| NYSE MKT | official_full | 235 | 223 | 234 | 146 | 151 | 51 | 309 | 230 | 31 | 48 | 74.43 | 25.57 | 82.73 | 48 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=79;symbol_collisions=31 | 100.0 |
| NZX | official_full | 45 | 45 | 43 | 0 | 45 | 1 | 172 | 45 | 126 | 1 | 26.16 | 73.84 | 97.83 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=126 | 100.0 |
| OSL | official_full | 306 | 306 | 293 | 2 | 258 | 243 | 297 | 285 | 7 | 5 | 95.96 | 4.04 | 98.28 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=7 | 100.0 |
| OTC | official_full | 11753 | 11100 | 11128 | 2000 | 8845 | 2847 | 11925 | 8268 | 24 | 3633 | 69.33 | 30.67 | 69.47 | 3633 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3657;symbol_collisions=24 | 88.97 |
| PSE | official_full | 155 | 155 | 90 | 1 | 88 | 16 | 385 | 155 | 119 | 111 | 40.26 | 59.74 | 58.27 | 111 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=119 | 100.0 |
| PSE_CZ | official_partial | 27 | 27 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 390 | 385 | 390 | 3 | 263 | 2 | 720 | 390 | 142 | 188 | 54.17 | 45.83 | 67.47 | 188 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=330;symbol_collisions=142 | 99.73 |
| QSE | official_full | 55 | 54 | 55 | 0 | 0 | 0 | 57 | 55 | 2 | 0 | 96.49 | 3.51 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 52 | 1 | 49 | 2 | 46 | 46 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 90.2 |
| SET | official_full | 779 | 777 | 779 | 4 | 335 | 4 | 943 | 775 | 132 | 36 | 82.18 | 17.82 | 95.56 | 36 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=168;symbol_collisions=132 | 100.0 |
| SGX | official_full | 613 | 613 | 613 | 0 | 8 | 18 | 746 | 610 | 122 | 14 | 81.77 | 18.23 | 97.76 | 14 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=136;symbol_collisions=122 | 99.63 |
| SIX | official_partial | 1263 | 1263 | 1260 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2795 | 2760 | 2795 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 129 | 102 | 120 | 0 | 85 | 1 | 122 | 122 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 878 | 878 | 877 | 2 | 818 | 798 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3150 | 3138 | 3150 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 199 | 199 | 199 | 0 | 191 | 0 | 413 | 198 | 210 | 5 | 47.94 | 52.06 | 97.54 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=215;symbol_collisions=210 | 100.0 |
| TASE | official_partial | 801 | 801 | 797 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1119 | 1119 | 1119 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4077 | 4067 | 4077 | 0 | 4060 | 485 | 4444 | 4043 | 335 | 66 | 90.98 | 9.02 | 98.39 | 66 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=335 | 100.0 |
| TSX | official_full | 2296 | 2219 | 2288 | 12 | 1620 | 40 | 788 | 593 | 190 | 5 | 75.25 | 24.75 | 99.16 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=190 | 99.32 |
| TSXV | official_full | 1422 | 1325 | 1391 | 17 | 911 | 9 | 1596 | 1400 | 194 | 2 | 87.72 | 12.28 | 99.86 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=194 | 92.78 |
| TWSE | official_full | 1239 | 1239 | 1239 | 0 | 1165 | 3 | 1095 | 1015 | 31 | 49 | 92.69 | 7.31 | 95.39 | 49 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=31 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 824 | 2 | 524 | 298 | 0.24 | 99.76 | 0.67 | 298 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=822;symbol_collisions=524 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 88 | 88 | 82 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 582 | 582 | 574 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XDUS | missing | 199 | 199 | 171 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XETRA | official_full | 4315 | 4314 | 4303 | 8 | 3827 | 1924 | 5093 | 4103 | 705 | 285 | 80.56 | 19.44 | 93.51 | 285 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=990;symbol_collisions=705 | 99.88 |
| XHAM | missing | 12 | 12 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XHAN | missing | 80 | 80 | 70 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XSTU | missing | 2773 | 2773 | 2399 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.67 | 824 | 822 | 298 | 524 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=822;symbol_collisions=524 |
| CSE_MA | 1.22 | 5.56 | 82 | 81 | 17 | 64 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=64 |
| Borsa Italiana | 8.61 | 23.88 | 2903 | 2653 | 797 | 1856 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2653;symbol_collisions=1856 |
| NSE_KE | 16.18 | 25.58 | 68 | 57 | 32 | 25 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=57;symbol_collisions=25 |
| BVB | 24.86 | 37.66 | 350 | 263 | 144 | 119 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=263;symbol_collisions=119 |
| NZX | 26.16 | 97.83 | 172 | 127 | 1 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=126 |
| HNX | 34.78 | 76.47 | 299 | 195 | 32 | 163 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=163 |
| BME | 36.0 | 36.0 | 50 | 32 | 32 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=0 |
| PSE | 40.26 | 58.27 | 385 | 230 | 111 | 119 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=119 |
| FSX | 44.42 | 56.86 | 18025 | 10019 | 6074 | 3945 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10019;symbol_collisions=3945 |
| NEO | 47.52 | 55.82 | 444 | 233 | 167 | 66 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=66 |
| TADAWUL | 47.94 | 97.54 | 413 | 215 | 5 | 210 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=215;symbol_collisions=210 |
| NYSE | 50.96 | 59.75 | 3891 | 1908 | 1336 | 572 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1908;symbol_collisions=572 |
| BSE_IN | 51.01 | 81.12 | 4970 | 2435 | 590 | 1845 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2435;symbol_collisions=1845 |
| PSX | 54.17 | 67.47 | 720 | 330 | 188 | 142 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=330;symbol_collisions=142 |
| ISE | 60.0 | 100.0 | 15 | 6 | 0 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 |
| LSE | 61.6 | 66.25 | 11107 | 4265 | 3486 | 779 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4265;symbol_collisions=779 |
| AMS | 61.92 | 87.59 | 604 | 230 | 53 | 177 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=177 |
| DFM | 63.38 | 83.33 | 71 | 26 | 9 | 17 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 |
| Euronext | 66.58 | 81.32 | 2014 | 673 | 308 | 365 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=673;symbol_collisions=365 |
| BHB | 68.29 | 87.5 | 41 | 13 | 4 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 |
| ADX | 69.11 | 93.41 | 123 | 38 | 6 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 |
| OTC | 69.33 | 69.47 | 11925 | 3657 | 3633 | 24 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3657;symbol_collisions=24 |
| NSE_IN | 72.8 | 82.89 | 3202 | 871 | 481 | 390 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=871;symbol_collisions=390 |
| BK | 72.86 | 90.27 | 140 | 38 | 11 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 |
| NYSE MKT | 74.43 | 82.73 | 309 | 79 | 48 | 31 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=79;symbol_collisions=31 |
| TSX | 75.25 | 99.16 | 788 | 195 | 5 | 190 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=190 |
| IDX | 78.59 | 97.55 | 962 | 206 | 19 | 187 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=187 |
| BATS | 78.73 | 81.68 | 1603 | 341 | 283 | 58 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=341;symbol_collisions=58 |
| XETRA | 80.56 | 93.51 | 5093 | 990 | 285 | 705 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=990;symbol_collisions=705 |
| NASDAQ | 81.51 | 82.39 | 5608 | 1037 | 977 | 60 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1037;symbol_collisions=60 |
| SGX | 81.77 | 97.76 | 746 | 136 | 14 | 122 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=136;symbol_collisions=122 |
| SET | 82.18 | 95.56 | 943 | 168 | 36 | 132 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=168;symbol_collisions=132 |
| MSX | 84.26 | 95.79 | 108 | 17 | 4 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 |
| TSXV | 87.72 | 99.86 | 1596 | 196 | 2 | 194 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=194 |
| KOSDAQ | 87.73 | 87.88 | 1818 | 223 | 220 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=223;symbol_collisions=3 |
| TSE | 90.98 | 98.39 | 4444 | 401 | 66 | 335 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=335 |
| TWSE | 92.69 | 95.39 | 1095 | 80 | 49 | 31 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=31 |
| KRX | 93.11 | 93.73 | 2104 | 145 | 131 | 14 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=145;symbol_collisions=14 |
| B3 | 93.66 | 93.66 | 1294 | 82 | 82 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=82;symbol_collisions=0 |
| BIST | 93.87 | 96.99 | 652 | 40 | 19 | 21 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=40;symbol_collisions=21 |
| HKEX | 95.12 | 97.25 | 3194 | 156 | 86 | 70 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=156;symbol_collisions=70 |
| NYSE ARCA | 95.52 | 96.56 | 2701 | 121 | 92 | 29 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=29 |
| OSL | 95.96 | 98.28 | 297 | 12 | 5 | 7 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=7 |
| QSE | 96.49 | 100.0 | 57 | 2 | 0 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 |
| CSE_LK | 96.54 | 96.54 | 318 | 11 | 11 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 66 | 66 | 63 | 0 | 56 | 0 |
| Australia | 1515 | 1410 | 1480 | 59 | 1183 | 118 |
| Austria | 49 | 49 | 48 | 1 | 41 | 39 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 29 | 29 | 29 | 0 | 27 | 7 |
| Belgium | 129 | 128 | 128 | 6 | 113 | 110 |
| Bermuda | 533 | 533 | 528 | 57 | 477 | 122 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1596 | 1586 | 1595 | 13 | 1259 | 2 |
| British Virgin Islands | 152 | 152 | 149 | 96 | 92 | 23 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4450 | 4248 | 4034 | 291 | 3269 | 34 |
| Cayman Islands | 2275 | 2266 | 2239 | 605 | 1880 | 247 |
| Chile | 116 | 89 | 116 | 3 | 84 | 2 |
| China | 6397 | 6344 | 6394 | 4 | 5245 | 10 |
| Colombia | 3 | 3 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 19 | 18 | 19 | 1 | 11 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 156 | 156 | 153 | 6 | 145 | 136 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 2 | 2 |
| Finland | 200 | 200 | 199 | 2 | 193 | 3 |
| France | 705 | 702 | 700 | 14 | 663 | 650 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 838 | 833 | 795 | 10 | 759 | 696 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 140 | 140 | 138 | 1 | 120 | 116 |
| Guernsey | 68 | 68 | 66 | 4 | 56 | 56 |
| Hong Kong | 472 | 469 | 468 | 0 | 461 | 5 |
| Hungary | 37 | 36 | 37 | 0 | 29 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5067 | 5067 | 5067 | 0 | 4982 | 4 |
| Indonesia | 712 | 708 | 699 | 1 | 578 | 0 |
| Ireland | 2528 | 2520 | 2525 | 37 | 2434 | 859 |
| Isle of Man | 13 | 13 | 13 | 1 | 13 | 11 |
| Israel | 754 | 753 | 751 | 72 | 703 | 2 |
| Italy | 241 | 240 | 230 | 1 | 223 | 216 |
| Japan | 3398 | 3393 | 3338 | 20 | 3324 | 446 |
| Jersey | 173 | 173 | 170 | 15 | 158 | 157 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 9 | 9 | 2 | 0 | 2 | 2 |
| Luxembourg | 1027 | 1024 | 1023 | 14 | 995 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 974 | 974 | 974 | 0 | 933 | 1 |
| Malta | 6 | 6 | 6 | 0 | 6 | 6 |
| Marshall Islands | 42 | 42 | 40 | 34 | 26 | 20 |
| Mauritius | 61 | 61 | 60 | 2 | 54 | 0 |
| Mexico | 135 | 117 | 132 | 6 | 109 | 2 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 66 | 0 | 62 | 0 |
| Netherlands | 192 | 192 | 187 | 28 | 172 | 127 |
| New Zealand | 75 | 75 | 71 | 0 | 64 | 0 |
| Nigeria | 147 | 147 | 147 | 0 | 135 | 78 |
| Norway | 225 | 224 | 220 | 5 | 209 | 210 |
| Oman | 90 | 90 | 90 | 0 | 0 | 0 |
| Pakistan | 371 | 366 | 371 | 3 | 263 | 0 |
| Panama | 6 | 5 | 5 | 3 | 4 | 0 |
| Papua New Guinea | 1 | 1 | 1 | 0 | 1 | 0 |
| Peru | 32 | 32 | 32 | 1 | 29 | 0 |
| Philippines | 100 | 100 | 93 | 1 | 93 | 17 |
| Poland | 366 | 365 | 362 | 8 | 357 | 354 |
| Portugal | 38 | 38 | 38 | 0 | 38 | 36 |
| Puerto Rico | 6 | 6 | 6 | 5 | 6 | 4 |
| Qatar | 54 | 54 | 54 | 0 | 0 | 0 |
| Romania | 81 | 81 | 80 | 0 | 80 | 77 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 191 | 0 | 191 | 0 |
| Singapore | 555 | 551 | 543 | 16 | 40 | 5 |
| Slovenia | 8 | 8 | 1 | 0 | 1 | 1 |
| South Africa | 252 | 252 | 227 | 9 | 175 | 141 |
| South Korea | 3585 | 3583 | 3582 | 1 | 3362 | 0 |
| Spain | 223 | 223 | 221 | 8 | 209 | 205 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 817 | 812 | 791 | 5 | 762 | 760 |
| Switzerland | 371 | 371 | 370 | 22 | 345 | 293 |
| Taiwan | 2277 | 2276 | 2277 | 1 | 2056 | 1 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 624 | 624 | 567 | 6 | 332 | 1 |
| Turkey | 619 | 619 | 619 | 0 | 619 | 553 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1308 | 1304 | 1290 | 45 | 1213 | 1023 |
| United States | 14741 | 14000 | 14059 | 5224 | 10700 | 3770 |
| Vietnam | 261 | 260 | 261 | 2 | 260 | 0 |
| Zambia | 22 | 22 | 22 | 0 | 21 | 0 |
| Zimbabwe | 28 | 28 | 28 | 0 | 25 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 4001 | 3150 | 0 | 850 | 1 |
| B3 | official_full | 766 | 766 | 0 | 0 | 0 |
| BMV | official_partial | 150 | 150 | 0 | 0 | 0 |
| BME | official_full | 93 | 93 | 0 | 0 | 0 |
| TSXV | official_full | 84 | 8 | 76 | 0 | 0 |
| NASDAQ | official_full | 82 | 67 | 0 | 15 | 0 |
| JSE | official_partial | 79 | 76 | 0 | 3 | 0 |
| NYSE ARCA | official_full | 70 | 70 | 0 | 0 | 0 |
| Euronext | official_full | 61 | 61 | 0 | 0 | 0 |
| BATS | official_full | 53 | 52 | 0 | 1 | 0 |
| LSE | official_full | 37 | 7 | 29 | 0 | 1 |
| EGX | official_partial | 34 | 34 | 0 | 0 | 0 |
| ASX | official_partial | 30 | 30 | 0 | 0 | 0 |
| ATHEX | official_partial | 26 | 26 | 0 | 0 | 0 |
| NYSE | official_full | 23 | 23 | 0 | 0 | 0 |
| BSE_HU | official_partial | 17 | 17 | 0 | 0 | 0 |
| TWSE | official_full | 16 | 16 | 0 | 0 | 0 |
| VSE | official_partial | 14 | 14 | 0 | 0 | 0 |
| BSE_BW | official_partial | 12 | 12 | 0 | 0 | 0 |
| NGX | official_full | 12 | 0 | 12 | 0 | 0 |

## B3 Masterfile Diagnostics

| Metric | Value |
|---|---:|
| Dataset rows | 1581 |
| Active exchange-directory rows | 1294 |
| Matched dataset rows | 1212 |
| Missing dataset rows | 369 |
| Dataset match rate | 76.66 |
| Any official B3 source matched dataset rows | 1241 |
| Any official B3 source missing dataset rows | 340 |
| Any official B3 source match rate | 78.49 |
| Official active symbols not in dataset | 82 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 16 |
| local_share_line | 269 |
| other | 15 |
| unit_or_fund_line | 69 |

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
| B3::BAOK39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES CORE 30/70 CONSERVATIVE ALLOCATION ETF |
| B3::BBCN39 | bdr_or_foreign_receipt | ETF | absent_from_all_b3_masterfile_sources | JPMORGAN BETABUILDERS CANADA ETF |
| B3::BBIL39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | JPMORGAN BETABUILDERS INTERNATIONAL EQUITY ETF |
| B3::BFIW39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | FIRST TRUST WATER ETF |
| B3::BFLO39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES FLOATING RATE BOND ETF |
| B3::CPTS11B | other | ETF | absent_from_all_b3_masterfile_sources | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DNEN3B | other | Stock | absent_from_all_b3_masterfile_sources | DINAMICA ENERGIA S.A. |
| B3::EQMA5B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | other | Stock | absent_from_all_b3_masterfile_sources | INVITEL LEGACY S.A. |
