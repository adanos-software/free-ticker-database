# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 65703 |
| core_listings | 63609 |
| aliases | 129124 |
| stocks | 48847 |
| etfs | 16856 |
| isin_coverage | 64414 |
| sector_coverage | 64951 |
| stock_sector_coverage | 48227 |
| etf_category_coverage | 16724 |
| cik_coverage | 7883 |
| figi_coverage | 64871 |
| lei_coverage | 17613 |
| listing_status_rows | 114145 |
| listing_status_intervals | 114145 |
| listing_events | 98949 |
| listing_keys | 97753 |
| instrument_scope_rows | 97753 |
| instrument_scope_core | 63609 |
| instrument_scope_extended | 34144 |
| instrument_scope_primary_listing | 62977 |
| instrument_scope_primary_listing_missing_isin | 632 |
| instrument_scope_otc_listing | 11752 |
| instrument_scope_secondary_cross_listing | 22392 |
| legacy_primary_ticker_collision_rows | 4844 |
| official_masterfile_symbols | 100321 |
| official_masterfile_matches | 70242 |
| official_masterfile_collisions | 11735 |
| official_masterfile_missing | 18344 |
| official_recall_denominator | 100321 |
| official_recall_matches | 70242 |
| official_recall_missing | 30079 |
| official_recall_pct | 70.02 |
| collision_adjusted_recall_denominator | 88586 |
| collision_adjusted_recall_missing | 18344 |
| collision_adjusted_recall_pct | 79.29 |
| collision_adjusted_recall_gap_rate | 20.71 |
| official_full_recall_target_exchanges | 50 |
| official_full_recall_passing_exchanges | 3 |
| official_full_recall_exception_exchanges | 47 |
| collision_adjusted_full_recall_passing_exchanges | 20 |
| collision_adjusted_full_recall_exception_exchanges | 30 |
| official_recall_decision_counts | {'fixed': 3, 'mostly_collision_hidden': 24, 'out_of_current_scope': 33, 'source_unavailable': 5, 'still_actionable': 23} |
| official_recall_exception_decision_counts | {'mostly_collision_hidden': 24, 'still_actionable': 23} |
| official_recall_unclassified_exception_exchanges | 0 |
| official_full_exchanges | 50 |
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
| tickers_built_at | 2026-11-13T16:00:48Z |
| tickers_age_hours | 0.11 |
| masterfiles_generated_at | 2026-11-13T15:08:54Z |
| masterfiles_age_hours | 0.97 |
| identifiers_generated_at | 2026-11-13T16:00:55Z |
| identifiers_age_hours | 0.1 |
| listing_history_observed_at | 2026-11-13T16:00:48Z |
| listing_history_age_hours | 0.11 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 4639.69 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 4639.69 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 4639.69 |
| symbol_changes_generated_at | 2026-11-12T15:07:51Z |
| symbol_changes_age_hours | 24.99 |
| symbol_changes_review_rows | 394 |
| entry_quality_generated_at | 2026-11-13T16:01:23Z |
| entry_quality_age_hours | 0.1 |
| entry_quality_rows | 97753 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 3932.81 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 2495.22 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-11-13T16:01:29Z |
| source_gap_classification_age_hours | 0.09 |
| source_gap_classification_rows | 8212 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-11-13T16:00:48Z | 0.11 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-11-13T15:08:54Z | 0.97 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-11-13T16:00:55Z | 0.1 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-11-13T16:00:48Z | 0.11 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 4639.69 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 4639.69 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-11-12T15:07:51Z | 24.99 | 394 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-11-13T16:01:23Z | 0.1 | 97753 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-11-13T16:01:29Z | 0.09 | 8212 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 3932.81 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 2495.22 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 28, "old": 82, "stale": 28} |
| source_age_bucket_totals | {"age_0_48h": 28, "age_168_336h": 16, "age_48_168h": 28, "age_over_336h": 66} |
| refresh_priority_totals | {"P1": 14, "P2": 96, "P4": 28} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 28, "refresh_official_subset_before_gap_enrichment": 70, "restore_or_replace_unavailable_source_before_data_fill": 40} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 14 | 39994 | 3932.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 63 | 38161 | 387.44 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 23 | 22164 | 3932.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 1043 | 363.28 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 2 | 64 | 3932.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4040 | 2063.86 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 266 | 290.94 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 6 | 363.28 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 3932.47 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 28 | 65480 | 47.17 | Freshness evidence is present; no data change is authorized by freshness alone. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5604 | 2026-11-13T15:08:54Z | 0.97 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7682 | 2026-11-13T15:08:54Z | 0.97 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 6 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | unavailable | 0 | 2026-10-11T12:38:35Z | 795.48 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | unavailable | 11184 | 2026-10-08T15:08:41Z | 864.97 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 2814.21 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 449 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_investment_products | ASX | listed_companies_subset | network | 471 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 930 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | unavailable | 943 | 2026-11-09T20:42:49Z | 91.4 | stale | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 581 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3619 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1851 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3817 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4192 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jpx_listed_issues | JPX | exchange_directory | unavailable | 4444 | 2026-08-25T08:23:53Z | 1927.72 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | unavailable | 4040 | 2026-08-19T16:15:20Z | 2063.86 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 462 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3771 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5517 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_frankfurt_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 18168 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | unavailable | 1 | 2026-08-19T14:30:55Z | 2065.6 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| six_etf_products | SIX | listed_companies_subset | network | 9316 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 835 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | unavailable | 1353 | 2026-09-13T11:40:38Z | 1468.44 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 236 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 321 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | unavailable | 141 | 2026-08-25T08:23:53Z | 1927.72 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| jse_etn_list | JSE | listed_companies_subset | unavailable | 104 | 2026-08-25T08:23:53Z | 1927.72 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 122 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 267 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 126 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1142 | 2026-09-07T14:52:25Z | 1609.24 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-09-07T14:52:25Z | 1609.24 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | network | 26 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 19 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | unavailable | 82 | 2026-07-27T10:06:05Z | 2622.02 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 319 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 322 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-08-24T09:23:58Z | 1950.72 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 92 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 8 | 2026-07-07T09:07:40Z | 3102.99 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 68 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3574 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_india_scrips | BSE India | exchange_directory | unavailable | 5165 | 2026-09-21T12:40:24Z | 1275.45 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hkex_securities_list | HKEX | exchange_directory | network | 3240 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sgx_securities_prices | SGX | exchange_directory | network | 768 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| dfm_listed_securities | DFM | exchange_directory | network | 72 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 139 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 663 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 414 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| adx_market_watch | ADX | exchange_directory | network | 126 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| muscat_securities_companies | MSX | exchange_directory | network | 110 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| rse_listed_companies | RSE | listed_companies_subset | network | 1 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | unavailable | 15 | 2026-09-28T18:10:57Z | 1101.94 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 122 | 2026-08-19T15:05:31Z | 2065.03 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | unavailable | 47 | 2026-11-02T21:05:34Z | 259.03 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | network | 172 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | network | 6 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 89 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 11 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | network | 128 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | unavailable | 130 | 2026-09-20T18:35:59Z | 1293.52 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_stock_search | BMV | listed_companies_subset | unavailable | 10 | 2026-08-02T08:35:09Z | 2479.53 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | unavailable | 5 | 2026-08-02T08:35:09Z | 2479.53 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 1 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | unavailable | 10 | 2026-08-02T08:35:09Z | 2479.53 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | unavailable | 76 | 2026-08-19T15:05:31Z | 2065.03 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | network | 739 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | network | 196 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 134 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | unavailable | 30 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | network | 144 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | network | 35 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | network | 2 | 2026-10-28T12:40:39Z | 387.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | network | 6 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1099 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| twse_etf_list | TWSE | listed_companies_subset | network | 275 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2359 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 947 | 2026-11-08T13:13:38Z | 122.89 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 36 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 772 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | unavailable | 896 | 2026-10-02T11:25:55Z | 1012.69 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 118 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 897 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 367 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | unavailable | 2758 | 2026-10-08T15:08:41Z | 864.97 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| krx_etf_finder | KRX | exchange_directory | network | 1190 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 568 | 2026-10-30T13:03:52Z | 339.05 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | unavailable | 383 | 2026-08-19T14:11:11Z | 2065.93 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| psx_dps_symbols | PSX | exchange_directory | unavailable | 724 | 2026-09-20T18:35:59Z | 1293.52 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| pse_listed_company_directory | PSE | exchange_directory | network | 384 | 2026-11-12T15:02:08Z | 25.08 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 63 | 2026-10-30T13:03:52Z | 339.05 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 962 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 944 | 2026-11-11T16:56:46Z | 47.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_listed_companies | GPW | listed_companies_subset | unavailable | 403 | 2026-08-19T14:53:11Z | 2065.23 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 346 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | unavailable | 38 | 2026-08-19T14:53:11Z | 2065.23 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 531 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 471 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 3932.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 408 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_etf_list | HOSE | listed_companies_subset | network | 21 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 3 | 2026-11-10T15:04:29Z | 73.04 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | unavailable | 299 | 2026-10-30T13:03:52Z | 339.05 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| upcom_registered_securities | HNX | exchange_directory | unavailable | 818 | 2026-09-08T06:43:05Z | 1593.4 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 66 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 72 | 2026-11-01T13:10:30Z | 290.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | network | 10185 | 2026-11-13T15:08:54Z | 0.97 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 914 | 2026-10-29T12:50:15Z | 363.28 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | unavailable | 11925 | 2026-06-02T19:38:59Z | 3932.47 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 14 |
| P2 | 96 |
| P4 | 28 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 28 |
| refresh_official_subset_before_gap_enrichment | 70 |
| restore_or_replace_unavailable_source_before_data_fill | 40 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 28 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 64 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 4 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 14 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 23 |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | 1 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 2 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 28 |
| refresh_official_subset_before_gap_enrichment | cache | 1 |
| refresh_official_subset_before_gap_enrichment | network | 69 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 40 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 28 |
| refresh_official_subset_before_gap_enrichment | P2 | 70 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 14 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 26 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 28 |
| age_168_336h | 16 |
| age_48_168h | 28 |
| age_over_336h | 66 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 28 |
| refresh_official_subset_before_gap_enrichment | age_168_336h | 15 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 27 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 28 |
| restore_or_replace_unavailable_source_before_data_fill | age_168_336h | 1 |
| restore_or_replace_unavailable_source_before_data_fill | age_48_168h | 1 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 38 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 28 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 70 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 40 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 28 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 70 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 40 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 14 | 39994 | 3932.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 63 | 38161 | 387.44 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 23 | 22164 | 3932.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 1043 | 363.28 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 2 | 64 | 3932.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4040 | 2063.86 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 266 | 290.94 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 6 | 363.28 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 3932.47 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 28 | 65480 | 47.17 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 96 | 96 | 96 | 0 | 85 | 7 | 126 | 94 | 30 | 2 | 74.6 | 25.4 | 97.92 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=30 | 100.0 |
| AMS | official_full | 748 | 747 | 696 | 0 | 320 | 153 | 615 | 572 | 39 | 4 | 93.01 | 6.99 | 99.31 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=43;symbol_collisions=39 | 100.0 |
| ASX | official_partial | 2259 | 2161 | 2248 | 30 | 1143 | 99 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 162 | 162 | 162 | 0 | 126 | 124 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1589 | 1577 | 1587 | 0 | 1250 | 0 | 1353 | 1228 | 0 | 125 | 90.76 | 9.24 | 90.76 | 125 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=125;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1391 | 1343 | 1382 | 0 | 992 | 230 | 1674 | 1323 | 58 | 293 | 79.03 | 20.97 | 81.87 | 293 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=351;symbol_collisions=58 | 100.0 |
| BCBA | official_partial | 92 | 92 | 91 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 31 | 31 | 31 | 0 | 26 | 7 | 41 | 31 | 9 | 1 | 75.61 | 24.39 | 96.88 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=9 | 100.0 |
| BIST | official_full | 643 | 643 | 627 | 0 | 613 | 549 | 663 | 641 | 22 | 0 | 96.68 | 3.32 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=22;symbol_collisions=22 | 100.0 |
| BK | official_full | 115 | 115 | 115 | 0 | 104 | 0 | 139 | 111 | 28 | 0 | 79.86 | 20.14 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=28;symbol_collisions=28 | 100.0 |
| BME | official_full | 276 | 276 | 276 | 3 | 220 | 212 | 267 | 242 | 0 | 25 | 90.64 | 9.36 | 90.64 | 25 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=0 | 100.0 |
| BMV | official_partial | 344 | 338 | 344 | 0 | 158 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 38 | 36 | 0 | 36 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 50 | 0 | 38 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 5051 | 5051 | 5033 | 0 | 2566 | 0 | 5165 | 5012 | 120 | 33 | 97.04 | 2.96 | 99.35 | 33 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=153;symbol_collisions=120 | 100.0 |
| BVB | official_full | 247 | 247 | 223 | 0 | 80 | 76 | 89 | 50 | 39 | 0 | 56.18 | 43.82 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=39;symbol_collisions=39 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 275 | 274 | 2976 | 246 | 1936 | 794 | 8.27 | 91.73 | 23.65 | 794 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2730;symbol_collisions=1936 |  |
| Bursa | official_partial | 1039 | 1039 | 1037 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 153 | 153 | 151 | 0 | 144 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 321 | 321 | 308 | 0 | 305 | 0 | 322 | 319 | 0 | 3 | 99.07 | 0.93 | 99.07 | 3 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 83 | 83 | 81 | 0 | 61 | 0 | 82 | 18 | 64 | 0 | 21.95 | 78.05 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=64;symbol_collisions=64 | 92.42 |
| DFM | official_full | 54 | 54 | 54 | 0 | 45 | 2 | 72 | 54 | 17 | 1 | 75.0 | 25.0 | 98.18 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=18;symbol_collisions=17 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 17 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 223 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1476 | 1476 | 1475 | 7 | 1065 | 839 | 2021 | 1310 | 372 | 339 | 64.82 | 35.18 | 79.44 | 339 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=711;symbol_collisions=372 | 100.0 |
| FSX | official_full | 8143 | 8141 | 7736 | 0 | 0 | 0 | 18168 | 7823 | 4429 | 5916 | 43.06 | 56.94 | 56.94 | 5916 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10345;symbol_collisions=4429 |  |
| GSE | official_partial | 19 | 19 | 19 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 199 | 199 | 199 | 1 | 193 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3255 | 3255 | 3220 | 0 | 2982 | 266 | 3240 | 3171 | 68 | 1 | 97.87 | 2.13 | 99.97 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=69;symbol_collisions=68 | 100.0 |
| HNX | official_full | 137 | 137 | 121 | 0 | 105 | 0 | 299 | 136 | 163 | 0 | 45.48 | 54.52 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=163;symbol_collisions=163 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 756 | 752 | 756 | 0 | 573 | 0 | 944 | 745 | 183 | 16 | 78.92 | 21.08 | 97.9 | 16 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=199;symbol_collisions=183 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 16 | 9 | 7 | 0 | 56.25 | 43.75 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=7 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 165 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1605 | 1605 | 1605 | 0 | 1578 | 0 | 1820 | 1587 | 3 | 230 | 87.2 | 12.8 | 87.34 | 230 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=3 | 99.62 |
| KRX | official_full | 1991 | 1990 | 1989 | 0 | 1793 | 0 | 2128 | 1945 | 14 | 169 | 91.4 | 8.6 | 92.01 | 169 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=183;symbol_collisions=14 | 99.76 |
| LSE | official_full | 7025 | 7021 | 7015 | 16 | 6474 | 4330 | 11184 | 6814 | 870 | 3500 | 60.93 | 39.07 | 66.07 | 3500 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4370;symbol_collisions=870 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 95 | 92 | 95 | 0 | 0 | 0 | 110 | 95 | 13 | 2 | 86.36 | 13.64 | 97.94 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=15;symbol_collisions=13 | 100.0 |
| Munich | missing | 223 | 223 | 207 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| NASDAQ | official_full | 4808 | 4738 | 4779 | 3511 | 3313 | 1365 | 5621 | 4579 | 60 | 982 | 81.46 | 18.54 | 82.34 | 982 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1042;symbol_collisions=60 | 99.56 |
| NEO | official_full | 247 | 222 | 245 | 0 | 145 | 1 | 449 | 208 | 69 | 172 | 46.33 | 53.67 | 54.74 | 172 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=241;symbol_collisions=69 | 100.0 |
| NGX | official_full | 145 | 145 | 145 | 0 | 133 | 76 | 130 | 130 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 3538 | 3537 | 3224 | 0 | 2474 | 0 | 3574 | 3501 | 73 | 0 | 97.96 | 2.04 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=73;symbol_collisions=73 | 100.0 |
| NSE_KE | official_full | 79 | 79 | 79 | 0 | 42 | 1 | 68 | 44 | 24 | 0 | 64.71 | 35.29 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=24 | 100.0 |
| NYSE | official_full | 2026 | 2001 | 2015 | 1940 | 1422 | 974 | 3840 | 1965 | 564 | 1311 | 51.17 | 48.83 | 59.98 | 1311 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1875;symbol_collisions=564 | 100.0 |
| NYSE ARCA | official_full | 2732 | 2630 | 2694 | 107 | 2013 | 334 | 2765 | 2634 | 29 | 102 | 95.26 | 4.74 | 96.27 | 102 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=131;symbol_collisions=29 | 100.0 |
| NYSE MKT | official_full | 229 | 225 | 227 | 127 | 129 | 44 | 307 | 224 | 34 | 49 | 72.96 | 27.04 | 82.05 | 49 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=34 | 100.0 |
| NZX | official_full | 85 | 85 | 85 | 0 | 45 | 1 | 172 | 85 | 86 | 1 | 49.42 | 50.58 | 98.84 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=87;symbol_collisions=86 | 100.0 |
| OSL | official_full | 316 | 316 | 315 | 2 | 258 | 243 | 292 | 285 | 6 | 1 | 97.6 | 2.4 | 99.65 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=6 | 100.0 |
| OTC | official_full | 11752 | 11048 | 11645 | 2078 | 8752 | 2802 | 11925 | 8267 | 25 | 3633 | 69.32 | 30.68 | 69.47 | 3633 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3658;symbol_collisions=25 | 88.97 |
| PSE | official_full | 173 | 173 | 158 | 1 | 87 | 16 | 384 | 169 | 121 | 94 | 44.01 | 55.99 | 64.26 | 94 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=215;symbol_collisions=121 | 100.0 |
| PSE_CZ | official_partial | 27 | 27 | 27 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 390 | 383 | 390 | 3 | 261 | 1 | 724 | 390 | 143 | 191 | 53.87 | 46.13 | 67.13 | 191 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=334;symbol_collisions=143 | 99.73 |
| QSE | official_full | 55 | 55 | 55 | 0 | 0 | 0 | 57 | 55 | 2 | 0 | 96.49 | 3.51 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 52 | 0 | 49 | 2 | 47 | 47 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 90.2 |
| SET | official_full | 779 | 769 | 779 | 4 | 325 | 1 | 943 | 770 | 135 | 38 | 81.65 | 18.35 | 95.3 | 38 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=173;symbol_collisions=135 | 100.0 |
| SGX | official_full | 645 | 645 | 626 | 0 | 8 | 18 | 768 | 636 | 132 | 0 | 82.81 | 17.19 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=132;symbol_collisions=132 | 99.63 |
| SIX | official_partial | 1263 | 1263 | 1262 | 2 | 755 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2795 | 2762 | 2795 | 0 | 2174 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 129 | 110 | 128 | 0 | 85 | 1 | 122 | 122 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 876 | 876 | 875 | 2 | 815 | 794 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3150 | 3139 | 3150 | 0 | 2593 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 205 | 205 | 203 | 0 | 191 | 0 | 414 | 203 | 211 | 0 | 49.03 | 50.97 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=211;symbol_collisions=211 | 100.0 |
| TASE | official_partial | 801 | 801 | 798 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1119 | 1119 | 1119 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4137 | 4137 | 4129 | 0 | 4060 | 485 | 4444 | 4103 | 340 | 1 | 92.33 | 7.67 | 99.98 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=341;symbol_collisions=340 | 100.0 |
| TSX | official_full | 2442 | 2359 | 2427 | 12 | 1614 | 36 | 785 | 590 | 189 | 6 | 75.16 | 24.84 | 98.99 | 6 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=189 | 99.32 |
| TSXV | official_full | 1422 | 1362 | 1416 | 17 | 892 | 5 | 1518 | 1322 | 193 | 3 | 87.09 | 12.91 | 99.77 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=193 | 92.78 |
| TWSE | official_full | 1286 | 1286 | 1284 | 0 | 1165 | 3 | 1099 | 1061 | 38 | 0 | 96.54 | 3.46 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=38 | 100.0 |
| TXSE | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 24 | 0 | 14 | 10 | 0.0 | 100.0 | 0.0 | 10 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=14 |  |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 818 | 2 | 523 | 293 | 0.24 | 99.76 | 0.68 | 293 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=816;symbol_collisions=523 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 88 | 87 | 86 | 0 | 53 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 582 | 582 | 578 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XDUS | missing | 199 | 199 | 189 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XETRA | official_full | 5561 | 5560 | 5216 | 8 | 3824 | 1921 | 5517 | 5274 | 240 | 3 | 95.6 | 4.4 | 99.94 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=243;symbol_collisions=240 | 99.88 |
| XHAM | missing | 12 | 12 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XHAN | missing | 80 | 80 | 79 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XSTU | missing | 2773 | 2773 | 2529 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| TXSE | 0.0 | 0.0 | 24 | 24 | 10 | 14 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=14 |
| UPCOM | 0.24 | 0.68 | 818 | 816 | 293 | 523 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=816;symbol_collisions=523 |
| Borsa Italiana | 8.27 | 23.65 | 2976 | 2730 | 794 | 1936 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2730;symbol_collisions=1936 |
| CSE_MA | 21.95 | 100.0 | 82 | 64 | 0 | 64 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=64;symbol_collisions=64 |
| FSX | 43.06 | 56.94 | 18168 | 10345 | 5916 | 4429 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10345;symbol_collisions=4429 |
| PSE | 44.01 | 64.26 | 384 | 215 | 94 | 121 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=215;symbol_collisions=121 |
| HNX | 45.48 | 100.0 | 299 | 163 | 0 | 163 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=163;symbol_collisions=163 |
| NEO | 46.33 | 54.74 | 449 | 241 | 172 | 69 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=241;symbol_collisions=69 |
| TADAWUL | 49.03 | 100.0 | 414 | 211 | 0 | 211 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=211;symbol_collisions=211 |
| NZX | 49.42 | 98.84 | 172 | 87 | 1 | 86 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=87;symbol_collisions=86 |
| NYSE | 51.17 | 59.98 | 3840 | 1875 | 1311 | 564 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1875;symbol_collisions=564 |
| PSX | 53.87 | 67.13 | 724 | 334 | 191 | 143 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=334;symbol_collisions=143 |
| BVB | 56.18 | 100.0 | 89 | 39 | 0 | 39 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=39;symbol_collisions=39 |
| ISE | 56.25 | 100.0 | 16 | 7 | 0 | 7 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=7 |
| LSE | 60.93 | 66.07 | 11184 | 4370 | 3500 | 870 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4370;symbol_collisions=870 |
| NSE_KE | 64.71 | 100.0 | 68 | 24 | 0 | 24 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=24 |
| Euronext | 64.82 | 79.44 | 2021 | 711 | 339 | 372 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=711;symbol_collisions=372 |
| OTC | 69.32 | 69.47 | 11925 | 3658 | 3633 | 25 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3658;symbol_collisions=25 |
| NYSE MKT | 72.96 | 82.05 | 307 | 83 | 49 | 34 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=34 |
| ADX | 74.6 | 97.92 | 126 | 32 | 2 | 30 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=30 |
| DFM | 75.0 | 98.18 | 72 | 18 | 1 | 17 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=18;symbol_collisions=17 |
| TSX | 75.16 | 98.99 | 785 | 195 | 6 | 189 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=189 |
| BHB | 75.61 | 96.88 | 41 | 10 | 1 | 9 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=9 |
| IDX | 78.92 | 97.9 | 944 | 199 | 16 | 183 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=199;symbol_collisions=183 |
| BATS | 79.03 | 81.87 | 1674 | 351 | 293 | 58 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=351;symbol_collisions=58 |
| BK | 79.86 | 100.0 | 139 | 28 | 0 | 28 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=28;symbol_collisions=28 |
| NASDAQ | 81.46 | 82.34 | 5621 | 1042 | 982 | 60 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1042;symbol_collisions=60 |
| SET | 81.65 | 95.3 | 943 | 173 | 38 | 135 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=173;symbol_collisions=135 |
| SGX | 82.81 | 100.0 | 768 | 132 | 0 | 132 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=132;symbol_collisions=132 |
| MSX | 86.36 | 97.94 | 110 | 15 | 2 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=15;symbol_collisions=13 |
| TSXV | 87.09 | 99.77 | 1518 | 196 | 3 | 193 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=193 |
| KOSDAQ | 87.2 | 87.34 | 1820 | 233 | 230 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=3 |
| BME | 90.64 | 90.64 | 267 | 25 | 25 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=0 |
| B3 | 90.76 | 90.76 | 1353 | 125 | 125 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=125;symbol_collisions=0 |
| KRX | 91.4 | 92.01 | 2128 | 183 | 169 | 14 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=183;symbol_collisions=14 |
| TSE | 92.33 | 99.98 | 4444 | 341 | 1 | 340 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=341;symbol_collisions=340 |
| AMS | 93.01 | 99.31 | 615 | 43 | 4 | 39 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=43;symbol_collisions=39 |
| NYSE ARCA | 95.26 | 96.27 | 2765 | 131 | 102 | 29 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=131;symbol_collisions=29 |
| XETRA | 95.6 | 99.94 | 5517 | 243 | 3 | 240 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=243;symbol_collisions=240 |
| QSE | 96.49 | 100.0 | 57 | 2 | 0 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 |
| TWSE | 96.54 | 100.0 | 1099 | 38 | 0 | 38 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=38 |
| BIST | 96.68 | 100.0 | 663 | 22 | 0 | 22 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=22;symbol_collisions=22 |
| BSE_IN | 97.04 | 99.35 | 5165 | 153 | 33 | 120 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=153;symbol_collisions=120 |
| OSL | 97.6 | 99.65 | 292 | 7 | 1 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=6 |
| HKEX | 97.87 | 99.97 | 3240 | 69 | 1 | 68 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=69;symbol_collisions=68 |
| NSE_IN | 97.96 | 100.0 | 3574 | 73 | 0 | 73 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=73;symbol_collisions=73 |
| CSE_LK | 99.07 | 99.07 | 322 | 3 | 3 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 62 | 62 | 62 | 0 | 56 | 0 |
| Australia | 1502 | 1401 | 1492 | 57 | 1176 | 113 |
| Austria | 50 | 50 | 50 | 1 | 41 | 39 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 32 | 32 | 32 | 0 | 27 | 7 |
| Belgium | 129 | 128 | 129 | 6 | 113 | 109 |
| Bermuda | 544 | 544 | 537 | 58 | 475 | 123 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1604 | 1594 | 1602 | 13 | 1259 | 2 |
| British Virgin Islands | 155 | 155 | 154 | 96 | 86 | 22 |
| Bulgaria | 12 | 12 | 11 | 1 | 11 | 0 |
| Canada | 4567 | 4398 | 4412 | 287 | 3229 | 34 |
| Cayman Islands | 2365 | 2356 | 2331 | 618 | 1842 | 244 |
| Chile | 115 | 96 | 115 | 3 | 84 | 2 |
| China | 6474 | 6425 | 6466 | 3 | 5244 | 10 |
| Colombia | 4 | 4 | 4 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 18 | 18 | 18 | 1 | 11 | 0 |
| Czech Republic | 24 | 24 | 24 | 0 | 23 | 20 |
| Denmark | 154 | 154 | 152 | 5 | 143 | 135 |
| Egypt | 230 | 229 | 230 | 0 | 200 | 0 |
| Estonia | 2 | 2 | 2 | 0 | 2 | 2 |
| Faroe Islands | 3 | 3 | 3 | 0 | 2 | 2 |
| Finland | 201 | 201 | 199 | 2 | 193 | 3 |
| France | 703 | 700 | 703 | 15 | 658 | 642 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 872 | 866 | 834 | 10 | 751 | 686 |
| Ghana | 18 | 18 | 18 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 141 | 141 | 139 | 1 | 121 | 118 |
| Guernsey | 68 | 68 | 68 | 4 | 54 | 54 |
| Hong Kong | 515 | 512 | 512 | 0 | 460 | 5 |
| Hungary | 37 | 36 | 37 | 0 | 27 | 0 |
| Iceland | 19 | 19 | 19 | 1 | 19 | 19 |
| India | 5769 | 5769 | 5710 | 0 | 3307 | 4 |
| Indonesia | 719 | 713 | 710 | 0 | 576 | 0 |
| Ireland | 2777 | 2717 | 2726 | 39 | 2428 | 852 |
| Isle of Man | 13 | 13 | 13 | 1 | 13 | 11 |
| Israel | 751 | 750 | 751 | 72 | 701 | 2 |
| Italy | 242 | 241 | 240 | 1 | 223 | 216 |
| Japan | 3454 | 3448 | 3435 | 21 | 3322 | 444 |
| Jersey | 175 | 175 | 174 | 13 | 157 | 155 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 55 | 55 | 55 | 0 | 30 | 0 |
| Kuwait | 108 | 108 | 108 | 0 | 101 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 9 | 9 | 8 | 0 | 2 | 2 |
| Luxembourg | 1070 | 1067 | 1064 | 16 | 992 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 974 | 974 | 974 | 0 | 933 | 1 |
| Malta | 7 | 7 | 7 | 0 | 6 | 6 |
| Marshall Islands | 43 | 43 | 42 | 36 | 26 | 20 |
| Mauritius | 57 | 57 | 57 | 1 | 51 | 0 |
| Mexico | 134 | 128 | 132 | 5 | 108 | 1 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 69 | 69 | 67 | 0 | 54 | 0 |
| Netherlands | 209 | 209 | 188 | 29 | 169 | 124 |
| New Zealand | 74 | 74 | 72 | 0 | 62 | 0 |
| Nigeria | 145 | 145 | 145 | 0 | 133 | 77 |
| Norway | 221 | 221 | 221 | 5 | 206 | 207 |
| Oman | 94 | 91 | 94 | 0 | 0 | 0 |
| Pakistan | 369 | 364 | 369 | 3 | 261 | 0 |
| Panama | 5 | 5 | 4 | 3 | 3 | 0 |
| Papua New Guinea | 1 | 1 | 1 | 0 | 1 | 0 |
| Peru | 32 | 32 | 32 | 1 | 29 | 0 |
| Philippines | 119 | 119 | 115 | 1 | 92 | 17 |
| Poland | 369 | 368 | 366 | 9 | 358 | 355 |
| Portugal | 38 | 38 | 38 | 0 | 38 | 36 |
| Puerto Rico | 6 | 6 | 6 | 5 | 6 | 4 |
| Qatar | 55 | 55 | 55 | 0 | 0 | 0 |
| Romania | 223 | 223 | 200 | 0 | 80 | 77 |
| Russia | 1 | 1 | 1 | 0 | 1 | 0 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 197 | 197 | 195 | 0 | 191 | 0 |
| Singapore | 580 | 576 | 558 | 17 | 40 | 4 |
| Slovenia | 8 | 8 | 7 | 0 | 1 | 1 |
| South Africa | 254 | 254 | 237 | 10 | 175 | 141 |
| South Korea | 3585 | 3583 | 3583 | 1 | 3362 | 0 |
| Spain | 225 | 225 | 225 | 8 | 210 | 206 |
| Sri Lanka | 321 | 321 | 308 | 0 | 305 | 0 |
| Sudan | 1 | 1 | 1 | 0 | 1 | 0 |
| Sweden | 817 | 810 | 807 | 5 | 760 | 753 |
| Switzerland | 373 | 373 | 372 | 22 | 344 | 293 |
| Taiwan | 2282 | 2281 | 2281 | 1 | 2056 | 1 |
| Tanzania | 14 | 14 | 14 | 0 | 12 | 0 |
| Thailand | 627 | 626 | 626 | 6 | 332 | 1 |
| Togo | 1 | 1 | 1 | 0 | 1 | 0 |
| Turkey | 648 | 648 | 632 | 0 | 618 | 552 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 125 | 125 | 125 | 0 | 120 | 0 |
| United Kingdom | 1308 | 1301 | 1296 | 50 | 1209 | 1018 |
| United States | 14841 | 14107 | 14680 | 5254 | 10563 | 3704 |
| Vietnam | 293 | 292 | 277 | 2 | 260 | 0 |
| Zambia | 22 | 22 | 22 | 0 | 21 | 0 |
| Zimbabwe | 27 | 27 | 27 | 0 | 24 | 0 |

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
| Dataset rows | 1589 |
| Active exchange-directory rows | 1353 |
| Matched dataset rows | 1228 |
| Missing dataset rows | 361 |
| Dataset match rate | 77.28 |
| Any official B3 source matched dataset rows | 1250 |
| Any official B3 source missing dataset rows | 339 |
| Any official B3 source match rate | 78.67 |
| Official active symbols not in dataset | 125 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 9 |
| local_share_line | 270 |
| other | 16 |
| unit_or_fund_line | 66 |

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
| B3::B5MB11 | unit_or_fund_line | ETF | present_only_in_non_exchange_directory_source | ETF Bradesco Ima-B5 Plus Fundo De Indice |
| B3::BAOK39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES CORE 30/70 CONSERVATIVE ALLOCATION ETF |
| B3::BBCN39 | bdr_or_foreign_receipt | ETF | absent_from_all_b3_masterfile_sources | JPMORGAN BETABUILDERS CANADA ETF |
| B3::BFIW39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | FIRST TRUST WATER ETF |
| B3::BIAU39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | Ishares Gold Trust |
| B3::BIGE39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES NORTH AMERICAN NATURAL RESOURCES ETF |
| B3::CPTS11B | other | ETF | absent_from_all_b3_masterfile_sources | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DNEN3B | other | Stock | absent_from_all_b3_masterfile_sources | DINAMICA ENERGIA S.A. |
| B3::EQMA5B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | other | Stock | absent_from_all_b3_masterfile_sources | INVITEL LEGACY S.A. |
