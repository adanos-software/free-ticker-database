# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63057 |
| core_listings | 57595 |
| aliases | 124954 |
| stocks | 47421 |
| etfs | 15636 |
| isin_coverage | 61764 |
| sector_coverage | 63056 |
| stock_sector_coverage | 47420 |
| etf_category_coverage | 15636 |
| cik_coverage | 7694 |
| figi_coverage | 65760 |
| lei_coverage | 17951 |
| listing_status_rows | 100646 |
| listing_status_intervals | 100646 |
| listing_events | 45535 |
| listing_keys | 74517 |
| instrument_scope_rows | 74517 |
| instrument_scope_core | 57595 |
| instrument_scope_extended | 16922 |
| instrument_scope_primary_listing | 56974 |
| instrument_scope_primary_listing_missing_isin | 621 |
| instrument_scope_otc_listing | 11024 |
| instrument_scope_secondary_cross_listing | 5898 |
| legacy_primary_ticker_collision_rows | 2022 |
| official_masterfile_symbols | 79615 |
| official_masterfile_matches | 53350 |
| official_masterfile_collisions | 11376 |
| official_masterfile_missing | 14889 |
| official_recall_denominator | 79615 |
| official_recall_matches | 53350 |
| official_recall_missing | 26265 |
| official_recall_pct | 67.01 |
| collision_adjusted_recall_denominator | 68239 |
| collision_adjusted_recall_missing | 14889 |
| collision_adjusted_recall_pct | 78.18 |
| collision_adjusted_recall_gap_rate | 21.82 |
| official_full_recall_target_exchanges | 47 |
| official_full_recall_passing_exchanges | 3 |
| official_full_recall_exception_exchanges | 44 |
| collision_adjusted_full_recall_passing_exchanges | 4 |
| collision_adjusted_full_recall_exception_exchanges | 43 |
| official_recall_decision_counts | {'fixed': 3, 'mostly_collision_hidden': 8, 'out_of_current_scope': 34, 'still_actionable': 36} |
| official_recall_exception_decision_counts | {'mostly_collision_hidden': 8, 'still_actionable': 36} |
| official_recall_unclassified_exception_exchanges | 0 |
| official_full_exchanges | 47 |
| official_partial_exchanges | 34 |
| manual_only_exchanges | 0 |
| missing_exchanges | 0 |
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
| tickers_built_at | 2026-07-07T22:56:15Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-07-07T19:39:54Z |
| masterfiles_age_hours | 3.27 |
| identifiers_generated_at | 2026-07-07T22:56:18Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-07-07T07:32:52Z |
| listing_history_age_hours | 15.39 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 1550.51 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 1550.51 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 1550.51 |
| symbol_changes_generated_at | 2026-07-07T12:19:38Z |
| symbol_changes_age_hours | 10.61 |
| symbol_changes_review_rows | 295 |
| entry_quality_generated_at | 2026-07-07T22:44:51Z |
| entry_quality_age_hours | 0.19 |
| entry_quality_rows | 74517 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 843.63 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-06-02T20:39:44Z |
| ohlcv_plausibility_age_hours | 842.28 |
| ohlcv_plausibility_rows | 240 |
| source_gap_classification_generated_at | 2026-07-07T22:44:53Z |
| source_gap_classification_age_hours | 0.19 |
| source_gap_classification_rows | 6361 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-07-07T22:56:15Z | 0.0 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-07-07T19:39:54Z | 3.27 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-07-07T22:56:18Z | 0.0 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-07-07T07:32:52Z | 15.39 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 1550.51 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 1550.51 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-07-07T12:19:38Z | 10.61 | 295 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-07-07T22:44:51Z | 0.19 | 74517 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-07-07T22:44:53Z | 0.19 | 6361 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 843.63 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-06-02T20:39:44Z | 842.28 | 240 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 10, "old": 127} |
| source_age_bucket_totals | {"age_0_48h": 10, "age_over_336h": 127} |
| refresh_priority_totals | {"P1": 35, "P2": 92, "P4": 10} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 10, "refresh_official_exchange_directory_before_identity_or_collision_work": 34, "refresh_official_subset_before_gap_enrichment": 89, "restore_or_replace_unavailable_source_before_data_fill": 4} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 21 | 27238 | 843.29 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 13 | 32264 | 843.29 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 0 | 843.29 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 44 | 17568 | 843.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 38 | 36962 | 843.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 843.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 3 | 0 | 843.29 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 745 | 843.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 843.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 6 | 25153 | 35.68 | Freshness evidence is present; no data change is authorized by freshness alone. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5537 | 2026-07-06T11:15:42Z | 35.68 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7420 | 2026-07-06T11:15:42Z | 35.68 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 36 | 2026-07-07T09:34:57Z | 13.36 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope corporate_action_daily_list. | Freshness evidence is present; no data change is authorized by freshness alone. |
| lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11021 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_investment_products | ASX | listed_companies_subset | network | 446 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 378 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3704 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1770 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 268 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3856 | 2026-07-07T09:02:49Z | 13.89 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4040 | 2026-07-07T09:02:49Z | 13.89 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_listed_issues | JPX | exchange_directory | network | 4437 | 2026-07-07T10:02:31Z | 12.9 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 4056 | 2026-07-07T10:06:43Z | 12.83 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 472 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3565 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 4544 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8707 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 830 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | cache | 1315 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 189 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 306 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | cache | 134 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_instrument_search | JSE | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 119 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | cache | 91 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1127 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | cache | 26 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | cache | 190 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | cache | 31 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | cache | 50 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | cache | 307 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | cache | 315 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | cache | 17 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | cache | 3 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| byma_equity_details | BYMA | security_lookup_subset | cache | 63 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | cache | 8 | 2026-07-07T09:07:40Z | 13.81 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 66 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3010 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | network | 4866 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| hkex_securities_list | HKEX | exchange_directory | network | 3164 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| sgx_securities_prices | SGX | exchange_directory | cache | 738 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dfm_listed_securities | DFM | exchange_directory | cache | 71 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | cache | 140 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | cache | 41 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | cache | 637 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | cache | 412 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| adx_market_watch | ADX | exchange_directory | cache | 122 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| qse_market_watch | QSE | exchange_directory | cache | 57 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | cache | 18 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | cache | 111 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| sem_isin | SEM | exchange_directory | cache | 47 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | cache | 7 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | cache | 173 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | cache | 27 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | cache | 348 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bvb_fund_units_directory | BVB | listed_companies_subset | cache | 9 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | cache | 133 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 15 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 7 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 7 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 17 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 7 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 746 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 191 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | cache | 32 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | cache | 134 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | cache | 53 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | cache | 30 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 143 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1090 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| twse_etf_list | TWSE | listed_companies_subset | network | 220 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2356 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 881 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2893 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 662 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 891 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 113 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | cache | 887 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | cache | 349 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2759 | 2026-07-07T19:39:54Z | 3.27 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1144 | 2026-07-07T19:39:54Z | 3.27 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 563 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 367 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 716 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | cache | 63 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 957 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 958 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| wse_listed_companies | GPW | listed_companies_subset | cache | 400 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | cache | 364 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | cache | 27 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 524 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 463 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 15 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 16 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | cache | 402 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_etf_list | HOSE | listed_companies_subset | cache | 18 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | cache | 4 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | network | 300 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| upcom_registered_securities | HNX | exchange_directory | network | 830 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | cache | 22 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | cache | 74 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 745 | 2026-06-02T19:38:59Z | 843.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 2026-06-02T19:38:59Z | 843.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 35 |
| P2 | 92 |
| P4 | 10 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 34 |
| refresh_official_subset_before_gap_enrichment | 89 |
| restore_or_replace_unavailable_source_before_data_fill | 4 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | corporate_action_daily_list | 1 |
| fresh_no_refresh_needed | exchange_directory | 6 |
| fresh_no_refresh_needed | listed_companies_subset | 2 |
| fresh_no_refresh_needed | security_identifier_registry_subset | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 34 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 82 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 6 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 1 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 3 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | cache | 1 |
| fresh_no_refresh_needed | network | 9 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 21 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 13 |
| refresh_official_subset_before_gap_enrichment | cache | 48 |
| refresh_official_subset_before_gap_enrichment | network | 41 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 4 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 34 |
| refresh_official_subset_before_gap_enrichment | P2 | 89 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 1 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 3 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 10 |
| age_over_336h | 127 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_over_336h | 34 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 89 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 4 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 34 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 89 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 4 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 34 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 89 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 4 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 21 | 27238 | 843.29 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 13 | 32264 | 843.29 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 0 | 843.29 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 44 | 17568 | 843.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 38 | 36962 | 843.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 4 | 165 | 843.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 3 | 0 | 843.29 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 745 | 843.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 843.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 6 | 25153 | 35.68 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 4056 | 12.83 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 1 | 4040 | 13.89 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | corporate_action_daily_list | network | P4 | 1 | 36 | 13.36 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope corporate_action_daily_list. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | listed_companies_subset | cache | P4 | 1 | 8 | 13.81 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 122 | 85 | 32 | 5 | 69.67 | 30.33 | 94.44 | 5 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=37;symbol_collisions=32 | 100.0 |
| AMS | official_full | 330 | 330 | 264 | 0 | 322 | 153 | 592 | 240 | 295 | 57 | 40.54 | 59.46 | 80.81 | 57 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=352;symbol_collisions=295 | 100.0 |
| ASX | official_partial | 1625 | 1526 | 1622 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 155 | 155 | 155 | 0 | 133 | 130 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1579 | 0 | 1252 | 0 | 1315 | 1245 | 0 | 70 | 94.68 | 5.32 | 94.68 | 70 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=70;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1241 | 1221 | 1225 | 0 | 1048 | 243 | 1490 | 1161 | 39 | 290 | 77.92 | 22.08 | 80.01 | 290 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=329;symbol_collisions=39 | 100.0 |
| BCBA | official_partial | 63 | 63 | 63 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 29 | 29 | 29 | 0 | 27 | 7 | 41 | 29 | 9 | 3 | 70.73 | 29.27 | 90.62 | 3 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 637 | 614 | 20 | 3 | 96.39 | 3.61 | 99.51 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=23;symbol_collisions=20 | 100.0 |
| BK | official_full | 104 | 104 | 103 | 0 | 104 | 0 | 140 | 104 | 27 | 9 | 74.29 | 25.71 | 92.04 | 9 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=27 | 100.0 |
| BME | official_partial | 221 | 221 | 221 | 3 | 221 | 213 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BMV | official_partial | 179 | 162 | 178 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 47 | 0 | 42 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2638 | 2638 | 2637 | 0 | 2620 | 0 | 4866 | 2459 | 1885 | 522 | 50.53 | 49.47 | 82.49 | 522 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2407;symbol_collisions=1885 | 100.0 |
| BVB | official_full | 80 | 80 | 80 | 0 | 80 | 76 | 348 | 75 | 124 | 149 | 21.55 | 78.45 | 33.48 | 149 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=273;symbol_collisions=124 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 277 | 277 | 277 | 0 | 277 | 275 | 2877 | 252 | 1800 | 825 | 8.76 | 91.24 | 23.4 | 825 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2625;symbol_collisions=1800 |  |
| Bursa | official_partial | 936 | 936 | 936 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 145 | 145 | 145 | 0 | 145 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 315 | 307 | 0 | 8 | 97.46 | 2.54 | 97.46 | 8 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=8;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 50 | 1 | 38 | 11 | 2.0 | 98.0 | 8.33 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=49;symbol_collisions=38 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 46 | 16 | 9 | 64.79 | 35.21 | 83.64 | 9 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=16 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 15 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 222 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1081 | 1081 | 994 | 7 | 1071 | 844 | 2003 | 964 | 670 | 369 | 48.13 | 51.87 | 72.32 | 369 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1039;symbol_collisions=670 | 100.0 |
| GSE | official_partial | 19 | 18 | 18 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 194 | 194 | 194 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3044 | 3044 | 3013 | 0 | 3030 | 269 | 3164 | 3037 | 83 | 44 | 95.99 | 4.01 | 98.57 | 44 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=83 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 300 | 105 | 156 | 39 | 35.0 | 65.0 | 72.92 | 39 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 694 | 694 | 694 | 0 | 577 | 0 | 958 | 694 | 244 | 20 | 72.44 | 27.56 | 97.2 | 20 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=264;symbol_collisions=244 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 19 | 9 | 6 | 4 | 47.37 | 52.63 | 69.23 | 4 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1578 | 1578 | 1578 | 0 | 1578 | 0 | 1817 | 1571 | 0 | 246 | 86.46 | 13.54 | 86.46 | 246 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=246;symbol_collisions=0 | 99.62 |
| KRX | official_full | 1796 | 1795 | 1796 | 0 | 1793 | 0 | 2086 | 1774 | 3 | 309 | 85.04 | 14.96 | 85.17 | 309 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=312;symbol_collisions=3 | 99.76 |
| LSE | official_full | 6557 | 6557 | 6266 | 16 | 6532 | 4364 | 11021 | 6419 | 1104 | 3498 | 58.24 | 41.76 | 64.73 | 3498 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4602;symbol_collisions=1104 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 13 | 4 | 84.26 | 15.74 | 95.79 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 | 100.0 |
| NASDAQ | official_full | 4675 | 4595 | 4664 | 3485 | 3442 | 1399 | 5648 | 4591 | 47 | 1010 | 81.29 | 18.71 | 81.97 | 1010 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1057;symbol_collisions=47 | 99.56 |
| NEO | official_full | 197 | 154 | 191 | 0 | 149 | 1 | 440 | 190 | 88 | 162 | 43.18 | 56.82 | 53.98 | 162 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=250;symbol_collisions=88 | 100.0 |
| NGX | official_full | 145 | 145 | 144 | 0 | 133 | 76 | 133 | 133 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2499 | 0 | 3010 | 2369 | 196 | 445 | 78.7 | 21.3 | 84.19 | 445 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 45 | 0 | 42 | 1 | 66 | 11 | 23 | 32 | 16.67 | 83.33 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 | 100.0 |
| NYSE | official_full | 2036 | 1988 | 2034 | 1959 | 1485 | 1031 | 3904 | 2024 | 500 | 1380 | 51.84 | 48.16 | 59.46 | 1380 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1880;symbol_collisions=500 | 100.0 |
| NYSE ARCA | official_full | 2654 | 2607 | 2628 | 116 | 2103 | 371 | 2672 | 2548 | 30 | 94 | 95.36 | 4.64 | 96.44 | 94 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=124;symbol_collisions=30 | 100.0 |
| NYSE MKT | official_full | 232 | 223 | 232 | 148 | 154 | 52 | 311 | 228 | 27 | 56 | 73.31 | 26.69 | 80.28 | 56 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=27 | 100.0 |
| NZX | official_full | 45 | 45 | 42 | 0 | 45 | 1 | 173 | 45 | 126 | 2 | 26.01 | 73.99 | 95.74 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 | 100.0 |
| OSL | official_full | 265 | 265 | 261 | 2 | 258 | 243 | 298 | 245 | 46 | 7 | 82.21 | 17.79 | 97.22 | 7 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 | 100.0 |
| OTC | official_full | 11024 | 10317 | 10788 | 1865 | 8937 | 2898 | 11925 | 7634 | 39 | 4252 | 64.02 | 35.98 | 64.23 | 4252 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4291;symbol_collisions=39 | 88.97 |
| PSE | official_full | 90 | 90 | 89 | 1 | 90 | 18 | 381 | 90 | 182 | 109 | 23.62 | 76.38 | 45.23 | 109 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=291;symbol_collisions=182 | 100.0 |
| PSE_CZ | official_partial | 26 | 26 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 371 | 366 | 371 | 3 | 263 | 2 | 716 | 371 | 155 | 190 | 51.82 | 48.18 | 66.13 | 190 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=155 | 99.73 |
| QSE | official_full | 54 | 54 | 54 | 0 | 0 | 0 | 57 | 54 | 2 | 1 | 94.74 | 5.26 | 98.18 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 53 | 53 | 51 | 1 | 50 | 2 | 47 | 47 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 90.2 |
| SET | official_full | 547 | 547 | 547 | 4 | 335 | 4 | 944 | 545 | 347 | 52 | 57.73 | 42.27 | 91.29 | 52 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=399;symbol_collisions=347 | 100.0 |
| SGX | official_full | 591 | 591 | 553 | 0 | 8 | 18 | 738 | 589 | 140 | 9 | 79.81 | 20.19 | 98.49 | 9 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=149;symbol_collisions=140 | 99.63 |
| SIX | official_partial | 757 | 757 | 757 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2789 | 2754 | 2789 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 116 | 89 | 115 | 0 | 85 | 1 | 111 | 111 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 834 | 834 | 833 | 2 | 829 | 809 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3083 | 3071 | 3083 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 191 | 191 | 191 | 0 | 191 | 0 | 412 | 191 | 217 | 4 | 46.36 | 53.64 | 97.95 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=217 | 100.0 |
| TASE | official_partial | 672 | 672 | 661 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1118 | 1118 | 1118 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4060 | 4060 | 4048 | 0 | 4060 | 485 | 4437 | 4036 | 342 | 59 | 90.96 | 9.04 | 98.56 | 59 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=342 | 100.0 |
| TSX | official_full | 1903 | 1813 | 1860 | 12 | 1656 | 43 | 788 | 326 | 454 | 8 | 41.37 | 58.63 | 97.6 | 8 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=462;symbol_collisions=454 | 99.32 |
| TSXV | official_full | 1066 | 989 | 1064 | 17 | 919 | 9 | 1600 | 1043 | 544 | 13 | 65.19 | 34.81 | 98.77 | 13 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=557;symbol_collisions=544 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1191 | 0 | 1165 | 3 | 1090 | 973 | 59 | 58 | 89.27 | 10.73 | 94.37 | 58 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=117;symbol_collisions=59 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 830 | 2 | 475 | 353 | 0.24 | 99.76 | 0.56 | 353 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=828;symbol_collisions=475 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 56 | 56 | 56 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 542 | 542 | 541 | 7 | 541 | 522 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XETRA | official_full | 3844 | 3844 | 3232 | 8 | 3830 | 1926 | 4544 | 3672 | 773 | 99 | 80.81 | 19.19 | 97.37 | 99 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=872;symbol_collisions=773 | 99.88 |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.56 | 830 | 828 | 353 | 475 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=828;symbol_collisions=475 |
| CSE_MA | 2.0 | 8.33 | 50 | 49 | 11 | 38 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=49;symbol_collisions=38 |
| Borsa Italiana | 8.76 | 23.4 | 2877 | 2625 | 825 | 1800 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2625;symbol_collisions=1800 |
| NSE_KE | 16.67 | 25.58 | 66 | 55 | 32 | 23 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 |
| BVB | 21.55 | 33.48 | 348 | 273 | 149 | 124 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=273;symbol_collisions=124 |
| PSE | 23.62 | 45.23 | 381 | 291 | 109 | 182 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=291;symbol_collisions=182 |
| NZX | 26.01 | 95.74 | 173 | 128 | 2 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 |
| HNX | 35.0 | 72.92 | 300 | 195 | 39 | 156 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 |
| AMS | 40.54 | 80.81 | 592 | 352 | 57 | 295 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=352;symbol_collisions=295 |
| TSX | 41.37 | 97.6 | 788 | 462 | 8 | 454 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=462;symbol_collisions=454 |
| NEO | 43.18 | 53.98 | 440 | 250 | 162 | 88 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=250;symbol_collisions=88 |
| TADAWUL | 46.36 | 97.95 | 412 | 221 | 4 | 217 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=217 |
| ISE | 47.37 | 69.23 | 19 | 10 | 4 | 6 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=6 |
| Euronext | 48.13 | 72.32 | 2003 | 1039 | 369 | 670 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1039;symbol_collisions=670 |
| BSE_IN | 50.53 | 82.49 | 4866 | 2407 | 522 | 1885 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2407;symbol_collisions=1885 |
| PSX | 51.82 | 66.13 | 716 | 345 | 190 | 155 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=155 |
| NYSE | 51.84 | 59.46 | 3904 | 1880 | 1380 | 500 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1880;symbol_collisions=500 |
| SET | 57.73 | 91.29 | 944 | 399 | 52 | 347 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=399;symbol_collisions=347 |
| LSE | 58.24 | 64.73 | 11021 | 4602 | 3498 | 1104 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4602;symbol_collisions=1104 |
| OTC | 64.02 | 64.23 | 11925 | 4291 | 4252 | 39 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4291;symbol_collisions=39 |
| DFM | 64.79 | 83.64 | 71 | 25 | 9 | 16 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=16 |
| TSXV | 65.19 | 98.77 | 1600 | 557 | 13 | 544 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=557;symbol_collisions=544 |
| ADX | 69.67 | 94.44 | 122 | 37 | 5 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=37;symbol_collisions=32 |
| BHB | 70.73 | 90.62 | 41 | 12 | 3 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=9 |
| IDX | 72.44 | 97.2 | 958 | 264 | 20 | 244 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=264;symbol_collisions=244 |
| NYSE MKT | 73.31 | 80.28 | 311 | 83 | 56 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=27 |
| BK | 74.29 | 92.04 | 140 | 36 | 9 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=27 |
| BATS | 77.92 | 80.01 | 1490 | 329 | 290 | 39 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=329;symbol_collisions=39 |
| NSE_IN | 78.7 | 84.19 | 3010 | 641 | 445 | 196 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 |
| SGX | 79.81 | 98.49 | 738 | 149 | 9 | 140 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=149;symbol_collisions=140 |
| XETRA | 80.81 | 97.37 | 4544 | 872 | 99 | 773 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=872;symbol_collisions=773 |
| NASDAQ | 81.29 | 81.97 | 5648 | 1057 | 1010 | 47 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1057;symbol_collisions=47 |
| OSL | 82.21 | 97.22 | 298 | 53 | 7 | 46 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 |
| MSX | 84.26 | 95.79 | 108 | 17 | 4 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 |
| KRX | 85.04 | 85.17 | 2086 | 312 | 309 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=312;symbol_collisions=3 |
| KOSDAQ | 86.46 | 86.46 | 1817 | 246 | 246 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=246;symbol_collisions=0 |
| TWSE | 89.27 | 94.37 | 1090 | 117 | 58 | 59 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=117;symbol_collisions=59 |
| TSE | 90.96 | 98.56 | 4437 | 401 | 59 | 342 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=342 |
| B3 | 94.68 | 94.68 | 1315 | 70 | 70 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=70;symbol_collisions=0 |
| QSE | 94.74 | 98.18 | 57 | 3 | 1 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=2 |
| NYSE ARCA | 95.36 | 96.44 | 2672 | 124 | 94 | 30 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=124;symbol_collisions=30 |
| HKEX | 95.99 | 98.57 | 3164 | 127 | 44 | 83 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=83 |
| BIST | 96.39 | 99.51 | 637 | 23 | 3 | 20 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=23;symbol_collisions=20 |
| CSE_LK | 97.46 | 97.46 | 315 | 8 | 8 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=8;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 59 | 59 | 59 | 0 | 56 | 0 |
| Australia | 1574 | 1468 | 1574 | 74 | 1339 | 138 |
| Austria | 63 | 63 | 63 | 0 | 55 | 60 |
| Bahrain | 30 | 30 | 30 | 0 | 28 | 7 |
| Belgium | 124 | 123 | 124 | 4 | 121 | 118 |
| Bermuda | 532 | 532 | 532 | 60 | 497 | 132 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1580 | 1569 | 1579 | 0 | 1249 | 0 |
| British Virgin Islands | 1 | 1 | 1 | 1 | 0 | 0 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4664 | 4442 | 4664 | 521 | 3979 | 53 |
| Cayman Islands | 2190 | 2184 | 2190 | 558 | 1883 | 238 |
| Chile | 113 | 86 | 113 | 0 | 82 | 0 |
| China | 6343 | 6293 | 6343 | 1 | 5234 | 3 |
| Colombia | 3 | 3 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 17 | 17 | 17 | 1 | 12 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 148 | 148 | 148 | 4 | 144 | 136 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 3 |
| Finland | 194 | 194 | 194 | 1 | 194 | 0 |
| France | 728 | 725 | 728 | 9 | 714 | 713 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 782 | 780 | 782 | 6 | 773 | 707 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 1 | 1 | 1 | 0 | 1 | 1 |
| Greece | 141 | 141 | 141 | 1 | 125 | 121 |
| Guernsey | 69 | 69 | 69 | 5 | 61 | 62 |
| Hong Kong | 470 | 469 | 470 | 1 | 466 | 2 |
| Hungary | 36 | 35 | 36 | 0 | 28 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5020 | 5020 | 5020 | 0 | 4998 | 0 |
| Indonesia | 705 | 701 | 705 | 3 | 582 | 0 |
| Ireland | 2570 | 2560 | 2570 | 36 | 2543 | 912 |
| Isle of Man | 14 | 14 | 14 | 1 | 13 | 12 |
| Israel | 753 | 750 | 753 | 87 | 730 | 2 |
| Italy | 229 | 227 | 229 | 1 | 222 | 217 |
| Japan | 3328 | 3326 | 3328 | 17 | 3320 | 463 |
| Jersey | 169 | 169 | 169 | 16 | 166 | 164 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 2 |
| Luxembourg | 1013 | 1010 | 1013 | 13 | 1007 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 940 | 940 | 940 | 0 | 932 | 1 |
| Malta | 6 | 6 | 6 | 0 | 6 | 6 |
| Marshall Islands | 41 | 41 | 41 | 36 | 28 | 22 |
| Mauritius | 61 | 61 | 61 | 2 | 56 | 0 |
| Mexico | 134 | 116 | 134 | 4 | 112 | 2 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 66 | 0 | 62 | 0 |
| Netherlands | 189 | 189 | 189 | 27 | 185 | 140 |
| New Zealand | 82 | 82 | 82 | 2 | 75 | 0 |
| Nigeria | 147 | 147 | 147 | 0 | 135 | 78 |
| Norway | 240 | 239 | 240 | 3 | 231 | 230 |
| Oman | 90 | 90 | 90 | 0 | 0 | 0 |
| Pakistan | 371 | 366 | 371 | 3 | 263 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 31 | 31 | 31 | 0 | 29 | 0 |
| Philippines | 99 | 99 | 99 | 2 | 98 | 24 |
| Poland | 367 | 366 | 367 | 8 | 364 | 361 |
| Portugal | 39 | 39 | 39 | 0 | 39 | 37 |
| Qatar | 54 | 54 | 54 | 0 | 0 | 0 |
| Romania | 80 | 80 | 80 | 0 | 80 | 77 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 191 | 0 | 191 | 0 |
| Singapore | 548 | 544 | 548 | 15 | 48 | 4 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 1 |
| South Africa | 230 | 230 | 230 | 5 | 179 | 145 |
| South Korea | 3365 | 3363 | 3365 | 0 | 3361 | 0 |
| Spain | 238 | 238 | 238 | 4 | 238 | 236 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 802 | 798 | 802 | 4 | 791 | 788 |
| Switzerland | 390 | 390 | 390 | 21 | 384 | 338 |
| Taiwan | 2273 | 2273 | 2273 | 0 | 2055 | 0 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 549 | 549 | 549 | 7 | 333 | 0 |
| Turkey | 614 | 614 | 614 | 0 | 614 | 550 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1318 | 1315 | 1318 | 46 | 1284 | 1096 |
| United States | 14557 | 13760 | 14557 | 5412 | 11227 | 4058 |
| Vietnam | 261 | 260 | 261 | 2 | 260 | 0 |
| Zambia | 22 | 22 | 22 | 0 | 21 | 0 |
| Zimbabwe | 28 | 28 | 28 | 0 | 25 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 4001 | 3150 | 0 | 850 | 1 |
| B3 | official_full | 766 | 766 | 0 | 0 | 0 |
| BMV | official_partial | 150 | 150 | 0 | 0 | 0 |
| BME | official_partial | 93 | 93 | 0 | 0 | 0 |
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
| Active exchange-directory rows | 1315 |
| Matched dataset rows | 1245 |
| Missing dataset rows | 336 |
| Dataset match rate | 78.75 |
| Any official B3 source matched dataset rows | 1262 |
| Any official B3 source missing dataset rows | 319 |
| Any official B3 source match rate | 79.82 |
| Official active symbols not in dataset | 70 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 2 |
| local_share_line | 265 |
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
