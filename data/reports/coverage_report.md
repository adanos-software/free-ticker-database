# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63119 |
| core_listings | 57644 |
| aliases | 124970 |
| stocks | 47358 |
| etfs | 15761 |
| isin_coverage | 61623 |
| sector_coverage | 62943 |
| stock_sector_coverage | 47304 |
| etf_category_coverage | 15639 |
| cik_coverage | 7672 |
| figi_coverage | 65450 |
| lei_coverage | 17809 |
| listing_status_rows | 100646 |
| listing_status_intervals | 100646 |
| listing_events | 45535 |
| listing_keys | 74554 |
| instrument_scope_rows | 74554 |
| instrument_scope_core | 57644 |
| instrument_scope_extended | 16910 |
| instrument_scope_primary_listing | 56867 |
| instrument_scope_primary_listing_missing_isin | 777 |
| instrument_scope_otc_listing | 11076 |
| instrument_scope_secondary_cross_listing | 5834 |
| legacy_primary_ticker_collision_rows | 2024 |
| official_masterfile_symbols | 80739 |
| official_masterfile_matches | 53344 |
| official_masterfile_collisions | 11543 |
| official_masterfile_missing | 15852 |
| official_recall_denominator | 80739 |
| official_recall_matches | 53344 |
| official_recall_missing | 27395 |
| official_recall_pct | 66.07 |
| collision_adjusted_recall_denominator | 69196 |
| collision_adjusted_recall_missing | 15852 |
| collision_adjusted_recall_pct | 77.09 |
| collision_adjusted_recall_gap_rate | 22.91 |
| official_full_recall_target_exchanges | 48 |
| official_full_recall_passing_exchanges | 2 |
| official_full_recall_exception_exchanges | 46 |
| collision_adjusted_full_recall_passing_exchanges | 3 |
| collision_adjusted_full_recall_exception_exchanges | 45 |
| official_recall_decision_counts | {'fixed': 2, 'mostly_collision_hidden': 7, 'out_of_current_scope': 33, 'still_actionable': 39} |
| official_recall_exception_decision_counts | {'mostly_collision_hidden': 7, 'still_actionable': 39} |
| official_recall_unclassified_exception_exchanges | 0 |
| official_full_exchanges | 48 |
| official_partial_exchanges | 33 |
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
| tickers_built_at | 2026-08-02T17:48:00Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-08-02T08:35:09Z |
| masterfiles_age_hours | 9.22 |
| identifiers_generated_at | 2026-08-02T17:48:00Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-07-07T07:32:52Z |
| listing_history_age_hours | 634.25 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 2169.37 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 2169.37 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 2169.37 |
| symbol_changes_generated_at | 2026-08-01T16:50:58Z |
| symbol_changes_age_hours | 24.95 |
| symbol_changes_review_rows | 317 |
| entry_quality_generated_at | 2026-08-02T08:52:24Z |
| entry_quality_age_hours | 8.93 |
| entry_quality_rows | 74554 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 1462.5 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 24.9 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-08-02T08:52:32Z |
| source_gap_classification_age_hours | 8.93 |
| source_gap_classification_rows | 6694 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-08-02T17:48:00Z | 0.0 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-08-02T08:35:09Z | 9.22 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-08-02T17:48:00Z | 0.0 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-07-07T07:32:52Z | 634.25 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 2169.37 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 2169.37 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-08-01T16:50:58Z | 24.95 | 317 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-08-02T08:52:24Z | 8.93 | 74554 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-08-02T08:52:32Z | 8.93 | 6694 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 1462.5 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 24.9 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 36, "old": 53, "stale": 48} |
| source_age_bucket_totals | {"age_0_48h": 36, "age_168_336h": 4, "age_48_168h": 48, "age_over_336h": 49} |
| refresh_priority_totals | {"P1": 13, "P2": 88, "P4": 36} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 36, "refresh_official_exchange_directory_before_identity_or_collision_work": 11, "refresh_official_subset_before_gap_enrichment": 75, "restore_or_replace_unavailable_source_before_data_fill": 15} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 8 | 22631 | 1462.15 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 3 | 4107 | 1462.15 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 3968 | 1462.15 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 28 | 22550 | 1462.15 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 21 | 1821 | 1462.15 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 19 | 37879 | 151.7 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 12 | 24522 | 1462.15 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 839 | 1462.15 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | network | P2 | 1 | 4030 | 128.68 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1462.15 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5569 | 2026-08-01T08:55:07Z | 32.88 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7512 | 2026-08-01T08:55:07Z | 32.88 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 36 | 2026-07-07T09:34:57Z | 632.22 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11092 | 2026-07-29T09:09:22Z | 104.65 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 343.9 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| asx_investment_products | ASX | listed_companies_subset | network | 458 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 493 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3619 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1774 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 267 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_equities | Euronext | exchange_directory | unavailable | 3857 | 2026-07-21T08:41:58Z | 297.1 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| euronext_etfs | Euronext | listed_companies_subset | unavailable | 4064 | 2026-07-21T08:41:58Z | 297.1 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| jpx_listed_issues | JPX | exchange_directory | network | 4437 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 4030 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_identifier_registry_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 468 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3652 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5080 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8900 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 844 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | network | 1327 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 213 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 314 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_etf_list | JSE | listed_companies_subset | network | 140 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 123 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 50 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 118 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1127 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | unavailable | 26 | 2026-07-20T09:31:47Z | 320.27 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 20 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 82 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 318 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 63 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | cache | 8 | 2026-07-07T09:07:40Z | 632.67 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 66 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3010 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | network | 5077 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hkex_securities_list | HKEX | exchange_directory | network | 3206 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sgx_securities_prices | SGX | exchange_directory | network | 746 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 647 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| adx_market_watch | ADX | exchange_directory | network | 123 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| qse_market_watch | QSE | exchange_directory | cache | 57 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 111 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | cache | 47 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nzx_instruments | NZX | exchange_directory | cache | 173 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_shares_directory | BVB | exchange_directory | network | 350 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 9 | 2026-07-27T10:06:05Z | 151.7 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | cache | 133 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 10 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_etf_search | BMV | listed_companies_subset | network | 2 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 9 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 9.22 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 746 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 191 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | cache | 32 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 125 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | cache | 53 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | cache | 30 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 143 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1093 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| twse_etf_list | TWSE | listed_companies_subset | network | 231 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2351 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 909 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 704 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 896 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 117 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 890 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 359 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_listed_companies | KRX | exchange_directory | network | 2760 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_etf_finder | KRX | exchange_directory | network | 1155 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_listed_companies | PSX | listed_companies_subset | network | 563 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 367 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 716 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | cache | 63 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 963 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 963 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_listed_companies | GPW | listed_companies_subset | network | 403 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | cache | 364 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | network | 36 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 530 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 465 | 2026-07-31T09:14:12Z | 56.56 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | unavailable | 403 | 2026-07-21T08:41:58Z | 297.1 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-07-28T09:07:29Z | 128.68 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| upcom_registered_securities | HNX | exchange_directory | network | 822 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 38 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 73 | 2026-08-01T08:32:15Z | 33.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 745 | 2026-06-02T19:38:59Z | 1462.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 2026-06-02T19:38:59Z | 1462.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 13 |
| P2 | 88 |
| P4 | 36 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 11 |
| refresh_official_subset_before_gap_enrichment | 75 |
| restore_or_replace_unavailable_source_before_data_fill | 15 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 9 |
| fresh_no_refresh_needed | interlisted_subset | 1 |
| fresh_no_refresh_needed | listed_companies_subset | 26 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 11 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | exchange_directory | 19 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 49 |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | 1 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 5 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 2 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 12 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 1 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 8 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 3 |
| refresh_official_subset_before_gap_enrichment | cache | 22 |
| refresh_official_subset_before_gap_enrichment | network | 53 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 15 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 11 |
| refresh_official_subset_before_gap_enrichment | P2 | 75 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 2 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 13 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 36 |
| age_168_336h | 4 |
| age_48_168h | 48 |
| age_over_336h | 49 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_over_336h | 11 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 48 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 27 |
| restore_or_replace_unavailable_source_before_data_fill | age_168_336h | 4 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 11 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 11 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 75 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 15 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 36 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 11 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 75 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 15 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 8 | 22631 | 1462.15 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 3 | 4107 | 1462.15 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 3968 | 1462.15 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 28 | 22550 | 1462.15 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 21 | 1821 | 1462.15 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 19 | 37879 | 151.7 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 12 | 24522 | 1462.15 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 839 | 1462.15 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_identifier_registry_subset | network | P2 | 1 | 4030 | 128.68 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_identifier_registry_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1462.15 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 36 | 632.22 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 1 | 7 | 1462.15 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 26 | 10256 | 33.26 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 9 | 17184 | 33.26 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | interlisted_subset | network | P4 | 1 | 267 | 33.26 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope interlisted_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 123 | 85 | 32 | 6 | 69.11 | 30.89 | 93.41 | 6 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 | 100.0 |
| AMS | official_full | 331 | 331 | 265 | 0 | 322 | 153 | 601 | 241 | 296 | 64 | 40.1 | 59.9 | 79.02 | 64 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=360;symbol_collisions=296 | 100.0 |
| ASX | official_partial | 1625 | 1526 | 1622 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 155 | 155 | 155 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1579 | 0 | 1252 | 0 | 1327 | 1220 | 0 | 107 | 91.94 | 8.06 | 91.94 | 107 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1314 | 1224 | 1228 | 0 | 1048 | 243 | 1559 | 1223 | 48 | 288 | 78.45 | 21.55 | 80.94 | 288 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=336;symbol_collisions=48 | 100.0 |
| BCBA | official_partial | 63 | 63 | 63 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 28 | 28 | 28 | 0 | 26 | 7 | 41 | 28 | 9 | 4 | 68.29 | 31.71 | 87.5 | 4 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 647 | 611 | 20 | 16 | 94.44 | 5.56 | 97.45 | 16 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=20 | 100.0 |
| BK | official_full | 104 | 104 | 103 | 0 | 104 | 0 | 140 | 102 | 27 | 11 | 72.86 | 27.14 | 90.27 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 | 100.0 |
| BME | official_full | 220 | 220 | 220 | 3 | 220 | 212 | 50 | 12 | 6 | 32 | 24.0 | 76.0 | 27.27 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=6 | 100.0 |
| BMV | official_partial | 179 | 162 | 178 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 47 | 0 | 41 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2637 | 2637 | 2636 | 0 | 2613 | 0 | 5077 | 2576 | 1893 | 608 | 50.74 | 49.26 | 80.9 | 608 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2501;symbol_collisions=1893 | 100.0 |
| BVB | official_full | 80 | 80 | 80 | 0 | 80 | 76 | 350 | 75 | 126 | 149 | 21.43 | 78.57 | 33.48 | 149 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=126 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 277 | 275 | 2893 | 253 | 1803 | 837 | 8.75 | 91.25 | 23.21 | 837 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2640;symbol_collisions=1803 |  |
| Bursa | official_partial | 936 | 936 | 936 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 145 | 145 | 145 | 0 | 145 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 318 | 307 | 0 | 11 | 96.54 | 3.46 | 96.54 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 82 | 1 | 61 | 20 | 1.22 | 98.78 | 4.76 | 20 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=61 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 45 | 16 | 10 | 63.38 | 36.62 | 81.82 | 10 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=16 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 15 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 222 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1083 | 1083 | 996 | 7 | 1071 | 844 | 2007 | 965 | 667 | 375 | 48.08 | 51.92 | 72.01 | 375 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1042;symbol_collisions=667 | 100.0 |
| GSE | official_partial | 19 | 18 | 18 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 194 | 194 | 194 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3044 | 3044 | 3013 | 0 | 3012 | 268 | 3206 | 3027 | 84 | 95 | 94.42 | 5.58 | 96.96 | 95 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=179;symbol_collisions=84 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 299 | 104 | 156 | 39 | 34.78 | 65.22 | 72.73 | 39 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 694 | 694 | 694 | 0 | 577 | 0 | 963 | 694 | 243 | 26 | 72.07 | 27.93 | 96.39 | 26 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=269;symbol_collisions=243 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 15 | 9 | 6 | 0 | 60.0 | 40.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1578 | 1578 | 1578 | 0 | 1578 | 0 | 1818 | 1570 | 0 | 248 | 86.36 | 13.64 | 86.36 | 248 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=248;symbol_collisions=0 | 99.62 |
| KRX | official_full | 1796 | 1795 | 1796 | 0 | 1793 | 0 | 2097 | 1768 | 3 | 326 | 84.31 | 15.69 | 84.43 | 326 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=329;symbol_collisions=3 | 99.76 |
| LSE | official_full | 6557 | 6556 | 6266 | 16 | 6499 | 4346 | 11092 | 6388 | 1123 | 3581 | 57.59 | 42.41 | 64.08 | 3581 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4704;symbol_collisions=1123 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 13 | 4 | 84.26 | 15.74 | 95.79 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 | 100.0 |
| NASDAQ | official_full | 4663 | 4525 | 4599 | 3448 | 3392 | 1384 | 5739 | 4563 | 63 | 1113 | 79.51 | 20.49 | 80.39 | 1113 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1176;symbol_collisions=63 | 99.56 |
| NEO | official_full | 197 | 154 | 191 | 0 | 149 | 1 | 440 | 183 | 86 | 171 | 41.59 | 58.41 | 51.69 | 171 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 | 100.0 |
| NGX | official_full | 145 | 145 | 144 | 0 | 133 | 76 | 133 | 133 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2499 | 0 | 3010 | 2369 | 196 | 445 | 78.7 | 21.3 | 84.19 | 445 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 45 | 0 | 42 | 1 | 66 | 11 | 23 | 32 | 16.67 | 83.33 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 | 100.0 |
| NYSE | official_full | 2002 | 1951 | 1992 | 1925 | 1442 | 995 | 3917 | 1991 | 501 | 1425 | 50.83 | 49.17 | 58.28 | 1425 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1926;symbol_collisions=501 | 100.0 |
| NYSE ARCA | official_full | 2684 | 2603 | 2628 | 113 | 2099 | 368 | 2697 | 2570 | 32 | 95 | 95.29 | 4.71 | 96.44 | 95 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=32 | 100.0 |
| NYSE MKT | official_full | 229 | 219 | 228 | 146 | 151 | 51 | 308 | 224 | 29 | 55 | 72.73 | 27.27 | 80.29 | 55 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=84;symbol_collisions=29 | 100.0 |
| NZX | official_full | 45 | 45 | 42 | 0 | 45 | 1 | 173 | 45 | 126 | 2 | 26.01 | 73.99 | 95.74 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 | 100.0 |
| OSL | official_full | 265 | 265 | 261 | 2 | 258 | 243 | 298 | 245 | 46 | 7 | 82.21 | 17.79 | 97.22 | 7 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 | 100.0 |
| OTC | official_full | 11076 | 10318 | 10840 | 1919 | 8846 | 2847 | 11925 | 7676 | 38 | 4211 | 64.37 | 35.63 | 64.57 | 4211 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4249;symbol_collisions=38 | 88.97 |
| PSE | official_full | 88 | 88 | 87 | 1 | 88 | 16 | 381 | 88 | 181 | 112 | 23.1 | 76.9 | 44.0 | 112 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=293;symbol_collisions=181 | 100.0 |
| PSE_CZ | official_partial | 26 | 26 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 371 | 366 | 371 | 3 | 263 | 2 | 716 | 371 | 154 | 191 | 51.82 | 48.18 | 66.01 | 191 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=154 | 99.73 |
| QSE | official_full | 54 | 54 | 54 | 0 | 0 | 0 | 57 | 54 | 2 | 1 | 94.74 | 5.26 | 98.18 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 50 | 1 | 49 | 2 | 47 | 46 | 0 | 1 | 97.87 | 2.13 | 97.87 | 1 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1;symbol_collisions=0 | 90.2 |
| SET | official_full | 547 | 547 | 547 | 4 | 335 | 4 | 944 | 543 | 347 | 54 | 57.52 | 42.48 | 90.95 | 54 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=347 | 100.0 |
| SGX | official_full | 591 | 591 | 553 | 0 | 8 | 18 | 746 | 589 | 142 | 15 | 78.95 | 21.05 | 97.52 | 15 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=157;symbol_collisions=142 | 99.63 |
| SIX | official_partial | 758 | 758 | 758 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2789 | 2754 | 2789 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 116 | 89 | 115 | 0 | 85 | 1 | 111 | 111 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 832 | 832 | 831 | 2 | 825 | 805 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3083 | 3071 | 3083 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 191 | 191 | 191 | 0 | 191 | 0 | 412 | 190 | 217 | 5 | 46.12 | 53.88 | 97.44 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=222;symbol_collisions=217 | 100.0 |
| TASE | official_partial | 672 | 672 | 661 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1118 | 1118 | 1118 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4060 | 4060 | 4048 | 0 | 4060 | 485 | 4437 | 4036 | 342 | 59 | 90.96 | 9.04 | 98.56 | 59 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=342 | 100.0 |
| TSX | official_full | 1844 | 1766 | 1803 | 12 | 1621 | 40 | 785 | 323 | 453 | 9 | 41.15 | 58.85 | 97.29 | 9 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=462;symbol_collisions=453 | 99.32 |
| TSXV | official_full | 1057 | 980 | 1055 | 17 | 911 | 9 | 1518 | 961 | 542 | 15 | 63.31 | 36.69 | 98.46 | 15 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=557;symbol_collisions=542 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1191 | 0 | 1165 | 3 | 1093 | 972 | 62 | 59 | 88.93 | 11.07 | 94.28 | 59 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=62 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 822 | 2 | 476 | 344 | 0.24 | 99.76 | 0.58 | 344 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=476 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 56 | 56 | 56 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 542 | 542 | 541 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XETRA | official_full | 3846 | 3845 | 3234 | 8 | 3828 | 1925 | 5080 | 3653 | 853 | 574 | 71.91 | 28.09 | 86.42 | 574 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1427;symbol_collisions=853 | 99.88 |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.58 | 822 | 820 | 344 | 476 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=476 |
| CSE_MA | 1.22 | 4.76 | 82 | 81 | 20 | 61 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=61 |
| Borsa Italiana | 8.75 | 23.21 | 2893 | 2640 | 837 | 1803 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2640;symbol_collisions=1803 |
| NSE_KE | 16.67 | 25.58 | 66 | 55 | 32 | 23 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 |
| BVB | 21.43 | 33.48 | 350 | 275 | 149 | 126 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=126 |
| PSE | 23.1 | 44.0 | 381 | 293 | 112 | 181 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=293;symbol_collisions=181 |
| BME | 24.0 | 27.27 | 50 | 38 | 32 | 6 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=6 |
| NZX | 26.01 | 95.74 | 173 | 128 | 2 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 |
| HNX | 34.78 | 72.73 | 299 | 195 | 39 | 156 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 |
| AMS | 40.1 | 79.02 | 601 | 360 | 64 | 296 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=360;symbol_collisions=296 |
| TSX | 41.15 | 97.29 | 785 | 462 | 9 | 453 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=462;symbol_collisions=453 |
| NEO | 41.59 | 51.69 | 440 | 257 | 171 | 86 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 |
| TADAWUL | 46.12 | 97.44 | 412 | 222 | 5 | 217 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=222;symbol_collisions=217 |
| Euronext | 48.08 | 72.01 | 2007 | 1042 | 375 | 667 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1042;symbol_collisions=667 |
| BSE_IN | 50.74 | 80.9 | 5077 | 2501 | 608 | 1893 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2501;symbol_collisions=1893 |
| NYSE | 50.83 | 58.28 | 3917 | 1926 | 1425 | 501 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1926;symbol_collisions=501 |
| PSX | 51.82 | 66.01 | 716 | 345 | 191 | 154 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=154 |
| SET | 57.52 | 90.95 | 944 | 401 | 54 | 347 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=347 |
| LSE | 57.59 | 64.08 | 11092 | 4704 | 3581 | 1123 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4704;symbol_collisions=1123 |
| ISE | 60.0 | 100.0 | 15 | 6 | 0 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 |
| TSXV | 63.31 | 98.46 | 1518 | 557 | 15 | 542 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=557;symbol_collisions=542 |
| DFM | 63.38 | 81.82 | 71 | 26 | 10 | 16 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=16 |
| OTC | 64.37 | 64.57 | 11925 | 4249 | 4211 | 38 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4249;symbol_collisions=38 |
| BHB | 68.29 | 87.5 | 41 | 13 | 4 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 |
| ADX | 69.11 | 93.41 | 123 | 38 | 6 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 |
| XETRA | 71.91 | 86.42 | 5080 | 1427 | 574 | 853 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1427;symbol_collisions=853 |
| IDX | 72.07 | 96.39 | 963 | 269 | 26 | 243 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=269;symbol_collisions=243 |
| NYSE MKT | 72.73 | 80.29 | 308 | 84 | 55 | 29 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=84;symbol_collisions=29 |
| BK | 72.86 | 90.27 | 140 | 38 | 11 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 |
| BATS | 78.45 | 80.94 | 1559 | 336 | 288 | 48 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=336;symbol_collisions=48 |
| NSE_IN | 78.7 | 84.19 | 3010 | 641 | 445 | 196 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 |
| SGX | 78.95 | 97.52 | 746 | 157 | 15 | 142 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=157;symbol_collisions=142 |
| NASDAQ | 79.51 | 80.39 | 5739 | 1176 | 1113 | 63 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1176;symbol_collisions=63 |
| OSL | 82.21 | 97.22 | 298 | 53 | 7 | 46 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 |
| MSX | 84.26 | 95.79 | 108 | 17 | 4 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 |
| KRX | 84.31 | 84.43 | 2097 | 329 | 326 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=329;symbol_collisions=3 |
| KOSDAQ | 86.36 | 86.36 | 1818 | 248 | 248 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=248;symbol_collisions=0 |
| TWSE | 88.93 | 94.28 | 1093 | 121 | 59 | 62 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=62 |
| TSE | 90.96 | 98.56 | 4437 | 401 | 59 | 342 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=342 |
| B3 | 91.94 | 91.94 | 1327 | 107 | 107 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 |
| HKEX | 94.42 | 96.96 | 3206 | 179 | 95 | 84 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=179;symbol_collisions=84 |
| BIST | 94.44 | 97.45 | 647 | 36 | 16 | 20 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=20 |
| QSE | 94.74 | 98.18 | 57 | 3 | 1 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=2 |
| NYSE ARCA | 95.29 | 96.44 | 2697 | 127 | 95 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=32 |
| CSE_LK | 96.54 | 96.54 | 318 | 11 | 11 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 |
| SEM | 97.87 | 97.87 | 47 | 1 | 1 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 61 | 61 | 61 | 2 | 58 | 0 |
| Australia | 1585 | 1479 | 1585 | 79 | 1342 | 143 |
| Austria | 67 | 67 | 67 | 1 | 59 | 62 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 29 | 29 | 29 | 0 | 27 | 7 |
| Belgium | 127 | 126 | 127 | 6 | 122 | 118 |
| Bermuda | 530 | 530 | 530 | 58 | 489 | 130 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1597 | 1587 | 1596 | 12 | 1261 | 2 |
| British Virgin Islands | 153 | 153 | 153 | 95 | 99 | 28 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4641 | 4424 | 4636 | 528 | 3959 | 50 |
| Cayman Islands | 2218 | 2208 | 2217 | 570 | 1882 | 246 |
| Chile | 116 | 89 | 116 | 3 | 84 | 2 |
| China | 6361 | 6308 | 6361 | 4 | 5248 | 9 |
| Colombia | 4 | 4 | 4 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 18 | 17 | 18 | 1 | 12 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 154 | 154 | 154 | 6 | 149 | 139 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 3 |
| Finland | 197 | 197 | 197 | 2 | 196 | 2 |
| France | 746 | 743 | 746 | 16 | 729 | 720 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 794 | 789 | 794 | 8 | 779 | 710 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 141 | 141 | 141 | 1 | 122 | 118 |
| Guernsey | 68 | 68 | 68 | 4 | 60 | 61 |
| Hong Kong | 464 | 461 | 464 | 0 | 457 | 4 |
| Hungary | 37 | 36 | 37 | 0 | 29 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5022 | 5022 | 5022 | 0 | 4995 | 4 |
| Indonesia | 703 | 699 | 703 | 1 | 582 | 0 |
| Ireland | 2571 | 2561 | 2571 | 37 | 2542 | 911 |
| Isle of Man | 14 | 14 | 14 | 1 | 13 | 12 |
| Israel | 754 | 752 | 754 | 88 | 730 | 3 |
| Italy | 235 | 233 | 234 | 1 | 225 | 220 |
| Japan | 3383 | 3377 | 3383 | 30 | 3367 | 485 |
| Jersey | 170 | 170 | 170 | 16 | 165 | 164 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 2 |
| Luxembourg | 1014 | 1011 | 1014 | 14 | 1007 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 943 | 943 | 943 | 0 | 935 | 2 |
| Malta | 6 | 6 | 6 | 0 | 6 | 6 |
| Marshall Islands | 40 | 40 | 40 | 34 | 26 | 20 |
| Mauritius | 60 | 60 | 60 | 2 | 55 | 0 |
| Mexico | 137 | 119 | 137 | 7 | 114 | 2 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 66 | 0 | 62 | 0 |
| Netherlands | 200 | 200 | 200 | 30 | 193 | 146 |
| New Zealand | 81 | 81 | 81 | 2 | 75 | 0 |
| Nigeria | 147 | 147 | 147 | 0 | 135 | 78 |
| Norway | 245 | 244 | 245 | 5 | 234 | 235 |
| Oman | 90 | 90 | 90 | 0 | 0 | 0 |
| Pakistan | 371 | 366 | 371 | 3 | 263 | 0 |
| Panama | 5 | 4 | 5 | 3 | 4 | 0 |
| Papua New Guinea | 3 | 3 | 3 | 0 | 3 | 0 |
| Peru | 32 | 32 | 32 | 1 | 29 | 0 |
| Philippines | 102 | 102 | 102 | 2 | 101 | 23 |
| Poland | 370 | 369 | 370 | 8 | 366 | 363 |
| Portugal | 39 | 39 | 39 | 0 | 39 | 37 |
| Puerto Rico | 6 | 6 | 6 | 5 | 6 | 4 |
| Qatar | 54 | 54 | 54 | 0 | 0 | 0 |
| Romania | 80 | 80 | 80 | 0 | 80 | 77 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 191 | 0 | 191 | 0 |
| Singapore | 548 | 544 | 548 | 15 | 49 | 3 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 1 |
| South Africa | 230 | 230 | 230 | 7 | 179 | 145 |
| South Korea | 3366 | 3364 | 3366 | 1 | 3362 | 0 |
| Spain | 246 | 246 | 246 | 7 | 242 | 239 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 806 | 801 | 805 | 5 | 790 | 787 |
| Switzerland | 401 | 401 | 401 | 25 | 393 | 340 |
| Taiwan | 2275 | 2274 | 2275 | 1 | 2056 | 1 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 550 | 550 | 550 | 7 | 334 | 0 |
| Turkey | 620 | 620 | 620 | 0 | 620 | 554 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1349 | 1345 | 1348 | 57 | 1296 | 1106 |
| United States | 14197 | 13210 | 14031 | 5223 | 10813 | 3844 |
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
| Active exchange-directory rows | 1327 |
| Matched dataset rows | 1220 |
| Missing dataset rows | 361 |
| Dataset match rate | 77.17 |
| Any official B3 source matched dataset rows | 1250 |
| Any official B3 source missing dataset rows | 331 |
| Any official B3 source match rate | 79.06 |
| Official active symbols not in dataset | 107 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 16 |
| local_share_line | 269 |
| other | 16 |
| unit_or_fund_line | 60 |

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
| B3::BAOA39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES CORE 80/20 AGGRESSIVE ALLOCATION ETF |
| B3::BBCN39 | bdr_or_foreign_receipt | ETF | absent_from_all_b3_masterfile_sources | JPMORGAN BETABUILDERS CANADA ETF |
| B3::BBIL39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | JPMORGAN BETABUILDERS INTERNATIONAL EQUITY ETF |
| B3::BFIW39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | FIRST TRUST WATER ETF |
| B3::BGAR39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES MSCI USA QUALITY GARP ETF |
| B3::CPTS11B | other | ETF | absent_from_all_b3_masterfile_sources | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DNEN3B | other | Stock | absent_from_all_b3_masterfile_sources | DINAMICA ENERGIA S.A. |
| B3::EQMA5B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::IVLG3B | other | Stock | absent_from_all_b3_masterfile_sources | INVITEL LEGACY S.A. |
