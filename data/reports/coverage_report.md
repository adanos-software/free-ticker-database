# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63241 |
| core_listings | 57731 |
| aliases | 125094 |
| stocks | 47504 |
| etfs | 15737 |
| isin_coverage | 61763 |
| sector_coverage | 63121 |
| stock_sector_coverage | 47479 |
| etf_category_coverage | 15642 |
| cik_coverage | 7782 |
| figi_coverage | 65733 |
| lei_coverage | 17943 |
| listing_status_rows | 100646 |
| listing_status_intervals | 100646 |
| listing_events | 45535 |
| listing_keys | 74711 |
| instrument_scope_rows | 74711 |
| instrument_scope_core | 57731 |
| instrument_scope_extended | 16980 |
| instrument_scope_primary_listing | 56973 |
| instrument_scope_primary_listing_missing_isin | 758 |
| instrument_scope_otc_listing | 11076 |
| instrument_scope_secondary_cross_listing | 5904 |
| legacy_primary_ticker_collision_rows | 2022 |
| official_masterfile_symbols | 79821 |
| official_masterfile_matches | 52769 |
| official_masterfile_collisions | 11539 |
| official_masterfile_missing | 15513 |
| official_recall_denominator | 79821 |
| official_recall_matches | 52769 |
| official_recall_missing | 27052 |
| official_recall_pct | 66.11 |
| collision_adjusted_recall_denominator | 68282 |
| collision_adjusted_recall_missing | 15513 |
| collision_adjusted_recall_pct | 77.28 |
| collision_adjusted_recall_gap_rate | 22.72 |
| official_full_recall_target_exchanges | 48 |
| official_full_recall_passing_exchanges | 3 |
| official_full_recall_exception_exchanges | 45 |
| collision_adjusted_full_recall_passing_exchanges | 4 |
| collision_adjusted_full_recall_exception_exchanges | 44 |
| official_recall_decision_counts | {'fixed': 3, 'mostly_collision_hidden': 9, 'out_of_current_scope': 33, 'still_actionable': 36} |
| official_recall_exception_decision_counts | {'mostly_collision_hidden': 9, 'still_actionable': 36} |
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
| tickers_built_at | 2026-07-22T09:22:34Z |
| tickers_age_hours | 0.01 |
| masterfiles_generated_at | 2026-07-22T09:12:41Z |
| masterfiles_age_hours | 0.17 |
| identifiers_generated_at | 2026-07-22T09:22:35Z |
| identifiers_age_hours | 0.01 |
| listing_history_observed_at | 2026-07-07T07:32:52Z |
| listing_history_age_hours | 361.84 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 1896.96 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 1896.96 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 1896.96 |
| symbol_changes_generated_at | 2026-07-21T08:42:24Z |
| symbol_changes_age_hours | 24.68 |
| symbol_changes_review_rows | 304 |
| entry_quality_generated_at | 2026-07-22T09:14:11Z |
| entry_quality_age_hours | 0.15 |
| entry_quality_rows | 74711 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 1190.08 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-06-02T20:39:44Z |
| ohlcv_plausibility_age_hours | 1188.72 |
| ohlcv_plausibility_rows | 240 |
| source_gap_classification_generated_at | 2026-07-22T09:14:14Z |
| source_gap_classification_age_hours | 0.15 |
| source_gap_classification_rows | 7115 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-07-22T09:22:34Z | 0.01 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-07-22T09:12:41Z | 0.17 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-07-22T09:22:35Z | 0.01 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-07-07T07:32:52Z | 361.84 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 1896.96 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 1896.96 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-07-21T08:42:24Z | 24.68 | 304 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-07-22T09:14:11Z | 0.15 | 74711 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-07-22T09:14:14Z | 0.15 | 7115 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 1190.08 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-06-02T20:39:44Z | 1188.72 | 240 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 35, "old": 84, "stale": 18} |
| source_age_bucket_totals | {"age_0_48h": 35, "age_168_336h": 1, "age_48_168h": 18, "age_over_336h": 83} |
| refresh_priority_totals | {"P1": 19, "P2": 83, "P4": 35} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 35, "refresh_official_exchange_directory_before_identity_or_collision_work": 17, "refresh_official_subset_before_gap_enrichment": 79, "restore_or_replace_unavailable_source_before_data_fill": 6} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 10 | 23781 | 1189.73 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 7 | 17992 | 1189.73 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 5179 | 1189.73 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 35 | 29699 | 1189.73 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 33 | 16568 | 1189.73 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 5 | 1333 | 71.48 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 4 | 2411 | 1189.73 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 745 | 1189.73 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 2 | 71 | 1189.73 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 1189.73 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5562 | 2026-07-22T09:12:41Z | 0.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7490 | 2026-07-22T09:12:41Z | 0.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 36 | 2026-07-07T09:34:57Z | 359.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11021 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_listed_companies | ASX | listed_companies_subset | network | 1987 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_investment_products | ASX | listed_companies_subset | network | 458 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 378 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3704 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1770 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 268 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3857 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4064 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_listed_issues | JPX | exchange_directory | network | 4437 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 4033 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 468 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3647 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5076 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8707 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 830 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | network | 431 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 205 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 314 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | network | 140 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 123 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 97 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 118 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1127 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | network | 26 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 20 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 81 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 317 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 63 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | cache | 8 | 2026-07-07T09:07:40Z | 360.26 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 66 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3010 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | unavailable | 5068 | 2026-07-13T12:00:16Z | 213.38 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hkex_securities_list | HKEX | exchange_directory | network | 3208 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sgx_securities_prices | SGX | exchange_directory | cache | 738 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 641 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | cache | 412 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| adx_market_watch | ADX | exchange_directory | network | 123 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| qse_market_watch | QSE | exchange_directory | cache | 57 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 111 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | cache | 47 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | cache | 7 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | cache | 173 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | cache | 27 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 350 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 9 | 2026-07-20T09:31:47Z | 47.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | cache | 133 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 10 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 5 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 2 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 9 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 0 | 2026-07-19T09:54:21Z | 71.48 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 746 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 191 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | cache | 32 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | cache | 134 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | cache | 53 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | cache | 30 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 143 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1090 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| twse_etf_list | TWSE | listed_companies_subset | network | 220 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2356 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 881 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2893 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 662 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 891 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 113 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | cache | 887 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | cache | 349 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2759 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1150 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 563 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 367 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 716 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | cache | 63 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 963 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| idx_company_profiles | IDX | exchange_directory | network | 963 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_listed_companies | GPW | listed_companies_subset | cache | 400 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | cache | 364 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | cache | 27 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 524 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 463 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 15 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 16 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 403 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-07-21T08:41:58Z | 24.69 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| upcom_registered_securities | HNX | exchange_directory | network | 830 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | cache | 22 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | cache | 74 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 745 | 2026-06-02T19:38:59Z | 1189.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 2026-06-02T19:38:59Z | 1189.73 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 19 |
| P2 | 83 |
| P4 | 35 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 17 |
| refresh_official_subset_before_gap_enrichment | 79 |
| restore_or_replace_unavailable_source_before_data_fill | 6 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 17 |
| fresh_no_refresh_needed | listed_companies_subset | 15 |
| fresh_no_refresh_needed | security_identifier_registry_subset | 1 |
| fresh_no_refresh_needed | security_lookup_subset | 2 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 17 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | exchange_directory | 5 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 68 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 4 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 2 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 4 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 10 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 7 |
| refresh_official_subset_before_gap_enrichment | cache | 35 |
| refresh_official_subset_before_gap_enrichment | network | 44 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 6 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 17 |
| refresh_official_subset_before_gap_enrichment | P2 | 79 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 2 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 4 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 35 |
| age_168_336h | 1 |
| age_48_168h | 18 |
| age_over_336h | 83 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_over_336h | 17 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 18 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 61 |
| restore_or_replace_unavailable_source_before_data_fill | age_168_336h | 1 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 5 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 17 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 79 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 6 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 35 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 17 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 79 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 6 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 10 | 23781 | 1189.73 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 7 | 17992 | 1189.73 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 5179 | 1189.73 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 35 | 29699 | 1189.73 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 33 | 16568 | 1189.73 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 5 | 1333 | 71.48 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 4 | 2411 | 1189.73 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 2 | 745 | 1189.73 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 2 | 71 | 1189.73 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 268 | 1189.73 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 36 | 359.8 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 17 | 36507 | 47.85 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 15 | 10094 | 47.85 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_lookup_subset | network | P4 | 2 | 94 | 47.85 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope security_lookup_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | security_identifier_registry_subset | network | P4 | 1 | 4033 | 24.69 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope security_identifier_registry_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 123 | 85 | 32 | 6 | 69.11 | 30.89 | 93.41 | 6 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 | 100.0 |
| AMS | official_full | 331 | 331 | 265 | 0 | 322 | 153 | 601 | 241 | 296 | 64 | 40.1 | 59.9 | 79.02 | 64 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=360;symbol_collisions=296 | 100.0 |
| ASX | official_partial | 1625 | 1526 | 1622 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 155 | 155 | 155 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1579 | 0 | 1252 | 0 | 431 | 400 | 0 | 31 | 92.81 | 7.19 | 92.81 | 31 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=31;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1297 | 1223 | 1227 | 0 | 1048 | 243 | 1545 | 1209 | 48 | 288 | 78.25 | 21.75 | 80.76 | 288 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=336;symbol_collisions=48 | 100.0 |
| BCBA | official_partial | 63 | 63 | 63 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 29 | 29 | 29 | 0 | 27 | 7 | 41 | 29 | 9 | 3 | 70.73 | 29.27 | 90.62 | 3 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 641 | 612 | 20 | 9 | 95.48 | 4.52 | 98.55 | 9 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=29;symbol_collisions=20 | 100.0 |
| BK | official_full | 104 | 104 | 103 | 0 | 104 | 0 | 140 | 102 | 27 | 11 | 72.86 | 27.14 | 90.27 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 | 100.0 |
| BME | official_full | 221 | 221 | 221 | 3 | 221 | 213 | 97 | 24 | 12 | 61 | 24.74 | 75.26 | 28.24 | 61 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=73;symbol_collisions=12 | 100.0 |
| BMV | official_partial | 179 | 162 | 178 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 47 | 0 | 42 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2637 | 2637 | 2636 | 0 | 2616 | 0 | 5068 | 2580 | 1896 | 592 | 50.91 | 49.09 | 81.34 | 592 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2488;symbol_collisions=1896 | 100.0 |
| BVB | official_full | 80 | 80 | 80 | 0 | 80 | 76 | 350 | 75 | 126 | 149 | 21.43 | 78.57 | 33.48 | 149 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=126 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 277 | 275 | 2893 | 253 | 1805 | 835 | 8.75 | 91.25 | 23.25 | 835 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2640;symbol_collisions=1805 |  |
| Bursa | official_partial | 936 | 936 | 936 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 145 | 145 | 145 | 0 | 145 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 317 | 307 | 0 | 10 | 96.85 | 3.15 | 96.85 | 10 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 81 | 1 | 61 | 19 | 1.23 | 98.77 | 5.0 | 19 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=61 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 45 | 16 | 10 | 63.38 | 36.62 | 81.82 | 10 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=16 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 15 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 222 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1081 | 1081 | 994 | 7 | 1071 | 844 | 2007 | 963 | 672 | 372 | 47.98 | 52.02 | 72.13 | 372 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1044;symbol_collisions=672 | 100.0 |
| GSE | official_partial | 19 | 18 | 18 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 194 | 194 | 194 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3044 | 3044 | 3013 | 0 | 3015 | 268 | 3208 | 3030 | 84 | 94 | 94.45 | 5.55 | 96.99 | 94 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=178;symbol_collisions=84 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 299 | 104 | 156 | 39 | 34.78 | 65.22 | 72.73 | 39 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 694 | 694 | 694 | 0 | 577 | 0 | 963 | 694 | 244 | 25 | 72.07 | 27.93 | 96.52 | 25 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=269;symbol_collisions=244 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 15 | 9 | 6 | 0 | 60.0 | 40.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1578 | 1578 | 1578 | 0 | 1578 | 0 | 1818 | 1571 | 0 | 247 | 86.41 | 13.59 | 86.41 | 247 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=247;symbol_collisions=0 | 99.62 |
| KRX | official_full | 1796 | 1795 | 1796 | 0 | 1793 | 0 | 2091 | 1768 | 3 | 320 | 84.55 | 15.45 | 84.67 | 320 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=323;symbol_collisions=3 | 99.76 |
| LSE | official_full | 6558 | 6557 | 6267 | 16 | 6532 | 4364 | 11021 | 6419 | 1108 | 3494 | 58.24 | 41.76 | 64.75 | 3494 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4602;symbol_collisions=1108 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 13 | 4 | 84.26 | 15.74 | 95.79 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 | 100.0 |
| NASDAQ | official_full | 4719 | 4595 | 4671 | 3513 | 3442 | 1399 | 5711 | 4631 | 56 | 1024 | 81.09 | 18.91 | 81.89 | 1024 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1080;symbol_collisions=56 | 99.56 |
| NEO | official_full | 197 | 154 | 191 | 0 | 149 | 1 | 440 | 183 | 86 | 171 | 41.59 | 58.41 | 51.69 | 171 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 | 100.0 |
| NGX | official_full | 145 | 145 | 144 | 0 | 133 | 76 | 133 | 133 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2499 | 0 | 3010 | 2369 | 196 | 445 | 78.7 | 21.3 | 84.19 | 445 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 45 | 0 | 42 | 1 | 66 | 11 | 23 | 32 | 16.67 | 83.33 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 | 100.0 |
| NYSE | official_full | 2046 | 1992 | 2039 | 1969 | 1485 | 1031 | 3909 | 2034 | 500 | 1375 | 52.03 | 47.97 | 59.67 | 1375 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1875;symbol_collisions=500 | 100.0 |
| NYSE ARCA | official_full | 2682 | 2607 | 2632 | 116 | 2103 | 371 | 2692 | 2570 | 30 | 92 | 95.47 | 4.53 | 96.54 | 92 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=122;symbol_collisions=30 | 100.0 |
| NYSE MKT | official_full | 230 | 221 | 230 | 147 | 153 | 51 | 308 | 225 | 28 | 55 | 73.05 | 26.95 | 80.36 | 55 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=28 | 100.0 |
| NZX | official_full | 45 | 45 | 42 | 0 | 45 | 1 | 173 | 45 | 126 | 2 | 26.01 | 73.99 | 95.74 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 | 100.0 |
| OSL | official_full | 265 | 265 | 261 | 2 | 258 | 243 | 298 | 245 | 46 | 7 | 82.21 | 17.79 | 97.22 | 7 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 | 100.0 |
| OTC | official_full | 11076 | 10317 | 10840 | 1916 | 8937 | 2898 | 11925 | 7675 | 38 | 4212 | 64.36 | 35.64 | 64.57 | 4212 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4250;symbol_collisions=38 | 88.97 |
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
| TSXV | official_full | 1069 | 989 | 1067 | 17 | 919 | 9 | 1600 | 1044 | 544 | 12 | 65.25 | 34.75 | 98.86 | 12 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=556;symbol_collisions=544 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1191 | 0 | 1165 | 3 | 1090 | 973 | 59 | 58 | 89.27 | 10.73 | 94.37 | 58 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=117;symbol_collisions=59 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 830 | 2 | 476 | 352 | 0.24 | 99.76 | 0.56 | 352 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=828;symbol_collisions=476 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 56 | 56 | 56 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 542 | 542 | 541 | 7 | 541 | 522 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XETRA | official_full | 3845 | 3844 | 3233 | 8 | 3828 | 1925 | 5076 | 3657 | 858 | 561 | 72.04 | 27.96 | 86.7 | 561 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1419;symbol_collisions=858 | 99.88 |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.56 | 830 | 828 | 352 | 476 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=828;symbol_collisions=476 |
| CSE_MA | 1.23 | 5.0 | 81 | 80 | 19 | 61 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=61 |
| Borsa Italiana | 8.75 | 23.25 | 2893 | 2640 | 835 | 1805 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2640;symbol_collisions=1805 |
| NSE_KE | 16.67 | 25.58 | 66 | 55 | 32 | 23 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 |
| BVB | 21.43 | 33.48 | 350 | 275 | 149 | 126 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=126 |
| PSE | 23.62 | 45.23 | 381 | 291 | 109 | 182 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=291;symbol_collisions=182 |
| BME | 24.74 | 28.24 | 97 | 73 | 61 | 12 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=73;symbol_collisions=12 |
| NZX | 26.01 | 95.74 | 173 | 128 | 2 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 |
| HNX | 34.78 | 72.73 | 299 | 195 | 39 | 156 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 |
| AMS | 40.1 | 79.02 | 601 | 360 | 64 | 296 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=360;symbol_collisions=296 |
| TSX | 41.37 | 97.6 | 788 | 462 | 8 | 454 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=462;symbol_collisions=454 |
| NEO | 41.59 | 51.69 | 440 | 257 | 171 | 86 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 |
| TADAWUL | 46.36 | 97.95 | 412 | 221 | 4 | 217 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=217 |
| Euronext | 47.98 | 72.13 | 2007 | 1044 | 372 | 672 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1044;symbol_collisions=672 |
| BSE_IN | 50.91 | 81.34 | 5068 | 2488 | 592 | 1896 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2488;symbol_collisions=1896 |
| PSX | 51.82 | 66.13 | 716 | 345 | 190 | 155 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=155 |
| NYSE | 52.03 | 59.67 | 3909 | 1875 | 1375 | 500 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1875;symbol_collisions=500 |
| SET | 57.73 | 91.29 | 944 | 399 | 52 | 347 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=399;symbol_collisions=347 |
| LSE | 58.24 | 64.75 | 11021 | 4602 | 3494 | 1108 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4602;symbol_collisions=1108 |
| ISE | 60.0 | 100.0 | 15 | 6 | 0 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 |
| DFM | 63.38 | 81.82 | 71 | 26 | 10 | 16 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=16 |
| OTC | 64.36 | 64.57 | 11925 | 4250 | 4212 | 38 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4250;symbol_collisions=38 |
| TSXV | 65.25 | 98.86 | 1600 | 556 | 12 | 544 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=556;symbol_collisions=544 |
| ADX | 69.11 | 93.41 | 123 | 38 | 6 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 |
| BHB | 70.73 | 90.62 | 41 | 12 | 3 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=9 |
| XETRA | 72.04 | 86.7 | 5076 | 1419 | 561 | 858 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1419;symbol_collisions=858 |
| IDX | 72.07 | 96.52 | 963 | 269 | 25 | 244 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=269;symbol_collisions=244 |
| BK | 72.86 | 90.27 | 140 | 38 | 11 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 |
| NYSE MKT | 73.05 | 80.36 | 308 | 83 | 55 | 28 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=83;symbol_collisions=28 |
| BATS | 78.25 | 80.76 | 1545 | 336 | 288 | 48 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=336;symbol_collisions=48 |
| NSE_IN | 78.7 | 84.19 | 3010 | 641 | 445 | 196 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=196 |
| SGX | 79.81 | 98.49 | 738 | 149 | 9 | 140 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=149;symbol_collisions=140 |
| NASDAQ | 81.09 | 81.89 | 5711 | 1080 | 1024 | 56 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1080;symbol_collisions=56 |
| OSL | 82.21 | 97.22 | 298 | 53 | 7 | 46 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=53;symbol_collisions=46 |
| MSX | 84.26 | 95.79 | 108 | 17 | 4 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=13 |
| KRX | 84.55 | 84.67 | 2091 | 323 | 320 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=323;symbol_collisions=3 |
| KOSDAQ | 86.41 | 86.41 | 1818 | 247 | 247 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=247;symbol_collisions=0 |
| TWSE | 89.27 | 94.37 | 1090 | 117 | 58 | 59 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=117;symbol_collisions=59 |
| TSE | 90.96 | 98.56 | 4437 | 401 | 59 | 342 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=401;symbol_collisions=342 |
| B3 | 92.81 | 92.81 | 431 | 31 | 31 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=31;symbol_collisions=0 |
| HKEX | 94.45 | 96.99 | 3208 | 178 | 94 | 84 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=178;symbol_collisions=84 |
| QSE | 94.74 | 98.18 | 57 | 3 | 1 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=2 |
| NYSE ARCA | 95.47 | 96.54 | 2692 | 122 | 92 | 30 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=122;symbol_collisions=30 |
| BIST | 95.48 | 98.55 | 641 | 29 | 9 | 20 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=29;symbol_collisions=20 |
| CSE_LK | 96.85 | 96.85 | 317 | 10 | 10 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 61 | 61 | 61 | 2 | 58 | 0 |
| Australia | 1587 | 1481 | 1587 | 79 | 1346 | 143 |
| Austria | 69 | 69 | 69 | 1 | 61 | 63 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 30 | 30 | 30 | 0 | 28 | 7 |
| Belgium | 129 | 128 | 129 | 5 | 125 | 119 |
| Bermuda | 532 | 532 | 532 | 59 | 491 | 131 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1596 | 1585 | 1595 | 11 | 1260 | 1 |
| British Virgin Islands | 154 | 154 | 154 | 96 | 100 | 29 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4675 | 4442 | 4675 | 532 | 3971 | 50 |
| Cayman Islands | 2217 | 2207 | 2217 | 570 | 1885 | 245 |
| Chile | 114 | 87 | 114 | 1 | 82 | 0 |
| China | 6365 | 6312 | 6365 | 5 | 5251 | 11 |
| Colombia | 4 | 4 | 4 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 18 | 17 | 18 | 1 | 12 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 154 | 154 | 154 | 6 | 149 | 139 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 3 |
| Finland | 197 | 197 | 197 | 2 | 196 | 2 |
| France | 751 | 748 | 751 | 16 | 732 | 723 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 806 | 801 | 806 | 11 | 790 | 719 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 142 | 142 | 142 | 1 | 123 | 118 |
| Guernsey | 68 | 68 | 68 | 4 | 61 | 62 |
| Hong Kong | 464 | 461 | 464 | 0 | 458 | 5 |
| Hungary | 37 | 36 | 37 | 0 | 29 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5022 | 5022 | 5022 | 0 | 4998 | 4 |
| Indonesia | 704 | 700 | 704 | 2 | 583 | 0 |
| Ireland | 2571 | 2561 | 2571 | 37 | 2543 | 912 |
| Isle of Man | 14 | 14 | 14 | 1 | 13 | 12 |
| Israel | 755 | 752 | 755 | 89 | 730 | 3 |
| Italy | 233 | 231 | 233 | 2 | 224 | 218 |
| Japan | 3394 | 3388 | 3394 | 29 | 3378 | 491 |
| Jersey | 171 | 171 | 171 | 17 | 167 | 165 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 2 |
| Luxembourg | 1014 | 1011 | 1014 | 14 | 1007 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 943 | 943 | 943 | 0 | 935 | 2 |
| Malta | 6 | 6 | 6 | 0 | 6 | 6 |
| Marshall Islands | 42 | 42 | 42 | 36 | 28 | 22 |
| Mauritius | 61 | 61 | 61 | 2 | 56 | 0 |
| Mexico | 139 | 121 | 139 | 9 | 115 | 2 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 66 | 0 | 62 | 0 |
| Netherlands | 203 | 203 | 203 | 31 | 197 | 149 |
| New Zealand | 81 | 81 | 81 | 2 | 75 | 0 |
| Nigeria | 147 | 147 | 147 | 0 | 135 | 78 |
| Norway | 249 | 248 | 249 | 5 | 239 | 238 |
| Oman | 90 | 90 | 90 | 0 | 0 | 0 |
| Pakistan | 371 | 366 | 371 | 3 | 263 | 0 |
| Panama | 5 | 4 | 5 | 3 | 4 | 0 |
| Papua New Guinea | 3 | 3 | 3 | 0 | 3 | 0 |
| Peru | 32 | 32 | 32 | 1 | 29 | 0 |
| Philippines | 103 | 103 | 103 | 2 | 102 | 24 |
| Poland | 368 | 367 | 368 | 8 | 365 | 362 |
| Portugal | 39 | 39 | 39 | 0 | 39 | 37 |
| Puerto Rico | 6 | 6 | 6 | 5 | 6 | 4 |
| Qatar | 54 | 54 | 54 | 0 | 0 | 0 |
| Romania | 80 | 80 | 80 | 0 | 80 | 77 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 191 | 0 | 191 | 0 |
| Singapore | 550 | 545 | 550 | 16 | 50 | 4 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 1 |
| South Africa | 230 | 230 | 230 | 6 | 180 | 146 |
| South Korea | 3366 | 3364 | 3366 | 1 | 3362 | 0 |
| Spain | 249 | 249 | 249 | 6 | 247 | 242 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 812 | 807 | 812 | 7 | 799 | 793 |
| Switzerland | 404 | 404 | 404 | 24 | 396 | 342 |
| Taiwan | 2275 | 2274 | 2275 | 1 | 2056 | 1 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 551 | 551 | 551 | 7 | 335 | 1 |
| Turkey | 620 | 620 | 620 | 0 | 620 | 554 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1350 | 1346 | 1350 | 56 | 1305 | 1111 |
| United States | 14218 | 13268 | 14099 | 5277 | 10855 | 3876 |
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
| Active exchange-directory rows | 431 |
| Matched dataset rows | 400 |
| Missing dataset rows | 1181 |
| Dataset match rate | 25.3 |
| Any official B3 source matched dataset rows | 672 |
| Any official B3 source missing dataset rows | 909 |
| Any official B3 source match rate | 42.5 |
| Official active symbols not in dataset | 31 |

### B3 Missing Categories

| Category | Rows |
|---|---:|
| bdr_or_foreign_receipt | 158 |
| local_share_line | 570 |
| other | 22 |
| unit_or_fund_line | 431 |

### B3 Missing Examples

| Listing key | Category | Asset Type | Source Presence | Name |
|---|---|---|---|---|
| B3::2WAV3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | 2W ECOBANK S.A. |
| B3::A6OP3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ACESSOPAR INVESTIMENTOS E PARTICIPAÇÕES S.A. |
| B3::AALR12 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR13 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AALR3 | local_share_line | Stock | absent_from_all_b3_masterfile_sources | ALLIANÇA SAÚDE E PARTICIPAÇÕES S.A. |
| B3::AAGR11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | ASSET BANK AGRONEGÓCIOS FIAGRO -DC |
| B3::ABCP11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | GRAND PLAZA SHOPPING FDO INV IMOB - RESP LIM |
| B3::ACWI11 | unit_or_fund_line | ETF | present_only_in_non_exchange_directory_source | TREND ETF BLOOMBERG ALL COUNTRIES FUNDO ÍNDICE |
| B3::ADSH11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | AD SHOPPING FUNDO DE INVESTIMENTO IMOB RESP LIM |
| B3::AERO11 | unit_or_fund_line | ETF | absent_from_all_b3_masterfile_sources | HCO OPPS AERO I FDO DE INV IMOB RESP LIM |
| B3::AETH39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | 21SHARES ETHEREUM STAKING ETP |
| B3::ARGT39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | GLOBAL X MSCI ARGENTINA ETF |
| B3::BAER39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | Ishares Us Aerospace & Defense ETF |
| B3::BAGG39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | ISHARES CORE U.S. AGGREGATE BOND ETF |
| B3::BAIQ39 | bdr_or_foreign_receipt | ETF | present_only_in_non_exchange_directory_source | GLOBAL X ARTIFICIAL INTELLIGENCE & TECHNOLOGY ETF |
| B3::CPTS11B | other | ETF | absent_from_all_b3_masterfile_sources | Capitania Securities II Fundo Investimento Imobiliario FII |
| B3::DNEN3B | other | Stock | absent_from_all_b3_masterfile_sources | DINAMICA ENERGIA S.A. |
| B3::EQMA3B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA5B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
| B3::EQMA6B | other | Stock | absent_from_all_b3_masterfile_sources | EQUATORIAL MARANHÃO DISTRIBUIDORA DE ENERGIA S.A. |
