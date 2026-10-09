# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 65543 |
| core_listings | 63459 |
| aliases | 128614 |
| stocks | 48752 |
| etfs | 16791 |
| isin_coverage | 64216 |
| sector_coverage | 64349 |
| stock_sector_coverage | 47679 |
| etf_category_coverage | 16670 |
| cik_coverage | 7864 |
| figi_coverage | 65153 |
| lei_coverage | 17676 |
| listing_status_rows | 113511 |
| listing_status_intervals | 113511 |
| listing_events | 95588 |
| listing_keys | 97347 |
| instrument_scope_rows | 97347 |
| instrument_scope_core | 63459 |
| instrument_scope_extended | 33888 |
| instrument_scope_primary_listing | 62794 |
| instrument_scope_primary_listing_missing_isin | 665 |
| instrument_scope_otc_listing | 11752 |
| instrument_scope_secondary_cross_listing | 22136 |
| legacy_primary_ticker_collision_rows | 4825 |
| official_masterfile_symbols | 99879 |
| official_masterfile_matches | 70095 |
| official_masterfile_collisions | 11465 |
| official_masterfile_missing | 18319 |
| official_recall_denominator | 99879 |
| official_recall_matches | 70095 |
| official_recall_missing | 29784 |
| official_recall_pct | 70.18 |
| collision_adjusted_recall_denominator | 88414 |
| collision_adjusted_recall_missing | 18319 |
| collision_adjusted_recall_pct | 79.28 |
| collision_adjusted_recall_gap_rate | 20.72 |
| official_full_recall_target_exchanges | 50 |
| official_full_recall_passing_exchanges | 3 |
| official_full_recall_exception_exchanges | 47 |
| collision_adjusted_full_recall_passing_exchanges | 21 |
| collision_adjusted_full_recall_exception_exchanges | 29 |
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
| tickers_built_at | 2026-10-09T08:07:14Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-10-08T15:08:41Z |
| masterfiles_age_hours | 16.98 |
| identifiers_generated_at | 2026-10-09T08:07:17Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-10-09T08:07:14Z |
| listing_history_age_hours | 0.0 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 3791.7 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 3791.7 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 3791.69 |
| symbol_changes_generated_at | 2026-10-07T13:18:58Z |
| symbol_changes_age_hours | 42.81 |
| symbol_changes_review_rows | 360 |
| entry_quality_generated_at | 2026-10-09T07:14:02Z |
| entry_quality_age_hours | 0.89 |
| entry_quality_rows | 97347 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 3084.82 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 1647.22 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-10-09T07:14:06Z |
| source_gap_classification_age_hours | 0.89 |
| source_gap_classification_rows | 8452 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-10-09T08:07:14Z | 0.0 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-10-08T15:08:41Z | 16.98 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-10-09T08:07:17Z | 0.0 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-10-09T08:07:14Z | 0.0 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 3791.7 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 3791.69 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-10-07T13:18:58Z | 42.81 | 360 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-10-09T07:14:02Z | 0.89 | 97347 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-10-09T07:14:06Z | 0.89 | 8452 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 3084.82 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 1647.22 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 23, "old": 99, "stale": 16} |
| source_age_bucket_totals | {"age_0_48h": 23, "age_168_336h": 18, "age_48_168h": 16, "age_over_336h": 81} |
| refresh_priority_totals | {"P1": 19, "P2": 96, "P4": 23} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 23, "refresh_official_exchange_directory_before_identity_or_collision_work": 10, "refresh_official_subset_before_gap_enrichment": 77, "restore_or_replace_unavailable_source_before_data_fill": 28} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 10 | 28295 | 235.44 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 9 | 24763 | 3084.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 69 | 41010 | 1177.1 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 17 | 18585 | 3084.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 5 | 1012 | 889.55 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4040 | 1215.87 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 266 | 164.69 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 3084.47 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 9 | 235.44 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 3084.47 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5630 | 2026-10-08T13:57:11Z | 18.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7663 | 2026-10-08T13:57:11Z | 18.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 9 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11184 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 1966.22 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 443 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| asx_investment_products | ASX | listed_companies_subset | network | 468 | 2026-09-21T12:40:24Z | 427.45 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 930 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 942 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 512 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3619 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1826 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3840 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4096 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jpx_listed_issues | JPX | exchange_directory | unavailable | 4444 | 2026-08-25T08:23:53Z | 1079.73 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | unavailable | 4040 | 2026-08-19T16:15:20Z | 1215.87 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 462 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3690 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5129 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| deutsche_boerse_frankfurt_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 18226 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | unavailable | 1 | 2026-08-19T14:30:55Z | 1217.61 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8976 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 850 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | unavailable | 1353 | 2026-09-13T11:40:38Z | 620.45 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 216 | 2026-08-27T12:33:13Z | 1027.57 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 321 | 2026-08-27T12:33:13Z | 1027.57 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | network | 141 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 122 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 269 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-09-21T12:40:24Z | 427.45 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 126 | 2026-09-21T12:40:24Z | 427.45 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1142 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | network | 26 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 19 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | unavailable | 82 | 2026-07-27T10:06:05Z | 1774.02 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 315 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 319 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-08-24T09:23:58Z | 1102.72 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 92 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 8 | 2026-07-07T09:07:40Z | 2255.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 68 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3530 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bse_india_scrips | BSE India | exchange_directory | unavailable | 5165 | 2026-09-21T12:40:24Z | 427.45 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hkex_securities_list | HKEX | exchange_directory | network | 3251 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sgx_securities_prices | SGX | exchange_directory | network | 748 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 663 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 413 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| adx_market_watch | ADX | exchange_directory | network | 122 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| muscat_securities_companies | MSX | exchange_directory | network | 108 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| rse_listed_companies | RSE | listed_companies_subset | network | 1 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | network | 15 | 2026-09-28T18:10:57Z | 253.94 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 122 | 2026-08-19T15:05:31Z | 1217.03 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | network | 46 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | network | 172 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | network | 6 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 89 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 10 | 2026-09-07T14:52:25Z | 761.25 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | network | 129 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | unavailable | 130 | 2026-09-20T18:35:59Z | 445.52 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_stock_search | BMV | listed_companies_subset | unavailable | 10 | 2026-08-02T08:35:09Z | 1631.54 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | unavailable | 5 | 2026-08-02T08:35:09Z | 1631.54 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 1 | 2026-09-21T12:40:24Z | 427.45 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | unavailable | 10 | 2026-08-02T08:35:09Z | 1631.54 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | unavailable | 76 | 2026-08-19T15:05:31Z | 1217.03 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | network | 742 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | network | 194 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 125 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | unavailable | 30 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | network | 145 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | network | 35 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | network | 2 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | network | 6 | 2026-09-02T06:34:41Z | 889.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1095 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| twse_etf_list | TWSE | listed_companies_subset | network | 271 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2355 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 920 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 717 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 896 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 118 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 892 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 361 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2758 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1172 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 569 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | unavailable | 383 | 2026-08-19T14:11:11Z | 1217.94 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| psx_dps_symbols | PSX | exchange_directory | unavailable | 724 | 2026-09-20T18:35:59Z | 445.52 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| pse_listed_company_directory | PSE | exchange_directory | network | 384 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 62 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 962 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 962 | 2026-09-29T12:40:53Z | 235.44 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| wse_listed_companies | GPW | listed_companies_subset | unavailable | 403 | 2026-08-19T14:53:11Z | 1217.24 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 346 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | unavailable | 38 | 2026-08-19T14:53:11Z | 1217.24 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 533 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 467 | 2026-08-21T07:01:15Z | 1177.1 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 3084.47 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 405 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 3 | 2026-08-25T08:23:53Z | 1079.73 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-10-08T15:08:41Z | 16.98 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| upcom_registered_securities | HNX | exchange_directory | unavailable | 818 | 2026-09-08T06:43:05Z | 745.41 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 66 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 73 | 2026-10-02T11:25:55Z | 164.69 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | network | 10211 | 2026-10-08T13:57:11Z | 18.17 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 883 | 2026-09-29T12:40:53Z | 235.44 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | unavailable | 11925 | 2026-06-02T19:38:59Z | 3084.47 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 19 |
| P2 | 96 |
| P4 | 23 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 10 |
| refresh_official_subset_before_gap_enrichment | 77 |
| restore_or_replace_unavailable_source_before_data_fill | 28 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 10 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 70 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 5 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 9 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 17 |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | 1 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 1 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 10 |
| refresh_official_subset_before_gap_enrichment | cache | 1 |
| refresh_official_subset_before_gap_enrichment | network | 76 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 28 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 10 |
| refresh_official_subset_before_gap_enrichment | P2 | 77 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 9 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 19 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 23 |
| age_168_336h | 18 |
| age_48_168h | 16 |
| age_over_336h | 81 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_168_336h | 10 |
| refresh_official_subset_before_gap_enrichment | age_168_336h | 8 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 16 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 53 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 28 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 10 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 77 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 28 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 23 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 10 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 77 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 28 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 10 | 28295 | 235.44 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 9 | 24763 | 3084.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 69 | 41010 | 1177.1 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 17 | 18585 | 3084.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 5 | 1012 | 889.55 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4040 | 1215.87 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 266 | 164.69 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 3084.47 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 9 | 235.44 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 1 | 0 | 3084.47 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 23 | 52065 | 18.17 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 93 | 93 | 93 | 0 | 85 | 7 | 122 | 92 | 30 | 0 | 75.41 | 24.59 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=30;symbol_collisions=30 | 100.0 |
| AMS | official_full | 737 | 736 | 669 | 0 | 321 | 153 | 609 | 565 | 40 | 4 | 92.78 | 7.22 | 99.3 | 4 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=44;symbol_collisions=40 | 100.0 |
| ASX | official_partial | 2259 | 2161 | 2256 | 30 | 1144 | 99 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 162 | 162 | 162 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1589 | 1579 | 1585 | 0 | 1252 | 0 | 1353 | 1228 | 0 | 125 | 90.76 | 9.24 | 90.76 | 125 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=125;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1416 | 1383 | 1412 | 0 | 1028 | 236 | 1656 | 1317 | 58 | 281 | 79.53 | 20.47 | 82.42 | 281 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=339;symbol_collisions=58 | 100.0 |
| BCBA | official_partial | 92 | 92 | 69 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 31 | 31 | 30 | 0 | 26 | 7 | 41 | 31 | 9 | 1 | 75.61 | 24.39 | 96.88 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=9 | 100.0 |
| BIST | official_full | 643 | 643 | 619 | 0 | 613 | 549 | 663 | 641 | 22 | 0 | 96.68 | 3.32 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=22;symbol_collisions=22 | 100.0 |
| BK | official_full | 115 | 115 | 113 | 0 | 104 | 0 | 140 | 112 | 28 | 0 | 80.0 | 20.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=28;symbol_collisions=28 | 100.0 |
| BME | official_full | 276 | 276 | 276 | 3 | 220 | 212 | 269 | 244 | 0 | 25 | 90.71 | 9.29 | 90.71 | 25 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=0 | 100.0 |
| BMV | official_partial | 344 | 326 | 343 | 0 | 158 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 38 | 36 | 0 | 36 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 50 | 0 | 40 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 5050 | 5050 | 5033 | 0 | 2566 | 0 | 5165 | 5011 | 121 | 33 | 97.02 | 2.98 | 99.35 | 33 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=154;symbol_collisions=121 | 100.0 |
| BVB | official_full | 247 | 247 | 215 | 0 | 80 | 76 | 89 | 50 | 39 | 0 | 56.18 | 43.82 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=39;symbol_collisions=39 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 275 | 274 | 2905 | 247 | 1898 | 760 | 8.5 | 91.5 | 24.53 | 760 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2658;symbol_collisions=1898 |  |
| Bursa | official_partial | 1039 | 1039 | 1036 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 153 | 153 | 151 | 0 | 144 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 317 | 317 | 307 | 0 | 305 | 0 | 319 | 316 | 0 | 3 | 99.06 | 0.94 | 99.06 | 3 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 83 | 83 | 79 | 0 | 62 | 0 | 82 | 18 | 64 | 0 | 21.95 | 78.05 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=64;symbol_collisions=64 | 92.42 |
| DFM | official_full | 54 | 54 | 54 | 0 | 45 | 2 | 71 | 54 | 16 | 1 | 76.06 | 23.94 | 98.18 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=16 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 17 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 223 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1477 | 1477 | 1476 | 7 | 1068 | 841 | 2015 | 1334 | 369 | 312 | 66.2 | 33.8 | 81.04 | 312 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=681;symbol_collisions=369 | 100.0 |
| FSX | official_full | 8143 | 8142 | 7716 | 0 | 0 | 0 | 18226 | 7955 | 4278 | 5993 | 43.65 | 56.35 | 57.03 | 5993 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10271;symbol_collisions=4278 |  |
| GSE | official_partial | 19 | 18 | 19 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 199 | 199 | 199 | 1 | 193 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3225 | 3225 | 3214 | 0 | 2990 | 266 | 3251 | 3181 | 69 | 1 | 97.85 | 2.15 | 99.97 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=70;symbol_collisions=69 | 100.0 |
| HNX | official_full | 137 | 137 | 105 | 0 | 105 | 0 | 299 | 136 | 163 | 0 | 45.48 | 54.52 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=163;symbol_collisions=163 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 756 | 751 | 756 | 0 | 574 | 0 | 962 | 756 | 188 | 18 | 78.59 | 21.41 | 97.67 | 18 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=188 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 16 | 9 | 7 | 0 | 56.25 | 43.75 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=7 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1605 | 1605 | 1605 | 0 | 1578 | 0 | 1820 | 1587 | 3 | 230 | 87.2 | 12.8 | 87.34 | 230 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=3 | 99.62 |
| KRX | official_full | 1991 | 1990 | 1988 | 0 | 1793 | 0 | 2110 | 1949 | 14 | 147 | 92.37 | 7.63 | 92.99 | 147 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=161;symbol_collisions=14 | 99.76 |
| LSE | official_full | 7026 | 7021 | 7010 | 16 | 6478 | 4332 | 11184 | 6813 | 867 | 3504 | 60.92 | 39.08 | 66.04 | 3504 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4371;symbol_collisions=867 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 93 | 91 | 93 | 0 | 0 | 0 | 108 | 93 | 13 | 2 | 86.11 | 13.89 | 97.89 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=15;symbol_collisions=13 | 100.0 |
| Munich | missing | 223 | 223 | 189 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| NASDAQ | official_full | 4812 | 4754 | 4803 | 3496 | 3345 | 1372 | 5658 | 4615 | 59 | 984 | 81.57 | 18.43 | 82.43 | 984 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1043;symbol_collisions=59 | 99.56 |
| NEO | official_full | 247 | 203 | 230 | 0 | 147 | 1 | 443 | 209 | 68 | 166 | 47.18 | 52.82 | 55.73 | 166 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=234;symbol_collisions=68 | 100.0 |
| NGX | official_full | 145 | 145 | 145 | 0 | 133 | 76 | 130 | 130 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 3483 | 3483 | 3150 | 0 | 2476 | 0 | 3530 | 3458 | 72 | 0 | 97.96 | 2.04 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=72;symbol_collisions=72 | 100.0 |
| NSE_KE | official_full | 79 | 79 | 76 | 0 | 42 | 1 | 68 | 44 | 24 | 0 | 64.71 | 35.29 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=24 | 100.0 |
| NYSE | official_full | 2032 | 2008 | 2024 | 1946 | 1435 | 986 | 3878 | 1984 | 570 | 1324 | 51.16 | 48.84 | 59.98 | 1324 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1894;symbol_collisions=570 | 100.0 |
| NYSE ARCA | official_full | 2804 | 2740 | 2785 | 113 | 2091 | 353 | 2753 | 2633 | 31 | 89 | 95.64 | 4.36 | 96.73 | 89 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=120;symbol_collisions=31 | 100.0 |
| NYSE MKT | official_full | 234 | 228 | 233 | 140 | 142 | 46 | 309 | 229 | 33 | 47 | 74.11 | 25.89 | 82.97 | 47 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=33 | 100.0 |
| NZX | official_full | 84 | 84 | 82 | 0 | 45 | 1 | 172 | 84 | 87 | 1 | 48.84 | 51.16 | 98.82 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=88;symbol_collisions=87 | 100.0 |
| OSL | official_full | 314 | 314 | 308 | 2 | 258 | 243 | 297 | 290 | 6 | 1 | 97.64 | 2.36 | 99.66 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=6 | 100.0 |
| OTC | official_full | 11752 | 11067 | 11132 | 2048 | 8810 | 2805 | 11925 | 8267 | 25 | 3633 | 69.32 | 30.68 | 69.47 | 3633 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3658;symbol_collisions=25 | 88.97 |
| PSE | official_full | 172 | 172 | 146 | 1 | 87 | 16 | 384 | 170 | 120 | 94 | 44.27 | 55.73 | 64.39 | 94 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=120 | 100.0 |
| PSE_CZ | official_partial | 27 | 27 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 390 | 383 | 390 | 3 | 261 | 1 | 724 | 390 | 143 | 191 | 53.87 | 46.13 | 67.13 | 191 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=334;symbol_collisions=143 | 99.73 |
| QSE | official_full | 55 | 55 | 55 | 0 | 0 | 0 | 57 | 55 | 2 | 0 | 96.49 | 3.51 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 52 | 1 | 49 | 2 | 46 | 46 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 90.2 |
| SET | official_full | 779 | 768 | 779 | 4 | 326 | 1 | 942 | 771 | 134 | 37 | 81.85 | 18.15 | 95.42 | 37 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=171;symbol_collisions=134 | 100.0 |
| SGX | official_full | 632 | 632 | 619 | 0 | 8 | 18 | 748 | 626 | 122 | 0 | 83.69 | 16.31 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=122;symbol_collisions=122 | 99.63 |
| SIX | official_partial | 1263 | 1263 | 1262 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2795 | 2761 | 2795 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 129 | 102 | 120 | 0 | 85 | 1 | 122 | 122 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 876 | 876 | 875 | 2 | 816 | 796 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3150 | 3138 | 3150 | 0 | 2593 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 205 | 205 | 199 | 0 | 191 | 0 | 413 | 203 | 210 | 0 | 49.15 | 50.85 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=210;symbol_collisions=210 | 100.0 |
| TASE | official_partial | 801 | 801 | 797 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1119 | 1119 | 1119 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4136 | 4136 | 4113 | 0 | 4060 | 485 | 4444 | 4102 | 341 | 1 | 92.3 | 7.7 | 99.98 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=342;symbol_collisions=341 | 100.0 |
| TSX | official_full | 2421 | 2293 | 2367 | 12 | 1617 | 38 | 785 | 590 | 189 | 6 | 75.16 | 24.84 | 98.99 | 6 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=189 | 99.32 |
| TSXV | official_full | 1422 | 1349 | 1414 | 17 | 903 | 7 | 1518 | 1322 | 193 | 3 | 87.09 | 12.91 | 99.77 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=193 | 92.78 |
| TWSE | official_full | 1285 | 1285 | 1279 | 0 | 1165 | 3 | 1095 | 1060 | 35 | 0 | 96.8 | 3.2 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=35;symbol_collisions=35 | 100.0 |
| TXSE | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 16 | 0 | 9 | 7 | 0.0 | 100.0 | 0.0 | 7 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=16;symbol_collisions=9 |  |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 818 | 2 | 523 | 293 | 0.24 | 99.76 | 0.68 | 293 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=816;symbol_collisions=523 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 88 | 88 | 82 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 582 | 582 | 574 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XDUS | missing | 199 | 199 | 174 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XETRA | official_full | 5187 | 5186 | 5161 | 8 | 3827 | 1924 | 5129 | 4954 | 173 | 2 | 96.59 | 3.41 | 99.96 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=175;symbol_collisions=173 | 99.88 |
| XHAM | missing | 12 | 12 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XHAN | missing | 80 | 80 | 71 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XSTU | missing | 2773 | 2773 | 2498 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| TXSE | 0.0 | 0.0 | 16 | 16 | 7 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=16;symbol_collisions=9 |
| UPCOM | 0.24 | 0.68 | 818 | 816 | 293 | 523 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=816;symbol_collisions=523 |
| Borsa Italiana | 8.5 | 24.53 | 2905 | 2658 | 760 | 1898 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2658;symbol_collisions=1898 |
| CSE_MA | 21.95 | 100.0 | 82 | 64 | 0 | 64 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=64;symbol_collisions=64 |
| FSX | 43.65 | 57.03 | 18226 | 10271 | 5993 | 4278 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10271;symbol_collisions=4278 |
| PSE | 44.27 | 64.39 | 384 | 214 | 94 | 120 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=120 |
| HNX | 45.48 | 100.0 | 299 | 163 | 0 | 163 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=163;symbol_collisions=163 |
| NEO | 47.18 | 55.73 | 443 | 234 | 166 | 68 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=234;symbol_collisions=68 |
| NZX | 48.84 | 98.82 | 172 | 88 | 1 | 87 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=88;symbol_collisions=87 |
| TADAWUL | 49.15 | 100.0 | 413 | 210 | 0 | 210 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=210;symbol_collisions=210 |
| NYSE | 51.16 | 59.98 | 3878 | 1894 | 1324 | 570 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1894;symbol_collisions=570 |
| PSX | 53.87 | 67.13 | 724 | 334 | 191 | 143 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=334;symbol_collisions=143 |
| BVB | 56.18 | 100.0 | 89 | 39 | 0 | 39 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=39;symbol_collisions=39 |
| ISE | 56.25 | 100.0 | 16 | 7 | 0 | 7 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=7 |
| LSE | 60.92 | 66.04 | 11184 | 4371 | 3504 | 867 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4371;symbol_collisions=867 |
| NSE_KE | 64.71 | 100.0 | 68 | 24 | 0 | 24 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=24;symbol_collisions=24 |
| Euronext | 66.2 | 81.04 | 2015 | 681 | 312 | 369 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=681;symbol_collisions=369 |
| OTC | 69.32 | 69.47 | 11925 | 3658 | 3633 | 25 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3658;symbol_collisions=25 |
| NYSE MKT | 74.11 | 82.97 | 309 | 80 | 47 | 33 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=80;symbol_collisions=33 |
| TSX | 75.16 | 98.99 | 785 | 195 | 6 | 189 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=189 |
| ADX | 75.41 | 100.0 | 122 | 30 | 0 | 30 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=30;symbol_collisions=30 |
| BHB | 75.61 | 96.88 | 41 | 10 | 1 | 9 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10;symbol_collisions=9 |
| DFM | 76.06 | 98.18 | 71 | 17 | 1 | 16 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=16 |
| IDX | 78.59 | 97.67 | 962 | 206 | 18 | 188 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=188 |
| BATS | 79.53 | 82.42 | 1656 | 339 | 281 | 58 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=339;symbol_collisions=58 |
| BK | 80.0 | 100.0 | 140 | 28 | 0 | 28 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=28;symbol_collisions=28 |
| NASDAQ | 81.57 | 82.43 | 5658 | 1043 | 984 | 59 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1043;symbol_collisions=59 |
| SET | 81.85 | 95.42 | 942 | 171 | 37 | 134 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=171;symbol_collisions=134 |
| SGX | 83.69 | 100.0 | 748 | 122 | 0 | 122 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=122;symbol_collisions=122 |
| MSX | 86.11 | 97.89 | 108 | 15 | 2 | 13 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=15;symbol_collisions=13 |
| TSXV | 87.09 | 99.77 | 1518 | 196 | 3 | 193 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=193 |
| KOSDAQ | 87.2 | 87.34 | 1820 | 233 | 230 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=233;symbol_collisions=3 |
| BME | 90.71 | 90.71 | 269 | 25 | 25 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=25;symbol_collisions=0 |
| B3 | 90.76 | 90.76 | 1353 | 125 | 125 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=125;symbol_collisions=0 |
| TSE | 92.3 | 99.98 | 4444 | 342 | 1 | 341 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=342;symbol_collisions=341 |
| KRX | 92.37 | 92.99 | 2110 | 161 | 147 | 14 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=161;symbol_collisions=14 |
| AMS | 92.78 | 99.3 | 609 | 44 | 4 | 40 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=44;symbol_collisions=40 |
| NYSE ARCA | 95.64 | 96.73 | 2753 | 120 | 89 | 31 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=120;symbol_collisions=31 |
| QSE | 96.49 | 100.0 | 57 | 2 | 0 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 |
| XETRA | 96.59 | 99.96 | 5129 | 175 | 2 | 173 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=175;symbol_collisions=173 |
| BIST | 96.68 | 100.0 | 663 | 22 | 0 | 22 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=22;symbol_collisions=22 |
| TWSE | 96.8 | 100.0 | 1095 | 35 | 0 | 35 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=35;symbol_collisions=35 |
| BSE_IN | 97.02 | 99.35 | 5165 | 154 | 33 | 121 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=154;symbol_collisions=121 |
| OSL | 97.64 | 99.66 | 297 | 7 | 1 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=7;symbol_collisions=6 |
| HKEX | 97.85 | 99.97 | 3251 | 70 | 1 | 69 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=70;symbol_collisions=69 |
| NSE_IN | 97.96 | 100.0 | 3530 | 72 | 0 | 72 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=72;symbol_collisions=72 |
| CSE_LK | 99.06 | 99.06 | 319 | 3 | 3 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 62 | 62 | 61 | 0 | 56 | 0 |
| Australia | 1504 | 1402 | 1501 | 58 | 1178 | 115 |
| Austria | 49 | 49 | 48 | 1 | 41 | 39 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 32 | 32 | 31 | 0 | 27 | 7 |
| Belgium | 129 | 128 | 129 | 6 | 113 | 109 |
| Bermuda | 540 | 540 | 535 | 58 | 476 | 123 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1604 | 1594 | 1600 | 13 | 1259 | 2 |
| British Virgin Islands | 153 | 153 | 151 | 96 | 92 | 24 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4562 | 4318 | 4361 | 296 | 3258 | 34 |
| Cayman Islands | 2350 | 2341 | 2324 | 621 | 1858 | 244 |
| Chile | 116 | 89 | 116 | 3 | 84 | 2 |
| China | 6467 | 6415 | 6467 | 4 | 5244 | 10 |
| Colombia | 3 | 3 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 20 | 19 | 19 | 1 | 11 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 156 | 156 | 154 | 6 | 145 | 136 |
| Egypt | 230 | 229 | 230 | 0 | 200 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 2 | 2 |
| Finland | 200 | 200 | 199 | 2 | 193 | 3 |
| France | 700 | 697 | 700 | 15 | 659 | 643 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 863 | 858 | 827 | 10 | 750 | 685 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 140 | 140 | 138 | 1 | 121 | 117 |
| Guernsey | 67 | 67 | 66 | 4 | 54 | 54 |
| Hong Kong | 510 | 507 | 506 | 0 | 461 | 5 |
| Hungary | 38 | 37 | 38 | 0 | 30 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5737 | 5737 | 5698 | 0 | 3312 | 4 |
| Indonesia | 718 | 712 | 709 | 0 | 576 | 0 |
| Ireland | 2661 | 2653 | 2649 | 37 | 2429 | 853 |
| Isle of Man | 13 | 13 | 13 | 1 | 13 | 11 |
| Israel | 750 | 749 | 748 | 72 | 702 | 2 |
| Italy | 242 | 241 | 240 | 1 | 223 | 216 |
| Japan | 3454 | 3448 | 3418 | 21 | 3322 | 444 |
| Jersey | 175 | 175 | 173 | 13 | 158 | 156 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 55 | 55 | 52 | 0 | 30 | 0 |
| Kuwait | 108 | 108 | 107 | 0 | 101 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 9 | 9 | 2 | 0 | 2 | 2 |
| Luxembourg | 1057 | 1054 | 1053 | 16 | 992 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 974 | 974 | 974 | 0 | 933 | 1 |
| Malta | 7 | 7 | 6 | 0 | 6 | 6 |
| Marshall Islands | 43 | 43 | 42 | 36 | 26 | 20 |
| Mauritius | 60 | 60 | 59 | 2 | 53 | 0 |
| Mexico | 135 | 118 | 133 | 6 | 109 | 1 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 69 | 69 | 65 | 0 | 55 | 0 |
| Netherlands | 210 | 210 | 190 | 29 | 173 | 127 |
| New Zealand | 74 | 74 | 71 | 0 | 63 | 0 |
| Nigeria | 146 | 146 | 146 | 0 | 134 | 77 |
| Norway | 222 | 221 | 222 | 5 | 206 | 207 |
| Oman | 92 | 90 | 92 | 0 | 0 | 0 |
| Pakistan | 370 | 365 | 370 | 3 | 262 | 0 |
| Panama | 6 | 6 | 5 | 3 | 4 | 0 |
| Papua New Guinea | 1 | 1 | 1 | 0 | 1 | 0 |
| Peru | 32 | 32 | 32 | 1 | 29 | 0 |
| Philippines | 117 | 117 | 113 | 1 | 92 | 17 |
| Poland | 368 | 367 | 363 | 9 | 358 | 355 |
| Portugal | 38 | 38 | 38 | 0 | 38 | 36 |
| Puerto Rico | 6 | 6 | 6 | 5 | 6 | 4 |
| Qatar | 55 | 55 | 55 | 0 | 0 | 0 |
| Romania | 223 | 223 | 193 | 0 | 80 | 77 |
| Rwanda | 2 | 2 | 2 | 0 | 2 | 0 |
| Saudi Arabia | 197 | 197 | 191 | 0 | 191 | 0 |
| Singapore | 571 | 567 | 553 | 16 | 40 | 4 |
| Slovenia | 8 | 8 | 5 | 0 | 1 | 1 |
| South Africa | 252 | 252 | 229 | 10 | 175 | 141 |
| South Korea | 3585 | 3583 | 3582 | 1 | 3362 | 0 |
| Spain | 225 | 225 | 224 | 8 | 210 | 206 |
| Sri Lanka | 317 | 317 | 307 | 0 | 305 | 0 |
| Sweden | 814 | 809 | 804 | 5 | 760 | 754 |
| Switzerland | 371 | 371 | 370 | 22 | 345 | 293 |
| Taiwan | 2281 | 2280 | 2280 | 1 | 2056 | 1 |
| Tanzania | 14 | 14 | 14 | 0 | 12 | 0 |
| Thailand | 625 | 625 | 620 | 6 | 332 | 1 |
| Turkey | 648 | 648 | 624 | 0 | 618 | 552 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 126 | 126 | 126 | 0 | 121 | 0 |
| United Kingdom | 1308 | 1301 | 1295 | 49 | 1212 | 1020 |
| United States | 14893 | 14187 | 14340 | 5252 | 10668 | 3738 |
| Vietnam | 293 | 292 | 261 | 2 | 260 | 0 |
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
| Dataset rows | 1589 |
| Active exchange-directory rows | 1353 |
| Matched dataset rows | 1228 |
| Missing dataset rows | 361 |
| Dataset match rate | 77.28 |
| Any official B3 source matched dataset rows | 1251 |
| Any official B3 source missing dataset rows | 338 |
| Any official B3 source match rate | 78.73 |
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
