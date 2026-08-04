# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63291 |
| core_listings | 58277 |
| aliases | 125251 |
| stocks | 47518 |
| etfs | 15773 |
| isin_coverage | 61736 |
| sector_coverage | 63005 |
| stock_sector_coverage | 47366 |
| etf_category_coverage | 15639 |
| cik_coverage | 7679 |
| figi_coverage | 65447 |
| lei_coverage | 17809 |
| listing_status_rows | 100646 |
| listing_status_intervals | 100646 |
| listing_events | 45535 |
| listing_keys | 75219 |
| instrument_scope_rows | 75219 |
| instrument_scope_core | 58277 |
| instrument_scope_extended | 16942 |
| instrument_scope_primary_listing | 57145 |
| instrument_scope_primary_listing_missing_isin | 1132 |
| instrument_scope_otc_listing | 11094 |
| instrument_scope_secondary_cross_listing | 5848 |
| legacy_primary_ticker_collision_rows | 2501 |
| official_masterfile_symbols | 80759 |
| official_masterfile_matches | 53860 |
| official_masterfile_collisions | 11248 |
| official_masterfile_missing | 15651 |
| official_recall_denominator | 80759 |
| official_recall_matches | 53860 |
| official_recall_missing | 26899 |
| official_recall_pct | 66.69 |
| collision_adjusted_recall_denominator | 69511 |
| collision_adjusted_recall_missing | 15651 |
| collision_adjusted_recall_pct | 77.48 |
| collision_adjusted_recall_gap_rate | 22.52 |
| official_full_recall_target_exchanges | 48 |
| official_full_recall_passing_exchanges | 2 |
| official_full_recall_exception_exchanges | 46 |
| collision_adjusted_full_recall_passing_exchanges | 4 |
| collision_adjusted_full_recall_exception_exchanges | 44 |
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
| tickers_built_at | 2026-08-04T10:15:56Z |
| tickers_age_hours | 0.39 |
| masterfiles_generated_at | 2026-08-04T09:48:20Z |
| masterfiles_age_hours | 0.85 |
| identifiers_generated_at | 2026-08-04T10:29:48Z |
| identifiers_age_hours | 0.15 |
| listing_history_observed_at | 2026-07-07T07:32:52Z |
| listing_history_age_hours | 675.1 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 2210.22 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 2210.22 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 2210.22 |
| symbol_changes_generated_at | 2026-08-04T09:48:02Z |
| symbol_changes_age_hours | 0.85 |
| symbol_changes_review_rows | 319 |
| entry_quality_generated_at | 2026-08-04T10:29:52Z |
| entry_quality_age_hours | 0.15 |
| entry_quality_rows | 75219 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 1503.35 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 65.75 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-08-04T10:29:56Z |
| source_gap_classification_age_hours | 0.15 |
| source_gap_classification_rows | 7176 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-08-04T10:15:56Z | 0.39 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-08-04T09:48:20Z | 0.85 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-08-04T10:29:48Z | 0.15 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-07-07T07:32:52Z | 675.1 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 2210.22 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 2210.22 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-08-04T09:48:02Z | 0.85 | 319 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-08-04T10:29:52Z | 0.15 | 75219 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-08-04T10:29:56Z | 0.15 | 7176 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 1503.35 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 65.75 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 18, "old": 67, "stale": 52} |
| source_age_bucket_totals | {"age_0_48h": 18, "age_168_336h": 17, "age_48_168h": 52, "age_over_336h": 50} |
| refresh_priority_totals | {"P1": 21, "P2": 98, "P4": 18} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 18, "refresh_official_exchange_directory_before_identity_or_collision_work": 20, "refresh_official_subset_before_gap_enrichment": 86, "restore_or_replace_unavailable_source_before_data_fill": 13} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 12 | 15972 | 1503.0 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 8 | 22631 | 1503.0 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 111 | 1503.0 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 47 | 31366 | 1503.0 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 21 | 1821 | 1503.0 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 11 | 17297 | 145.5 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 10 | 20055 | 1503.0 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 839 | 1503.0 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 169.53 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 267 | 74.11 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5565 | 2026-08-04T09:23:31Z | 1.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7524 | 2026-08-04T09:23:31Z | 1.26 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 36 | 2026-07-07T09:34:57Z | 673.07 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11092 | 2026-07-29T09:09:22Z | 145.5 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 384.75 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_investment_products | ASX | listed_companies_subset | network | 458 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 493 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3619 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1774 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 267 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3854 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4071 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_listed_issues | JPX | exchange_directory | network | 4437 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | unavailable | 4030 | 2026-07-28T09:07:29Z | 169.53 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 468 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3652 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5080 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8900 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 844 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | network | 1327 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 213 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 314 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | network | 141 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 123 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 50 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 118 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1127 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | unavailable | 26 | 2026-07-20T09:31:47Z | 361.12 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 20 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 82 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 318 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 63 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | cache | 8 | 2026-07-07T09:07:40Z | 673.52 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 66 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 3010 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | network | 5077 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| hkex_securities_list | HKEX | exchange_directory | network | 3200 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| sgx_securities_prices | SGX | exchange_directory | network | 746 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 647 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| adx_market_watch | ADX | exchange_directory | network | 123 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| qse_market_watch | QSE | exchange_directory | cache | 57 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 111 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | cache | 47 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | cache | 173 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 350 | 2026-07-27T10:06:05Z | 192.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 9 | 2026-07-27T10:06:05Z | 192.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | cache | 133 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 10 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 2 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 9 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 50.07 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 746 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 191 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | cache | 32 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 125 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | cache | 53 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | cache | 30 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 143 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1093 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_etf_list | TWSE | listed_companies_subset | network | 231 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2351 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 909 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 704 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 896 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 117 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 890 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 359 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2759 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| krx_etf_finder | KRX | exchange_directory | network | 1160 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| psx_listed_companies | PSX | listed_companies_subset | network | 563 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 367 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 716 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | cache | 63 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 962 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| idx_company_profiles | IDX | exchange_directory | network | 962 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| wse_listed_companies | GPW | listed_companies_subset | network | 403 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | cache | 364 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | network | 36 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 530 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 465 | 2026-07-31T09:14:12Z | 97.41 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 403 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-08-04T09:48:20Z | 0.85 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| upcom_registered_securities | HNX | exchange_directory | network | 822 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 38 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 73 | 2026-08-01T08:32:15Z | 74.11 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10122 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 745 | 2026-06-02T19:38:59Z | 1503.0 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 11925 | 2026-06-02T19:38:59Z | 1503.0 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 21 |
| P2 | 98 |
| P4 | 18 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 20 |
| refresh_official_subset_before_gap_enrichment | 86 |
| restore_or_replace_unavailable_source_before_data_fill | 13 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 9 |
| fresh_no_refresh_needed | listed_companies_subset | 9 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 20 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | exchange_directory | 11 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 68 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 5 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 1 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 10 |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | 1 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 1 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 8 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 12 |
| refresh_official_subset_before_gap_enrichment | cache | 22 |
| refresh_official_subset_before_gap_enrichment | network | 64 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 13 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 20 |
| refresh_official_subset_before_gap_enrichment | P2 | 86 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 1 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 12 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 18 |
| age_168_336h | 17 |
| age_48_168h | 52 |
| age_over_336h | 50 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_168_336h | 9 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_over_336h | 11 |
| refresh_official_subset_before_gap_enrichment | age_168_336h | 7 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 52 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 27 |
| restore_or_replace_unavailable_source_before_data_fill | age_168_336h | 1 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 12 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 20 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 86 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 13 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 18 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 20 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 86 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 13 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 12 | 15972 | 1503.0 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 8 | 22631 | 1503.0 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 1 | 111 | 1503.0 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 47 | 31366 | 1503.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 21 | 1821 | 1503.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 11 | 17297 | 145.5 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 10 | 20055 | 1503.0 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 839 | 1503.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 169.53 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 267 | 74.11 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1503.0 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 36 | 673.07 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 1 | 7 | 1503.0 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 9 | 29760 | 1.26 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| fresh_no_refresh_needed | listed_companies_subset | network | P4 | 9 | 5914 | 0.85 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope listed_companies_subset. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 123 | 85 | 32 | 6 | 69.11 | 30.89 | 93.41 | 6 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 | 100.0 |
| AMS | official_full | 331 | 331 | 265 | 0 | 322 | 153 | 602 | 241 | 296 | 65 | 40.03 | 59.97 | 78.76 | 65 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=361;symbol_collisions=296 | 100.0 |
| ASX | official_partial | 1708 | 1526 | 1704 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 155 | 155 | 155 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1579 | 0 | 1252 | 0 | 1327 | 1220 | 0 | 107 | 91.94 | 8.06 | 91.94 | 107 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1324 | 1224 | 1228 | 0 | 1048 | 243 | 1573 | 1233 | 52 | 288 | 78.39 | 21.61 | 81.07 | 288 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=340;symbol_collisions=52 | 100.0 |
| BCBA | official_partial | 63 | 63 | 63 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 28 | 28 | 28 | 0 | 26 | 7 | 41 | 28 | 9 | 4 | 68.29 | 31.71 | 87.5 | 4 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 647 | 611 | 21 | 15 | 94.44 | 5.56 | 97.6 | 15 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=21 | 100.0 |
| BK | official_full | 104 | 104 | 103 | 0 | 104 | 0 | 140 | 102 | 27 | 11 | 72.86 | 27.14 | 90.27 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 | 100.0 |
| BME | official_full | 220 | 220 | 220 | 3 | 220 | 212 | 50 | 12 | 6 | 32 | 24.0 | 76.0 | 27.27 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=6 | 100.0 |
| BMV | official_partial | 179 | 162 | 178 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 47 | 0 | 41 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2732 | 2732 | 2636 | 0 | 2613 | 0 | 5077 | 2671 | 1842 | 564 | 52.61 | 47.39 | 82.57 | 564 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2406;symbol_collisions=1842 | 100.0 |
| BVB | official_full | 80 | 80 | 80 | 0 | 80 | 76 | 350 | 75 | 127 | 148 | 21.43 | 78.57 | 33.63 | 148 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=127 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 276 | 275 | 2898 | 251 | 1808 | 839 | 8.66 | 91.34 | 23.03 | 839 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2647;symbol_collisions=1808 |  |
| Bursa | official_partial | 936 | 936 | 936 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 147 | 147 | 146 | 0 | 145 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 318 | 307 | 0 | 11 | 96.54 | 3.46 | 96.54 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 82 | 1 | 61 | 20 | 1.22 | 98.78 | 4.76 | 20 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=61 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 45 | 17 | 9 | 63.38 | 36.62 | 83.33 | 9 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 15 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 222 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1087 | 1087 | 996 | 7 | 1071 | 844 | 2007 | 966 | 666 | 375 | 48.13 | 51.87 | 72.04 | 375 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1041;symbol_collisions=666 | 100.0 |
| GSE | official_partial | 19 | 18 | 18 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 196 | 196 | 196 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3058 | 3058 | 3013 | 0 | 3010 | 268 | 3200 | 3038 | 70 | 92 | 94.94 | 5.06 | 97.06 | 92 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=162;symbol_collisions=70 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 299 | 104 | 156 | 39 | 34.78 | 65.22 | 72.73 | 39 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 756 | 694 | 756 | 0 | 577 | 0 | 962 | 756 | 186 | 20 | 78.59 | 21.41 | 97.42 | 20 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=186 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 15 | 9 | 6 | 0 | 60.0 | 40.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1605 | 1605 | 1605 | 0 | 1578 | 0 | 1817 | 1596 | 0 | 221 | 87.84 | 12.16 | 87.84 | 221 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=0 | 99.62 |
| KRX | official_full | 1812 | 1811 | 1812 | 0 | 1793 | 0 | 2102 | 1784 | 3 | 315 | 84.87 | 15.13 | 84.99 | 315 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=318;symbol_collisions=3 | 99.76 |
| LSE | official_full | 6606 | 6605 | 6266 | 16 | 6499 | 4346 | 11092 | 6437 | 1096 | 3559 | 58.03 | 41.97 | 64.4 | 3559 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4655;symbol_collisions=1096 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 14 | 3 | 84.26 | 15.74 | 96.81 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=14 | 100.0 |
| NASDAQ | official_full | 4667 | 4525 | 4599 | 3448 | 3392 | 1384 | 5745 | 4567 | 66 | 1112 | 79.5 | 20.5 | 80.42 | 1112 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1178;symbol_collisions=66 | 99.56 |
| NEO | official_full | 197 | 154 | 191 | 0 | 149 | 1 | 440 | 183 | 86 | 171 | 41.59 | 58.41 | 51.69 | 171 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 | 100.0 |
| NGX | official_full | 145 | 145 | 144 | 0 | 133 | 76 | 133 | 133 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2499 | 0 | 3010 | 2369 | 231 | 410 | 78.7 | 21.3 | 85.25 | 410 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=231 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 45 | 0 | 42 | 1 | 66 | 11 | 23 | 32 | 16.67 | 83.33 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 | 100.0 |
| NYSE | official_full | 2005 | 1951 | 1992 | 1927 | 1442 | 995 | 3919 | 1994 | 511 | 1414 | 50.88 | 49.12 | 58.51 | 1414 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1925;symbol_collisions=511 | 100.0 |
| NYSE ARCA | official_full | 2687 | 2604 | 2628 | 113 | 2099 | 368 | 2693 | 2566 | 32 | 95 | 95.28 | 4.72 | 96.43 | 95 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=32 | 100.0 |
| NYSE MKT | official_full | 229 | 219 | 228 | 146 | 151 | 51 | 308 | 224 | 31 | 53 | 72.73 | 27.27 | 80.87 | 53 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=84;symbol_collisions=31 | 100.0 |
| NZX | official_full | 45 | 45 | 42 | 0 | 45 | 1 | 173 | 45 | 126 | 2 | 26.01 | 73.99 | 95.74 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 | 100.0 |
| OSL | official_full | 269 | 269 | 261 | 2 | 258 | 243 | 297 | 248 | 42 | 7 | 83.5 | 16.5 | 97.25 | 7 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=49;symbol_collisions=42 | 100.0 |
| OTC | official_full | 11094 | 10319 | 10840 | 1924 | 8846 | 2847 | 11925 | 7693 | 38 | 4194 | 64.51 | 35.49 | 64.72 | 4194 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4232;symbol_collisions=38 | 88.97 |
| PSE | official_full | 88 | 88 | 87 | 1 | 88 | 16 | 381 | 88 | 181 | 112 | 23.1 | 76.9 | 44.0 | 112 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=293;symbol_collisions=181 | 100.0 |
| PSE_CZ | official_partial | 27 | 27 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 371 | 366 | 371 | 3 | 263 | 2 | 716 | 371 | 156 | 189 | 51.82 | 48.18 | 66.25 | 189 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=156 | 99.73 |
| QSE | official_full | 55 | 54 | 55 | 0 | 0 | 0 | 57 | 55 | 2 | 0 | 96.49 | 3.51 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 50 | 1 | 49 | 2 | 47 | 46 | 0 | 1 | 97.87 | 2.13 | 97.87 | 1 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1;symbol_collisions=0 | 90.2 |
| SET | official_full | 691 | 547 | 691 | 4 | 335 | 4 | 944 | 687 | 212 | 45 | 72.78 | 27.22 | 93.85 | 45 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=212 | 100.0 |
| SGX | official_full | 613 | 613 | 553 | 0 | 8 | 18 | 746 | 611 | 121 | 14 | 81.9 | 18.1 | 97.76 | 14 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=135;symbol_collisions=121 | 99.63 |
| SIX | official_partial | 759 | 759 | 758 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2789 | 2754 | 2789 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 116 | 89 | 115 | 0 | 85 | 1 | 111 | 111 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 843 | 843 | 840 | 2 | 825 | 805 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3083 | 3071 | 3083 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 199 | 199 | 191 | 0 | 191 | 0 | 412 | 198 | 209 | 5 | 48.06 | 51.94 | 97.54 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=209 | 100.0 |
| TASE | official_partial | 695 | 695 | 661 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1119 | 1119 | 1119 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4077 | 4060 | 4065 | 0 | 4060 | 485 | 4437 | 4053 | 325 | 59 | 91.35 | 8.65 | 98.57 | 59 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=384;symbol_collisions=325 | 100.0 |
| TSX | official_full | 1845 | 1766 | 1803 | 12 | 1621 | 40 | 785 | 324 | 452 | 9 | 41.27 | 58.73 | 97.3 | 9 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=461;symbol_collisions=452 | 99.32 |
| TSXV | official_full | 1085 | 980 | 1055 | 17 | 911 | 9 | 1518 | 989 | 517 | 12 | 65.15 | 34.85 | 98.8 | 12 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=529;symbol_collisions=517 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1191 | 0 | 1165 | 3 | 1093 | 972 | 62 | 59 | 88.93 | 11.07 | 94.28 | 59 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=62 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 822 | 2 | 477 | 343 | 0.24 | 99.76 | 0.58 | 343 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=477 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 56 | 56 | 56 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 542 | 542 | 541 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XETRA | official_full | 3857 | 3856 | 3234 | 8 | 3828 | 1925 | 5080 | 3657 | 853 | 570 | 71.99 | 28.01 | 86.52 | 570 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1423;symbol_collisions=853 | 99.88 |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.58 | 822 | 820 | 343 | 477 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=477 |
| CSE_MA | 1.22 | 4.76 | 82 | 81 | 20 | 61 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=61 |
| Borsa Italiana | 8.66 | 23.03 | 2898 | 2647 | 839 | 1808 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2647;symbol_collisions=1808 |
| NSE_KE | 16.67 | 25.58 | 66 | 55 | 32 | 23 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=55;symbol_collisions=23 |
| BVB | 21.43 | 33.63 | 350 | 275 | 148 | 127 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=275;symbol_collisions=127 |
| PSE | 23.1 | 44.0 | 381 | 293 | 112 | 181 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=293;symbol_collisions=181 |
| BME | 24.0 | 27.27 | 50 | 38 | 32 | 6 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=6 |
| NZX | 26.01 | 95.74 | 173 | 128 | 2 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=128;symbol_collisions=126 |
| HNX | 34.78 | 72.73 | 299 | 195 | 39 | 156 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=156 |
| AMS | 40.03 | 78.76 | 602 | 361 | 65 | 296 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=361;symbol_collisions=296 |
| TSX | 41.27 | 97.3 | 785 | 461 | 9 | 452 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=461;symbol_collisions=452 |
| NEO | 41.59 | 51.69 | 440 | 257 | 171 | 86 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=86 |
| TADAWUL | 48.06 | 97.54 | 412 | 214 | 5 | 209 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=209 |
| Euronext | 48.13 | 72.04 | 2007 | 1041 | 375 | 666 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1041;symbol_collisions=666 |
| NYSE | 50.88 | 58.51 | 3919 | 1925 | 1414 | 511 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1925;symbol_collisions=511 |
| PSX | 51.82 | 66.25 | 716 | 345 | 189 | 156 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=345;symbol_collisions=156 |
| BSE_IN | 52.61 | 82.57 | 5077 | 2406 | 564 | 1842 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2406;symbol_collisions=1842 |
| LSE | 58.03 | 64.4 | 11092 | 4655 | 3559 | 1096 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4655;symbol_collisions=1096 |
| ISE | 60.0 | 100.0 | 15 | 6 | 0 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 |
| DFM | 63.38 | 83.33 | 71 | 26 | 9 | 17 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 |
| OTC | 64.51 | 64.72 | 11925 | 4232 | 4194 | 38 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4232;symbol_collisions=38 |
| TSXV | 65.15 | 98.8 | 1518 | 529 | 12 | 517 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=529;symbol_collisions=517 |
| BHB | 68.29 | 87.5 | 41 | 13 | 4 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 |
| ADX | 69.11 | 93.41 | 123 | 38 | 6 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 |
| XETRA | 71.99 | 86.52 | 5080 | 1423 | 570 | 853 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1423;symbol_collisions=853 |
| NYSE MKT | 72.73 | 80.87 | 308 | 84 | 53 | 31 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=84;symbol_collisions=31 |
| SET | 72.78 | 93.85 | 944 | 257 | 45 | 212 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=257;symbol_collisions=212 |
| BK | 72.86 | 90.27 | 140 | 38 | 11 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 |
| BATS | 78.39 | 81.07 | 1573 | 340 | 288 | 52 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=340;symbol_collisions=52 |
| IDX | 78.59 | 97.42 | 962 | 206 | 20 | 186 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=186 |
| NSE_IN | 78.7 | 85.25 | 3010 | 641 | 410 | 231 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=641;symbol_collisions=231 |
| NASDAQ | 79.5 | 80.42 | 5745 | 1178 | 1112 | 66 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1178;symbol_collisions=66 |
| SGX | 81.9 | 97.76 | 746 | 135 | 14 | 121 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=135;symbol_collisions=121 |
| OSL | 83.5 | 97.25 | 297 | 49 | 7 | 42 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=49;symbol_collisions=42 |
| MSX | 84.26 | 96.81 | 108 | 17 | 3 | 14 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=14 |
| KRX | 84.87 | 84.99 | 2102 | 318 | 315 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=318;symbol_collisions=3 |
| KOSDAQ | 87.84 | 87.84 | 1817 | 221 | 221 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=0 |
| TWSE | 88.93 | 94.28 | 1093 | 121 | 59 | 62 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=62 |
| TSE | 91.35 | 98.57 | 4437 | 384 | 59 | 325 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=384;symbol_collisions=325 |
| B3 | 91.94 | 91.94 | 1327 | 107 | 107 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 |
| BIST | 94.44 | 97.6 | 647 | 36 | 15 | 21 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=21 |
| HKEX | 94.94 | 97.06 | 3200 | 162 | 92 | 70 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=162;symbol_collisions=70 |
| NYSE ARCA | 95.28 | 96.43 | 2693 | 127 | 95 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=32 |
| QSE | 96.49 | 100.0 | 57 | 2 | 0 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 |
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
| Bermuda | 530 | 530 | 530 | 58 | 488 | 130 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1597 | 1587 | 1596 | 12 | 1261 | 2 |
| British Virgin Islands | 154 | 154 | 153 | 95 | 99 | 28 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4641 | 4424 | 4636 | 528 | 3959 | 50 |
| Cayman Islands | 2218 | 2208 | 2217 | 570 | 1881 | 246 |
| Chile | 116 | 89 | 116 | 3 | 84 | 2 |
| China | 6361 | 6308 | 6361 | 4 | 5248 | 9 |
| Colombia | 4 | 4 | 4 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 19 | 18 | 18 | 1 | 12 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 154 | 154 | 154 | 6 | 149 | 139 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 3 |
| Finland | 198 | 198 | 198 | 2 | 196 | 2 |
| France | 747 | 744 | 746 | 16 | 729 | 720 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 798 | 793 | 794 | 8 | 779 | 710 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 141 | 141 | 141 | 1 | 122 | 118 |
| Guernsey | 68 | 68 | 68 | 4 | 60 | 61 |
| Hong Kong | 464 | 461 | 464 | 0 | 457 | 4 |
| Hungary | 37 | 36 | 37 | 0 | 29 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5069 | 5069 | 5018 | 0 | 4991 | 4 |
| Indonesia | 703 | 699 | 703 | 1 | 582 | 0 |
| Ireland | 2571 | 2561 | 2571 | 37 | 2542 | 911 |
| Isle of Man | 14 | 14 | 14 | 1 | 13 | 12 |
| Israel | 756 | 754 | 754 | 88 | 730 | 3 |
| Italy | 236 | 234 | 234 | 1 | 225 | 220 |
| Japan | 3383 | 3377 | 3383 | 30 | 3367 | 485 |
| Jersey | 171 | 171 | 170 | 16 | 165 | 164 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 2 |
| Luxembourg | 1017 | 1014 | 1014 | 14 | 1007 | 2 |
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
| Singapore | 552 | 548 | 548 | 15 | 49 | 3 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 1 |
| South Africa | 230 | 230 | 230 | 7 | 179 | 145 |
| South Korea | 3409 | 3407 | 3409 | 1 | 3362 | 0 |
| Spain | 246 | 246 | 246 | 7 | 242 | 239 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 805 | 800 | 804 | 5 | 789 | 786 |
| Switzerland | 401 | 401 | 401 | 25 | 393 | 340 |
| Taiwan | 2276 | 2275 | 2276 | 1 | 2056 | 1 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 550 | 550 | 550 | 7 | 334 | 0 |
| Turkey | 620 | 620 | 620 | 0 | 620 | 554 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1349 | 1345 | 1348 | 57 | 1296 | 1106 |
| United States | 14218 | 13214 | 14031 | 5223 | 10813 | 3844 |
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
