# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 63790 |
| core_listings | 61682 |
| aliases | 125401 |
| stocks | 47782 |
| etfs | 16008 |
| isin_coverage | 62461 |
| sector_coverage | 61860 |
| stock_sector_coverage | 46099 |
| etf_category_coverage | 15761 |
| cik_coverage | 7788 |
| figi_coverage | 65430 |
| lei_coverage | 17806 |
| listing_status_rows | 107885 |
| listing_status_intervals | 107885 |
| listing_events | 80335 |
| listing_keys | 91977 |
| instrument_scope_rows | 91977 |
| instrument_scope_core | 61682 |
| instrument_scope_extended | 30295 |
| instrument_scope_primary_listing | 60808 |
| instrument_scope_primary_listing_missing_isin | 874 |
| instrument_scope_otc_listing | 11753 |
| instrument_scope_secondary_cross_listing | 18542 |
| legacy_primary_ticker_collision_rows | 4786 |
| official_masterfile_symbols | 98636 |
| official_masterfile_matches | 64868 |
| official_masterfile_collisions | 13621 |
| official_masterfile_missing | 20147 |
| official_recall_denominator | 98636 |
| official_recall_matches | 64868 |
| official_recall_missing | 33768 |
| official_recall_pct | 65.77 |
| collision_adjusted_recall_denominator | 85015 |
| collision_adjusted_recall_missing | 20147 |
| collision_adjusted_recall_pct | 76.3 |
| collision_adjusted_recall_gap_rate | 23.7 |
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
| tickers_built_at | 2026-08-18T20:23:15Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-08-18T05:44:22Z |
| masterfiles_age_hours | 14.65 |
| identifiers_generated_at | 2026-08-18T20:23:19Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-08-18T20:23:15Z |
| listing_history_age_hours | 0.0 |
| latest_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_verification_age_hours | 2555.96 |
| latest_stock_verification_run | data/stock_verification/run-20260504-sgx-isin-refresh |
| latest_stock_verification_generated_at | 2026-05-04T08:25:42Z |
| latest_stock_verification_age_hours | 2555.96 |
| latest_etf_verification_run | data/etf_verification/run-20260504-sgx-isin-refresh |
| latest_etf_verification_generated_at | 2026-05-04T08:25:46Z |
| latest_etf_verification_age_hours | 2555.96 |
| symbol_changes_generated_at | 2026-08-18T07:02:32Z |
| symbol_changes_age_hours | 13.35 |
| symbol_changes_review_rows | 325 |
| entry_quality_generated_at | 2026-08-18T08:42:48Z |
| entry_quality_age_hours | 11.68 |
| entry_quality_rows | 91977 |
| masterfile_collision_review_generated_at | 2026-06-02T19:18:19Z |
| masterfile_collision_review_age_hours | 1849.09 |
| masterfile_collision_review_rows | 11176 |
| ohlcv_plausibility_generated_at | 2026-08-01T16:53:59Z |
| ohlcv_plausibility_age_hours | 411.49 |
| ohlcv_plausibility_rows | 143 |
| source_gap_classification_generated_at | 2026-08-18T08:42:49Z |
| source_gap_classification_age_hours | 11.68 |
| source_gap_classification_rows | 9389 |

## Freshness Review Summary

Freshness is visibility evidence only. It does not authorize identifiers, sectors, categories, names, or symbol changes.

| Signal | Generated At | Age Hours | Rows | Source Gate |
|---|---|---:|---:|---|
| Dataset build | 2026-08-18T20:23:15Z | 0.0 |  | dataset_age_visibility_no_data_change_authorized |
| Masterfiles | 2026-08-18T05:44:22Z | 14.65 |  | refresh_old_official_sources_before_identity_or_gap_work |
| Identifiers | 2026-08-18T20:23:19Z | 0.0 |  | identifier_age_visibility_no_identifier_backfill_authorized |
| Listing history | 2026-08-18T20:23:15Z | 0.0 |  | refresh_listing_history_before_fresh_listing_status_claims |
| Stock verification | 2026-05-04T08:25:42Z | 2555.96 |  | rerun_verification_before_closing_stock_source_gaps |
| ETF verification | 2026-05-04T08:25:46Z | 2555.96 |  | rerun_verification_before_closing_etf_source_gaps |
| Symbol changes | 2026-08-18T07:02:32Z | 13.35 | 325 | symbol_change_age_visibility_no_symbol_change_authorized |
| Entry quality | 2026-08-18T08:42:48Z | 11.68 | 91977 | entry_quality_age_visibility_no_quality_gate_override |
| Source gaps | 2026-08-18T08:42:49Z | 11.68 | 9389 | source_gap_age_visibility_no_gap_fill_authorized |
| Masterfile collisions | 2026-06-02T19:18:19Z | 1849.09 | 11176 | collision_review_age_visibility_no_symbol_only_match_authorized |
| OHLCV plausibility | 2026-08-01T16:53:59Z | 411.49 | 143 | ohlcv_age_visibility_plausibility_only |

### Source Freshness Totals

| Metric | Value |
|---|---|
| freshness_status_totals | {"fresh": 1, "old": 135, "stale": 2} |
| source_age_bucket_totals | {"age_0_48h": 1, "age_168_336h": 18, "age_48_168h": 2, "age_over_336h": 117} |
| refresh_priority_totals | {"P1": 39, "P2": 98, "P4": 1} |
| refresh_queue_totals | {"fresh_no_refresh_needed": 1, "refresh_official_exchange_directory_before_identity_or_collision_work": 37, "refresh_official_subset_before_gap_enrichment": 85, "restore_or_replace_unavailable_source_before_data_fill": 15} |

### Highest Priority Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Source Gate |
|---|---|---|---|---:|---:|---:|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 36 | 60556 | 538.29 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 12036 | 1848.74 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 1 | 108 | 1848.74 | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 61 | 37906 | 538.29 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 15 | 1177 | 1848.74 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 11 | 20085 | 1848.74 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 954 | 1848.74 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 2 | 13144 | 85.25 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 515.27 | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 267 | 419.85 | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At | Age Hours | Freshness | Refresh Priority | Refresh Queue | Action | Recommended next source | Source gate |
|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5594 | 2026-08-15T07:08:36Z | 85.25 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7550 | 2026-08-15T07:08:36Z | 85.25 | stale | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_trading_system_adds_deletes | Nasdaq Trader | corporate_action_daily_list | network | 13 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_company_reports | LSE | listed_companies_subset | unavailable | 12707 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| lse_instrument_directory | LSE | security_lookup_subset | unavailable | 64 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| lse_price_explorer | LSE | exchange_directory | network | 11092 | 2026-07-29T09:09:22Z | 491.23 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_listed_companies | ASX | listed_companies_subset | unavailable | 1987 | 2026-07-19T09:54:21Z | 730.48 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 440 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| asx_investment_products | ASX | listed_companies_subset | network | 458 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_listed_companies | SET | listed_companies_subset | network | 931 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_stock_search | SET | exchange_directory | network | 944 | 2026-07-31T09:14:12Z | 443.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| set_dr_search | SET | listed_companies_subset | network | 493 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3619 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1775 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 267 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| euronext_equities | Euronext | exchange_directory | network | 3854 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| euronext_etfs | Euronext | listed_companies_subset | network | 4071 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jpx_listed_issues | JPX | exchange_directory | network | 4437 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | unavailable | 4030 | 2026-07-28T09:07:29Z | 515.27 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 468 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3652 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 5080 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| deutsche_boerse_frankfurt_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 18016 | 2026-08-18T05:44:22Z | 14.65 | fresh | P4 | fresh_no_refresh_needed | no_refresh_needed | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etf_products | SIX | listed_companies_subset | network | 8900 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| six_etp_products | SIX | listed_companies_subset | network | 844 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_instruments_equities | B3 | exchange_directory | network | 1327 | 2026-08-02T08:35:09Z | 395.8 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| b3_listed_etfs | B3 | listed_companies_subset | network | 213 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 314 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etf_list | JSE | listed_companies_subset | network | 141 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_etn_list | JSE | listed_companies_subset | network | 104 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| jse_instrument_search | JSE | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_companies | BME | listed_companies_subset | network | 123 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bme_security_prices_directory | BME | exchange_directory | network | 50 | 2026-08-02T08:35:09Z | 395.8 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bme_growth_prices | BME Growth | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 118 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | unavailable | 1127 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | unavailable | 1281 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | unavailable | 26 | 2026-07-20T09:31:47Z | 706.86 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 20 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| egx_listed_stocks | EGX | listed_companies_subset | network | 191 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 82 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 307 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 318 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvc_colombia_issuers | BVC | listed_companies_subset | unavailable | 3 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| byma_equity_details | BYMA | security_lookup_subset | network | 63 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | cache | 8 | 2026-07-07T09:07:40Z | 1019.26 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 68 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nse_india_securities_available | NSE India | exchange_directory | network | 2978 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bse_india_scrips | BSE India | exchange_directory | network | 5077 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| hkex_securities_list | HKEX | exchange_directory | network | 3200 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| sgx_securities_prices | SGX | exchange_directory | network | 746 | 2026-07-31T09:14:12Z | 443.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 140 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-08-02T08:35:09Z | 395.8 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 647 | 2026-08-02T08:35:09Z | 395.8 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 412 | 2026-07-31T09:14:12Z | 443.15 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| adx_market_watch | ADX | exchange_directory | network | 123 | 2026-08-02T08:35:09Z | 395.8 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-06-02T19:38:59Z | 1848.74 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| rse_listed_companies | RSE | listed_companies_subset | network | 1 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | unavailable | 111 | 2026-06-02T19:38:59Z | 1848.74 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| sem_isin | SEM | exchange_directory | network | 46 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nzx_instruments | NZX | exchange_directory | network | 172 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 26 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bvb_shares_directory | BVB | exchange_directory | network | 350 | 2026-07-27T10:06:05Z | 538.29 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 9 | 2026-07-27T10:06:05Z | 538.29 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_equities_price_list | NGX | listed_companies_subset | network | 130 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngx_company_profile_directory | NGX | exchange_directory | network | 130 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| bmv_stock_search | BMV | listed_companies_subset | network | 10 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 5 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_etf_search | BMV | listed_companies_subset | network | 2 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 9 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 0 | 2026-08-02T08:35:09Z | 395.8 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 746 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 191 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | cache | 32 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 125 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| ngm_market_data_equities | NGM | listed_companies_subset | unavailable | 30 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 143 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| twse_listed_companies | TWSE | exchange_directory | network | 1093 | 2026-08-01T08:32:15Z | 419.85 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| twse_etf_list | TWSE | listed_companies_subset | network | 231 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_a_share_list | SSE | listed_companies_subset | network | 2351 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sse_etf_list | SSE | listed_companies_subset | network | 909 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 2893 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| szse_etf_list | SZSE | listed_companies_subset | network | 704 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 896 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 117 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 890 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 361 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| krx_listed_companies | KRX | exchange_directory | network | 2759 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| krx_etf_finder | KRX | exchange_directory | network | 1160 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| psx_listed_companies | PSX | listed_companies_subset | network | 563 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 365 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| psx_dps_symbols | PSX | exchange_directory | network | 720 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_listed_company_directory | PSE | exchange_directory | network | 385 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 62 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_listed_companies | IDX | listed_companies_subset | network | 962 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| idx_company_profiles | IDX | exchange_directory | network | 962 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| wse_listed_companies | GPW | listed_companies_subset | network | 403 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 348 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| wse_etf_list | GPW | listed_companies_subset | network | 38 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 531 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 465 | 2026-07-31T09:14:12Z | 443.15 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| tase_foreign_etf_search | TASE | listed_companies_subset | unavailable | 15 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| tase_participating_unit_search | TASE | listed_companies_subset | unavailable | 16 | 2026-06-02T19:38:59Z | 1848.74 | old | P2 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 403 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_etf_list | HOSE | listed_companies_subset | network | 20 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-08-04T09:48:20Z | 346.58 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| hnx_listed_securities | HNX | exchange_directory | network | 299 | 2026-08-04T09:48:20Z | 346.58 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| upcom_registered_securities | HNX | exchange_directory | network | 822 | 2026-08-01T08:32:15Z | 419.85 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 66 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 73 | 2026-08-01T08:32:15Z | 419.85 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| sec_company_tickers_exchange | SEC | exchange_directory | network | 10167 | 2026-08-06T08:50:31Z | 299.55 | old | P1 | refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 860 | 2026-08-06T08:50:31Z | 299.55 | old | P2 | refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| otc_markets_stock_screener | OTC Markets | exchange_directory | unavailable | 11925 | 2026-06-02T19:38:59Z | 1848.74 | old | P1 | restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |

## Source Refresh Priority

| Priority | Sources |
|---|---:|
| P1 | 39 |
| P2 | 98 |
| P4 | 1 |

## Source Refresh Queues

| Queue | Sources |
|---|---:|
| fresh_no_refresh_needed | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | 37 |
| refresh_official_subset_before_gap_enrichment | 85 |
| restore_or_replace_unavailable_source_before_data_fill | 15 |

## Source Refresh Queue By Scope

| Queue | Scope | Sources |
|---|---|---:|
| fresh_no_refresh_needed | exchange_directory | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | 37 |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | 1 |
| refresh_official_subset_before_gap_enrichment | exchange_directory | 2 |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | 1 |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | 76 |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | 5 |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | 2 |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | 11 |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | 1 |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | 1 |

## Source Refresh Queue By Mode

| Queue | Mode | Sources |
|---|---|---:|
| fresh_no_refresh_needed | network | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | cache | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | network | 36 |
| refresh_official_subset_before_gap_enrichment | cache | 16 |
| refresh_official_subset_before_gap_enrichment | network | 69 |
| restore_or_replace_unavailable_source_before_data_fill | unavailable | 15 |

## Source Refresh Queue By Priority

| Queue | Priority | Sources |
|---|---|---:|
| fresh_no_refresh_needed | P4 | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | P1 | 37 |
| refresh_official_subset_before_gap_enrichment | P2 | 85 |
| restore_or_replace_unavailable_source_before_data_fill | P1 | 2 |
| restore_or_replace_unavailable_source_before_data_fill | P2 | 13 |

## Source Age Buckets

| Age bucket | Sources |
|---|---:|
| age_0_48h | 1 |
| age_168_336h | 18 |
| age_48_168h | 2 |
| age_over_336h | 117 |

## Source Refresh Queue By Age Bucket

| Queue | Age bucket | Sources |
|---|---|---:|
| fresh_no_refresh_needed | age_0_48h | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_168_336h | 9 |
| refresh_official_exchange_directory_before_identity_or_collision_work | age_over_336h | 28 |
| refresh_official_subset_before_gap_enrichment | age_168_336h | 9 |
| refresh_official_subset_before_gap_enrichment | age_48_168h | 2 |
| refresh_official_subset_before_gap_enrichment | age_over_336h | 74 |
| restore_or_replace_unavailable_source_before_data_fill | age_over_336h | 15 |

## Source Refresh Strategies

| Queue | Strategy | Sources |
|---|---|---:|
| fresh_no_refresh_needed | no_refresh_required | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | refresh_official_exchange_directory_before_identity_or_collision_work | 37 |
| refresh_official_subset_before_gap_enrichment | refresh_official_subset_before_gap_enrichment | 85 |
| restore_or_replace_unavailable_source_before_data_fill | restore_or_replace_unavailable_source_before_data_fill | 15 |

## Source Refresh Evidence

| Queue | Evidence required | Sources |
|---|---|---:|
| fresh_no_refresh_needed | fresh_source_generated_at_with_age_under_48h | 1 |
| refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | 37 |
| refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | 85 |
| restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | 15 |

## Top Source Refresh Batches

| Queue | Scope | Mode | Priority | Sources | Rows | Max age hours | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | network | P1 | 36 | 60556 | 538.29 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode network. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| restore_or_replace_unavailable_source_before_data_fill | exchange_directory | unavailable | P1 | 2 | 12036 | 1848.74 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_exchange_directory_before_identity_or_collision_work | exchange_directory | cache | P1 | 1 | 108 | 1848.74 | refresh_official_exchange_directory_before_identity_or_collision_work | official_exchange_directory_refresh_artifact_with_generated_at_and_row_count | Refresh the official exchange-directory source for scope exchange_directory using mode cache. | Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | network | P2 | 61 | 37906 | 538.29 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | listed_companies_subset | cache | P2 | 15 | 1177 | 1848.74 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | listed_companies_subset | unavailable | P2 | 11 | 20085 | 1848.74 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope listed_companies_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | network | P2 | 4 | 954 | 1848.74 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | exchange_directory | network | P2 | 2 | 13144 | 85.25 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope exchange_directory before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_identifier_registry_subset | unavailable | P2 | 1 | 4030 | 515.27 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_identifier_registry_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | interlisted_subset | network | P2 | 1 | 267 | 419.85 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope interlisted_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| restore_or_replace_unavailable_source_before_data_fill | security_lookup_subset | unavailable | P2 | 1 | 64 | 1848.74 | restore_or_replace_unavailable_source_before_data_fill | source_restored_or_replaced_with_official_or_documented_unavailable_decision | Restore the unavailable official source for scope security_lookup_subset, or document an official replacement/unavailable decision. | Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists. |
| refresh_official_subset_before_gap_enrichment | corporate_action_daily_list | network | P2 | 1 | 13 | 299.55 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope corporate_action_daily_list before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| refresh_official_subset_before_gap_enrichment | security_lookup_subset | cache | P2 | 1 | 7 | 1848.74 | refresh_official_subset_before_gap_enrichment | official_subset_refresh_artifact_with_generated_at_scope_and_row_count | Refresh the official subset source for scope security_lookup_subset before identifier or metadata gap work. | Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists. |
| fresh_no_refresh_needed | exchange_directory | network | P4 | 1 | 18016 | 14.65 | no_refresh_required | fresh_source_generated_at_with_age_under_48h | No refresh needed; retain current fresh source evidence for scope exchange_directory. | Freshness evidence is present; no data change is authorized by freshness alone. |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Recall % | Recall Gap % | Collision-Adjusted Recall % | Collision-Adjusted Missing | Recall Decision | Recall Exception | Verified on Covered |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 7 | 123 | 85 | 32 | 6 | 69.11 | 30.89 | 93.41 | 6 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 | 100.0 |
| AMS | official_full | 546 | 546 | 514 | 0 | 322 | 153 | 602 | 374 | 176 | 52 | 62.13 | 37.87 | 87.79 | 52 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=228;symbol_collisions=176 | 100.0 |
| ASX | official_partial | 2259 | 2090 | 2075 | 30 | 1147 | 101 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ATHEX | official_partial | 163 | 163 | 161 | 0 | 128 | 125 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| B3 | official_full | 1581 | 1571 | 1580 | 0 | 1252 | 0 | 1327 | 1220 | 0 | 107 | 91.94 | 8.06 | 91.94 | 107 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 | 100.0 |
| BATS | official_full | 1340 | 1314 | 1340 | 0 | 1048 | 243 | 1588 | 1245 | 56 | 287 | 78.4 | 21.6 | 81.27 | 287 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=343;symbol_collisions=56 | 100.0 |
| BCBA | official_partial | 92 | 92 | 69 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BHB | official_full | 28 | 28 | 28 | 0 | 26 | 7 | 41 | 28 | 9 | 4 | 68.29 | 31.71 | 87.5 | 4 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 | 100.0 |
| BIST | official_full | 614 | 614 | 614 | 0 | 614 | 550 | 647 | 611 | 21 | 15 | 94.44 | 5.56 | 97.6 | 15 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=21 | 100.0 |
| BK | official_full | 104 | 104 | 104 | 0 | 104 | 0 | 140 | 102 | 27 | 11 | 72.86 | 27.14 | 90.27 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 | 100.0 |
| BME | official_full | 276 | 276 | 266 | 3 | 220 | 212 | 50 | 18 | 0 | 32 | 36.0 | 64.0 | 36.0 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=0 | 100.0 |
| BMV | official_partial | 344 | 327 | 330 | 0 | 159 | 47 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 36 | 0 | 37 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_HU | official_partial | 50 | 50 | 50 | 0 | 41 | 6 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BSE_IN | official_full | 2732 | 2732 | 2641 | 0 | 2613 | 0 | 5077 | 2671 | 1843 | 563 | 52.61 | 47.39 | 82.59 | 563 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2406;symbol_collisions=1843 | 100.0 |
| BVB | official_full | 92 | 92 | 82 | 0 | 80 | 76 | 350 | 87 | 119 | 144 | 24.86 | 75.14 | 37.66 | 144 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=263;symbol_collisions=119 | 100.0 |
| BVC | official_partial | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| BVL | official_partial | 33 | 33 | 33 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Borsa Italiana | official_full | 278 | 278 | 278 | 0 | 276 | 275 | 2898 | 251 | 1855 | 792 | 8.66 | 91.34 | 24.07 | 792 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2647;symbol_collisions=1855 |  |
| Bursa | official_partial | 1039 | 1039 | 1036 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CPH | official_partial | 153 | 153 | 149 | 0 | 145 | 138 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 307 | 0 | 305 | 0 | 318 | 307 | 0 | 11 | 96.54 | 3.46 | 96.54 | 11 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 66 | 0 | 62 | 0 | 82 | 1 | 64 | 17 | 1.22 | 98.78 | 5.56 | 17 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=64 | 92.42 |
| DFM | official_full | 46 | 46 | 46 | 0 | 46 | 2 | 71 | 45 | 17 | 9 | 63.38 | 36.62 | 83.33 | 9 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 | 100.0 |
| DSE_TZ | official_partial | 17 | 17 | 17 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| EGX | official_partial | 223 | 223 | 223 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| Euronext | official_full | 1477 | 1477 | 1389 | 7 | 1071 | 844 | 2007 | 1341 | 366 | 300 | 66.82 | 33.18 | 81.72 | 300 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=666;symbol_collisions=366 | 100.0 |
| FSX | official_full | 8143 | 8143 | 6531 | 0 | 0 | 0 | 18016 | 8007 | 3945 | 6064 | 44.44 | 55.56 | 56.9 | 6064 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10009;symbol_collisions=3945 |  |
| GSE | official_partial | 19 | 18 | 19 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HEL | official_partial | 200 | 200 | 199 | 1 | 194 | 5 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| HKEX | official_full | 3058 | 3058 | 3043 | 0 | 3007 | 268 | 3200 | 3038 | 70 | 92 | 94.94 | 5.06 | 97.06 | 92 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=162;symbol_collisions=70 | 100.0 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 299 | 104 | 163 | 32 | 34.78 | 65.22 | 76.47 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=163 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 18 | 1 | 18 | 18 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| IDX | official_full | 756 | 694 | 756 | 0 | 577 | 0 | 962 | 756 | 187 | 19 | 78.59 | 21.41 | 97.55 | 19 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=187 | 100.0 |
| ISE | official_full | 14 | 14 | 14 | 0 | 12 | 9 | 15 | 9 | 6 | 0 | 60.0 | 40.0 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 | 100.0 |
| JSE | official_partial | 212 | 212 | 212 | 2 | 166 | 131 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| KOSDAQ | official_full | 1605 | 1605 | 1605 | 0 | 1578 | 0 | 1817 | 1596 | 3 | 218 | 87.84 | 12.16 | 87.98 | 218 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=3 | 99.62 |
| KRX | official_full | 1991 | 1990 | 1988 | 0 | 1793 | 0 | 2102 | 1960 | 14 | 128 | 93.24 | 6.76 | 93.87 | 128 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=142;symbol_collisions=14 | 99.76 |
| LSE | official_full | 7030 | 7029 | 6771 | 16 | 6499 | 4346 | 11092 | 6852 | 773 | 3467 | 61.77 | 38.23 | 66.4 | 3467 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4240;symbol_collisions=773 | 99.32 |
| LUSE | official_partial | 22 | 22 | 22 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 8 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| MSX | official_full | 91 | 91 | 91 | 0 | 0 | 0 | 108 | 91 | 14 | 3 | 84.26 | 15.74 | 96.81 | 3 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=14 | 100.0 |
| Munich | missing | 223 | 223 | 183 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| NASDAQ | official_full | 4764 | 4629 | 4628 | 3469 | 3390 | 1382 | 5622 | 4588 | 54 | 980 | 81.61 | 18.39 | 82.4 | 980 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1034;symbol_collisions=54 | 99.56 |
| NEO | official_full | 247 | 204 | 204 | 0 | 149 | 1 | 440 | 213 | 64 | 163 | 48.41 | 51.59 | 56.65 | 163 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=227;symbol_collisions=64 | 100.0 |
| NGX | official_full | 145 | 145 | 144 | 0 | 133 | 76 | 130 | 130 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 100.0 |
| NMFQS | official_partial | 6 | 6 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  |  |
| NSE_IN | official_full | 2503 | 2503 | 2503 | 0 | 2490 | 0 | 2978 | 2290 | 235 | 453 | 76.9 | 23.1 | 83.49 | 453 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=688;symbol_collisions=235 | 100.0 |
| NSE_KE | official_full | 46 | 46 | 46 | 0 | 42 | 1 | 68 | 11 | 25 | 32 | 16.18 | 83.82 | 25.58 | 32 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=57;symbol_collisions=25 | 100.0 |
| NYSE | official_full | 2020 | 1969 | 2000 | 1936 | 1440 | 994 | 3900 | 1987 | 569 | 1344 | 50.95 | 49.05 | 59.65 | 1344 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1913;symbol_collisions=569 | 100.0 |
| NYSE ARCA | official_full | 2722 | 2649 | 2667 | 113 | 2099 | 368 | 2707 | 2586 | 29 | 92 | 95.53 | 4.47 | 96.56 | 92 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=29 | 100.0 |
| NYSE MKT | official_full | 235 | 223 | 232 | 146 | 151 | 51 | 309 | 230 | 31 | 48 | 74.43 | 25.57 | 82.73 | 48 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=79;symbol_collisions=31 | 100.0 |
| NZX | official_full | 45 | 45 | 43 | 0 | 45 | 1 | 172 | 45 | 126 | 1 | 26.16 | 73.84 | 97.83 | 1 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=126 | 100.0 |
| OSL | official_full | 306 | 306 | 293 | 2 | 258 | 243 | 297 | 285 | 7 | 5 | 95.96 | 4.04 | 98.28 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=7 | 100.0 |
| OTC | official_full | 11753 | 11099 | 11127 | 2003 | 8845 | 2847 | 11925 | 8268 | 24 | 3633 | 69.33 | 30.67 | 69.47 | 3633 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3657;symbol_collisions=24 | 88.97 |
| PSE | official_full | 155 | 155 | 90 | 1 | 88 | 16 | 385 | 155 | 119 | 111 | 40.26 | 59.74 | 58.27 | 111 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=119 | 100.0 |
| PSE_CZ | official_partial | 27 | 27 | 26 | 0 | 23 | 21 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| PSX | official_full | 390 | 385 | 389 | 3 | 263 | 2 | 720 | 390 | 142 | 188 | 54.17 | 45.83 | 67.47 | 188 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=330;symbol_collisions=142 | 99.73 |
| QSE | official_full | 55 | 54 | 55 | 0 | 0 | 0 | 57 | 55 | 2 | 0 | 96.49 | 3.51 | 100.0 | 0 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 | 100.0 |
| RSE | official_partial | 2 | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SEM | official_full | 52 | 52 | 52 | 1 | 49 | 2 | 46 | 46 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 90.2 |
| SET | official_full | 779 | 771 | 779 | 4 | 335 | 4 | 944 | 775 | 132 | 37 | 82.1 | 17.9 | 95.44 | 37 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=169;symbol_collisions=132 | 100.0 |
| SGX | official_full | 613 | 613 | 578 | 0 | 8 | 18 | 746 | 611 | 122 | 13 | 81.9 | 18.1 | 97.92 | 13 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=135;symbol_collisions=122 | 99.63 |
| SIX | official_partial | 1263 | 1263 | 1259 | 2 | 756 | 348 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE | official_partial | 2795 | 2760 | 2795 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SSE_CL | official_full | 129 | 102 | 120 | 0 | 85 | 1 | 111 | 111 | 0 | 0 | 100.0 | 0.0 | 100.0 | 0 | fixed |  | 98.97 |
| STO | official_partial | 878 | 878 | 875 | 2 | 825 | 805 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| SZSE | official_partial | 3150 | 3138 | 3150 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TADAWUL | official_full | 199 | 199 | 191 | 0 | 191 | 0 | 412 | 198 | 209 | 5 | 48.06 | 51.94 | 97.54 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=209 | 100.0 |
| TASE | official_partial | 801 | 801 | 704 | 0 | 670 | 14 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TPEX | official_partial | 1119 | 1119 | 1119 | 0 | 917 | 2 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| TSE | official_full | 4077 | 4067 | 4074 | 0 | 4060 | 485 | 4437 | 4053 | 334 | 50 | 91.35 | 8.65 | 98.78 | 50 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=384;symbol_collisions=334 | 100.0 |
| TSX | official_full | 2296 | 2219 | 1984 | 12 | 1621 | 40 | 785 | 590 | 190 | 5 | 75.16 | 24.84 | 99.16 | 5 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=190 | 99.32 |
| TSXV | official_full | 1422 | 1325 | 1069 | 17 | 911 | 9 | 1518 | 1322 | 194 | 2 | 87.09 | 12.91 | 99.85 | 2 | mostly_collision_hidden | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=194 | 92.78 |
| TWSE | official_full | 1239 | 1239 | 1191 | 0 | 1165 | 3 | 1093 | 1015 | 30 | 48 | 92.86 | 7.14 | 95.48 | 48 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=78;symbol_collisions=30 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 822 | 2 | 523 | 297 | 0.24 | 99.76 | 0.67 | 297 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=523 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 7 | 0 | 7 | 7 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| VSE | official_partial | 88 | 88 | 82 | 0 | 54 | 50 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| WSE | official_partial | 582 | 582 | 574 | 7 | 540 | 521 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| XDUS | missing | 199 | 199 | 170 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XETRA | official_full | 4315 | 4314 | 4216 | 8 | 3828 | 1925 | 5080 | 4113 | 700 | 267 | 80.96 | 19.04 | 93.9 | 267 | still_actionable | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=967;symbol_collisions=700 | 99.88 |
| XHAM | missing | 12 | 12 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XHAN | missing | 80 | 80 | 70 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| XSTU | missing | 2773 | 2773 | 2397 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | source_unavailable |  |  |
| ZSE | official_partial | 23 | 23 | 23 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 27 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  |  |  | 0 | out_of_current_scope |  | 100.0 |

## Per-Exchange Recall Exceptions

| Exchange | Recall % | Collision-Adjusted Recall % | Official Rows | Missing Or Collision-Hidden | True Missing Excluding Collisions | Collision-Hidden | Decision | Next Action | Exception |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| UPCOM | 0.24 | 0.67 | 822 | 820 | 297 | 523 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=820;symbol_collisions=523 |
| CSE_MA | 1.22 | 5.56 | 82 | 81 | 17 | 64 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=81;symbol_collisions=64 |
| Borsa Italiana | 8.66 | 24.07 | 2898 | 2647 | 792 | 1855 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2647;symbol_collisions=1855 |
| NSE_KE | 16.18 | 25.58 | 68 | 57 | 32 | 25 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=57;symbol_collisions=25 |
| BVB | 24.86 | 37.66 | 350 | 263 | 144 | 119 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=263;symbol_collisions=119 |
| NZX | 26.16 | 97.83 | 172 | 127 | 1 | 126 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=127;symbol_collisions=126 |
| HNX | 34.78 | 76.47 | 299 | 195 | 32 | 163 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=163 |
| BME | 36.0 | 36.0 | 50 | 32 | 32 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=32;symbol_collisions=0 |
| PSE | 40.26 | 58.27 | 385 | 230 | 111 | 119 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=230;symbol_collisions=119 |
| FSX | 44.44 | 56.9 | 18016 | 10009 | 6064 | 3945 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=10009;symbol_collisions=3945 |
| TADAWUL | 48.06 | 97.54 | 412 | 214 | 5 | 209 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=214;symbol_collisions=209 |
| NEO | 48.41 | 56.65 | 440 | 227 | 163 | 64 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=227;symbol_collisions=64 |
| NYSE | 50.95 | 59.65 | 3900 | 1913 | 1344 | 569 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1913;symbol_collisions=569 |
| BSE_IN | 52.61 | 82.59 | 5077 | 2406 | 563 | 1843 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2406;symbol_collisions=1843 |
| PSX | 54.17 | 67.47 | 720 | 330 | 188 | 142 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=330;symbol_collisions=142 |
| ISE | 60.0 | 100.0 | 15 | 6 | 0 | 6 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=6;symbol_collisions=6 |
| LSE | 61.77 | 66.4 | 11092 | 4240 | 3467 | 773 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=4240;symbol_collisions=773 |
| AMS | 62.13 | 87.79 | 602 | 228 | 52 | 176 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=228;symbol_collisions=176 |
| DFM | 63.38 | 83.33 | 71 | 26 | 9 | 17 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=26;symbol_collisions=17 |
| Euronext | 66.82 | 81.72 | 2007 | 666 | 300 | 366 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=666;symbol_collisions=366 |
| BHB | 68.29 | 87.5 | 41 | 13 | 4 | 9 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=13;symbol_collisions=9 |
| ADX | 69.11 | 93.41 | 123 | 38 | 6 | 32 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=32 |
| OTC | 69.33 | 69.47 | 11925 | 3657 | 3633 | 24 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=3657;symbol_collisions=24 |
| BK | 72.86 | 90.27 | 140 | 38 | 11 | 27 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=38;symbol_collisions=27 |
| NYSE MKT | 74.43 | 82.73 | 309 | 79 | 48 | 31 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=79;symbol_collisions=31 |
| TSX | 75.16 | 99.16 | 785 | 195 | 5 | 190 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=195;symbol_collisions=190 |
| NSE_IN | 76.9 | 83.49 | 2978 | 688 | 453 | 235 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=688;symbol_collisions=235 |
| BATS | 78.4 | 81.27 | 1588 | 343 | 287 | 56 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=343;symbol_collisions=56 |
| IDX | 78.59 | 97.55 | 962 | 206 | 19 | 187 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=206;symbol_collisions=187 |
| XETRA | 80.96 | 93.9 | 5080 | 967 | 267 | 700 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=967;symbol_collisions=700 |
| NASDAQ | 81.61 | 82.4 | 5622 | 1034 | 980 | 54 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=1034;symbol_collisions=54 |
| SGX | 81.9 | 97.92 | 746 | 135 | 13 | 122 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=135;symbol_collisions=122 |
| SET | 82.1 | 95.44 | 944 | 169 | 37 | 132 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=169;symbol_collisions=132 |
| MSX | 84.26 | 96.81 | 108 | 17 | 3 | 14 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=17;symbol_collisions=14 |
| TSXV | 87.09 | 99.85 | 1518 | 196 | 2 | 194 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=196;symbol_collisions=194 |
| KOSDAQ | 87.84 | 87.98 | 1817 | 221 | 218 | 3 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=221;symbol_collisions=3 |
| TSE | 91.35 | 98.78 | 4437 | 384 | 50 | 334 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=384;symbol_collisions=334 |
| B3 | 91.94 | 91.94 | 1327 | 107 | 107 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=107;symbol_collisions=0 |
| TWSE | 92.86 | 95.48 | 1093 | 78 | 48 | 30 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=78;symbol_collisions=30 |
| KRX | 93.24 | 93.87 | 2102 | 142 | 128 | 14 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=142;symbol_collisions=14 |
| BIST | 94.44 | 97.6 | 647 | 36 | 15 | 21 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=36;symbol_collisions=21 |
| HKEX | 94.94 | 97.06 | 3200 | 162 | 92 | 70 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=162;symbol_collisions=70 |
| NYSE ARCA | 95.53 | 96.56 | 2707 | 121 | 92 | 29 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=121;symbol_collisions=29 |
| OSL | 95.96 | 98.28 | 297 | 12 | 5 | 7 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=12;symbol_collisions=7 |
| QSE | 96.49 | 100.0 | 57 | 2 | 0 | 2 | mostly_collision_hidden | review collision-hidden rows separately and prioritize the remaining true missing symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=2;symbol_collisions=2 |
| CSE_LK | 96.54 | 96.54 | 318 | 11 | 11 | 0 | still_actionable | repair parser/source coverage or add reviewed official evidence for true missing active symbols. | below_99_5_active_official_masterfile_recall;missing_or_collision_hidden=11;symbol_collisions=0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 66 | 66 | 63 | 0 | 56 | 0 |
| Australia | 1518 | 1413 | 1438 | 59 | 1184 | 118 |
| Austria | 49 | 49 | 48 | 1 | 41 | 39 |
| Bahamas | 5 | 5 | 5 | 2 | 5 | 0 |
| Bahrain | 29 | 29 | 29 | 0 | 27 | 7 |
| Belgium | 129 | 128 | 128 | 6 | 113 | 110 |
| Bermuda | 533 | 533 | 528 | 57 | 478 | 122 |
| Botswana | 24 | 24 | 24 | 0 | 24 | 0 |
| Brazil | 1596 | 1586 | 1595 | 13 | 1259 | 2 |
| British Virgin Islands | 152 | 152 | 148 | 96 | 94 | 24 |
| Bulgaria | 11 | 11 | 11 | 1 | 11 | 0 |
| Canada | 4451 | 4247 | 3980 | 293 | 3270 | 34 |
| Cayman Islands | 2241 | 2232 | 2219 | 576 | 1882 | 247 |
| Chile | 116 | 89 | 116 | 3 | 84 | 2 |
| China | 6397 | 6344 | 6394 | 4 | 5245 | 10 |
| Colombia | 3 | 3 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 23 | 0 | 23 | 0 |
| Cyprus | 19 | 18 | 18 | 1 | 11 | 0 |
| Czech Republic | 23 | 23 | 23 | 0 | 22 | 20 |
| Denmark | 157 | 157 | 152 | 6 | 146 | 136 |
| Egypt | 231 | 230 | 231 | 0 | 201 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 2 | 2 |
| Finland | 200 | 200 | 198 | 2 | 193 | 3 |
| France | 704 | 701 | 692 | 14 | 663 | 650 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 1 |
| Germany | 837 | 832 | 784 | 10 | 760 | 697 |
| Ghana | 19 | 18 | 19 | 0 | 17 | 0 |
| Gibraltar | 3 | 3 | 3 | 1 | 3 | 2 |
| Greece | 140 | 140 | 138 | 1 | 120 | 116 |
| Guernsey | 68 | 68 | 65 | 4 | 56 | 56 |
| Hong Kong | 472 | 469 | 468 | 0 | 461 | 5 |
| Hungary | 37 | 36 | 37 | 0 | 29 | 0 |
| Iceland | 18 | 18 | 18 | 1 | 18 | 18 |
| India | 5067 | 5067 | 5022 | 0 | 4983 | 4 |
| Indonesia | 712 | 708 | 699 | 1 | 578 | 0 |
| Ireland | 2530 | 2520 | 2490 | 37 | 2434 | 861 |
| Isle of Man | 13 | 13 | 13 | 1 | 13 | 11 |
| Israel | 754 | 752 | 745 | 73 | 703 | 2 |
| Italy | 240 | 239 | 228 | 1 | 222 | 215 |
| Japan | 3398 | 3393 | 3338 | 20 | 3324 | 446 |
| Jersey | 172 | 172 | 168 | 14 | 159 | 158 |
| Kazakhstan | 1 | 1 | 1 | 0 | 1 | 0 |
| Kenya | 44 | 44 | 44 | 0 | 41 | 0 |
| Kuwait | 102 | 102 | 102 | 0 | 102 | 0 |
| Liechtenstein | 3 | 3 | 3 | 0 | 3 | 3 |
| Lithuania | 9 | 9 | 2 | 0 | 2 | 2 |
| Luxembourg | 1027 | 1024 | 1008 | 14 | 995 | 2 |
| Malawi | 8 | 8 | 8 | 0 | 7 | 0 |
| Malaysia | 974 | 974 | 974 | 0 | 933 | 1 |
| Malta | 6 | 6 | 6 | 0 | 6 | 6 |
| Marshall Islands | 42 | 42 | 40 | 34 | 26 | 20 |
| Mauritius | 61 | 61 | 60 | 2 | 54 | 0 |
| Mexico | 135 | 117 | 132 | 6 | 109 | 2 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 66 | 0 | 62 | 0 |
| Netherlands | 192 | 192 | 186 | 28 | 172 | 127 |
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
| Singapore | 554 | 550 | 540 | 15 | 40 | 5 |
| Slovenia | 8 | 8 | 1 | 0 | 1 | 1 |
| South Africa | 252 | 252 | 227 | 9 | 175 | 141 |
| South Korea | 3585 | 3583 | 3582 | 1 | 3362 | 0 |
| Spain | 223 | 223 | 221 | 8 | 209 | 205 |
| Sri Lanka | 307 | 307 | 307 | 0 | 305 | 0 |
| Sweden | 815 | 810 | 788 | 5 | 769 | 767 |
| Switzerland | 370 | 370 | 368 | 21 | 345 | 293 |
| Taiwan | 2277 | 2276 | 2276 | 1 | 2056 | 1 |
| Tanzania | 15 | 15 | 15 | 0 | 13 | 0 |
| Thailand | 624 | 624 | 567 | 6 | 332 | 1 |
| Turkey | 619 | 619 | 619 | 0 | 619 | 553 |
| Uganda | 7 | 7 | 7 | 0 | 7 | 7 |
| United Arab Emirates | 123 | 123 | 123 | 0 | 123 | 0 |
| United Kingdom | 1308 | 1304 | 1276 | 45 | 1215 | 1025 |
| United States | 14751 | 13937 | 13919 | 5261 | 10701 | 3768 |
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
