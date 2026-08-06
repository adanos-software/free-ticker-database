# Improvement Baseline

Generated: `2026-08-06T14:13:29Z`

Baseline snapshot for future before/after deltas. It does not authorize inferred metadata changes.

## Summary

| Metric | Value |
|---|---:|
| global_metric_count | `16` |
| campaign_count | `10` |
| exchange_count | `87` |
| source_file_count | `18` |
| baseline_context | `"metric_count=16;tickers=63748;listing_keys=91927;source_gap_rows=9847;warn_rows=278;quarantine_rows=0;validation_failed_error_gates=2"` |

Global context: `metric_count=16;tickers=63748;listing_keys=91927;source_gap_rows=9847;warn_rows=278;quarantine_rows=0;validation_failed_error_gates=2`

## Global

| Metric | Value |
|---|---:|
| tickers | `63748` |
| listing_keys | `91927` |
| isin_coverage | `62309` |
| sector_coverage | `61482` |
| stock_sector_coverage | `45881` |
| etf_category_coverage | `15601` |
| figi_coverage | `65431` |
| official_masterfile_matches | `56811` |
| official_masterfile_collisions | `9665` |
| official_masterfile_missing | `14077` |
| source_gap_rows | `9847` |
| entry_quality_warn_rows | `278` |
| entry_quality_source_gap_rows | `20869` |
| entry_quality_quarantine_rows | `0` |
| validation_failed_error_gates | `2` |
| source_freshness_status_totals | `{"fresh": 20, "old": 50, "stale": 67}` |

## Campaign Baseline

### b3

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=b3;metric_count=2;nested_metric_count=0;numeric_row_total=11` |
| missing_isin_residual_rows | `10` |
| missing_sector_residual_rows | `1` |

### otc

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=otc;metric_count=3;nested_metric_count=0;numeric_row_total=15499` |
| scope_review_rows | `11754` |
| accepted_source_gap_rows | `3745` |
| drop_override_rows_still_present | `0` |

### canada

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=canada;metric_count=5;nested_metric_count=0;numeric_row_total=1217` |
| residual_rows | `457` |
| figi_queue_rows | `0` |
| missing_isin_rows | `229` |
| missing_figi_rows | `380` |
| reviewed_openfigi_source_gap_rows | `151` |

### asx

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=asx;metric_count=2;nested_metric_count=1;numeric_row_total=114` |
| residual_rows | `114` |
| field_totals | `{"missing_etf_category": 9, "missing_isin_primary": 105}` |

### weak_sector

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=weak_sector;metric_count=2;nested_metric_count=1;numeric_row_total=646` |
| residual_rows | `646` |
| exchange_totals | `{"BK": 102, "CSE_LK": 143, "CSE_MA": 64, "Euronext": 132, "NGX": 24, "OSL": 58, "PSE": 76, "SEM": 47}` |

### masterfile_collisions

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=masterfile_collisions;metric_count=4;nested_metric_count=3;numeric_row_total=11176` |
| review_rows | `11176` |
| decision_totals | `{"new_listing_candidate_requires_official_listing_add_review": 2789, "same_isin_cross_listing_candidate_requires_exchange_scope_review": 4613, "symbol_collision_requires_non_symbol_identity_source": 3774}` |
| review_bucket_totals | `{"distinct_official_isin_new_listing_candidate": 2196, "hold_symbol_only_collision_needs_non_symbol_identity": 3190, "resolve_asset_type_conflict_before_identity_review": 1620, "same_isin_cross_listing_needs_name_or_scope_review": 3839, "same_isin_exact_name_cross_listing_candidate": 331}` |
| review_priority_totals | `{"P1": 331, "P2": 7655, "P3": 3190}` |

### symbol_changes

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=symbol_changes;metric_count=3;nested_metric_count=2;numeric_row_total=319` |
| review_rows | `319` |
| exchange_scope_status_counts | `{"global_symbol_collision_outside_source_scope": 64, "matches_within_source_scope": 250, "unscoped_source_hint": 5}` |
| review_bucket_counts | `{"action_required_duplicate_or_cross_listing": 30, "action_required_possible_rename_or_delisting": 2, "already_reflected_in_scope_with_global_symbol_collision": 43, "already_reflected_in_source_scope": 203, "hold_out_of_scope_symbol_collision": 14, "manual_review_due_to_out_of_scope_collision": 7, "manual_scope_mapping_required": 5, "no_dataset_match_for_source_scope": 15}` |

### ohlcv

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=ohlcv;metric_count=4;nested_metric_count=2;numeric_row_total=263` |
| sample_rows | `143` |
| status_counts | `{"not_checked": 3, "notice": 35, "pass": 33, "source_gap": 28, "warn": 44}` |
| warning_review_rows | `120` |
| warning_review_authorization_counts | `{"blocked_until_official_listing_keyed_review": 120}` |

### freshness

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=freshness;metric_count=14;nested_metric_count=8;numeric_row_total=11016` |
| source_count | `137` |
| source_freshness_status_totals | `{"fresh": 20, "old": 50, "stale": 67}` |
| source_refresh_priority_totals | `{"P1": 13, "P2": 104, "P4": 20}` |
| source_refresh_queue_priority_totals | `{"fresh_no_refresh_needed": {"P4": 20}, "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 11}, "refresh_official_subset_before_gap_enrichment": {"P2": 91}, "restore_or_replace_unavailable_source_before_data_fill": {"P1": 2, "P2": 13}}` |
| source_refresh_action_totals | `{"no_refresh_needed": 20, "refresh_official_exchange_directory_before_identity_or_collision_work": 11, "refresh_official_subset_before_gap_enrichment": 91, "restore_or_replace_unavailable_source_before_data_fill": 15}` |
| old_official_exchange_directory_count | `13` |
| source_gap_rows | `9847` |
| source_gap_class_totals | `{"adr_cdr_or_depositary_identifier_gap": 43, "adr_cdr_or_depositary_sector_gap": 15, "capital_pool_or_halted_identifier_gap": 33, "commodity_etf_category_gap": 14, "debt_or_securitized_identifier_gap": 76, "digital_asset_etf_category_gap": 9, "equity_etf_category_gap": 83, "exchange_industry_source_gap": 922, "fixed_income_etf_category_gap": 29, "fund_or_trust_identifier_gap": 363, "fundlike_stock_sector_gap": 11, "generic_etf_category_source_gap": 3, "inactive_or_legacy_identifier_gap": 91, "official_current_directory_absent_identifier_gap": 2, "official_identifier_not_exposed_source_gap": 484, "official_identifier_reference_unmatched_gap": 10, "official_identifier_source_gap": 9, "official_industry_taxonomy_unavailable_gap": 357, "official_product_reference_unmatched_category_gap": 16, "official_product_taxonomy_unavailable_gap": 222, "official_reference_symbol_collision_gap": 1236, "official_reference_unmatched_source_gap": 5234, "otc_sector_source_gap": 556, "shell_or_cpc_sector_gap": 29}` |
| top_source_gap_review_batches | `20` ranked batches |

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `OTC` | 3119 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_sector_stock` | `exchange_industry_source_gap` | `FSX` | 859 | Official exchange industry feed or reviewed secondary company profile. | Exact exchange/symbol/name mapped to canonical stock sector. |
| `missing_sector_stock` | `otc_sector_source_gap` | `OTC` | 556 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `B3` | 331 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `NSE_IN` | 174 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `AMS` | 168 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `BMV` | 165 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BMV` | 162 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `SET` | 140 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `OTC` | 139 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `NASDAQ` | 125 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `NYSE ARCA` | 104 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `BATS` | 103 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `XETRA` | 102 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `TSX` | 96 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `NASDAQ` | 95 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `TSX` | 91 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BME` | 88 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `TSXV` | 86 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `LSE` | 85 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |

| Metric | Value |
|---|---:|
| symbol_changes_review_rows | `319` |
| ohlcv_plausibility_rows | `143` |
| financialdata_supplement_rows | `557` |
| financialdata_apply_eligibility_counts | `{"blocked_until_exchange_scope_explicitly_allowed": 91, "blocked_until_unique_official_isin_candidate_resolved": 163, "keep_absent_until_name_gated_official_isin_match": 169, "no_supplement_apply_existing_identifier_or_collision_guard": 199, "preserve_existing_reviewed_supplement_no_new_apply": 43}` |
| financialdata_verification_evidence_required_counts | `{"existing_database_isin_confirms_no_supplement_needed_or_cross_listing_review": 16, "existing_listing_key_confirms_no_supplement_needed": 33, "existing_reviewed_supplement_retained_with_original_official_source": 43, "explicit_exchange_scope_decision_before_financialdata_discovery_use": 91, "identity_resolution_before_any_global_ticker_reuse": 150, "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 169, "single_official_active_listing_with_valid_isin_and_name_gate": 163}` |

### baseline

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=baseline;metric_count=5;nested_metric_count=0;numeric_row_total=104` |
| tracked_campaigns | `10` |
| global_metric_count | `16` |
| exchange_baseline_enabled | `1` |
| baseline_snapshot_rows | `1` |
| exchange_count | `87` |


## Exchange Baseline

| Exchange | Tickers | ISIN | Sector | Source Gaps | Warns | Quality Source Gaps | Quarantine | Review Context |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| ADX | 86 | 86 | 86 | 1 | 0 | 1 | 0 | `exchange=ADX;tickers=86;isin_coverage=86;sector_coverage=86;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| AMS | 546 | 546 | 265 | 182 | 2 | 183 | 0 | `exchange=AMS;tickers=546;isin_coverage=546;sector_coverage=265;source_gap_rows=182;warn_rows=2;quality_source_gap_rows=183;quarantine_rows=0` |
| ASX | 2259 | 2090 | 2072 | 261 | 4 | 387 | 0 | `exchange=ASX;tickers=2259;isin_coverage=2090;sector_coverage=2072;source_gap_rows=261;warn_rows=4;quality_source_gap_rows=387;quarantine_rows=0` |
| ATHEX | 163 | 163 | 155 | 45 | 0 | 45 | 0 | `exchange=ATHEX;tickers=163;isin_coverage=163;sector_coverage=155;source_gap_rows=45;warn_rows=0;quality_source_gap_rows=45;quarantine_rows=0` |
| B3 | 1581 | 1571 | 1579 | 342 | 0 | 331 | 0 | `exchange=B3;tickers=1581;isin_coverage=1571;sector_coverage=1579;source_gap_rows=342;warn_rows=0;quality_source_gap_rows=331;quarantine_rows=0` |
| BATS | 1329 | 1224 | 1228 | 283 | 3 | 192 | 0 | `exchange=BATS;tickers=1329;isin_coverage=1224;sector_coverage=1228;source_gap_rows=283;warn_rows=3;quality_source_gap_rows=192;quarantine_rows=0` |
| BCBA | 92 | 92 | 63 | 36 | 0 | 29 | 0 | `exchange=BCBA;tickers=92;isin_coverage=92;sector_coverage=63;source_gap_rows=36;warn_rows=0;quality_source_gap_rows=29;quarantine_rows=0` |
| BHB | 28 | 28 | 28 | 0 | 0 | 0 | 0 | `exchange=BHB;tickers=28;isin_coverage=28;sector_coverage=28;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BIST | 614 | 614 | 614 | 3 | 0 | 3 | 0 | `exchange=BIST;tickers=614;isin_coverage=614;sector_coverage=614;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| BK | 104 | 104 | 103 | 2 | 0 | 2 | 0 | `exchange=BK;tickers=104;isin_coverage=104;sector_coverage=103;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| BME | 276 | 276 | 266 | 147 | 3 | 146 | 0 | `exchange=BME;tickers=276;isin_coverage=276;sector_coverage=266;source_gap_rows=147;warn_rows=3;quality_source_gap_rows=146;quarantine_rows=0` |
| BMV | 344 | 327 | 178 | 346 | 0 | 344 | 0 | `exchange=BMV;tickers=344;isin_coverage=327;sector_coverage=178;source_gap_rows=346;warn_rows=0;quality_source_gap_rows=344;quarantine_rows=0` |
| BSE_BW | 39 | 39 | 36 | 13 | 0 | 13 | 0 | `exchange=BSE_BW;tickers=39;isin_coverage=39;sector_coverage=36;source_gap_rows=13;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| BSE_HU | 50 | 50 | 47 | 30 | 2 | 28 | 0 | `exchange=BSE_HU;tickers=50;isin_coverage=50;sector_coverage=47;source_gap_rows=30;warn_rows=2;quality_source_gap_rows=28;quarantine_rows=0` |
| BSE_IN | 2732 | 2732 | 2636 | 110 | 0 | 156 | 0 | `exchange=BSE_IN;tickers=2732;isin_coverage=2732;sector_coverage=2636;source_gap_rows=110;warn_rows=0;quality_source_gap_rows=156;quarantine_rows=0` |
| BVB | 92 | 92 | 80 | 0 | 0 | 10 | 0 | `exchange=BVB;tickers=92;isin_coverage=92;sector_coverage=80;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=10;quarantine_rows=0` |
| BVC | 3 | 3 | 3 | 0 | 0 | 0 | 0 | `exchange=BVC;tickers=3;isin_coverage=3;sector_coverage=3;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BVL | 33 | 33 | 33 | 2 | 0 | 2 | 0 | `exchange=BVL;tickers=33;isin_coverage=33;sector_coverage=33;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| Borsa Italiana | 278 | 278 | 278 | 28 | 0 | 28 | 0 | `exchange=Borsa Italiana;tickers=278;isin_coverage=278;sector_coverage=278;source_gap_rows=28;warn_rows=0;quality_source_gap_rows=28;quarantine_rows=0` |
| Bursa | 1039 | 1039 | 1036 | 0 | 0 | 3 | 0 | `exchange=Bursa;tickers=1039;isin_coverage=1039;sector_coverage=1036;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| CPH | 153 | 153 | 149 | 17 | 0 | 15 | 0 | `exchange=CPH;tickers=153;isin_coverage=153;sector_coverage=149;source_gap_rows=17;warn_rows=0;quality_source_gap_rows=15;quarantine_rows=0` |
| CSE_LK | 307 | 307 | 307 | 0 | 0 | 0 | 0 | `exchange=CSE_LK;tickers=307;isin_coverage=307;sector_coverage=307;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| CSE_MA | 66 | 66 | 66 | 65 | 0 | 65 | 0 | `exchange=CSE_MA;tickers=66;isin_coverage=66;sector_coverage=66;source_gap_rows=65;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| DFM | 46 | 46 | 46 | 1 | 0 | 1 | 0 | `exchange=DFM;tickers=46;isin_coverage=46;sector_coverage=46;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| DSE_TZ | 17 | 17 | 15 | 0 | 0 | 0 | 0 | `exchange=DSE_TZ;tickers=17;isin_coverage=17;sector_coverage=15;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| EGX | 223 | 223 | 222 | 32 | 0 | 32 | 0 | `exchange=EGX;tickers=223;isin_coverage=223;sector_coverage=222;source_gap_rows=32;warn_rows=0;quality_source_gap_rows=32;quarantine_rows=0` |
| Euronext | 1477 | 1476 | 996 | 158 | 21 | 235 | 0 | `exchange=Euronext;tickers=1477;isin_coverage=1476;sector_coverage=996;source_gap_rows=158;warn_rows=21;quality_source_gap_rows=235;quarantine_rows=0` |
| FSX | 8143 | 8141 | 0 | 894 | 62 | 8081 | 0 | `exchange=FSX;tickers=8143;isin_coverage=8141;sector_coverage=0;source_gap_rows=894;warn_rows=62;quality_source_gap_rows=8081;quarantine_rows=0` |
| GSE | 19 | 18 | 18 | 2 | 0 | 2 | 0 | `exchange=GSE;tickers=19;isin_coverage=18;sector_coverage=18;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| HEL | 200 | 200 | 198 | 12 | 0 | 11 | 0 | `exchange=HEL;tickers=200;isin_coverage=200;sector_coverage=198;source_gap_rows=12;warn_rows=0;quality_source_gap_rows=11;quarantine_rows=0` |
| HKEX | 3058 | 3055 | 3013 | 24 | 0 | 38 | 0 | `exchange=HKEX;tickers=3058;isin_coverage=3055;sector_coverage=3013;source_gap_rows=24;warn_rows=0;quality_source_gap_rows=38;quarantine_rows=0` |
| HNX | 105 | 105 | 105 | 1 | 0 | 1 | 0 | `exchange=HNX;tickers=105;isin_coverage=105;sector_coverage=105;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| HOSE | 153 | 153 | 153 | 1 | 0 | 1 | 0 | `exchange=HOSE;tickers=153;isin_coverage=153;sector_coverage=153;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| ICE_IS | 18 | 18 | 18 | 1 | 0 | 1 | 0 | `exchange=ICE_IS;tickers=18;isin_coverage=18;sector_coverage=18;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| IDX | 756 | 694 | 756 | 62 | 0 | 62 | 0 | `exchange=IDX;tickers=756;isin_coverage=694;sector_coverage=756;source_gap_rows=62;warn_rows=0;quality_source_gap_rows=62;quarantine_rows=0` |
| ISE | 14 | 14 | 14 | 5 | 0 | 5 | 0 | `exchange=ISE;tickers=14;isin_coverage=14;sector_coverage=14;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| JSE | 212 | 212 | 212 | 87 | 0 | 87 | 0 | `exchange=JSE;tickers=212;isin_coverage=212;sector_coverage=212;source_gap_rows=87;warn_rows=0;quality_source_gap_rows=87;quarantine_rows=0` |
| KOSDAQ | 1605 | 1605 | 1605 | 9 | 0 | 9 | 0 | `exchange=KOSDAQ;tickers=1605;isin_coverage=1605;sector_coverage=1605;source_gap_rows=9;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| KRX | 1991 | 1990 | 1988 | 36 | 0 | 32 | 0 | `exchange=KRX;tickers=1991;isin_coverage=1990;sector_coverage=1988;source_gap_rows=36;warn_rows=0;quality_source_gap_rows=32;quarantine_rows=0` |
| LSE | 7030 | 7029 | 6266 | 228 | 24 | 483 | 0 | `exchange=LSE;tickers=7030;isin_coverage=7029;sector_coverage=6266;source_gap_rows=228;warn_rows=24;quality_source_gap_rows=483;quarantine_rows=0` |
| LUSE | 22 | 22 | 22 | 7 | 0 | 7 | 0 | `exchange=LUSE;tickers=22;isin_coverage=22;sector_coverage=22;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| MSE_MW | 8 | 8 | 8 | 0 | 0 | 0 | 0 | `exchange=MSE_MW;tickers=8;isin_coverage=8;sector_coverage=8;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| MSX | 91 | 91 | 91 | 0 | 0 | 0 | 0 | `exchange=MSX;tickers=91;isin_coverage=91;sector_coverage=91;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| Munich | 223 | 223 | 0 | 10 | 0 | 223 | 0 | `exchange=Munich;tickers=223;isin_coverage=223;sector_coverage=0;source_gap_rows=10;warn_rows=0;quality_source_gap_rows=223;quarantine_rows=0` |
| NASDAQ | 4742 | 4597 | 4603 | 437 | 8 | 363 | 0 | `exchange=NASDAQ;tickers=4742;isin_coverage=4597;sector_coverage=4603;source_gap_rows=437;warn_rows=8;quality_source_gap_rows=363;quarantine_rows=0` |
| NEO | 247 | 204 | 191 | 87 | 1 | 104 | 0 | `exchange=NEO;tickers=247;isin_coverage=204;sector_coverage=191;source_gap_rows=87;warn_rows=1;quality_source_gap_rows=104;quarantine_rows=0` |
| NGX | 145 | 145 | 144 | 15 | 0 | 15 | 0 | `exchange=NGX;tickers=145;isin_coverage=145;sector_coverage=144;source_gap_rows=15;warn_rows=0;quality_source_gap_rows=15;quarantine_rows=0` |
| NMFQS | 6 | 6 | 6 | 0 | 0 | 0 | 0 | `exchange=NMFQS;tickers=6;isin_coverage=6;sector_coverage=6;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| NSE_IN | 2503 | 2503 | 2503 | 213 | 0 | 213 | 0 | `exchange=NSE_IN;tickers=2503;isin_coverage=2503;sector_coverage=2503;source_gap_rows=213;warn_rows=0;quality_source_gap_rows=213;quarantine_rows=0` |
| NSE_KE | 46 | 46 | 45 | 35 | 0 | 35 | 0 | `exchange=NSE_KE;tickers=46;isin_coverage=46;sector_coverage=45;source_gap_rows=35;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| NYSE | 2017 | 1968 | 1994 | 101 | 6 | 85 | 0 | `exchange=NYSE;tickers=2017;isin_coverage=1968;sector_coverage=1994;source_gap_rows=101;warn_rows=6;quality_source_gap_rows=85;quarantine_rows=0` |
| NYSE ARCA | 2709 | 2625 | 2628 | 272 | 1 | 225 | 0 | `exchange=NYSE ARCA;tickers=2709;isin_coverage=2625;sector_coverage=2628;source_gap_rows=272;warn_rows=1;quality_source_gap_rows=225;quarantine_rows=0` |
| NYSE MKT | 233 | 223 | 227 | 15 | 1 | 12 | 0 | `exchange=NYSE MKT;tickers=233;isin_coverage=223;sector_coverage=227;source_gap_rows=15;warn_rows=1;quality_source_gap_rows=12;quarantine_rows=0` |
| NZX | 45 | 45 | 42 | 0 | 0 | 0 | 0 | `exchange=NZX;tickers=45;isin_coverage=45;sector_coverage=42;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| OSL | 306 | 306 | 261 | 23 | 5 | 51 | 0 | `exchange=OSL;tickers=306;isin_coverage=306;sector_coverage=261;source_gap_rows=23;warn_rows=5;quality_source_gap_rows=51;quarantine_rows=0` |
| OTC | 11754 | 11100 | 10843 | 3814 | 108 | 3256 | 0 | `exchange=OTC;tickers=11754;isin_coverage=11100;sector_coverage=10843;source_gap_rows=3814;warn_rows=108;quality_source_gap_rows=3256;quarantine_rows=0` |
| PSE | 155 | 155 | 87 | 7 | 1 | 64 | 0 | `exchange=PSE;tickers=155;isin_coverage=155;sector_coverage=87;source_gap_rows=7;warn_rows=1;quality_source_gap_rows=64;quarantine_rows=0` |
| PSE_CZ | 27 | 27 | 26 | 5 | 0 | 6 | 0 | `exchange=PSE_CZ;tickers=27;isin_coverage=27;sector_coverage=26;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| PSX | 390 | 385 | 389 | 5 | 0 | 6 | 0 | `exchange=PSX;tickers=390;isin_coverage=385;sector_coverage=389;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| QSE | 55 | 54 | 55 | 1 | 0 | 1 | 0 | `exchange=QSE;tickers=55;isin_coverage=54;sector_coverage=55;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| RSE | 2 | 2 | 2 | 1 | 0 | 1 | 0 | `exchange=RSE;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| SEM | 52 | 52 | 50 | 6 | 0 | 6 | 0 | `exchange=SEM;tickers=52;isin_coverage=52;sector_coverage=50;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| SET | 779 | 639 | 779 | 144 | 0 | 144 | 0 | `exchange=SET;tickers=779;isin_coverage=639;sector_coverage=779;source_gap_rows=144;warn_rows=0;quality_source_gap_rows=144;quarantine_rows=0` |
| SGX | 613 | 613 | 553 | 4 | 0 | 23 | 0 | `exchange=SGX;tickers=613;isin_coverage=613;sector_coverage=553;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=23;quarantine_rows=0` |
| SIX | 1263 | 1262 | 1226 | 31 | 0 | 57 | 0 | `exchange=SIX;tickers=1263;isin_coverage=1262;sector_coverage=1226;source_gap_rows=31;warn_rows=0;quality_source_gap_rows=57;quarantine_rows=0` |
| SSE | 2795 | 2760 | 2795 | 46 | 0 | 46 | 0 | `exchange=SSE;tickers=2795;isin_coverage=2760;sector_coverage=2795;source_gap_rows=46;warn_rows=0;quality_source_gap_rows=46;quarantine_rows=0` |
| SSE_CL | 129 | 102 | 115 | 45 | 0 | 42 | 0 | `exchange=SSE_CL;tickers=129;isin_coverage=102;sector_coverage=115;source_gap_rows=45;warn_rows=0;quality_source_gap_rows=42;quarantine_rows=0` |
| STO | 878 | 878 | 873 | 54 | 2 | 55 | 0 | `exchange=STO;tickers=878;isin_coverage=878;sector_coverage=873;source_gap_rows=54;warn_rows=2;quality_source_gap_rows=55;quarantine_rows=0` |
| SZSE | 3150 | 3138 | 3150 | 14 | 1 | 14 | 0 | `exchange=SZSE;tickers=3150;isin_coverage=3138;sector_coverage=3150;source_gap_rows=14;warn_rows=1;quality_source_gap_rows=14;quarantine_rows=0` |
| TADAWUL | 199 | 199 | 191 | 1 | 0 | 9 | 0 | `exchange=TADAWUL;tickers=199;isin_coverage=199;sector_coverage=191;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| TASE | 801 | 801 | 661 | 104 | 1 | 196 | 0 | `exchange=TASE;tickers=801;isin_coverage=801;sector_coverage=661;source_gap_rows=104;warn_rows=1;quality_source_gap_rows=196;quarantine_rows=0` |
| TPEX | 1119 | 1119 | 1119 | 10 | 1 | 10 | 0 | `exchange=TPEX;tickers=1119;isin_coverage=1119;sector_coverage=1119;source_gap_rows=10;warn_rows=1;quality_source_gap_rows=10;quarantine_rows=0` |
| TSE | 4077 | 4067 | 4065 | 34 | 0 | 34 | 0 | `exchange=TSE;tickers=4077;isin_coverage=4067;sector_coverage=4065;source_gap_rows=34;warn_rows=0;quality_source_gap_rows=34;quarantine_rows=0` |
| TSX | 2296 | 2219 | 1803 | 302 | 0 | 592 | 0 | `exchange=TSX;tickers=2296;isin_coverage=2219;sector_coverage=1803;source_gap_rows=302;warn_rows=0;quality_source_gap_rows=592;quarantine_rows=0` |
| TSXV | 1422 | 1325 | 1056 | 226 | 3 | 514 | 0 | `exchange=TSXV;tickers=1422;isin_coverage=1325;sector_coverage=1056;source_gap_rows=226;warn_rows=3;quality_source_gap_rows=514;quarantine_rows=0` |
| TWSE | 1239 | 1239 | 1191 | 27 | 0 | 65 | 0 | `exchange=TWSE;tickers=1239;isin_coverage=1239;sector_coverage=1191;source_gap_rows=27;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| UPCOM | 2 | 2 | 2 | 0 | 0 | 0 | 0 | `exchange=UPCOM;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| USE_UG | 7 | 7 | 7 | 0 | 0 | 0 | 0 | `exchange=USE_UG;tickers=7;isin_coverage=7;sector_coverage=7;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| VSE | 88 | 88 | 56 | 56 | 0 | 50 | 0 | `exchange=VSE;tickers=88;isin_coverage=88;sector_coverage=56;source_gap_rows=56;warn_rows=0;quality_source_gap_rows=50;quarantine_rows=0` |
| WSE | 582 | 582 | 570 | 23 | 1 | 30 | 0 | `exchange=WSE;tickers=582;isin_coverage=582;sector_coverage=570;source_gap_rows=23;warn_rows=1;quality_source_gap_rows=30;quarantine_rows=0` |
| XDUS | 199 | 199 | 0 | 3 | 0 | 199 | 0 | `exchange=XDUS;tickers=199;isin_coverage=199;sector_coverage=0;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=199;quarantine_rows=0` |
| XETRA | 4315 | 4314 | 3317 | 148 | 3 | 200 | 0 | `exchange=XETRA;tickers=4315;isin_coverage=4314;sector_coverage=3317;source_gap_rows=148;warn_rows=3;quality_source_gap_rows=200;quarantine_rows=0` |
| XHAM | 12 | 12 | 0 | 4 | 0 | 12 | 0 | `exchange=XHAM;tickers=12;isin_coverage=12;sector_coverage=0;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=12;quarantine_rows=0` |
| XHAN | 80 | 80 | 0 | 0 | 0 | 80 | 0 | `exchange=XHAN;tickers=80;isin_coverage=80;sector_coverage=0;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=80;quarantine_rows=0` |
| XSTU | 2773 | 2771 | 0 | 50 | 14 | 2759 | 0 | `exchange=XSTU;tickers=2773;isin_coverage=2771;sector_coverage=0;source_gap_rows=50;warn_rows=14;quality_source_gap_rows=2759;quarantine_rows=0` |
| ZSE | 23 | 23 | 23 | 0 | 0 | 0 | 0 | `exchange=ZSE;tickers=23;isin_coverage=23;sector_coverage=23;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ZSE_ZW | 27 | 27 | 27 | 1 | 0 | 1 | 0 | `exchange=ZSE_ZW;tickers=27;isin_coverage=27;sector_coverage=27;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |

## Source Files

| Key | Path |
|---|---|
| `asx_residual_review` | `data/reports/asx_residual_review.json` |
| `b3_residual_isin_review` | `data/reports/b3_residual_isin_review.json` |
| `b3_residual_sector_review` | `data/reports/b3_residual_sector_review.json` |
| `canada_figi_queue` | `data/reports/canada_figi_queue.json` |
| `canada_residual_review` | `data/reports/canada_residual_review.json` |
| `coverage_report` | `data/reports/coverage_report.json` |
| `entry_quality_csv` | `data/reports/entry_quality.csv` |
| `entry_quality_json` | `data/reports/entry_quality.json` |
| `financialdata_isin_supplements_review` | `data/reports/financialdata_isin_supplements_review.json` |
| `masterfile_collision_review` | `data/reports/masterfile_collision_review.json` |
| `ohlcv_plausibility` | `data/reports/ohlcv_plausibility.json` |
| `ohlcv_warning_review` | `data/reports/ohlcv_warning_review.json` |
| `otc_scope_review` | `data/reports/otc_scope_review.json` |
| `source_gap_classification_csv` | `data/reports/source_gap_classification.csv` |
| `source_gap_classification_json` | `data/reports/source_gap_classification.json` |
| `symbol_changes_review` | `data/reports/symbol_changes_review.json` |
| `validation_report` | `data/reports/validation_report.json` |
| `weak_sector_residual_review` | `data/reports/weak_sector_residual_review.json` |
