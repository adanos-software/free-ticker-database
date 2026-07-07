# Improvement Baseline

Generated: `2026-07-07T22:56:52Z`

Baseline snapshot for future before/after deltas. It does not authorize inferred metadata changes.

## Summary

| Metric | Value |
|---|---:|
| global_metric_count | `16` |
| campaign_count | `10` |
| exchange_count | `81` |
| source_file_count | `18` |
| baseline_context | `"metric_count=16;tickers=63057;listing_keys=74517;source_gap_rows=6333;warn_rows=75;quarantine_rows=0;validation_failed_error_gates=0"` |

Global context: `metric_count=16;tickers=63057;listing_keys=74517;source_gap_rows=6333;warn_rows=75;quarantine_rows=0;validation_failed_error_gates=0`

## Global

| Metric | Value |
|---|---:|
| tickers | `63057` |
| listing_keys | `74517` |
| isin_coverage | `61764` |
| sector_coverage | `63056` |
| stock_sector_coverage | `47420` |
| etf_category_coverage | `15636` |
| figi_coverage | `65760` |
| official_masterfile_matches | `53350` |
| official_masterfile_collisions | `11376` |
| official_masterfile_missing | `14889` |
| source_gap_rows | `6333` |
| entry_quality_warn_rows | `75` |
| entry_quality_source_gap_rows | `6272` |
| entry_quality_quarantine_rows | `0` |
| validation_failed_error_gates | `0` |
| source_freshness_status_totals | `{"fresh": 10, "old": 127}` |

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
| baseline_context | `campaign_key=otc;metric_count=3;nested_metric_count=0;numeric_row_total=11900` |
| scope_review_rows | `11054` |
| accepted_source_gap_rows | `846` |
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
| baseline_context | `campaign_key=symbol_changes;metric_count=3;nested_metric_count=2;numeric_row_total=295` |
| review_rows | `295` |
| exchange_scope_status_counts | `{"global_symbol_collision_outside_source_scope": 56, "matches_within_source_scope": 236, "unscoped_source_hint": 3}` |
| review_bucket_counts | `{"action_required_duplicate_or_cross_listing": 18, "action_required_possible_rename_or_delisting": 2, "already_reflected_in_scope_with_global_symbol_collision": 41, "already_reflected_in_source_scope": 204, "hold_out_of_scope_symbol_collision": 12, "manual_review_due_to_out_of_scope_collision": 3, "manual_scope_mapping_required": 3, "no_dataset_match_for_source_scope": 12}` |

### ohlcv

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=ohlcv;metric_count=4;nested_metric_count=2;numeric_row_total=360` |
| sample_rows | `240` |
| status_counts | `{"not_checked": 12, "notice": 24, "pass": 45, "source_gap": 39, "warn": 120}` |
| warning_review_rows | `120` |
| warning_review_authorization_counts | `{"blocked_until_official_listing_keyed_review": 120}` |

### freshness

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=freshness;metric_count=14;nested_metric_count=8;numeric_row_total=7597` |
| source_count | `137` |
| source_freshness_status_totals | `{"fresh": 10, "old": 127}` |
| source_refresh_priority_totals | `{"P1": 35, "P2": 92, "P4": 10}` |
| source_refresh_queue_priority_totals | `{"fresh_no_refresh_needed": {"P4": 10}, "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 34}, "refresh_official_subset_before_gap_enrichment": {"P2": 89}, "restore_or_replace_unavailable_source_before_data_fill": {"P1": 1, "P2": 3}}` |
| source_refresh_action_totals | `{"no_refresh_needed": 10, "refresh_official_exchange_directory_before_identity_or_collision_work": 34, "refresh_official_subset_before_gap_enrichment": 89, "restore_or_replace_unavailable_source_before_data_fill": 4}` |
| old_official_exchange_directory_count | `35` |
| source_gap_rows | `6333` |
| source_gap_class_totals | `{"adr_cdr_or_depositary_identifier_gap": 43, "capital_pool_or_halted_identifier_gap": 33, "debt_or_securitized_identifier_gap": 76, "fund_or_trust_identifier_gap": 231, "inactive_or_legacy_identifier_gap": 16, "official_current_directory_absent_identifier_gap": 9, "official_identifier_not_exposed_source_gap": 213, "official_industry_taxonomy_unavailable_gap": 1, "official_reference_symbol_collision_gap": 706, "official_reference_unmatched_source_gap": 5005}` |
| top_source_gap_review_batches | `20` ranked batches |

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `OTC` | 3034 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `B3` | 319 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BSE_IN` | 179 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BMV` | 151 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `OTC` | 126 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `TSX` | 122 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `NSE_IN` | 109 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BME` | 93 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `NYSE ARCA` | 90 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `AMS` | 85 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `LSE` | 84 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `XETRA` | 81 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `BATS` | 72 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `TASE` | 71 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `Euronext` | 67 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `CSE_MA` | 65 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `NASDAQ` | 61 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `official_reference_gap` | `official_reference_unmatched_source_gap` | `JSE` | 60 | Fresh official exchange directory, scoped alias review, or source-of-truth decision. | Require an active official reference match for the listing key, or a documented blocker/out-of-scope decision. |
| `missing_isin_primary` | `debt_or_securitized_identifier_gap` | `ASX` | 57 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| `official_reference_gap` | `official_reference_symbol_collision_gap` | `BME` | 50 | Official exchange directory plus listing-key review for the row's exchange/security. | Do not close the gap from a same-symbol match on another exchange; require exact exchange/symbol/name/identifier evidence. |

| Metric | Value |
|---|---:|
| symbol_changes_review_rows | `295` |
| ohlcv_plausibility_rows | `240` |
| financialdata_supplement_rows | `557` |
| financialdata_apply_eligibility_counts | `{"blocked_until_exchange_scope_explicitly_allowed": 91, "blocked_until_unique_official_isin_candidate_resolved": 163, "keep_absent_until_name_gated_official_isin_match": 169, "no_supplement_apply_existing_identifier_or_collision_guard": 199, "preserve_existing_reviewed_supplement_no_new_apply": 43}` |
| financialdata_verification_evidence_required_counts | `{"existing_database_isin_confirms_no_supplement_needed_or_cross_listing_review": 16, "existing_listing_key_confirms_no_supplement_needed": 33, "existing_reviewed_supplement_retained_with_original_official_source": 43, "explicit_exchange_scope_decision_before_financialdata_discovery_use": 91, "identity_resolution_before_any_global_ticker_reuse": 150, "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 169, "single_official_active_listing_with_valid_isin_and_name_gate": 163}` |

### baseline

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=baseline;metric_count=5;nested_metric_count=0;numeric_row_total=98` |
| tracked_campaigns | `10` |
| global_metric_count | `16` |
| exchange_baseline_enabled | `1` |
| baseline_snapshot_rows | `1` |
| exchange_count | `81` |


## Exchange Baseline

| Exchange | Tickers | ISIN | Sector | Source Gaps | Warns | Quality Source Gaps | Quarantine | Review Context |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| ADX | 86 | 86 | 86 | 1 | 0 | 1 | 0 | `exchange=ADX;tickers=86;isin_coverage=86;sector_coverage=86;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| AMS | 330 | 330 | 264 | 90 | 0 | 90 | 0 | `exchange=AMS;tickers=330;isin_coverage=330;sector_coverage=264;source_gap_rows=90;warn_rows=0;quality_source_gap_rows=90;quarantine_rows=0` |
| ASX | 1625 | 1526 | 1622 | 126 | 1 | 123 | 0 | `exchange=ASX;tickers=1625;isin_coverage=1526;sector_coverage=1622;source_gap_rows=126;warn_rows=1;quality_source_gap_rows=123;quarantine_rows=0` |
| ATHEX | 155 | 155 | 155 | 66 | 0 | 66 | 0 | `exchange=ATHEX;tickers=155;isin_coverage=155;sector_coverage=155;source_gap_rows=66;warn_rows=0;quality_source_gap_rows=66;quarantine_rows=0` |
| B3 | 1581 | 1571 | 1579 | 330 | 0 | 319 | 0 | `exchange=B3;tickers=1581;isin_coverage=1571;sector_coverage=1579;source_gap_rows=330;warn_rows=0;quality_source_gap_rows=319;quarantine_rows=0` |
| BATS | 1241 | 1221 | 1225 | 103 | 0 | 99 | 0 | `exchange=BATS;tickers=1241;isin_coverage=1221;sector_coverage=1225;source_gap_rows=103;warn_rows=0;quality_source_gap_rows=99;quarantine_rows=0` |
| BCBA | 63 | 63 | 63 | 0 | 0 | 0 | 0 | `exchange=BCBA;tickers=63;isin_coverage=63;sector_coverage=63;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BHB | 29 | 29 | 29 | 0 | 0 | 0 | 0 | `exchange=BHB;tickers=29;isin_coverage=29;sector_coverage=29;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BIST | 614 | 614 | 614 | 0 | 0 | 0 | 0 | `exchange=BIST;tickers=614;isin_coverage=614;sector_coverage=614;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BK | 104 | 104 | 103 | 0 | 0 | 0 | 0 | `exchange=BK;tickers=104;isin_coverage=104;sector_coverage=103;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BME | 221 | 221 | 221 | 143 | 0 | 143 | 0 | `exchange=BME;tickers=221;isin_coverage=221;sector_coverage=221;source_gap_rows=143;warn_rows=0;quality_source_gap_rows=143;quarantine_rows=0` |
| BMV | 179 | 162 | 178 | 168 | 0 | 167 | 0 | `exchange=BMV;tickers=179;isin_coverage=162;sector_coverage=178;source_gap_rows=168;warn_rows=0;quality_source_gap_rows=167;quarantine_rows=0` |
| BSE_BW | 39 | 39 | 36 | 13 | 0 | 13 | 0 | `exchange=BSE_BW;tickers=39;isin_coverage=39;sector_coverage=36;source_gap_rows=13;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| BSE_HU | 50 | 50 | 47 | 48 | 1 | 47 | 0 | `exchange=BSE_HU;tickers=50;isin_coverage=50;sector_coverage=47;source_gap_rows=48;warn_rows=1;quality_source_gap_rows=47;quarantine_rows=0` |
| BSE_IN | 2638 | 2638 | 2637 | 179 | 0 | 179 | 0 | `exchange=BSE_IN;tickers=2638;isin_coverage=2638;sector_coverage=2637;source_gap_rows=179;warn_rows=0;quality_source_gap_rows=179;quarantine_rows=0` |
| BVB | 80 | 80 | 80 | 0 | 0 | 0 | 0 | `exchange=BVB;tickers=80;isin_coverage=80;sector_coverage=80;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BVC | 3 | 3 | 3 | 0 | 0 | 0 | 0 | `exchange=BVC;tickers=3;isin_coverage=3;sector_coverage=3;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BVL | 33 | 33 | 33 | 2 | 0 | 2 | 0 | `exchange=BVL;tickers=33;isin_coverage=33;sector_coverage=33;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| Borsa Italiana | 277 | 277 | 277 | 26 | 0 | 26 | 0 | `exchange=Borsa Italiana;tickers=277;isin_coverage=277;sector_coverage=277;source_gap_rows=26;warn_rows=0;quality_source_gap_rows=26;quarantine_rows=0` |
| Bursa | 936 | 936 | 936 | 0 | 0 | 0 | 0 | `exchange=Bursa;tickers=936;isin_coverage=936;sector_coverage=936;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| CPH | 145 | 145 | 145 | 11 | 0 | 11 | 0 | `exchange=CPH;tickers=145;isin_coverage=145;sector_coverage=145;source_gap_rows=11;warn_rows=0;quality_source_gap_rows=11;quarantine_rows=0` |
| CSE_LK | 307 | 307 | 307 | 0 | 0 | 0 | 0 | `exchange=CSE_LK;tickers=307;isin_coverage=307;sector_coverage=307;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| CSE_MA | 66 | 66 | 66 | 65 | 0 | 65 | 0 | `exchange=CSE_MA;tickers=66;isin_coverage=66;sector_coverage=66;source_gap_rows=65;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| DFM | 46 | 46 | 46 | 0 | 0 | 0 | 0 | `exchange=DFM;tickers=46;isin_coverage=46;sector_coverage=46;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| DSE_TZ | 17 | 17 | 15 | 0 | 0 | 0 | 0 | `exchange=DSE_TZ;tickers=17;isin_coverage=17;sector_coverage=15;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| EGX | 223 | 223 | 222 | 33 | 0 | 33 | 0 | `exchange=EGX;tickers=223;isin_coverage=223;sector_coverage=222;source_gap_rows=33;warn_rows=0;quality_source_gap_rows=33;quarantine_rows=0` |
| Euronext | 1081 | 1081 | 994 | 98 | 0 | 98 | 0 | `exchange=Euronext;tickers=1081;isin_coverage=1081;sector_coverage=994;source_gap_rows=98;warn_rows=0;quality_source_gap_rows=98;quarantine_rows=0` |
| GSE | 19 | 18 | 18 | 2 | 0 | 2 | 0 | `exchange=GSE;tickers=19;isin_coverage=18;sector_coverage=18;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| HEL | 194 | 194 | 194 | 9 | 0 | 9 | 0 | `exchange=HEL;tickers=194;isin_coverage=194;sector_coverage=194;source_gap_rows=9;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| HKEX | 3044 | 3044 | 3013 | 7 | 0 | 7 | 0 | `exchange=HKEX;tickers=3044;isin_coverage=3044;sector_coverage=3013;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| HNX | 105 | 105 | 105 | 0 | 0 | 0 | 0 | `exchange=HNX;tickers=105;isin_coverage=105;sector_coverage=105;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| HOSE | 153 | 153 | 153 | 0 | 0 | 0 | 0 | `exchange=HOSE;tickers=153;isin_coverage=153;sector_coverage=153;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ICE_IS | 18 | 18 | 18 | 1 | 0 | 1 | 0 | `exchange=ICE_IS;tickers=18;isin_coverage=18;sector_coverage=18;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| IDX | 694 | 694 | 694 | 0 | 0 | 0 | 0 | `exchange=IDX;tickers=694;isin_coverage=694;sector_coverage=694;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ISE | 14 | 14 | 14 | 5 | 0 | 5 | 0 | `exchange=ISE;tickers=14;isin_coverage=14;sector_coverage=14;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| JSE | 212 | 212 | 212 | 87 | 0 | 87 | 0 | `exchange=JSE;tickers=212;isin_coverage=212;sector_coverage=212;source_gap_rows=87;warn_rows=0;quality_source_gap_rows=87;quarantine_rows=0` |
| KOSDAQ | 1578 | 1578 | 1578 | 7 | 0 | 7 | 0 | `exchange=KOSDAQ;tickers=1578;isin_coverage=1578;sector_coverage=1578;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| KRX | 1796 | 1795 | 1796 | 24 | 0 | 23 | 0 | `exchange=KRX;tickers=1796;isin_coverage=1795;sector_coverage=1796;source_gap_rows=24;warn_rows=0;quality_source_gap_rows=23;quarantine_rows=0` |
| LSE | 6557 | 6557 | 6266 | 131 | 60 | 131 | 0 | `exchange=LSE;tickers=6557;isin_coverage=6557;sector_coverage=6266;source_gap_rows=131;warn_rows=60;quality_source_gap_rows=131;quarantine_rows=0` |
| LUSE | 22 | 22 | 22 | 7 | 0 | 7 | 0 | `exchange=LUSE;tickers=22;isin_coverage=22;sector_coverage=22;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| MSE_MW | 8 | 8 | 8 | 0 | 0 | 0 | 0 | `exchange=MSE_MW;tickers=8;isin_coverage=8;sector_coverage=8;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| MSX | 91 | 91 | 91 | 0 | 0 | 0 | 0 | `exchange=MSX;tickers=91;isin_coverage=91;sector_coverage=91;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| NASDAQ | 4675 | 4595 | 4664 | 165 | 1 | 160 | 0 | `exchange=NASDAQ;tickers=4675;isin_coverage=4595;sector_coverage=4664;source_gap_rows=165;warn_rows=1;quality_source_gap_rows=160;quarantine_rows=0` |
| NEO | 197 | 154 | 191 | 50 | 0 | 49 | 0 | `exchange=NEO;tickers=197;isin_coverage=154;sector_coverage=191;source_gap_rows=50;warn_rows=0;quality_source_gap_rows=49;quarantine_rows=0` |
| NGX | 145 | 145 | 144 | 12 | 1 | 12 | 0 | `exchange=NGX;tickers=145;isin_coverage=145;sector_coverage=144;source_gap_rows=12;warn_rows=1;quality_source_gap_rows=12;quarantine_rows=0` |
| NMFQS | 6 | 6 | 6 | 0 | 0 | 0 | 0 | `exchange=NMFQS;tickers=6;isin_coverage=6;sector_coverage=6;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| NSE_IN | 2503 | 2503 | 2503 | 134 | 0 | 134 | 0 | `exchange=NSE_IN;tickers=2503;isin_coverage=2503;sector_coverage=2503;source_gap_rows=134;warn_rows=0;quality_source_gap_rows=134;quarantine_rows=0` |
| NSE_KE | 46 | 46 | 45 | 35 | 0 | 35 | 0 | `exchange=NSE_KE;tickers=46;isin_coverage=46;sector_coverage=45;source_gap_rows=35;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| NYSE | 2036 | 1988 | 2034 | 62 | 0 | 59 | 0 | `exchange=NYSE;tickers=2036;isin_coverage=1988;sector_coverage=2034;source_gap_rows=62;warn_rows=0;quality_source_gap_rows=59;quarantine_rows=0` |
| NYSE ARCA | 2654 | 2607 | 2628 | 166 | 0 | 155 | 0 | `exchange=NYSE ARCA;tickers=2654;isin_coverage=2607;sector_coverage=2628;source_gap_rows=166;warn_rows=0;quality_source_gap_rows=155;quarantine_rows=0` |
| NYSE MKT | 232 | 223 | 232 | 13 | 0 | 12 | 0 | `exchange=NYSE MKT;tickers=232;isin_coverage=223;sector_coverage=232;source_gap_rows=13;warn_rows=0;quality_source_gap_rows=12;quarantine_rows=0` |
| NZX | 45 | 45 | 42 | 0 | 0 | 0 | 0 | `exchange=NZX;tickers=45;isin_coverage=45;sector_coverage=42;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| OSL | 265 | 265 | 261 | 20 | 0 | 20 | 0 | `exchange=OSL;tickers=265;isin_coverage=265;sector_coverage=261;source_gap_rows=20;warn_rows=0;quality_source_gap_rows=20;quarantine_rows=0` |
| OTC | 11024 | 10317 | 10788 | 3160 | 7 | 3160 | 0 | `exchange=OTC;tickers=11024;isin_coverage=10317;sector_coverage=10788;source_gap_rows=3160;warn_rows=7;quality_source_gap_rows=3160;quarantine_rows=0` |
| PSE | 90 | 90 | 89 | 0 | 0 | 0 | 0 | `exchange=PSE;tickers=90;isin_coverage=90;sector_coverage=89;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| PSE_CZ | 26 | 26 | 26 | 4 | 0 | 4 | 0 | `exchange=PSE_CZ;tickers=26;isin_coverage=26;sector_coverage=26;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=4;quarantine_rows=0` |
| PSX | 371 | 366 | 371 | 5 | 0 | 5 | 0 | `exchange=PSX;tickers=371;isin_coverage=366;sector_coverage=371;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| QSE | 54 | 54 | 54 | 0 | 0 | 0 | 0 | `exchange=QSE;tickers=54;isin_coverage=54;sector_coverage=54;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| RSE | 2 | 2 | 2 | 1 | 0 | 1 | 0 | `exchange=RSE;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| SEM | 53 | 53 | 51 | 6 | 0 | 6 | 0 | `exchange=SEM;tickers=53;isin_coverage=53;sector_coverage=51;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| SET | 547 | 547 | 547 | 2 | 0 | 2 | 0 | `exchange=SET;tickers=547;isin_coverage=547;sector_coverage=547;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| SGX | 591 | 591 | 553 | 2 | 0 | 2 | 0 | `exchange=SGX;tickers=591;isin_coverage=591;sector_coverage=553;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| SIX | 757 | 757 | 757 | 20 | 0 | 20 | 0 | `exchange=SIX;tickers=757;isin_coverage=757;sector_coverage=757;source_gap_rows=20;warn_rows=0;quality_source_gap_rows=20;quarantine_rows=0` |
| SSE | 2789 | 2754 | 2789 | 37 | 0 | 37 | 0 | `exchange=SSE;tickers=2789;isin_coverage=2754;sector_coverage=2789;source_gap_rows=37;warn_rows=0;quality_source_gap_rows=37;quarantine_rows=0` |
| SSE_CL | 116 | 89 | 115 | 32 | 0 | 29 | 0 | `exchange=SSE_CL;tickers=116;isin_coverage=89;sector_coverage=115;source_gap_rows=32;warn_rows=0;quality_source_gap_rows=29;quarantine_rows=0` |
| STO | 834 | 834 | 833 | 48 | 0 | 48 | 0 | `exchange=STO;tickers=834;isin_coverage=834;sector_coverage=833;source_gap_rows=48;warn_rows=0;quality_source_gap_rows=48;quarantine_rows=0` |
| SZSE | 3083 | 3071 | 3083 | 14 | 1 | 14 | 0 | `exchange=SZSE;tickers=3083;isin_coverage=3071;sector_coverage=3083;source_gap_rows=14;warn_rows=1;quality_source_gap_rows=14;quarantine_rows=0` |
| TADAWUL | 191 | 191 | 191 | 0 | 0 | 0 | 0 | `exchange=TADAWUL;tickers=191;isin_coverage=191;sector_coverage=191;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| TASE | 672 | 672 | 661 | 75 | 0 | 75 | 0 | `exchange=TASE;tickers=672;isin_coverage=672;sector_coverage=661;source_gap_rows=75;warn_rows=0;quality_source_gap_rows=75;quarantine_rows=0` |
| TPEX | 1118 | 1118 | 1118 | 5 | 1 | 5 | 0 | `exchange=TPEX;tickers=1118;isin_coverage=1118;sector_coverage=1118;source_gap_rows=5;warn_rows=1;quality_source_gap_rows=5;quarantine_rows=0` |
| TSE | 4060 | 4060 | 4048 | 8 | 0 | 8 | 0 | `exchange=TSE;tickers=4060;isin_coverage=4060;sector_coverage=4048;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=8;quarantine_rows=0` |
| TSX | 1903 | 1813 | 1860 | 212 | 0 | 199 | 0 | `exchange=TSX;tickers=1903;isin_coverage=1813;sector_coverage=1860;source_gap_rows=212;warn_rows=0;quality_source_gap_rows=199;quarantine_rows=0` |
| TSXV | 1066 | 989 | 1064 | 97 | 2 | 94 | 0 | `exchange=TSXV;tickers=1066;isin_coverage=989;sector_coverage=1064;source_gap_rows=97;warn_rows=2;quality_source_gap_rows=94;quarantine_rows=0` |
| TWSE | 1191 | 1191 | 1191 | 16 | 0 | 16 | 0 | `exchange=TWSE;tickers=1191;isin_coverage=1191;sector_coverage=1191;source_gap_rows=16;warn_rows=0;quality_source_gap_rows=16;quarantine_rows=0` |
| UPCOM | 2 | 2 | 2 | 0 | 0 | 0 | 0 | `exchange=UPCOM;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| USE_UG | 7 | 7 | 7 | 0 | 0 | 0 | 0 | `exchange=USE_UG;tickers=7;isin_coverage=7;sector_coverage=7;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| VSE | 56 | 56 | 56 | 34 | 0 | 34 | 0 | `exchange=VSE;tickers=56;isin_coverage=56;sector_coverage=56;source_gap_rows=34;warn_rows=0;quality_source_gap_rows=34;quarantine_rows=0` |
| WSE | 542 | 542 | 541 | 17 | 0 | 17 | 0 | `exchange=WSE;tickers=542;isin_coverage=542;sector_coverage=541;source_gap_rows=17;warn_rows=0;quality_source_gap_rows=17;quarantine_rows=0` |
| XETRA | 3844 | 3844 | 3232 | 99 | 0 | 99 | 0 | `exchange=XETRA;tickers=3844;isin_coverage=3844;sector_coverage=3232;source_gap_rows=99;warn_rows=0;quality_source_gap_rows=99;quarantine_rows=0` |
| ZSE | 23 | 23 | 23 | 0 | 0 | 0 | 0 | `exchange=ZSE;tickers=23;isin_coverage=23;sector_coverage=23;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ZSE_ZW | 27 | 27 | 27 | 0 | 0 | 0 | 0 | `exchange=ZSE_ZW;tickers=27;isin_coverage=27;sector_coverage=27;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |

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
