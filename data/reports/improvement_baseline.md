# Improvement Baseline

Generated: `2026-07-01T05:20:49Z`

Baseline snapshot for future before/after deltas. It does not authorize inferred metadata changes.

## Summary

| Metric | Value |
|---|---:|
| global_metric_count | `16` |
| campaign_count | `10` |
| exchange_count | `81` |
| source_file_count | `18` |
| baseline_context | `"metric_count=16;tickers=63137;listing_keys=74533;source_gap_rows=1090;warn_rows=75;quarantine_rows=0;validation_failed_error_gates=0"` |

Global context: `metric_count=16;tickers=63137;listing_keys=74533;source_gap_rows=1090;warn_rows=75;quarantine_rows=0;validation_failed_error_gates=0`

## Global

| Metric | Value |
|---|---:|
| tickers | `63137` |
| listing_keys | `74533` |
| isin_coverage | `61477` |
| sector_coverage | `62970` |
| stock_sector_coverage | `47389` |
| etf_category_coverage | `15581` |
| figi_coverage | `65768` |
| official_masterfile_matches | `53149` |
| official_masterfile_collisions | `11225` |
| official_masterfile_missing | `14470` |
| source_gap_rows | `1090` |
| entry_quality_warn_rows | `75` |
| entry_quality_source_gap_rows | `6802` |
| entry_quality_quarantine_rows | `0` |
| validation_failed_error_gates | `0` |
| source_freshness_status_totals | `{"old": 136}` |

## Campaign Baseline

### b3

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=b3;metric_count=2;nested_metric_count=0;numeric_row_total=15` |
| missing_isin_residual_rows | `10` |
| missing_sector_residual_rows | `5` |

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
| baseline_context | `campaign_key=symbol_changes;metric_count=3;nested_metric_count=2;numeric_row_total=288` |
| review_rows | `288` |
| exchange_scope_status_counts | `{"global_symbol_collision_outside_source_scope": 52, "matches_within_source_scope": 233, "unscoped_source_hint": 3}` |
| review_bucket_counts | `{"action_required_duplicate_or_cross_listing": 25, "action_required_possible_rename_or_delisting": 3, "already_reflected_in_scope_with_global_symbol_collision": 39, "already_reflected_in_source_scope": 193, "hold_out_of_scope_symbol_collision": 12, "manual_review_due_to_out_of_scope_collision": 1, "manual_scope_mapping_required": 3, "no_dataset_match_for_source_scope": 12}` |

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
| baseline_context | `campaign_key=freshness;metric_count=14;nested_metric_count=8;numeric_row_total=2352` |
| source_count | `136` |
| source_freshness_status_totals | `{"old": 136}` |
| source_refresh_priority_totals | `{"P1": 41, "P2": 95}` |
| source_refresh_queue_priority_totals | `{"refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 40}, "refresh_official_subset_before_gap_enrichment": {"P2": 91}, "restore_or_replace_unavailable_source_before_data_fill": {"P1": 1, "P2": 4}}` |
| source_refresh_action_totals | `{"refresh_official_exchange_directory_before_identity_or_collision_work": 40, "refresh_official_subset_before_gap_enrichment": 91, "restore_or_replace_unavailable_source_before_data_fill": 5}` |
| old_official_exchange_directory_count | `41` |
| source_gap_rows | `1090` |
| source_gap_class_totals | `{"adr_cdr_or_depositary_identifier_gap": 43, "adr_cdr_or_depositary_sector_gap": 1, "capital_pool_or_halted_identifier_gap": 35, "commodity_etf_category_gap": 3, "debt_or_securitized_identifier_gap": 81, "digital_asset_etf_category_gap": 1, "equity_etf_category_gap": 6, "fixed_income_etf_category_gap": 3, "fund_or_trust_identifier_gap": 300, "fundlike_stock_sector_gap": 1, "inactive_or_legacy_identifier_gap": 19, "official_current_directory_absent_identifier_gap": 12, "official_identifier_not_exposed_source_gap": 369, "official_identifier_reference_unmatched_gap": 64, "official_industry_taxonomy_unavailable_gap": 25, "official_product_reference_unmatched_category_gap": 19, "official_product_taxonomy_unavailable_gap": 64, "otc_sector_source_gap": 43, "shell_or_cpc_sector_gap": 1}` |
| top_source_gap_review_batches | `20` ranked batches |

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `NASDAQ` | 130 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `NYSE` | 71 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `NYSE ARCA` | 67 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_isin_primary` | `debt_or_securitized_identifier_gap` | `ASX` | 57 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `TSX` | 50 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_sector_stock` | `otc_sector_source_gap` | `OTC` | 43 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| `missing_isin_primary` | `capital_pool_or_halted_identifier_gap` | `TSXV` | 35 | Current exchange issuer/status file or CPC/shell prospectus. | Exact halted/CPC symbol and direct current identifier evidence. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `TSXV` | 35 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `NASDAQ` | 29 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `ASX` | 26 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_isin_primary` | `adr_cdr_or_depositary_identifier_gap` | `NEO` | 25 | Depositary/CDR program identifier source, not underlying equity ISIN. | Exact program symbol, issuer/program name, expected country prefix, and ISIN checksum. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `BATS` | 23 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `QSE` | 23 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `SSE` | 22 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `PSX` | 21 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `TSX` | 19 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `adr_cdr_or_depositary_identifier_gap` | `TSX` | 18 | Depositary/CDR program identifier source, not underlying equity ISIN. | Exact program symbol, issuer/program name, expected country prefix, and ISIN checksum. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `SSE_CL` | 17 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `BMV` | 16 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_isin_primary` | `official_identifier_reference_unmatched_gap` | `NASDAQ` | 16 | Official exchange directory, alias review, or CSD/security registry detail. | Require an exact official symbol/alias match or direct registry record before filling ISIN. |

| Metric | Value |
|---|---:|
| symbol_changes_review_rows | `288` |
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
| ADX | 86 | 86 | 86 | 0 | 0 | 1 | 0 | `exchange=ADX;tickers=86;isin_coverage=86;sector_coverage=86;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| AMS | 330 | 326 | 262 | 6 | 0 | 90 | 0 | `exchange=AMS;tickers=330;isin_coverage=326;sector_coverage=262;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=90;quarantine_rows=0` |
| ASX | 1629 | 1523 | 1592 | 118 | 1 | 152 | 0 | `exchange=ASX;tickers=1629;isin_coverage=1523;sector_coverage=1592;source_gap_rows=118;warn_rows=1;quality_source_gap_rows=152;quarantine_rows=0` |
| ATHEX | 155 | 150 | 155 | 5 | 0 | 66 | 0 | `exchange=ATHEX;tickers=155;isin_coverage=150;sector_coverage=155;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=66;quarantine_rows=0` |
| B3 | 1583 | 1573 | 1577 | 15 | 0 | 321 | 0 | `exchange=B3;tickers=1583;isin_coverage=1573;sector_coverage=1577;source_gap_rows=15;warn_rows=0;quality_source_gap_rows=321;quarantine_rows=0` |
| BATS | 1241 | 1215 | 1220 | 31 | 0 | 87 | 0 | `exchange=BATS;tickers=1241;isin_coverage=1215;sector_coverage=1220;source_gap_rows=31;warn_rows=0;quality_source_gap_rows=87;quarantine_rows=0` |
| BCBA | 64 | 61 | 64 | 3 | 0 | 3 | 0 | `exchange=BCBA;tickers=64;isin_coverage=61;sector_coverage=64;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| BHB | 29 | 29 | 29 | 0 | 0 | 0 | 0 | `exchange=BHB;tickers=29;isin_coverage=29;sector_coverage=29;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BIST | 614 | 614 | 614 | 0 | 0 | 0 | 0 | `exchange=BIST;tickers=614;isin_coverage=614;sector_coverage=614;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BK | 104 | 104 | 100 | 3 | 0 | 3 | 0 | `exchange=BK;tickers=104;isin_coverage=104;sector_coverage=100;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| BME | 221 | 221 | 221 | 0 | 0 | 143 | 0 | `exchange=BME;tickers=221;isin_coverage=221;sector_coverage=221;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=143;quarantine_rows=0` |
| BMV | 179 | 162 | 176 | 19 | 0 | 168 | 0 | `exchange=BMV;tickers=179;isin_coverage=162;sector_coverage=176;source_gap_rows=19;warn_rows=0;quality_source_gap_rows=168;quarantine_rows=0` |
| BSE_BW | 39 | 39 | 35 | 1 | 0 | 13 | 0 | `exchange=BSE_BW;tickers=39;isin_coverage=39;sector_coverage=35;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| BSE_HU | 50 | 45 | 47 | 5 | 1 | 47 | 0 | `exchange=BSE_HU;tickers=50;isin_coverage=45;sector_coverage=47;source_gap_rows=5;warn_rows=1;quality_source_gap_rows=47;quarantine_rows=0` |
| BSE_IN | 2638 | 2638 | 2637 | 0 | 0 | 179 | 0 | `exchange=BSE_IN;tickers=2638;isin_coverage=2638;sector_coverage=2637;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=179;quarantine_rows=0` |
| BVB | 80 | 80 | 76 | 4 | 0 | 4 | 0 | `exchange=BVB;tickers=80;isin_coverage=80;sector_coverage=76;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=4;quarantine_rows=0` |
| BVC | 3 | 3 | 3 | 0 | 0 | 0 | 0 | `exchange=BVC;tickers=3;isin_coverage=3;sector_coverage=3;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| BVL | 33 | 32 | 31 | 3 | 0 | 3 | 0 | `exchange=BVL;tickers=33;isin_coverage=32;sector_coverage=31;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| Borsa Italiana | 277 | 277 | 277 | 0 | 0 | 277 | 0 | `exchange=Borsa Italiana;tickers=277;isin_coverage=277;sector_coverage=277;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=277;quarantine_rows=0` |
| Bursa | 936 | 936 | 936 | 0 | 0 | 0 | 0 | `exchange=Bursa;tickers=936;isin_coverage=936;sector_coverage=936;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| CPH | 145 | 145 | 144 | 1 | 0 | 12 | 0 | `exchange=CPH;tickers=145;isin_coverage=145;sector_coverage=144;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=12;quarantine_rows=0` |
| CSE_LK | 307 | 307 | 305 | 2 | 0 | 2 | 0 | `exchange=CSE_LK;tickers=307;isin_coverage=307;sector_coverage=305;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| CSE_MA | 66 | 66 | 65 | 1 | 0 | 65 | 0 | `exchange=CSE_MA;tickers=66;isin_coverage=66;sector_coverage=65;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| DFM | 46 | 46 | 46 | 0 | 0 | 0 | 0 | `exchange=DFM;tickers=46;isin_coverage=46;sector_coverage=46;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| DSE_TZ | 17 | 17 | 15 | 0 | 0 | 0 | 0 | `exchange=DSE_TZ;tickers=17;isin_coverage=17;sector_coverage=15;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| EGX | 223 | 223 | 222 | 0 | 0 | 33 | 0 | `exchange=EGX;tickers=223;isin_coverage=223;sector_coverage=222;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=33;quarantine_rows=0` |
| Euronext | 1083 | 1080 | 992 | 7 | 0 | 80 | 0 | `exchange=Euronext;tickers=1083;isin_coverage=1080;sector_coverage=992;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=80;quarantine_rows=0` |
| GSE | 19 | 18 | 18 | 1 | 0 | 2 | 0 | `exchange=GSE;tickers=19;isin_coverage=18;sector_coverage=18;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| HEL | 194 | 194 | 193 | 1 | 0 | 10 | 0 | `exchange=HEL;tickers=194;isin_coverage=194;sector_coverage=193;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=10;quarantine_rows=0` |
| HKEX | 3044 | 3044 | 3013 | 0 | 0 | 7 | 0 | `exchange=HKEX;tickers=3044;isin_coverage=3044;sector_coverage=3013;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| HNX | 105 | 105 | 105 | 0 | 0 | 0 | 0 | `exchange=HNX;tickers=105;isin_coverage=105;sector_coverage=105;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| HOSE | 153 | 153 | 153 | 0 | 0 | 0 | 0 | `exchange=HOSE;tickers=153;isin_coverage=153;sector_coverage=153;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ICE_IS | 18 | 18 | 18 | 0 | 0 | 1 | 0 | `exchange=ICE_IS;tickers=18;isin_coverage=18;sector_coverage=18;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| IDX | 694 | 692 | 694 | 2 | 0 | 2 | 0 | `exchange=IDX;tickers=694;isin_coverage=692;sector_coverage=694;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| ISE | 14 | 14 | 14 | 0 | 0 | 5 | 0 | `exchange=ISE;tickers=14;isin_coverage=14;sector_coverage=14;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| JSE | 212 | 205 | 211 | 8 | 0 | 88 | 0 | `exchange=JSE;tickers=212;isin_coverage=205;sector_coverage=211;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=88;quarantine_rows=0` |
| KOSDAQ | 1581 | 1578 | 1581 | 3 | 0 | 7 | 0 | `exchange=KOSDAQ;tickers=1581;isin_coverage=1578;sector_coverage=1581;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| KRX | 1796 | 1794 | 1794 | 4 | 0 | 16 | 0 | `exchange=KRX;tickers=1796;isin_coverage=1794;sector_coverage=1794;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=16;quarantine_rows=0` |
| LSE | 6563 | 6554 | 6260 | 15 | 60 | 140 | 0 | `exchange=LSE;tickers=6563;isin_coverage=6554;sector_coverage=6260;source_gap_rows=15;warn_rows=60;quality_source_gap_rows=140;quarantine_rows=0` |
| LUSE | 22 | 22 | 22 | 0 | 0 | 7 | 0 | `exchange=LUSE;tickers=22;isin_coverage=22;sector_coverage=22;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| MSE_MW | 8 | 8 | 8 | 0 | 0 | 8 | 0 | `exchange=MSE_MW;tickers=8;isin_coverage=8;sector_coverage=8;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=8;quarantine_rows=0` |
| MSX | 91 | 90 | 91 | 1 | 0 | 1 | 0 | `exchange=MSX;tickers=91;isin_coverage=90;sector_coverage=91;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| NASDAQ | 4661 | 4483 | 4648 | 180 | 1 | 266 | 0 | `exchange=NASDAQ;tickers=4661;isin_coverage=4483;sector_coverage=4648;source_gap_rows=180;warn_rows=1;quality_source_gap_rows=266;quarantine_rows=0` |
| NEO | 197 | 154 | 190 | 44 | 0 | 49 | 0 | `exchange=NEO;tickers=197;isin_coverage=154;sector_coverage=190;source_gap_rows=44;warn_rows=0;quality_source_gap_rows=49;quarantine_rows=0` |
| NGX | 145 | 145 | 138 | 6 | 1 | 12 | 0 | `exchange=NGX;tickers=145;isin_coverage=145;sector_coverage=138;source_gap_rows=6;warn_rows=1;quality_source_gap_rows=12;quarantine_rows=0` |
| NMFQS | 6 | 6 | 5 | 1 | 0 | 1 | 0 | `exchange=NMFQS;tickers=6;isin_coverage=6;sector_coverage=5;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| NSE_IN | 2503 | 2503 | 2503 | 0 | 0 | 134 | 0 | `exchange=NSE_IN;tickers=2503;isin_coverage=2503;sector_coverage=2503;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=134;quarantine_rows=0` |
| NSE_KE | 46 | 46 | 45 | 0 | 0 | 35 | 0 | `exchange=NSE_KE;tickers=46;isin_coverage=46;sector_coverage=45;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| NYSE | 2025 | 1937 | 2020 | 91 | 0 | 104 | 0 | `exchange=NYSE;tickers=2025;isin_coverage=1937;sector_coverage=2020;source_gap_rows=91;warn_rows=0;quality_source_gap_rows=104;quarantine_rows=0` |
| NYSE ARCA | 2653 | 2584 | 2613 | 83 | 0 | 162 | 0 | `exchange=NYSE ARCA;tickers=2653;isin_coverage=2584;sector_coverage=2613;source_gap_rows=83;warn_rows=0;quality_source_gap_rows=162;quarantine_rows=0` |
| NYSE MKT | 237 | 227 | 237 | 10 | 0 | 13 | 0 | `exchange=NYSE MKT;tickers=237;isin_coverage=227;sector_coverage=237;source_gap_rows=10;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| NZX | 45 | 45 | 42 | 0 | 0 | 0 | 0 | `exchange=NZX;tickers=45;isin_coverage=45;sector_coverage=42;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| OSL | 266 | 263 | 260 | 5 | 0 | 19 | 0 | `exchange=OSL;tickers=266;isin_coverage=263;sector_coverage=260;source_gap_rows=5;warn_rows=0;quality_source_gap_rows=19;quarantine_rows=0` |
| OTC | 11038 | 10263 | 10741 | 52 | 6 | 3174 | 0 | `exchange=OTC;tickers=11038;isin_coverage=10263;sector_coverage=10741;source_gap_rows=52;warn_rows=6;quality_source_gap_rows=3174;quarantine_rows=0` |
| PSE | 90 | 90 | 89 | 0 | 0 | 0 | 0 | `exchange=PSE;tickers=90;isin_coverage=90;sector_coverage=89;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| PSE_CZ | 26 | 25 | 26 | 1 | 0 | 4 | 0 | `exchange=PSE_CZ;tickers=26;isin_coverage=25;sector_coverage=26;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=4;quarantine_rows=0` |
| PSX | 373 | 340 | 373 | 33 | 0 | 33 | 0 | `exchange=PSX;tickers=373;isin_coverage=340;sector_coverage=373;source_gap_rows=33;warn_rows=0;quality_source_gap_rows=33;quarantine_rows=0` |
| QSE | 54 | 29 | 54 | 25 | 0 | 25 | 0 | `exchange=QSE;tickers=54;isin_coverage=29;sector_coverage=54;source_gap_rows=25;warn_rows=0;quality_source_gap_rows=25;quarantine_rows=0` |
| RSE | 2 | 2 | 2 | 0 | 0 | 1 | 0 | `exchange=RSE;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| SEM | 53 | 53 | 51 | 0 | 0 | 6 | 0 | `exchange=SEM;tickers=53;isin_coverage=53;sector_coverage=51;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| SET | 547 | 543 | 547 | 4 | 0 | 6 | 0 | `exchange=SET;tickers=547;isin_coverage=543;sector_coverage=547;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| SGX | 593 | 591 | 555 | 2 | 0 | 4 | 0 | `exchange=SGX;tickers=593;isin_coverage=591;sector_coverage=555;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=4;quarantine_rows=0` |
| SIX | 757 | 757 | 757 | 0 | 0 | 20 | 0 | `exchange=SIX;tickers=757;isin_coverage=757;sector_coverage=757;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=20;quarantine_rows=0` |
| SSE | 2789 | 2752 | 2789 | 37 | 0 | 39 | 0 | `exchange=SSE;tickers=2789;isin_coverage=2752;sector_coverage=2789;source_gap_rows=37;warn_rows=0;quality_source_gap_rows=39;quarantine_rows=0` |
| SSE_CL | 116 | 88 | 107 | 36 | 0 | 30 | 0 | `exchange=SSE_CL;tickers=116;isin_coverage=88;sector_coverage=107;source_gap_rows=36;warn_rows=0;quality_source_gap_rows=30;quarantine_rows=0` |
| STO | 834 | 834 | 832 | 1 | 0 | 49 | 0 | `exchange=STO;tickers=834;isin_coverage=834;sector_coverage=832;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=49;quarantine_rows=0` |
| SZSE | 3083 | 3071 | 3083 | 12 | 1 | 14 | 0 | `exchange=SZSE;tickers=3083;isin_coverage=3071;sector_coverage=3083;source_gap_rows=12;warn_rows=1;quality_source_gap_rows=14;quarantine_rows=0` |
| TADAWUL | 191 | 191 | 191 | 0 | 0 | 0 | 0 | `exchange=TADAWUL;tickers=191;isin_coverage=191;sector_coverage=191;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| TASE | 672 | 672 | 659 | 2 | 0 | 77 | 0 | `exchange=TASE;tickers=672;isin_coverage=672;sector_coverage=659;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=77;quarantine_rows=0` |
| TPEX | 1118 | 1118 | 1118 | 0 | 1 | 5 | 0 | `exchange=TPEX;tickers=1118;isin_coverage=1118;sector_coverage=1118;source_gap_rows=0;warn_rows=1;quality_source_gap_rows=5;quarantine_rows=0` |
| TSE | 4060 | 4060 | 4043 | 5 | 1 | 6 | 0 | `exchange=TSE;tickers=4060;isin_coverage=4060;sector_coverage=4043;source_gap_rows=5;warn_rows=1;quality_source_gap_rows=6;quarantine_rows=0` |
| TSX | 1903 | 1803 | 1852 | 106 | 0 | 205 | 0 | `exchange=TSX;tickers=1903;isin_coverage=1803;sector_coverage=1852;source_gap_rows=106;warn_rows=0;quality_source_gap_rows=205;quarantine_rows=0` |
| TSXV | 1066 | 984 | 1060 | 86 | 2 | 95 | 0 | `exchange=TSXV;tickers=1066;isin_coverage=984;sector_coverage=1060;source_gap_rows=86;warn_rows=2;quality_source_gap_rows=95;quarantine_rows=0` |
| TWSE | 1191 | 1191 | 1191 | 0 | 0 | 16 | 0 | `exchange=TWSE;tickers=1191;isin_coverage=1191;sector_coverage=1191;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=16;quarantine_rows=0` |
| UPCOM | 2 | 2 | 2 | 0 | 0 | 0 | 0 | `exchange=UPCOM;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| USE_UG | 7 | 7 | 7 | 0 | 0 | 0 | 0 | `exchange=USE_UG;tickers=7;isin_coverage=7;sector_coverage=7;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| VSE | 56 | 56 | 56 | 0 | 0 | 34 | 0 | `exchange=VSE;tickers=56;isin_coverage=56;sector_coverage=56;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=34;quarantine_rows=0` |
| WSE | 542 | 542 | 539 | 2 | 0 | 19 | 0 | `exchange=WSE;tickers=542;isin_coverage=542;sector_coverage=539;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=19;quarantine_rows=0` |
| XETRA | 3844 | 3841 | 3228 | 7 | 0 | 102 | 0 | `exchange=XETRA;tickers=3844;isin_coverage=3841;sector_coverage=3228;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=102;quarantine_rows=0` |
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
