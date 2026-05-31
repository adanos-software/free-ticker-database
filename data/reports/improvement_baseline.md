# Improvement Baseline

Generated: `2026-05-25T12:54:49Z`

Baseline snapshot for future before/after deltas. It does not authorize inferred metadata changes.

## Summary

| Metric | Value |
|---|---:|
| global_metric_count | `16` |
| campaign_count | `10` |
| exchange_count | `80` |
| source_file_count | `18` |
| baseline_context | `"metric_count=16;tickers=61465;listing_keys=71043;source_gap_rows=3548;warn_rows=217;quarantine_rows=0;validation_failed_error_gates=1"` |

Global context: `metric_count=16;tickers=61465;listing_keys=71043;source_gap_rows=3548;warn_rows=217;quarantine_rows=0;validation_failed_error_gates=1`

## Global

| Metric | Value |
|---|---:|
| tickers | `61465` |
| listing_keys | `71043` |
| isin_coverage | `59847` |
| sector_coverage | `58763` |
| stock_sector_coverage | `43310` |
| etf_category_coverage | `15453` |
| figi_coverage | `64353` |
| official_masterfile_matches | `51140` |
| official_masterfile_collisions | `11107` |
| official_masterfile_missing | `16920` |
| source_gap_rows | `3548` |
| entry_quality_warn_rows | `217` |
| entry_quality_source_gap_rows | `7454` |
| entry_quality_quarantine_rows | `0` |
| validation_failed_error_gates | `1` |
| source_freshness_status_totals | `{"fresh": 38, "old": 98}` |

## Campaign Baseline

### b3

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=b3;metric_count=2;nested_metric_count=0;numeric_row_total=206` |
| missing_isin_residual_rows | `12` |
| missing_sector_residual_rows | `194` |

### otc

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=otc;metric_count=3;nested_metric_count=0;numeric_row_total=11902` |
| scope_review_rows | `11056` |
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
| baseline_context | `campaign_key=masterfile_collisions;metric_count=4;nested_metric_count=3;numeric_row_total=11107` |
| review_rows | `11107` |
| decision_totals | `{"new_listing_candidate_requires_official_listing_add_review": 2762, "same_isin_cross_listing_candidate_requires_exchange_scope_review": 4578, "symbol_collision_requires_non_symbol_identity_source": 3767}` |
| review_bucket_totals | `{"distinct_official_isin_new_listing_candidate": 2177, "hold_symbol_only_collision_needs_non_symbol_identity": 3184, "resolve_asset_type_conflict_before_identity_review": 1612, "same_isin_cross_listing_needs_name_or_scope_review": 3805, "same_isin_exact_name_cross_listing_candidate": 329}` |
| review_priority_totals | `{"P1": 329, "P2": 7594, "P3": 3184}` |

### symbol_changes

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=symbol_changes;metric_count=3;nested_metric_count=2;numeric_row_total=263` |
| review_rows | `263` |
| exchange_scope_status_counts | `{"global_symbol_collision_outside_source_scope": 61, "matches_within_source_scope": 200, "unscoped_source_hint": 2}` |
| review_bucket_counts | `{"action_required_duplicate_or_cross_listing": 18, "action_required_possible_rename_or_delisting": 25, "already_reflected_in_scope_with_global_symbol_collision": 33, "already_reflected_in_source_scope": 139, "hold_out_of_scope_symbol_collision": 21, "manual_review_due_to_out_of_scope_collision": 7, "manual_scope_mapping_required": 2, "no_dataset_match_for_source_scope": 18}` |

### ohlcv

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=ohlcv;metric_count=4;nested_metric_count=2;numeric_row_total=361` |
| sample_rows | `345` |
| status_counts | `{"not_checked": 320, "notice": 8, "pass": 1, "warn": 16}` |
| warning_review_rows | `16` |
| warning_review_authorization_counts | `{"blocked_until_official_listing_keyed_review": 16}` |

### freshness

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=freshness;metric_count=14;nested_metric_count=8;numeric_row_total=4855` |
| source_count | `136` |
| source_freshness_status_totals | `{"fresh": 38, "old": 98}` |
| source_refresh_priority_totals | `{"P1": 6, "P2": 92, "P4": 38}` |
| source_refresh_queue_priority_totals | `{"fresh_no_refresh_needed": {"P4": 38}, "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 5}, "refresh_official_subset_before_gap_enrichment": {"P2": 85}, "restore_or_replace_unavailable_source_before_data_fill": {"P1": 1, "P2": 7}}` |
| source_refresh_action_totals | `{"no_refresh_needed": 38, "refresh_official_exchange_directory_before_identity_or_collision_work": 5, "refresh_official_subset_before_gap_enrichment": 85, "restore_or_replace_unavailable_source_before_data_fill": 8}` |
| old_official_exchange_directory_count | `6` |
| source_gap_rows | `3548` |
| source_gap_class_totals | `{"adr_cdr_or_depositary_identifier_gap": 45, "adr_cdr_or_depositary_sector_gap": 39, "capital_pool_or_halted_identifier_gap": 35, "commodity_etf_category_gap": 3, "debt_or_securitized_identifier_gap": 83, "digital_asset_etf_category_gap": 2, "equity_etf_category_gap": 2, "fixed_income_etf_category_gap": 2, "fund_or_trust_identifier_gap": 274, "fundlike_stock_sector_gap": 73, "inactive_or_legacy_identifier_gap": 17, "official_current_directory_absent_identifier_gap": 12, "official_identifier_not_exposed_source_gap": 314, "official_identifier_reference_unmatched_gap": 66, "official_industry_taxonomy_unavailable_gap": 1581, "official_product_reference_unmatched_category_gap": 21, "official_product_taxonomy_unavailable_gap": 72, "otc_sector_source_gap": 817, "shell_or_cpc_sector_gap": 90}` |
| top_source_gap_review_batches | `20` ranked batches |

| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |
|---|---|---|---:|---|---|
| `missing_sector_stock` | `otc_sector_source_gap` | `OTC` | 817 | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `B3` | 194 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `CSE_LK` | 140 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `Euronext` | 132 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `BK` | 102 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `LSE` | 102 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `MSX` | 90 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |
| `missing_sector_stock` | `shell_or_cpc_sector_gap` | `TSXV` | 80 | Official TMX issuer workbook classifies this row as CPC. | Do not fill stock_sector; review for core exclusion as a capital-pool issuer. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `PSE` | 67 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `CSE_MA` | 64 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `debt_or_securitized_identifier_gap` | `ASX` | 59 | Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed. | Exact instrument code/name and ISIN checksum; never issuer-equity propagation. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `OSL` | 58 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `TSX` | 54 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `STO` | 54 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `fund_or_trust_identifier_gap` | `NYSE ARCA` | 48 | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | Exact fund/trust symbol and product name with checksum. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `SEM` | 45 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `XETRA` | 42 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `SGX` | 39 | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | Keep stock_sector blank until an official taxonomy source exposes a canonical mappable industry value. |
| `missing_isin_primary` | `capital_pool_or_halted_identifier_gap` | `TSXV` | 35 | Current exchange issuer/status file or CPC/shell prospectus. | Exact halted/CPC symbol and direct current identifier evidence. |
| `missing_isin_primary` | `official_identifier_not_exposed_source_gap` | `TSXV` | 35 | Separate official CSD/security registry or exchange detail feed with ISIN. | Exact symbol/name and direct ISIN evidence; do not infer from issuer name or exchange membership. |

| Metric | Value |
|---|---:|
| symbol_changes_review_rows | `263` |
| ohlcv_plausibility_rows | `345` |
| financialdata_supplement_rows | `557` |
| financialdata_apply_eligibility_counts | `{"blocked_until_exchange_scope_explicitly_allowed": 91, "blocked_until_unique_official_isin_candidate_resolved": 163, "keep_absent_until_name_gated_official_isin_match": 172, "no_supplement_apply_existing_identifier_or_collision_guard": 196, "preserve_existing_reviewed_supplement_no_new_apply": 43}` |
| financialdata_verification_evidence_required_counts | `{"existing_database_isin_confirms_no_supplement_needed_or_cross_listing_review": 16, "existing_listing_key_confirms_no_supplement_needed": 33, "existing_reviewed_supplement_retained_with_original_official_source": 43, "explicit_exchange_scope_decision_before_financialdata_discovery_use": 91, "identity_resolution_before_any_global_ticker_reuse": 147, "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 172, "single_official_active_listing_with_valid_isin_and_name_gate": 163}` |

### baseline

| Metric | Value |
|---|---:|
| baseline_context | `campaign_key=baseline;metric_count=5;nested_metric_count=0;numeric_row_total=97` |
| tracked_campaigns | `10` |
| global_metric_count | `16` |
| exchange_baseline_enabled | `1` |
| baseline_snapshot_rows | `1` |
| exchange_count | `80` |


## Exchange Baseline

| Exchange | Tickers | ISIN | Sector | Source Gaps | Warns | Quality Source Gaps | Quarantine | Review Context |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| ADX | 86 | 86 | 86 | 0 | 0 | 1 | 0 | `exchange=ADX;tickers=86;isin_coverage=86;sector_coverage=86;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| AMS | 314 | 310 | 226 | 26 | 0 | 96 | 0 | `exchange=AMS;tickers=314;isin_coverage=310;sector_coverage=226;source_gap_rows=26;warn_rows=0;quality_source_gap_rows=96;quarantine_rows=0` |
| ASX | 1298 | 1193 | 1253 | 146 | 1 | 141 | 0 | `exchange=ASX;tickers=1298;isin_coverage=1193;sector_coverage=1253;source_gap_rows=146;warn_rows=1;quality_source_gap_rows=141;quarantine_rows=0` |
| ATHEX | 117 | 109 | 117 | 8 | 0 | 26 | 0 | `exchange=ATHEX;tickers=117;isin_coverage=109;sector_coverage=117;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=26;quarantine_rows=0` |
| B3 | 1584 | 1572 | 1389 | 206 | 0 | 212 | 0 | `exchange=B3;tickers=1584;isin_coverage=1572;sector_coverage=1389;source_gap_rows=206;warn_rows=0;quality_source_gap_rows=212;quarantine_rows=0` |
| BATS | 1241 | 1219 | 1220 | 27 | 0 | 83 | 0 | `exchange=BATS;tickers=1241;isin_coverage=1219;sector_coverage=1220;source_gap_rows=27;warn_rows=0;quality_source_gap_rows=83;quarantine_rows=0` |
| BCBA | 64 | 61 | 50 | 17 | 0 | 14 | 0 | `exchange=BCBA;tickers=64;isin_coverage=61;sector_coverage=50;source_gap_rows=17;warn_rows=0;quality_source_gap_rows=14;quarantine_rows=0` |
| BHB | 29 | 29 | 2 | 27 | 0 | 27 | 0 | `exchange=BHB;tickers=29;isin_coverage=29;sector_coverage=2;source_gap_rows=27;warn_rows=0;quality_source_gap_rows=27;quarantine_rows=0` |
| BIST | 614 | 614 | 608 | 6 | 0 | 6 | 0 | `exchange=BIST;tickers=614;isin_coverage=614;sector_coverage=608;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=6;quarantine_rows=0` |
| BK | 104 | 104 | 1 | 102 | 0 | 102 | 0 | `exchange=BK;tickers=104;isin_coverage=104;sector_coverage=1;source_gap_rows=102;warn_rows=0;quality_source_gap_rows=102;quarantine_rows=0` |
| BME | 169 | 169 | 160 | 9 | 0 | 102 | 0 | `exchange=BME;tickers=169;isin_coverage=169;sector_coverage=160;source_gap_rows=9;warn_rows=0;quality_source_gap_rows=102;quarantine_rows=0` |
| BMV | 179 | 160 | 174 | 23 | 0 | 29 | 0 | `exchange=BMV;tickers=179;isin_coverage=160;sector_coverage=174;source_gap_rows=23;warn_rows=0;quality_source_gap_rows=29;quarantine_rows=0` |
| BSE_BW | 39 | 39 | 28 | 8 | 0 | 13 | 0 | `exchange=BSE_BW;tickers=39;isin_coverage=39;sector_coverage=28;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| BSE_HU | 31 | 23 | 13 | 23 | 1 | 28 | 0 | `exchange=BSE_HU;tickers=31;isin_coverage=23;sector_coverage=13;source_gap_rows=23;warn_rows=1;quality_source_gap_rows=28;quarantine_rows=0` |
| BSE_IN | 2642 | 2642 | 2599 | 42 | 0 | 204 | 0 | `exchange=BSE_IN;tickers=2642;isin_coverage=2642;sector_coverage=2599;source_gap_rows=42;warn_rows=0;quality_source_gap_rows=204;quarantine_rows=0` |
| BVB | 80 | 80 | 76 | 4 | 0 | 4 | 0 | `exchange=BVB;tickers=80;isin_coverage=80;sector_coverage=76;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=4;quarantine_rows=0` |
| BVC | 3 | 0 | 3 | 3 | 0 | 3 | 0 | `exchange=BVC;tickers=3;isin_coverage=0;sector_coverage=3;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| BVL | 33 | 31 | 3 | 32 | 0 | 30 | 0 | `exchange=BVL;tickers=33;isin_coverage=31;sector_coverage=3;source_gap_rows=32;warn_rows=0;quality_source_gap_rows=30;quarantine_rows=0` |
| Bursa | 936 | 936 | 934 | 2 | 0 | 2 | 0 | `exchange=Bursa;tickers=936;isin_coverage=936;sector_coverage=934;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| CPH | 131 | 131 | 130 | 1 | 0 | 5 | 0 | `exchange=CPH;tickers=131;isin_coverage=131;sector_coverage=130;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| CSE_LK | 307 | 307 | 164 | 143 | 0 | 143 | 0 | `exchange=CSE_LK;tickers=307;isin_coverage=307;sector_coverage=164;source_gap_rows=143;warn_rows=0;quality_source_gap_rows=143;quarantine_rows=0` |
| CSE_MA | 66 | 66 | 2 | 64 | 0 | 65 | 0 | `exchange=CSE_MA;tickers=66;isin_coverage=66;sector_coverage=2;source_gap_rows=64;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| DFM | 46 | 46 | 45 | 1 | 0 | 1 | 0 | `exchange=DFM;tickers=46;isin_coverage=46;sector_coverage=45;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| DSE_TZ | 17 | 15 | 2 | 15 | 0 | 13 | 0 | `exchange=DSE_TZ;tickers=17;isin_coverage=15;sector_coverage=2;source_gap_rows=15;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| EGX | 225 | 225 | 196 | 28 | 0 | 35 | 0 | `exchange=EGX;tickers=225;isin_coverage=225;sector_coverage=196;source_gap_rows=28;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| Euronext | 975 | 972 | 755 | 136 | 0 | 149 | 0 | `exchange=Euronext;tickers=975;isin_coverage=972;sector_coverage=755;source_gap_rows=136;warn_rows=0;quality_source_gap_rows=149;quarantine_rows=0` |
| GSE | 19 | 18 | 2 | 17 | 0 | 16 | 0 | `exchange=GSE;tickers=19;isin_coverage=18;sector_coverage=2;source_gap_rows=17;warn_rows=0;quality_source_gap_rows=16;quarantine_rows=0` |
| HEL | 188 | 188 | 185 | 3 | 0 | 9 | 0 | `exchange=HEL;tickers=188;isin_coverage=188;sector_coverage=185;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| HKEX | 3044 | 3044 | 3005 | 8 | 0 | 9 | 0 | `exchange=HKEX;tickers=3044;isin_coverage=3044;sector_coverage=3005;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| HNX | 105 | 105 | 105 | 0 | 0 | 0 | 0 | `exchange=HNX;tickers=105;isin_coverage=105;sector_coverage=105;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| HOSE | 153 | 153 | 153 | 0 | 0 | 0 | 0 | `exchange=HOSE;tickers=153;isin_coverage=153;sector_coverage=153;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| ICE_IS | 18 | 18 | 16 | 2 | 0 | 2 | 0 | `exchange=ICE_IS;tickers=18;isin_coverage=18;sector_coverage=16;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| IDX | 694 | 689 | 694 | 5 | 2 | 4 | 0 | `exchange=IDX;tickers=694;isin_coverage=689;sector_coverage=694;source_gap_rows=5;warn_rows=2;quality_source_gap_rows=4;quarantine_rows=0` |
| ISE | 14 | 14 | 13 | 1 | 0 | 5 | 0 | `exchange=ISE;tickers=14;isin_coverage=14;sector_coverage=13;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| JSE | 212 | 204 | 210 | 10 | 0 | 88 | 0 | `exchange=JSE;tickers=212;isin_coverage=204;sector_coverage=210;source_gap_rows=10;warn_rows=0;quality_source_gap_rows=88;quarantine_rows=0` |
| KOSDAQ | 1583 | 1578 | 1580 | 8 | 0 | 8 | 0 | `exchange=KOSDAQ;tickers=1583;isin_coverage=1578;sector_coverage=1580;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=8;quarantine_rows=0` |
| KRX | 1796 | 1794 | 1794 | 4 | 0 | 13 | 0 | `exchange=KRX;tickers=1796;isin_coverage=1794;sector_coverage=1794;source_gap_rows=4;warn_rows=0;quality_source_gap_rows=13;quarantine_rows=0` |
| LSE | 6415 | 6404 | 5701 | 141 | 60 | 206 | 0 | `exchange=LSE;tickers=6415;isin_coverage=6404;sector_coverage=5701;source_gap_rows=141;warn_rows=60;quality_source_gap_rows=206;quarantine_rows=0` |
| LUSE | 22 | 22 | 2 | 20 | 0 | 21 | 0 | `exchange=LUSE;tickers=22;isin_coverage=22;sector_coverage=2;source_gap_rows=20;warn_rows=0;quality_source_gap_rows=21;quarantine_rows=0` |
| MSE_MW | 8 | 8 | 0 | 8 | 0 | 8 | 0 | `exchange=MSE_MW;tickers=8;isin_coverage=8;sector_coverage=0;source_gap_rows=8;warn_rows=0;quality_source_gap_rows=8;quarantine_rows=0` |
| MSX | 91 | 1 | 91 | 90 | 0 | 90 | 0 | `exchange=MSX;tickers=91;isin_coverage=1;sector_coverage=91;source_gap_rows=90;warn_rows=0;quality_source_gap_rows=90;quarantine_rows=0` |
| NASDAQ | 4635 | 4595 | 4612 | 51 | 5 | 145 | 0 | `exchange=NASDAQ;tickers=4635;isin_coverage=4595;sector_coverage=4612;source_gap_rows=51;warn_rows=5;quality_source_gap_rows=145;quarantine_rows=0` |
| NEO | 197 | 152 | 164 | 72 | 0 | 46 | 0 | `exchange=NEO;tickers=197;isin_coverage=152;sector_coverage=164;source_gap_rows=72;warn_rows=0;quality_source_gap_rows=46;quarantine_rows=0` |
| NGX | 145 | 143 | 114 | 32 | 0 | 38 | 0 | `exchange=NGX;tickers=145;isin_coverage=143;sector_coverage=114;source_gap_rows=32;warn_rows=0;quality_source_gap_rows=38;quarantine_rows=0` |
| NMFQS | 7 | 7 | 6 | 1 | 0 | 1 | 0 | `exchange=NMFQS;tickers=7;isin_coverage=7;sector_coverage=6;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=1;quarantine_rows=0` |
| NSE_IN | 1234 | 1234 | 1228 | 6 | 0 | 27 | 0 | `exchange=NSE_IN;tickers=1234;isin_coverage=1234;sector_coverage=1228;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=27;quarantine_rows=0` |
| NSE_KE | 46 | 46 | 13 | 32 | 0 | 40 | 0 | `exchange=NSE_KE;tickers=46;isin_coverage=46;sector_coverage=13;source_gap_rows=32;warn_rows=0;quality_source_gap_rows=40;quarantine_rows=0` |
| NYSE | 2082 | 2043 | 2069 | 49 | 1 | 74 | 0 | `exchange=NYSE;tickers=2082;isin_coverage=2043;sector_coverage=2069;source_gap_rows=49;warn_rows=1;quality_source_gap_rows=74;quarantine_rows=0` |
| NYSE ARCA | 2653 | 2601 | 2600 | 58 | 0 | 145 | 0 | `exchange=NYSE ARCA;tickers=2653;isin_coverage=2601;sector_coverage=2600;source_gap_rows=58;warn_rows=0;quality_source_gap_rows=145;quarantine_rows=0` |
| NYSE MKT | 236 | 235 | 236 | 1 | 0 | 5 | 0 | `exchange=NYSE MKT;tickers=236;isin_coverage=235;sector_coverage=236;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| NZX | 45 | 45 | 23 | 19 | 0 | 19 | 0 | `exchange=NZX;tickers=45;isin_coverage=45;sector_coverage=23;source_gap_rows=19;warn_rows=0;quality_source_gap_rows=19;quarantine_rows=0` |
| OSL | 241 | 237 | 178 | 63 | 0 | 65 | 0 | `exchange=OSL;tickers=241;isin_coverage=237;sector_coverage=178;source_gap_rows=63;warn_rows=0;quality_source_gap_rows=65;quarantine_rows=0` |
| OTC | 11056 | 10284 | 9911 | 846 | 146 | 3097 | 0 | `exchange=OTC;tickers=11056;isin_coverage=10284;sector_coverage=9911;source_gap_rows=846;warn_rows=146;quality_source_gap_rows=3097;quarantine_rows=0` |
| PSE | 90 | 90 | 13 | 76 | 0 | 76 | 0 | `exchange=PSE;tickers=90;isin_coverage=90;sector_coverage=13;source_gap_rows=76;warn_rows=0;quality_source_gap_rows=76;quarantine_rows=0` |
| PSE_CZ | 24 | 23 | 12 | 13 | 0 | 14 | 0 | `exchange=PSE_CZ;tickers=24;isin_coverage=23;sector_coverage=12;source_gap_rows=13;warn_rows=0;quality_source_gap_rows=14;quarantine_rows=0` |
| PSX | 373 | 338 | 373 | 35 | 0 | 35 | 0 | `exchange=PSX;tickers=373;isin_coverage=338;sector_coverage=373;source_gap_rows=35;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| QSE | 54 | 27 | 47 | 34 | 0 | 27 | 0 | `exchange=QSE;tickers=54;isin_coverage=27;sector_coverage=47;source_gap_rows=34;warn_rows=0;quality_source_gap_rows=27;quarantine_rows=0` |
| RSE | 2 | 2 | 0 | 2 | 0 | 2 | 0 | `exchange=RSE;tickers=2;isin_coverage=2;sector_coverage=0;source_gap_rows=2;warn_rows=0;quality_source_gap_rows=2;quarantine_rows=0` |
| SEM | 53 | 53 | 4 | 47 | 0 | 50 | 0 | `exchange=SEM;tickers=53;isin_coverage=53;sector_coverage=4;source_gap_rows=47;warn_rows=0;quality_source_gap_rows=50;quarantine_rows=0` |
| SET | 547 | 541 | 547 | 6 | 0 | 8 | 0 | `exchange=SET;tickers=547;isin_coverage=541;sector_coverage=547;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=8;quarantine_rows=0` |
| SGX | 594 | 591 | 515 | 43 | 0 | 41 | 0 | `exchange=SGX;tickers=594;isin_coverage=591;sector_coverage=515;source_gap_rows=43;warn_rows=0;quality_source_gap_rows=41;quarantine_rows=0` |
| SIX | 743 | 743 | 743 | 0 | 0 | 9 | 0 | `exchange=SIX;tickers=743;isin_coverage=743;sector_coverage=743;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=9;quarantine_rows=0` |
| SSE | 2789 | 2747 | 2789 | 42 | 0 | 560 | 0 | `exchange=SSE;tickers=2789;isin_coverage=2747;sector_coverage=2789;source_gap_rows=42;warn_rows=0;quality_source_gap_rows=560;quarantine_rows=0` |
| SSE_CL | 116 | 87 | 101 | 43 | 0 | 35 | 0 | `exchange=SSE_CL;tickers=116;isin_coverage=87;sector_coverage=101;source_gap_rows=43;warn_rows=0;quality_source_gap_rows=35;quarantine_rows=0` |
| STO | 725 | 725 | 669 | 55 | 0 | 64 | 0 | `exchange=STO;tickers=725;isin_coverage=725;sector_coverage=669;source_gap_rows=55;warn_rows=0;quality_source_gap_rows=64;quarantine_rows=0` |
| SZSE | 3083 | 3069 | 3083 | 14 | 1 | 16 | 0 | `exchange=SZSE;tickers=3083;isin_coverage=3069;sector_coverage=3083;source_gap_rows=14;warn_rows=1;quality_source_gap_rows=16;quarantine_rows=0` |
| TADAWUL | 191 | 191 | 188 | 3 | 0 | 3 | 0 | `exchange=TADAWUL;tickers=191;isin_coverage=191;sector_coverage=188;source_gap_rows=3;warn_rows=0;quality_source_gap_rows=3;quarantine_rows=0` |
| TASE | 673 | 673 | 647 | 13 | 0 | 109 | 0 | `exchange=TASE;tickers=673;isin_coverage=673;sector_coverage=647;source_gap_rows=13;warn_rows=0;quality_source_gap_rows=109;quarantine_rows=0` |
| TPEX | 1118 | 1118 | 1118 | 0 | 0 | 5 | 0 | `exchange=TPEX;tickers=1118;isin_coverage=1118;sector_coverage=1118;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=5;quarantine_rows=0` |
| TSE | 3216 | 3212 | 3178 | 30 | 0 | 36 | 0 | `exchange=TSE;tickers=3216;isin_coverage=3212;sector_coverage=3178;source_gap_rows=30;warn_rows=0;quality_source_gap_rows=36;quarantine_rows=0` |
| TSX | 1904 | 1802 | 1663 | 136 | 0 | 213 | 0 | `exchange=TSX;tickers=1904;isin_coverage=1802;sector_coverage=1663;source_gap_rows=136;warn_rows=0;quality_source_gap_rows=213;quarantine_rows=0` |
| TSXV | 1066 | 984 | 964 | 182 | 0 | 230 | 0 | `exchange=TSXV;tickers=1066;isin_coverage=984;sector_coverage=964;source_gap_rows=182;warn_rows=0;quality_source_gap_rows=230;quarantine_rows=0` |
| TWSE | 1191 | 1191 | 1190 | 1 | 0 | 17 | 0 | `exchange=TWSE;tickers=1191;isin_coverage=1191;sector_coverage=1190;source_gap_rows=1;warn_rows=0;quality_source_gap_rows=17;quarantine_rows=0` |
| UPCOM | 2 | 2 | 2 | 0 | 0 | 0 | 0 | `exchange=UPCOM;tickers=2;isin_coverage=2;sector_coverage=2;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0` |
| USE_UG | 7 | 7 | 0 | 7 | 0 | 7 | 0 | `exchange=USE_UG;tickers=7;isin_coverage=7;sector_coverage=0;source_gap_rows=7;warn_rows=0;quality_source_gap_rows=7;quarantine_rows=0` |
| VSE | 36 | 34 | 32 | 6 | 0 | 15 | 0 | `exchange=VSE;tickers=36;isin_coverage=34;sector_coverage=32;source_gap_rows=6;warn_rows=0;quality_source_gap_rows=15;quarantine_rows=0` |
| WSE | 348 | 348 | 322 | 25 | 0 | 27 | 0 | `exchange=WSE;tickers=348;isin_coverage=348;sector_coverage=322;source_gap_rows=25;warn_rows=0;quality_source_gap_rows=27;quarantine_rows=0` |
| XETRA | 3779 | 3776 | 3109 | 46 | 0 | 97 | 0 | `exchange=XETRA;tickers=3779;isin_coverage=3776;sector_coverage=3109;source_gap_rows=46;warn_rows=0;quality_source_gap_rows=97;quarantine_rows=0` |
| ZSE | 23 | 23 | 1 | 22 | 0 | 22 | 0 | `exchange=ZSE;tickers=23;isin_coverage=23;sector_coverage=1;source_gap_rows=22;warn_rows=0;quality_source_gap_rows=22;quarantine_rows=0` |
| ZSE_ZW | 27 | 27 | 6 | 21 | 0 | 21 | 0 | `exchange=ZSE_ZW;tickers=27;isin_coverage=27;sector_coverage=6;source_gap_rows=21;warn_rows=0;quality_source_gap_rows=21;quarantine_rows=0` |

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
