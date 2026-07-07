# PR Review Summary

Generated: `2026-07-07T08:41:07Z`

This PR improves the ticker database through source-gated review workflows, refreshed official masterfile evidence, and release acceptance checks. It does not authorize guessed ISINs, sectors, ETF categories, names, listings, or symbol changes.

## Scope

- Added and updated review workflows for B3 residuals, OTC scope, Canada FIGI/ISIN review, ASX residuals, weak-sector venues, masterfile collisions, symbol-change review, OHLCV plausibility, source freshness, and release acceptance.
- Regenerated listing-keyed review artifacts under `data/reports/` so unresolved gaps remain explicit source gaps instead of inferred data fills.
- Refreshed selected official exchange-directory sources through controlled network refreshes with generated-at and row-count evidence.
- Added refresh safety for partial masterfile updates so an unavailable selected source does not silently delete existing reference rows.

## Data Safety

- No uncertain identifier, sector, category, name, listing, or symbol value is filled from symbol shape, issuer-name shape, peer instruments, or stale secondary evidence.
- Review artifacts are gates, not automatic apply instructions. Rows remain blank unless exact listing-keyed official evidence passes the relevant source gate.
- OHLCV evidence is plausibility-only and never authorizes canonical data changes.
- Freshness evidence only proves source age and row count; it does not authorize data changes by itself.

## Current Evidence

| Metric | Value |
|---|---:|
| Tickers | 63,161 |
| Listing keys | 74,557 |
| Official masterfile symbols | 79,160 |
| Official masterfile matches | 53,162 |
| Official masterfile collisions queued | 11,262 |
| Official masterfile missing queued | 14,736 |
| Source gaps | 1,138 |
| Entry-quality warnings | 75 |
| Quarantine rows | 0 |

## Acceptance

| Gate | Result |
|---|---|
| `python -m pytest tests/ -q` | run before release; not captured by generated report JSON |
| `python scripts/check_entry_quality_gate.py` | passed; `unexpected_warn_count=0`, `quarantine_count=0` |
| `python scripts/validate_database.py` | passed; `failed_error_gates=0` |
| `python scripts/build_release_acceptance_report.py` | passed; `61/61` |
| CRLF-aware `git diff --check` | run before release; not captured by generated report JSON |

## Freshness

| Metric | Value |
|---|---:|
| Fresh sources | 2 |
| Old sources | 134 |
| Remaining old P1 exchange-directory sources | 39 |

Remaining P1 exchange-directory refresh work:

- `otc_markets_stock_screener`
- `lse_price_explorer`
- `sec_company_tickers_exchange`
- `bse_india_scrips`
- `deutsche_boerse_xetra_all_tradable_equities`
- `jpx_listed_issues`
- `euronext_equities`
- `hkex_securities_list`
- `nse_india_securities_available`
- `krx_listed_companies`
- `b3_instruments_equities`
- `krx_etf_finder`
- `twse_listed_companies`
- `idx_company_profiles`
- `set_stock_search`
- `upcom_registered_securities`
- `sgx_securities_prices`
- `psx_dps_symbols`
- `bist_kap_mkk_listed_securities`
- `cboe_canada_listing_directory`
- `tadawul_main_market_watch`
- `pse_listed_company_directory`
- `bvb_shares_directory`
- `cse_lk_company_info_summary`
- `cse_lk_all_security_code`

## Review Backlog

| Campaign | Rows | Status |
|---|---:|---|
| B3 official coverage, ISIN and sector residuals | 393 | partially_improved_with_residual_source_gaps |
| OTC scope review | 11,078 | scoped_as_extended_with_source_gaps_documented |
| Canada ISIN/FIGI review | 525 | figi_queue_drained_remaining_isin_first_gaps |
| ASX ETF/ISIN residuals | 114 | official_probe_reviewed_residuals_documented |
| Weak sector venue residuals | 670 | venue_specific_review_queue_with_safe_ngx_apply |
| Masterfile collision identity review | 11,176 | listing_keyed_review_queue_ready_no_symbol_only_additions |
| Symbol-change workflow | 292 | source_scope_aware_review_queue |
| OHLCV plausibility sampling | 360 | sampling_queue_enabled_plausibility_only |
| Freshness and reporting | 1,803 | global_and_source_freshness_visible |
| Before/after delta baseline | 0 | baseline_snapshot_available_for_future_campaign_deltas |

## Primary Review Files

- `data/reports/release_acceptance.md`
- `data/reports/improvement_campaigns.md`
- `data/reports/improvement_deltas.md`
- `data/reports/coverage_report.md`
- `data/reports/source_gap_classification.md`
- `data/reports/source_of_truth_decisions.md`
- `data/reports/masterfile_collision_review.md`
