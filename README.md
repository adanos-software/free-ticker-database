# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Free stock and ETF ticker reference data with collision-safe core listings, legacy primary tickers, listing-keyed venue rows, aliases, ISIN/WKN identifiers, cross-listings, and coverage reports.

## Snapshot

| Metric | Value | Meaning |
|---|---:|---|
| Core listings | 61,694 | Rows in `data/core_listings.csv`; one collision-safe core row per security keyed by `listing_key`. |
| Primary tickers | 63,824 | Rows in `data/tickers.csv`; one primary row per security. |
| Full listing rows | 92,030 | Rows in `data/listings.csv`; venue-level rows keyed by `listing_key`, including cross/secondary listings. |
| Stocks | 47,780 | Primary ticker rows where `asset_type=Stock`. |
| ETFs | 16,044 | Primary ticker rows where `asset_type=ETF`. |
| Exchanges | 86 | Distinct primary-listing exchange codes in `data/tickers.csv`. |
| Countries | 91 | Distinct non-empty `country` values in `data/tickers.csv`. |
| Aliases | 125,532 | Rows in `data/aliases.csv`; structured alias/name/identifier lookup rows. |
| ISIN coverage | 62,554 (98.0%) | Primary ticker rows with a non-empty `isin`. |
| FIGI coverage | 65,397 | Listing-keyed rows in `data/identifiers_extended.csv` with OpenFIGI coverage. |
| Sector/category coverage | 62,298 (97.6%) | Primary ticker rows with either `stock_sector` or `etf_category`. |
| Stock sector coverage | 46,267 | Primary ticker rows with a non-empty `stock_sector`. |
| ETF category coverage | 16,031 | Primary ticker rows with a non-empty `etf_category`. |
| Core listing-scope rows | 61,694 | Rows in `data/instrument_scopes.csv` where `instrument_scope=core`. |
| Core primary rows with ISIN | 60,981 | Core primary listing rows with an ISIN; tracked as `scope_reason=primary_listing`. |
| Core primary rows missing ISIN | 713 | Core primary listing rows still missing ISIN; tracked as `scope_reason=primary_listing_missing_isin`. |
| Extended listing-scope rows | 30,336 | Rows in `data/instrument_scopes.csv` where `instrument_scope=extended`. |
| Official full exchanges | 49 | Exchange codes backed by a complete official exchange directory. |
| Official partial exchanges | 33 | Exchange codes backed by an official subset or security lookup, but not yet a proven complete directory. |
| Missing current-scope exchanges | 5 | Exchange codes without official source coverage; see `data/reports/source_inventory_gap.md`. |
| Entry quality source-gap rows | 11,374 | Listing-keyed rows that are structurally valid but retain explicit source or metadata gaps. |
| Entry quality warn rows | 33 | Listing-keyed rows with deterministic warnings requiring review/allowlist coverage. |

Snapshot values are generated-report backed and intentionally human-formatted with comma separators and one-decimal coverage percentages. `data/reports/coverage_report.json`, `data/reports/source_inventory_gap.json`, and `data/reports/entry_quality.json` are the canonical machine-readable sources for these counts. `source_inventory_gap.md` is authoritative for current-scope source gaps; this snapshot must not claim zero missing current-scope sources while that report lists a missing source.

## Core Files

| File | Use |
|---|---|
| [`data/core_listings.csv`](data/core_listings.csv) | Collision-safe canonical security universe keyed by `listing_key` |
| [`data/listings.csv`](data/listings.csv) | Venue-level source of truth keyed by `listing_key`, including cross/secondary listings |
| [`data/tickers.csv`](data/tickers.csv) | Legacy/global-unique ticker compatibility export, one row per exported ticker |
| [`data/instrument_scopes.csv`](data/instrument_scopes.csv) | Core vs. extended listing scope and primary-listing links |
| [`data/core_aliases.csv`](data/core_aliases.csv) | Collision-safe alias/name/identifier lookup keyed by `listing_key` |
| [`data/aliases.csv`](data/aliases.csv) | Alias/name/identifier lookup |
| [`data/adanos/ticker_reference.csv`](data/adanos/ticker_reference.csv) | Adanos Sentiment API-safe reference export with conservative natural-language aliases |
| [`data/adanos/natural_language_aliases.csv`](data/adanos/natural_language_aliases.csv) | Natural-language alias candidates with detection policy and confidence |
| [`data/identifiers.csv`](data/identifiers.csv) | Compact ISIN/WKN lookup |
| [`data/cross_listings.csv`](data/cross_listings.csv) | Same-ISIN listings across exchanges |

Generated release assets:

| Asset | Use |
|---|---|
| `data/tickers.json` | JSON export for APIs and apps, built by CI/release workflows |
| `data/core_listings.json` | Collision-safe JSON export, built by CI/release workflows |
| `data/tickers.parquet` | Analytics export, built by CI/release workflows |
| `data/core_listings.parquet` | Collision-safe analytics export, built by CI/release workflows |
| `data/tickers.db` | SQLite export, built by CI/release workflows |

Reference and audit files:

| File | Use |
|---|---|
| [`data/listing_index.csv`](data/listing_index.csv) | Listing-keyed identity bridge |
| [`data/identifiers_extended.csv`](data/identifiers_extended.csv) | FIGI/CIK/LEI enrichment snapshot |
| [`data/masterfiles/reference.csv`](data/masterfiles/reference.csv) | Official exchange-masterfile reference rows |
| [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) | Official source candidates not yet implemented as parsers |
| [`data/masterfiles/exchange_scope_decisions.csv`](data/masterfiles/exchange_scope_decisions.csv) | Explicit public-scope decision and promotion evidence required for every official-partial exchange |
| [`data/masterfiles/commercial_source_options.csv`](data/masterfiles/commercial_source_options.csv) | Review-gated commercial reference-data options, access models, redistribution status, and next actions |
| [`data/masterfiles/supplemental_listings.csv`](data/masterfiles/supplemental_listings.csv) | Safe official listings added to the core export |
| [`data/masterfiles/financialdata_isin_supplemental_listings.csv`](data/masterfiles/financialdata_isin_supplemental_listings.csv) | FinancialData-discovered rows accepted only after official ISIN-bearing masterfile match |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | Machine-readable coverage report |
| [`docs/SOURCE_LICENSING.md`](docs/SOURCE_LICENSING.md) | Source-license policy; unknown stays `review_required`, restricted reviews do not unlock `stable` |
| [`data/reports/pr_review_summary.md`](data/reports/pr_review_summary.md) | Compact PR review entry point for scope, safety policy, acceptance status, and remaining risks |
| [`data/reports/release_acceptance.md`](data/reports/release_acceptance.md) | Release acceptance matrix covering data invariants, review traceability, campaign evidence, and gates |
| [`data/reports/improvement_campaigns.md`](data/reports/improvement_campaigns.md) | Campaign status, next review batches, source gates, and closure blockers |
| [`data/reports/m3_correctness_campaigns.md`](data/reports/m3_correctness_campaigns.md) | M3 correctness campaign rollup for sector/category, name freshness, identity residuals, non-equity leakage, and re-audit evidence |
| [`data/reports/m3_correctness_audit.md`](data/reports/m3_correctness_audit.md) | Re-audit artifact generated after each M3 correctness block; does not claim 99% correctness without external stratified audit evidence |
| [`data/reports/m3_non_equity_leakage_guard.md`](data/reports/m3_non_equity_leakage_guard.md) | Scheduled guard report for preferreds, warrants, units, rights, notes, CEFs, and other non-common-stock leakage |
| [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md) | Missing/partial/global official-source backlog |
| [`data/reports/exchange_source_audit.md`](data/reports/exchange_source_audit.md) | One-row-per-exchange source scope, product-class, denominator, recall, and freshness audit |
| [`data/reports/etf_universe_completeness.md`](data/reports/etf_universe_completeness.md) | Official ETF-directory comparison against the DB ETF universe; missing rows are gated review candidates |
| [`data/reports/completion_backlog.md`](data/reports/completion_backlog.md) | Prioritized missing ISIN/sector/category backlog |
| [`data/reports/primary_isin_completeness.md`](data/reports/primary_isin_completeness.md) | D1 missing primary-ISIN source paths, priority venues, and apply gates |
| [`data/reports/cfi_code_review.md`](data/reports/cfi_code_review.md) | Review-only CFI evidence surfaced from official masterfiles for product-class gates |
| [`data/reports/source_gap_classification.md`](data/reports/source_gap_classification.md) | Deterministic residual source-gap classes and row-level source gates |
| [`data/reports/source_of_truth_decisions.md`](data/reports/source_of_truth_decisions.md) | Source-of-truth outcomes for each residual gap: fill, accepted source gap, or core-exclusion candidate |
| [`data/reports/b3_residual_isin_review.md`](data/reports/b3_residual_isin_review.md) | Listing-keyed review of the final B3 ISIN residuals after official B3 refreshes |
| [`data/reports/b3_residual_sector_review.md`](data/reports/b3_residual_sector_review.md) | Listing-keyed review of B3 stock-sector residuals after the official B3 taxonomy probe |
| [`data/reports/otc_scope_review.md`](data/reports/otc_scope_review.md) | Listing-keyed OTC scope guard before OTC metadata enrichment |
| [`data/reports/canada_residual_review.md`](data/reports/canada_residual_review.md) | Listing-keyed TSX/TSXV/NEO ISIN, FIGI, and source-gap review with TMX/Cboe context |
| [`data/reports/canada_figi_queue.md`](data/reports/canada_figi_queue.md) | ISIN-gated TSX/TSXV/NEO OpenFIGI batch queue |
| [`data/reports/canada_figi_batch_probe.md`](data/reports/canada_figi_batch_probe.md) | Read-only OpenFIGI probe for one Canada FIGI queue slice |
| [`data/reports/canada_figi_apply_report.md`](data/reports/canada_figi_apply_report.md) | Strict-gated apply report for accepted Canada OpenFIGI probe rows |
| [`data/reports/alias_quality.md`](data/reports/alias_quality.md) | Alias safety report for natural-language mention detection |
| [`data/reports/adanos_detection_simulation.md`](data/reports/adanos_detection_simulation.md) | Mention-detection smoke test for Adanos natural-language aliases |
| [`data/reports/entry_quality.md`](data/reports/entry_quality.md) | Per-listing deterministic quality scan summary |
| [`data/reports/validation_report.md`](data/reports/validation_report.md) | Release-gate validation summary across structure, ISINs, scopes, aliases, and reports |
| [`data/reports/override_debt_report.md`](data/reports/override_debt_report.md) | Open reviewed metadata/alias override debt after canonical normalization |
| [`data/reports/ohlcv_plausibility.md`](data/reports/ohlcv_plausibility.md) | Kronos-inspired market-data plausibility queue |
| [`data/reports/masterfile_collision_report.json`](data/reports/masterfile_collision_report.json) | Official-symbol gaps blocked by ticker collisions |
| [`data/reports/isin_identity_collision_review_queue.md`](data/reports/isin_identity_collision_review_queue.md) | ISINs shared by distinct issuer names (identity-collision review, gated for official evidence) |
| [`data/reports/deepseek_isin_collision_validation.md`](data/reports/deepseek_isin_collision_validation.md) | DeepSeek triage cross-check of the highest-risk ISIN identity collisions |
| [`docs/quality_improvement_plan.md`](docs/quality_improvement_plan.md) | Structured quality roadmap from the latest full-dataset audit |

## Data Model

`core_listings.csv` is the collision-safe canonical core export:

```csv
listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases,instrument_group_key,scope_reason
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc,Stock,Information Technology,,United States,US,US0378331005,apple,US0378331005,primary_listing
```

`tickers.csv` is the legacy compatibility export:

```csv
ticker,name,exchange,asset_type,stock_sector,etf_category,country,country_code,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,,United States,US,US1912161007,coca-cola
```

`listings.csv` is the full venue export:

```csv
listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc,Stock,Information Technology,,United States,US,US0378331005,apple
```

Important rules:

- `core_listings.csv` is the canonical core security export; `listing_key` is its collision-safe current venue/symbol key.
- `tickers.csv` is a compatibility export that keeps one row per globally unique `ticker`.
- `listings.csv` and `listing_key` are the venue-level source of truth for exchange-specific listing identity.
- `ticker` is globally unique only in `tickers.csv`; use `listing_key` for venue-level identity.
- Stocks use `stock_sector`; ETFs use `etf_category`.
- `instrument_scopes.csv` marks `core`, OTC `extended`, and secondary cross-listings.
- Core rows without ISIN are tagged as `scope_reason=primary_listing_missing_isin`.
- Secondary listings stay in `listings.csv` and `cross_listings.csv`; `core_listings.csv` keeps one primary listing row per security.
- `tickers.csv.aliases` is restricted to conservative natural-language aliases. ISINs, WKNs, and exchange-ticker aliases stay in `data/aliases.csv`, `data/core_aliases.csv`, and identifier exports.
- `data/adanos/ticker_reference.csv` is the preferred import for Adanos Sentiment API ticker detection.

JSON metadata:

```json
{
  "_meta": {
    "version": "3.37.0",
    "built_at": "2026-08-27T13:07:28Z",
    "total_tickers": 63824
  },
  "tickers": []
}
```

SQLite tables: `tickers`, `listings`, `aliases`, `cross_listings`, and `instrument_scopes`.
Additional collision-safe tables: `core_listings` and `core_aliases`.

<!-- canonical-v4-quality:start -->
## Canonical v4 and release truth

`listing_key` is the collision-safe **current venue/symbol key**. It is not a permanent historical identifier because symbols can change or be reused. Canonical v4 separates listing lifecycles from instruments and venues, while source observations and assertion tables preserve the evidence behind accepted values. Conflicting identifiers never merge listings into one instrument; they remain quarantined assertions until reviewed evidence resolves them.

The quality contract is cumulative: `merge` blocks structural, identity, history, safe-merge, source-governance, and canonical-schema failures; `stable` additionally requires passing official-full coverage, verified contributing-source rights, complete field provenance, and MIC mappings; `complete` additionally requires zero metadata and official-reference gaps. A green merge check never claims that the database is already complete or legally ready for a stable data release.

Canonical implementation source is committed as ordinary reviewable files. CI rejects compressed source payloads, workflow-time patching, and self-pushing workflows. It validates the canonical CSV contract, loads the result into PostgreSQL, and verifies deterministic repeat builds.

Operational rebuilds use:

```bash
python scripts/rebuild_canonical.py
```

Direct execution of `scripts/rebuild_dataset.py` remains available only for compatibility-export validation.
<!-- canonical-v4-quality:end -->
## Quality

- Valid ISINs are checksum-verified.
- `data/reports/alias_quality.csv` classifies every alias as safe, review-only, or identifier-only for mention detection.
- `data/reports/adanos_detection_simulation.json` measures positive alias hits and negative false-positive probes for the Sentiment API import.
- Natural-language aliases are derived from current security names on every rebuild, then normalized to API-safe aliases.
- Duplicate natural-language aliases are either assigned to a clear best owner or removed from public alias columns.
- `data/reports/entry_quality.csv` stores one deterministic quality row per `listing_key`.
- `data/reports/validation_report.json` is the release gate: duplicate keys, invalid ISINs, typed sector/category leakage, blank country metadata on ISIN-bearing rows, mojibake name corruption, Adanos alias findings, unexpected entry-quality warnings, stale coverage counts, stale/unclassified residual source gaps, unreviewed US-primary foreign ISINs (ticker-collision suspects, allowlisted in `data/review_overrides/foreign_isin_reviewed.csv`), and stale source-of-truth decisions must be clean.
- `data/reports/ohlcv_plausibility.csv` stores optional market-data hygiene checks; default runs are no-network and omit unchecked rows unless local OHLCV samples, `--fetch-yahoo`, or `--include-not-checked` are provided.
- Obvious common-word, wrapper, celebrity, product, junk, short, and numeric aliases are filtered.
- Rights, units, warrants, notes, preferreds, and depositary lines are filtered from the stock universe.
- Foreign OTC country metadata is corrected from valid ISIN prefixes where possible.
- Official masterfiles are kept separate from secondary sources.
- Source licenses are recorded in `data/masterfiles/sources.json`. Only `verified_open` with hashed evidence can pass official-full contracts; `verified_restricted` and `review_required` stay fail-closed. See [`docs/SOURCE_LICENSING.md`](docs/SOURCE_LICENSING.md).
- Yahoo, EODHD, XTB, and FinanceDatabase are treated as reviewed candidate sources, not as exchange authority.
- Local probe/test artifacts are ignored via `output/` and `test-results/`. CSVs under `data/` are the diffable source of truth; generated JSON, SQLite, and Parquet files are release assets.

## Coverage

Top exchanges by primary ticker count:

| Exchange | Tickers |
|---|---:|
| OTC | 6,895 |
| NASDAQ | 4,601 |
| LSE | 3,560 |
| TSE | 3,201 |
| SZSE | 3,111 |
| HKEX | 2,840 |
| SSE | 2,793 |
| BSE_IN | 2,684 |
| NYSE ARCA | 2,651 |
| NSE_IN | 2,379 |
| XETRA | 2,236 |
| NYSE | 1,878 |
| KRX | 1,990 |
| TSX | 1,687 |
| KOSDAQ | 1,603 |
| B3 | 1,578 |
| ASX | 1,394 |

For full exchange, country, source, and verification coverage, use:

```bash
python3 scripts/build_entry_quality_report.py
python3 scripts/build_coverage_report.py
python3 scripts/build_source_inventory.py
python3 scripts/build_exchange_source_audit.py
python3 scripts/build_etf_universe_completeness.py
python3 scripts/build_completion_backlog.py
python3 scripts/build_primary_isin_completeness.py
python3 scripts/build_cfi_code_review.py
python3 scripts/build_b3_residual_isin_review.py
python3 scripts/build_b3_residual_sector_review.py
python3 scripts/build_otc_scope_review.py
python3 scripts/build_canada_residual_review.py
python3 scripts/build_canada_figi_queue.py
python3 scripts/probe_canada_figi_batch.py --batch-id canada-figi-0001 --limit 10
python3 scripts/build_alias_quality_report.py
python3 scripts/build_adanos_ticker_reference.py
python3 scripts/simulate_adanos_detection.py
python3 scripts/validate_database.py
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/fetch_symbol_changes.py
FINANCIALDATA_API_KEY=... python3 scripts/fetch_financialdata_symbols.py
```

Long OHLCV fetch runs should use streaming checkpoints:

```bash
python3 scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --include-not-checked --stream --resume
```

## Refresh Pipeline

Quick rebuild:

```bash
python3 scripts/rebuild_dataset.py
python3 scripts/build_listing_history.py
python3 scripts/build_entry_quality_report.py
python3 scripts/build_coverage_report.py
python3 scripts/build_source_inventory.py
python3 scripts/build_exchange_source_audit.py
python3 scripts/build_etf_universe_completeness.py
python3 scripts/build_completion_backlog.py
python3 scripts/build_primary_isin_completeness.py
python3 scripts/build_cfi_code_review.py
python3 scripts/build_b3_residual_isin_review.py
python3 scripts/build_b3_residual_sector_review.py
python3 scripts/build_otc_scope_review.py
python3 scripts/build_canada_residual_review.py
python3 scripts/build_canada_figi_queue.py
python3 scripts/probe_canada_figi_batch.py --batch-id canada-figi-0001 --limit 10
python3 scripts/build_alias_quality_report.py
python3 scripts/build_adanos_ticker_reference.py
python3 scripts/simulate_adanos_detection.py
python3 scripts/validate_database.py
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/fetch_symbol_changes.py
```

Planned enrichment run:

```bash
python3 scripts/run_enrichment_pipeline.py --dry-run
```

Use `--include-secondary-network` for EODHD/Yahoo candidate stages. Use `--apply-reviewed-backfills` only when reviewed candidates should be merged into overrides.

Main targeted backfills:

| Task | Script |
|---|---|
| Official masterfiles | `scripts/fetch_exchange_masterfiles.py` |
| Nasdaq Trader new US stocks auto-apply | `scripts/apply_nasdaq_us_new_listings.py` |
| Safe official supplements | `scripts/build_masterfile_supplements.py` |
| Extended FIGI/CIK/LEI identifiers | `scripts/enrich_global_identifiers.py` |
| Same-ISIN sector/category peers | `scripts/backfill_sector_from_isin_peers.py` |
| FinanceDatabase sectors | `scripts/backfill_financedatabase_metadata.py` |
| EODHD ISIN candidates | `scripts/backfill_eodhd_metadata.py` |
| XTB OMI ISIN candidates | `scripts/backfill_xtb_omi_isins.py` |
| Yahoo OTC ISIN candidates | `scripts/backfill_yahoo_otc_isins.py` |
| ASX official ISINs | `scripts/backfill_asx_isins.py` |
| B3 COTAHIST ISINs | `scripts/backfill_b3_cotahist_isins.py` |
| Thailand SEC official SET ISINs | `scripts/backfill_set_sec_isins.py` |
| NYSE Group Security Master sample ISIN/category candidates | `scripts/backfill_nyse_security_master_sample.py` |
| TradingView free scanner ISIN candidates | `scripts/backfill_tradingview_missing_isins.py` |
| TradingView free scanner stock-sector candidates | `scripts/backfill_tradingview_stock_sectors.py` |
| Daily symbol-change feed | `scripts/fetch_symbol_changes.py` |
| FinancialData.net symbol match | `scripts/fetch_financialdata_symbols.py` |
| FinancialData.net official-ISIN supplements | `scripts/build_financialdata_isin_supplements.py` |

Review queue:

```bash
python3 scripts/build_entry_quality_report.py
python3 scripts/build_b3_residual_isin_review.py
python3 scripts/build_b3_residual_sector_review.py
python3 scripts/build_otc_scope_review.py
python3 scripts/build_canada_residual_review.py
python3 scripts/build_canada_figi_queue.py
python3 scripts/probe_canada_figi_batch.py --batch-id canada-figi-0001 --limit 10
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/audit_dataset.py --write-defaults
python3 scripts/run_claude_review_queue.py --model sonnet --skip-existing
python3 scripts/build_claude_review_overrides.py --min-confidence 0.8
python3 scripts/rebuild_dataset.py
```

## Sources

The primary-ticker universe covers 86 exchanges. Source coverage is explicit: 48 exchanges are `official_full`, 33 are `official_partial`, and 6 current-scope exchanges have an official-source candidate awaiting parser implementation; a partial listing-company page or security lookup is never presented as a complete exchange directory. Implemented primary exchange/reference inputs include Nasdaq Trader, Nasdaq Nordic, ASX, Deutsche Boerse, B3, TMX, Euronext, JPX/TSE, TWSE, TPEX, SSE/SZSE, Bursa Malaysia, BME, BMV, WSE/NewConnect, TASE, KRX, HOSE/HNX/UPCOM, CSE Sri Lanka, and SEC company tickers.

Official source candidates and reconciled source gaps are tracked in [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) and summarized by [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md). Current source coverage status: `5` missing current-scope exchanges, `5` parser todo rows, `0` real global-expansion candidates, `49` official-full exchanges, and `33` official-partial exchanges. Remaining work includes source-parser backlog plus field-completion and taxonomy coverage.

[`data/reports/exchange_source_audit.md`](data/reports/exchange_source_audit.md) is the one-row-per-exchange operational audit for product-class gaps, source freshness and availability, official denominators, recall, blocker class, and promotion readiness. [`data/masterfiles/exchange_scope_decisions.csv`](data/masterfiles/exchange_scope_decisions.csv) gives every partial exchange a distribution-safe public scope. Its `reason_code` records the blocker observed at review time; validation remains fail-closed by requiring the current audit to contain a known promotion blocker, without invalidating a safe retained scope merely because another blocker becomes higher priority. A current audit with no blocker requires explicit scope review. Commercial products are only evaluation candidates: [`data/masterfiles/commercial_source_options.csv`](data/masterfiles/commercial_source_options.csv) does not grant redistribution rights, and its validator requires explicit contract review or prior written permission.

Secondary/reviewed enrichment inputs include [EODHD](https://eodhd.com/financial-apis/), [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase), official B3 COTAHIST files, NYSE Group Security Master sample files, TradingView free scanner metadata, XTB OMI specification data, Yahoo Finance review helpers, [FinancialData.net](https://financialdata.net/documentation) symbol-universe matching, OpenFIGI, GLEIF, and curated production aliases from [api.adanos.org](https://api.adanos.org).

FinancialData.net output is intentionally review-only: the international-symbols endpoint has `trading_symbol` and `registrant_name`, but no ISIN or sector. The sync writes [`data/financialdata/international_stock_symbols.csv`](data/financialdata/international_stock_symbols.csv), [`data/reports/financialdata_symbol_match.md`](data/reports/financialdata_symbol_match.md), [`data/reports/financialdata_current_exchange_gaps.csv`](data/reports/financialdata_current_exchange_gaps.csv), and [`data/reports/financialdata_global_expansion_candidates.csv`](data/reports/financialdata_global_expansion_candidates.csv). Missing rows are split into current-exchange gaps and global expansion candidates. The follow-up [`scripts/build_financialdata_isin_supplements.py`](scripts/build_financialdata_isin_supplements.py) only writes supplemental core rows when the FinancialData discovery row matches an official active masterfile row with a valid ISIN, name gate, no existing global ticker, and no existing/selected ISIN.

## Project

- License: [MIT](LICENSE)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- M2 operations: [docs/m2_operations.md](docs/m2_operations.md)
- Releases: [GitHub Releases](https://github.com/adanos-software/free-ticker-database/releases)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
