# Changelog

## [Unreleased]

## [3.10.0] - 2026-04-14

### Added

- Added a source-inventory backlog builder (`data/reports/source_inventory_gap.*`) that reconciles official full, official partial, normalization-alias, and global-expansion candidates against the coverage report.
- Added curated official source candidates in `data/masterfiles/source_candidates.json` so source gaps have explicit implementation status, blockers, review policy, and provenance.
- Added official CSE Sri Lanka company-info detail coverage with 310 ISIN-bearing reference rows while keeping CSE_LK out of the core export until reliable sector taxonomy is available.
- Added reviewed StockAnalysis metadata backfill tooling for tightly gated secondary ISIN and stock-sector fills.

### Changed

- Rebuilt the canonical exports to 53,446 primary tickers, 61,944 listing rows, 48,235 ISIN-covered rows, and 44,865 sector/category-covered rows.
- Reconciled the source inventory to 0 missing current-scope sources, 0 parser todo rows, and 0 real global-expansion candidates; remaining work is field completion and taxonomy coverage.
- Refreshed official masterfile reference coverage, completion backlog, listing history, identifier snapshots, and coverage reports against the expanded source set.

## [3.9.0] - 2026-04-13

### Changed

- Removed the legacy `sector` column from public ticker, listing, JSON, Parquet, SQLite, and latest-snapshot exports; consumers should use `stock_sector` for stocks and `etf_category` for ETFs.
- Rebuilt coverage, completion-backlog, identifier, and listing-history artifacts against the typed sector/category schema.

### Breaking

- Public exports no longer include the duplicated `sector` field.

## [3.8.0] - 2026-04-13

### Added

- Added a local Gemma plausibility-review workflow with resumable checkpoints, human-readable `error.txt` output, accepted-false-positive overrides, and tests for stale finding reconciliation.
- Added reviewed LLM plausibility accepts for known false positives so local model output does not masquerade as authoritative exchange evidence.

### Changed

- Completed the local Gemma pass across the primary ticker export with zero active structured data findings; remaining review-error rows are parse/retry cases.
- Applied reviewed Yahoo, XTB, and official cross-market reference overrides to replace or clear same-ticker cross-exchange ISIN contamination.
- Rebuilt the canonical exports to 53,789 primary tickers, 61,955 listing rows, 45,281 ISIN-covered rows, and 41,714 sector/category-covered rows.
- Tightened dataset audit coverage for invalid ISIN, country/ISIN, and alias-contamination findings; the rebuilt audit now reports zero flagged entries.

## [3.7.0] - 2026-04-12

### Added

- Added a field-level completion backlog report (`data/reports/completion_backlog.*`) that splits missing primary ISINs, stock sectors, and ETF categories by exchange and review policy.
- Added a reproducible enrichment pipeline orchestrator for masterfile refreshes, backlog builds, reviewed local backfills, rebuilds, coverage reports, and audit queue refreshes.
- Added a deterministic ETF-name category backfill that writes reviewed `etf_category` fills while keeping legacy `sector` output derived for compatibility.

### Changed

- Rebuilt coverage to 45,375 ISIN-covered rows and 41,738 sector/category-covered rows, reducing the ETF category backlog from 8,298 to 4,505 rows.
- Added typed metadata outputs for `stock_sector` and `etf_category` across CSV, JSON, Parquet, SQLite, listing history, audit, coverage, and review workflows while retaining `sector` as a legacy derived field.
- Switched listing-history identity comparisons to `listing_key` while preserving the existing event output order.
- Switched coverage-report identifier and masterfile-collision lookups to listing-key identity with ticker/exchange fallback for legacy inputs.
- Added `listing_key` to `identifiers_extended.csv` so FIGI/CIK/LEI enrichment rows are explicitly listing-keyed while preserving `ticker` and `exchange`.
- Documented ignored local probe/test artifacts and cleaned stale ETF-category provenance wording in the enrichment pipeline.

## [3.6.0] - 2026-04-12

### Added

- Added a FinanceDatabase metadata backfill workflow that applies ticker/exchange/asset/name gates and keeps ISIN updates disabled by default unless `--enable-isin` is explicitly used for reviewed batches.
- Added an EODHD exchange-symbol-list ISIN backfill workflow that reads the API key from `EODHD_API_TOKEN`, writes audit reports outside tracked data, and defaults to accepting only new ISINs that do not already exist in the primary export.
- Added a same-ISIN listing-peer sector/category propagation workflow with conflict detection and audit reports.
- Added Alpha Vantage, SEC SIC, JPX/TSE, and XTB-backed enrichment helpers for reviewed sector and identifier batches.

### Changed

- Enriched 1,746 sector rows and 14 strictly gated NYSE ETF ISIN rows, rebuilding coverage to 44,145 ISIN-covered rows and 33,573 sector-covered rows out of 52,747 primary tickers.
- Kept FinanceDatabase ISIN candidates out of the default pipeline after detecting cross-listing collision risk; accepted FinanceDatabase output is sector-only unless identifier review is explicitly enabled.
- Enriched 90 additional ISIN rows with strictly gated EODHD exchange-symbol-list candidates, rebuilding coverage to 44,235 ISIN-covered rows while keeping the primary ticker export stable at 52,747 rows.
- Enriched 346 additional sector/category rows from same-ISIN listing peers, rebuilding sector coverage to 33,919 rows while keeping the primary ticker export stable at 52,747 rows.
- Refreshed SSE, SZSE, TMX, Bursa, and IDX official masterfile inputs and rebuilt the canonical exports to 53,826 primary tickers and 61,956 listing rows.
- Hardened supplemental-listing rebuilds so official partial rows no longer overwrite existing issuer name, asset type, country, or country-code metadata.
- Tightened reviewed FinanceDatabase ISIN backfills with existing-ISIN peer-name conflict checks before accepting identifier overrides.
- Rebuilt coverage to 45,375 ISIN-covered rows and 37,945 sector-covered rows, with 38,324 core primary rows now carrying ISINs and 7,089 core primary rows still explicitly scoped as `primary_listing_missing_isin`.

## [3.5.0] - 2026-04-12

### Added

- Added a gated Yahoo Finance OTC ISIN backfill workflow that writes accepted ISINs to review overrides only when venue, quote type, issuer name, and ISIN checksum all pass.
- Added a gated official ASX `ISIN.xls` backfill workflow for missing ASX ISINs.
- Added a strict selected-exchange Yahoo missing-ISIN helper for US ETF batches with venue, quote type, expected ISIN country prefix, issuer/product-name, numeric-token, checksum, and progress gates.

### Changed

- Enriched 1,103 OTC rows with Yahoo Finance ISIN overrides and rebuilt core exports, reducing OTC rows without ISIN in `tickers.csv` from 2,524 to 1,421.
- Enriched 154 ASX rows with official ASX ISIN overrides and rebuilt core exports and coverage reports.
- Enriched 485 BATS ETF rows, 496 NASDAQ ETF rows, and 161 NYSE ARCA ETF rows with strictly gated secondary Yahoo ISIN overrides and rebuilt core exports and coverage reports to 44,131 ISIN-covered rows out of 52,747 primary tickers.
- Made rebuild country handling idempotent for foreign listings by deriving issuer country from valid ISIN prefixes instead of repeatedly clearing and restoring cross-listing ISINs.

## [3.4.0] - 2026-04-11

### Added

- Added official ISIN propagation from B3 `InstrumentsEquities` for Brazilian cash equities, ETF/fund lines, and ETF/ETP BDR rows.
- Added explicit `primary_listing_missing_isin` instrument-scope classification so ISIN-ready core rows can be filtered cleanly.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, verification runs, and coverage reports to the 2026-04-11 build.
- Expanded KRX/KOSDAQ and LSE/XETRA official reference coverage and corrected stale XETRA metadata where official venue data had safer ISINs.
- Improved B3 cash-instrument classification for `FUNDS`, BDR ETF/ETP rows, and uncategorized Brazilian stock classes.
- Improved global ISIN coverage to 41,846 rows, kept core primary rows missing ISIN explicitly scoped at 8,771, and improved ETF verification to 16,125 verified rows with 449 reference gaps.

## [3.3.0] - 2026-04-10

### Added

- Added official IDX coverage and promoted IDX to zero unresolved gaps.
- Added official TPEX mainboard stock coverage via the TWSE MOPS `t187ap03_O.csv` feed and completed TPEX stock/ETF reference coverage.
- Added official TASE `searchentities` supplements for foreign ETFs and participating units.
- Added official SZSE B-share coverage via the `ShowReport` `TABKEY=tab2` feed.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, verification runs, and coverage reports to the 2026-04-10 build.
- Reduced Stockholm unresolved gaps with additional official NGM, Spotlight, and Nasdaq Nordic mappings plus stale-listing cleanup.
- Reduced TASE unresolved gaps from 26 to 14 by normalizing legacy `PSG-*` ETF rows to current official `IBI.*` listings where the mapping was uniquely supported.
- Resolved remaining venue tails for TSX, TSXV, BMV, SZSE, and TPEX using official source expansions and conservative stale-row cleanup.
- Improved global verification coverage to 31,379 verified stocks with 12,363 stock reference gaps and 15,964 verified ETFs with 601 ETF reference gaps.

## [3.2.0] - 2026-04-09

### Added

- Added official Philippine Stock Exchange coverage via the PSE listed company directory frame feed, including active common, preferred, and ETF listings.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, and coverage reports to the 2026-04-09 PSE coverage build.
- Promoted PSE from a missing venue to official full coverage and pulled official names and ISINs into the rebuilt dataset for code-like preferred share rows.
- Hardened masterfile row deduplication so partial source refreshes no longer fail on `None` values in incoming reference rows.

## [3.1.0] - 2026-04-09

### Added

- Added official TPEX ETF coverage via the TPEx ETF InfoHub export.
- Added a TMX GraphQL ETF fallback/merge path so TSX/TSXV ETF reference coverage no longer depends on a single screener response shape.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, and coverage reports to the 2026-04-09 build.
- Prefer current official TMX ETF listings over stale same-venue duplicate fund rows when the product identity already matches on the exchange.
- Extended verification to accept same-exchange ISIN matches and official SIX ETF/ETP fund products where the reference source is authoritative.

### Fixed

- Fixed TPEX ETF parsing for numeric bond-style symbols such as `00679B`.
- Reduced TSX ETF reference gaps caused by stale legacy symbols that still pointed to the same underlying fund product.

## [3.0.0] - 2026-04-07

### Breaking

- `data/tickers.csv` is now a primary-listing-only core export. Secondary venue lines no longer appear in the canonical flat dataset.
- Multi-venue identity is now represented explicitly through `data/listings.csv`, `data/listing_index.csv`, and `data/cross_listings.csv`.

### Added

- Added stable listing-keyed exports and cross-listing artifacts for downstream systems that need every venue line without global ticker ambiguity.
- Added broader official reference coverage for LSE, TMX, SSE ETFs, KRX/KOSDAQ, XETRA stocks, and XETRA ETFs/ETPs.
- Added richer machine-readable coverage reporting via `data/reports/coverage_report.json` and `data/reports/masterfile_collision_report.json`.

### Changed

- Tightened the stock core export to exclude preferreds, depositary lines, corporate-action rows, and other non-canonical secondary lines.
- Improved venue normalization so the core export prefers official primary markets when the same issuer trades across multiple exchanges.

### Fixed

- Replaced the LSE HTML parsing path with a dependency-light stdlib parser so CI no longer depends on optional `lxml` support.
