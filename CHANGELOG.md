# Changelog

## [Unreleased]

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
