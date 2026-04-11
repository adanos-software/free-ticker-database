# Changelog

## [Unreleased]

### Added

- Added a gated Yahoo Finance OTC ISIN backfill workflow that writes accepted ISINs to review overrides only when venue, quote type, issuer name, and ISIN checksum all pass.

### Changed

- Enriched 1,103 OTC rows with Yahoo Finance ISIN overrides and rebuilt core exports, reducing OTC rows without ISIN in `tickers.csv` from 2,524 to 1,421.

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
