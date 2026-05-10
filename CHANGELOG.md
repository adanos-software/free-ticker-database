# Changelog

## [Unreleased]

### Added

- Added free-source B3 COTAHIST and NYSE Group Security Master sample backfills for review-gated ISIN and ETF-category fills.
- Added review-gated TradingView free scanner backfills for missing ISINs and stock-sector candidates across supported venues.

### Changed

- Rebuilt canonical exports to 61,653 primary tickers, 71,092 listing rows, 58,477 ISIN-covered rows, 57,123 sector/category-covered rows, and 120,454 structured alias rows.
- Reduced the field-completion backlog to 1,971 missing core-primary ISINs, 4,292 missing stock sectors, and 238 missing ETF categories.
- Refreshed coverage, completion-backlog, Adanos reference, detection simulation, entry-quality, and validation artifacts.

## [3.20.0] - 2026-05-05

### Added

- Added official HKEX HSIC browser-backed sector backfill for Hong Kong equities after matching live HKEX quote-page industry metadata to the official HKEX securities workbook.
- Added official B3 and BSE India sector backfills, plus a reviewed StockAnalysis OTC metadata backfill for selected OTC rows.
- Added broader database validation gates covering core listings, listing index, cross-listing pairs, instrument scopes, identifier summaries, FIGI/ISIN collisions, canonical sector/category values, metadata override leakage, entry-quality coverage, duplicate public aliases, and trimmed Adanos names.

### Changed

- Rebuilt canonical exports to 61,844 primary tickers, 71,092 listing rows, 56,704 ISIN-covered rows, 55,843 sector/category-covered rows, and 118,657 structured alias rows.
- Increased stock-sector coverage to 40,711 rows and kept ETF category coverage at 15,132 rows after normalizing legacy ETF-category override values to canonical buckets.
- Reduced the field-completion backlog to 3,917 missing core-primary ISINs, 5,549 missing stock sectors, and 452 missing ETF categories.
- Tightened identifier enrichment by dropping stale ISIN carry-forward and ambiguous/stale OpenFIGI mappings instead of preserving questionable FIGI coverage.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Fixed case/normalization-sensitive alias duplicate detection so equivalent natural-language aliases are held for review instead of exported to Adanos detection.
- Fixed stale identifier propagation where `identifiers_extended.csv` could retain ISINs after the authoritative listing row no longer had one.
- Fixed Adanos ticker-reference name hygiene by trimming API export text fields and adding a release gate for untrimmed names.

## [3.19.0] - 2026-05-04

### Added

- Added official SGX marketmetadata v2 enrichment to the SGX securities-prices parser, filling official ISINs for almost all current SGX rows from SGX-provided identifier metadata.
- Expanded deterministic ETF-name category rules for volatility, fixed-income, municipal, duration, multi-asset, leveraged/inverse, digital-asset, commodity, real-estate, technology, and equity-index products.

### Changed

- Rebuilt canonical exports to 61,846 primary tickers, 71,092 listing rows, 56,675 ISIN-covered rows, 51,713 sector/category-covered rows, and 118,631 structured alias rows.
- Increased ETF category coverage to 15,098 rows and reduced the ETF category backlog from 892 to 486 rows.
- Reduced missing core-primary ISIN rows to 3,941 after the official SGX metadata refresh.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Updated ETF outlier expectations for Yahoo-corrected ETF rows that are now deterministically categorized by the ETF-name classifier.

## [3.18.0] - 2026-05-04

### Added

- Added official-source ETF category normalization for SSE, SZSE, B3, and KRX exchange-masterfile feeds.
- Added OpenFIGI-backed FIGI enrichment to the listing-keyed extended identifier snapshot.

### Changed

- Rebuilt canonical exports to 61,941 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 51,364 sector/category-covered rows, and 118,208 structured alias rows.
- Expanded ETF category coverage to 14,710 rows and reduced the ETF category backlog to 876 entry-quality source gaps.
- Expanded listing-keyed FIGI coverage to 63,603 rows while keeping missing primary ISIN rows review-gated by venue source.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Let `scripts/enrich_global_identifiers.py` read `OPENFIGI_API_KEY` from the environment for keyed FIGI refreshes without passing secrets as CLI arguments.

## [3.17.0] - 2026-05-04

### Added

- Added the listing-key-first core data model with collision-safe `listing_key` identity across core, listing, alias, scope, and report exports.
- Added listing-scope exports and collision reports so global ticker collisions no longer block venue-level official rows.

### Changed

- Rebuilt canonical exports to 61,941 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 50,898 sector/category-covered rows, and 118,208 structured alias rows.
- Reduced legacy primary ticker collision exposure to one compatibility row while preserving official venue rows in listing-keyed exports.
- Refreshed validation, source inventory, completion backlog, entry-quality, alias-quality, Adanos reference, and listing-history artifacts for the listing-key-first model.

## [3.16.0] - 2026-05-03

### Added

- Added official supplemental-listing coverage for ADX, Bahrain Bourse, Borsa Istanbul, Boursa Kuwait, BSE India, CSE Sri Lanka, DFM, HKEX, MSX, NSE India, NZX, QSE, SGX, and Saudi Exchange.
- Added StockAnalysis batch controls and exchange mappings for BIST, HKEX, NSE India, SGX, and BSE India review runs.
- Added broader deterministic ETF category rules for money-market, fixed-income, commodity, real-estate, alternative, large-cap, factor, and equity-index product names.

### Changed

- Rebuilt canonical exports to 61,984 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 49,846 sector/category-covered rows, and 118,209 structured alias rows.
- Expanded ETF category coverage to 13,152 rows while keeping non-matching active/structured ETF products in the review backlog instead of applying a generic ETF fallback.
- Reduced the field-completion backlog to 4,578 missing core-primary ISINs, 9,686 missing stock sectors, and 2,452 missing ETF categories.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts for the expanded dataset.

### Fixed

- Removed BSE India fund plan and segregated-portfolio lines from the stock universe.
- Preserved official sector/category metadata when building safe supplemental listings.
- Updated cross-listing expectations for Microsoft after HKEX official coverage added `HKEX::04338`.

## [3.15.0] - 2026-04-22

### Added

- Added a persistent OTC review-decision workflow with committed `otc_review_decisions.csv` overrides so reviewed keep-current and hold-unresolved cases no longer re-open on every rebuild.
- Added release-tested OTC review suppressions and queue handling for reviewed stale-name cases in the entry-quality and OTC review reports.

### Changed

- Resolved the remaining active OTC name-mismatch queue to zero by applying reviewed issuer-name, ISIN, country, and drop decisions for the final unresolved OTC cases.
- Rebuilt canonical exports to 54,037 primary tickers, 62,539 listing rows, 48,900 ISIN-covered rows, 45,163 sector/category-covered rows, and 103,580 structured alias rows.
- Refreshed validation, listing-history, identifier, coverage, completion-backlog, OTC review, override-debt, and Adanos reference artifacts against the cleaned dataset.

## [3.14.0] - 2026-04-20

### Added

- Added targeted metadata refreshes for B3, TMX, LSE, and related review-gated coverage gaps.
- Added official B3 ETF and BDR fund rows plus refreshed official B3 ISIN coverage from the latest B3 masterfile data.
- Added reviewed FinanceDatabase stock-sector overrides for selected LSE investment trust and energy listings.

### Changed

- Rebuilt canonical exports to 54,026 primary tickers, 62,496 listing rows, 48,808 ISIN-covered rows, 45,006 sector/category-covered rows, and 103,499 structured alias rows.
- Reduced core primary rows missing ISIN to 3,920 and refreshed validation, source inventory, completion backlog, entry-quality, alias-quality, Adanos reference, and listing-history artifacts.
- Fixed the scheduled symbol-changes workflow by preventing duplicate GitHub auth headers and upgrading `peter-evans/create-pull-request` to `v8.1.1`.

## [3.13.0] - 2026-04-18

### Added

- Added a central database release-gate validator with JSON/Markdown reports for structural integrity, ISIN validity, listing scope consistency, Adanos alias safety, and coverage-report coherence.
- Added an Adanos Sentiment API detection simulator that smoke-tests natural-language ticker aliases against positive and negative text probes.
- Added CI enforcement for the database release-gate validator.

### Changed

- Blocked generic organization aliases such as `central bank` from natural-language detection exports after the simulator identified a false-positive risk.
- Rebuilt canonical exports to 54,020 primary tickers, 62,496 listing rows, 48,794 ISIN-covered rows, 44,996 sector/category-covered rows, and 103,489 structured alias rows.

## [3.12.1] - 2026-04-18

### Fixed

- Updated the entry-quality warn allowlist to match the reviewed v3.12.0 warning queue so CI blocks only new warnings.
- Updated GitHub Actions workflow dependencies to Node 24-compatible `actions/checkout@v6` and `actions/setup-python@v6`.

## [3.12.0] - 2026-04-17

### Added

- Added an alias-policy module, alias quality reports, and Adanos Sentiment API-safe ticker-reference exports with natural-language detection policies.

### Changed

- Restricted `tickers.csv.aliases` to conservative natural-language aliases while keeping ISIN/WKN and exchange-ticker identifiers in structured alias/identifier exports.
- Normalized API aliases by stripping security/legal suffixes, removing trademark/non-ASCII symbols, shortening ETF product names, deriving concise company aliases, and dropping cross-exchange alias contamination.
- Rebuilt canonical exports to 54,020 primary tickers, 62,496 listing rows, 48,794 ISIN-covered rows, 44,996 sector/category-covered rows, and 103,490 structured alias rows.

## [3.11.0] - 2026-04-17

### Added

- Added FinancialData.net international-symbol ingestion as a secondary discovery source with match, current-gap, and global-expansion reports.
- Added an official-ISIN supplement builder that accepts FinancialData-discovered rows only after matching an official active masterfile row with a valid ISIN, issuer-name gate, and no global ticker/ISIN collision.
- Added persistent FinancialData review artifacts and tests so accepted official-ISIN supplements remain idempotent across rebuilds.

### Changed

- Rebuilt the canonical exports to 53,998 primary tickers, 62,496 listing rows, 48,787 ISIN-covered rows, and 44,884 sector/category-covered rows.
- Expanded official-ISIN-backed coverage with 555 supplemental rows across NSE India, HKEX, Bursa, KRX, LSE, BSE India, and B3 while keeping FinancialData itself review-only.
- Refreshed listing history, identifier snapshots, completion backlog, source inventory, entry-quality reports, and README metrics against the expanded dataset.

## [3.10.1] - 2026-04-14

### Fixed

- Added `lxml` as an explicit dependency so CI can run `pandas.read_html` parser tests on fresh GitHub Actions runners.

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
