# Changelog

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
