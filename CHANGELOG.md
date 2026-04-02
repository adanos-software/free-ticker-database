# Changelog

## Unreleased

- Added official masterfile ingestion scaffolding for Nasdaq Trader and SEC reference feeds
- Added listing snapshot/history artifacts for listings, renames, and delistings
- Added extended identifier exports with support for FIGI, CIK, and LEI enrichment
- Added exchange/country coverage reports beyond the alias-quality audit

## 2.0.0

- Current dataset release with 59,177 tickers (43,085 stocks, 16,092 ETFs) across 67 exchanges and 68 countries
- Output formats: CSV, JSON, Parquet, SQLite
- JSON outputs use a `_meta` envelope with version/build metadata
- Added `country_code` (ISO 3166-1 alpha-2) to dataset exports
- Added `cross_listings.csv` and SQLite `cross_listings` for multi-exchange securities
- 102,943 aliases with type classification (`isin`, `wkn`, `name`, `exchange_ticker`)
- ISIN coverage: 76.1%
- Sector coverage: 65.7%
- Quality filters include duplicate removal, alias cleanup, stock-universe instrument filtering,
  ISIN-based country correction, sector normalization, and ISIN check-digit validation
