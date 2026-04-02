# Changelog

## Unreleased

- Added official ASX company-directory ingestion and TMX interlisted-company reference ingestion alongside Nasdaq Trader feeds
- Added official Euronext live equities directory ingestion
- Added SEC official-snapshot cache fallback for environments blocked from live SEC downloads
- Added listing snapshot/history artifacts for listings, renames, and delistings
- Improved extended identifier exports with listing-level FIGI matching, retry-safe partial progress, and conservative LEI backfills
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
