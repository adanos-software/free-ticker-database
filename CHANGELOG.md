# Changelog

## Unreleased

- No unreleased changes yet.

## 2.0.0

- Current dataset release with 59,178 tickers (43,086 stocks, 16,092 ETFs) across 67 exchanges and 68 countries
- Output formats: CSV, JSON, Parquet, SQLite
- JSON outputs use a `_meta` envelope with version/build metadata
- Added `country_code` (ISO 3166-1 alpha-2) to dataset exports
- Added `cross_listings.csv` and SQLite `cross_listings` for multi-exchange securities
- 103,882 aliases with type classification (`isin`, `wkn`, `name`, `exchange_ticker`)
- ISIN coverage: 75.6%
- Sector coverage: 65.7%
- Quality filters include duplicate removal, alias cleanup, stock-universe instrument filtering,
  ISIN-based country correction, sector normalization, and ISIN check-digit validation
