# Changelog

## Unreleased

- Initial public release
- 60,109 tickers (44,015 stocks, 16,094 ETFs) across 67 exchanges and 67 countries
- Output formats: CSV, JSON, Parquet, SQLite
- Version `2.0.0` introduces a `_meta` envelope for JSON outputs with version/build metadata
- Added `country_code` (ISO 3166-1 alpha-2) to dataset exports
- 107,074 aliases with type classification (ISIN, WKN, name, exchange_ticker)
- ISIN coverage: 76.2%
- Sector coverage: 66.1%
- Quality filters: rights/warrants/preferred exclusion, common-word alias removal,
  ISIN-based country correction, namespace collision detection
