# Changelog

## Unreleased

- Added official ASX company-directory ingestion and TMX interlisted-company reference ingestion alongside Nasdaq Trader feeds
- Added official Euronext live equities directory ingestion
- Added official JPX listed-issues ingestion plus a conservative masterfile supplement layer for collision-free TSE listings
- Expanded the conservative official supplement layer to safe ASX, AMS, and OSL listings
- Added SEC official-snapshot cache fallback for environments blocked from live SEC downloads
- Activated live CIK enrichment from the official SEC snapshot and persisted the cache for repeatable runs
- Added `listing_index.csv` as a listing-keyed bridge artifact using `exchange::ticker`
- Added masterfile collision reporting to separate official-symbol matches, collisions, and true missing rows
- Added listing snapshot/history artifacts for listings, renames, and delistings
- Improved extended identifier exports with listing-level FIGI matching, exact listing-level CIK matching, retry-safe partial progress, and ISIN-first LEI backfills
- Added exchange/country coverage reports beyond the alias-quality audit
- Added chunked stock-universe verification against official masterfiles plus conservative override generation for stale, misclassified, and non-common listings

## 2.0.0

- Current dataset release with 62,998 tickers (46,305 stocks, 16,693 ETFs) across 68 exchanges and 68 countries
- Output formats: CSV, JSON, Parquet, SQLite
- JSON outputs use a `_meta` envelope with version/build metadata
- Added `country_code` (ISO 3166-1 alpha-2) to dataset exports
- Added `cross_listings.csv` and SQLite `cross_listings` for multi-exchange securities
- 101,693 aliases with type classification (`isin`, `wkn`, `name`, `exchange_ticker`)
- ISIN coverage: 70.6%
- Sector coverage: 62.2%
- Quality filters include duplicate removal, alias cleanup, stock-universe instrument filtering,
  ISIN-based country correction, sector normalization, and ISIN check-digit validation
