# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Free stock and ETF ticker reference data with primary tickers, listing-keyed venue rows, aliases, ISIN/WKN identifiers, cross-listings, and coverage reports.

## Snapshot

| Metric | Value |
|---|---:|
| Primary tickers | 53,826 |
| Full listing rows | 61,956 |
| Stocks | 38,735 |
| ETFs | 15,091 |
| Exchanges | 67 |
| Countries | 69 |
| Aliases | 91,697 |
| ISIN coverage | 45,375 (84.3%) |
| Sector/category coverage | 41,738 (77.5%) |
| Stock sector coverage | 31,152 |
| ETF category coverage | 10,586 |
| Core listing-scope rows | 45,413 |
| Core primary rows with ISIN | 38,324 |
| Core primary rows missing ISIN | 7,089 |
| Extended listing-scope rows | 16,543 |

## Core Files

| File | Use |
|---|---|
| [`data/tickers.csv`](data/tickers.csv) | Canonical primary ticker export, one row per security |
| [`data/listings.csv`](data/listings.csv) | Full listing-level export keyed by `listing_key` |
| [`data/instrument_scopes.csv`](data/instrument_scopes.csv) | Core vs. extended listing scope and primary-listing links |
| [`data/aliases.csv`](data/aliases.csv) | Alias/name/identifier lookup |
| [`data/identifiers.csv`](data/identifiers.csv) | Compact ISIN/WKN lookup |
| [`data/cross_listings.csv`](data/cross_listings.csv) | Same-ISIN listings across exchanges |
| [`data/tickers.json`](data/tickers.json) | JSON export for APIs and apps |
| [`data/tickers.parquet`](data/tickers.parquet) | Analytics export |
| [`data/tickers.db`](data/tickers.db) | SQLite export |

Reference and audit files:

| File | Use |
|---|---|
| [`data/listing_index.csv`](data/listing_index.csv) | Listing-keyed identity bridge |
| [`data/identifiers_extended.csv`](data/identifiers_extended.csv) | FIGI/CIK/LEI enrichment snapshot |
| [`data/masterfiles/reference.csv`](data/masterfiles/reference.csv) | Official exchange-masterfile reference rows |
| [`data/masterfiles/supplemental_listings.csv`](data/masterfiles/supplemental_listings.csv) | Safe official listings added to the core export |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | Machine-readable coverage report |
| [`data/reports/completion_backlog.md`](data/reports/completion_backlog.md) | Prioritized missing ISIN/sector/category backlog |
| [`data/reports/masterfile_collision_report.json`](data/reports/masterfile_collision_report.json) | Official-symbol gaps blocked by ticker collisions |

## Data Model

`tickers.csv` is the backward-compatible primary export:

```csv
ticker,name,exchange,asset_type,sector,stock_sector,etf_category,country,country_code,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,Consumer Staples,,United States,US,US1912161007,191216|coca-cola|850663
```

`listings.csv` is the full venue export:

```csv
listing_key,ticker,exchange,name,asset_type,sector,stock_sector,etf_category,country,country_code,isin,aliases
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc.,Stock,Information Technology,Information Technology,,United States,US,US0378331005,apple|865985
```

Important rules:

- `ticker` is globally unique only in `tickers.csv`; use `listing_key` for venue-level identity.
- `sector` is legacy output. Stocks use `stock_sector`; ETFs use `etf_category`.
- `instrument_scopes.csv` marks `core`, OTC `extended`, and secondary cross-listings.
- Core rows without ISIN are tagged as `scope_reason=primary_listing_missing_isin`.
- Secondary listings stay in `listings.csv` and `cross_listings.csv`; `tickers.csv` keeps one primary row per security.

JSON metadata:

```json
{
  "_meta": {
    "version": "3.7.0",
    "built_at": "2026-04-12T14:24:33Z",
    "total_tickers": 53826
  },
  "tickers": []
}
```

SQLite tables: `tickers`, `listings`, `aliases`, `cross_listings`, and `instrument_scopes`.

## Quality

- Valid ISINs are checksum-verified.
- Obvious common-word, wrapper, celebrity, product, junk, short, and numeric aliases are filtered.
- Rights, units, warrants, notes, preferreds, and depositary lines are filtered from the stock universe.
- Foreign OTC country metadata is corrected from valid ISIN prefixes where possible.
- Official masterfiles are kept separate from secondary sources.
- Yahoo, EODHD, XTB, and FinanceDatabase are treated as reviewed candidate sources, not as exchange authority.
- Local probe/test artifacts are ignored via `output/` and `test-results/`; committed artifacts live under `data/`.

## Coverage

Top exchanges by primary ticker count:

| Exchange | Tickers |
|---|---:|
| OTC | 8,413 |
| NASDAQ | 4,538 |
| LSE | 3,892 |
| TSE | 3,197 |
| SZSE | 3,083 |
| SSE | 2,789 |
| NYSE ARCA | 2,571 |
| XETRA | 2,242 |
| NYSE | 2,043 |
| KRX | 1,788 |
| TSX | 1,658 |
| KOSDAQ | 1,583 |
| B3 | 1,506 |
| ASX | 1,291 |
| TWSE | 1,242 |

For full exchange, country, source, and verification coverage, use:

```bash
python3 scripts/build_coverage_report.py
python3 scripts/build_completion_backlog.py
```

## Refresh Pipeline

Quick rebuild:

```bash
python3 scripts/rebuild_dataset.py
python3 scripts/build_listing_history.py
python3 scripts/build_coverage_report.py
python3 scripts/build_completion_backlog.py
```

Planned enrichment run:

```bash
python3 scripts/run_enrichment_pipeline.py --dry-run
```

Use `--include-secondary-network` for EODHD/Yahoo candidate stages. Use `--apply-reviewed-backfills` only when reviewed candidates should be merged into overrides.

Main targeted backfills:

| Task | Script |
|---|---|
| Official masterfiles | `scripts/fetch_exchange_masterfiles.py` |
| Safe official supplements | `scripts/build_masterfile_supplements.py` |
| Extended FIGI/CIK/LEI identifiers | `scripts/enrich_global_identifiers.py` |
| Same-ISIN sector/category peers | `scripts/backfill_sector_from_isin_peers.py` |
| FinanceDatabase sectors | `scripts/backfill_financedatabase_metadata.py` |
| EODHD ISIN candidates | `scripts/backfill_eodhd_metadata.py` |
| XTB OMI ISIN candidates | `scripts/backfill_xtb_omi_isins.py` |
| Yahoo OTC ISIN candidates | `scripts/backfill_yahoo_otc_isins.py` |
| ASX official ISINs | `scripts/backfill_asx_isins.py` |

Review queue:

```bash
python3 scripts/audit_dataset.py --write-defaults
python3 scripts/run_claude_review_queue.py --model sonnet --skip-existing
python3 scripts/build_claude_review_overrides.py --min-confidence 0.8
python3 scripts/rebuild_dataset.py
```

## Sources

Primary exchange/reference inputs include Nasdaq Trader, Nasdaq Nordic, ASX, Deutsche Boerse, B3, TMX, Euronext, JPX, TWSE, TPEX, Bursa Malaysia, BME, BMV, WSE/NewConnect, TASE, KRX, HOSE/HNX/UPCOM, and SEC company tickers.

Secondary/reviewed enrichment inputs include [EODHD](https://eodhd.com/financial-apis/), [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase), XTB OMI specification data, Yahoo Finance review helpers, OpenFIGI, GLEIF, and curated production aliases from [api.adanos.org](https://api.adanos.org).

## Project

- License: [MIT](LICENSE)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Releases: [GitHub Releases](https://github.com/adanos-software/free-ticker-database/releases)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
