# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Free stock and ETF ticker reference data with primary tickers, listing-keyed venue rows, aliases, ISIN/WKN identifiers, cross-listings, and coverage reports.

## Snapshot

| Metric | Value | Meaning |
|---|---:|---|
| Primary tickers | 53,446 | Rows in `data/tickers.csv`; one primary row per security. |
| Full listing rows | 61,944 | Rows in `data/listings.csv`; venue-level rows keyed by `listing_key`, including cross/secondary listings. |
| Stocks | 38,437 | Primary ticker rows where `asset_type=Stock`. |
| ETFs | 15,009 | Primary ticker rows where `asset_type=ETF`. |
| Exchanges | 66 | Distinct primary-listing exchange codes in `data/tickers.csv`. |
| Countries | 81 | Distinct non-empty `country` values in `data/tickers.csv`. |
| Aliases | 93,855 | Rows in `data/aliases.csv`; alias-to-listing lookup rows after generic-word filtering. |
| ISIN coverage | 48,235 (90.2%) | Primary ticker rows with a non-empty `isin`. |
| Sector/category coverage | 44,865 (83.9%) | Primary ticker rows with either `stock_sector` or `etf_category`. |
| Stock sector coverage | 33,909 | Primary ticker rows with a non-empty `stock_sector`. |
| ETF category coverage | 10,956 | Primary ticker rows with a non-empty `etf_category`. |
| Core listing-scope rows | 45,220 | Rows in `data/instrument_scopes.csv` where `instrument_scope=core`. |
| Core primary rows with ISIN | 41,302 | Core primary listing rows with an ISIN; tracked as `scope_reason=primary_listing`. |
| Core primary rows missing ISIN | 3,918 | Core primary listing rows still missing ISIN; tracked as `scope_reason=primary_listing_missing_isin`. |
| Extended listing-scope rows | 16,724 | Rows in `data/instrument_scopes.csv` where `instrument_scope=extended`. |

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
| [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) | Official source candidates not yet implemented as parsers |
| [`data/masterfiles/supplemental_listings.csv`](data/masterfiles/supplemental_listings.csv) | Safe official listings added to the core export |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | Machine-readable coverage report |
| [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md) | Missing/partial/global official-source backlog |
| [`data/reports/completion_backlog.md`](data/reports/completion_backlog.md) | Prioritized missing ISIN/sector/category backlog |
| [`data/reports/entry_quality.md`](data/reports/entry_quality.md) | Per-listing deterministic quality scan summary |
| [`data/reports/ohlcv_plausibility.md`](data/reports/ohlcv_plausibility.md) | Kronos-inspired market-data plausibility queue |
| [`data/reports/masterfile_collision_report.json`](data/reports/masterfile_collision_report.json) | Official-symbol gaps blocked by ticker collisions |

## Data Model

`tickers.csv` is the primary-security export:

```csv
ticker,name,exchange,asset_type,stock_sector,etf_category,country,country_code,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,,United States,US,US1912161007,191216|coca-cola|850663
```

`listings.csv` is the full venue export:

```csv
listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc.,Stock,Information Technology,,United States,US,US0378331005,apple|865985
```

Important rules:

- `ticker` is globally unique only in `tickers.csv`; use `listing_key` for venue-level identity.
- Stocks use `stock_sector`; ETFs use `etf_category`.
- `instrument_scopes.csv` marks `core`, OTC `extended`, and secondary cross-listings.
- Core rows without ISIN are tagged as `scope_reason=primary_listing_missing_isin`.
- Secondary listings stay in `listings.csv` and `cross_listings.csv`; `tickers.csv` keeps one primary row per security.

JSON metadata:

```json
{
  "_meta": {
    "version": "3.10.1",
    "built_at": "2026-04-14T05:01:29Z",
    "total_tickers": 53446
  },
  "tickers": []
}
```

SQLite tables: `tickers`, `listings`, `aliases`, `cross_listings`, and `instrument_scopes`.

## Quality

- Valid ISINs are checksum-verified.
- `data/reports/entry_quality.csv` stores one deterministic quality row per `listing_key`.
- `data/reports/ohlcv_plausibility.csv` stores optional market-data hygiene checks; default runs are no-network and omit unchecked rows unless local OHLCV samples, `--fetch-yahoo`, or `--include-not-checked` are provided.
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
| OTC | 8,474 |
| NASDAQ | 4,538 |
| LSE | 3,781 |
| TSE | 3,197 |
| SZSE | 3,083 |
| SSE | 2,789 |
| NYSE ARCA | 2,571 |
| XETRA | 2,205 |
| NYSE | 2,042 |
| KRX | 1,788 |
| TSX | 1,658 |
| KOSDAQ | 1,583 |
| B3 | 1,505 |
| ASX | 1,291 |
| TWSE | 1,242 |

For full exchange, country, source, and verification coverage, use:

```bash
python3 scripts/build_coverage_report.py
python3 scripts/build_source_inventory.py
python3 scripts/build_completion_backlog.py
python3 scripts/build_entry_quality_report.py
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/fetch_symbol_changes.py
```

Long OHLCV fetch runs should use streaming checkpoints:

```bash
python3 scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --include-not-checked --stream --resume
```

## Refresh Pipeline

Quick rebuild:

```bash
python3 scripts/rebuild_dataset.py
python3 scripts/build_listing_history.py
python3 scripts/build_coverage_report.py
python3 scripts/build_source_inventory.py
python3 scripts/build_completion_backlog.py
python3 scripts/build_entry_quality_report.py
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/fetch_symbol_changes.py
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
| Daily symbol-change feed | `scripts/fetch_symbol_changes.py` |

Review queue:

```bash
python3 scripts/build_entry_quality_report.py
python3 scripts/build_ohlcv_plausibility_report.py
python3 scripts/audit_dataset.py --write-defaults
python3 scripts/run_claude_review_queue.py --model sonnet --skip-existing
python3 scripts/build_claude_review_overrides.py --min-confidence 0.8
python3 scripts/rebuild_dataset.py
```

## Sources

Implemented primary exchange/reference inputs include Nasdaq Trader, Nasdaq Nordic, ASX, Deutsche Boerse, B3, TMX, Euronext, JPX/TSE, TWSE, TPEX, SSE/SZSE, Bursa Malaysia, BME, BMV, WSE/NewConnect, TASE, KRX, HOSE/HNX/UPCOM, CSE Sri Lanka, and SEC company tickers.

Official source candidates and reconciled source gaps are tracked in [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) and summarized by [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md). Current source inventory status: `0` missing current-scope sources, `0` parser todo rows, `0` real global-expansion candidates, `30` official-full rows, and `34` official-partial rows. Remaining work is now field-completion and taxonomy coverage, not undiscovered exchange-source inventory.

Secondary/reviewed enrichment inputs include [EODHD](https://eodhd.com/financial-apis/), [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase), XTB OMI specification data, Yahoo Finance review helpers, OpenFIGI, GLEIF, and curated production aliases from [api.adanos.org](https://api.adanos.org).

## Project

- License: [MIT](LICENSE)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Releases: [GitHub Releases](https://github.com/adanos-software/free-ticker-database/releases)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
