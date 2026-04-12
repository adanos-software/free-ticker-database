# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive, free-to-use stock and ETF ticker reference database covering 53,000+ primary securities and 61,000+ exchange listings across 67 exchanges and 69 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 53,826 |
| Stocks | 38,735 |
| ETFs | 15,091 |
| Exchanges | 67 |
| Countries | 69 |
| ISIN coverage | 45,375 (84.3%) |
| Sector coverage | 37,945 (70.5%) |
| Core listing-scope rows | 45,413 |
| Core primary rows with ISIN | 38,324 |
| Core primary rows missing ISIN | 7,089 |
| Extended listing-scope rows | 16,543 |
| Total aliases | 91,697 |

## Formats

Choose the format that fits your use case:

| File | Size | Best for |
|---|---|---|
| [`data/tickers.csv`](data/tickers.csv) | 5.0 MB | Excel, spreadsheets, quick lookups |
| [`data/listings.csv`](data/listings.csv) | 6.4 MB | Listing-keyed export without global ticker ambiguity |
| [`data/instrument_scopes.csv`](data/instrument_scopes.csv) | 5.2 MB | Core vs. extended listing scope |
| [`data/tickers.json`](data/tickers.json) | 10.7 MB | Web apps, APIs |
| [`data/tickers.parquet`](data/tickers.parquet) | 2.5 MB | Pandas, data science |
| [`data/tickers.db`](data/tickers.db) | 37.8 MB | SQL queries, local apps |
| [`data/aliases.csv`](data/aliases.csv) | 2.3 MB | Alias/name resolution |
| [`data/identifiers.csv`](data/identifiers.csv) | 1007 KB | ISIN/WKN lookups |
| [`data/cross_listings.csv`](data/cross_listings.csv) | 512 KB | Cross-listed securities |

Additional reference artifacts:

| File | Size | Best for |
|---|---|---|
| [`data/identifiers_extended.csv`](data/identifiers_extended.csv) | 2.2 MB | FIGI/CIK/LEI enrichment snapshot |
| [`data/listing_index.csv`](data/listing_index.csv) | 5.3 MB | Listing-keyed identity/export bridge |
| [`data/masterfiles/reference.csv`](data/masterfiles/reference.csv) | 24.0 MB | Official exchange-masterfile reference rows |
| [`data/masterfiles/supplemental_listings.csv`](data/masterfiles/supplemental_listings.csv) | 2.2 MB | Safe official listings added to the core export |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | 7.2 MB | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | 334 KB | Machine-readable coverage metrics |
| [`data/reports/completion_backlog.md`](data/reports/completion_backlog.md) | 8 KB | Prioritized ISIN/sector/category completion backlog |
| [`data/reports/masterfile_collision_report.json`](data/reports/masterfile_collision_report.json) | 57 KB | Official-symbol gaps blocked by cross-exchange collisions |

### tickers.csv (flat, Excel-friendly, one row per security)

```
ticker,name,exchange,asset_type,sector,country,country_code,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,United States,US,US1912161007,191216|coca-cola|850663
LPP,LPP S.A.,WSE,Stock,Consumer Cyclical,Poland,PL,PLLPP0000011,lpp|cropp|121065
```

This is the canonical core export. When a security trades on multiple exchanges, `tickers.csv` keeps the primary listing only. Secondary listings stay in `cross_listings.csv` and `listings.csv`.

ISIN is a dedicated column. Aliases are pipe-separated (`|`) for easy splitting.

### aliases.csv (1 row per alias)

```
ticker,alias,alias_type
KO,US1912161007,isin
KO,coca-cola,name
NVDA,918422,wkn
```

Types: `isin`, `wkn`, `name`, `exchange_ticker`

### identifiers.csv (ISIN + WKN lookup)

```
ticker,isin,wkn
KO,US1912161007,191216
NVDA,US67066G1040,918422
VOW,DE0007664039,766403
```

### cross_listings.csv (multi-exchange securities)

```
isin,listing_key,ticker,exchange,is_primary
AN8068571086,NYSE::SLB,SLB,NYSE,1
AN8068571086,B3::SLBG34,SLBG34,B3,0
AN8068571086,BMV::SLBN,SLBN,BMV,0
```

Securities traded on multiple exchanges share the same ISIN. The `is_primary` flag marks the home-exchange listing (based on ISIN country prefix and exchange ranking). `listing_key` links each row back to `listings.csv`.

### identifiers_extended.csv (FIGI / CIK / LEI)

```
ticker,exchange,isin,wkn,figi,cik,lei,figi_source,cik_source,lei_source
AAPL,NASDAQ,US0378331005,865985,,0000320193,,,SEC company_tickers_exchange.json,
```

This is an auxiliary enrichment file layered on top of the core dataset. `CIK` comes from the SEC company-ticker reference when available, `FIGI` from OpenFIGI, and `LEI` from GLEIF.

### listing_index.csv (listing-keyed bridge)

```
listing_key,ticker,exchange,name,asset_type,country,country_code,isin,wkn,figi,cik,lei
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc.,Stock,United States,US,US0378331005,865985,,0000320193,
```

This auxiliary export makes the current listing identity explicit as `exchange::ticker`. It does not replace the core global-unique `ticker` model yet, but it gives downstream users a stable per-listing key and makes future `(ticker, exchange)`-first migrations easier.

### listings.csv (full listing rows)

```
listing_key,ticker,exchange,name,asset_type,sector,country,country_code,isin,aliases
NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc.,Stock,Information Technology,United States,US,US0378331005,apple|865985
```

This is the full listing-level export for downstream systems that want every venue line with a stable `listing_key` today.

### instrument_scopes.csv (core vs. extended listing model)

```
listing_key,ticker,exchange,asset_type,isin,instrument_group_key,instrument_scope,scope_reason,primary_listing_key
NASDAQ::MSFT,MSFT,NASDAQ,Stock,US5949181045,US5949181045,core,primary_listing,NASDAQ::MSFT
XETRA::MSF,MSF,XETRA,Stock,US5949181045,US5949181045,extended,secondary_cross_listing,NASDAQ::MSFT
OTC::VROYF,VROYF,OTC,Stock,CA92859L2012,CA92859L2012,extended,otc_listing,TSXV::VROY
```

`core` marks the primary listing used by the canonical export. `extended` keeps OTC and secondary cross-listings available without polluting the core ticker universe. Core rows without a validated ISIN are explicitly tagged as `primary_listing_missing_isin`; filter to `scope_reason=primary_listing` when you need an ISIN-ready core universe. `primary_listing_key` links each extended row back to the chosen primary listing when the shared ISIN makes that possible.

### tickers.json

```json
{
  "_meta": {
    "version": "3.6.0",
    "built_at": "2026-04-12T12:41:02Z",
    "total_tickers": 53826
  },
  "tickers": [
    {
      "ticker": "KO",
      "name": "The Coca-Cola Company",
      "exchange": "NYSE",
      "asset_type": "Stock",
      "sector": "Consumer Staples",
      "country": "United States",
      "country_code": "US",
      "isin": "US1912161007",
      "aliases": ["191216", "coca-cola", "850663"]
    }
  ]
}
```

JSON outputs use an envelope with a `_meta` block and a `tickers` array as of version `3.6.0`.

### tickers.db (SQLite)

```sql
-- Find all tech stocks on NASDAQ
SELECT ticker, name FROM tickers WHERE exchange = 'NASDAQ' AND sector = 'Information Technology';

-- Look up a company by alias
SELECT t.* FROM tickers t JOIN aliases a ON t.ticker = a.ticker WHERE a.alias = 'nvidia';

-- Find ticker by ISIN
SELECT * FROM tickers WHERE isin = 'US1912161007';
```

Tables: `tickers` (53,826 rows), `listings` (61,956 rows), `aliases` (91,697 rows), `cross_listings` (14,196 rows), and `instrument_scopes` (61,956 rows), with indexes on alias, exchange, country, sector, ISIN, listing scope, and instrument group key.

## Schema

### tickers

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Primary ticker symbol (max 10 chars) |
| `name` | string | Company / fund name (max 200 chars) |
| `exchange` | string | Exchange (NYSE, NASDAQ, LSE, HKEX, etc.) |
| `asset_type` | string | `Stock` or `ETF` |
| `sector` | string | GICS sector (e.g. Information Technology) |
| `country` | string | Country of incorporation |
| `country_code` | string | ISO 3166-1 alpha-2 code (e.g. US, DE, GB) |
| `isin` | string | International Securities Identification Number |

### aliases

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Foreign key to tickers |
| `alias` | string | Alternative name, identifier, or keyword |
| `alias_type` | string | `isin`, `wkn`, `name`, or `exchange_ticker` |

## Exchange Coverage

| Exchange | Tickers | Description |
|---|---|---|
| OTC | 8,413 | US OTC / Pink Sheets |
| NASDAQ | 4,538 | NASDAQ |
| LSE | 3,892 | London Stock Exchange |
| TSE | 3,197 | Tokyo Stock Exchange |
| SZSE | 3,083 | Shenzhen Stock Exchange |
| SSE | 2,789 | Shanghai Stock Exchange |
| NYSE ARCA | 2,571 | NYSE ARCA (ETFs) |
| XETRA | 2,242 | Deutsche Boerse |
| NYSE | 2,043 | New York Stock Exchange |
| KRX | 1,788 | Korea Exchange |
| TSX | 1,658 | Toronto Stock Exchange |
| KOSDAQ | 1,583 | Korean OTC |
| B3 | 1,506 | Brasil Bolsa Balcao |
| ASX | 1,291 | Australian Securities Exchange |
| TWSE | 1,242 | Taiwan Stock Exchange |
| BATS | 1,207 | Cboe BZX Exchange |
| TPEX | 1,118 | Taipei Exchange |
| TSXV | 1,058 | TSX Venture Exchange |
| Bursa | 925 | Bursa Malaysia |
| STO | 709 | Nasdaq Stockholm |
| Euronext | 699 | Euronext |
| IDX | 694 | Indonesia Stock Exchange |
| + 47 more | ... | |

## Data Quality

- Zero duplicate tickers
- Exact duplicate alias rows removed
- Conservative filtering for obvious common-word, wrapper, celebrity, and product aliases
- Very short (1-2 char) and pure-numeric name aliases removed to reduce ambiguity
- Zero junk aliases ("Not Available", "N/A", etc.)
- All field lengths within database constraints
- Rights, units, warrants, notes, and preferred/depositary issues filtered from the stock universe
- ISIN-based country corrections applied for foreign OTC rows
- Sector names normalized to canonical GICS sectors (stocks) and standardized ETF categories
- ISIN check digits validated via Luhn algorithm; invalid ISINs removed

## Reference Coverage

The repo now includes a second layer of reference tooling beyond the core dataset exports:

- official venue masterfile snapshots
- listing-status / rename / delisting history baselines
- extended identifiers (`FIGI`, `CIK`, `LEI`)
- exchange/country coverage reports
- core vs. extended instrument-scope classification
- chunked stock-universe verification against official exchange directories

Generate the official masterfile reference rows:

```bash
python3 scripts/fetch_exchange_masterfiles.py
```

Verify the stock universe against the official reference layer in 10 local chunks:

```bash
for i in $(seq 1 10); do
  python3 scripts/verify_stock_masterfiles.py --chunk-index "$i" --chunk-count 10 &
done
wait
python3 scripts/summarize_stock_masterfile_verification.py
python3 scripts/build_stock_verification_overrides.py
```

This flow is used to identify stale company names, non-active test symbols, and stock rows that official exchange directories classify as ETFs, notes, warrants, preferreds, or depositary lines.

Current live sources:

- Nasdaq Trader `nasdaqlisted.txt`
- Nasdaq Trader `otherlisted.txt`
- Nasdaq Nordic listed shares / ETF JSON endpoints for Stockholm, Helsinki, and Copenhagen
- ASX `ASXListedCompanies.csv` and official ASX `ISIN.xls`
- Deutsche Börse `Listed-companies.xlsx`
- B3 `InstrumentsEquities` public API
- TMX issuer workbook + ETF screener + `interlisted-companies.txt`
- Euronext Live equities CSV export
- JPX listed issues XLS
- TWSE listed companies open-data JSON
- TPEX mainboard daily quotes open-data JSON
- TPEX ETF InfoHub export endpoint
- Bursa Malaysia equity ISIN PDF
- BME official listed-values PDF for SIBE, ETFs, and LATIBEX
- BMV issuer, stock, capital-trust, and ETF search endpoints
- WSE / NewConnect listed companies and WSE ETF lists
- TASE securities, ETF, foreign ETF, and participating-unit market data
- KRX listed company and ETF finder endpoints
- HOSE/HNX/UPCOM securities directories
- SEC `company_tickers_exchange.json` when the environment is allowed to fetch it, or a cached official snapshot when present locally

Bursa Malaysia protects the equity ISIN PDF behind a browser session. To refresh that source from scratch, install the Python dependencies and run `python3 -m playwright install chromium` once before `python3 scripts/fetch_exchange_masterfiles.py --source bursa_equity_isin`.

`data/reports/coverage_report.json` exposes both `exchange_coverage` and a machine-friendly `by_exchange` alias, plus separate stock and ETF verification summaries. That makes backlog prioritization easier without scraping the markdown report.

Generate safe official listing supplements:

```bash
python3 scripts/build_masterfile_supplements.py
```

This currently adds only collision-free official listings that fit the core dataset's global-unique `ticker` model. Today that includes safe `TSE`, `ASX`, `AMS`, and `OSL` rows that do not collide with existing symbols on other venues.

Generate listing history artifacts:

```bash
python3 scripts/build_listing_history.py
```

This writes:

- `data/history/latest_snapshot.csv`
- `data/history/listing_events.csv`
- `data/history/listing_status_history.csv`
- `data/history/daily_listing_summary.json`
- `data/history/daily_listing_summary.csv`

History artifacts are listing-keyed (`exchange::ticker`) so per-venue state changes stay explicit even when raw symbols collide globally. `listing_status_history.csv` is stored as compact status intervals with `first_observed_at` / `last_observed_at`, not one row per listing per rebuild.

Generate extended identifiers:

```bash
python3 scripts/enrich_global_identifiers.py \
  --enable-figi --figi-exchanges ASX,TSX,TSXV --figi-limit 3000 \
  --enable-lei --lei-exchanges ASX,TSX,NASDAQ,NYSE --lei-limit 200
```

Notes:

- `FIGI` enrichment is live via OpenFIGI and matched at listing level, not blindly by ISIN across venues.
- `FIGI` enrichment is best run venue by venue; without an API key OpenFIGI can return `429`, and the script now preserves partial progress plus batch-level errors.
- `LEI` enrichment is live via GLEIF and now uses `ISIN` first, with exact normalized legal-name fallback only when the identifier lookup misses.
- `CIK` enrichment uses the official SEC company-ticker file. The current snapshot is cached under `data/masterfiles/cache/sec_company_tickers_exchange.json`; if live SEC access is blocked later, the scripts reuse that official cache instead of dropping `CIK` coverage back to zero.

Build exchange/country coverage reports:

```bash
python3 scripts/build_coverage_report.py
```

This writes:

- `data/reports/coverage_report.json`
- `data/reports/coverage_report.md`
- `data/reports/masterfile_collision_report.json`

The coverage report now includes:

- exchange venue-status classification (`official_full`, `official_partial`, `manual_only`, `missing`)
- artifact freshness timestamps for the dataset, masterfiles, identifiers, listing history, and latest stock/ETF verification runs
- source coverage rows with fetch mode and row counts
- global instrument-scope counts for `core`, OTC `extended`, and secondary cross-listings
- per-exchange stock-verification rates for officially covered venues
- unresolved gap summaries per exchange
- B3-specific missing-symbol breakdowns

It also separates official masterfile rows into:

- `match`: listing already present in the core export
- `collision`: official symbol blocked by the current global-unique `ticker` model because the symbol already exists on another exchange
- `missing`: official symbol not present and not blocked by a known collision

Build the prioritized completion backlog:

```bash
python3 scripts/build_completion_backlog.py
```

This writes `data/reports/completion_backlog.csv`, `data/reports/completion_backlog.json`, and `data/reports/completion_backlog.md`. The backlog uses `instrument_scopes.csv` for `primary_listing_missing_isin`, splits missing stock sectors from missing ETF categories, and assigns a recommended source plus review/confidence policy to each exchange-field block.

Run the deterministic enrichment pipeline:

```bash
python3 scripts/run_enrichment_pipeline.py --dry-run
```

By default the pipeline plans `fetch_exchange_masterfiles`, completion backlog generation, local reviewed sector backfill reports, rebuilds, identifier/listing-history refreshes, coverage reports, audit queue refreshes, and a final backlog rebuild. Add `--include-secondary-network` to include EODHD/Yahoo candidate stages, and add `--apply-reviewed-backfills` only when you intentionally want accepted reviewed candidates merged into review overrides.

## Review Queue

Generate a scored queue of suspicious entries for manual or model-assisted review:

```bash
python3 scripts/audit_dataset.py --write-defaults
```

This creates:

- `data/review_queue.json` - grouped review items with findings and scores
- `data/review_queue.csv` - flat finding rows for spreadsheet triage

Recommended workflow:

1. Run the audit after rebuilding the dataset.
2. Preferred local flow: run the queue through Claude CLI on this machine:

```bash
python3 scripts/run_claude_review_queue.py --model sonnet --skip-existing
```

This uses `claude --dangerously-skip-permissions -p` locally, batches reviews in groups of `10` by default, and defers any tail batch smaller than `10` unless `--allow-partial-batch` is passed. It writes:

- `data/claude_review_jobs/raw_responses.jsonl`
- `data/claude_review_jobs/normalized_reviews.json`
- `data/claude_review_jobs/normalized_reviews.csv`
- `data/claude_review_jobs/errors.json`

3. Optional: use Yahoo Finance search to validate the hardest residual alias collisions before you touch overrides:

```bash
python3 scripts/verify_yahoo_high_risk_aliases.py --merge-remove-overrides
```

This writes:

- `data/yahoo_verification/high_risk_aliases.json`
- `data/yahoo_verification/high_risk_aliases.csv`

And, when `--merge-remove-overrides` is passed, it appends conservative alias removals into `data/review_overrides/remove_aliases.csv`.

4. Derive conservative override files from high-confidence Claude decisions:

```bash
python3 scripts/build_claude_review_overrides.py --min-confidence 0.8
```

This writes:

- `data/review_overrides/remove_aliases.csv`
- `data/review_overrides/metadata_updates.csv`
- `data/review_overrides/drop_entries.csv`

These overrides are applied automatically by `scripts/rebuild_dataset.py`.

5. Rebuild the dataset with the review-derived overrides:

```bash
python3 scripts/rebuild_dataset.py
```

5. Build small, actionable PR batches from the Claude decisions:

```bash
python3 scripts/build_pr_review_batches.py
```

6. Apply confirmed review batches back to the source CSVs:

```bash
python3 scripts/apply_review_batches.py --execute
```

By default the script reads `data/pr_review_batches/manifest.json`, updates the source CSVs, and rebuilds derived artifacts unless `--skip-rebuild` is passed.

7. Keep PRs batched by finding type or source update, not one PR per ticker.

Prompt and response schema:

- [`docs/claude_review_prompt.md`](docs/claude_review_prompt.md)
- [`docs/review_response.schema.json`](docs/review_response.schema.json)

Optional Yahoo verification:

```bash
python3 scripts/verify_yahoo_listings.py --finding-type thin_otc_metadata --limit 20
```

This uses `yfinance` as an external verification helper for suspicious listings and writes generated reports under `data/yahoo_verification/`. Treat Yahoo results as review signals, not a hard source of truth.

For missing OTC ISINs, use the stricter gated override workflow:

```bash
python3 scripts/backfill_yahoo_otc_isins.py --apply --timeout-seconds 10
```

This accepts Yahoo Finance ISINs only when the ISIN checksum is valid and the Yahoo venue, quote type, and issuer name match the current OTC row. Accepted rows are merged into `data/review_overrides/metadata_updates.csv` and must still flow through `python3 scripts/rebuild_dataset.py`.

For missing ASX ISINs, use the official ASX ISIN workbook:

```bash
python3 scripts/backfill_asx_isins.py --apply
python3 scripts/rebuild_dataset.py
```

This reads ASX `ISIN.xls`, writes audit reports under `data/asx_verification/`, and merges accepted ISINs into `data/review_overrides/metadata_updates.csv` only after exact ASX code, issuer-name, numeric-token, and ISIN-checksum gates match.

For small selected US ETF batches, Yahoo can be used as a secondary ISIN candidate source:

```bash
python3 scripts/backfill_yahoo_missing_isins.py --exchange BATS --asset-type ETF --limit 50 --apply
python3 scripts/rebuild_dataset.py
```

Use this in small chunks unless you intentionally run a full venue block. It is stricter than general Yahoo verification because it checks venue, quote type, expected ISIN country prefix, strict name match, numeric-token match, and checksum; Yahoo remains a secondary source and should not be used for Japan/TSE ISINs.

For secondary broker coverage, the XTB OMI specification table can fill missing ISINs after strict ticker suffix/exchange, asset-type, and issuer-name gates:

```bash
python3 scripts/backfill_xtb_omi_isins.py --apply --timeout-seconds 30
python3 scripts/rebuild_dataset.py
```

This reads the current XTB OMI PDF, writes audit reports under `data/xtb_verification/`, and only merges accepted ISINs into `data/review_overrides/metadata_updates.csv`. Treat XTB as a secondary verification source, not an official exchange masterfile.

For FinanceDatabase sector enrichment, use the ticker/exchange/name-gated workflow:

```bash
python3 scripts/backfill_financedatabase_metadata.py --apply
python3 scripts/rebuild_dataset.py
```

By default this emits sector updates only and writes audit reports under `data/financedatabase_verification/`. FinanceDatabase ISINs are intentionally opt-in via `--enable-isin` because cross-listing collisions need separate manual review before they can be trusted as identifier overrides.

For EODHD exchange-symbol-list ISIN enrichment, pass the API key via environment only:

```bash
EODHD_API_TOKEN=... python3 scripts/backfill_eodhd_metadata.py --apply
python3 scripts/rebuild_dataset.py
```

By default this accepts only new ISINs that are not already present in the primary export and writes audit reports under `data/eodhd_verification/`. Use `--allow-existing-isin` only for manually reviewed cross-listing batches, because existing-ISIN matches can intentionally move rows from core primary to secondary cross-listing scope.

For same-ISIN sector/category propagation, use the local peer workflow:

```bash
python3 scripts/backfill_sector_from_isin_peers.py --apply
python3 scripts/rebuild_dataset.py
```

This writes audit reports under `data/isin_peer_sector_verification/` and only propagates a sector/category when the primary row is missing sector and every same-asset listing peer with the same ISIN normalizes to one sector value.

## Data Sources

- **[EODHD](https://eodhd.com/financial-apis/)** - Secondary exchange-symbol-list ISIN candidates after strict venue/type/name/checksum gates
- **[FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)** - Sector classification and optional reviewed identifier candidates
- **Production data** from [api.adanos.org](https://api.adanos.org) - Curated aliases, company name variants

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Project Health

- CI: [GitHub Actions](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
- Release notes: [GitHub Releases](https://github.com/adanos-software/free-ticker-database/releases)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
