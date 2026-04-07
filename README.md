# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive, free-to-use stock and ETF ticker reference database covering 61,000+ securities across 68 exchanges and 68 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 61,353 |
| Stocks | 44,638 |
| ETFs | 16,715 |
| Exchanges | 68 |
| Countries | 68 |
| ISIN coverage | 43,656 (71.2%) |
| Sector coverage | 37,839 (61.7%) |
| Total aliases | 98,167 |

## Formats

Choose the format that fits your use case:

| File | Size | Best for |
|---|---|---|
| [`data/tickers.csv`](data/tickers.csv) | 5.4 MB | Excel, spreadsheets, quick lookups |
| [`data/tickers.json`](data/tickers.json) | 11.8 MB | Web apps, APIs |
| [`data/tickers.parquet`](data/tickers.parquet) | 2.6 MB | Pandas, data science |
| [`data/tickers.db`](data/tickers.db) | 18.7 MB | SQL queries, local apps |
| [`data/aliases.csv`](data/aliases.csv) | 2.7 MB | Alias/name resolution |
| [`data/identifiers.csv`](data/identifiers.csv) | 1.0 MB | ISIN/WKN lookups |
| [`data/cross_listings.csv`](data/cross_listings.csv) | 0.3 MB | Cross-listed securities |

Additional reference artifacts:

| File | Size | Best for |
|---|---|---|
| [`data/identifiers_extended.csv`](data/identifiers_extended.csv) | 1.7 MB | FIGI/CIK/LEI enrichment snapshot |
| [`data/listing_index.csv`](data/listing_index.csv) | 5.2 MB | Listing-keyed identity/export bridge |
| [`data/masterfiles/reference.csv`](data/masterfiles/reference.csv) | 3.1 MB | Official exchange-masterfile reference rows |
| [`data/masterfiles/supplemental_listings.csv`](data/masterfiles/supplemental_listings.csv) | 0.3 MB | Safe official listings added to the core export |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | 6.1 MB | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | 33 KB | Machine-readable coverage metrics |
| [`data/reports/masterfile_collision_report.json`](data/reports/masterfile_collision_report.json) | 184 KB | Official-symbol gaps blocked by cross-exchange collisions |

### tickers.csv (flat, Excel-friendly)

```
ticker,name,exchange,asset_type,sector,country,country_code,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,United States,US,US1912161007,191216|coca-cola|850663
LPP,LPP S.A.,WSE,Stock,Consumer Cyclical,Poland,PL,PLLPP0000011,lpp|cropp|121065
```

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
isin,ticker,exchange,is_primary
AN8068571086,SLB,NYSE,1
AN8068571086,SLBG34,B3,0
AN8068571086,SLBN,BMV,0
```

Securities traded on multiple exchanges share the same ISIN. The `is_primary` flag marks the home-exchange listing (based on ISIN country prefix and exchange ranking).

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

### tickers.json

```json
{
  "_meta": {
    "version": "2.1.0",
    "built_at": "2026-04-06T20:23:15Z",
    "total_tickers": 61353
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

JSON outputs use an envelope with a `_meta` block and a `tickers` array as of version `2.1.0`.

### tickers.db (SQLite)

```sql
-- Find all tech stocks on NASDAQ
SELECT ticker, name FROM tickers WHERE exchange = 'NASDAQ' AND sector = 'Information Technology';

-- Look up a company by alias
SELECT t.* FROM tickers t JOIN aliases a ON t.ticker = a.ticker WHERE a.alias = 'nvidia';

-- Find ticker by ISIN
SELECT * FROM tickers WHERE isin = 'US1912161007';
```

Tables: `tickers` (61,479 rows) + `aliases` (98,566 rows) + `cross_listings` (8,815 rows) with indexes on `alias`, `exchange`, `country`, `sector`, and `isin`.

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
| OTC | 10,596 | US OTC / Pink Sheets |
| LSE | 6,409 | London Stock Exchange |
| NASDAQ | 4,795 | NASDAQ |
| SZSE | 3,096 | Shenzhen Stock Exchange |
| XETRA | 3,017 | Deutsche Boerse |
| SSE | 2,811 | Shanghai Stock Exchange |
| NYSE | 2,599 | New York Stock Exchange |
| NYSE ARCA | 2,619 | NYSE ARCA (ETFs) |
| KRX | 2,282 | Korea Exchange |
| TSX | 1,766 | Toronto Stock Exchange |
| B3 | 1,773 | Sao Paulo Exchange |
| TWSE | 1,245 | Taiwan Stock Exchange |
| ASX | 1,298 | Australian Securities Exchange |
| KOSDAQ | 1,140 | Korean OTC |
| TPEX | 1,126 | Taipei Exchange |
| + 52 more | ... | |

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
- ASX `ASXListedCompanies.csv`
- Deutsche Börse `Listed-companies.xlsx`
- B3 `InstrumentsEquities` public API
- TMX `interlisted-companies.txt` (official interlisted subset, not a full TSX/TSXV directory)
- Euronext Live equities CSV export
- JPX listed issues XLS
- TWSE listed companies open-data JSON
- TPEX mainboard daily quotes open-data JSON (cache-first fallback when the official endpoint blocks the current environment)
- SEC `company_tickers_exchange.json` when the environment is allowed to fetch it, or a cached official snapshot when present locally

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
- artifact freshness timestamps for the dataset, masterfiles, identifiers, listing history, and latest stock-verification run
- source coverage rows with fetch mode and row counts
- per-exchange stock-verification rates for officially covered venues
- unresolved gap summaries per exchange
- B3-specific missing-symbol breakdowns

It also separates official masterfile rows into:

- `match`: listing already present in the core export
- `collision`: official symbol blocked by the current global-unique `ticker` model because the symbol already exists on another exchange
- `missing`: official symbol not present and not blocked by a known collision

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

## Data Sources

- **[FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)** - Sector classification, WKNs, additional ISINs
- **Production data** from [api.adanos.org](https://api.adanos.org) - Curated aliases, company name variants

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Project Health

- Code of Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Security policy: [SECURITY.md](SECURITY.md)
- CI: [GitHub Actions](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
- Release notes: [GitHub Releases](https://github.com/adanos-software/free-ticker-database/releases)
