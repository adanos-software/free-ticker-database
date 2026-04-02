# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive, free-to-use stock and ETF ticker reference database covering 59,000+ securities across 67 exchanges and 68 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 59,177 |
| Stocks | 43,085 |
| ETFs | 16,092 |
| Exchanges | 67 |
| Countries | 68 |
| ISIN coverage | 45,039 (76.1%) |
| Sector coverage | 38,894 (65.7%) |
| Total aliases | 102,943 |

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
| [`data/masterfiles/reference.csv`](data/masterfiles/reference.csv) | 3.1 MB | Official exchange-masterfile reference rows |
| [`data/history/latest_snapshot.csv`](data/history/latest_snapshot.csv) | 6.1 MB | Current listing-status baseline |
| [`data/reports/coverage_report.json`](data/reports/coverage_report.json) | 33 KB | Machine-readable coverage metrics |

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
AAPL,NASDAQ,US0378331005,865985,BBG000B9XRY4,,HWUPKR0MPOU8FGXBT394,OpenFIGI,,GLEIF
```

This is an auxiliary enrichment file layered on top of the core dataset. `CIK` comes from the SEC company-ticker reference when available, `FIGI` from OpenFIGI, and `LEI` from GLEIF.

### tickers.json

```json
{
  "_meta": {
    "version": "2.0.0",
    "built_at": "2026-04-02T07:11:51Z",
    "total_tickers": 59177
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

JSON outputs use an envelope with a `_meta` block and a `tickers` array as of version `2.0.0`.

### tickers.db (SQLite)

```sql
-- Find all tech stocks on NASDAQ
SELECT ticker, name FROM tickers WHERE exchange = 'NASDAQ' AND sector = 'Information Technology';

-- Look up a company by alias
SELECT t.* FROM tickers t JOIN aliases a ON t.ticker = a.ticker WHERE a.alias = 'nvidia';

-- Find ticker by ISIN
SELECT * FROM tickers WHERE isin = 'US1912161007';
```

Tables: `tickers` (59,177 rows) + `aliases` (102,943 rows) + `cross_listings` (10,165 rows) with indexes on `alias`, `exchange`, `country`, `sector`, and `isin`.

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
| NASDAQ | 4,819 | NASDAQ |
| SZSE | 3,096 | Shenzhen Stock Exchange |
| XETRA | 2,947 | Deutsche Boerse |
| SSE | 2,811 | Shanghai Stock Exchange |
| NYSE | 2,618 | New York Stock Exchange |
| NYSE ARCA | 2,619 | NYSE ARCA (ETFs) |
| KRX | 2,282 | Korea Exchange |
| TSX | 1,766 | Toronto Stock Exchange |
| B3 | 1,773 | Sao Paulo Exchange |
| TWSE | 1,245 | Taiwan Stock Exchange |
| ASX | 1,236 | Australian Securities Exchange |
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

Generate the official masterfile reference rows:

```bash
python3 scripts/fetch_exchange_masterfiles.py
```

Current live sources:

- Nasdaq Trader `nasdaqlisted.txt`
- Nasdaq Trader `otherlisted.txt`
- ASX `ASXListedCompanies.csv`
- TMX `interlisted-companies.txt` (official interlisted subset, not a full TSX/TSXV directory)
- Euronext Live equities CSV export
- SEC `company_tickers_exchange.json` when the environment is allowed to fetch it, or a cached official snapshot when present locally

Generate listing history artifacts:

```bash
python3 scripts/build_listing_history.py
```

This writes:

- `data/history/latest_snapshot.csv`
- `data/history/listing_events.csv`
- `data/history/listing_status_history.csv`

Generate extended identifiers:

```bash
python3 scripts/enrich_global_identifiers.py \
  --enable-figi --figi-exchanges ASX,TSX,TSXV --figi-limit 3000 \
  --enable-lei --lei-exchanges ASX,TSX,NASDAQ,NYSE --lei-limit 200
```

Notes:

- `FIGI` enrichment is live via OpenFIGI and matched at listing level, not blindly by ISIN across venues.
- `FIGI` enrichment is best run venue by venue; without an API key OpenFIGI can return `429`, and the script now preserves partial progress plus batch-level errors.
- `LEI` enrichment is live via GLEIF and uses exact normalized legal-name matching to stay conservative.
- `CIK` enrichment uses the official SEC company-ticker file. Some environments are blocked by SEC with `403`; in that case the script falls back to a cached official snapshot when available, otherwise it keeps `CIK` empty and records the error in `data/identifier_summary.json`.

Build exchange/country coverage reports:

```bash
python3 scripts/build_coverage_report.py
```

This writes:

- `data/reports/coverage_report.json`
- `data/reports/coverage_report.md`

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
