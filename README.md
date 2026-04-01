# Free Global Ticker Database

[![CI](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml/badge.svg)](https://github.com/adanos-software/free-ticker-database/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive, free-to-use stock and ETF ticker reference database covering 59,000+ securities across 67 exchanges and 68 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 59,178 |
| Stocks | 43,086 |
| ETFs | 16,092 |
| Exchanges | 67 |
| Countries | 68 |
| ISIN coverage | 44,839 (75.8%) |
| Sector coverage | 38,900 (65.7%) |
| Total aliases | 104,387 |

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

### tickers.json

```json
{
  "_meta": {
    "version": "2.0.0",
    "built_at": "2026-04-01T18:37:40Z",
    "total_tickers": 59178
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

Tables: `tickers` (59,178 rows) + `aliases` (104,387 rows) + `cross_listings` (10,186 rows) with indexes on `alias`, `exchange`, `country`, `sector`, and `isin`.

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

3. Derive conservative override files from high-confidence Claude decisions:

```bash
python3 scripts/build_claude_review_overrides.py --min-confidence 0.8
```

This writes:

- `data/review_overrides/remove_aliases.csv`
- `data/review_overrides/metadata_updates.csv`
- `data/review_overrides/drop_entries.csv`

These overrides are applied automatically by `scripts/rebuild_dataset.py`.

4. Rebuild the dataset with the review-derived overrides:

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
