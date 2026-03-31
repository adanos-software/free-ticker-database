# Free Global Ticker Database

A comprehensive, free-to-use stock and ETF ticker reference database covering 60,000+ securities across 67 exchanges and 67 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 60,109 |
| Stocks | 44,015 |
| ETFs | 16,094 |
| Exchanges | 67 |
| Countries | 67 |
| ISIN coverage | 45,773 (76.2%) |
| Sector coverage | 39,677 (66.0%) |
| Total aliases | 107,074 |

## Formats

Choose the format that fits your use case:

| File | Size | Best for |
|---|---|---|
| [`data/tickers.csv`](data/tickers.csv) | 5.5 MB | Excel, spreadsheets, quick lookups |
| [`data/tickers.json`](data/tickers.json) | 11.0 MB | Web apps, APIs |
| [`data/tickers.parquet`](data/tickers.parquet) | 2.7 MB | Pandas, data science |
| [`data/tickers.db`](data/tickers.db) | 18.9 MB | SQL queries, local apps |
| [`data/aliases.csv`](data/aliases.csv) | 2.8 MB | Alias/name resolution |
| [`data/identifiers.csv`](data/identifiers.csv) | 1.1 MB | ISIN/WKN lookups |

### tickers.csv (flat, Excel-friendly)

```
ticker,name,exchange,asset_type,sector,country,isin,aliases
KO,The Coca-Cola Company,NYSE,Stock,Consumer Staples,United States,US1912161007,191216|coca-cola|850663
LPP,LPP S.A.,WSE,Stock,Consumer Cyclical,Poland,PLLPP0000011,lpp|cropp|121065
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

### tickers.json

```json
{
  "_meta": {
    "version": "2.0.0",
    "built_at": "2026-03-31T00:00:00Z",
    "total_tickers": 60109
  },
  "tickers": [
    {
      "ticker": "KO",
      "name": "The Coca-Cola Company",
      "exchange": "NYSE",
      "asset_type": "Stock",
      "sector": "Consumer Staples",
      "country": "United States",
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

Tables: `tickers` (60,109 rows) + `aliases` (107,074 rows) with indexes on `alias`, `exchange`, `country`, `sector`, `isin`.

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
| OTC | 11,407 | US OTC / Pink Sheets |
| LSE | 6,430 | London Stock Exchange |
| NASDAQ | 4,914 | NASDAQ |
| SZSE | 3,096 | Shenzhen Stock Exchange |
| XETRA | 2,951 | Deutsche Boerse |
| SSE | 2,811 | Shanghai Stock Exchange |
| NYSE | 2,736 | New York Stock Exchange |
| NYSE ARCA | 2,619 | NYSE ARCA (ETFs) |
| KRX | 2,395 | Korea Exchange |
| TSX | 1,915 | Toronto Stock Exchange |
| B3 | 1,773 | Sao Paulo Exchange |
| TWSE | 1,312 | Taiwan Stock Exchange |
| ASX | 1,237 | Australian Securities Exchange |
| TPEX | 1,205 | Taipei Exchange |
| KOSDAQ | 1,145 | Korean OTC |
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

## Data Sources

- **[FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)** - Sector classification, WKNs, additional ISINs
- **Production data** from [api.adanos.org](https://api.adanos.org) - Curated aliases, company name variants

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
