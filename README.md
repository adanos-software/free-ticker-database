# Free Global Ticker Database

A comprehensive, free-to-use stock and ETF ticker reference database covering 61,000+ securities across 67 exchanges and 56 countries.

## Stats

| Metric | Value |
|---|---|
| **Total tickers** | 61,217 |
| Stocks | 45,114 |
| ETFs | 16,103 |
| Exchanges | 67 |
| Countries | 56 |
| ISIN coverage | 44,265 (72.3%) |
| Sector coverage | 32,809 (53.6%) |
| Total aliases | 106,391 |

## Formats

Choose the format that fits your use case:

| File | Size | Best for |
|---|---|---|
| [`data/tickers.csv`](data/tickers.csv) | 5.3 MB | Excel, spreadsheets, quick lookups |
| [`data/tickers.json`](data/tickers.json) | 10.4 MB | Web apps, APIs |
| [`data/tickers.parquet`](data/tickers.parquet) | 2.4 MB | Pandas, data science |
| [`data/tickers.db`](data/tickers.db) | 19.0 MB | SQL queries, local apps |
| [`data/aliases.csv`](data/aliases.csv) | 2.6 MB | Alias/name resolution |
| [`data/identifiers.csv`](data/identifiers.csv) | 911 KB | ISIN/WKN lookups |

### tickers.csv (flat, Excel-friendly)

```
ticker,name,exchange,asset_type,sector,country,isin,aliases
AAPL,Apple Inc,NASDAQ,Stock,Information Technology,United States,US0378331005,apple|iphone|tim cook
TSLA,Tesla Inc,NASDAQ,Stock,Consumer Discretionary,United States,US88160R1014,tesla|elon|musk|cybertruck
```

ISIN is a dedicated column. Aliases are pipe-separated (`|`) for easy splitting.

### aliases.csv (1 row per alias)

```
ticker,alias,alias_type
AAPL,US0378331005,isin
AAPL,apple,name
AAPL,iphone,name
NVDA,918422,wkn
```

Types: `isin`, `wkn`, `name`, `exchange_ticker`

### identifiers.csv (ISIN + WKN lookup)

```
ticker,isin,wkn
AAPL,US0378331005,
NVDA,US67066G1040,918422
VOW,DE0007664039,766403
```

### tickers.json

```json
[
  {
    "ticker": "AAPL",
    "name": "Apple Inc",
    "exchange": "NASDAQ",
    "asset_type": "Stock",
    "sector": "Information Technology",
    "country": "United States",
    "isin": "US0378331005",
    "aliases": ["apple", "iphone", "tim cook"]
  }
]
```

### tickers.db (SQLite)

```sql
-- Find all tech stocks on NASDAQ
SELECT ticker, name FROM tickers WHERE exchange = 'NASDAQ' AND sector = 'Information Technology';

-- Look up a company by alias
SELECT t.* FROM tickers t JOIN aliases a ON t.ticker = a.ticker WHERE a.alias = 'nvidia';

-- Find ticker by ISIN
SELECT * FROM tickers WHERE isin = 'US0378331005';
```

Tables: `tickers` (61,217 rows) + `aliases` (106,391 rows) with indexes on `alias`, `exchange`, `country`, `sector`, `isin`.

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
| OTC | 11,444 | US OTC / Pink Sheets |
| LSE | 6,432 | London Stock Exchange |
| NASDAQ | 5,084 | NASDAQ |
| SZSE | 3,096 | Shenzhen Stock Exchange |
| NYSE | 3,007 | New York Stock Exchange |
| XETRA | 2,951 | Deutsche Boerse |
| SSE | 2,811 | Shanghai Stock Exchange |
| NYSE ARCA | 2,619 | NYSE ARCA (ETFs) |
| KRX | 2,401 | Korea Exchange |
| TSX | 1,925 | Toronto Stock Exchange |
| B3 | 1,773 | Sao Paulo Exchange |
| TWSE | 1,313 | Taiwan Stock Exchange |
| ASX | 1,240 | Australian Securities Exchange |
| KOSDAQ | 1,145 | Korean OTC |
| BATS | 1,103 | Cboe BATS (ETFs) |
| + 52 more | ... | |

## Data Quality

- Zero duplicate tickers
- Zero common-word aliases (no "gold", "iron", "shell", etc. as standalone aliases)
- Zero junk aliases ("Not Available", "N/A", etc.)
- All field lengths within database constraints
- Warrants, notes, bonds, and preferred stock debt instruments excluded
- 10-pass automated quality validation

## Data Sources

- **[FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)** - Sector classification, WKNs, additional ISINs
- **Production data** from [api.adanos.org](https://api.adanos.org) - Curated aliases, company name variants

## License

This database is provided free of charge for any purpose. The underlying data is sourced from public financial APIs. No warranty is provided regarding accuracy or completeness.

## Contributing

Issues and pull requests are welcome. Please ensure any additions pass the 10-pass quality validation.
