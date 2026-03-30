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

## Schema

| Column | Type | Description |
|---|---|---|
| `ticker` | string (max 10) | Primary ticker symbol |
| `name` | string (max 200) | Company / fund name |
| `exchange` | string (max 20) | Exchange name (NYSE, NASDAQ, LSE, HKEX, etc.) |
| `asset_type` | string | `Stock` or `ETF` |
| `aliases` | JSON array | ISINs, WKNs, company name aliases, alternate tickers |
| `sector` | string (max 50) | GICS sector (e.g. Information Technology, Financials) |
| `country` | string (max 50) | Country of incorporation |

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
| + 52 more | ... | See CSV for full list |

## Data Sources

- **[EODHD](https://eodhd.com)** - Ticker listings, ISINs, exchange data
- **[FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)** - Sector classification, WKNs, additional ISINs
- **Production data** from [api.adanos.org](https://api.adanos.org) - Curated aliases, company name variants

## Alias Examples

```
AAPL -> ["US0378331005", "apple", "iphone", "tim cook"]
TSLA -> ["US88160R1014", "tesla", "elon", "musk", "cybertruck", "model 3", "model y"]
NVDA -> ["US67066G1040", "nvidia", "jensen", "gpu"]
VOW  -> ["DE0007664039", "volkswagen", "vw"]
```

## Data Quality

- Zero duplicate tickers
- Zero common-word aliases (no "gold", "iron", "shell", etc. as standalone aliases)
- Zero junk aliases ("Not Available", "N/A", etc.)
- All field lengths within database constraints
- Warrants, notes, bonds, and preferred stock debt instruments excluded
- 10-pass automated quality validation

## Usage

```python
import csv
import json

with open('tickers.csv') as f:
    for row in csv.DictReader(f):
        ticker = row['ticker']
        name = row['name']
        aliases = json.loads(row['aliases'])
        print(f"{ticker}: {name} ({row['exchange']}) - aliases: {aliases}")
```

## License

This database is provided free of charge for any purpose. The underlying data is sourced from public financial APIs. No warranty is provided regarding accuracy or completeness.

## Contributing

Issues and pull requests are welcome. Please ensure any additions pass the 10-pass quality validation.
