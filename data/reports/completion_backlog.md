# Completion Backlog

Generated at: `2026-04-22T09:47:02Z`

## Summary

- Missing primary ISIN rows: `3894`
- Missing stock sectors: `5009`
- Missing ETF categories: `3865`
- Official symbol collisions blocking global-unique ticker ingestion: `10877`

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | SSE | All | 614 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 2 | TSX | All | 571 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 3 | TSXV | All | 545 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | SZSE | All | 487 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 5 | B3 | All | 276 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 6 | NYSE ARCA | All | 206 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | SET | All | 196 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 8 | NASDAQ | All | 155 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | All | 118 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 10 | IDX | All | 115 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | PSX | All | 107 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 1851 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | NSE_IN | Stock | 483 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | TSXV | Stock | 418 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 4 | LSE | Stock | 328 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 5 | B3 | Stock | 279 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | STO | Stock | 231 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | Euronext | Stock | 132 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | XETRA | Stock | 120 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | TSX | Stock | 97 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | HNX | Stock | 92 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | PSE | Stock | 77 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | TASE | Stock | 70 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | B3 | ETF | 621 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | KRX | ETF | 526 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NYSE ARCA | ETF | 486 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | SSE | ETF | 479 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | SZSE | ETF | 337 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | NASDAQ | ETF | 217 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | XETRA | ETF | 194 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | BATS | ETF | 193 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | LSE | ETF | 188 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | TSX | ETF | 162 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | ASX | ETF | 144 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | TSE | ETF | 70 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 1886 | 1851 | 35 | official_full |
| 2 | B3 | 900 | 279 | 621 | official_full |
| 3 | KRX | 526 | 0 | 526 | official_full |
| 4 | LSE | 516 | 328 | 188 | official_full |
| 5 | NYSE ARCA | 486 | 0 | 486 | official_full |
| 6 | NSE_IN | 483 | 483 | 0 | official_full |
| 7 | SSE | 479 | 0 | 479 | official_partial |
| 8 | TSXV | 421 | 418 | 3 | official_full |
| 9 | SZSE | 337 | 0 | 337 | official_partial |
| 10 | XETRA | 314 | 120 | 194 | official_full |
| 11 | TSX | 259 | 97 | 162 | official_full |
| 12 | STO | 235 | 231 | 4 | official_partial |

## Model Migration Prep

- `stock_sector` should become the internal target for stock sector backfills.
- `etf_category` should become the internal target for ETF category backfills.
- The legacy `sector` export has been removed to avoid duplicating typed metadata.
- Future full-universe ingestion should be `listing_key`-first because official symbol collisions still block global-unique ticker ingestion.

## Source Block Order

1. High-count primary ISIN residuals
2. High-count stock-sector residuals
3. High-count ETF-category residuals
4. OTC warning review queue
5. Source-gap venues by missing count
6. Missing venues
