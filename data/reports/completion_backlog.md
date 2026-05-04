# Completion Backlog

Generated at: `2026-05-04T08:48:10Z`

## Summary

- Missing primary ISIN rows: `3941`
- Missing stock sectors: `9647`
- Missing ETF categories: `486`
- Official symbol collisions tracked in exchange references: `10923`
- Core rows hidden only by the legacy global-ticker compatibility export: `1`

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | SSE | All | 614 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 2 | TSX | All | 562 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 3 | TSXV | All | 512 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | SZSE | All | 487 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 5 | B3 | All | 271 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 6 | SET | All | 196 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 7 | NYSE ARCA | All | 173 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NASDAQ | All | 147 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | All | 118 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 10 | IDX | All | 115 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | PSX | All | 107 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BSE_IN | Stock | 2632 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 1805 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | HKEX | Stock | 1139 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | SGX | Stock | 470 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | TSXV | Stock | 417 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | LSE | Stock | 325 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | CSE_LK | Stock | 307 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | B3 | Stock | 279 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | NSE_IN | Stock | 244 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | STO | Stock | 231 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 11 | TADAWUL | Stock | 171 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | Euronext | Stock | 132 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | ETF | 181 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NASDAQ | ETF | 62 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | BATS | ETF | 52 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | ASX | ETF | 32 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | B3 | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | TSX | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | LSE | ETF | 19 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | OTC | ETF | 18 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | NYSE | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | SSE_CL | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | XETRA | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | NGX | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BSE_IN | 2633 | 2632 | 1 | official_full |
| 2 | OTC | 1823 | 1805 | 18 | official_full |
| 3 | HKEX | 1139 | 1139 | 0 | official_full |
| 4 | SGX | 470 | 470 | 0 | official_full |
| 5 | TSXV | 420 | 417 | 3 | official_full |
| 6 | LSE | 344 | 325 | 19 | official_full |
| 7 | B3 | 308 | 279 | 29 | official_full |
| 8 | CSE_LK | 307 | 307 | 0 | official_full |
| 9 | NSE_IN | 244 | 244 | 0 | official_full |
| 10 | STO | 232 | 231 | 1 | official_partial |
| 11 | NYSE ARCA | 181 | 0 | 181 | official_full |
| 12 | TADAWUL | 171 | 171 | 0 | official_full |

## Model Migration Prep

- `stock_sector` should become the internal target for stock sector backfills.
- `etf_category` should become the internal target for ETF category backfills.
- The legacy `sector` export has been removed to avoid duplicating typed metadata.
- `core_listings.csv` is the collision-safe canonical core export keyed by `listing_key`.
- `tickers.csv` remains the legacy one-row-per-global-ticker compatibility export.

## Source Block Order

1. High-count primary ISIN residuals
2. High-count stock-sector residuals
3. High-count ETF-category residuals
4. OTC warning review queue
5. Source-gap venues by missing count
6. Missing venues
