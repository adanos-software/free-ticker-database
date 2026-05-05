# Completion Backlog

Generated at: `2026-05-05T06:47:49Z`

## Summary

- Missing primary ISIN rows: `3917`
- Missing stock sectors: `5549`
- Missing ETF categories: `452`
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
| 6 | SET | All | 195 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 7 | NYSE ARCA | All | 173 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NASDAQ | All | 139 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | All | 118 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 10 | IDX | All | 115 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | PSX | All | 107 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 1787 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | TSXV | Stock | 416 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 3 | LSE | Stock | 325 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 4 | SGX | Stock | 268 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | NSE_IN | Stock | 244 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | STO | Stock | 231 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | B3 | Stock | 204 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | TADAWUL | Stock | 163 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | CSE_LK | Stock | 143 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | BSE_IN | Stock | 141 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | Euronext | Stock | 132 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | XETRA | Stock | 120 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | ETF | 180 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NASDAQ | ETF | 56 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | BATS | ETF | 49 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | B3 | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | LSE | ETF | 19 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | OTC | ETF | 18 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | TSX | ETF | 16 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | ASX | ETF | 15 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | NYSE | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | SSE_CL | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | XETRA | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | NGX | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 1805 | 1787 | 18 | official_full |
| 2 | TSXV | 419 | 416 | 3 | official_full |
| 3 | LSE | 344 | 325 | 19 | official_full |
| 4 | SGX | 268 | 268 | 0 | official_full |
| 5 | NSE_IN | 244 | 244 | 0 | official_full |
| 6 | B3 | 233 | 204 | 29 | official_full |
| 7 | STO | 232 | 231 | 1 | official_partial |
| 8 | NYSE ARCA | 180 | 0 | 180 | official_full |
| 9 | TADAWUL | 163 | 163 | 0 | official_full |
| 10 | CSE_LK | 143 | 143 | 0 | official_full |
| 11 | BSE_IN | 142 | 141 | 1 | official_full |
| 12 | Euronext | 133 | 132 | 1 | official_full |

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
