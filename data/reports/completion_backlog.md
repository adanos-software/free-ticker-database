# Completion Backlog

Generated at: `2026-05-16T17:23:11Z`

## Summary

- Missing primary ISIN rows: `1059`
- Missing stock sectors: `2598`
- Missing ETF categories: `105`
- Official symbol collisions tracked in exchange references: `11096`
- Core rows hidden only by the legacy global-ticker compatibility export: `1`

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | B3 | All | 225 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 2 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |
| 3 | TSX | All | 102 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | MSX | All | 90 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 5 | TSXV | All | 82 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | NYSE ARCA | All | 52 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NEO | All | 45 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 8 | SSE | All | 42 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 9 | NASDAQ | All | 40 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | NYSE | All | 39 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 11 | PSX | All | 35 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SSE_CL | All | 29 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 817 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | B3 | Stock | 190 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 3 | CSE_LK | Stock | 143 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | Euronext | Stock | 132 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | LSE | Stock | 128 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | BK | Stock | 102 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | TSXV | Stock | 97 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | PSE | Stock | 76 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | CSE_MA | Stock | 64 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | OSL | Stock | 58 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | STO | Stock | 54 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 12 | SEM | Stock | 47 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | ASX | ETF | 9 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | SSE_CL | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | NYSE ARCA | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | NGX | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | BATS | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | TSE | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | BVB | ETF | 4 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | B3 | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | TSX | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | TSXV | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | KRX | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 846 | 817 | 29 | official_full |
| 2 | B3 | 193 | 190 | 3 | official_full |
| 3 | CSE_LK | 143 | 143 | 0 | official_full |
| 4 | Euronext | 133 | 132 | 1 | official_full |
| 5 | LSE | 130 | 128 | 2 | official_full |
| 6 | BK | 102 | 102 | 0 | official_full |
| 7 | TSXV | 100 | 97 | 3 | official_full |
| 8 | PSE | 76 | 76 | 0 | official_full |
| 9 | CSE_MA | 64 | 64 | 0 | official_full |
| 10 | OSL | 59 | 58 | 1 | official_full |
| 11 | STO | 55 | 54 | 1 | official_partial |
| 12 | SEM | 47 | 47 | 0 | official_full |

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
