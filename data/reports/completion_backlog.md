# Completion Backlog

Generated at: `2026-05-03T20:01:08Z`

## Summary

- Missing primary ISIN rows: `4578`
- Missing stock sectors: `9686`
- Missing ETF categories: `2452`
- Official symbol collisions blocking global-unique ticker ingestion: `10922`

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | SSE | All | 614 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 2 | SGX | All | 584 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 3 | TSX | All | 572 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | TSXV | All | 545 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | SZSE | All | 487 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 6 | B3 | All | 276 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 7 | SET | All | 196 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 8 | NYSE ARCA | All | 174 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NASDAQ | All | 147 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | NEO | All | 118 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 11 | IDX | All | 115 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | PSX | All | 107 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BSE_IN | Stock | 2632 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 1812 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | HKEX | Stock | 1139 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | SGX | Stock | 501 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
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
| 1 | KRX | ETF | 477 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | SSE | ETF | 438 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NYSE ARCA | ETF | 327 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | SZSE | ETF | 322 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | B3 | ETF | 170 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | NASDAQ | ETF | 157 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | ASX | ETF | 133 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | BATS | ETF | 126 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | TSX | ETF | 71 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | LSE | ETF | 42 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | TWSE | ETF | 37 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | TSE | ETF | 27 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BSE_IN | 2633 | 2632 | 1 | official_full |
| 2 | OTC | 1831 | 1812 | 19 | official_full |
| 3 | HKEX | 1139 | 1139 | 0 | official_full |
| 4 | SGX | 501 | 501 | 0 | official_full |
| 5 | KRX | 477 | 0 | 477 | official_full |
| 6 | B3 | 449 | 279 | 170 | official_full |
| 7 | SSE | 438 | 0 | 438 | official_partial |
| 8 | TSXV | 420 | 417 | 3 | official_full |
| 9 | LSE | 367 | 325 | 42 | official_full |
| 10 | NYSE ARCA | 327 | 0 | 327 | official_full |
| 11 | SZSE | 322 | 0 | 322 | official_partial |
| 12 | CSE_LK | 307 | 307 | 0 | official_full |

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
