# Completion Backlog

Generated at: `2026-04-12T14:24:53Z`

## Summary

- Missing primary ISIN rows: `7089`
- Missing stock sectors: `7583`
- Missing ETF categories: `4505`
- Official symbol collisions blocking global-unique ticker ingestion: `6345`

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSE | All | 3148 | official_full | Official JPX/TSE ISIN-capable source; current JPX listed-issues coverage does not fill most ISINs. | yes |
| 2 | SSE | All | 614 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 3 | TSX | All | 562 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | TSXV | All | 538 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | SZSE | All | 487 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 6 | B3 | All | 285 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 7 | NYSE ARCA | All | 197 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | SET | All | 195 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 9 | NASDAQ | All | 140 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | NEO | All | 118 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 11 | IDX | All | 115 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 1996 | official_partial | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | SSE | Stock | 788 | official_partial | Official exchange industry classifications first; FinanceDatabase as reviewed fallback. | yes |
| 3 | SZSE | Stock | 807 | official_partial | Official exchange industry classifications first; FinanceDatabase as reviewed fallback. | yes |
| 4 | XETRA | Stock | 126 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 5 | B3 | Stock | 279 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | KRX | Stock | 94 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | LSE | Stock | 348 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | TSX | Stock | 97 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | TSXV | Stock | 418 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | KOSDAQ | Stock | 355 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 11 | ASX | Stock | 312 | official_partial | Official exchange industry classifications first; FinanceDatabase as reviewed fallback. | yes |
| 12 | STO | Stock | 231 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | ETF | 40 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | SSE | ETF | 479 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | SZSE | ETF | 337 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | XETRA | ETF | 397 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | B3 | ETF | 603 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | NYSE ARCA | ETF | 522 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | KRX | ETF | 527 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | LSE | ETF | 223 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | TSX | ETF | 205 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | NASDAQ | ETF | 236 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | BATS | ETF | 229 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | ASX | ETF | 146 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 2036 | 1996 | 40 | official_partial |
| 2 | SSE | 1267 | 788 | 479 | official_partial |
| 3 | SZSE | 1144 | 807 | 337 | official_partial |
| 4 | XETRA | 523 | 126 | 397 | official_partial |
| 5 | B3 | 882 | 279 | 603 | official_full |
| 6 | NYSE ARCA | 522 | 0 | 522 | official_full |
| 7 | KRX | 621 | 94 | 527 | official_partial |
| 8 | LSE | 571 | 348 | 223 | official_partial |
| 9 | TSX | 302 | 97 | 205 | official_full |
| 10 | ASX | 458 | 312 | 146 | official_partial |
| 11 | TSXV | 421 | 418 | 3 | official_full |
| 12 | KOSDAQ | 355 | 355 | 0 | official_partial |

## Model Migration Prep

- `stock_sector` should become the internal target for stock sector backfills.
- `etf_category` should become the internal target for ETF category backfills.
- The existing `sector` field should remain as a legacy derived output until downstream consumers migrate.
- Future full-universe ingestion should be `listing_key`-first because official symbol collisions still block global-unique ticker ingestion.

## Source Block Order

1. TSE ISIN
2. China ETF/Sector
3. Canada
4. B3
5. XETRA/LSE ETF categories
6. Missing venues
