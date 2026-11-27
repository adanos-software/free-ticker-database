# Completion Backlog

Generated at: `2026-11-27T16:41:31Z`

## Summary

- Missing primary ISIN rows: `646`
- Missing stock sectors: `571`
- Missing ETF categories: `78`
- Official symbol collisions tracked in exchange references: `11773`
- Core rows hidden only by the legacy global-ticker compatibility export: `4841`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | missing_isin_primary | 110 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 2 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 3 | TSE | missing_sector_stock | 1 | candidate_for_official_followup | Official JPX listed-issues verification shows exact TSE matches but no JPX 33-industry values; use official REIT/infrastructure-fund taxonomy evidence before any stock_sector update. | yes |
| 4 | FSX | missing_sector_stock | 201 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 5 | OTC | missing_sector_stock | 87 | candidate_for_official_followup | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | yes |
| 6 | NASDAQ | missing_isin_primary | 87 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | TSX | missing_isin_primary | 75 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 8 | TSXV | missing_isin_primary | 56 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 9 | BATS | missing_isin_primary | 56 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 10 | HKEX | missing_sector_stock | 46 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | NSE_IN | missing_sector_stock | 43 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | XSTU | missing_sector_stock | 33 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | All | 110 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 2 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 3 | NASDAQ | All | 87 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | TSX | All | 75 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | TSXV | All | 56 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | BATS | All | 56 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | SSE | All | 31 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 8 | NEO | All | 25 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | NYSE | All | 25 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | SSE_CL | All | 19 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | B3 | All | 12 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 12 | SZSE | All | 11 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 201 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 87 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | HKEX | Stock | 46 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | NSE_IN | Stock | 43 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | XSTU | Stock | 33 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | NASDAQ | Stock | 18 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | BSE_IN | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | CSE_LK | Stock | 16 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | XETRA | Stock | 15 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | BVB | Stock | 14 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | B3 | Stock | 11 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 12 | HNX | Stock | 10 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | AMS | ETF | 17 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | XETRA | ETF | 16 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 12 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | NYSE ARCA | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | BATS | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | TSE | ETF | 7 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | TSX | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 201 | 201 | 0 | official_full |
| 2 | OTC | 87 | 87 | 0 | official_full |
| 3 | HKEX | 46 | 46 | 0 | official_full |
| 4 | NSE_IN | 43 | 43 | 0 | official_full |
| 5 | XSTU | 33 | 33 | 0 | missing |
| 6 | XETRA | 31 | 15 | 16 | official_full |
| 7 | NASDAQ | 30 | 18 | 12 | official_full |
| 8 | AMS | 18 | 1 | 17 | official_full |
| 9 | BSE_IN | 17 | 17 | 0 | official_full |
| 10 | CSE_LK | 16 | 16 | 0 | official_full |
| 11 | BVB | 14 | 14 | 0 | official_full |
| 12 | B3 | 11 | 11 | 0 | official_full |

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
