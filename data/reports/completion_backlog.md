# Completion Backlog

Generated at: `2026-10-28T13:33:34Z`

## Summary

- Missing primary ISIN rows: `685`
- Missing stock sectors: `1087`
- Missing ETF categories: `142`
- Official symbol collisions tracked in exchange references: `11529`
- Core rows hidden only by the legacy global-ticker compatibility export: `4853`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | missing_sector_stock | 552 | candidate_for_official_followup | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | yes |
| 2 | TSX | missing_isin_primary | 128 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 3 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 4 | FSX | missing_sector_stock | 238 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 5 | TSXV | missing_isin_primary | 73 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 6 | NYSE ARCA | missing_isin_primary | 73 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 7 | NASDAQ | missing_isin_primary | 67 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | TSX | missing_etf_category | 51 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | NEO | missing_isin_primary | 44 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 10 | XSTU | missing_sector_stock | 41 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | BATS | missing_isin_primary | 39 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 12 | SSE | missing_isin_primary | 34 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSX | All | 128 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 2 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 3 | TSXV | All | 73 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | NYSE ARCA | All | 73 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | NASDAQ | All | 67 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | NEO | All | 44 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 7 | BATS | All | 39 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | SSE | All | 34 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 9 | SSE_CL | All | 26 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 10 | NYSE | All | 24 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 11 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SZSE | All | 12 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 552 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | FSX | Stock | 238 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | XSTU | Stock | 41 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | HNX | Stock | 32 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | NSE_IN | Stock | 32 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | BVB | Stock | 29 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | BIST | Stock | 24 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | HKEX | Stock | 19 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | BSE_IN | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | CSE_LK | Stock | 12 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | SGX | Stock | 12 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | Munich | Stock | 10 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSX | ETF | 51 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | AMS | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | TSE | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | XETRA | ETF | 17 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | NYSE ARCA | ETF | 14 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | NASDAQ | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | NYSE | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | BATS | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 552 | 552 | 0 | official_full |
| 2 | FSX | 238 | 238 | 0 | official_full |
| 3 | TSX | 51 | 0 | 51 | official_full |
| 4 | XSTU | 41 | 41 | 0 | missing |
| 5 | HNX | 32 | 32 | 0 | official_full |
| 6 | NSE_IN | 32 | 32 | 0 | official_full |
| 7 | AMS | 29 | 0 | 29 | official_full |
| 8 | BVB | 29 | 29 | 0 | official_full |
| 9 | BIST | 24 | 24 | 0 | official_full |
| 10 | TSE | 23 | 0 | 23 | official_full |
| 11 | XETRA | 22 | 5 | 17 | official_full |
| 12 | HKEX | 19 | 19 | 0 | official_full |

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
