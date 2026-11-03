# Completion Backlog

Generated at: `2026-11-03T17:33:24Z`

## Summary

- Missing primary ISIN rows: `633`
- Missing stock sectors: `628`
- Missing ETF categories: `96`
- Official symbol collisions tracked in exchange references: `11739`
- Core rows hidden only by the legacy global-ticker compatibility export: `4858`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | FSX | missing_sector_stock | 233 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 3 | OTC | missing_sector_stock | 92 | candidate_for_official_followup | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | yes |
| 4 | TSX | missing_isin_primary | 89 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 5 | NYSE ARCA | missing_isin_primary | 73 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 6 | NASDAQ | missing_isin_primary | 72 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | TSXV | missing_isin_primary | 61 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 8 | XETRA | missing_etf_category | 42 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | NEO | missing_isin_primary | 41 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 10 | BATS | missing_isin_primary | 40 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 11 | XSTU | missing_sector_stock | 35 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | SSE | missing_isin_primary | 34 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 2 | TSX | All | 89 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 3 | NYSE ARCA | All | 73 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | NASDAQ | All | 72 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | TSXV | All | 61 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | NEO | All | 41 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 7 | BATS | All | 40 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | SSE | All | 34 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 9 | NYSE | All | 26 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | SSE_CL | All | 22 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SZSE | All | 11 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 233 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 92 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | XSTU | Stock | 35 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | NSE_IN | Stock | 34 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | HNX | Stock | 29 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | BVB | Stock | 28 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | HKEX | Stock | 23 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | BIST | Stock | 19 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | XETRA | Stock | 18 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | BSE_IN | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | SGX | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | CSE_LK | Stock | 14 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | XETRA | ETF | 42 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | AMS | ETF | 18 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NYSE ARCA | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | TSX | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | TSE | ETF | 7 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | NASDAQ | ETF | 4 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | NYSE | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 233 | 233 | 0 | official_full |
| 2 | OTC | 92 | 92 | 0 | official_full |
| 3 | XETRA | 60 | 18 | 42 | official_full |
| 4 | XSTU | 35 | 35 | 0 | missing |
| 5 | NSE_IN | 34 | 34 | 0 | official_full |
| 6 | HNX | 29 | 29 | 0 | official_full |
| 7 | BVB | 28 | 28 | 0 | official_full |
| 8 | HKEX | 23 | 23 | 0 | official_full |
| 9 | AMS | 19 | 1 | 18 | official_full |
| 10 | BIST | 19 | 19 | 0 | official_full |
| 11 | BSE_IN | 17 | 17 | 0 | official_full |
| 12 | SGX | 17 | 17 | 0 | official_full |

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
