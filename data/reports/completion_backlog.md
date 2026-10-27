# Completion Backlog

Generated at: `2026-10-27T12:43:04Z`

## Summary

- Missing primary ISIN rows: `737`
- Missing stock sectors: `1080`
- Missing ETF categories: `187`
- Official symbol collisions tracked in exchange references: `11529`
- Core rows hidden only by the legacy global-ticker compatibility export: `4848`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | missing_sector_stock | 552 | candidate_for_official_followup | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | yes |
| 2 | TSX | missing_isin_primary | 128 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 3 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 4 | FSX | missing_sector_stock | 239 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 5 | NASDAQ | missing_isin_primary | 86 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | NYSE ARCA | missing_isin_primary | 82 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 7 | TSXV | missing_isin_primary | 73 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 8 | BATS | missing_isin_primary | 57 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 9 | TSX | missing_etf_category | 51 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | NEO | missing_isin_primary | 44 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 11 | XSTU | missing_sector_stock | 42 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | SSE | missing_isin_primary | 34 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSX | All | 128 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 2 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 3 | NASDAQ | All | 86 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | NYSE ARCA | All | 82 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | TSXV | All | 73 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | BATS | All | 57 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NEO | All | 44 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 8 | SSE | All | 34 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 9 | NYSE | All | 26 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | SSE_CL | All | 26 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | BMV | All | 18 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SZSE | All | 12 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 552 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | FSX | Stock | 239 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | XSTU | Stock | 42 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | HNX | Stock | 32 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | BVB | Stock | 30 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | BIST | Stock | 24 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | NSE_IN | Stock | 22 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | BSE_IN | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | NASDAQ | Stock | 15 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | SGX | Stock | 12 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | HKEX | Stock | 11 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | CSE_LK | Stock | 10 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSX | ETF | 51 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | AMS | ETF | 29 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | TSE | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | BATS | ETF | 19 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | XETRA | ETF | 17 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | NASDAQ | ETF | 15 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | NYSE | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 552 | 552 | 0 | official_full |
| 2 | FSX | 239 | 239 | 0 | official_full |
| 3 | TSX | 51 | 0 | 51 | official_full |
| 4 | XSTU | 42 | 42 | 0 | missing |
| 5 | HNX | 32 | 32 | 0 | official_full |
| 6 | BVB | 30 | 30 | 0 | official_full |
| 7 | NASDAQ | 30 | 15 | 15 | official_full |
| 8 | AMS | 29 | 0 | 29 | official_full |
| 9 | NYSE ARCA | 29 | 0 | 29 | official_full |
| 10 | BIST | 24 | 24 | 0 | official_full |
| 11 | TSE | 23 | 0 | 23 | official_full |
| 12 | NSE_IN | 22 | 22 | 0 | official_full |

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
