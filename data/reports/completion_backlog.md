# Completion Backlog

Generated at: `2026-08-01T08:56:22Z`

## Summary

- Missing primary ISIN rows: `799`
- Missing stock sectors: `47`
- Missing ETF categories: `122`
- Official symbol collisions tracked in exchange references: `11563`
- Core rows hidden only by the legacy global-ticker compatibility export: `2030`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | missing_isin_primary | 142 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 2 | ASX | missing_isin_primary | 99 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 3 | BATS | missing_isin_primary | 90 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 4 | TSX | missing_isin_primary | 88 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 5 | NYSE ARCA | missing_isin_primary | 81 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 6 | TSXV | missing_isin_primary | 80 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 7 | BATS | missing_etf_category | 69 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | NYSE | missing_isin_primary | 56 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | missing_isin_primary | 43 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 10 | SSE | missing_isin_primary | 35 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 11 | NASDAQ | missing_sector_stock | 32 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | NYSE ARCA | missing_etf_category | 30 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | All | 142 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 2 | ASX | All | 99 | official_partial | Official ASX ISIN workbook. | no |
| 3 | BATS | All | 90 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | TSX | All | 88 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | NYSE ARCA | All | 81 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | TSXV | All | 80 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 7 | NYSE | All | 56 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 10 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SZSE | All | 12 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | Stock | 32 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | NYSE | Stock | 8 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | LSE | Stock | 2 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 4 | OTC | Stock | 1 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 5 | XETRA | Stock | 1 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | B3 | Stock | 1 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | HKEX | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | NYSE MKT | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BATS | ETF | 69 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 30 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BATS | 69 | 0 | 69 | official_full |
| 2 | NASDAQ | 55 | 32 | 23 | official_full |
| 3 | NYSE ARCA | 30 | 0 | 30 | official_full |
| 4 | NYSE | 8 | 8 | 0 | official_full |
| 5 | LSE | 2 | 2 | 0 | official_full |
| 6 | B3 | 1 | 1 | 0 | official_full |
| 7 | HKEX | 1 | 1 | 0 | official_full |
| 8 | NYSE MKT | 1 | 1 | 0 | official_full |
| 9 | OTC | 1 | 1 | 0 | official_full |
| 10 | XETRA | 1 | 1 | 0 | official_full |

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
