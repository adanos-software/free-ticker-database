# Completion Backlog

Generated at: `2026-07-02T12:49:20Z`

## Summary

- Missing primary ISIN rows: `946`
- Missing stock sectors: `94`
- Missing ETF categories: `96`
- Official symbol collisions tracked in exchange references: `11262`
- Core rows hidden only by the legacy global-ticker compatibility export: `2025`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 106 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | NASDAQ | missing_isin_primary | 194 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 3 | TSX | missing_isin_primary | 100 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 4 | NYSE | missing_isin_primary | 95 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 5 | TSXV | missing_isin_primary | 82 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 6 | NYSE ARCA | missing_isin_primary | 69 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 7 | OTC | missing_sector_stock | 43 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 8 | NEO | missing_isin_primary | 43 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 9 | SSE | missing_isin_primary | 37 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 10 | PSX | missing_isin_primary | 33 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 11 | SSE_CL | missing_isin_primary | 28 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 12 | BATS | missing_isin_primary | 26 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | All | 194 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 2 | ASX | All | 106 | official_partial | Official ASX ISIN workbook. | no |
| 3 | TSX | All | 100 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | NYSE | All | 95 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | TSXV | All | 82 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | NYSE ARCA | All | 69 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 8 | SSE | All | 37 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 9 | PSX | All | 33 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 10 | SSE_CL | All | 28 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | BATS | All | 26 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 12 | QSE | All | 25 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 43 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | NASDAQ | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | NYSE | Stock | 7 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | B3 | Stock | 5 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 5 | LSE | Stock | 3 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | ASX | Stock | 3 | official_partial | Official exchange industry classifications first; FinanceDatabase as reviewed fallback. | yes |
| 7 | BK | Stock | 3 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | Euronext | Stock | 3 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | XETRA | Stock | 2 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | CSE_LK | Stock | 2 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | TSX | Stock | 1 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 12 | BMV | Stock | 1 | official_partial | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | ETF | 14 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | OTC | ETF | 9 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | ASX | ETF | 9 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | SSE_CL | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | NGX | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | TSX | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | BATS | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | TSE | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | BVB | ETF | 4 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | LSE | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | NYSE | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | TSXV | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 52 | 43 | 9 | official_full |
| 2 | NASDAQ | 18 | 17 | 1 | official_full |
| 3 | NYSE ARCA | 14 | 0 | 14 | official_full |
| 4 | ASX | 12 | 3 | 9 | official_partial |
| 5 | NYSE | 10 | 7 | 3 | official_full |
| 6 | SSE_CL | 8 | 0 | 8 | official_full |
| 7 | LSE | 6 | 3 | 3 | official_full |
| 8 | NGX | 6 | 0 | 6 | official_full |
| 9 | TSX | 6 | 1 | 5 | official_full |
| 10 | B3 | 5 | 5 | 0 | official_full |
| 11 | BATS | 5 | 0 | 5 | official_full |
| 12 | TSE | 5 | 0 | 5 | official_full |

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
