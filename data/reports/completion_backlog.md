# Completion Backlog

Generated at: `2026-08-06T09:27:14Z`

## Summary

- Missing primary ISIN rows: `1144`
- Missing stock sectors: `157`
- Missing ETF categories: `141`
- Official symbol collisions tracked in exchange references: `11248`
- Core rows hidden only by the legacy global-ticker compatibility export: `2501`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 182 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | NASDAQ | missing_isin_primary | 148 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 3 | SET | missing_isin_primary | 144 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 4 | TSXV | missing_isin_primary | 105 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 5 | BATS | missing_isin_primary | 105 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 6 | NYSE ARCA | missing_isin_primary | 84 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 7 | BATS | missing_etf_category | 84 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | TSX | missing_isin_primary | 79 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 9 | IDX | missing_isin_primary | 62 | candidate_for_official_followup | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 10 | NYSE | missing_isin_primary | 54 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 11 | BSE_IN | missing_sector_stock | 51 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | NEO | missing_isin_primary | 43 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | All | 182 | official_partial | Official ASX ISIN workbook. | no |
| 2 | NASDAQ | All | 148 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 3 | SET | All | 144 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 4 | TSXV | All | 105 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | BATS | All | 105 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | NYSE ARCA | All | 84 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | TSX | All | 79 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 8 | IDX | All | 62 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 9 | NYSE | All | 54 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 11 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 12 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BSE_IN | Stock | 51 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | NASDAQ | Stock | 41 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | OTC | Stock | 22 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 4 | LSE | Stock | 17 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 5 | NYSE | Stock | 12 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | SGX | Stock | 4 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | XETRA | Stock | 2 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | TASE | Stock | 2 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | TSXV | Stock | 2 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | B3 | Stock | 1 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 11 | Euronext | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | HKEX | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BATS | ETF | 84 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 33 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 24 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BATS | 84 | 0 | 84 | official_full |
| 2 | NASDAQ | 65 | 41 | 24 | official_full |
| 3 | BSE_IN | 51 | 51 | 0 | official_full |
| 4 | NYSE ARCA | 33 | 0 | 33 | official_full |
| 5 | OTC | 22 | 22 | 0 | official_full |
| 6 | LSE | 17 | 17 | 0 | official_full |
| 7 | NYSE | 12 | 12 | 0 | official_full |
| 8 | SGX | 4 | 4 | 0 | official_full |
| 9 | TASE | 2 | 2 | 0 | official_partial |
| 10 | TSXV | 2 | 2 | 0 | official_full |
| 11 | XETRA | 2 | 2 | 0 | official_full |
| 12 | B3 | 1 | 1 | 0 | official_full |

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
