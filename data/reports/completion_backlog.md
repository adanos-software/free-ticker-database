# Completion Backlog

Generated at: `2026-07-28T09:22:11Z`

## Summary

- Missing primary ISIN rows: `778`
- Missing stock sectors: `34`
- Missing ETF categories: `108`
- Official symbol collisions tracked in exchange references: `11538`
- Core rows hidden only by the legacy global-ticker compatibility export: `2028`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | missing_isin_primary | 132 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 2 | ASX | missing_isin_primary | 99 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 3 | TSX | missing_isin_primary | 90 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 4 | BATS | missing_isin_primary | 83 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 5 | TSXV | missing_isin_primary | 80 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 6 | NYSE ARCA | missing_isin_primary | 77 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 7 | BATS | missing_etf_category | 62 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | NYSE | missing_isin_primary | 54 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | missing_isin_primary | 43 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 10 | SSE | missing_isin_primary | 35 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 11 | SSE_CL | missing_isin_primary | 27 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe cannot map this venue; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source before applying ISINs. | yes |
| 12 | NYSE ARCA | missing_etf_category | 26 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | All | 132 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 2 | ASX | All | 99 | official_partial | Official ASX ISIN workbook. | no |
| 3 | TSX | All | 90 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | BATS | All | 83 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | TSXV | All | 80 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | NYSE ARCA | All | 77 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NYSE | All | 54 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 10 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | SZSE | All | 12 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NASDAQ | Stock | 25 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | NYSE | Stock | 6 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | B3 | Stock | 1 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 4 | HKEX | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | NYSE MKT | Stock | 1 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BATS | ETF | 62 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 26 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 20 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BATS | 62 | 0 | 62 | official_full |
| 2 | NASDAQ | 45 | 25 | 20 | official_full |
| 3 | NYSE ARCA | 26 | 0 | 26 | official_full |
| 4 | NYSE | 6 | 6 | 0 | official_full |
| 5 | B3 | 1 | 1 | 0 | official_full |
| 6 | HKEX | 1 | 1 | 0 | official_full |
| 7 | NYSE MKT | 1 | 1 | 0 | official_full |

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
