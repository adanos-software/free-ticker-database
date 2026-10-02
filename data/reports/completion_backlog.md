# Completion Backlog

Generated at: `2026-10-02T13:10:36Z`

## Summary

- Missing primary ISIN rows: `1108`
- Missing stock sectors: `4051`
- Missing ETF categories: `435`
- Official symbol collisions tracked in exchange references: `11445`
- Core rows hidden only by the legacy global-ticker compatibility export: `4813`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 101 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | BSE_IN | missing_sector_stock | 2063 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 3 | FSX | missing_sector_stock | 795 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 4 | OTC | missing_sector_stock | 554 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 5 | TSX | missing_isin_primary | 149 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 6 | NASDAQ | missing_isin_primary | 143 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NYSE ARCA | missing_isin_primary | 117 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 8 | BATS | missing_isin_primary | 115 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 9 | TSXV | missing_isin_primary | 100 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 10 | XETRA | missing_etf_category | 147 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | BVB | missing_sector_stock | 143 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 12 | HKEX | missing_sector_stock | 92 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSX | All | 149 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 2 | NASDAQ | All | 143 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 3 | NYSE ARCA | All | 117 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | BATS | All | 115 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | ASX | All | 101 | official_partial | Official ASX ISIN workbook. | no |
| 6 | TSXV | All | 100 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 7 | NYSE | All | 68 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | IDX | All | 65 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 9 | TSE | All | 57 | official_full | Official JPX/TSE Stock Data Search detail API; supplements listed-issues rows with ISINs. | yes |
| 10 | NEO | All | 45 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 11 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 12 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BSE_IN | Stock | 2063 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | FSX | Stock | 795 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 3 | OTC | Stock | 554 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 4 | BVB | Stock | 143 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | HKEX | Stock | 92 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | NASDAQ | Stock | 67 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | NSE_IN | Stock | 49 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | XSTU | Stock | 46 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | HNX | Stock | 32 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | XETRA | Stock | 29 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 11 | BIST | Stock | 24 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | PSE | Stock | 24 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | XETRA | ETF | 147 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | TSX | ETF | 71 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NYSE ARCA | ETF | 52 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | NASDAQ | ETF | 43 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | BATS | ETF | 40 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | BSE_IN | ETF | 28 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | AMS | ETF | 27 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | TSE | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | NYSE | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | BSE_IN | 2091 | 2063 | 28 | official_full |
| 2 | FSX | 795 | 795 | 0 | official_full |
| 3 | OTC | 554 | 554 | 0 | official_full |
| 4 | XETRA | 176 | 29 | 147 | official_full |
| 5 | BVB | 143 | 143 | 0 | official_full |
| 6 | NASDAQ | 110 | 67 | 43 | official_full |
| 7 | HKEX | 92 | 92 | 0 | official_full |
| 8 | TSX | 71 | 0 | 71 | official_full |
| 9 | NYSE ARCA | 52 | 0 | 52 | official_full |
| 10 | NSE_IN | 49 | 49 | 0 | official_full |
| 11 | XSTU | 46 | 46 | 0 | missing |
| 12 | BATS | 40 | 0 | 40 | official_full |

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
