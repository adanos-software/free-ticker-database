# Completion Backlog

Generated at: `2026-06-10T12:45:40Z`

## Summary

- Missing primary ISIN rows: `929`
- Missing stock sectors: `2607`
- Missing ETF categories: `171`
- Official symbol collisions tracked in exchange references: `11181`
- Core rows hidden only by the legacy global-ticker compatibility export: `1`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 105 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | OTC | missing_sector_stock | 820 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 3 | TSX | missing_isin_primary | 102 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 4 | TSE | missing_sector_stock | 21 | candidate_for_official_followup | Official JPX listed-issues verification shows exact TSE matches but no JPX 33-industry values; use official REIT/infrastructure-fund taxonomy evidence before any stock_sector update. | yes |
| 5 | B3 | missing_sector_stock | 194 | candidate_for_official_followup | Stronger official B3 or issuer taxonomy source exposing sector for the exact listing. | yes |
| 6 | CSE_LK | missing_sector_stock | 143 | candidate_for_official_followup | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: cse_lk_all_security_code. | yes |
| 7 | Euronext | missing_sector_stock | 132 | candidate_for_official_followup | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: euronext_equities. | yes |
| 8 | LSE | missing_sector_stock | 130 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 9 | BK | missing_sector_stock | 102 | candidate_for_official_followup | Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: boursa_kuwait_stocks. | yes |
| 10 | TSXV | missing_sector_stock | 97 | candidate_for_official_followup | Official TMX issuer workbook classifies this row as CPC. | yes |
| 11 | MSX | missing_isin_primary | 90 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 12 | TSXV | missing_isin_primary | 82 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | All | 105 | official_partial | Official ASX ISIN workbook. | no |
| 2 | TSX | All | 102 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 3 | MSX | All | 90 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 4 | TSXV | All | 82 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | NYSE ARCA | All | 82 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | NYSE | All | 63 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NASDAQ | All | 62 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NEO | All | 45 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | SSE | All | 42 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 10 | PSX | All | 35 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | SSE_CL | All | 29 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | BATS | All | 28 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | OTC | Stock | 820 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 2 | B3 | Stock | 194 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 3 | CSE_LK | Stock | 143 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | Euronext | Stock | 132 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | LSE | Stock | 130 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 6 | BK | Stock | 102 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | TSXV | Stock | 97 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | PSE | Stock | 76 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | CSE_MA | Stock | 64 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | OSL | Stock | 58 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | STO | Stock | 54 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 12 | SEM | Stock | 47 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | LSE | ETF | 50 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | OTC | ETF | 26 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NYSE ARCA | ETF | 17 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | XETRA | ETF | 13 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | ASX | ETF | 9 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | SSE_CL | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | NGX | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | BATS | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | TSE | ETF | 5 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | BVB | ETF | 4 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | TSX | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | TSXV | ETF | 3 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | OTC | 846 | 820 | 26 | official_full |
| 2 | B3 | 194 | 194 | 0 | official_full |
| 3 | LSE | 180 | 130 | 50 | official_full |
| 4 | CSE_LK | 143 | 143 | 0 | official_full |
| 5 | Euronext | 133 | 132 | 1 | official_full |
| 6 | BK | 102 | 102 | 0 | official_full |
| 7 | TSXV | 100 | 97 | 3 | official_full |
| 8 | PSE | 76 | 76 | 0 | official_full |
| 9 | CSE_MA | 64 | 64 | 0 | official_full |
| 10 | OSL | 59 | 58 | 1 | official_full |
| 11 | XETRA | 56 | 43 | 13 | official_full |
| 12 | STO | 55 | 54 | 1 | official_partial |

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
