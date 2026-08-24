# Completion Backlog

Generated at: `2026-08-24T09:31:38Z`

## Summary

- Missing primary ISIN rows: `690`
- Missing stock sectors: `1508`
- Missing ETF categories: `31`
- Official symbol collisions tracked in exchange references: `13816`
- Core rows hidden only by the legacy global-ticker compatibility export: `4780`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | missing_sector_stock | 810 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 2 | OTC | missing_sector_stock | 553 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 3 | ASX | missing_isin_primary | 99 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 4 | TSXV | missing_isin_primary | 97 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 5 | TSX | missing_isin_primary | 77 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 6 | IDX | missing_isin_primary | 62 | candidate_for_official_followup | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 7 | NYSE | missing_isin_primary | 53 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NASDAQ | missing_isin_primary | 49 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NYSE ARCA | missing_isin_primary | 48 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 10 | XSTU | missing_sector_stock | 45 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | NEO | missing_isin_primary | 43 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN. | yes |
| 12 | BATS | missing_isin_primary | 37 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | All | 99 | official_partial | Official ASX ISIN workbook. | no |
| 2 | TSXV | All | 97 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 3 | TSX | All | 77 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 4 | IDX | All | 62 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 5 | NYSE | All | 53 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | NASDAQ | All | 49 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | NYSE ARCA | All | 48 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | BATS | All | 37 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 11 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 810 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 553 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | XSTU | Stock | 45 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | NASDAQ | Stock | 34 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | NYSE | Stock | 12 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | Munich | Stock | 10 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | PSE | Stock | 7 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | LSE | Stock | 5 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | XHAM | Stock | 4 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | KRX | Stock | 3 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 11 | BCBA | Stock | 3 | official_partial | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | XDUS | Stock | 3 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BATS | ETF | 17 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 8 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 4 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 810 | 810 | 0 | official_full |
| 2 | OTC | 553 | 553 | 0 | official_full |
| 3 | XSTU | 45 | 45 | 0 | missing |
| 4 | NASDAQ | 38 | 34 | 4 | official_full |
| 5 | BATS | 17 | 0 | 17 | official_full |
| 6 | NYSE | 12 | 12 | 0 | official_full |
| 7 | Munich | 10 | 10 | 0 | missing |
| 8 | NYSE ARCA | 8 | 0 | 8 | official_full |
| 9 | PSE | 7 | 7 | 0 | official_full |
| 10 | LSE | 5 | 5 | 0 | official_full |
| 11 | XHAM | 4 | 4 | 0 | missing |
| 12 | BCBA | 3 | 3 | 0 | official_partial |

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
