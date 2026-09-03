# Completion Backlog

Generated at: `2026-09-03T11:44:33Z`

## Summary

- Missing primary ISIN rows: `764`
- Missing stock sectors: `1525`
- Missing ETF categories: `43`
- Official symbol collisions tracked in exchange references: `13826`
- Core rows hidden only by the legacy global-ticker compatibility export: `4772`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | missing_sector_stock | 807 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 2 | OTC | missing_sector_stock | 554 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 3 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 4 | TSXV | missing_isin_primary | 98 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 5 | NYSE ARCA | missing_isin_primary | 81 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 6 | TSX | missing_isin_primary | 77 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 7 | NASDAQ | missing_isin_primary | 68 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | IDX | missing_isin_primary | 62 | candidate_for_official_followup | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 9 | NYSE | missing_isin_primary | 58 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | BATS | missing_isin_primary | 53 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 11 | NASDAQ | missing_sector_stock | 48 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | XSTU | missing_sector_stock | 45 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | TSXV | All | 98 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 2 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 3 | NYSE ARCA | All | 81 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | TSX | All | 77 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | NASDAQ | All | 68 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 6 | IDX | All | 62 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 7 | NYSE | All | 58 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 8 | BATS | All | 53 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 9 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 10 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 11 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 12 | BMV | All | 17 | official_partial | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 807 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 554 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | NASDAQ | Stock | 48 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | XSTU | Stock | 45 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | NYSE | Stock | 14 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | Munich | Stock | 10 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | PSE | Stock | 7 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | LSE | Stock | 6 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | KRX | Stock | 4 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | XHAM | Stock | 4 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | BCBA | Stock | 3 | official_partial | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | XDUS | Stock | 3 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | ETF | 28 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | BATS | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | NASDAQ | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | NYSE | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 807 | 807 | 0 | official_full |
| 2 | OTC | 554 | 554 | 0 | official_full |
| 3 | NASDAQ | 54 | 48 | 6 | official_full |
| 4 | XSTU | 45 | 45 | 0 | missing |
| 5 | NYSE ARCA | 28 | 0 | 28 | official_full |
| 6 | NYSE | 15 | 14 | 1 | official_full |
| 7 | Munich | 10 | 10 | 0 | missing |
| 8 | PSE | 7 | 7 | 0 | official_full |
| 9 | BATS | 6 | 0 | 6 | official_full |
| 10 | LSE | 6 | 6 | 0 | official_full |
| 11 | KRX | 4 | 4 | 0 | official_full |
| 12 | XHAM | 4 | 4 | 0 | missing |

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
