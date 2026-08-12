# Completion Backlog

Generated at: `2026-08-12T08:03:43Z`

## Summary

- Missing primary ISIN rows: `1134`
- Missing stock sectors: `1898`
- Missing ETF categories: `395`
- Official symbol collisions tracked in exchange references: `9674`
- Core rows hidden only by the legacy global-ticker compatibility export: `4787`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | missing_isin_primary | 169 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 2 | FSX | missing_sector_stock | 883 | candidate_for_official_followup | Official exchange industry feed or reviewed secondary company profile. | yes |
| 3 | OTC | missing_sector_stock | 556 | candidate_for_official_followup | Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status. | yes |
| 4 | NASDAQ | missing_isin_primary | 154 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 5 | SET | missing_isin_primary | 140 | candidate_for_official_followup | Separate official CSD/security registry or exchange detail feed with ISIN. | yes |
| 6 | BATS | missing_isin_primary | 115 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 7 | TSXV | missing_isin_primary | 97 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 8 | TSX | missing_sector_stock | 96 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 9 | BATS | missing_etf_category | 94 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | NYSE ARCA | missing_isin_primary | 90 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 11 | TSX | missing_isin_primary | 77 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 12 | IDX | missing_isin_primary | 62 | candidate_for_official_followup | Official exchange masterfile or reviewed secondary identifier source. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | ASX | All | 169 | official_partial | Official ASX ISIN workbook. | no |
| 2 | NASDAQ | All | 154 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 3 | SET | All | 140 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 4 | BATS | All | 115 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 5 | TSXV | All | 97 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | NYSE ARCA | All | 90 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | TSX | All | 77 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 8 | IDX | All | 62 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 9 | NYSE | All | 51 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | NEO | All | 43 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 11 | SSE | All | 35 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 12 | SSE_CL | All | 27 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 883 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 556 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | TSX | Stock | 96 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 4 | NASDAQ | Stock | 60 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | BSE_IN | Stock | 49 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | XSTU | Stock | 45 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 7 | LSE | Stock | 38 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 8 | TSXV | Stock | 32 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 9 | TASE | Stock | 25 | official_partial | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 10 | NYSE | Stock | 18 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | Munich | Stock | 10 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | TWSE | Stock | 10 | official_full | Official exchange industry classifications first; FinanceDatabase as reviewed fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | BATS | ETF | 94 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NASDAQ | ETF | 62 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | LSE | ETF | 50 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | NYSE ARCA | ETF | 47 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | Euronext | ETF | 37 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | ASX | ETF | 35 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | TSX | ETF | 23 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | XETRA | ETF | 20 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | AMS | ETF | 7 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | FSX | ETF | 7 | missing | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 11 | NEO | ETF | 7 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 12 | NYSE | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 890 | 883 | 7 | missing |
| 2 | OTC | 556 | 556 | 0 | official_full |
| 3 | NASDAQ | 122 | 60 | 62 | official_full |
| 4 | TSX | 119 | 96 | 23 | official_full |
| 5 | BATS | 94 | 0 | 94 | official_full |
| 6 | LSE | 88 | 38 | 50 | official_full |
| 7 | BSE_IN | 49 | 49 | 0 | official_full |
| 8 | NYSE ARCA | 47 | 0 | 47 | official_full |
| 9 | XSTU | 47 | 45 | 2 | missing |
| 10 | Euronext | 46 | 9 | 37 | official_full |
| 11 | ASX | 41 | 6 | 35 | official_partial |
| 12 | TSXV | 32 | 32 | 0 | official_full |

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
