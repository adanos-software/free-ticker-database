# Completion Backlog

Generated at: `2026-11-18T15:39:33Z`

## Summary

- Missing primary ISIN rows: `636`
- Missing stock sectors: `592`
- Missing ETF categories: `154`
- Official symbol collisions tracked in exchange references: `11752`
- Core rows hidden only by the legacy global-ticker compatibility export: `4837`

## Next Safe Batches

| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | missing_isin_primary | 104 | candidate_for_official_followup | Official fund/trust masterfile, prospectus, or reviewed identifier feed. | yes |
| 2 | ASX | missing_isin_primary | 98 | candidate_for_official_followup | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | yes |
| 3 | TSE | missing_sector_stock | 1 | candidate_for_official_followup | Official JPX listed-issues verification shows exact TSE matches but no JPX 33-industry values; use official REIT/infrastructure-fund taxonomy evidence before any stock_sector update. | yes |
| 4 | FSX | missing_sector_stock | 218 | candidate_for_official_followup | Implemented official venue source layer; residual row needs a stronger official taxonomy/detail source. | yes |
| 5 | OTC | missing_sector_stock | 90 | candidate_for_official_followup | SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile. | yes |
| 6 | NASDAQ | missing_isin_primary | 82 | candidate_for_official_followup | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | TSX | missing_isin_primary | 81 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 8 | XETRA | missing_etf_category | 62 | candidate_for_official_followup | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | TSXV | missing_isin_primary | 57 | candidate_for_official_followup | Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN. | yes |
| 10 | BATS | missing_isin_primary | 47 | candidate_for_official_followup | Current OpenFIGI missing-ISIN probe found no accepted ISIN candidates; use official exchange, CSD, issuer, prospectus, or another reviewed identifier source. | yes |
| 11 | NSE_IN | missing_sector_stock | 42 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | HKEX | missing_sector_stock | 39 | candidate_for_official_followup | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.

## Top Missing Primary ISINs

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | NYSE ARCA | All | 104 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 2 | ASX | All | 98 | official_partial | Official ASX ISIN workbook. | no |
| 3 | NASDAQ | All | 82 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 4 | TSX | All | 81 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 5 | TSXV | All | 57 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 6 | BATS | All | 47 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 7 | SSE | All | 33 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |
| 8 | NEO | All | 25 | official_full | TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks. | yes |
| 9 | NYSE | All | 25 | official_full | Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals. | yes |
| 10 | SSE_CL | All | 19 | official_full | Official exchange masterfile or reviewed secondary identifier source. | yes |
| 11 | B3 | All | 12 | official_full | Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers. | yes |
| 12 | SZSE | All | 11 | official_partial | Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows. | yes |

## Top Missing Stock Sectors

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | FSX | Stock | 218 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 2 | OTC | Stock | 90 | official_full | SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals. | yes |
| 3 | NSE_IN | Stock | 42 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 4 | HKEX | Stock | 39 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 5 | XSTU | Stock | 33 | missing | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 6 | XETRA | Stock | 18 | official_full | FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available. | yes |
| 7 | BSE_IN | Stock | 18 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 8 | BVB | Stock | 17 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 9 | BIST | Stock | 15 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 10 | CSE_LK | Stock | 15 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 11 | NASDAQ | Stock | 15 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |
| 12 | SGX | Stock | 15 | official_full | Official industry classification or reviewed FinanceDatabase sector fallback. | yes |

## Top Missing ETF Categories

| Rank | Exchange | Asset type | Missing | Venue | Source | Review |
|---|---|---|---:|---|---|---|
| 1 | XETRA | ETF | 62 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 2 | NYSE ARCA | ETF | 31 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 3 | AMS | ETF | 18 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 4 | NASDAQ | ETF | 15 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 5 | TSX | ETF | 11 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 6 | TSE | ETF | 7 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 7 | BATS | ETF | 6 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 8 | NYSE | ETF | 2 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 9 | Euronext | ETF | 1 | official_full | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |
| 10 | WSE | ETF | 1 | official_partial | Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available. | yes |

## Combined Sector/ETF Category Priority

| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |
|---|---|---:|---:|---:|---|
| 1 | FSX | 218 | 218 | 0 | official_full |
| 2 | OTC | 90 | 90 | 0 | official_full |
| 3 | XETRA | 80 | 18 | 62 | official_full |
| 4 | NSE_IN | 42 | 42 | 0 | official_full |
| 5 | HKEX | 39 | 39 | 0 | official_full |
| 6 | XSTU | 33 | 33 | 0 | missing |
| 7 | NYSE ARCA | 31 | 0 | 31 | official_full |
| 8 | NASDAQ | 30 | 15 | 15 | official_full |
| 9 | AMS | 19 | 1 | 18 | official_full |
| 10 | BSE_IN | 18 | 18 | 0 | official_full |
| 11 | BVB | 17 | 17 | 0 | official_full |
| 12 | BIST | 15 | 15 | 0 | official_full |

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
