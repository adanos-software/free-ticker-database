# Source Inventory Gap

Generated at: `2026-07-06T11:17:00Z`

## Summary

- Rows: `66`
- Current-scope candidates: `65`
- Global expansion candidates: `0`
- Todo rows: `1`
- High-priority rows: `22`
- Status counts: missing: `2`, not_in_current_universe: `1`, official_full: `30`, official_partial: `33`
- Scope counts: exchange_directory_candidate: `8`, global_expansion_candidate: `13`, listed_companies_candidate: `17`, normalization_alias: `1`, security_identifier_registry_candidate: `2`, security_lookup_subset: `1`, source_expansion_candidate: `24`

## Missing Current-Scope Sources

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|
| 1 | Borsa Italiana | missing | 277 | 0 | 0 | borsa_italiana_listino_a_z | Borsa Italiana |  |  | needs endpoint discovery and parser coverage before this current-scope venue can be marked official |
| 2 | MSE_MW | missing | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | unavailable | MSE Malawi mainboard unavailable (HTTP 403 from official host mse.co.mw) | implemented via official mainboard table and company links |

## Partial Current-Scope Sources

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|
| 3 | SSE | official_partial | 2789 | 37 | 0 | sse_industry_classification | SSE |  |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 4 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 5 | ASX | official_partial | 1629 | 106 | 37 | asx_cash_market_directory | ASX |  |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 6 | ATHEX | official_partial | 155 | 5 | 0 | athex_sector_classification | ATHEX | cache |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 7 | STO | official_partial | 834 | 0 | 2 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 8 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  |  | implemented via existing official SZSE report-list industry fields |
| 9 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 10 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 11 | BME | official_partial | 221 | 0 | 0 | bme_security_prices_directory | BME | unavailable | BME security prices rows unavailable; official detail fetch failed and no complete cache (>=500 rows) is available; partial caches are ignored | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 12 | JSE | official_partial | 212 | 7 | 1 | jse_listed_companies_directory | JSE |  |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 13 | TASE | official_partial | 672 | 0 | 13 | tase_company_profiles | TASE |  |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 14 | BSE_HU | official_partial | 50 | 5 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | cache |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 15 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | cache |  | implemented via official listed-securities table |
| 16 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  |  | parser implemented; residual gaps need ticker-level review |
| 17 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | cache |  | implemented as ISIN join against current VSE listings |
| 18 | BSE_BW | official_partial | 39 | 0 | 4 | bse_bw_listed_companies | BSE Botswana | cache |  | implemented via official companies page with conservative local listing-name matching |
| 19 | BMV | official_partial | 179 | 17 | 3 | bmv_market_data_securities | BMV | network |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 20 | HEL | official_partial | 194 | 0 | 1 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 21 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 22 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | cache |  | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | WSE | official_partial | 542 | 0 | 3 | gpw_instrument_cards | GPW |  |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 24 | CPH | official_partial | 145 | 0 | 1 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 25 | BVL | official_partial | 33 | 1 | 2 | bvl_issuers_directory | CAVALI | cache |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 26 | PSE_CZ | official_partial | 26 | 1 | 0 | pse_cz_shares_directory | Prague Stock Exchange | cache |  | implemented via official market pages plus detail-page ticker extraction |
| 27 | SIX | official_partial | 757 | 0 | 0 | six_shares_explorer_full | SIX | network |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 28 | BCBA | official_partial | 64 | 3 | 0 | byma_equity_details | BYMA | cache |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 29 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | cache |  | implemented via official ZSE front-end API and price-sheet API |
| 30 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 31 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | cache |  | implemented via official market-snapshot table |
| 32 | DSE_TZ | official_partial | 17 | 0 | 2 | dse_tz_listed_companies | DSE Tanzania | cache |  | implemented via official listed-company table; profile pages still need a richer ISIN/sector endpoint |

## Global Expansion Candidates

_No rows._

## Completed Or Reconciled Candidates

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|
| 2 | MSE_MW | missing | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | unavailable | MSE Malawi mainboard unavailable (HTTP 403 from official host mse.co.mw) | implemented via official mainboard table and company links |
| 3 | SSE | official_partial | 2789 | 37 | 0 | sse_industry_classification | SSE |  |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 4 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 5 | ASX | official_partial | 1629 | 106 | 37 | asx_cash_market_directory | ASX |  |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 6 | ATHEX | official_partial | 155 | 5 | 0 | athex_sector_classification | ATHEX | cache |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 7 | STO | official_partial | 834 | 0 | 2 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 8 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  |  | implemented via existing official SZSE report-list industry fields |
| 9 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 10 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 11 | BME | official_partial | 221 | 0 | 0 | bme_security_prices_directory | BME | unavailable | BME security prices rows unavailable; official detail fetch failed and no complete cache (>=500 rows) is available; partial caches are ignored | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 12 | JSE | official_partial | 212 | 7 | 1 | jse_listed_companies_directory | JSE |  |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 13 | TASE | official_partial | 672 | 0 | 13 | tase_company_profiles | TASE |  |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 14 | BSE_HU | official_partial | 50 | 5 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | cache |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 15 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | cache |  | implemented via official listed-securities table |
| 16 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  |  | parser implemented; residual gaps need ticker-level review |
| 17 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | cache |  | implemented as ISIN join against current VSE listings |
| 18 | BSE_BW | official_partial | 39 | 0 | 4 | bse_bw_listed_companies | BSE Botswana | cache |  | implemented via official companies page with conservative local listing-name matching |
| 19 | BMV | official_partial | 179 | 17 | 3 | bmv_market_data_securities | BMV | network |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 20 | HEL | official_partial | 194 | 0 | 1 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 21 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 22 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | cache |  | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | WSE | official_partial | 542 | 0 | 3 | gpw_instrument_cards | GPW |  |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 24 | CPH | official_partial | 145 | 0 | 1 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 25 | BVL | official_partial | 33 | 1 | 2 | bvl_issuers_directory | CAVALI | cache |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 26 | PSE_CZ | official_partial | 26 | 1 | 0 | pse_cz_shares_directory | Prague Stock Exchange | cache |  | implemented via official market pages plus detail-page ticker extraction |
| 27 | SIX | official_partial | 757 | 0 | 0 | six_shares_explorer_full | SIX | network |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 28 | BCBA | official_partial | 64 | 3 | 0 | byma_equity_details | BYMA | cache |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 29 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | cache |  | implemented via official ZSE front-end API and price-sheet API |
| 30 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 31 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | cache |  | implemented via official market-snapshot table |

## Policy

- `sources.json` remains limited to implemented source feeds.
- `source_candidates.json` tracks official candidates that still need endpoint discovery, parsing, or venue-code reconciliation.
- Candidate rows are not authoritative exchange data until a parser writes audited `reference.csv` rows and coverage reports mark the venue as covered.
