# Source Inventory Gap

Generated at: `2026-04-22T09:47:02Z`

## Summary

- Rows: `65`
- Current-scope candidates: `64`
- Global expansion candidates: `0`
- Todo rows: `0`
- High-priority rows: `22`
- Status counts: not_in_current_universe: `1`, official_full: `30`, official_partial: `34`
- Scope counts: exchange_directory_candidate: `7`, global_expansion_candidate: `13`, listed_companies_candidate: `17`, normalization_alias: `1`, security_identifier_registry_candidate: `2`, security_lookup_subset: `1`, source_expansion_candidate: `24`

## Missing Current-Scope Sources

_No rows._

## Partial Current-Scope Sources

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Blocker |
|---|---|---|---:|---:|---:|---|---|---|
| 1 | STO | official_partial | 725 | 0 | 237 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 2 | EGX | official_partial | 225 | 0 | 29 | egx_listed_securities | EGX | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 3 | ASX | official_partial | 1298 | 105 | 185 | asx_cash_market_directory | ASX | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 4 | ATHEX | official_partial | 117 | 19 | 0 | athex_sector_classification | ATHEX | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 5 | SZSE | official_partial | 3083 | 487 | 337 | szse_industry_classification | SZSE | implemented via existing official SZSE report-list industry fields |
| 6 | SSE | official_partial | 2789 | 614 | 479 | sse_industry_classification | SSE | implemented via existing official SSE stock-list CSRC_CODE fields |
| 7 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | SIX | official_partial | 743 | 0 | 0 | six_shares_explorer_full | SIX | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 9 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 10 | BMV | official_partial | 179 | 19 | 5 | bmv_market_data_securities | BMV | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 11 | BME | official_partial | 169 | 0 | 11 | bme_security_prices_directory | BME | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 12 | TASE | official_partial | 673 | 0 | 86 | tase_company_profiles | TASE | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 13 | JSE | official_partial | 212 | 29 | 5 | jse_listed_companies_directory | JSE | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 14 | WSE | official_partial | 348 | 0 | 35 | gpw_instrument_cards | GPW | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 15 | ZSE | official_partial | 23 | 0 | 22 | zagreb_securities_directory | ZSE Croatia | implemented via official listed-securities table |
| 16 | CPH | official_partial | 131 | 0 | 19 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 17 | BSE_HU | official_partial | 31 | 8 | 19 | bse_hu_listed_companies | Budapest Stock Exchange | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 18 | ICE_IS | official_partial | 18 | 0 | 15 | nasdaq_iceland_shares | Nasdaq Nordic | parser implemented; residual gaps need ticker-level review |
| 19 | VSE | official_partial | 36 | 2 | 4 | vienna_listed_companies | Wiener Boerse | implemented as ISIN join against current VSE listings |
| 20 | BSE_BW | official_partial | 39 | 0 | 11 | bse_bw_listed_companies | BSE Botswana | implemented via official companies page with conservative local listing-name matching |
| 21 | HEL | official_partial | 188 | 0 | 47 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 22 | BVC | official_partial | 3 | 3 | 0 | bvc_colombia_issuers | BVC | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | Bursa | official_partial | 936 | 0 | 2 | bursa_equities_prices_directory | Bursa Malaysia | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 24 | BVL | official_partial | 33 | 2 | 30 | bvl_issuers_directory | CAVALI | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 25 | PSE_CZ | official_partial | 24 | 1 | 14 | pse_cz_shares_directory | Prague Stock Exchange | implemented via official market pages plus detail-page ticker extraction |
| 26 | BCBA | official_partial | 64 | 3 | 14 | byma_equity_details | BYMA | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 27 | ZSE_ZW | official_partial | 27 | 0 | 23 | zse_zw_listed_companies | ZSE Zimbabwe | implemented via official ZSE front-end API and price-sheet API |
| 28 | MSE_MW | official_partial | 8 | 0 | 8 | mse_mw_listed_companies | MSE Malawi | implemented via official mainboard table and company links |
| 29 | LUSE | official_partial | 22 | 0 | 20 | luse_listed_companies | LuSE | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 30 | USE_UG | official_partial | 7 | 0 | 7 | use_ug_listed_companies | USE Uganda | implemented via official market-snapshot table |

## Global Expansion Candidates

_No rows._

## Completed Or Reconciled Candidates

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Blocker |
|---|---|---|---:|---:|---:|---|---|---|
| 1 | STO | official_partial | 725 | 0 | 237 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 2 | EGX | official_partial | 225 | 0 | 29 | egx_listed_securities | EGX | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 3 | ASX | official_partial | 1298 | 105 | 185 | asx_cash_market_directory | ASX | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 4 | ATHEX | official_partial | 117 | 19 | 0 | athex_sector_classification | ATHEX | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 5 | SZSE | official_partial | 3083 | 487 | 337 | szse_industry_classification | SZSE | implemented via existing official SZSE report-list industry fields |
| 6 | SSE | official_partial | 2789 | 614 | 479 | sse_industry_classification | SSE | implemented via existing official SSE stock-list CSRC_CODE fields |
| 7 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | SIX | official_partial | 743 | 0 | 0 | six_shares_explorer_full | SIX | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 9 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 10 | BMV | official_partial | 179 | 19 | 5 | bmv_market_data_securities | BMV | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 11 | BME | official_partial | 169 | 0 | 11 | bme_security_prices_directory | BME | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 12 | TASE | official_partial | 673 | 0 | 86 | tase_company_profiles | TASE | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 13 | JSE | official_partial | 212 | 29 | 5 | jse_listed_companies_directory | JSE | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 14 | WSE | official_partial | 348 | 0 | 35 | gpw_instrument_cards | GPW | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 15 | ZSE | official_partial | 23 | 0 | 22 | zagreb_securities_directory | ZSE Croatia | implemented via official listed-securities table |
| 16 | CPH | official_partial | 131 | 0 | 19 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 17 | BSE_HU | official_partial | 31 | 8 | 19 | bse_hu_listed_companies | Budapest Stock Exchange | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 18 | ICE_IS | official_partial | 18 | 0 | 15 | nasdaq_iceland_shares | Nasdaq Nordic | parser implemented; residual gaps need ticker-level review |
| 19 | VSE | official_partial | 36 | 2 | 4 | vienna_listed_companies | Wiener Boerse | implemented as ISIN join against current VSE listings |
| 20 | BSE_BW | official_partial | 39 | 0 | 11 | bse_bw_listed_companies | BSE Botswana | implemented via official companies page with conservative local listing-name matching |
| 21 | HEL | official_partial | 188 | 0 | 47 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 22 | BVC | official_partial | 3 | 3 | 0 | bvc_colombia_issuers | BVC | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | Bursa | official_partial | 936 | 0 | 2 | bursa_equities_prices_directory | Bursa Malaysia | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 24 | BVL | official_partial | 33 | 2 | 30 | bvl_issuers_directory | CAVALI | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 25 | PSE_CZ | official_partial | 24 | 1 | 14 | pse_cz_shares_directory | Prague Stock Exchange | implemented via official market pages plus detail-page ticker extraction |
| 26 | BCBA | official_partial | 64 | 3 | 14 | byma_equity_details | BYMA | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 27 | ZSE_ZW | official_partial | 27 | 0 | 23 | zse_zw_listed_companies | ZSE Zimbabwe | implemented via official ZSE front-end API and price-sheet API |
| 28 | MSE_MW | official_partial | 8 | 0 | 8 | mse_mw_listed_companies | MSE Malawi | implemented via official mainboard table and company links |
| 29 | LUSE | official_partial | 22 | 0 | 20 | luse_listed_companies | LuSE | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 30 | USE_UG | official_partial | 7 | 0 | 7 | use_ug_listed_companies | USE Uganda | implemented via official market-snapshot table |

## Policy

- `sources.json` remains limited to implemented source feeds.
- `source_candidates.json` tracks official candidates that still need endpoint discovery, parsing, or venue-code reconciliation.
- Candidate rows are not authoritative exchange data until a parser writes audited `reference.csv` rows and coverage reports mark the venue as covered.
