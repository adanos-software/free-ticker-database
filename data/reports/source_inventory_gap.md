# Source Inventory Gap

Generated at: `2026-07-07T17:25:57Z`

## Summary

- Rows: `68`
- Current-scope candidates: `67`
- Global expansion candidates: `0`
- Todo rows: `0`
- High-priority rows: `24`
- Status counts: not_in_current_universe: `1`, official_full: `33`, official_partial: `34`
- Scope counts: exchange_directory_candidate: `8`, global_expansion_candidate: `13`, listed_companies_candidate: `17`, normalization_alias: `1`, security_identifier_registry_candidate: `2`, security_lookup_subset: `1`, source_expansion_candidate: `26`
- Official-full upgrade plan counts: {"add_or_replace_with_active_exchange_directory_before_recall_claim": 3, "complete_official_exchange_directory_present": 33, "expand_subset_to_active_exchange_directory_or_document_scope_exception": 31}

## Missing Current-Scope Sources

_No rows._

## Partial Current-Scope Sources

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Upgrade Plan | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|---|
| 1 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 2 | ASX | official_partial | 1626 | 100 | 3 | asx_cash_market_directory | ASX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 3 | ATHEX | official_partial | 155 | 5 | 0 | athex_sector_classification | ATHEX | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 4 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SZSE report-list industry fields |
| 5 | SSE | official_partial | 2789 | 37 | 0 | sse_industry_classification | SSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 6 | STO | official_partial | 834 | 0 | 1 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 7 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 9 | SIX | official_partial | 757 | 0 | 0 | six_shares_explorer_full | SIX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 10 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 11 | BMV | official_partial | 179 | 17 | 1 | bmv_market_data_securities | BMV | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 12 | CPH | official_partial | 145 | 0 | 0 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 13 | BME | official_partial | 221 | 0 | 0 | bme_security_prices_directory | BME | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | BME security prices rows unavailable; official detail fetch failed and no complete cache (>=500 rows) is available; partial caches are ignored | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 14 | JSE | official_partial | 212 | 7 | 0 | jse_listed_companies_directory | JSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 15 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-securities table |
| 16 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | parser implemented; residual gaps need ticker-level review |
| 17 | BSE_HU | official_partial | 50 | 5 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 18 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented as ISIN join against current VSE listings |
| 19 | BSE_BW | official_partial | 39 | 0 | 3 | bse_bw_listed_companies | BSE Botswana | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official companies page with conservative local listing-name matching |
| 20 | TASE | official_partial | 672 | 0 | 11 | tase_company_profiles | TASE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 21 | HEL | official_partial | 194 | 0 | 0 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 22 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | BVL | official_partial | 33 | 1 | 0 | bvl_issuers_directory | CAVALI | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 24 | PSE_CZ | official_partial | 26 | 1 | 0 | pse_cz_shares_directory | Prague Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market pages plus detail-page ticker extraction |
| 25 | WSE | official_partial | 542 | 0 | 1 | gpw_instrument_cards | GPW |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 26 | BCBA | official_partial | 64 | 3 | 0 | byma_equity_details | BYMA | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 27 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official ZSE front-end API and price-sheet API |
| 28 | MSE_MW | official_partial | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official mainboard listed-company table |
| 29 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 30 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market-snapshot table |

## Global Expansion Candidates

_No rows._

## Completed Or Reconciled Candidates

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Upgrade Plan | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|---|
| 1 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 2 | ASX | official_partial | 1626 | 100 | 3 | asx_cash_market_directory | ASX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 3 | ATHEX | official_partial | 155 | 5 | 0 | athex_sector_classification | ATHEX | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 4 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SZSE report-list industry fields |
| 5 | SSE | official_partial | 2789 | 37 | 0 | sse_industry_classification | SSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 6 | STO | official_partial | 834 | 0 | 1 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 7 | TPEX | official_partial | 1118 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 9 | SIX | official_partial | 757 | 0 | 0 | six_shares_explorer_full | SIX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 10 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 11 | BMV | official_partial | 179 | 17 | 1 | bmv_market_data_securities | BMV | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 12 | CPH | official_partial | 145 | 0 | 0 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 13 | BME | official_partial | 221 | 0 | 0 | bme_security_prices_directory | BME | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | BME security prices rows unavailable; official detail fetch failed and no complete cache (>=500 rows) is available; partial caches are ignored | parser implemented via official BME ListedCompanies API with SIBE, Floor, Latibex, MTF, and ETF trading-system parameters; live refresh is currently host-blocked/403 from this environment, and partial caches are ignored |
| 14 | JSE | official_partial | 212 | 7 | 0 | jse_listed_companies_directory | JSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 15 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-securities table |
| 16 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | parser implemented; residual gaps need ticker-level review |
| 17 | BSE_HU | official_partial | 50 | 5 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 18 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented as ISIN join against current VSE listings |
| 19 | BSE_BW | official_partial | 39 | 0 | 3 | bse_bw_listed_companies | BSE Botswana | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official companies page with conservative local listing-name matching |
| 20 | TASE | official_partial | 672 | 0 | 11 | tase_company_profiles | TASE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 21 | HEL | official_partial | 194 | 0 | 0 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 22 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 23 | BVL | official_partial | 33 | 1 | 0 | bvl_issuers_directory | CAVALI | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 24 | PSE_CZ | official_partial | 26 | 1 | 0 | pse_cz_shares_directory | Prague Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market pages plus detail-page ticker extraction |
| 25 | WSE | official_partial | 542 | 0 | 1 | gpw_instrument_cards | GPW |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 26 | BCBA | official_partial | 64 | 3 | 0 | byma_equity_details | BYMA | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 27 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official ZSE front-end API and price-sheet API |
| 28 | MSE_MW | official_partial | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official mainboard listed-company table |
| 29 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 30 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market-snapshot table |

## Policy

- `sources.json` remains limited to implemented source feeds.
- `source_candidates.json` tracks official candidates that still need endpoint discovery, parsing, or venue-code reconciliation.
- Candidate rows are not authoritative exchange data until a parser writes audited `reference.csv` rows and coverage reports mark the venue as covered.
