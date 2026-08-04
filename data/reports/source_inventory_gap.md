# Source Inventory Gap

Generated at: `2026-08-04T10:29:55Z`

## Summary

- Rows: `68`
- Current-scope candidates: `67`
- Global expansion candidates: `0`
- Todo rows: `0`
- High-priority rows: `24`
- Status counts: not_in_current_universe: `1`, official_full: `34`, official_partial: `33`
- Scope counts: exchange_directory_candidate: `8`, global_expansion_candidate: `13`, listed_companies_candidate: `17`, normalization_alias: `1`, security_identifier_registry_candidate: `2`, security_lookup_subset: `1`, source_expansion_candidate: `26`
- Official-full upgrade plan counts: {"add_or_replace_with_active_exchange_directory_before_recall_claim": 3, "complete_official_exchange_directory_present": 34, "expand_subset_to_active_exchange_directory_or_document_scope_exception": 30}

## Missing Current-Scope Sources

_No rows._

## Partial Current-Scope Sources

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Upgrade Plan | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|---|
| 1 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 2 | ASX | official_partial | 1708 | 182 | 4 | asx_cash_market_directory | ASX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 3 | ATHEX | official_partial | 155 | 0 | 0 | athex_sector_classification | ATHEX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 4 | STO | official_partial | 843 | 0 | 3 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 5 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SZSE report-list industry fields |
| 6 | SSE | official_partial | 2789 | 35 | 0 | sse_industry_classification | SSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 7 | TPEX | official_partial | 1119 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 9 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 10 | BMV | official_partial | 179 | 17 | 1 | bmv_market_data_securities | BMV | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 11 | JSE | official_partial | 212 | 0 | 0 | jse_listed_companies_directory | JSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 12 | TASE | official_partial | 695 | 0 | 34 | tase_company_profiles | TASE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 13 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-securities table |
| 14 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | parser implemented; residual gaps need ticker-level review |
| 15 | BSE_HU | official_partial | 50 | 0 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 16 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented as ISIN join against current VSE listings |
| 17 | BSE_BW | official_partial | 39 | 0 | 3 | bse_bw_listed_companies | BSE Botswana | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | Empty refresh result; preserved 26 existing rows | implemented via official companies page with conservative local listing-name matching |
| 18 | HEL | official_partial | 196 | 0 | 0 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 19 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | Empty refresh result; preserved 3 existing rows | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 20 | BVL | official_partial | 33 | 0 | 0 | bvl_issuers_directory | CAVALI | network | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 21 | PSE_CZ | official_partial | 27 | 0 | 1 | pse_cz_shares_directory | Prague Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market pages plus detail-page ticker extraction |
| 22 | SIX | official_partial | 759 | 0 | 1 | six_shares_explorer_full | SIX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 23 | WSE | official_partial | 542 | 0 | 1 | gpw_instrument_cards | GPW |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 24 | CPH | official_partial | 147 | 0 | 1 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 25 | BCBA | official_partial | 63 | 0 | 0 | byma_equity_details | BYMA | network | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 26 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official ZSE front-end API and price-sheet API |
| 27 | MSE_MW | official_partial | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official mainboard listed-company table |
| 28 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 29 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market-snapshot table |
| 30 | NMFQS | official_partial | 6 | 0 | 0 | nasdaq_mutual_fund_quotes | Nasdaq | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Nasdaq Fund Network quote API for current NMFQS symbols |

## Global Expansion Candidates

_No rows._

## Completed Or Reconciled Candidates

| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Source Mode | Upgrade Plan | Last Error | Blocker |
|---|---|---|---:|---:|---:|---|---|---|---|---|---|
| 1 | EGX | official_partial | 223 | 0 | 1 | egx_listed_securities | EGX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via browser-captured official ASP.NET ViewState; raw non-browser requests still hit the EGX/TSPD challenge |
| 2 | ASX | official_partial | 1708 | 182 | 4 | asx_cash_market_directory | ASX |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by mapping the existing official ASX listed-companies CSV GICS industry-group column to canonical stock_sector values |
| 3 | ATHEX | official_partial | 155 | 0 | 0 | athex_sector_classification | ATHEX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | active stock-market pages are Incapsula-blocked from this environment; implemented reachable official sector-classification PDF as a conservative listed-company subset |
| 4 | STO | official_partial | 843 | 0 | 3 | nasdaq_nordic_stockholm_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Stockholm shares, share-search, ETF, tracker, Spotlight, and NGM feeds |
| 5 | SZSE | official_partial | 3083 | 12 | 0 | szse_industry_classification | SZSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SZSE report-list industry fields |
| 6 | SSE | official_partial | 2789 | 35 | 0 | sse_industry_classification | SSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via existing official SSE stock-list CSRC_CODE fields |
| 7 | TPEX | official_partial | 1119 | 0 | 0 | stockanalysis_tpex_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source because official TPEX/MOPS feeds identify the KY issuers but do not expose their foreign ISINs |
| 8 | Bursa | official_partial | 936 | 0 | 0 | bursa_equities_prices_directory | Bursa Malaysia |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official Bursa year-end closing-price PDF captured through browser download; live equities-prices API still Cloudflare-blocked for repeatable direct refreshes |
| 9 | HOSE | official_partial | 153 | 0 | 0 | stockanalysis_hose_company_profiles | StockAnalysis |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | resolved by scripts/backfill_stockanalysis_metadata.py as a reviewed secondary company-profile source after official HOSE/VSDC feeds did not expose the single LCG stock-sector residual |
| 10 | BMV | official_partial | 179 | 17 | 1 | bmv_market_data_securities | BMV | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official BMV issuer market-data/profile pages; some local trust/equity rows still omit ISIN in the reachable BMV instrument table |
| 11 | JSE | official_partial | 212 | 0 | 0 | jse_listed_companies_directory | JSE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official JSE instrument profile parser to read Sector/Industry fields; reachable instrument pages still do not expose ISIN values, so residual ISIN gaps need a separate official registry or reviewed issuer filings |
| 12 | TASE | official_partial | 695 | 0 | 34 | tase_company_profiles | TASE |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | stock-sector taxonomy still not exposed by the reachable marketdata endpoint |
| 13 | ZSE | official_partial | 23 | 0 | 0 | zagreb_securities_directory | ZSE Croatia | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-securities table |
| 14 | ICE_IS | official_partial | 18 | 0 | 0 | nasdaq_iceland_shares | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | parser implemented; residual gaps need ticker-level review |
| 15 | BSE_HU | official_partial | 50 | 0 | 3 | bse_hu_listed_companies | Budapest Stock Exchange | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official embedded market-data feed; residual local shortcut tickers need explicit symbol alias review |
| 16 | VSE | official_partial | 56 | 0 | 0 | vienna_listed_companies | Wiener Boerse | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented as ISIN join against current VSE listings |
| 17 | BSE_BW | official_partial | 39 | 0 | 3 | bse_bw_listed_companies | BSE Botswana | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | Empty refresh result; preserved 26 existing rows | implemented via official companies page with conservative local listing-name matching |
| 18 | HEL | official_partial | 196 | 0 | 0 | nasdaq_nordic_helsinki_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Helsinki shares, share-search, and ETF feeds |
| 19 | BVC | official_partial | 3 | 0 | 0 | bvc_colombia_issuers | BVC | unavailable | expand_subset_to_active_exchange_directory_or_document_scope_exception | Empty refresh result; preserved 3 existing rows | implemented via official BVC local-equity issuer API using the site handshake token; rows only enter through reviewed current BVC listings |
| 20 | BVL | official_partial | 33 | 0 | 0 | bvl_issuers_directory | CAVALI | network | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official CAVALI issuer securities registry; BVL Angular issuer page still needs endpoint discovery for a pure exchange directory |
| 21 | PSE_CZ | official_partial | 27 | 0 | 1 | pse_cz_shares_directory | Prague Stock Exchange | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market pages plus detail-page ticker extraction |
| 22 | SIX | official_partial | 759 | 0 | 1 | six_shares_explorer_full | SIX | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via the official SIX FQS ref.json detail endpoint; residual gaps are now data-level taxonomy mapping only |
| 23 | WSE | official_partial | 542 | 0 | 1 | gpw_instrument_cards | GPW |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented by extending existing official GPW/NewConnect list parsers to read the sector label from result rows; residual gaps are mostly unclassified official labels or ETF category tail |
| 24 | CPH | official_partial | 147 | 0 | 1 | nasdaq_nordic_copenhagen_full_search | Nasdaq Nordic |  | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | reconciled through existing official Nasdaq Nordic Copenhagen shares, share-search, ETF, and ETF-search feeds |
| 25 | BCBA | official_partial | 63 | 0 | 0 | byma_equity_details | BYMA | network | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Open BYMADATA equity-detail endpoint; some legacy BCBA symbols remain unmatched without manual ticker normalization |
| 26 | ZSE_ZW | official_partial | 27 | 0 | 0 | zse_zw_listed_companies | ZSE Zimbabwe | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official ZSE front-end API and price-sheet API |
| 27 | MSE_MW | official_partial | 8 | 0 | 0 | mse_mw_listed_companies | MSE Malawi | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official mainboard listed-company table |
| 28 | LUSE | official_partial | 22 | 0 | 0 | luse_listed_companies | LuSE | cache | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official listed-company page captured through the reader fallback because direct requests hit a Cloudflare challenge |
| 29 | USE_UG | official_partial | 7 | 0 | 0 | use_ug_listed_companies | USE Uganda | network | expand_subset_to_active_exchange_directory_or_document_scope_exception |  | implemented via official market-snapshot table |
| 30 | NMFQS | official_partial | 6 | 0 | 0 | nasdaq_mutual_fund_quotes | Nasdaq | cache | add_or_replace_with_active_exchange_directory_before_recall_claim |  | implemented via official Nasdaq Fund Network quote API for current NMFQS symbols |

## Policy

- `sources.json` remains limited to implemented source feeds.
- `source_candidates.json` tracks official candidates that still need endpoint discovery, parsing, or venue-code reconciliation.
- Candidate rows are not authoritative exchange data until a parser writes audited `reference.csv` rows and coverage reports mark the venue as covered.
