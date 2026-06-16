# Changelog

## [Unreleased]

## [3.30.13] - 2026-06-16

### Summary

Data-integrity release. A full cross-check of our ISIN↔name mappings against divvydiary's sitemap universe (97,613 ISIN→name pairs), verified with OpenFIGI (authoritative ISIN→issuer) and a multi-agent web-research pass, fixes 233 identity discrepancies — including a systematic source-join bug that the existing collision gate did not cover.

### Changed

- Cross-checked all ~46.7k covered ISINs against divvydiary; 90.9% already agreed. Fixed the verified residual:
  - **192 wrong-ISIN collisions** (stored ISIN belonged to a different issuer; OpenFIGI + divvydiary agree): **91** replaced with the OpenFIGI-confirmed correct ISIN (e.g. `EOGSF` Emerald Resources → `AU000000EMR4`, `RGR` Sturm Ruger → `US8641591081`, `RCL` Royal Caribbean → `LR0008862868`), **101** cleared where no correct ISIN was confidently found (never fabricated).
  - Surfaced a systematic source-join bug: Weyerhaeuser's `US9621661043` was attached to **13 unrelated foreign OTC tickers** (Capital A, CIMB, Genting, Tenaga, …). These are OTC rows, which the `us_foreign_isin` gate (US-primary only) does not cover.
  - **40 stale company names** updated to current rebrands (web-confirmed): `Cassava Sciences→Filana Therapeutics`, `Montrose Environmental→Onterris`, `Mainz Biomed→Quantum Cyber`, `NL Industries→NLI Holdings`, `ASGN→Everforth`, `Sayona Mining→Elevra Lithium`, `Hypermarcas→Hypera`.
  - +1 further verified wrong-ISIN clear (`TBLU` held Tortoise North American Pipeline's ISIN).
- Clearing the wrongly-*shared* ISINs correctly un-merged 93 companies previously conflated into one global-ticker row by the bad join: primary tickers 61,554 → 61,647; full listing rows unchanged at 71,035.
- Bumps VERSION to 3.30.13 and refreshes data `_meta`, reports, README, and CHANGELOG.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates.
- `pytest tests/ -q`: 1,451 passed.

## [3.30.12] - 2026-06-16

### Summary

Anti-drift release. Adds a weekly freshness/drift workflow and acts on the drift it surfaces: applies verified ticker renames from the symbol-change feed.

### Added

- `scripts/build_drift_report.py` + `.github/workflows/freshness.yml`: a weekly check that reports dataset staleness, feed-detected renames not yet applied, and release-gate quality indicators — opening a review PR only when drift is detected. Complements the existing daily symbol-changes feed.

### Changed

- Applied 32 verified ticker renames (re-keyed across listings/listing_index/identifiers_extended, names updated): e.g. `USEG→BSIN`, `VSCO→VSXY`, `KFS→KWY`, `EDAP→FOCL`, `NBY→SDEV`, `IINN→QTEX`, `TSE→TSEOF`. Each was confirmed by multi-agent verification as a same-company ticker change; 11 feed entries were rejected as de-SPAC combinations / bankruptcy delistings (not renames), and 8 messy de-SPAC results were excluded for manual handling.
- Dropped 3 stale-duplicate rows where the new ticker already existed with the same ISIN (`RYI→RYZ`, `PX→RPC`, `AXL→DCH`). Allowlisted 3 foreign-incorporated US-listed renames (QTEX/NEXR/TSEOF).
- Primary tickers 61,555 → 61,554; full listing rows 71,038 → 71,035.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates.
- `pytest tests/ -q`: 1,450 passed.

## [3.30.11] - 2026-06-16

### Summary

Completeness + enrichment release. Closes the sector-coverage gap (the weakest axis), enriches LEI coverage from the official GLEIF mapping, and cleans up the residual non-sector errors from the post-campaign correctness re-audit.

### Changed

- **Sector completeness**: filled 2,462 missing `stock_sector` values across source-uncovered markets (US OTC, B3, frontier — Sri Lanka/Casablanca/Mauritius/Philippines/Thailand/Kenya — plus Euronext/LSE/XETRA/OSL/STO residual), each determined from the issuer's actual business via the multi-agent pipeline (accuracy sample 40: 36 correct + 4 defensible, 0 wrong). Plus 13 verified sector corrections in markets that aggregator sources don't cover (Japan TSE, Korea KOSDAQ, Taiwan TWSE, OTC). **stock_sector coverage 94.5% → 99.9%** (missing 2,515 → 53); sector/category coverage → 99.8%. 53 undeterminable shells left empty rather than guessed.
- **LEI enrichment**: joined the free official GLEIF ISIN-to-LEI relationship file → LEI coverage 919 → 17,490 (~19×); listings with any identifier 65,229 → 65,497.
- **Residual cleanup**: 3 stale-name fixes (Micronet→Jeen Technologies, Banganga Paper→Asgard Alcobev, Gansu Qilianshan→CCCC Design & Consulting), PHAG/LSE etf_category Equity→Commodity, and dropped WSE/ASX (an RMBS securitisation trust mislabeled as an ETF). Primary tickers 61,556 → 61,555.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,555 ticker rows, and 71,038 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.10] - 2026-06-16

### Summary

Completes the sector-correctness campaign. Verified the final 36 expansion batches (rows 4561-4915), so all 492/492 single-source expansion candidates (4,915) have now been independently agent-checked. Ships the last **258 verified `stock_sector` corrections**, bringing the campaign total to **3,603 verified sector corrections** (PR #62/#63/#64/#65) against the dominant error class from the correctness audit.

### Changed

- 258 GICS stock-sector corrections (189 source-confirmed + 68 where the verifier found a third, truer sector). The verifier rejected ~26% of this final batch where the stored value was actually right.
- Pure sector-value changes: no row-count change (primary tickers 61,556; stocks 45,898).

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,556 ticker rows, and 71,039 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.9] - 2026-06-16

### Summary

Sector-correctness release. Following the measured correctness audit (which put row-level correctness at 91.16% with sector/category as the dominant error class), this ships **3,345 verified `stock_sector` corrections** across three merged waves. Every correction was candidate-proposed by a market-data source (stockanalysis and/or TradingView) and then **independently confirmed by an agent against the issuer's actual business** — only confirmed fixes were applied.

### Changed

- 3,345 GICS stock-sector corrections (PR #62: 726 two-source-agreed across 7 markets; PR #63: 618; PR #64: 2,001 across ~25 TradingView markets + OTC). Catch-all `Industrials`/`Financials` mislabels corrected to specific sectors (gold/lithium miners → Materials, software/IoT → Information Technology, homebuilders/auto/appliances → Consumer Discretionary, pharma/CRO/medtech → Health Care, oil/gas/coal/uranium → Energy, REITs/property → Real Estate, sugar/agri/personal-care → Consumer Staples).
- Pure sector-value changes: no row-count change (primary tickers 61,556; stocks 45,898).

### Safety

- The verification step REJECTED a large share of single-source candidates where the stored value was actually right (~37–45% on the expansion set, e.g. fintech mislabeled IT that is really Financials), preventing thousands of regressions. Corrections were verified per row, never bulk-applied from a single feed.
- A small remainder (~36 batches / rows 4561-4915 of the expansion set, ~150 candidates) is durably checkpointed for a follow-up.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,556 ticker rows, and 71,039 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.8] - 2026-06-15

### Summary

Correctness release driven by a measured audit. A multi-agent statistical correctness audit (400-row reproducible sample; every row verified against ≥2 independent authoritative sources and every flagged error adversarially re-checked) put row-level correctness at **91.16% (Wilson-95% CI [87.96%, 93.58%])**, with identity fields (name/ISIN/country/asset_type) ~97% correct and sector/category classification as the dominant error class. This release fixes the 35 confirmed errors from that sample.

### Changed

- Corrected 23 sector/category misclassifications confirmed against the issuer's actual business + OpenFIGI/justETF: GICS `stock_sector` fixes (e.g. `GGM`/`WIA` gold → Materials, `UNO-H` uranium → Energy, `HNGE` → Health Care, `VVV` → Consumer Discretionary) and ETF `etf_category` fixes (`USCR`/`KNRG` bond ETFs → Fixed Income, `GLTR` → Commodity, `310970` → Equity, `00711B` → Fixed Income). `EGSE`'s clearly-wrong sector cleared.
- Fixed 5 stale issuer names (`000782` Highsun, `3131` Grand Process Technology, `OLGERD` Bera hf, `232140` YC Corporation, `WEBI` Amundi Core MSCI USA).
- Fixed identity errors: `SORT` ISIN cleared + domicile GB→US; `EGSE` ISIN → `US2999331018`; domicile fixes `AMIN` ID→US, `FHI` CA→US, `EVOK` JE→GI (matches its GI ISIN; adds Gibraltar as a country).
- Dropped 2 SPAC warrants mislabeled `asset_type=Stock` (`ATCHW`, `CELUW`) and pruned their keys from `identifiers_extended.csv` + `listing_index.csv`.
- Snapshot: primary tickers 61,558 → 61,556; stocks → 45,898; ISIN coverage 60,087 (97.6%); countries 86 → 87.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,556 ticker rows, and 71,039 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.7] - 2026-06-15

### Summary

ISIN-tail release sourced from a national CSD. Closed 89 of the 90 Muscat Securities Market (Oman) ISIN gaps using the authoritative numbering agency — Muscat Clearing & Depository (MCD) — rather than a third-party aggregator. No values were fabricated and nothing was applied on a single unverified source.

### Changed

- Backfilled 89 MSX (Oman) ISINs from MCD's `ListedInstrumentsInfo` instrument register, matched to our gaps by exact security symbol with issuer-name consistency confirmed for every row. The 6 abbreviation-heavy names were additionally cross-checked against OpenFIGI ISIN→issuer (exact ticker + name match). All are clean fills (new ISINs, OM prefix, valid ISO checksum, no collapse).
- Added Euronext Amsterdam (`AMS`) handling to the EODHD backfill maps in v3.30.6 is now joined by the CSD-sourcing pattern; `scripts/backfill_eodhd_metadata.py` remains the EODHD path.
- Public snapshot: ISIN coverage 60,001 → 60,090 (97.6%); core primary rows missing ISIN 854 → 765; MSX core gaps 90 → 1. Primary ticker count unchanged at 61,558.

### Safety

- MCD is Oman's authoritative CSD/numbering agency (the ISIN issuer), so the symbol+name match is a primary-source confirmation, not an aggregator guess.
- Rejected `BWRQ` (MCD lists it as a BOND on the Third Market; our row is a Stock) to avoid attaching a debt ISIN to an equity line.
- The remaining tail (Qatar, Pakistan, Chile, …) was investigated but is not freely accessible: Qatar's QSE renders ISINs inconsistently, Pakistan's CDC is behind bot protection, Chile's exchange API is auth-gated. Those need a credentialed source (e.g. an EODHD Fundamentals-tier subscription) and were not fabricated.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,558 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.6] - 2026-06-15

### Summary

ISIN-tail release. Closed the two ISINs that the EODHD pass could still reach on the unsupported-exchange tail, and resolved the row deferred in v3.30.5. The deep tail (frontier/emerging exchanges with no ISIN in EODHD's bulk list on this plan) remains open and is documented as needing a Fundamentals-tier subscription or per-exchange official feeds. No values were fabricated and nothing was applied on a single source alone.

### Changed

- `IGBG` (Euronext Amsterdam) → `IE000J8Z5N74` (iShares Broad Global Govt Bond UCITS ETF): the one residual-tail row EODHD's bulk symbol list could fill, confirmed against the iShares/justETF/Cbonds public ETF profile. Euronext Amsterdam (`AMS`) was added to the EODHD backfill maps so the venue is covered going forward.
- `CURN` (OTC) → `US23131B3078`: resolves the row deferred last release. EODHD's previously-proposed `US2312921032` is a non-existent identifier (OpenFIGI returns no match); the correct ISIN `US23131B3078` maps via OpenFIGI to Currency Exchange International (tickers `CURN`/`CXI`/`CXIUSD`). It is the same security as the `CXI`/TSX primary, so applying it links the OTC line as a cross-listing.
- Public snapshot: ISIN coverage 60,000 → 60,001; core primary rows missing ISIN 855 → 854; primary tickers 61,559 → 61,558 (CURN collapses into CXI's cross-listing group — collision-safe deduplication, not a coverage loss).

### Safety

- Each ISIN was confirmed by an independent second source before inclusion (EODHD + iShares/justETF/Cbonds for IGBG; public profile + OpenFIGI ISIN→issuer for CURN).
- The deep tail (`MSX`/Oman, `QSE`/Qatar, `PSX`, `SSE_CL`, `JSE`, `ATHEX`, …) is not closeable on the current EODHD plan: its bulk `exchange-symbol-list` carries no ISIN for these markets and the `fundamentals` endpoint (which does) is not entitled (HTTP 403). OpenFIGI returns FIGIs, not ISINs, so it cannot fill the gap. No values were fabricated.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,558 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.5] - 2026-06-15

### Summary

ISIN-coverage release. Filled 47 missing ISINs on rows that the earlier ticker-collision sweeps had deliberately *cleared* (a foreign same-ticker namesake's ISIN was removed and the row left blank, pending a sourced correct ISIN). The values come from EODHD's `exchange-symbol-list` and were applied only after a per-row collision-safety check and confirmation by an independent second source. No values were fabricated and nothing was applied on a single source alone.

### Changed

- Backfilled 47 ISINs from EODHD onto previously-cleared collision rows, restoring each issuer's genuine home-domicile ISIN (e.g. Australian/Canadian/Swedish/Thai namesake ISINs replaced by the correct US ISIN; Cayman issuers `TOP`/`BHAT`/`WXM` → KY ISINs; Israeli issuers `BMR`/`WLDS` → IL ISINs). ISIN coverage 59,953 → 60,000 (97.5%); core primary rows missing ISIN 885 → 855; core primary rows with ISIN 53,210 → 53,240; entry-quality source-gap rows 7,288 → 7,261. Primary ticker count unchanged at 61,559.
- Allowlisted `BMR` (Beamr Imaging, Israel) and `WLDS` (Wearable Devices, Israel) in `foreign_isin_reviewed.csv`: legitimate foreign-incorporated NASDAQ issuers carrying their home IL ISINs (reviewed `us_foreign_isin` rows).
- `BMR`'s test invariant updated to its verified Israeli ISIN `IL0011832438`; the collision-cleanup assertions (country Israel, no Ballymore Resources aliases) are retained.

### Safety

- Each ISIN had to (1) pass the strict EODHD gate (ticker + EODHD subvenue + asset type + expected ISIN prefix + strict issuer/product name + numeric tokens + checksum), (2) differ from the foreign-namesake ISIN the row was previously cleared of, and (3) be confirmed by an independent second source — OpenFIGI ISIN→issuer (44/48) or, for the few absent from OpenFIGI, the web (`WXM`/`BMR` exact ISIN via TradingView/Nasdaq/MarketScreener/cbonds).
- Rejected `RNA` (NASDAQ): EODHD still returns the stale Avidity Biosciences ISIN `US05370A1088` after the ticker was reassigned to Atrium Therapeutics — exactly the collision the strict name-match cannot catch. Deferred `CURN` (OTC): Canadian issuer with an unconfirmed US ISIN.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,559 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.4] - 2026-06-14

### Summary

Identity-correctness release. Completed the deferred per-fund verification of the OTC "collapse" candidates: 64 OTC grey-market shadow listings were independently verified (Yahoo cross-check of the StockAnalysis-mapped ISIN) and linked to their primary securities as cross-listings. No values were fabricated and no candidate was applied on a single source alone.

### Changed

- Linked 64 per-fund-verified OTC shadows to their primaries as cross-listings by applying their ISINs: each was confirmed by an independent second source (Yahoo) returning the exact ISIN or a specific fund name matching the ISIN holder beyond the issuer (brand + specific-fund token match). Two same-company rescues applied on corporate-fact grounds (`SOTDF` Ströer=Stroeer; `AMVMF` AMG Advanced Metallurgical → AMG Critical Materials rename).
- These OTC shadows leave the one-per-security `tickers.csv` export and are preserved as venue-level cross-listings in `listings.csv`/`cross_listings.csv`: primary tickers 61,623 → 61,559, ETFs 15,712 → 15,658, full listing rows unchanged at 71,041. This is collision-safe deduplication, not a coverage loss.
- Added `VIVHY` (Vivendi, France) and `WBRBY` (Wienerberger, Austria) to `entry_quality_warn_allowlist.csv`: correct US ADR ISINs on foreign issuers, a reviewed country/ISIN-prefix mismatch.

### Safety

- Deferred ~18 candidates that no independent source could confirm to the specific fund (Yahoo delisted/no-data, or only the generic umbrella issuer name).
- Rejected `AMFN` (offered Renewal Fuels' ISIN) and `IDOWF` (Yahoo ISIN conflict); caught and deferred `IAINF` (Yahoo "IA Global Infrastructure (Lazard)" vs holder "iShares AI Infrastructure" — generic-token false match).

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,559 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.3] - 2026-06-12

### Summary

ISIN-coverage release. A per-row, collision-reviewed pass over the free sources (TradingView scanner + StockAnalysis lists) added 54 missing ISINs to primary listings. Only "clean fill" candidates were applied — a previously-missing ISIN that is new to the dataset (no existing holder), so the row keeps its ISIN with no cross-listing collapse and a consistent ISIN prefix. No values were fabricated.

### Changed

- Backfilled 54 missing ISINs on primary listings (mostly non-OTC core primaries: US ETFs such as Baron and Capital Group, TSX/TSXV stocks, SSE), each gated by symbol + issuer-name match and validated against ISIN checksum and exchange-prefix consistency.
- Updated the public snapshot: ISIN coverage 59,899 → 59,953 (97.3%), core primary rows missing ISIN 929 → 885, core primary rows with ISIN 53,167 → 53,211, entry-quality source-gap rows 7,329 → 7,288. Primary ticker count unchanged at 61,623.

### Safety

- Excluded 83 OTC "collapse" candidates whose proposed ISIN already belongs to an existing primary: they would link OTC grey-market shadows into cross-listings, but most carry generic issuer names that confirm only the issuer, not the specific fund, so the provider-mapped ISIN cannot be verified per-row. Left to a dedicated per-fund pass.
- Caught and rejected one genuine wrong-ISIN match (`AMFN` was offered Renewal Fuels' ISIN); deferred two ADR country/ISIN warn cases (`VIVHY`/`WBRBY`).
- OpenFIGI and the official ASX ISIN feed were run against the full residual and yielded 0; the remaining ~885 core gaps are delisted/obscure rows uncovered by free sources and are left as-is rather than filled with fabricated values.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates (`us_foreign_isin_unreviewed_count` = 0), 61,623 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.2] - 2026-06-11

### Summary

Coverage release. With outbound network access restored, the free-source sector/category backfills were run against the residual gaps and 616 reviewed, source-backed fills were applied. No values were fabricated; each was gated by symbol and issuer-name match. ISIN bulk-backfilling was deliberately excluded because it collapses OTC duplicate listings into cross-listings and trips entry-quality gates — it needs a dedicated per-row collision review, not a bulk fill.

### Changed

- Backfilled 616 missing sector/category values:
  - 20 from same-ISIN listing peers (no network).
  - 596 from StockAnalysis exchange lists and the TradingView scanner (497 ETF categories + 99 stock sectors), gated by symbol + issuer-name match and validated against the canonical taxonomy. ETF categories are correctly differentiated (Equity / Fixed Income / Commodity / Currency).
- Updated the public snapshot: sector/category coverage 58,845 → 59,027 (95.8%), stock sector coverage → 43,397, ETF category coverage → 15,630, entry-quality source-gap rows 7,410 → 7,329. Primary ticker count unchanged at 61,623.

### Safety

- Did not bulk-apply free-source ISIN candidates: they collapsed 87 OTC duplicate listings into cross-listings and produced unexpected entry-quality warns (ADR country/ISIN conflicts, e.g. `VIVHY`/`WBRBY`). ISIN gap-closing is left to a dedicated collision-reviewed pass.
- Remaining coverage gaps are left as-is rather than filled with fabricated values: the residual `missing_stock_sector` tail is uncovered by the free sources (StockAnalysis, TradingView, FinanceDatabase), `source_gap_rows` depend on partial or unavailable official sources, and `expected_missing_primary_isin` is tracked as expected-missing by design.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,623 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.1] - 2026-06-10

### Summary

Patch release correcting stale corporate actions, one erroneous source name, and missing country metadata surfaced by re-running the Twelve Data comparison against a refreshed stock dump. Every change was validated per-row against primary sources (SEC/EDGAR, official exchange references, company filings, Bloomberg) and applied through the override + clean-rebuild workflow; instrument identity (ISIN) was preserved wherever only a name or ticker changed.

### Fixed

- Corrected stale, ISIN-stable issuer renames:
  - `NASDAQ:IAC` → `NASDAQ:PPLI` (IAC Inc. → People Incorporated), effective 2026-06-04; CUSIP/ISIN `US44891N2080` unchanged.
  - `LSE:0QK3` Dufry AG → Avolta AG (same ISIN `CH0023405456`).
  - `LSE:ALBA` Alba Mineral Resources → Arkadian Strategic Metals Plc (same ISIN `GB00B06KBB18`).
- Dropped the stale `NASDAQ:LITM` row (Snow Lake Resources → Frontier Nuclear and Minerals `FNUC`, 2026-03-16; the `FNUC` row already exists).
- Consolidated the OPAP → Allwyn AG transition: renamed `ATHEX:OPAP` → `ATHEX:ALWN` (Allwyn AG, ISIN `GRS419003009` retained, FIGI preserved) as the canonical Athens primary, merged the redundant ISIN-less `ALWN` placeholder, renamed the `LSE:0FI1` line, and linked the `OTC:GRKZF` line as a cross-listing; the `OTC:GOFPY` US ADR is unchanged.
- Corrected an erroneous Twelve Data name on `OTC:AAYYY` (was "AACL Holdings Ltd.", a separate delisted entity) to Australian Agricultural Company Limited (OTC ADR of ASX:AAC, ISIN `AU000000AAC9`).

### Changed

- Filled missing `country`/`country_code` on 28 rows left blank by prior name-update sweeps, using incorporation domicile (e.g. `CHNVF` Youzan Technology → Bermuda, `ACCL` Acco Group Holdings → Cayman Islands).
- Refreshed all canonical exports and reports; updated the public snapshot to 61,623 primary tickers, 71,041 listing rows, 45,911 stocks, 15,712 ETFs, and 122,004 aliases.

### Safety

- Left `NYSE:AERO` country blank by design — an existing reviewed override deliberately cleared a same-ticker ISIN collision with Montana Aerospace AG.
- Kept country as Greece for the Allwyn AG `GRS419003009` lines (ISIN prefix + Athens listing); the May-2026 redomiciliation to Switzerland is noted but not forced against the Greek ISIN.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,623 ticker rows, and 71,041 listing rows.
- `pytest tests/ -q`: 1,445 passed.

## [3.30.0] - 2026-06-10

### Summary

Twelve Data reconciliation release. The full local listing universe was compared against the supplied Twelve Data stock dump, all 4,946 supported stock-like name divergences were triaged with DeepSeek, and only source-adjudicated, provider/identifier-supported `apply_*` decisions were promoted into canonical data. Twelve Data remains a challenger source: conflicts, scope risks, source gaps, and pending provider validations stay blocked in machine-readable reports instead of being applied blindly.

### Added

- Added the Twelve Data comparison and adjudication workflow:
  - `scripts/compare_twelvedata_stocks.py`
  - `scripts/build_twelvedata_review_queues.py`
  - `scripts/build_twelvedata_deepseek_batches.py`
  - `scripts/run_deepseek_review_queue.py`
  - `scripts/build_twelvedata_second_source_queue.py`
  - `scripts/validate_twelvedata_second_sources.py`
  - `scripts/build_twelvedata_source_adjudication.py`
  - `scripts/build_twelvedata_manual_apply_queue.py`
  - `scripts/build_twelvedata_review_rollup.py`
- Added generated Twelve Data evidence reports covering the 190,193-row input dump, matched/unmatched local listings, name mismatches, stale-local candidates, provider validation queues, DeepSeek advisory output, source adjudication, and source-name update packages.
- Added regression tests for the Twelve Data review, DeepSeek batch, second-source, source-adjudication, manual-apply, and rollup builders.

### Changed

- Applied 344 source-adjudicated Twelve Data name updates that passed provider/identifier gates and clean rebuild validation.
- Expanded accepted stock-name update scope to include reviewed ADR/DR rows while preserving listing-key and identifier stability.
- Refreshed all canonical exports and reports after the Twelve Data updates: `tickers.csv/json/parquet/db`, listing/core/cross-listing exports, alias exports, Adanos reference exports, coverage, entry-quality, completion backlog, source-gap, source-of-truth, validation, and README snapshot metrics.
- Updated the public snapshot to 61,627 primary tickers, 71,043 listing rows, 45,915 stocks, 15,712 ETFs, and 122,012 aliases.
- Refreshed daily symbol-change artifacts merged since v3.29.1.

### Safety

- Kept 4,602 Twelve Data name divergences blocked or pending when evidence was insufficient, providers disagreed, FIGI identity conflicted, scope was risky, or primary-source validation was still required.
- Explicitly excluded stale or unsafe Twelve Data candidates such as `OTC::GLVT` and `TSXV::KLX` after source review and rebuild validation.
- DeepSeek output is retained as advisory review evidence only; it does not directly authorize canonical data changes.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates, 61,627 ticker rows, and 71,043 listing rows.
- `pytest tests/ -q`: 1,445 passed.
- GitHub CI on `main` after the merged stack: pass.

## [3.29.1] - 2026-06-06

### Changed

- Removed stale generated artifacts from earlier verification runs, including `error.txt` and 200 verification run logs. No canonical ticker data changed.

## [3.29.0] - 2026-06-05

### Summary

Completes the ticker-collision ISIN cleanup across the remaining asset classes and venues (ETFs and OTC), after a re-audit showed the class was not limited to US-primary stocks. Together with v3.28.0 this resolves the systematic ticker-collision ISIN errors end to end and recovers real foreign listings that had been suppressed by them.

### Fixed

- Extended the ticker-collision ISIN fix to **US-exchange ETFs**: a re-audit found the collision class was not limited to stocks (e.g. the iShares MSCI Chile ETF carried Echo Investment's Polish ISIN; iShares Bitcoin Trust an Australian ISIN). A two-pass multi-agent sweep of all 443 US-exchange ETFs with non-US ISINs found ~439 collisions (only Sprott PHYS/SPPP are genuinely foreign-domiciled). 409 were corrected to the verified US ISIN (checksum-validated) and the remainder cleared (missing > wrong). Correcting these *also recovered 184 real foreign listings* (LSE/XETRA UCITS ETFs and OTC foreign stocks) that had been suppressed because a US ticker had taken over their ISIN.
- Swept the **OTC** foreign-ISIN pool (~2,360 rows, predominantly legitimate foreign grey-market listings) for the same collision class: 117 confirmed collisions where an OTC ticker carried a different same-ticker company's ISIN (e.g. Amundi SA with a Philippine AgriNurture ISIN, Euronext N.V. with an Australian Enegex ISIN, Exmar NV with a Canadian EXMceuticals ISIN). Corrected to the verified ISIN where available, otherwise cleared.

### Changed

- Extended the `us_foreign_isin_unreviewed_count` release gate (and its regression test + `data/review_overrides/foreign_isin_reviewed.csv` allowlist) to cover US-primary **ETFs** in addition to stocks, so a US-domiciled ETF carrying a foreign ISIN is now flagged.

## [3.28.0] - 2026-06-04

### Summary

Correctness-hardening release built on top of v3.27.0. A 384-row statistical audit (validated against authoritative live sources) measured row-level correctness at 97.1% (95% Wilson CI [94.9%, 98.4%]) and surfaced three error classes; the two systematic ones are fixed wholesale here and the largest is now guarded by a deterministic release gate.

### Fixed

- Fixed a systematic ticker-collision ISIN error class: 571 US-exchange listings carried the ISIN (and country) of a *different* same-ticker foreign company picked up by a ticker-only identifier match (e.g. Aflac holding an ASX ISIN, American Electric Power a Canadian ISIN, Assurant an Air New Zealand ISIN, Carnival the Carnival plc UK ISIN, IAMGOLD the Insurance Australia Group ISIN). These were internally consistent (country matched the wrong ISIN) and so evaded the existing gates. Each row was validated against authoritative sources (OpenFIGI/SEC/MarketScreener/issuer pages); 537 were corrected to the verified ISIN and 34 were cleared where the correct value could not be confirmed (missing > wrong).
- Fixed 11 row-level errors surfaced by a 384-row statistical correctness audit: wrong-issuer ISINs (BMY=Bristol-Myers had Bloomsbury's GB ISIN; AMP=Ameriprise had AMP Ltd's AU ISIN; BME, NXL, GREN), a stale post-spinoff ISIN (RNA/Atrium Therapeutics, cleared), stale names (Cloud Air→HMNEX, Sinosteel Anhui→Sinosteel New Materials, generic trust name on SMRF→ALPS Nautilus SMR ETF), and a wrong country (CKALF/Cokal→Australia).
- Fixed 87 generic ETF names that stored the registrant/umbrella trust name instead of the specific fund, each replaced with the verified marketed fund name from the issuer/exchange (e.g. "VanEck ETF Trust"→"VanEck AA-BB CLO ETF", "Cohen & Steers ETF Trust"→"Cohen & Steers Infrastructure Opportunities Active ETF", "Capital Group Equity ETF Trust"→"Capital Group U.S. Large Growth ETF", "iShares V PLC"→"iShares iBonds Dec 2031 Term $ Corp UCITS ETF"), including disentangling several mutually-mislabeled Harel (TASE) index funds.

### Added

- Added a release gate `us_foreign_isin_unreviewed_count` plus the reviewed allowlist `data/review_overrides/foreign_isin_reviewed.csv` (469 reviewed legitimate foreign-incorporated / cross-listing rows). Any US-primary Stock listing carrying a non-US, non-offshore ISIN must now be reviewed, preventing the ticker-collision-ISIN class from silently recurring. Covered by a regression test.

## [3.27.0] - 2026-06-03

### Summary

Large data-correctness release: the committed exports were stale relative to the refreshed official masterfile reference. Every disputed company name and identifier was re-validated against authoritative external sources (SEC EDGAR, exchange announcements, company IR/press releases, stockanalysis.com) before any change. Net effect: `official_isin_mismatch` 23 → 0, `official_name_mismatch` 39 → 3, total entry-quality warnings 115 → 66, with `validate_database.py` passing all 82 error gates and the full test suite (1419) green.

### Fixed

- Synced the canonical dataset to the refreshed official masterfile reference, absorbing externally validated 2025–2026 corporate renames, stock splits, and identifier changes the committed exports had not yet picked up — e.g. AMC Networks→AMC Global Media, Nextracker→Nextpower, Beauty Health→SkinHealth Systems, Fidelis→Pelagos, SolarBank→PowerBank (SUUN), Matahari→MDS Retailing, Premier1→PLC Resources, GreenHy2→H2G, Mora Telematika→Ekamas Mora Republik, plus the Swissquote 1:10 split (ISIN CH1548235246) and the Scynexis 1:8 reverse split (ISIN US8112923094).
- Resolved nine OTC reassignments so name and ISIN stay consistent (RPX Gold/RDEXF, Safi Silver/PNTZF, Aurbis Resources/QNICF, Eureka Metals/UREKF, Lion Critical Minerals/GBBGF, Osiris One Metals/IONGF, NorthPalm Capital/SCYRF, Altrova Health/SSPLF, plus the Dai-ichi Life ADR/DLICY) via reviewed name overrides rather than an ISIN-only update.
- Applied 417 additional externally validated company-name renames discovered by a systematic multi-source validation sweep of all 984 Latin-script dataset-vs-reference name divergences (e.g. Eletrobras→Axia Energia, Opcom→Hextar Capital, GD Express→GDEX, Speqta→BrightBid, Salini Impregilo→Webuild, plus many KOSDAQ/KRX/OTC issuers). 442 divergences were confirmed as benign romanization/abbreviation variants and left unchanged; 0 were left unresolved.
- Cleaned 49 stale or defective official-reference name entries where the dataset was already correct (outdated pre-rename names such as Essilor International→EssilorLuxottica, Schlumberger→SLB, JX Holdings→ENEOS, LiveChat Software→Text S.A., Specialty Holdco Belgium→Syensqo; ticker codes and venue abbreviations used as names). The dataset itself was unchanged by this cleanup.
- Corrected an erroneous official reference entry: ASX:LEL was mislabeled "Le Minerals Limited"; restored the externally verified current name "Lithium Energy Limited".
- Fixed post-corporate-action cross-listing ISIN mappings so split securities stay grouped: NASDAQ:SCYX now carries the post-reverse-split ISIN US8112923094 (shared with LSE:0L49), and OTC:SWQGF carries Swissquote's post-split ISIN CH1548235246 (shared with SIX:SQN and LSE:0QLD).
- Refreshed README snapshot metrics and all coverage, entry-quality, completion-backlog, source-gap, source-of-truth, alias-quality, and override-debt reports to the corrected dataset.

### Notes

- The 63 remaining country/ISIN-prefix divergences are by design — ADR/foreign lines where the issuer country is correct and the ISIN is a US ADR ISIN (e.g. Anheuser-Busch InBev, SAP, Sanofi) — and are not data bugs.

## [3.26.0] - 2026-06-03

### Added

- Added refreshed DeepSeek advisory batch planning and review evidence for collision, OTC/name-mismatch, and weak-sector triage. DeepSeek output remains advisory only and does not authorize direct data changes.
- Added source-refresh queue reporting with provider, source-mode, review-gate, global expansion, and last-error context for unavailable official sources.
- Added source-inventory context for blocker classes, source modes, refresh queues, and unavailable-source errors.

### Changed

- Refreshed data-quality reports and review queues across masterfile collisions, ISIN identity collisions, OTC/name mismatches, weak sectors, B3/Canada/ASX residuals, source freshness, OHLCV plausibility, FinancialData ISIN supplements, alias/detection quality, and override-debt tracking.
- Refreshed the completion backlog to 846 missing primary ISIN rows, 2,600 missing stock-sector rows, 102 missing ETF-category rows, and 12 source-gated next actions.
- Pruned the entry-quality allowlist to 115 expected warnings, 0 unexpected warnings, and 0 quarantined rows.

### Fixed

- Hardened release acceptance gates for source inventory and source-refresh queues, including candidate keys, providers, source evidence, review flags, classifications, source modes, unavailable-source last errors, and global expansion counts.
- Documented the MSE Malawi official mainboard HTTP 403 block and kept it source-gated instead of applying unsupported fallback data.

### Safety

- No DeepSeek-provided values are applied directly to canonical ticker data; advisory output is retained only as review evidence.
- Remaining metadata gaps stay review-gated or source-gated until official listing-keyed evidence is available.

## [3.25.0] - 2026-05-31

### Added

- Added an ISIN identity-collision review campaign (`scripts/build_isin_identity_collision_review_queue.py`, report `data/reports/isin_identity_collision_review_queue.*`) that flags ISINs shared by two or more distinct issuer-name clusters — a provable anomaly, since an ISIN identifies exactly one issuer. The current dataset surfaces 426 collision groups across 1,019 listings (for example a US-listed fund inheriting a foreign issuer's ISIN through a shared ticker). Name clustering reconciles share-class, depositary, spacing/punctuation, trailing trading-currency, and unicode/transliteration variants (Nordic diacritic-drop and German umlaut conventions) so only genuinely distinct issuers are reported.
- Added a DeepSeek v4 Pro triage cross-check (`scripts/validate_isin_collisions_with_deepseek.py`, report `data/reports/deepseek_isin_collision_validation.*`) that independently classifies every collision group and attributes the likely-misassigned listing. Across the full sweep of all 426 groups it confirmed 355 (83%) as distinct-issuer collisions; the 71 disagreements (63 same-issuer, 8 uncertain) are corporate renames, abbreviated trading names, and bilingual/nickname name variants that the review gate is designed to surface. The validator retries transient batch failures, records any residual error and continues instead of aborting, and supports `--offset` so a large sweep can run in parallel chunks (the full run completed with 0 errors). DeepSeek output is advisory triage and authorizes no data change.

### Changed

- Kept the campaign report-and-gate only: no ISIN, country, name, or scope value is changed. Each group is gated as `open_needs_official_identifier_evidence` until a national numbering agency, issuer, or exchange security master keyed to the exact listing confirms the holder. DeepSeek output is advisory triage and authorizes no data change.

## [3.24.0] - 2026-05-16

### Added

- Added a full official/review-gated source-refresh pass across exchange masterfiles, listing exports, identifiers, sector/category fills, aliases, residual source-gap reports, and source-of-truth decisions.
- Added resilient official-reader parsing for Ghana Stock Exchange markdown tables and a Casablanca Stock Exchange reader fallback when the public Next.js/API path is unavailable.
- Added fresh stock and ETF verification runs for the release, covering 52,170 stock checks and 18,873 ETF checks against the current official/reference universe.

### Changed

- Rebuilt canonical exports to 61,439 primary tickers, 71,043 listing rows, 59,608 ISIN-covered rows, 58,736 sector/category-covered rows, and 121,431 structured alias rows.
- Refreshed corporate-action symbol-change inputs to 230 fetched rows and 255 reviewed rows.
- Applied only review-gated refresh changes: 10 same-ISIN sector peer updates, 2 official-name-gated FinancialData ISIN supplement rows, and 2,433 deterministic ETF-name category updates.
- Rebuilt completion, source-gap, source-of-truth, entry-quality, OHLCV plausibility, Adanos reference, alias audit, detection simulation, coverage, listing-history, and validation reports.
- Kept `official_fill_required` at 0, leaving residual metadata gaps classified only as `accepted_source_gap` or `core_exclusion_candidate`.

### Fixed

- Classified temporarily blocked but implemented official sources as accepted source gaps when the source inventory documents an official implementation path.

## [3.23.0] - 2026-05-11

### Added

- Added the source-of-truth decision report (`data/reports/source_of_truth_decisions.*`) that assigns each residual metadata gap to `accepted_source_gap` or `core_exclusion_candidate`.
- Added review-gated residual classes for official identifier sources that do not expose ISINs, current official directory misses, unmatched official symbol references, official product-taxonomy gaps, and unmatched ETF product references.
- Added official Thailand SEC SET ISIN backfill coverage for review-gated SET identifier fills.

### Changed

- Reduced `official_fill_required` from 2,199 to 0 by separating real fill obligations from documented source gaps and scope-review candidates.
- Rebuilt source-gap, source-of-truth, and validation reports with 3,042 accepted source gaps and 818 core-exclusion candidates.
- Refreshed validation artifacts with 82 error gates and 0 failed gates.

## [3.22.0] - 2026-05-11

### Added

- Added a residual source-gap classification report (`data/reports/source_gap_classification.*`) that assigns every remaining primary-ISIN, stock-sector, and ETF-category gap to a deterministic rest class with a row-level source gate.
- Added release-gate validation for the source-gap classification report so stale, missing, unclassified, or non-review-gated residual gaps fail validation.
- Added a StockAnalysis list-screener backfill for review-gated free-source ISIN, stock-sector, and ETF-category evidence across OTC, B3, LSE, TSX, TSXV, ASX, SSE, and SZSE.
- Added official TWSE ISIN/sector and TMX TSX/TSXV stock-sector backfills with audit reports and focused unit coverage.
- Added TradingView ETF asset-class category backfill, a China bilingual ISIN gate for exact SSE/SZSE ETF symbols, same-ISIN TradingView gates for abbreviated stock/ETF product names, and additional official SEC SIC-to-sector mappings for OTC rows.

### Changed

- Rebuilt canonical exports to 61,455 primary tickers, 71,041 listing rows, 59,445 ISIN-covered rows, 58,654 sector/category-covered rows, and 121,314 structured alias rows.
- Reduced the field-completion backlog to 1,099 missing core-primary ISINs, 2,707 missing stock sectors, and 94 missing ETF categories.
- Classified all 3,900 remaining source gaps into hard residual classes without filling any values heuristically.
- Refreshed coverage, completion-backlog, source-gap classification, Adanos reference, detection simulation, entry-quality, and validation artifacts.

## [3.21.0] - 2026-05-10

### Added

- Added free-source B3 COTAHIST and NYSE Group Security Master sample backfills for review-gated ISIN and ETF-category fills.
- Added review-gated TradingView free scanner backfills for missing ISINs and stock-sector candidates across supported venues.

### Changed

- Rebuilt canonical exports to 61,653 primary tickers, 71,092 listing rows, 58,477 ISIN-covered rows, 57,123 sector/category-covered rows, and 120,454 structured alias rows.
- Reduced the field-completion backlog to 1,971 missing core-primary ISINs, 4,292 missing stock sectors, and 238 missing ETF categories.
- Refreshed coverage, completion-backlog, Adanos reference, detection simulation, entry-quality, and validation artifacts.

## [3.20.0] - 2026-05-05

### Added

- Added official HKEX HSIC browser-backed sector backfill for Hong Kong equities after matching live HKEX quote-page industry metadata to the official HKEX securities workbook.
- Added official B3 and BSE India sector backfills, plus a reviewed StockAnalysis OTC metadata backfill for selected OTC rows.
- Added broader database validation gates covering core listings, listing index, cross-listing pairs, instrument scopes, identifier summaries, FIGI/ISIN collisions, canonical sector/category values, metadata override leakage, entry-quality coverage, duplicate public aliases, and trimmed Adanos names.

### Changed

- Rebuilt canonical exports to 61,844 primary tickers, 71,092 listing rows, 56,704 ISIN-covered rows, 55,843 sector/category-covered rows, and 118,657 structured alias rows.
- Increased stock-sector coverage to 40,711 rows and kept ETF category coverage at 15,132 rows after normalizing legacy ETF-category override values to canonical buckets.
- Reduced the field-completion backlog to 3,917 missing core-primary ISINs, 5,549 missing stock sectors, and 452 missing ETF categories.
- Tightened identifier enrichment by dropping stale ISIN carry-forward and ambiguous/stale OpenFIGI mappings instead of preserving questionable FIGI coverage.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Fixed case/normalization-sensitive alias duplicate detection so equivalent natural-language aliases are held for review instead of exported to Adanos detection.
- Fixed stale identifier propagation where `identifiers_extended.csv` could retain ISINs after the authoritative listing row no longer had one.
- Fixed Adanos ticker-reference name hygiene by trimming API export text fields and adding a release gate for untrimmed names.

## [3.19.0] - 2026-05-04

### Added

- Added official SGX marketmetadata v2 enrichment to the SGX securities-prices parser, filling official ISINs for almost all current SGX rows from SGX-provided identifier metadata.
- Expanded deterministic ETF-name category rules for volatility, fixed-income, municipal, duration, multi-asset, leveraged/inverse, digital-asset, commodity, real-estate, technology, and equity-index products.

### Changed

- Rebuilt canonical exports to 61,846 primary tickers, 71,092 listing rows, 56,675 ISIN-covered rows, 51,713 sector/category-covered rows, and 118,631 structured alias rows.
- Increased ETF category coverage to 15,098 rows and reduced the ETF category backlog from 892 to 486 rows.
- Reduced missing core-primary ISIN rows to 3,941 after the official SGX metadata refresh.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Updated ETF outlier expectations for Yahoo-corrected ETF rows that are now deterministically categorized by the ETF-name classifier.

## [3.18.0] - 2026-05-04

### Added

- Added official-source ETF category normalization for SSE, SZSE, B3, and KRX exchange-masterfile feeds.
- Added OpenFIGI-backed FIGI enrichment to the listing-keyed extended identifier snapshot.

### Changed

- Rebuilt canonical exports to 61,941 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 51,364 sector/category-covered rows, and 118,208 structured alias rows.
- Expanded ETF category coverage to 14,710 rows and reduced the ETF category backlog to 876 entry-quality source gaps.
- Expanded listing-keyed FIGI coverage to 63,603 rows while keeping missing primary ISIN rows review-gated by venue source.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts.

### Fixed

- Let `scripts/enrich_global_identifiers.py` read `OPENFIGI_API_KEY` from the environment for keyed FIGI refreshes without passing secrets as CLI arguments.

## [3.17.0] - 2026-05-04

### Added

- Added the listing-key-first core data model with collision-safe `listing_key` identity across core, listing, alias, scope, and report exports.
- Added listing-scope exports and collision reports so global ticker collisions no longer block venue-level official rows.

### Changed

- Rebuilt canonical exports to 61,941 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 50,898 sector/category-covered rows, and 118,208 structured alias rows.
- Reduced legacy primary ticker collision exposure to one compatibility row while preserving official venue rows in listing-keyed exports.
- Refreshed validation, source inventory, completion backlog, entry-quality, alias-quality, Adanos reference, and listing-history artifacts for the listing-key-first model.

## [3.16.0] - 2026-05-03

### Added

- Added official supplemental-listing coverage for ADX, Bahrain Bourse, Borsa Istanbul, Boursa Kuwait, BSE India, CSE Sri Lanka, DFM, HKEX, MSX, NSE India, NZX, QSE, SGX, and Saudi Exchange.
- Added StockAnalysis batch controls and exchange mappings for BIST, HKEX, NSE India, SGX, and BSE India review runs.
- Added broader deterministic ETF category rules for money-market, fixed-income, commodity, real-estate, alternative, large-cap, factor, and equity-index product names.

### Changed

- Rebuilt canonical exports to 61,984 primary tickers, 71,092 listing rows, 56,175 ISIN-covered rows, 49,846 sector/category-covered rows, and 118,209 structured alias rows.
- Expanded ETF category coverage to 13,152 rows while keeping non-matching active/structured ETF products in the review backlog instead of applying a generic ETF fallback.
- Reduced the field-completion backlog to 4,578 missing core-primary ISINs, 9,686 missing stock sectors, and 2,452 missing ETF categories.
- Refreshed listing history, identifier snapshots, coverage, source-inventory, completion-backlog, alias-quality, Adanos reference, entry-quality, and validation artifacts for the expanded dataset.

### Fixed

- Removed BSE India fund plan and segregated-portfolio lines from the stock universe.
- Preserved official sector/category metadata when building safe supplemental listings.
- Updated cross-listing expectations for Microsoft after HKEX official coverage added `HKEX::04338`.

## [3.15.0] - 2026-04-22

### Added

- Added a persistent OTC review-decision workflow with committed `otc_review_decisions.csv` overrides so reviewed keep-current and hold-unresolved cases no longer re-open on every rebuild.
- Added release-tested OTC review suppressions and queue handling for reviewed stale-name cases in the entry-quality and OTC review reports.

### Changed

- Resolved the remaining active OTC name-mismatch queue to zero by applying reviewed issuer-name, ISIN, country, and drop decisions for the final unresolved OTC cases.
- Rebuilt canonical exports to 54,037 primary tickers, 62,539 listing rows, 48,900 ISIN-covered rows, 45,163 sector/category-covered rows, and 103,580 structured alias rows.
- Refreshed validation, listing-history, identifier, coverage, completion-backlog, OTC review, override-debt, and Adanos reference artifacts against the cleaned dataset.

## [3.14.0] - 2026-04-20

### Added

- Added targeted metadata refreshes for B3, TMX, LSE, and related review-gated coverage gaps.
- Added official B3 ETF and BDR fund rows plus refreshed official B3 ISIN coverage from the latest B3 masterfile data.
- Added reviewed FinanceDatabase stock-sector overrides for selected LSE investment trust and energy listings.

### Changed

- Rebuilt canonical exports to 54,026 primary tickers, 62,496 listing rows, 48,808 ISIN-covered rows, 45,006 sector/category-covered rows, and 103,499 structured alias rows.
- Reduced core primary rows missing ISIN to 3,920 and refreshed validation, source inventory, completion backlog, entry-quality, alias-quality, Adanos reference, and listing-history artifacts.
- Fixed the scheduled symbol-changes workflow by preventing duplicate GitHub auth headers and upgrading `peter-evans/create-pull-request` to `v8.1.1`.

## [3.13.0] - 2026-04-18

### Added

- Added a central database release-gate validator with JSON/Markdown reports for structural integrity, ISIN validity, listing scope consistency, Adanos alias safety, and coverage-report coherence.
- Added an Adanos Sentiment API detection simulator that smoke-tests natural-language ticker aliases against positive and negative text probes.
- Added CI enforcement for the database release-gate validator.

### Changed

- Blocked generic organization aliases such as `central bank` from natural-language detection exports after the simulator identified a false-positive risk.
- Rebuilt canonical exports to 54,020 primary tickers, 62,496 listing rows, 48,794 ISIN-covered rows, 44,996 sector/category-covered rows, and 103,489 structured alias rows.

## [3.12.1] - 2026-04-18

### Fixed

- Updated the entry-quality warn allowlist to match the reviewed v3.12.0 warning queue so CI blocks only new warnings.
- Updated GitHub Actions workflow dependencies to Node 24-compatible `actions/checkout@v6` and `actions/setup-python@v6`.

## [3.12.0] - 2026-04-17

### Added

- Added an alias-policy module, alias quality reports, and Adanos Sentiment API-safe ticker-reference exports with natural-language detection policies.

### Changed

- Restricted `tickers.csv.aliases` to conservative natural-language aliases while keeping ISIN/WKN and exchange-ticker identifiers in structured alias/identifier exports.
- Normalized API aliases by stripping security/legal suffixes, removing trademark/non-ASCII symbols, shortening ETF product names, deriving concise company aliases, and dropping cross-exchange alias contamination.
- Rebuilt canonical exports to 54,020 primary tickers, 62,496 listing rows, 48,794 ISIN-covered rows, 44,996 sector/category-covered rows, and 103,490 structured alias rows.

## [3.11.0] - 2026-04-17

### Added

- Added FinancialData.net international-symbol ingestion as a secondary discovery source with match, current-gap, and global-expansion reports.
- Added an official-ISIN supplement builder that accepts FinancialData-discovered rows only after matching an official active masterfile row with a valid ISIN, issuer-name gate, and no global ticker/ISIN collision.
- Added persistent FinancialData review artifacts and tests so accepted official-ISIN supplements remain idempotent across rebuilds.

### Changed

- Rebuilt the canonical exports to 53,998 primary tickers, 62,496 listing rows, 48,787 ISIN-covered rows, and 44,884 sector/category-covered rows.
- Expanded official-ISIN-backed coverage with 555 supplemental rows across NSE India, HKEX, Bursa, KRX, LSE, BSE India, and B3 while keeping FinancialData itself review-only.
- Refreshed listing history, identifier snapshots, completion backlog, source inventory, entry-quality reports, and README metrics against the expanded dataset.

## [3.10.1] - 2026-04-14

### Fixed

- Added `lxml` as an explicit dependency so CI can run `pandas.read_html` parser tests on fresh GitHub Actions runners.

## [3.10.0] - 2026-04-14

### Added

- Added a source-inventory backlog builder (`data/reports/source_inventory_gap.*`) that reconciles official full, official partial, normalization-alias, and global-expansion candidates against the coverage report.
- Added curated official source candidates in `data/masterfiles/source_candidates.json` so source gaps have explicit implementation status, blockers, review policy, and provenance.
- Added official CSE Sri Lanka company-info detail coverage with 310 ISIN-bearing reference rows while keeping CSE_LK out of the core export until reliable sector taxonomy is available.
- Added reviewed StockAnalysis metadata backfill tooling for tightly gated secondary ISIN and stock-sector fills.

### Changed

- Rebuilt the canonical exports to 53,446 primary tickers, 61,944 listing rows, 48,235 ISIN-covered rows, and 44,865 sector/category-covered rows.
- Reconciled the source inventory to 0 missing current-scope sources, 0 parser todo rows, and 0 real global-expansion candidates; remaining work is field completion and taxonomy coverage.
- Refreshed official masterfile reference coverage, completion backlog, listing history, identifier snapshots, and coverage reports against the expanded source set.

## [3.9.0] - 2026-04-13

### Changed

- Removed the legacy `sector` column from public ticker, listing, JSON, Parquet, SQLite, and latest-snapshot exports; consumers should use `stock_sector` for stocks and `etf_category` for ETFs.
- Rebuilt coverage, completion-backlog, identifier, and listing-history artifacts against the typed sector/category schema.

### Breaking

- Public exports no longer include the duplicated `sector` field.

## [3.8.0] - 2026-04-13

### Added

- Added a local Gemma plausibility-review workflow with resumable checkpoints, human-readable `error.txt` output, accepted-false-positive overrides, and tests for stale finding reconciliation.
- Added reviewed LLM plausibility accepts for known false positives so local model output does not masquerade as authoritative exchange evidence.

### Changed

- Completed the local Gemma pass across the primary ticker export with zero active structured data findings; remaining review-error rows are parse/retry cases.
- Applied reviewed Yahoo, XTB, and official cross-market reference overrides to replace or clear same-ticker cross-exchange ISIN contamination.
- Rebuilt the canonical exports to 53,789 primary tickers, 61,955 listing rows, 45,281 ISIN-covered rows, and 41,714 sector/category-covered rows.
- Tightened dataset audit coverage for invalid ISIN, country/ISIN, and alias-contamination findings; the rebuilt audit now reports zero flagged entries.

## [3.7.0] - 2026-04-12

### Added

- Added a field-level completion backlog report (`data/reports/completion_backlog.*`) that splits missing primary ISINs, stock sectors, and ETF categories by exchange and review policy.
- Added a reproducible enrichment pipeline orchestrator for masterfile refreshes, backlog builds, reviewed local backfills, rebuilds, coverage reports, and audit queue refreshes.
- Added a deterministic ETF-name category backfill that writes reviewed `etf_category` fills while keeping legacy `sector` output derived for compatibility.

### Changed

- Rebuilt coverage to 45,375 ISIN-covered rows and 41,738 sector/category-covered rows, reducing the ETF category backlog from 8,298 to 4,505 rows.
- Added typed metadata outputs for `stock_sector` and `etf_category` across CSV, JSON, Parquet, SQLite, listing history, audit, coverage, and review workflows while retaining `sector` as a legacy derived field.
- Switched listing-history identity comparisons to `listing_key` while preserving the existing event output order.
- Switched coverage-report identifier and masterfile-collision lookups to listing-key identity with ticker/exchange fallback for legacy inputs.
- Added `listing_key` to `identifiers_extended.csv` so FIGI/CIK/LEI enrichment rows are explicitly listing-keyed while preserving `ticker` and `exchange`.
- Documented ignored local probe/test artifacts and cleaned stale ETF-category provenance wording in the enrichment pipeline.

## [3.6.0] - 2026-04-12

### Added

- Added a FinanceDatabase metadata backfill workflow that applies ticker/exchange/asset/name gates and keeps ISIN updates disabled by default unless `--enable-isin` is explicitly used for reviewed batches.
- Added an EODHD exchange-symbol-list ISIN backfill workflow that reads the API key from `EODHD_API_TOKEN`, writes audit reports outside tracked data, and defaults to accepting only new ISINs that do not already exist in the primary export.
- Added a same-ISIN listing-peer sector/category propagation workflow with conflict detection and audit reports.
- Added Alpha Vantage, SEC SIC, JPX/TSE, and XTB-backed enrichment helpers for reviewed sector and identifier batches.

### Changed

- Enriched 1,746 sector rows and 14 strictly gated NYSE ETF ISIN rows, rebuilding coverage to 44,145 ISIN-covered rows and 33,573 sector-covered rows out of 52,747 primary tickers.
- Kept FinanceDatabase ISIN candidates out of the default pipeline after detecting cross-listing collision risk; accepted FinanceDatabase output is sector-only unless identifier review is explicitly enabled.
- Enriched 90 additional ISIN rows with strictly gated EODHD exchange-symbol-list candidates, rebuilding coverage to 44,235 ISIN-covered rows while keeping the primary ticker export stable at 52,747 rows.
- Enriched 346 additional sector/category rows from same-ISIN listing peers, rebuilding sector coverage to 33,919 rows while keeping the primary ticker export stable at 52,747 rows.
- Refreshed SSE, SZSE, TMX, Bursa, and IDX official masterfile inputs and rebuilt the canonical exports to 53,826 primary tickers and 61,956 listing rows.
- Hardened supplemental-listing rebuilds so official partial rows no longer overwrite existing issuer name, asset type, country, or country-code metadata.
- Tightened reviewed FinanceDatabase ISIN backfills with existing-ISIN peer-name conflict checks before accepting identifier overrides.
- Rebuilt coverage to 45,375 ISIN-covered rows and 37,945 sector-covered rows, with 38,324 core primary rows now carrying ISINs and 7,089 core primary rows still explicitly scoped as `primary_listing_missing_isin`.

## [3.5.0] - 2026-04-12

### Added

- Added a gated Yahoo Finance OTC ISIN backfill workflow that writes accepted ISINs to review overrides only when venue, quote type, issuer name, and ISIN checksum all pass.
- Added a gated official ASX `ISIN.xls` backfill workflow for missing ASX ISINs.
- Added a strict selected-exchange Yahoo missing-ISIN helper for US ETF batches with venue, quote type, expected ISIN country prefix, issuer/product-name, numeric-token, checksum, and progress gates.

### Changed

- Enriched 1,103 OTC rows with Yahoo Finance ISIN overrides and rebuilt core exports, reducing OTC rows without ISIN in `tickers.csv` from 2,524 to 1,421.
- Enriched 154 ASX rows with official ASX ISIN overrides and rebuilt core exports and coverage reports.
- Enriched 485 BATS ETF rows, 496 NASDAQ ETF rows, and 161 NYSE ARCA ETF rows with strictly gated secondary Yahoo ISIN overrides and rebuilt core exports and coverage reports to 44,131 ISIN-covered rows out of 52,747 primary tickers.
- Made rebuild country handling idempotent for foreign listings by deriving issuer country from valid ISIN prefixes instead of repeatedly clearing and restoring cross-listing ISINs.

## [3.4.0] - 2026-04-11

### Added

- Added official ISIN propagation from B3 `InstrumentsEquities` for Brazilian cash equities, ETF/fund lines, and ETF/ETP BDR rows.
- Added explicit `primary_listing_missing_isin` instrument-scope classification so ISIN-ready core rows can be filtered cleanly.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, verification runs, and coverage reports to the 2026-04-11 build.
- Expanded KRX/KOSDAQ and LSE/XETRA official reference coverage and corrected stale XETRA metadata where official venue data had safer ISINs.
- Improved B3 cash-instrument classification for `FUNDS`, BDR ETF/ETP rows, and uncategorized Brazilian stock classes.
- Improved global ISIN coverage to 41,846 rows, kept core primary rows missing ISIN explicitly scoped at 8,771, and improved ETF verification to 16,125 verified rows with 449 reference gaps.

## [3.3.0] - 2026-04-10

### Added

- Added official IDX coverage and promoted IDX to zero unresolved gaps.
- Added official TPEX mainboard stock coverage via the TWSE MOPS `t187ap03_O.csv` feed and completed TPEX stock/ETF reference coverage.
- Added official TASE `searchentities` supplements for foreign ETFs and participating units.
- Added official SZSE B-share coverage via the `ShowReport` `TABKEY=tab2` feed.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, verification runs, and coverage reports to the 2026-04-10 build.
- Reduced Stockholm unresolved gaps with additional official NGM, Spotlight, and Nasdaq Nordic mappings plus stale-listing cleanup.
- Reduced TASE unresolved gaps from 26 to 14 by normalizing legacy `PSG-*` ETF rows to current official `IBI.*` listings where the mapping was uniquely supported.
- Resolved remaining venue tails for TSX, TSXV, BMV, SZSE, and TPEX using official source expansions and conservative stale-row cleanup.
- Improved global verification coverage to 31,379 verified stocks with 12,363 stock reference gaps and 15,964 verified ETFs with 601 ETF reference gaps.

## [3.2.0] - 2026-04-09

### Added

- Added official Philippine Stock Exchange coverage via the PSE listed company directory frame feed, including active common, preferred, and ETF listings.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, and coverage reports to the 2026-04-09 PSE coverage build.
- Promoted PSE from a missing venue to official full coverage and pulled official names and ISINs into the rebuilt dataset for code-like preferred share rows.
- Hardened masterfile row deduplication so partial source refreshes no longer fail on `None` values in incoming reference rows.

## [3.1.0] - 2026-04-09

### Added

- Added official TPEX ETF coverage via the TPEx ETF InfoHub export.
- Added a TMX GraphQL ETF fallback/merge path so TSX/TSXV ETF reference coverage no longer depends on a single screener response shape.

### Changed

- Refreshed all core exports, listing history artifacts, identifier snapshots, and coverage reports to the 2026-04-09 build.
- Prefer current official TMX ETF listings over stale same-venue duplicate fund rows when the product identity already matches on the exchange.
- Extended verification to accept same-exchange ISIN matches and official SIX ETF/ETP fund products where the reference source is authoritative.

### Fixed

- Fixed TPEX ETF parsing for numeric bond-style symbols such as `00679B`.
- Reduced TSX ETF reference gaps caused by stale legacy symbols that still pointed to the same underlying fund product.

## [3.0.0] - 2026-04-07

### Breaking

- `data/tickers.csv` is now a primary-listing-only core export. Secondary venue lines no longer appear in the canonical flat dataset.
- Multi-venue identity is now represented explicitly through `data/listings.csv`, `data/listing_index.csv`, and `data/cross_listings.csv`.

### Added

- Added stable listing-keyed exports and cross-listing artifacts for downstream systems that need every venue line without global ticker ambiguity.
- Added broader official reference coverage for LSE, TMX, SSE ETFs, KRX/KOSDAQ, XETRA stocks, and XETRA ETFs/ETPs.
- Added richer machine-readable coverage reporting via `data/reports/coverage_report.json` and `data/reports/masterfile_collision_report.json`.

### Changed

- Tightened the stock core export to exclude preferreds, depositary lines, corporate-action rows, and other non-canonical secondary lines.
- Improved venue normalization so the core export prefers official primary markets when the same issuer trades across multiple exchanges.

### Fixed

- Replaced the LSE HTML parsing path with a dependency-light stdlib parser so CI no longer depends on optional `lxml` support.
