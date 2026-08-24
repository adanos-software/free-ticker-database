# Changelog

## [Unreleased]

### Added

- Classify vanished official-reference rows with the delisting classifier and a still-in-database backlog. Rotation no longer treats feed absence as a drop.

### Changed

- Burned listing-keyed entry-quality contradictions: official exchange ISINs replace provider-locked ISINs where the unique active official reference matches the same identity; venue-directory renames keep the listing ISIN; DAVIDsTEA no longer shares Deutsche Telekom's ADR ISIN.
- Completed country+ISIN review pairs for genuine issuer-domicile vs ISIN-prefix cases (ADR/CDR). Residual warns are name/ISIN disagreements without official ISIN evidence or with ticker-reuse risk.

## [3.35.0] - 2026-08-20

### Summary

**Merge-profile dataset release after official rotation, listing-keyed identity recodes, and hashed license reviews.** Publishes the work accumulated since v3.34.0. This is still a `merge` claim, not `stable` or `complete`: official-full contracts stay license-blocked, identity conflicts remain explicit, and unverified sources stay `review_required`.

### Changed

- Rotated official masterfiles (19 Aug overlay) with gated completeness fills and scheduled Nasdaq, symbol-change, and BME directory refreshes.
- Filled listing-level sectors from same-ISIN non-OTC peers, then `CPH::RTX` from official Nordic Telecommunications via canonical GICS.
- Recoded `LSE::GEM` to the official Guernsey ISIN and `OSL::ADS` from Norway to Cyprus using the official CY ISIN.
- Pinned official ADR and GDR listings as reviewed depositaries. Accepted OTC ordinary CINS duals of confirmed ADRs, then OTC ordinary US-CINS lines with unique issuer-country evidence, without recoding siblings to US.
- Recorded hashed Nasdaq Trader, Euronext, QSE, PSE, DFM, and KAP website terms as `verified_restricted`. Left `bolsa_santiago_instruments` as `review_required` after a bot-blocked terms fetch.

### Fixed

- Kept a BME official snapshot when share-detail calls 403 or time out, and kept listing joins when share-detail impersonation times out.
- Restored live BME ListedCompanies after it returned 200 (123 listed companies; security-prices 271→272).
- Pruned 124 stale entry-quality allowlist keys after warns fell to 81, and listed `masterfile_vanished_delisting_review.json` in release source-report integrity.

### Safety

- Unknown legal terms stay `review_required`. Only SEC is `verified_open`; restricted reviews do not unlock official-full or `stable`/`complete`. No license, ISIN, name, or sector was invented.
- FSX supplement extras remain out of the database. Unevidenced listing drops and critical-field changes still fail closed.

### Verification

- `python -m pytest tests/ -q` and `scripts/validate_database.py` are re-run by the tag workflow, together with `scripts/build_release_acceptance_report.py --fail-on-failure` and `scripts/build_quality_contract.py --profile merge --strict`.
- Snapshot: 63,802 primary tickers, 92,006 listing rows, 98.0% ISIN coverage, 97.6% sector/category coverage, 81 entry-quality warns, 11,458 source-gap rows. Source registry: 1 `verified_open`, 9 `verified_restricted`, 128 `review_required`.

## [3.34.0] - 2026-08-18

### Summary

**Canonical v4 foundation and listing-keyed evidence release.** Publishes the fail-closed canonical rebuild, official Frankfurt directory, ISO MIC mapping, and gated ISIN/taxonomy fills accumulated since v3.33.0. This is a `merge`-profile dataset release, not a `stable` or `complete` claim: unresolved identity groups, unverified source licenses, and remaining ISIN/sector gaps stay explicit.

### Added

- Reviewable canonical v4 pipeline: listing-keyed identity adjudication, fail-closed merge evidence, source/coverage contracts, and CSV plus PostgreSQL canonical exports.
- Official Deutsche Boerse T7 Frankfurt all-tradable directory (`XFRA` → `FSX`), refresh-only so extra dual listings are not dumped into the public set.
- ISO 10383 MIC mapping for every venue, gated Nasdaq Trader `Delete` handling, and listing-keyed SET SEC ISIN plus Cboe/Yahoo ETF evidence.

### Changed

- Daily masterfile, Nasdaq-new-listing, symbol-change, and delisting workflows now rebuild through `scripts/rebuild_canonical.py` and `scripts/check_safe_merge.py`.
- Tag releases enforce `merge --strict` and keep `stable`/`complete` advisory until licenses, official-full contracts, provenance, and identity are actually earned.
- Filled residual US ETF ISINs from gated Yahoo evidence, then 281 same-ISIN non-OTC peer sector/category values plus `NYSE::GCPB` (`US38151N8092`).
- Rotated official masterfiles and published scheduled Nasdaq, symbol-change, drift, delisting, and OpenFIGI validation refreshes; corrected JPX REIT asset-type inference.

### Fixed

- Stopped same-ticker unique-name ISIN copies from filling OTC peers from BATS/NASDAQ namesakes.
- Kept Frankfurt T7 as listing presence rather than identity, so bilingual/short official names do not emit false ISIN or name mismatches.
- Identity apply remains limited to decisive families; proposed official-name updates and the 749 `official_exact_listing_match` identifier clears are not bulk-applied.
- Aligned the public Kaggle/Hugging Face listing-events schema with the evidence-bound history columns so tag packaging no longer fails closed on header drift.

### Safety

- Unknown legal terms stay `review_required`; only SEC is `verified_open`. No license, ISIN, or sector was invented to pass `stable`/`complete`.
- FSX supplement extras remain out of the database. Unevidenced listing drops and critical-field changes still fail closed.

### Verification

- `python -m pytest tests/ -q` and `scripts/validate_database.py` are re-run by the tag workflow, together with `scripts/build_release_acceptance_report.py --fail-on-failure` and `scripts/build_quality_contract.py --profile merge --strict`.
- Snapshot after the latest canonical rebuild: 63,790 primary tickers, 91,977 listing rows, 97.9% ISIN coverage, 97.0% sector/category coverage.

## [3.33.0] - 2026-08-06

### Summary

**Official-masterfile rotation and provider-validated global stock/ETF expansion.** Adds the Twelve Data candidates that pass the EODHD identity gate, while preserving official-source precedence and explicit review gates for unresolved identities.

### Changed

- Rotated official masterfile references and regenerated canonical listings, identifiers, aliases, coverage, and review artifacts.
- Added provider-validated Twelve Data stocks and ETFs with ISIN, FIGI where available, and listing-keyed metadata updates.
- Hardened EODHD venue validation for OTC/PINK candidates and rejected the PINX-to-NASDAQ false match for `LIPO` from the provider import.

### Safety

- Potential symbol-reuse and OTC identity conflicts remain explicit `hold_unresolved` review items; no name, ISIN, or alias change was inferred.
- Extended OTC source gaps remain blank and review-gated; core-listing identifier requirements are unchanged.

### Verification

- `python -m pytest tests/ -q`: 1,717 passed. `scripts/validate_database.py`: 84/84 error gates passed.
- `scripts/check_entry_quality_gate.py --fail-on-stale-allowlist`: 205 reviewed warnings allowed, 0 unexpected, 0 stale. `scripts/build_release_acceptance_report.py`: 67/67 criteria passed.

## [3.32.10] - 2026-08-03

### Summary

**Official-scope hardening and current exchange-audit release.** Publishes the listing updates accumulated since v3.32.05, removes official non-equity leakage, and aligns release decisions with the latest source-coverage and freshness evidence.

### Changed

- Refreshed Nasdaq US additions, daily symbol changes, LSE symbol rotations, and official exchange masterfiles.
- Added explicit exchange-scope coverage decisions and validation gates without promoting any incompletely verified exchange.
- Updated the TPEX, USE_UG, VSE, ZSE, and ZSE_ZW decision reasons after their official source snapshots crossed the freshness boundary; every exchange remains `official_partial`.
- Regenerated coverage, exchange-source, drift, pending-rename, delisting-candidate, and delisting-apply reports.

### Fixed

- Removed 205 officially identified units, warrants, notes, preferreds, rights, and fund-instrument rows that had been admitted as `Stock` because generic issuer names hid the security type.
- Dropped the stale `NYSE::GRTUF` row after official evidence confirmed the NYSE line had ended and `GRTUF` moved to OTCQX; retained the documented SEC-directory false-positive exception for `NYSE::PHXE-P`.
- Removed the now-stale `OTC::FSLUF` entry-quality warning allowlist row and regenerated extended identifiers, listing index, exports, quality reports, and release acceptance artifacts.
- Made resumable OHLCV sampling rewrite the completed CSV to the current selection, preventing stale rows from surviving sample-selection changes.
- Corrected release validation ordering so source-derived coverage is rebuilt before scope-decision validation.

### Safety

- The current drift report contains no pending rename candidates.
- The current delisting apply report performs no automatic drops: 26 suspended listings remain retained by policy and 144 candidates remain queued for manual rename-versus-delisting review.

### Verification

- `python -m pytest tests/ -q`: 1682 passed. `scripts/validate_database.py`: 84/84 error gates passed.
- `scripts/check_entry_quality_gate.py --fail-on-stale-allowlist`: 20 reviewed warnings allowed, 0 unexpected, 0 stale. `scripts/build_release_acceptance_report.py`: 67/67 criteria passed.

## [3.32.05] - 2026-07-28

### Summary

**Resilient HNX identifier refresh and current official-masterfile release.** Prevents transient VSDC lookup failures from dropping already verified HNX/UPCoM ISINs, and publishes the latest validated official exchange refresh.

### Changed

- Reused valid identifiers from the tracked masterfile reference for unchanged HNX and UPCoM listings before requesting missing identifiers from VSDC.
- Refreshed official HKEX, HNX, IDX, JPX, and KRX masterfile evidence, including 10 new rows, 9 vanished-source rows retained for delisting review, and 16 source-reported name changes.
- Regenerated canonical listings, identifiers, aliases, coverage, entry-quality, source-gap, validation, and Adanos reference outputs.

### Fixed

- Prevented an isolated VSDC request failure from erasing `HNX::VNR`'s verified ISIN and producing a false `official_name_mismatch` release-gate failure.
- Added a regression test covering a cacheless CI runner with an unavailable VSDC lookup and a valid tracked HNX identifier.

### Verification

- `python -m pytest tests/ -q`: 1661 passed. `scripts/validate_database.py`: 84/84 error gates passed. `scripts/check_entry_quality_gate.py`: 21 reviewed warnings allowed, 0 unexpected, 0 stale. Entry-quality coverage: 74,743/74,743 listings.

## [3.32.04] - 2026-07-23

### Summary

**Scheduled-pipeline reliability and ADR data-quality release.** Publishes the automated listing, symbol-change, masterfile, drift, and delisting refreshes accumulated since v3.32.03, together with fixes that keep those pipelines fail-closed and make ADR validation issuer-aware.

### Changed

- Refreshed daily symbol changes, Nasdaq US new listings, official exchange masterfiles, weekly drift/freshness data, and delisting-candidate evidence.
- Corrected issuer-country metadata for reviewed ADR listings and regenerated the canonical listing, identifier, alias, coverage, entry-quality, and validation outputs.
- Made ADR entry-quality checks use issuer identity instead of depositary boilerplate while preserving strict country and instrument-type validation.

### Fixed

- Repaired scheduled-workflow authentication and report dependency ordering, including generating entry-quality inputs before downstream coverage checks.
- Preserved previously verified masterfile sources when an upstream refresh returns no usable rows, and installed the browser runtime required by browser-backed rotations.
- Enforced exact entry-quality warning allowlist maintenance so unexpected and stale exceptions both fail CI.

### Verification

- `python -m pytest tests/ -q`: 1659 passed. `scripts/validate_database.py`: 84/84 error gates passed. `scripts/check_entry_quality_gate.py`: 21 reviewed warnings allowed, 0 unexpected, 0 stale. Entry-quality coverage: 74,722/74,722 listings.

## [3.32.03] - 2026-07-13

### Summary

**Issue #125 news-coverage gap closure.** Adds 71 news-observed securities that resolved to no instrument in the downstream Instrument Graph, and introduces a reviewed preferred/notes admission mechanism. Analysis of the reported 870-ticker gap found the large majority were already present in this database — 604 as primary-export rows (`data/adanos/ticker_reference.csv`) under the exact news symbol, and 29 as ISIN-linked venue/cross-listing rows of a company already carried under its primary symbol. This release adds the genuinely-missing common stocks, open-end ETFs, and exchange-listed preferreds/notes that were in scope.

### Added

- 71 primary listings via `data/coverage_expansion_listings.csv`: OTC/ADR variant symbols of listed companies (e.g. `DTEGY`, `VWAGY`, `SSUMY`, `CTATF`), foreign primaries (e.g. `ISP`), US small/mid caps (`PFBC`, `SATA`, `XOMAO`, `ALBC`), open-end ETFs (`DRAM`, `REXC`, `ETHE`, `BBAG`, `IRBO`, …), and 5 exchange-listed preferreds/notes (`SAJ`, `STRD`, `STRF`, `BRKRP`).
- `data/review_overrides/preferred_allowlist.csv` and `load_preferred_allowlist()` in `scripts/rebuild_dataset.py`: a reviewed `(ticker, exchange)` allowlist that admits exchange-listed preferreds/notes and common-stock name false-positives (e.g. "Preferred Bank") past the non-common-stock name filters.

### Changed

- Un-dropped `SAJ` (Saratoga Investment Corp 8.00% Notes) from `data/review_overrides/drop_entries.csv` per the preferred/notes scope policy.
- Two beneficial primary re-keyings driven by adding the news-observed symbols: Intesa Sanpaolo `0HBC` (LSE placeholder) → `ISP` (Borsa Italiana home), and JPMorgan BetaBuilders U.S. Aggregate Bond ETF `JAGG` → `BBAG` (current ticker). Security identity preserved in both.
- Regenerated dataset exports, extended identifiers, Adanos reference, coverage/entry-quality/source-gap/validation reports, and the README snapshot.

### Scope decisions (issue #125)

- Closed-end funds (OpenFIGI `securityType=Mutual Fund`) and SPAC units/rights remain **out of scope** and stay recorded in `drop_entries.csv`; the news-requested CEFs (`ECC`, `RQI`, `HFRO`, `KYN`, `RCG`, …) are intentionally not reintroduced.
- Same-security OTC/ADR variants (e.g. `ERIXF`, `CSLLY`, `STMEF`) are not added as duplicate primaries — the ISIN de-dup pipeline correctly folds them into the existing security; they remain resolvable through the ISIN-linked rows in `data/listings.csv` / `data/cross_listings.csv`.

### Verification

- `python -m pytest tests/ -q`: 1623 passed. `scripts/validate_database.py`: pass, 0 failed error gates. `scripts/check_entry_quality_gate.py`: pass, 0 unexpected warns. `scripts/check_readme_snapshot.py`: pass.
- Ticker-set diff vs prior head: 71 added, 2 re-keyed (identity preserved), 0 securities lost.

## [3.32.02] - 2026-07-08

### Summary

**v3.32.01 quality-gap closure release.** Publishes the source-backed cleanup, regenerated quality reports, and release-gate hardening from PR #124.

### Changed

- Reduced residual source gaps and primary ISIN backlog through reviewed source evidence and strict Yahoo/TradingView/OpenFIGI-backed workflows.
- Added and refreshed verification artifacts for reviewed-source, QFMA, MCD, OpenFIGI, TMX, and TradingView residual workflows.
- Hardened source-gap, release-acceptance, drift, and ISIN-validation reporting so remaining residuals are classified instead of hidden.
- Regenerated dataset exports, Adanos reference files, README snapshot, completion backlog, drift report, release acceptance matrix, and validation reports.

### Verification

- `scripts/validate_database.py`: pass with 0 failed error gates. `scripts/check_entry_quality_gate.py`: pass. `scripts/check_readme_snapshot.py`: pass. `scripts/build_release_acceptance_report.py`: 67/67 criteria passed. `scripts/build_drift_report.py`: pass, `pending_renames_count=0`, `drift_detected=false`. Focused pytest suite: 305 passed.

## [3.32.01] - 2026-07-07

### Summary

**Quality-gate hardening for the v3.32.01 completeness backlog.** Adds release metadata consistency checks, stale warning allowlist enforcement, and drift regression snapshots for source-gap and official-recall metrics.

### Changed

- Added a release-acceptance criterion that requires `VERSION` to have a matching dated `CHANGELOG.md` release section.
- Tightened entry-quality release acceptance so stale warning allowlist rows fail instead of silently lingering.
- Extended the drift report with quality-count and per-exchange official-recall regression detection against the previous generated drift snapshot.
- Pruned stale entry-quality warning allowlist rows that no longer correspond to current warning rows.

### Verification

- `scripts/rebuild_dataset.py`: pass. `scripts/validate_database.py`: 0/83 failed error gates. `scripts/check_entry_quality_gate.py`: pass with 0 stale allowlist rows. `scripts/check_readme_snapshot.py`: pass. `scripts/build_release_acceptance_report.py`: 65/65 criteria passed. `scripts/build_drift_report.py`: pass, `drift_detected=false`. Focused pytest suite: 290 passed.

## [3.32.00] - 2026-07-07

### Summary

**M4 completeness campaign for issue #118.** Publishes the completeness layer from PR #122, including per-exchange recall, explicit residual source decisions, ETF universe comparison, primary-ISIN backlog evidence, and listing-status history support.

### Added

- Added per-exchange official-masterfile recall reporting to the coverage and release-acceptance reports.
- Added primary-ISIN completeness, ETF universe completeness, and CFI code review reports with explicit source gates.
- Added Borsa Italiana, MSE Malawi, Nasdaq Trader Daily List, OTC/FINRA, JPX, and issuer/exchange ETF source coverage paths where available.
- Added listing-status history output for active, suspended, and delisted listings with effective-date support.

### Changed

- Promoted `core_listings.csv` as the canonical collision-safe consumer export while keeping `tickers.csv` as the legacy compatibility export.
- Reduced current-scope missing-source inventory to zero and moved remaining residual gaps into generated source-of-truth decisions.
- Stabilized rebuilt CSV line endings so generated source artifacts are byte-stable across platforms.

### Verification

- `pytest`: 1,537 passed. `scripts/validate_database.py`: 0/83 failed error gates. `scripts/build_release_acceptance_report.py`: 64/64 criteria passed. `scripts/check_readme_snapshot.py`: pass. Bumps VERSION to 3.32.00.

## [3.31.01] - 2026-07-07

### Summary

**M3 correctness campaign fix release for issue #118.** Publishes the merged M3 correctness campaign reports from PR #121, including source-gated sector/category, name freshness, identity residual, non-equity leakage, and re-audit evidence.

### Added

- Added M3 correctness campaign artifacts and release-acceptance coverage for the C1, C2, C4, C5, and C6 campaign gates.
- Added a shared non-equity guard used by M3 reporting and Nasdaq new-listing ingestion.

### Fixed

- Ensured Unit/Warrant/Right/Debt/Preferred evidence is evaluated before common-share allow-listing in the non-equity guard.
- Ensured release acceptance regenerates M3 reports before evaluating them, preventing stale M3 artifacts from passing the release gate.

### Verification

- `pytest`: 1,512 passed. `scripts/validate_database.py`: 0/83 failed error gates. `scripts/build_release_acceptance_report.py`: 61/61 criteria passed. `scripts/check_readme_snapshot.py`: pass. Bumps VERSION to 3.31.01.

## [3.31.00] - 2026-07-07

### Summary

**M2 automation release for issue #118.** Promotes the new release pipeline and operational automation merged in PR #120, including tag-triggered release assets, generated binary/JSON/parquet outputs as release artifacts, high-confidence rename application, gated delisting application, scheduled masterfile rotation, campaign evidence archiving, and shared script helpers.

### Added

- Added tag-triggered GitHub Release automation that rebuilds public artifacts from source CSVs, runs validation gates, attaches release assets, and keeps Kaggle/Hugging Face publishing behind guarded repository configuration.
- Added strict apply paths for verified symbol changes and delisting candidates, plus scheduled masterfile rotation and per-venue diff reporting.
- Added release-asset and evidence-archive builders so generated exports are no longer normal git-maintained artifacts.
- Split dormant campaign scripts into `scripts/archive/` and introduced `scripts/lib/` helpers for shared data I/O, HTTP, key, and normalization behavior.

### Verification

- `scripts/rebuild_dataset.py`: pass. `python -m pytest tests/ -q`: pass. Bumps VERSION to 3.31.00.

## [3.30.48] - 2026-07-01

### Summary

**Processed the merged automation findings from the daily symbol-change, weekly drift, and weekly delisting-candidate reports.** High-confidence corporate-action findings were applied through review overrides, then the database, Adanos reference exports, history, source inventory, and quality reports were rebuilt.

### Changed

- Re-keyed current US listings: `SATS`→`ECHO` (EchoStar, Nasdaq), `SKLZ`→`FIRY` (Firy, NYSE), and `LC`→`HAPN` with exchange transfer from NYSE to Nasdaq.
- Dropped stale pre-change duplicate rows confirmed against official Nasdaq Trader and SEC evidence: `ASGN`, `BK`, `CGCT`, `MEG`, `QHUOY`, `SAVA`, `SCVL`, `SNSE`, `THAR`, and `VRAR`.
- Carried forward the verified `SHOE` ISIN after the `SCVL`→`SHOE` evidence review.
- Populated missing source-inventory metadata for the current-scope `Borsa Italiana` backlog row (`venue_name=Borsa Italiana`, `country=Italy`) and added a regression test for the generator fallback.
- Refreshed dataset exports, listing history, identifier coverage, source-gap/source-of-truth reports, Adanos ticker reference exports, validation reports, and README snapshot counts.

### Verification

- `scripts/check_entry_quality_gate.py`: pass. `scripts/check_readme_snapshot.py`: pass. `scripts/validate_database.py`: 0/83 failed error gates. `pytest tests/ -q`: 1,473 passed. Bumps VERSION to 3.30.48.

## [3.30.47] - 2026-06-22

### Summary

**Symbol reconciliation of the 7 renamed-AND-reticker rows** flagged in v3.30.46. Each was verified to carry a defunct old symbol (the security now trades under a new ticker). All 7 stale-symbol rows are dropped. DB-wide ISIN name-mismatch **105 → 98**.

### Removed

- **7 defunct-symbol rows dropped** (renamed + retickered; old symbol no longer trades): `LRGR`→FRTU (Fortun Holdings), `NBRY`→DAJL (Dajialai Digital Technology), `ADGL`→CODV (Compliance Advocates), `BRGC`→NALG (North America Lithium and Gold), `TEAH`→BJBJ (BJ Bio-Tech), `RTGC`→BQHG (Bo Qi Yi Hao) — all OTC — and `REDC`→VZLA (Red Capital → Apertura Energy Plc) on LSE.
- Primary tickers 63,148 → 63,141.

### Rationale

Re-adding the new symbols was investigated and deliberately deferred to the vetted coverage-expansion pipeline rather than hand-grafted, because: (1) **`REDC`→`VZLA` cannot be re-keyed** — `VZLA` is already held by Vizsla Silver (NYSE MKT), and the dataset enforces one symbol per global ticker; (2) the **six OTC cases are reverse-merger shells** whose name, sector, and domicile all changed (e.g. Newberry Specialty Bakers / US Consumer Staples → Dajialai Digital Technology / Chinese tech), so carrying the old fields onto a new symbol would inject stale/incorrect data. Dropping cleanly removes the confirmed-defunct symbols; the new listings can re-enter via the verify-active coverage-expansion process with correct current classification.

### Verification

- `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `isin_validation_report` refreshed (98 mismatches, all confirmed false positives). Bumps VERSION to 3.30.47.

## [3.30.46] - 2026-06-22

### Summary

**External verification of the residual 118 ISIN name-mismatches.** Rather than assume the post-campaign residue was all false positives, each was checked against external sources (exchange sites, SEC EDGAR, marketscreener, stockanalysis). Result: **no data errors** (every flagged ISIN belongs to the correct entity), but **14 more name-quality fixes** were found in markets outside the earlier passes' scope, plus 7 renamed-and-reticker cases identified for a separate symbol-reconciliation pass. DB-wide ISIN name-mismatch **118 → 105**.

### Changed

- **9 abbreviation/stale names → current proper names** (markets outside the earlier scope): `00721`/HKEX (C FIN INT INV → China Financial International Investments Limited), `LAFARGEHOL`/CSE_MA (stale → Holcim Maroc), `BMCI`/`COSUMAR`/CSE_MA, Bursa ticker-codes `0260`/`0307`/`0368`/`0370` → full Berhad names, `VGE-ETF`/BSE_BW → Vunani Global Equity ETF.
- **5 ticker-unchanged company renames → current names**: `PMMCF` (Pampa Metals → Andina Copper Corp), `MLYCF` (American CuMo Mining → Multi-Metal Development Ltd), `SPKSJF` (Sparekassen Sjaelland-Fyn → SJF Bank A/S), `BBS-EQO` (Botswana Building Society → BBS Bank Limited), `AIVCB` (Al Arafa Investment → Concrete Fashion Group).
- `PRHI`/NASDAQ confirmed correct (Conifer Holdings rebranded to Presurance Holdings; our name already current — verified via SEC EDGAR).
- No ISIN changed; no row-count change (63,148).

### Note

- **7 renamed-AND-reticker cases deferred** for a future symbol-reconciliation pass (the row carries the old OTC/LSE symbol, so a name-only fix would leave a stale ticker): `LRGR`→FRTU (Fortun Holdings), `NBRY`→DAJL, `ADGL`→CODV, `BRGC`→NALG, `TEAH`→BJBJ, `RTGC`→BQHG, `REDC`→VZLA (Apertura Energy). `MEPET`/BIST left as-is (its rename to Break Mola has a pending revert).
- The residual ~105 mismatches are confirmed false positives: ADRs ("…UNSPON ADR"), GSE/utility preferred series (Freddie/Fannie, Connecticut Light & Power), confirmed renames where our name is already current, and ETFs whose clean names simply don't token-match OpenFIGI's cryptic abbreviation. **No data errors remain.**

### Verification

- No ISIN changed. `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `isin_validation_report` refreshed (105 mismatches). Bumps VERSION to 3.30.46.

## [3.30.45] - 2026-06-22

### Summary

**Name-quality refresh, pass 3: Indian (NSE) ETF scheme-code names → official fund names.** The third name-quality class from the OpenFIGI name-mismatch findings: 124 NSE ETFs whose stored `name` was an AMC scheme-code (e.g. `ICICIPRAMC - CASHIETF`, `KOTAKMAMC - NIFTY100EW`, `GROWWAMC - GROWWGOLD`) replaced with the official fund name, agent-verified against NSE/AMC/AMFI. **No ISIN changed** (these were correct-ISIN name issues). DB-wide ISIN name-mismatch **234 → 118**.

### Changed

- **124 NSE ETF names** → official fund names — e.g. `CASHIETF`→ICICI Prudential BSE Liquid Rate ETF, `NIFTY100EW`→Kotak Nifty 100 Equal Weight ETF, `GOLDBEES`→Nippon India ETF Gold BeES, `MON100`→Motilal Oswal NASDAQ 100 ETF, `BBETF0432`→Bharat Bond ETF - April 2032, the full Groww/Motilal Oswal/SBI/Edelweiss/Angel One ETF families, etc. Abbreviations expanded and OpenFIGI plan-suffix noise (`-RG`/`-DG`/`-IDCWD`) dropped.
- `AMBEY` and `FACT` left unchanged (`AMBEY`: conflicting rename-direction evidence, ticker matches stored name → kept; `FACT`: American-spelling variant of the same name).
- No row-count change (63,148) — ETFs carry unique ISINs, so no dedup churn.

### Verification

- No ISIN changed. `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `isin_validation_report` refreshed (118 mismatches). Bumps VERSION to 3.30.45.

### Note

The remaining 118 ISIN name-mismatches are OTC ADRs/preferred series, legitimate-rename cases already confirmed correct, and a handful of NSE ETFs whose now-clean names simply don't token-match OpenFIGI's cryptic abbreviation — i.e. residual false positives, not data errors. The OpenFIGI ISIN-validation campaign (identity correctness + name quality) is complete.

## [3.30.44] - 2026-06-22

### Summary

**Name-quality refresh from the OpenFIGI name-mismatch findings.** The 372 mismatches remaining after the ISIN-correctness triage (v3.30.42/43) were predominantly name-quality false positives (correct ISIN, abbreviated or stale name). This release fixes the two clear classes: **(1) ticker-abbreviation/code names → proper current legal names** (Saudi TADAWUL + Singapore SGX), and **(2) stale pre-rename names → current names** (Indian BSE_IN + Israeli TASE renames). 141 names updated, each agent-verified against authoritative sources (Saudi Exchange/argaam, SGX, BSE/screener.in, TASE/maya). **ISINs unchanged — these were correct-ISIN name issues.** DB-wide ISIN name-mismatch **372 → 234** (the remaining 234 are mostly Indian ETF scheme-code names, a separate class).

### Changed

- **TADAWUL (53) + SGX (38): ticker abbreviations/codes → proper company names** — e.g. `RIBL`→Riyad Bank, `MAADEN`→Saudi Arabian Mining Co. (Maaden), `EXTRA`→United Electronics Co., `BAHRI`→National Shipping Company of Saudi Arabia (Bahri), `SGX`→Singapore Exchange Limited, `UOB`→United Overseas Bank Limited, `OCBC Bank`→Oversea-Chinese Banking Corporation Limited.
- **BSE_IN + TASE: stale pre-rename names → current names** — e.g. `GZT` Gazit Globe→G City Ltd, `NALA` Nala Digital Commerce→Sade Real Estate-Y.S. Ltd, `CLBV` Clal Industries→Carmel Corp, `ABHIJIT`→Malt Land Distilleries, `COVIDH`→iSERA Lifesciences, `LKPFIN`→Gyftr, `NIBEORD`→Global Defence Industries, plus ~30 more Indian/Israeli micro-cap renames. Two typo fixes (`MAXHEIGHTS`, `PMTELELIN`).
- **6 left unchanged** (already correct, only spelling/spacing variants): `ALSL`, `ALUFLUOR`, `KGPETRO`, `UNIJOLL`, `ZBINTXPP`, `STCORP`.
- Side effect of the name normalization: `FSLUF` (First Ship Lease Trust OTC) consolidated as a cross-listing of `D8DU` (SGX) once their names matched. Primary tickers 63,149 → 63,148.

### Verification

- All names agent-verified against authoritative sources; no ISIN changed. `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `isin_validation_report` refreshed (234 mismatches). Bumps VERSION to 3.30.44.

## [3.30.43] - 2026-06-22

### Summary

**Triage tranche 2 of the OpenFIGI ISIN-mismatch queue — the remaining 347 same-jurisdiction mismatches.** Bucketed them: ~203 are confirmable false positives (the ISIN is correct, our `name` is a short ticker-abbreviation/scheme-code — e.g. all 52 TADAWUL rows `BAHRI`=National Shipping/`RIBL`=Riyad Bank, all 36 SGX abbreviations, ~95 NSE_IN `<AMC>-<code>` ETF rows). The remaining 144 genuine candidates were each externally verified (12 parallel agents + authoritative sources). The verification confirmed bucket B is overwhelmingly **correct ISINs** — dozens of legitimate renames (BSE/TASE micro-caps, `FLG`=NYCB→Flagstar, `PENG`=SMART Global→Penguin, `CIB`=Bancolombia→Grupo Cibest), ADRs, GSE preferred series, and ETF name-abbreviations. DB-wide ISIN mismatch **394 → 372**.

### Changed / Fixed

- **19 genuine wrong ISINs corrected** (recycled tickers — our name matches the current ticker but the ISIN was the *previous* holder's): 3 replaced with a verified issuer ISIN (`DIVY`→US8863647934, `JSP`→THA520010002, `LQPE`→US45259A5552) and 16 cleared where no sourced replacement applied or the correct ISIN already sits on the primary listing (`IFN`, `WMX`, `SIXD`, `TACO`, `TDI`, `MNR`, `OSG`, `GARA`, `IRET`, `JABS`, `ELCM`, `ISC`, `ELY`, `SSEZF`, `ENZN`, `TROLB`).
- **3 name errors fixed** where the ISIN was correct for the ticker but the name was wrong/swapped: `UMB`/BME (was "Kaldvik AS" → Umbrella Global Energy SA), `LETS-EQO`/`LLR-EQU` BSE_BW (Letshego/Letlole names were swapped; `LETS` sector also corrected to Financials).
- **11 legitimately-suppressed securities restored** — they had been hidden because the wrong rows held *their* ISINs: `FECM`, `JSI`, `OMAH`, `SANG`, `SENX`, `TROUF` (with their correct ISINs) and `GAUD`/`SOLR`/`JEMB`/`JIII`/`SNTH` (distinct ETFs, wrong shared ISIN cleared).
- **5 cascade duplicates cleared** — distinct ETFs that wrongly shared one ISIN with a sibling fund (`GAUD`/`SOLR`=GAIL GDR, `JEMB`/`JIII`=JSI's ISIN, `SNTH`=OMAH's ISIN).
- `SANG`/NASDAQ (Sangoma Technologies, Canadian co dual-listed on Nasdaq) added to `foreign_isin_reviewed.csv` (legitimate CA ISIN on a US listing).
- Primary tickers 63,138 → 63,149.

### Verification

- Every applied ISIN Luhn-valid, non-colliding, OpenFIGI `ID_ISIN`-confirmed (or authoritatively sourced where the security is too new for OpenFIGI). `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `isin_validation_report` refreshed (372 mismatches). Bumps VERSION to 3.30.43.

### Note

The remaining 372 mismatches are predominantly **name-quality** false positives (correct ISIN, abbreviated/stale name) rather than wrong ISINs — a separate stale-name refresh, not an ISIN-correctness issue.

## [3.30.42] - 2026-06-22

### Summary

**Triage tranche 1 of the OpenFIGI ISIN-mismatch queue (v3.30.41).** Of the 421 baseline mismatches, the 33 highest-confidence *cross-jurisdiction* cases (ISIN country-prefix foreign/implausible for the listing) were each externally verified (multi-agent + authoritative sources), applying the same-legal-entity test so legitimate renames are not mistaken for collisions. Result: 27 confirmed wrong ISINs corrected, 6 confirmed correct and kept. DB-wide ISIN mismatch count **421 → 394**.

### Changed / Fixed

- **17 wrong ISINs replaced** with the verified issuer ISIN (each Luhn-valid, collision-checked, and OpenFIGI `ID_ISIN` name-matched to our company): e.g. `STRK`/IDX (was MicroStrategy's US ISIN → `ID1000198708`), `EMCO`+`GLPL`/PSX (were Israeli ISINs → PK), the 6 Thai SET rows (were CA/IL/JP/KE/US ISINs → TH), `RCM`/ASX, `ZENA`+`GEOS`/NASDAQ, `RGT`/NYSE, `BNY`/NEO, `OPTI`/`OCEA`/`VTHPF`/OTC.
- **10 wrong ISINs cleared** where no authoritatively-sourced replacement applied — either the correct ISIN already sits on the security's primary listing (OTC shadows `PHYOF`/IXI·LSE, `SLFIF`/SLF·NYSE, `TCEYF`/TRP·NYSE, `TCAPF`/TCAP·LSE), the proposed replacement failed checksum (`PCFBF`), or none was found (`CITY`, `SSTPW`, `VHLUF`, `NXTG`, `SLGC`).
- **6 confirmed correct (kept) — rename traps, not collisions:** `REDC` (Red Capital → Apertura Energy), `ATAI` (atai Life Sciences → AtaiBeckley NV), `PTMGF` (Platinum Asset Mgmt → L1 Group), `CYJBY` (Cargotec → Hiab Oyj), and SGX `T14`/`VI2` (Chinese/Cayman dual-listings carrying their own home ISIN).
- **4 legitimately-suppressed listings restored** — they had been hidden because the wrong rows held *their* ISINs: `AQUEF` (Veloryx), `FRHYF` (Frontier Energy), `SATLF` (Zozo), `SDCCQ` (SmileDirectClub), each OpenFIGI-confirmed against its own correct ISIN.
- **4 redundant grey-market OTC shadows dropped** (`SUNFF`, `TRPEF`, `TRPPF`, `TRPRF`) — duplicate OTC tickers of Sun Life / TC Energy that only ever surfaced as separate rows because a wrong ISIN de-collided them; each company keeps one OTC shadow + its NYSE primary.
- Primary tickers 63,134 → 63,138.

### Verification

- Every applied ISIN is Luhn-valid, non-colliding, and OpenFIGI `ID_ISIN`-confirmed to map to our company. `scripts/validate_database.py`: 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. `data/reports/isin_validation_report.{json,md}` refreshed (mismatch 394). Bumps VERSION to 3.30.42.

## [3.30.41] - 2026-06-22

### Summary

**Free, complete, deterministic external identity check of the whole DB.** Adds an OpenFIGI `ID_ISIN` validator that maps every primary ticker's ISIN to OpenFIGI's view of that security and classifies identity consistency — the one per-row external validation that can cover all ISINs at zero cost. Detection only (review-only PR via weekly CI); nothing is auto-applied.

Baseline over **all 61,503 non-blank ISINs**: **match 59,388 (96.6%)**, **no_data 1,694 (2.8%, OpenFIGI coverage gaps — not errors)**, **mismatch 421 (0.68%)**. The 421 mismatches are review candidates where OpenFIGI resolves the ISIN to a security whose ticker **and** name both differ from ours — e.g. an Indonesian brewery carrying MicroStrategy's US ISIN, Pakistani stocks carrying Israeli ISINs, and ASX tickers carrying recycled-predecessor ISINs (Infigen, Odin Metals).

### Added

- **`scripts/build_isin_validation_report.py`** — OpenFIGI `ID_ISIN` validator. Classifies each ISIN as `match` / `mismatch` / `no_data`. Uses **name-consistency, not naive ticker equality** (OpenFIGI's ticker convention differs from local formats outside the US, which would mass-false-positive), with leading-zero normalization for Asian zero-padded codes, LSE slash stripping, and diacritic folding (`Frøy`==`FROY`). Incremental + compact: caches `{isin: verdict}` so re-runs only query new ISINs, keeps full detail only for mismatches; checkpoints every 2,000; `OPENFIGI_API_KEY` support with 429 backoff and a `FAILED` sentinel that leaves unqueryable ISINs uncached for retry (never crashes the run, never miscaches). Writes `data/reports/isin_validation_report.{json,md}` and emits `isin_issues_detected` to `$GITHUB_OUTPUT`.
- **`.github/workflows/isin-validation.yml`** — weekly cron (Mon 07:53 UTC) that runs the validator and opens a **review-only** PR when new mismatches appear. Uses the `OPENFIGI_API_KEY` secret if present (full DB in one run); keyless it caps per-run and converges over a few weeks, then stays incremental.
- **`tests/test_build_isin_validation_report.py`** — 13 pure-logic tests (no network) covering ticker/name normalization, diacritics, zero-padding, and the three verdict classes.
- Baseline report committed at `data/reports/isin_validation_report.{json,md}`.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,472 passed. Bumps VERSION to 3.30.41. No dataset rows changed (tooling + report only).

## [3.30.40] - 2026-06-21

### Summary

Resolves the re-ticker cases deferred from the v3.30.39 audit — verified against the dataset's own ISINs + external sources.

### Changed

- **3 stale-duplicate old tickers dropped** (the renamed/re-tickered successor already exists with the SAME ISIN, so the old is redundant): `INLOT`→`BYLOT` (Intralot → Bally's Intralot, ATHEX, ISIN GRS343313003), `FNTS`→`WILK` (Fantasy Network → Wilk Technologies, TASE, IL0002780109), `GDEP`→`GGEP` (Guardian Directed Equity Path, TSX, CA40090B1022). The successors `BYLOT`/`WILK`/`GGEP` are now the primaries.
- **`CPKL` re-keyed to `CRWN`** (NSE Kenya): the audit's "CPKL→CRWN" was correct — Crown Paints Kenya's actual NSE ticker is `CRWN` (ISIN KE0000000141), not the non-standard `CPKL`. This collides with the unrelated `CRWN` = Crown Place VCT (LSE); per the collision-safe rule the LSE listing keeps the `tickers.csv` primary and Crown Paints Kenya is retained in `core_listings` under its correct ticker. (My first reading mistook the suggestion for a hallucination because `CRWN` was already a different company in our data — it was not.)
- Primary tickers 63,135 → 63,134.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,459 passed. Bumps VERSION to 3.30.40.

## [3.30.39] - 2026-06-21

### Summary

**Full-DB error scan + DB-wide external-source validation.** Two passes: (1) a deterministic scan over **all 63,147 primary tickers** (every entry), and (2) a multi-agent external validation (field-by-field vs ≥2 independent sources, adversarially verified) of a **500-row stratified sample spanning all 81 markets**.

- Deterministic scan: the dataset is **structurally clean** (0 ISIN-checksum / duplicate-symbol / cross-name-ISIN-collision / invalid-sector / invalid-asset_type) — the 83 gates hold. Only 8 real issues: 7 UTF-8-mojibake ETF names + 1 blank country.
- External sample: **85.4% row-level correct** (Wilson95 82.0–88.2%; n=500) — lower than the prior 89.7% fixed-scope audit because this sample reaches every market incl. weak frontier exchanges and was stricter on name-currency/delisting. Error mix: name 29, sector 25, exists 11, isin 7, etf_category 4, asset_type 3, exchange 1.

### Changed / Fixed

- **7 mojibake ETF names** repaired (XETRA, mangled en-dash/ZWSP) + **AERO** (Grupo Aeroméxico) blank country → Mexico.
- **~27 stale/wrong names** corrected (e.g. `DVRE`→WEBs Real Estate XLRE, `INLOT`→Bally's Intralot, `BTV`→BlueRush, `GCB`→GCB Bank, `FNTS`→Wilk Technologies, several CN/KR/ETF names).
- **24 GICS `stock_sector`** + **4 `etf_category`** corrections (e.g. `EXITO`→Consumer Staples, `NEWSLV`→Commodity).
- **3 ISINs** corrected (`HAD`, `SGH`, `TRAN` — OpenFIGI `ID_ISIN` ticker-match verified). 3 further proposed ISINs (`VFDGROUP`, `2760`, `6832`) reverted as they conflicted with the official masterfile reference; 1 (`MTNR`/Rwanda) left as unverifiable.
- **9 dropped**: 3 non-equity mis-tags (`VRTVX` mutual fund, `FM1` RMBS note, `SKTPP` preferred) + 6 confirmed delisted/defunct (`IRAX`, `NBKE` EGX-delisted; `RDEMF`, `SANY6`, `FRDU`, `FGPRB`).
- Primary tickers 63,147 → 63,135.

### Note

Exhaustive per-row external validation of all 63k is not feasible (would need a credentialed bulk feed, not agents); the full deterministic scan covers every entry, the agent pass measures the DB-wide rate on a representative sample. `stock_sector` remains the dominant correctable error class. A handful of re-ticker cases (`CPKL`→`CRWN`, `GDEP`→`GGEP.F`) were noted but deferred (rename-rekey, separate pass).

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,459 passed. Bumps VERSION to 3.30.39.

## [3.30.38] - 2026-06-19

### Summary

**Operationalizes delisting detection** so the staleness sweeps (done manually in v3.30.32–37) now run automatically. No dataset rows change — this is tooling + a baseline report.

### Added

- `scripts/build_delisting_report.py` — diffs our primary stock holdings against **board-complete** official exchange masters and writes `data/reports/delisting_report.{json,md}` (candidates classified `delisted` / `suspended` / `master_absent`; emits `delisting_detected` for CI):
  - US — NASDAQ Trader `nasdaqlisted.txt` + `otherlisted.txt`
  - JP — JPX `data_j.xls` (all TSE boards)
  - AU — ASX `ListedCompanies.csv`
  - IN-NSE — `EQUITY_L.csv` + `SME_EQUITY_L.csv` (main + EMERGE SME → complete)
  - IN-BSE — `ListofScripData` API with authoritative Active/Suspended/Delisted status
  - A failed master fetch **skips that market** (with a recorded reason) and never emits candidates — a network/session hiccup can't falsely flag an exchange. Masters below a plausibility floor are treated as failed.
- `.github/workflows/delisting-report.yml` — weekly cron (Mondays 07:41 UTC) that runs the report and opens a **review-only** PR when the candidate set changes. Detection only; drops still go through the verified override/verify pipeline.
- `tests/test_build_delisting_report.py` (7 tests).
- Baseline `data/reports/delisting_report.{json,md}`: 5/5 markets reachable, **0 delisted**, 172 BSE-suspended (kept by policy), 82 `master_absent` (mostly kept renames + review items).

### Note

This deliberately does what the drift report avoids (a source-vs-dataset diff) but only where the master is **board-complete** and verified low-false-positive — the lesson from the India SME false-positive explosion (main-board-only masters are unsafe to diff).

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,459 passed (incl. 7 new). Bumps VERSION to 3.30.38.

## [3.30.37] - 2026-06-19

### Summary

**India delisting cleanup completed (NSE + BSE).** NSE came back clean once the SME EMERGE master was included (`EQUITY_L` + `SME_EQUITY_L` = full ~2,925-name universe → 0 delistings). BSE: the BSE `ListofScripData` API gives the full scrip list **with authoritative status** (Active / Suspended / Delisted), so candidates are classified by BSE's own status rather than guessed. Of our 2,634 BSE_IN stocks, 176 were absent from the Active list; BSE status splits them into **4 Delisted** + **172 Suspended**.

### Removed

- **4 BSE-confirmed delistings dropped** (official BSE `status=Delisted`): `KASHYAP`, `MAHAVEER`, `OSWALEA`, `QUINTEGRA`.
- **172 BSE-suspended kept** (BSE `status=Suspended` — trading halted but not formally delisted; consistent with keeping other suspended names like Grupo Elektra, they can resume). Flagged as a separate policy decision; left in by choice.
- Primary tickers 63,151 → 63,147.

### Note

This closes the deterministic non-US delisting work for the markets with obtainable board-complete masters (US, JP, AU, IN-NSE, IN-BSE). Remaining markets (no free complete master) stay on the symbol-changes feed + periodic completeness re-audits; durable automation (weekly CI master-diff) is the recommended next step.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.37.

## [3.30.36] - 2026-06-19

### Summary

**Non-US delisting cleanup — Tier 1 (Japan + Australia), where the official masters ARE complete.** Unlike NSE India (main-board-only), JPX `data_j.xls` covers all TSE boards (Prime/Standard/Growth) and ASX `ListedCompanies.csv` is the full ASX list, so the US-style master-diff is reliable here. Diffing our holdings: 14/2,866 TSE + 29/954 ASX absent. Each verified (adversarial before drop); renames/active kept.

### Removed

- **22 web+adversarially-confirmed delistings dropped** (12 TSE + 10 ASX), almost all 2026 take-privates/MBOs (post-knowledge-cutoff). TSE: `ITOCHU-SHOKUHIN` (Itochu TOB), `Medical Data Vision` (Nippon Life), `RAKSUL` (Goldman-backed MBO), `CANON ELECTRONICS`, `MANDOM`, `TOHO TITANIUM`, `NIPPON PALLET POOL`, `Fast Fitness Japan`, `SUNDAY`, `Maruc`, `Friendly`, `Wellbin`. ASX: `IFL` Insignia Financial (CC Capital, delisted 2026-04-28), `NSR` National Storage REIT (Brookfield/GIC $4bn), `Apiam`, `Diversified United Investment`, `Winsome`, `Robex`, `African Gold`, `Audio Pixels`, `Icandy`, `Emu` (deferred-settlement line). The two large ASX names independently web-confirmed.
- **Kept** the 16 renames (ASX re-tickers like `5EA`→`FEAM`, `ADG`→`AM3` — rename-dedup deferred) and 5 refuted (`6489`, ASX special-condition/`CA` lines).
- Primary tickers 63,169 → 63,151 (a few delisted names with a surviving non-JP/AU cross-listing keep their primary there).

### Note

Tier 2 (India NSE+SME / BSE) needs the session-gated SME EMERGE master; Tier 3 (markets with no complete master) stays on the symbol-changes feed + periodic completeness re-audits. The reliable rule: master-diff only where the master is board-complete.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.36.

## [3.30.35] - 2026-06-19

### Summary

**Non-US delistings (the third open item).** Confirmed recently-delisted non-US securities removed. A deterministic per-market master-diff (which worked for US via NASDAQ Trader) was found **not viable for non-US**: the free NSE India master (`EQUITY_L.csv`) covers only the main board and excludes the NSE EMERGE/SME platform, so a diff flagged 501 mostly-active SME companies (e.g. ALPEXSOLAR, AIMTRON — verified active) as false delistings. The SME master isn't freely accessible and most other markets have no free current-listing master. So non-US delistings were handled by **web-verifying the specific candidates already surfaced** (the v3.30.31 completeness "not found" set), not by mass master-diff.

### Removed

- **7 web-verified delistings dropped**: `444920` & `455910` (KOSDAQ SPACs liquidated/dissolved 2026-03/04), `202A` MAMEZO (TSE, EQT take-private), `5883` GT Holdings (TSE, JPX delisting decision 2026-03-12), `5BS` Sen Yue (SGX, compulsory acquisition, delisted 2026-04-30), `SCOIN` StandardCoin (Euronext Growth Oslo, liquidated late 2025), `DMYYU` dMY Squared (defunct SPAC unit).
- Kept candidates that are **suspended/conditional/unconfirmed** (not delisted): `ELEKTRA` (Grupo Elektra, BMV-suspended), `L38` AF Global (SGX take-private only conditional), `LOKOS` (unconfirmed), `NESB3` (suspended).
- Primary tickers 63,176 → 63,169.

### Note

A comprehensive non-US delisting sweep is not deterministically achievable without per-market SME-inclusive masters or a credentialed feed; the daily symbol-changes feed + periodic completeness re-audits remain the practical detector.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.35.

## [3.30.34] - 2026-06-19

### Summary

Cleanup of two items left open by v3.30.33: the 3 deferred rekeys and the contaminated-ISIN rows surfaced by the rename audit.

### Changed

- **Rekeys / leftover renames** (OpenFIGI `ID_ISIN`-verified): `OTH`→`NXB` (NextBoat Inc., re-keyed — new symbol wasn't in the data); `MYNZ` dropped as a stale duplicate of the already-present `QUCY` (Quantum Cyber N.V., same ISIN `NL0015000LC2`, now the primary; allowlisted as a Dutch-incorporated NASDAQ listing); `FITBP` dropped (Fifth Third Bancorp **preferred** — OpenFIGI `securityType=Preferred Stock`, out of Stock scope per v3.30.25, not a common-stock rename).
- **Contaminated ISINs corrected** (and the now-safe v3.30.33 dedup completed): `AHRT` (AH Realty Trust / Armada Hoffler) re-pointed from a defunct American-Healthcare-REIT pre-listing ISIN to its real `US04208T1088`; `ARIS` (Aris Mining) re-pointed from the Aris Mining **Holdings** subsidiary ISIN to the parent `CA04040Y1097`. With the surviving rows now correct, the stale old tickers `AHH` and `ARMN` were dropped. American Healthcare REIT remains correctly represented under `AHR` (`US3981823038`).
- Primary tickers 63,179 → 63,176.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.34.

## [3.30.33] - 2026-06-19

### Summary

**Rename dedup (the deferred half of v3.30.32) — and a correction of v3.30.29.** Each of the 44 alleged old→new ticker changes from the delisting pass was verified for *same-issuer* (vs SPAC recycling / ticker collisions), with an adversarial confirm. Only adversarially-confirmed true renames were acted on; the rest left untouched (e.g. `ARMN`/`ARIS`, `BBU`/`BBUC`, `CWEN-A`/`CWEN` refuted as distinct/contaminated; `SCVL`↛`SHOE`, `ASGN`↛Turkish `EFOR`, `ATON`↛Kenyan `ALP` left as collisions).

### Fixed

- **Corrects 4 v3.30.29 mistakes.** v3.30.29 treated four real reverse-merger/rebrand renames as wrong ISIN-collisions (blanked the new ticker's ISIN, kept the old). Verified via SEC-CIK + web that these are the **same legal entity renamed**: **urban-gro→Flash Sports & Media** (`UGRO`→`FLZH`, CIK 1706524), **Mawson Infrastructure→Big Digital Energy** (`MIGI`→`BGDE`), **Sharps Technology→SkyAI** (`STSS`→`SKYA`), **StableX→Fabric.AI** (`SBLX`→`FABC`). Restored the carried-through ISIN onto each new ticker (OpenFIGI `ID_ISIN`-confirmed) and dropped the stale old symbol.
- **14 stale renamed-away tickers dropped** (`UGRO`,`MIGI`,`STSS`,`SBLX`,`GLTO`,`GMRE`,`BLBX`,`HSPT`,`HYAC`,`IPOD`,`KLTO`,`SKBL`,`TBMC`,`TIVC`), each a confirmed same-issuer rename whose current ticker we already carry. **7 new-ticker ISINs set/restored** (`FLZH`,`BGDE`,`SKYA`,`FABC`,`CCAQ`,`GRML`,`KAZR`), all OpenFIGI-verified.
- Primary tickers 63,194 → 63,179.

### Note

3 rekeys whose new symbol isn't yet in our data (`FITBP`→`FITB.PRA`, `MYNZ`→`QUCY`, `OTH`→`NXB`) were deferred (ticker re-key, lower stakes). 3 proposed drops were adversarially **refuted and kept** (`ARMN`/`ARIS`, `BBU`/`BBUC`, `CWEN-A`/`CWEN` — distinct paired securities or identifier contamination on the surviving row, the latter a separate fixable issue).

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.33.

## [3.30.32] - 2026-06-19

### Summary

**US delisting-staleness cleanup.** The completeness audit surfaced a staleness class (securities our data still marks active that are no longer listed). Detected deterministically by diffing our US-exchange stocks against the **current NASDAQ Trader master**: 179 were absent. Each was classified (delisted/acquired vs renamed vs mis-tagged non-common vs active) and **every removal adversarially confirmed**; the 18 that still trade (e.g. `ABVE`→OTC `ABVEF`, `SGMO`) were kept.

### Removed

- **101 stale US listings dropped**: **73 delisted/acquired** + **28 mis-tagged warrants/units** (e.g. `ABPWW`, `*-UN` lines tagged as common stock). The acquisitions are real 2026 M&A (post-knowledge-cutoff) — independently web-verified for the large names: **Exact Sciences→Abbott** ($21B, delisted 2026-03-23), **Hologic→Blackstone/TPG** ($18B, 2026-04-07), **Coterra→Devon** (2026-05-07, now `DVN`), **Arcellx→Gilead**, **Aspen Insurance→Sompo**, plus liquidated SPACs (`AAM`). Companies with a surviving non-US cross-listing keep their primary there.
- Primary tickers 63,279 → 63,194.

### Note

The **renamed/re-ticker dedup was deliberately deferred**: 5 of the 38 auto-classified "renames" were actually the v3.30.29 ISIN-collision mixups of *distinct* companies (`SCVL`/Shoe Carnival↛`SHOE`, `UGRO`/Urban-gro↛`FLZH`, `STSS`/Sharps↛`SKYA`, `SBLX`/StableX↛`FABC`, `MIGI`/Mawson↛`BGDE`) — blindly dropping the "old" tickers would have deleted the very companies restored in v3.30.29. Rename dedup needs its own verification pass. Non-US delistings (outside the NASDAQ master's scope) also remain for a future pass.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.32.

## [3.30.31] - 2026-06-19

### Summary

**Per-market completeness audit + acted-on fixes.** A reproducible stratified sample of **≥5 stocks across all 79 markets** (385 stocks, seed 20260619) was verified field-by-field against ≥2 independent sources, with an adversarial refute pass. Finding: **name/country/sector are ~complete everywhere; the ISIN is the only systematic gap** (worst in Colombia/BVC, NEO, Qatar/QSE, Hungary, Mexico/BMV, TSX, Chile). 271/385 fully complete; the rest mostly missing-ISIN.

### Changed

- **45 verified ISINs filled/fixed** (37 previously-blank + 8 wrong), each gated by adversarial refute + checksum + OpenFIGI `ID_ISIN` ticker match. The gate rejected 18 agent-proposed ISINs (e.g. bond ISINs mis-mapped to `DTG`/`CPIP`/`1053P`, official-reference conflicts on `LEBEK`/`3595`/`6787`) — left blank rather than risk a wrong value.
- **Confirmed sector/country/name corrections** applied (incl. the identity fix `SZSE::001316` = Lubair Aviation, not "Zhejiang XiaSha").
- **2 stale-rename duplicates dropped**: `TV1`→`FVEN` (Foresight Ventures VCT) and `SLPE`→`PPET` (Patria Private Equity Trust) — the renamed successors already existed as primaries.
- Regenerated `identifiers_extended`/`listing_index` via `enrich_global_identifiers.py`.
- Primary tickers 63,288 → 63,279 (2 stale-rename drops + 7 same-company dedup merges from the ISIN fills, e.g. Mincon `MCON`/`MIO`).

### Note

The audit's 22 "not found" rows were re-verified rigorously: **none were phantoms** — the adversarial pass refuted every drop. They are renames (kept), genuinely active-but-obscure (kept), or **recently delisted/taken-private in 2026** (e.g. KOSDAQ SPACs `444920`/`455910`, `202A` MAMEZO buyout, Grupo Elektra suspension) — a *staleness* class deferred for a future delisting-status pass, not dropped here. Country flags conflicting with the ISIN-prefix convention were reverted (not real errors).

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.31.

## [3.30.30] - 2026-06-18

### Summary

**Correctness audit + acted-on fixes.** A reproducible stratified 300-row sample (seed 20260618; 150 recent additions + 150 base) was audited field-by-field against ≥2 independent sources, with an adversarial refute pass on every flagged error. **Measured correctness: 89.7% overall (Wilson95 85.7–92.6%); new additions 90.0% vs base 89.3% — the gap-closing campaign did not degrade quality.** Error distribution confirmed sector as the recurring lever, plus a residual closed-end-fund leak.

### Fixed

- **7 closed-end funds removed** that leaked into the v3.30.28 adds (`ETO`, `GAB`, `GUT`, `MGF`, `PSUS`, `PWRL`, `VVR`) — found via an OpenFIGI `securityType` sweep over all new additions. They evaded the name filters because they are named "… Trust" or carry no fund keyword (e.g. Pershing Square USA).
- **20 confirmed metadata corrections**: 7 `stock_sector` (e.g. `SLBT`→Health Care, `FRTT`→Materials, `YMT`→Information Technology), 3 ISIN (`QH`→US74841Q4073, `WZRD`, `SDIV` — each re-verified by checksum + OpenFIGI `ID_ISIN` ticker match), `BHST` country→Canada, 5 `etf_category` (bond/money-market ETFs mis-tagged Equity: `PZA`, `UTIP`, `FRXE`, `511850`, `CRPT11`), 3 stale names (`603337`→Jack Technology, `032300`→Korea Pharma, `000158`→Changshan Beiming).
- Regenerated `identifiers_extended`/`listing_index` via `enrich_global_identifiers.py` (the post-#90 path).
- Primary tickers 63,295 → 63,288.

### Note

A few audit flags were deferred as uncertain or involving exchange re-keys (`VGNT`/`AVLN` identity, `NGEN`/`ELOX`/`RMT` stale venue shadows). Many `country=US` flags on foreign ADRs are the ISIN-prefix/listing convention and were correctly not counted as errors.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,452 passed. Bumps VERSION to 3.30.30.

## [3.30.29] - 2026-06-18

### Summary

**Scope fix + ISIN backfill for the v3.30.28 US-domestic adds.** Two corrections in one release: (1) removes closed-end funds that wrongly slipped into v3.30.28, and (2) backfills verified US ISINs onto the blank common-stock rows.

### Fixed

- **194 closed-end funds removed** from the v3.30.28 additions. They evaded the v3.30.28 filter because CEFs are named "… Fund … Common Stock" / "Common Shares of Beneficial Interest" (no literal "Closed-End"), so they matched the common-stock pattern. Per the v3.30.25 scope policy CEFs are out of the Stock universe (abrdn/BlackRock/Blackstone/Eaton Vance/Nuveen/Gabelli/DoubleLine/Cohen & Steers funds).

### Changed

- **130 US ISINs backfilled** onto the v3.30.28 blank common-stock rows, each **triple-verified** (agent lookup + ISIN checksum + OpenFIGI `ID_ISIN` ticker match). The remaining rows stay ISIN-blank (no fabrication).
- The OpenFIGI/collision gate caught **5 wrong agent ISINs** that would have displaced real distinct companies — `BGDE`, `FABC`, `FLZH`, `SKYA`, and a Shoe Station/Shoe Carnival mixup — each had grabbed an existing security's ISIN (Mawson `MIGI`, StableX `SBLX`, Urban-gro `UGRO`, Sharps `STSS`, Shoe Carnival `SCVL`); blanked, and all five incumbents verified intact. 14 same-issuer collisions (BBVA/Barclays/HSBC/ING/Honda/Eni/Gerdau/Prudential/STMicro/etc.) correctly merged the new US line with the existing foreign pseudo-listing.
- ISIN coverage 97.0% → **97.3%** (backfilled ISINs + removed blank-ISIN CEFs).
- Primary tickers 63,397 → 63,295; NYSE 2,076 → 1,998 (CEFs were mostly NYSE).

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.29.

## [3.30.28] - 2026-06-18

### Summary

**Closes the broader US-domestic common-stock coverage gap** surfaced by the v3.30.27 ADR work. Diffing the official NASDAQ Trader master (`nasdaqlisted.txt` + `otherlisted.txt`) against our US-exchange holdings left 576 currently-listed common stocks we did not carry (after excluding warrants/units/rights/preferreds/notes and — per the v3.30.25 scope policy — closed-end funds). Because every symbol comes from the **current** master, "is it trading" is guaranteed; the only resolution needed was GICS sector (a 20-agent name-based classification pass).

### Changed

- **576 US-listed common stocks added**: 469 operating companies + 107 SPAC/blank-check shells (SPACs are already in the dataset, so included for consistency). New primaries where the symbol was globally free; collision-safe (core-listing only) where the symbol was held by another primary. `country=United States` per the ADR/US-listing convention; GICS sector assigned per company.
- **7 foreign ADR/registry stragglers** missed by v3.30.27 (different spelling "American Depos**i**tory" / "New York Registry Shares" / "ADRs representing") added with **triple-verified ISINs** (agent + checksum + OpenFIGI `ID_ISIN` ticker match): `ASML` (USN070592100, NY registry), `BZ` (Kanzhun), `AEG` (Aegon, post-redomicile ISIN), `NVX` (Novonix), `NCTY` (The9), `AGMB` (AgomAb), `QH` (Quhuo).
- ISINs left **blank** for the US-domestic adds (no free authoritative US-CUSIP source; never fabricated — backfill later) — this lowers headline ISIN coverage from 97.5% to **97.0%** by design.
- **11 name-merge-propagated ISINs cleared** (the rebuild propagates a same-name peer's ISIN onto a blank-ISIN add): 10 foreign (e.g. MAKO, EU) + `NHP` (had inherited the `NHPBP` preferred-series ISIN; cleared, and `NHPBP` restored intact — zero displacement).
- Primary tickers 63,096 → 63,397; NASDAQ 4,479 → 4,620; NYSE 1,933 → 2,076.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.28.

## [3.30.27] - 2026-06-18

### Summary

**Closes the "invisible" ADR coverage gap** — US-listed American Depositary Receipts of foreign issuers that had no ISIN anywhere in the dataset (undetectable from our own data; the prior v3.30.26 sweep could only find ADR ISINs we already held). Sized against an **external US-exchange master** (the official NASDAQ Trader `nasdaqlisted.txt` + `otherlisted.txt` symbol directories): of 308 ADRs in the master we held 180, leaving **113 missing**. A 113-candidate multi-agent resolution (58 agents) returned name/country/GICS/US-ADR-ISIN per ADR, with two independent ISIN safeguards: an adversarial refute pass **and** a deterministic OpenFIGI `ID_ISIN` ticker-match cross-check.

### Changed

- **112 US-listed ADR listings added** (1 candidate dropped as already-present under symbol normalization, `CIG.C`=`CIG-C`): 99 as new primaries (free symbols), 13 collision-safe (symbol held by another primary → core-listing only). Blue-chips closed include **ARM** (Arm Holdings), **ERIC** (Ericsson), **GSK**, **VOD** (Vodafone), **UL** (Unilever), **SHEL** (Shell), **TAK** (Takeda), **JD** (JD.com), **TCOM** (Trip.com), **RYAAY** (Ryanair), **NICE**, **GRFS** (Grifols), **ARGX** (argenx), **RELX**, **WPP**, **IHG**, **GFI** (Gold Fields), **NMR** (Nomura), **INFY** (Infosys), **OTLY** (Oatly).
- **90 of the added ISINs are triple-verified** (adversarial agent + checksum + OpenFIGI ticker match); **22 were left blank** rather than risk a wrong value (pending verified backfill). The OpenFIGI cross-check caught real errors the agent pass missed — e.g. `SHEL` was given the **defunct Royal Dutch Shell `RDS/A` ADR ISIN**, and `ASX` (ASE Technology) was given **ATN International's ISIN** — both correctly rejected.
- Primary tickers 63,019 → 63,096; NASDAQ 4,417 → 4,479; NYSE 1,905 → 1,933.
- Test invariant `test_depositary_and_cross_issuer_aliases_removed` updated: `ARM` is now a covered primary (Arm Holdings) with the safe alias `arm holdings` (the bare common-word `arm` remains filtered).

### Note

The broader 928 US-listed common stocks we don't hold are predominantly US-domestic small-caps/SPACs (a separate coverage-breadth question), not ADRs.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.27.

## [3.30.26] - 2026-06-18

### Summary

**US national-exchange listing-gap close.** A spot check of `SAP` revealed a class of securities held only on foreign venues / US-OTC whose actual US national-exchange listing was missing (e.g. the NYSE `SAP` ADR; `HWM` Howmet — an S&P 500 stock — had no US row at all). Detectable scope: 106 US-CUSIP (non-Reg-S) ISINs with no US-exchange listing. A 106-candidate multi-agent verification (44 agents, adversarial refute pass on every actionable verdict) classified each.

### Changed

- **15 US-exchange listings added** (all adversarially confirmed as currently trading on a US national exchange): `ABEV`, `DEO`, `HWM`, `MUFG`, `NOK`, `PBR`, `TEVA`, `VALE` as new primaries (globally free symbols, upgraded from London SETS placeholder codes like `0A2W`); `SAN`, `BCHT`, `NVS`, `RIO`, `SNY`, `SAP`, `TM` added collision-safe (symbol held by another primary → core-listing only, never displaces the incumbent).
- **2 ISIN corrections**: `NYSE::KEYS` (Keysight) carried a Swiss crypto-ETP ISIN (`CH0475986318` = 21Shares Bitwise on SIX) → corrected to `US49338L1035`; `NYSE::BHP` carried the AU ordinary ISIN → set to the US ADR ISIN `US0886061086` (consistent with the BABA/NVO/TSM convention), which also retired its `foreign_isin_reviewed` allowlist entry.
- 82 candidates confirmed correctly foreign-/OTC-only (e.g. ABB delisted its NYSE ADR in 2023; Adecco/Advantest/Aena/Aston Martin trade OTC Pink) and 7 confirmed delisted/defunct (e.g. Big Lots) — left unchanged.
- Primary tickers 63,026 → 63,019 (the 7 collision-safe ADRs move to core-listing scope; net of the symbol upgrades). NYSE 1,896 → 1,905.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.26.

## [3.30.25] - 2026-06-18

### Summary

**US non-equity scope sweep + re-audit corrections.** Removes non-common-stock instruments that were mis-scoped as `asset_type=Stock`, and applies the verified corrections from the fresh US re-audit (which measured ~89% US-stock correctness).

### Changed

- **254 non-common-stock instruments dropped** from the Stock scope (classified by OpenFIGI `securityType`): **110 closed-end / mutual funds**, **104 preferred stock / baby-bonds**, **34 units**, **2 rights**, and 4 further non-common lines. These are not common equity and do not belong in the Stock universe.
- **15 re-audit corrections** (each verified high-confidence): 3 `stock_sector` (`PAYX`→Industrials, `MBLY`→Consumer Discretionary, `CHHL`→Financials), 5 name renames (`GLTO`→Damora Therapeutics, `OGI`→Organigram Global, `RVLV`, `IVZ`, `PRMLF`→NexMetals Mining), 3 country (`HIG`→US, `RHCCF`→Canada, `ICOSF`→Italy), 3 ISIN ticker-collision fixes (`CRTO`→US2267181046, `OTGLF`→PLOPTTC00011, `HGKGF`→US7391971014), and 1 country-code infra fix (`AGMH` BVI/`VG`).
- Removed the 254 dropped listing_keys from `data/identifiers_extended.csv` + `data/listing_index.csv` (orphan-gate avoidance) and recomputed `data/identifier_summary.json`.
- Primary tickers 63,257 → 63,026; NASDAQ 4,506 → 4,417; NYSE 2,043 → 1,896.

### Note

US-stock correctness was re-measured at ~89.0% (up from 87.1% in v3.30.23 after the stale-ISIN/warrant/sector sweeps). Dropping the CEF/preferred/unit/right class addresses the remaining "non-equity in Stock scope" error class; the small per-exchange count shifts elsewhere (OTC/LSE/XETRA/ASX/TSX) reflect cross-listings reassigned as primary after the US drops.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.25.

## [3.30.24] - 2026-06-17

### Summary

US ISIN-resolve **wave 2** — completes the US stale-ISIN sweep that hit a session limit in v3.30.23. The agent pass was resumed (full 510/510 suspects verified) and the remaining suspects applied.

### Changed

- **183 further US ISIN corrections** (179 updated to the current sourced ISIN — more 2026 reverse-splits/redomiciles; 4 cleared as delisted) — the deferred ~198 from v3.30.23, now resolved.
- Allowlisted 3 Israeli-incorporated NASDAQ listings whose home IL ISIN was set (`RDCM` RADCOM, `SILC` Silicom, `TATT` TAT Technologies).
- Primary tickers 63,245 → 63,257.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.24.

## [3.30.23] - 2026-06-17

### Summary

**US-stock data-quality sweep** acting on the US audit (which measured ~87% US-stock correctness). Fixes stale ISINs, mis-tagged warrants, and sector misclassifications across US-listed stocks.

### Changed

- **293 ISIN corrections**: detected via an OpenFIGI validity sweep of all 5,465 US-listed ISINs (510 unresolvable → agent-verified). **269 updated** to the current sourced ISIN (stale after 2026 reverse-splits / redomiciles / rebrands — e.g. `KNSA` Bermuda→UK, `HIVE` rebrand, `AZ`/`AKAN`/`GP` reverse-splits), **17 cleared** as delisted/defunct (`AHL` acquired, `BBU` simplified), **+9** from the audit. (The agent pass hit a session limit at ~312/510 suspects; ~198 remain durably resumable.)
- **332 `stock_sector` corrections** (US-listed sector sweep — ~6% of 5,437 verified; +9 audit) — GICS verified from each company's business.
- **46 warrants dropped** from the Stock scope (OpenFIGI `securityType=Warrant`; W-suffix tickers e.g. `BZFDW`/`AISPW`) — removed from listings + identifiers.
- 4 country fixes (`UTL` Bermuda→US, `GNS` UK→Singapore, …) + 1 name. Allowlisted `ASND` (Ascendis, Danish-incorporated NASDAQ listing).
- Primary tickers 63,277 → 63,245; NASDAQ 4,535 → 4,498.

### Note

Measured US-stock correctness was 87.1% (NYSE/NASDAQ-listed and OTC both ~87%); errors dominated by stale-reverse-split ISINs + sector + warrant/SPAC tagging — all addressed here. ~198 US ISIN suspects remain (session-limit cutoff), durably resumable.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.23.

## [3.30.22] - 2026-06-17

### Summary

Coverage long-tail — **Australia (ASX)** via the official ASX listed-companies master. **389 new Australian securities.**

### Added

- **389 ASX securities** (43 new primaries + 346 collision-safe `coverage_expansion`) resolved **deterministically** from the official ASX `ListedCompanies` master: ISIN-embedded code (+ name fallback) → code/name, and **GICS sector from the master's GICS industry group**. Active-confirmed (present in the current master).
- Bumps VERSION to 3.30.22.

### Changed

- Primary tickers 63,259 → 63,277; core listings 57,351 → 57,694; ASX 1,290 → 1,308. **Zero displacement.**

### Note

The remaining global long-tail (Canada/Brazil/Thailand/Taiwan/deep frontier, several thousand more) is continuable with the same pipeline (official exchange masters where available, multi-agent verify otherwise, + the collision-safe `coverage_expansion` mechanism). New ASX rows carry ISIN+sector+name; FIGI/LEI backfill for them is a future deterministic pass.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.21] - 2026-06-17

### Summary

**Japan/Korea sector-correctness sweep** — the largest error class identified by the 2026-06-17 re-audit. **1,174 `stock_sector` corrections** on TSE/KRX/KOSDAQ.

### Changed

- **1,174 stock_sector corrections** (748 TSE, 130 KRX, 296 KOSDAQ): **82 deterministic** from the official JPX 33-industry classification (unambiguous 1:1 buckets — Banks/Insurance/Pharma/Utilities/Steel/Foods/Transport/Real Estate) + **1,092 multi-agent-verified** (correct GICS from each company's business; TSE used the JPX official industry as a strong hint, agents resolving the ambiguous sub-classifications, e.g. 建設業→Industrials/Consumer Discretionary/Real Estate, 電気機器→IT/Industrials, 化学 cosmetics→Consumer Staples). Conservative — only high/medium-confidence clear changes applied. Measured mixed-bucket error rate ~24% (TSE) / ~17% (Korea).
- Bumps VERSION to 3.30.21.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.20] - 2026-06-17

### Summary

Identifier backfill for the coverage-expansion additions + verified identity corrections from the post-expansion re-audit.

### Changed

- **FIGI backfill** via OpenFIGI for **3,236 of 3,240** new listing rows → FIGI coverage **64,311 → 67,546**.
- **LEI backfill** via GLEIF (ISIN→LEI) for **1,097** new rows → LEI coverage **17,489 → 18,586** (EU/Italy high, India low as expected — Indian issuers rarely have LEI).
- **10 verified identity corrections** from the 2026-06-17 fresh-sample re-audit: 7 ISIN (`TPEX:6650`→TW0006650005, `NYSE:REX`→US7616241052, `NGX:TOTAL`→NGTOTAL00001, `OTC:BRBL`/`OTC:ESWW`/`JSE:STXGVI`/`TSX:IGB`) and 3 names (`OTC:BIEI`→Nova Graphene Ballistics, `KOSDAQ:450940`→Yuanta 14 SPAC, `Bursa:7099`→Mayu Global Group). REX/TOTAL country auto-re-inferred from the corrected ISIN.
- Allowlisted 2 benign `official_name_mismatch` (TOTAL, 6650) after their ISIN corrections; cleared 1 cross-ISIN-collision FIGI (`XETRA:GTY`).

### Note — re-audit measurement (2026-06-17)

Fresh-sample re-audit (seed 20260617, 400 rows, 125-agent adversarial, 3.66M tokens): **overall 87.2 %** (Wilson 82.5–90.8 %); **new additions 95.3 %** — the coverage expansion is *cleaner* than the base DB and identity-clean. Sector is the dominant error class (concentrated in Asian markets) → next: Japan/Korea sector sweep.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed. Bumps VERSION to 3.30.20.

## [3.30.19] - 2026-06-16

### Summary

Coverage expansion — **adds Italy as a new exchange** (`Borsa Italiana` / Euronext Milan), then populates it. 277 verified currently-trading Italian common stocks (99 new primaries + 178 collision-safe).

### Added

- New exchange **`Borsa Italiana`** (Euronext Milan), country Italy/IT — registered in `EXCHANGE_ISIN_PREFIX` (IT).
- **277 verified-active Italian common stocks** via a 46-agent web pass (278 trade / 77 delisted / 7 non-stock from 362 candidates): **99 with globally-free tickers as new `Borsa Italiana` primaries** (Italian Sea Group/TISG, Sanlorenzo/SNL, Lottomatica/LTMC, SAES Getters/SG, NewPrinces/NWL, Equita/EQUI, Pharmanutra/PHN, Wiit/WIIT, S.S. Lazio/SSL, …) and **178 as collision-safe `coverage_expansion` rows**. Each with the correct Borsa Italiana ticker + GICS sector. Rejected delisted: Atlantia (→Mundys), Saras (→Vitol), UnipolSai, Autogrill, Tod's.

### Changed

- Primary tickers 63,159 → 63,258; core listings 57,073 → 57,350; ISIN coverage → 61,695; stock-sector coverage held at 99.8%. **Zero displacement** of existing tickers.
- Key-patched `identifiers_extended`/`listing_index`/`identifier_summary`; regenerated derived + entry-quality. Bumps VERSION to 3.30.19.

### Verification

- Borsa Italiana verification (46-agent web pass): 278 trades / 77 not_trading / 7 non-stock from 362.
- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.18] - 2026-06-16

### Summary

Coverage expansion — **rest of EU + the Japan/India/EU symbol-collision bucket**. Adds **331 new EU primary listings** and **1,423 collision-safe coverage-expansion rows** (Japan 834, EU 466, India 123) for verified-active securities whose ticker symbol is already the global primary for another security.

### Added

- **331 EU common stocks as new primary tickers** (LSE, Euronext, Nasdaq Stockholm/Copenhagen/Helsinki, SIX, BME, WSE, Oslo, ATHEX, Vienna, Prague, Budapest) — each verified currently trading on its home exchange by a 203-agent web pass that rejected **668 delisted/acquired** (CureVac, Steinhoff, Credit Suisse, Direct Line, CRH→NYSE-only, Just Eat Takeaway, Siemens Gamesa, …) + 52 non-stocks, with GICS sectors.
- **1,423 collision-safe `coverage_expansion` rows** for verified-active securities whose symbol is taken globally: **Japan 834** (Kikkoman, Mitsui Chemicals, Mitsui Kinzoku, …), **EU 466**, **India 123** (TCS, Indian Oil, Power Finance, GSK Pharma India, Sanofi-collision-set, …). They enrich `core_listings.csv`/`listings.csv` but never displace a `tickers.csv` primary.

### Changed

- Primary tickers 62,843 → 63,159; core listings 55,347 → 57,073; ISIN coverage → 61,596; stock-sector coverage held at 99.8%. **Zero displacement** of existing tickers.
- A self-correcting insert loop excluded same-company duplicates of existing OTC shadows (e.g. `TSE:8032` Japan Pulp & Paper = our `JPPPF`/OTC). Allowlisted 1 benign `official_name_mismatch` (`TSE:3184`).
- Key-patched `identifiers_extended`/`listing_index`/`identifier_summary`; regenerated derived + entry-quality. Bumps VERSION to 3.30.18.

### Verification

- EU verification (203-agent web pass): 902 trades / 668 not_trading / 52 non-stock from 1,622 candidates.
- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.17] - 2026-06-16

### Summary

Adds a **collision-safe coverage-expansion mechanism** and uses it for the 40 deferred German XETRA stocks. Real venue listings whose ticker symbol is already the global primary for a *different* security can now be added to `listings.csv` / `core_listings.csv` (collision-safe, keyed by `listing_key`) **without displacing** the existing `tickers.csv` primary.

### Added

- `scripts/rebuild_dataset.py`: new optional input `data/coverage_expansion_listings.csv`. Its rows flow through the full pipeline into `listings.csv` + `core_listings.csv`, but are demoted in the `tickers.csv` global ticker-symbol collision step (`primary_ticker_collision_sort_key`) so they can never win a symbol already owned by another security.
- **40 German XETRA common stocks** — the v3.30.14 deferred symbol-collision set: **Fuchs SE, Berentzen-Gruppe, Baader Bank, Springer Nature, Gelsenwasser, Grammer, SYZYGY, ALBA, Daldrup & Söhne, …** — each verified actively trading on XETRA, with GICS sectors. They appear in `core_listings.csv` / `listings.csv` but not `tickers.csv` (whose symbols FPE/BEZ/SPG/etc. belong to other securities globally).

### Changed

- Core listings 55,307 → 55,347; full listing rows 72,231 → 72,271. **Primary tickers unchanged (62,843) — zero displacement.**
- Key-patched `identifiers_extended`/`listing_index`/`identifier_summary`; regenerated derived + entry-quality reports. Bumps VERSION to 3.30.17.

### Verification

- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.16] - 2026-06-16

### Summary

Coverage expansion — **Japan**. Adds 24 currently-listed TSE common stocks with globally-free ticker codes. Most divvydiary Japanese candidates (835) have numeric/alphanumeric TSE codes that already exist as a global primary ticker on another exchange, so they are deferred to the collision-safe handling decision (same bucket as the DE-40).

### Added

- **24 new `TSE` common-stock rows** (e.g. Kokusai Electric, Alps Alpine, Pola Orbis Holdings, Iriso Electronics, Ferrotec, JCR Pharmaceuticals, T. Hasegawa, EneChange) — confirmed currently listed via the official JPX listed-issues master (`data_j.xls`, domestic-stock markets), with GICS sectors (20/24; 4 recent-IPO micro-caps left blank pending classification).
- Discovery: divvydiary JP ISINs → OpenFIGI ISIN→TSE code → matched to the **current JPX master** (859 active) → 24 with globally-free codes (835 code-collisions deferred).

### Changed

- Primary tickers 62,819 → 62,843; `TSE` 3,191 → 3,215. Zero displacement (free codes only).
- Key-patched `identifiers_extended`/`listing_index`/`identifier_summary`; regenerated derived + entry-quality reports. Bumps VERSION to 3.30.16.

### Verification

- JPX listed-issues master = authoritative for current-listing + code.
- `scripts/validate_database.py`: pass with 0/83 failed error gates. `check_readme_snapshot`: pass. `pytest -q`: 1,451 passed.

## [3.30.15] - 2026-06-16

### Summary

Coverage expansion — **India**. Adds **1,147 currently-listed NSE common stocks** that were missing, resolved deterministically against the official NSE equity master (authoritative symbol + active-listing confirmation), each with a GICS sector.

### Added

- **1,147 new `NSE_IN` common-stock rows** — e.g. Colgate Palmolive (India), Siemens India, Bosch, BASF India, Sanofi India, CESC, APL Apollo Tubes, Hindustan Aeronautics, Tata Elxsi, InterGlobe Aviation (IndiGo), Shriram Finance, Power Finance, Indian Oil. Each confirmed currently listed via the official NSE `EQUITY_L` master (symbol + ISIN + active series), with a GICS sector assigned by a multi-agent classification pass.
- Discovery funnel: divvydiary sitemap ISINs absent from our DB → 1,436 India candidates → matched against the **current NSE equity master** → 1,269 active → **1,147 with globally-free ticker symbols** (122 symbol-collisions deferred; 167 not in the current master → delisted/SME, skipped).

### Changed

- Primary tickers 61,672 → 62,819; `NSE_IN` roughly doubled; ISIN coverage 60,108 → 61,255; stock-sector coverage held at 99.9%.
- Manually key-patched `identifiers_extended.csv` / `listing_index.csv` / `identifier_summary.json` for the new listing keys; regenerated all derived + entry-quality reports.
- Bumps VERSION to 3.30.15 and refreshes data `_meta`, reports, README, and CHANGELOG.

### Verification

- The official NSE `EQUITY_L` master is authoritative for the symbol + current-listing status (no per-row guessing); zero displacement of existing tickers (free symbols only).
- `scripts/validate_database.py`: pass with 0/83 failed error gates.
- `scripts/check_readme_snapshot.py`: pass.
- `pytest tests/ -q`: 1,451 passed.

## [3.30.14] - 2026-06-16

### Summary

Coverage expansion (first slice). Using the divvydiary sitemap universe as a discovery source, adds 25 verified currently-trading German common stocks on XETRA that were missing from the database.

### Added

- **25 new XETRA common-stock rows** (e.g. STEMMER IMAGING, Funkwerk, Lechwerke, EUROKAI, DEAG, InVision, BAVARIA Industries, creditshelf, Halloren, ifa systems) — all confirmed actively trading on XETRA as of June 2026, each with a GICS sector. Verified via a 36-agent pass (web + OpenFIGI) that rejected take-privates/delisted/insolvent and non-XETRA (Freiverkehr-only) listings.
- Discovery funnel: 50,945 divvydiary ISINs absent from our DB → after stripping crypto/funds/ADRs + OpenFIGI `securityType` filtering → 287 German common-stock candidates on a German venue → **68 confirmed actively trading on XETRA** (154 trade only on Frankfurt/Freiverkehr, 64 delisted/acquired, 1 non-stock) → 28 with globally-free ticker symbols → 25 promoted (3 filtered by the pipeline's entry-quality cleaning).

### Changed

- Primary tickers 61,647 → 61,672; XETRA 2,256 → 2,281; ISIN coverage 60,083 → 60,108.
- Manually key-patched `identifiers_extended.csv` / `listing_index.csv` / `identifier_summary.json` for the new listing keys; regenerated the entry-quality report and all derived reports.
- Allowlisted 2 benign `official_name_mismatch` warns surfaced from the v3.30.13 rebrands (`NYSE::OTH` NextBoat, `OTC::NMKCP` Niagara Mohawk Power) — verified current names; official SEC/OTC reference lags.
- Bumps VERSION to 3.30.14 and refreshes data `_meta`, reports, README, and CHANGELOG.

### Not added (deferred)

- 40 verified-active XETRA stocks whose Xetra ticker symbol is already the global primary for a different security (e.g. Fuchs `FPE` = a US ETF, Berentzen `BEZ` = ASX, Springer Nature `SPG` = Simon Property Group) — the one-symbol-per-global-ticker design cannot add them as primary without displacing existing securities; deferred pending a collision-safe handling decision.

### Verification

- `scripts/check_readme_snapshot.py`: pass.
- `scripts/validate_database.py`: pass with 0/83 failed error gates.
- `pytest tests/ -q`: 1,451 passed.

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
