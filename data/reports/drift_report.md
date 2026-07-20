# Drift / freshness report

Generated: 2026-07-20T10:22:48Z
Dataset built_at: 2026-07-20T09:45:07Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 10}

## Blocked/manual rename review rows: 10
- RAAQ -> IQMX (Iqm Quantum Computers Oyj, 2026-07-02): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- SSSS -> NSLR (Neostellar Capital Corp, 2026-07-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- QH -> QHUOY (Quhuo Ltd., 2026-04-02): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::QH
- KBFR -> LVROF (Lavoro Ltd., 2026-02-23): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NYSE ARCA::KBFR
- ABP -> ABPO (Abpro Holdings Inc, 2026-02-20): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: Borsa Italiana::ABP
- OPT -> OPTEY (Opthea Ltd., 2025-11-20): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- PET -> PETXQ (Wag! Group Co., 2025-07-29): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::PET|LSE::PET
- SUP -> SSUP (Superior Industries International Inc, 2025-06-25): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: LSE::SUP
- WW -> WGHTQ (Ww International Inc, 2025-05-15): manual: source exchange scope is not mapped to a safe listing-keyed apply path

## Quality indicators (release-gate info counts)
- allowed_warn_rows: 74
- expected_missing_primary_isin: 752
- missing_etf_category: 93
- missing_stock_sector: 21
- source_gap_rows: 6886

## Quality regressions: 4
- source_gap_rows: 6302 -> 6886 (+584)
- expected_missing_primary_isin: 639 -> 752 (+113)
- missing_stock_sector: 1 -> 21 (+20)
- missing_etf_category: 0 -> 93 (+93)

## Official recall regressions: 25
- ADX official_recall_missing: 37 -> 38 (+1)
- ADX collision_adjusted_recall_missing: 5 -> 6 (+1)
- BATS official_recall_missing: 328 -> 336 (+8)
- BIST official_recall_missing: 23 -> 29 (+6)
- BIST collision_adjusted_recall_missing: 3 -> 9 (+6)
- BK official_recall_missing: 36 -> 38 (+2)
- BK collision_adjusted_recall_missing: 9 -> 11 (+2)
- BME official_recall_missing: 0 -> 73 (+73)
- BME collision_adjusted_recall_missing: 0 -> 61 (+61)
- BSE_IN official_recall_missing: 2407 -> 2488 (+81)
- BSE_IN collision_adjusted_recall_missing: 522 -> 592 (+70)
- BVB official_recall_missing: 273 -> 275 (+2)
- CSE_LK official_recall_missing: 8 -> 10 (+2)
- CSE_LK collision_adjusted_recall_missing: 8 -> 10 (+2)
- CSE_MA official_recall_missing: 49 -> 80 (+31)
- CSE_MA collision_adjusted_recall_missing: 11 -> 19 (+8)
- DFM official_recall_missing: 25 -> 26 (+1)
- DFM collision_adjusted_recall_missing: 9 -> 10 (+1)
- NASDAQ official_recall_missing: 1051 -> 1074 (+23)
- NASDAQ collision_adjusted_recall_missing: 1004 -> 1021 (+17)
- NEO official_recall_missing: 250 -> 257 (+7)
- NEO collision_adjusted_recall_missing: 162 -> 171 (+9)
- NYSE ARCA official_recall_missing: 121 -> 122 (+1)
- XETRA official_recall_missing: 872 -> 1419 (+547)
- XETRA collision_adjusted_recall_missing: 99 -> 561 (+462)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
