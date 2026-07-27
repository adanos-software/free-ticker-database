# Drift / freshness report

Generated: 2026-07-27T10:52:33Z
Dataset built_at: 2026-07-27T10:29:28Z (0.0 days ago; threshold 45.0)
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
- allowed_warn_rows: 21
- expected_missing_primary_isin: 771
- missing_etf_category: 104
- missing_stock_sector: 31
- source_gap_rows: 7258

## Quality regressions: 4
- source_gap_rows: 6886 -> 7258 (+372)
- expected_missing_primary_isin: 752 -> 771 (+19)
- missing_stock_sector: 21 -> 31 (+10)
- missing_etf_category: 93 -> 104 (+11)

## Official recall regressions: 29
- AMS official_recall_missing: 351 -> 360 (+9)
- AMS collision_adjusted_recall_missing: 56 -> 64 (+8)
- BIST official_recall_missing: 29 -> 31 (+2)
- BIST collision_adjusted_recall_missing: 9 -> 11 (+2)
- BME official_recall_missing: 73 -> 75 (+2)
- BME collision_adjusted_recall_missing: 61 -> 63 (+2)
- BSE_IN official_recall_missing: 2488 -> 2501 (+13)
- BSE_IN collision_adjusted_recall_missing: 592 -> 608 (+16)
- Borsa Italiana official_recall_missing: 2624 -> 2640 (+16)
- Borsa Italiana collision_adjusted_recall_missing: 821 -> 835 (+14)
- CSE_LK official_recall_missing: 10 -> 11 (+1)
- CSE_LK collision_adjusted_recall_missing: 10 -> 11 (+1)
- CSE_MA official_recall_missing: 80 -> 81 (+1)
- CSE_MA collision_adjusted_recall_missing: 19 -> 20 (+1)
- Euronext official_recall_missing: 1039 -> 1042 (+3)
- Euronext collision_adjusted_recall_missing: 367 -> 372 (+5)
- HKEX official_recall_missing: 127 -> 178 (+51)
- HKEX collision_adjusted_recall_missing: 44 -> 94 (+50)
- IDX official_recall_missing: 264 -> 269 (+5)
- IDX collision_adjusted_recall_missing: 20 -> 25 (+5)
- KOSDAQ official_recall_missing: 246 -> 247 (+1)
- KOSDAQ collision_adjusted_recall_missing: 246 -> 247 (+1)
- KRX official_recall_missing: 312 -> 323 (+11)
- KRX collision_adjusted_recall_missing: 309 -> 320 (+11)
- NASDAQ official_recall_missing: 1074 -> 1091 (+17)
- NASDAQ collision_adjusted_recall_missing: 1021 -> 1030 (+9)
- NYSE ARCA official_recall_missing: 122 -> 123 (+1)
- XETRA official_recall_missing: 1419 -> 1427 (+8)
- XETRA collision_adjusted_recall_missing: 561 -> 573 (+12)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
