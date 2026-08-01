# Drift / freshness report

Generated: 2026-08-01T16:39:03Z
Dataset built_at: 2026-08-01T16:34:14Z (0.0 days ago; threshold 45.0)
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
- allowed_warn_rows: 20
- expected_missing_primary_isin: 771
- missing_etf_category: 120
- missing_stock_sector: 42
- source_gap_rows: 7226

## Quality regressions: 2
- missing_stock_sector: 31 -> 42 (+11)
- missing_etf_category: 104 -> 120 (+16)

## Official recall regressions: 33
- BHB official_recall_missing: 12 -> 13 (+1)
- BHB collision_adjusted_recall_missing: 3 -> 4 (+1)
- Borsa Italiana collision_adjusted_recall_missing: 835 -> 837 (+2)
- Euronext collision_adjusted_recall_missing: 372 -> 375 (+3)
- HKEX official_recall_missing: 178 -> 179 (+1)
- HKEX collision_adjusted_recall_missing: 94 -> 95 (+1)
- IDX collision_adjusted_recall_missing: 25 -> 26 (+1)
- KOSDAQ official_recall_missing: 247 -> 248 (+1)
- KOSDAQ collision_adjusted_recall_missing: 247 -> 248 (+1)
- KRX official_recall_missing: 323 -> 329 (+6)
- KRX collision_adjusted_recall_missing: 320 -> 326 (+6)
- LSE official_recall_missing: 4601 -> 4704 (+103)
- LSE collision_adjusted_recall_missing: 3494 -> 3581 (+87)
- NASDAQ official_recall_missing: 1091 -> 1167 (+76)
- NASDAQ collision_adjusted_recall_missing: 1030 -> 1105 (+75)
- NYSE official_recall_missing: 1875 -> 1928 (+53)
- NYSE collision_adjusted_recall_missing: 1375 -> 1426 (+51)
- NYSE ARCA official_recall_missing: 123 -> 127 (+4)
- NYSE ARCA collision_adjusted_recall_missing: 92 -> 95 (+3)
- NYSE MKT official_recall_missing: 82 -> 84 (+2)
- NYSE MKT collision_adjusted_recall_missing: 54 -> 55 (+1)
- OTC official_recall_missing: 4248 -> 4249 (+1)
- OTC collision_adjusted_recall_missing: 4210 -> 4211 (+1)
- PSE official_recall_missing: 291 -> 293 (+2)
- PSE collision_adjusted_recall_missing: 109 -> 112 (+3)
- PSX collision_adjusted_recall_missing: 190 -> 191 (+1)
- SEM official_recall_missing: 0 -> 1 (+1)
- SEM collision_adjusted_recall_missing: 0 -> 1 (+1)
- SET collision_adjusted_recall_missing: 51 -> 53 (+2)
- TSX collision_adjusted_recall_missing: 8 -> 9 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
