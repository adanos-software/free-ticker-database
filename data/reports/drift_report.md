# Drift / freshness report

Generated: 2026-08-01T16:50:53Z
Dataset built_at: 2026-08-01T16:49:58Z (0.0 days ago; threshold 45.0)
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
- expected_missing_primary_isin: 799
- missing_etf_category: 122
- missing_stock_sector: 46
- source_gap_rows: 7393

## Quality regressions: 4
- source_gap_rows: 7226 -> 7393 (+167)
- expected_missing_primary_isin: 771 -> 799 (+28)
- missing_stock_sector: 42 -> 46 (+4)
- missing_etf_category: 120 -> 122 (+2)

## Official recall regressions: 10
- NASDAQ official_recall_missing: 1167 -> 1176 (+9)
- NASDAQ collision_adjusted_recall_missing: 1105 -> 1113 (+8)
- SET official_recall_missing: 399 -> 401 (+2)
- SET collision_adjusted_recall_missing: 53 -> 54 (+1)
- SGX official_recall_missing: 149 -> 157 (+8)
- SGX collision_adjusted_recall_missing: 9 -> 15 (+6)
- TADAWUL official_recall_missing: 221 -> 222 (+1)
- TADAWUL collision_adjusted_recall_missing: 4 -> 5 (+1)
- TWSE official_recall_missing: 117 -> 121 (+4)
- TWSE collision_adjusted_recall_missing: 58 -> 59 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
