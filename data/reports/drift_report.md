# Drift / freshness report

Generated: 2026-07-08T04:36:28Z
Dataset built_at: 2026-07-08T04:35:56Z (0.0 days ago; threshold 45.0)
**drift_detected: False**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 10}

## Blocked/manual rename review rows: 10
- RAAQ -> IQMX (Iqm Quantum Computers Oyj, 2026-07-02): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- SSSS -> NSLR (Neostellar Capital Corp, 2026-07-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- QH -> QHUOY (Quhuo Ltd., 2026-04-02): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::QH
- KBFR -> LVROF (Lavoro Ltd., 2026-02-23): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NYSE ARCA::KBFR
- ABP -> ABPO (Abpro Holdings Inc, 2026-02-20): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: Borsa Italiana::ABP
- OPT -> OPTEY (Opthea Ltd., 2025-11-20): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: LSE::OPT
- PET -> PETXQ (Wag! Group Co., 2025-07-29): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::PET|LSE::PET
- SUP -> SSUP (Superior Industries International Inc, 2025-06-25): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: LSE::SUP
- WW -> WGHTQ (Ww International Inc, 2025-05-15): manual: source exchange scope is not mapped to a safe listing-keyed apply path

## Quality indicators (release-gate info counts)
- allowed_warn_rows: 75
- expected_missing_primary_isin: 621
- missing_etf_category: 0
- missing_stock_sector: 1
- source_gap_rows: 6272

## Quality regressions: 0

## Official recall regressions: 0

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
