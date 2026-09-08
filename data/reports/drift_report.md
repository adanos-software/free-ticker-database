# Drift / freshness report

Generated: 2026-09-08T09:29:57Z
Dataset built_at: 2026-09-08T09:13:01Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 21}

## Blocked/manual rename review rows: 21
- ISSC -> IA (Innovative Solutions & Support Inc, 2026-08-18): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- EQR -> VRMK (Vivmark Residential, 2026-08-18): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- NCL -> NCLX (Northann Corp, 2026-08-13): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: FSX::NCL|NYSE::NCL|SET::NCL|WSE::NCL|XSTU::NCL
- GV -> GVHGF (Visionary Holdings Inc, 2026-07-31): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::GV
- HOTH -> RKTO (Rocket One Inc, 2026-05-28): manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- KFS -> KWY (Kingsway Corp, 2026-05-20): manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first
- IINN -> QTEX (Qtrex Quantum Ltd., 2026-05-20): manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first
- FIHL -> PLGO (Pelagos Insurance Capital Ltd., 2026-05-12): manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first
- ETHM -> DYNC (Dynamix Corp, 2026-05-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- ZGM -> ZTG (Zenta Group Co Ltd., 2026-04-14): manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first
- CIGL -> YOOV (Concorde International Group Ltd., 2026-04-13): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::YOOV
- CAPT -> CPTAF (Captivision Inc, 2026-04-08): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::CAPT|TSXV::CAPT
- QH -> QHUOY (Quhuo Ltd., 2026-04-02): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::QH|SET::QH
- KBFR -> LVROF (Lavoro Ltd., 2026-02-23): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NYSE ARCA::KBFR
- ABP -> ABPO (Abpro Holdings Inc, 2026-02-20): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: Borsa Italiana::ABP
- OPT -> OPTEY (Opthea Ltd., 2025-11-20): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- EBR -> AXIAY (Axia Energia SA, 2025-11-10): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::EBR
- PET -> PETXQ (Wag! Group Co., 2025-07-29): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::PET|LSE::PET|TSX::PET
- SUP -> SSUP (Superior Industries International Inc, 2025-06-25): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: LSE::SUP
- WW -> WGHTQ (Ww International Inc, 2025-05-15): manual: source exchange scope is not mapped to a safe listing-keyed apply path

## Quality indicators (release-gate info counts)
- allowed_warn_rows: 31
- expected_missing_primary_isin: 768
- missing_etf_category: 56
- missing_stock_sector: 1302
- source_gap_rows: 11356

## Quality regressions: 0

## Official recall regressions: 16
- BIST official_recall_missing: 40 -> 44 (+4)
- BIST collision_adjusted_recall_missing: 19 -> 22 (+3)
- Borsa Italiana official_recall_missing: 2658 -> 2659 (+1)
- Borsa Italiana collision_adjusted_recall_missing: 800 -> 801 (+1)
- Euronext official_recall_missing: 672 -> 678 (+6)
- Euronext collision_adjusted_recall_missing: 308 -> 314 (+6)
- ISE official_recall_missing: 6 -> 7 (+1)
- NSE_IN official_recall_missing: 871 -> 1000 (+129)
- NSE_IN collision_adjusted_recall_missing: 481 -> 538 (+57)
- OSL official_recall_missing: 12 -> 13 (+1)
- SET official_recall_missing: 170 -> 171 (+1)
- SET collision_adjusted_recall_missing: 37 -> 38 (+1)
- SGX official_recall_missing: 136 -> 140 (+4)
- SGX collision_adjusted_recall_missing: 14 -> 17 (+3)
- TADAWUL official_recall_missing: 215 -> 216 (+1)
- TADAWUL collision_adjusted_recall_missing: 5 -> 6 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
