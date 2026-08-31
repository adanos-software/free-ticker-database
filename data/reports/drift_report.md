# Drift / freshness report

Generated: 2026-08-31T15:13:02Z
Dataset built_at: 2026-08-31T14:02:51Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 21}

## Blocked/manual rename review rows: 21
- ISSC -> IA (Innovative Solutions & Support Inc, 2026-08-18): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- EQR -> VRMK (Vivmark Residential, 2026-08-18): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- NCL -> NCLX (Northann Corp, 2026-08-13): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: FSX::NCL|NYSE::NCL|SET::NCL|WSE::NCL|XSTU::NCL
- GV -> GVHGF (Visionary Holdings Inc, 2026-07-31): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::GV
- HOTH -> RKTO (Rocket One Inc, 2026-05-28): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::RKTO
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- KFS -> KWY (Kingsway Corp, 2026-05-20): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NYSE::KWY
- IINN -> QTEX (Qtrex Quantum Ltd., 2026-05-20): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::QTEX
- FIHL -> PLGO (Pelagos Insurance Capital Ltd., 2026-05-12): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NYSE::PLGO
- ETHM -> DYNC (Dynamix Corp, 2026-05-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- ZGM -> ZTG (Zenta Group Co Ltd., 2026-04-14): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::ZTG
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
- allowed_warn_rows: 32
- expected_missing_primary_isin: 752
- missing_etf_category: 46
- missing_stock_sector: 1298
- source_gap_rows: 11455

## Quality regressions: 3
- expected_missing_primary_isin: 690 -> 752 (+62)
- missing_stock_sector: 1284 -> 1298 (+14)
- missing_etf_category: 36 -> 46 (+10)

## Official recall regressions: 22
- AMS official_recall_missing: 230 -> 236 (+6)
- AMS collision_adjusted_recall_missing: 53 -> 58 (+5)
- BSE_IN official_recall_missing: 2435 -> 2467 (+32)
- BSE_IN collision_adjusted_recall_missing: 590 -> 617 (+27)
- Borsa Italiana official_recall_missing: 2653 -> 2658 (+5)
- Borsa Italiana collision_adjusted_recall_missing: 797 -> 800 (+3)
- FSX official_recall_missing: 10019 -> 10101 (+82)
- FSX collision_adjusted_recall_missing: 6074 -> 6150 (+76)
- HKEX official_recall_missing: 159 -> 160 (+1)
- HKEX collision_adjusted_recall_missing: 89 -> 90 (+1)
- KOSDAQ official_recall_missing: 223 -> 226 (+3)
- KOSDAQ collision_adjusted_recall_missing: 220 -> 223 (+3)
- KRX official_recall_missing: 145 -> 148 (+3)
- KRX collision_adjusted_recall_missing: 131 -> 134 (+3)
- LSE official_recall_missing: 4265 -> 4285 (+20)
- LSE collision_adjusted_recall_missing: 3486 -> 3498 (+12)
- NASDAQ official_recall_missing: 1037 -> 1049 (+12)
- NASDAQ collision_adjusted_recall_missing: 977 -> 988 (+11)
- NYSE collision_adjusted_recall_missing: 1336 -> 1339 (+3)
- OTC official_recall_missing: 3657 -> 3658 (+1)
- XETRA official_recall_missing: 990 -> 1000 (+10)
- XETRA collision_adjusted_recall_missing: 285 -> 292 (+7)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
