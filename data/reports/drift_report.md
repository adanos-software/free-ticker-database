# Drift / freshness report

Generated: 2026-08-24T08:09:21Z
Dataset built_at: 2026-08-21T07:18:05Z (3.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 20}

## Blocked/manual rename review rows: 20
- ISSC -> IA (Innovative Solutions & Support Inc, 2026-08-18): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- EQR -> VRMK (Vivmark Residential, 2026-08-18): manual: source exchange scope is not mapped to a safe listing-keyed apply path
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
- allowed_warn_rows: 81
- expected_missing_primary_isin: 690
- missing_etf_category: 36
- missing_stock_sector: 1284
- source_gap_rows: 11460

## Quality regressions: 0

## Official recall regressions: 39
- AMS official_recall_missing: 228 -> 230 (+2)
- AMS collision_adjusted_recall_missing: 52 -> 53 (+1)
- BIST official_recall_missing: 36 -> 40 (+4)
- BIST collision_adjusted_recall_missing: 15 -> 19 (+4)
- BK official_recall_missing: 38 -> 39 (+1)
- BSE_IN official_recall_missing: 2406 -> 2435 (+29)
- BSE_IN collision_adjusted_recall_missing: 563 -> 590 (+27)
- Borsa Italiana official_recall_missing: 2647 -> 2653 (+6)
- Borsa Italiana collision_adjusted_recall_missing: 792 -> 797 (+5)
- CSE_LK official_recall_missing: 11 -> 13 (+2)
- CSE_LK collision_adjusted_recall_missing: 11 -> 13 (+2)
- Euronext official_recall_missing: 666 -> 673 (+7)
- Euronext collision_adjusted_recall_missing: 300 -> 308 (+8)
- FSX official_recall_missing: 0 -> 10019 (+10019)
- FSX collision_adjusted_recall_missing: 0 -> 6074 (+6074)
- KOSDAQ official_recall_missing: 221 -> 223 (+2)
- KOSDAQ collision_adjusted_recall_missing: 218 -> 220 (+2)
- KRX official_recall_missing: 142 -> 145 (+3)
- KRX collision_adjusted_recall_missing: 128 -> 131 (+3)
- LSE official_recall_missing: 4240 -> 4265 (+25)
- LSE collision_adjusted_recall_missing: 3467 -> 3486 (+19)
- MSX collision_adjusted_recall_missing: 3 -> 4 (+1)
- NASDAQ official_recall_missing: 1034 -> 1037 (+3)
- NEO official_recall_missing: 227 -> 233 (+6)
- NEO collision_adjusted_recall_missing: 163 -> 167 (+4)
- NSE_IN official_recall_missing: 688 -> 871 (+183)
- NSE_IN collision_adjusted_recall_missing: 453 -> 481 (+28)
- SET official_recall_missing: 169 -> 170 (+1)
- SGX official_recall_missing: 135 -> 136 (+1)
- SGX collision_adjusted_recall_missing: 13 -> 14 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
