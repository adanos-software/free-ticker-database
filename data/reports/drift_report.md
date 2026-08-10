# Drift / freshness report

Generated: 2026-08-10T08:54:52Z
Dataset built_at: 2026-08-10T08:17:05Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 17}

## Blocked/manual rename review rows: 17
- GV -> GVHGF (Visionary Holdings Inc, 2026-07-31): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::GV
- HOTH -> RKTO (Rocket One Inc, 2026-05-28): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::RKTO
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- KFS -> KWY (Kingsway Corp, 2026-05-20): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NYSE::KWY
- IINN -> QTEX (Qtrex Quantum Ltd., 2026-05-20): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::QTEX
- FIHL -> PLGO (Pelagos Insurance Capital Ltd., 2026-05-12): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NYSE::PLGO
- ETHM -> DYNC (Dynamix Corp, 2026-05-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- ZGM -> ZTG (Zenta Group Co Ltd., 2026-04-14): blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: NASDAQ::ZTG
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
- allowed_warn_rows: 198
- expected_missing_primary_isin: 1118
- missing_etf_category: 909
- missing_stock_sector: 2655
- source_gap_rows: 20956

## Quality regressions: 4
- source_gap_rows: 6445 -> 20956 (+14511)
- expected_missing_primary_isin: 777 -> 1118 (+341)
- missing_stock_sector: 48 -> 2655 (+2607)
- missing_etf_category: 122 -> 909 (+787)

## Official recall regressions: 5
- BATS official_recall_missing: 336 -> 340 (+4)
- Borsa Italiana official_recall_missing: 2640 -> 2647 (+7)
- NSE_IN official_recall_missing: 641 -> 688 (+47)
- NSE_IN collision_adjusted_recall_missing: 445 -> 453 (+8)
- NSE_KE official_recall_missing: 55 -> 57 (+2)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
