# Drift / freshness report

Generated: 2026-09-07T13:35:12Z
Dataset built_at: 2026-09-03T11:44:20Z (4.1 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 22}

## Blocked/manual rename review rows: 22
- BTOG -> SGRX (Sangrix Inc, 2026-09-04): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
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
- expected_missing_primary_isin: 764
- missing_etf_category: 54
- missing_stock_sector: 1301
- source_gap_rows: 11485

## Quality regressions: 4
- source_gap_rows: 11455 -> 11485 (+30)
- expected_missing_primary_isin: 752 -> 764 (+12)
- missing_stock_sector: 1298 -> 1301 (+3)
- missing_etf_category: 46 -> 54 (+8)

## Official recall regressions: 5
- LSE official_recall_missing: 4285 -> 4289 (+4)
- LSE collision_adjusted_recall_missing: 3498 -> 3501 (+3)
- NYSE official_recall_missing: 1902 -> 1903 (+1)
- NYSE collision_adjusted_recall_missing: 1339 -> 1340 (+1)
- NYSE MKT official_recall_missing: 79 -> 80 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
