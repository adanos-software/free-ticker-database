# Drift / freshness report

Generated: 2026-09-21T14:22:26Z
Dataset built_at: 2026-09-21T12:51:48Z (0.1 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 16}

## Blocked/manual rename review rows: 16
- ISSC -> IA (Innovative Solutions & Support Inc, 2026-08-18): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- EQR -> VRMK (Vivmark Residential, 2026-08-18): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- NCL -> NCLX (Northann Corp, 2026-08-13): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: FSX::NCL|NYSE::NCL|SET::NCL|WSE::NCL|XSTU::NCL
- GV -> GVHGF (Visionary Holdings Inc, 2026-07-31): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::GV
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
- ETHM -> DYNC (Dynamix Corp, 2026-05-01): manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven and the old symbol is still present in an official source
- CIGL -> YOOV (Concorde International Group Ltd., 2026-04-13): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
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
- allowed_warn_rows: 33
- expected_missing_primary_isin: 860
- missing_etf_category: 109
- missing_stock_sector: 1306
- source_gap_rows: 11455

## Quality regressions: 3
- expected_missing_primary_isin: 819 -> 860 (+41)
- missing_stock_sector: 1296 -> 1306 (+10)
- missing_etf_category: 76 -> 109 (+33)

## Official recall regressions: 21
- ADX official_recall_missing: 37 -> 38 (+1)
- BIST official_recall_missing: 49 -> 50 (+1)
- BIST collision_adjusted_recall_missing: 27 -> 28 (+1)
- BSE_IN official_recall_missing: 2476 -> 2515 (+39)
- BSE_IN collision_adjusted_recall_missing: 631 -> 663 (+32)
- Borsa Italiana official_recall_missing: 2657 -> 2658 (+1)
- Borsa Italiana collision_adjusted_recall_missing: 801 -> 802 (+1)
- FSX official_recall_missing: 10161 -> 10188 (+27)
- FSX collision_adjusted_recall_missing: 6213 -> 6238 (+25)
- HKEX official_recall_missing: 169 -> 177 (+8)
- HKEX collision_adjusted_recall_missing: 99 -> 108 (+9)
- KRX official_recall_missing: 153 -> 156 (+3)
- KRX collision_adjusted_recall_missing: 139 -> 142 (+3)
- LSE official_recall_missing: 4303 -> 4312 (+9)
- LSE collision_adjusted_recall_missing: 3513 -> 3519 (+6)
- NSE_IN official_recall_missing: 1000 -> 1013 (+13)
- NSE_IN collision_adjusted_recall_missing: 540 -> 553 (+13)
- PSX official_recall_missing: 332 -> 334 (+2)
- PSX collision_adjusted_recall_missing: 190 -> 192 (+2)
- XETRA official_recall_missing: 1022 -> 1031 (+9)
- XETRA collision_adjusted_recall_missing: 315 -> 323 (+8)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
