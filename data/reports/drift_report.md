# Drift / freshness report

Generated: 2026-09-14T14:16:50Z
Dataset built_at: 2026-09-14T13:27:43Z (0.0 days ago; threshold 45.0)
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
- allowed_warn_rows: 31
- expected_missing_primary_isin: 819
- missing_etf_category: 76
- missing_stock_sector: 1296
- source_gap_rows: 11515

## Quality regressions: 3
- source_gap_rows: 11356 -> 11515 (+159)
- expected_missing_primary_isin: 768 -> 819 (+51)
- missing_etf_category: 56 -> 76 (+20)

## Official recall regressions: 25
- B3 official_recall_missing: 82 -> 133 (+51)
- B3 collision_adjusted_recall_missing: 82 -> 133 (+51)
- BIST official_recall_missing: 44 -> 49 (+5)
- BIST collision_adjusted_recall_missing: 22 -> 27 (+5)
- Euronext official_recall_missing: 678 -> 679 (+1)
- Euronext collision_adjusted_recall_missing: 314 -> 315 (+1)
- FSX official_recall_missing: 10129 -> 10161 (+32)
- FSX collision_adjusted_recall_missing: 6179 -> 6213 (+34)
- HKEX official_recall_missing: 160 -> 169 (+9)
- HKEX collision_adjusted_recall_missing: 90 -> 99 (+9)
- KOSDAQ official_recall_missing: 226 -> 227 (+1)
- KOSDAQ collision_adjusted_recall_missing: 223 -> 224 (+1)
- KRX official_recall_missing: 148 -> 153 (+5)
- KRX collision_adjusted_recall_missing: 134 -> 139 (+5)
- LSE official_recall_missing: 4289 -> 4303 (+14)
- LSE collision_adjusted_recall_missing: 3501 -> 3513 (+12)
- NSE_IN collision_adjusted_recall_missing: 538 -> 540 (+2)
- NYSE official_recall_missing: 1904 -> 1906 (+2)
- PSE official_recall_missing: 230 -> 231 (+1)
- PSE collision_adjusted_recall_missing: 111 -> 112 (+1)
- PSX official_recall_missing: 330 -> 332 (+2)
- PSX collision_adjusted_recall_missing: 188 -> 190 (+2)
- TSXV collision_adjusted_recall_missing: 2 -> 3 (+1)
- XETRA official_recall_missing: 1014 -> 1022 (+8)
- XETRA collision_adjusted_recall_missing: 307 -> 315 (+8)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
