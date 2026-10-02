# Drift / freshness report

Generated: 2026-10-02T07:25:25Z
Dataset built_at: 2026-10-02T06:35:05Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 15}

## Blocked/manual rename review rows: 15
- BGI -> BGICF (Birks Group Inc, 2026-08-27): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: TSX::BGI
- EQR -> VRMK (Vivmark Residential, 2026-08-18): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- NCL -> NCLX (Northann Corp, 2026-08-13): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: FSX::NCL|NYSE::NCL|SET::NCL|WSE::NCL|XSTU::NCL
- GV -> GVHGF (Visionary Holdings Inc, 2026-07-31): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NASDAQ::GV
- BTM -> BTMCQ (Bitcoin Depot Inc, 2026-05-22): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::BTM
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
- allowed_warn_rows: 35
- expected_missing_primary_isin: 1076
- missing_etf_category: 435
- missing_stock_sector: 3933
- source_gap_rows: 14483

## Quality regressions: 4
- source_gap_rows: 14432 -> 14483 (+51)
- expected_missing_primary_isin: 1028 -> 1076 (+48)
- missing_stock_sector: 3907 -> 3933 (+26)
- missing_etf_category: 434 -> 435 (+1)

## Official recall regressions: 8
- FSX official_recall_missing: 10188 -> 10271 (+83)
- FSX collision_adjusted_recall_missing: 5926 -> 5993 (+67)
- NASDAQ collision_adjusted_recall_missing: 982 -> 983 (+1)
- SET official_recall_missing: 170 -> 171 (+1)
- SGX official_recall_missing: 122 -> 124 (+2)
- SGX collision_adjusted_recall_missing: 0 -> 2 (+2)
- TWSE official_recall_missing: 78 -> 79 (+1)
- XETRA official_recall_missing: 172 -> 175 (+3)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
