# Drift / freshness report

Generated: 2026-09-28T15:40:18Z
Dataset built_at: 2026-09-28T14:41:54Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 17}

## Blocked/manual rename review rows: 17
- BGI -> BGICF (Birks Group Inc, 2026-08-27): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: TSX::BGI
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
- allowed_warn_rows: 36
- expected_missing_primary_isin: 1028
- missing_etf_category: 434
- missing_stock_sector: 3907
- source_gap_rows: 14432

## Quality regressions: 4
- source_gap_rows: 11455 -> 14432 (+2977)
- expected_missing_primary_isin: 860 -> 1028 (+168)
- missing_stock_sector: 1306 -> 3907 (+2601)
- missing_etf_category: 109 -> 434 (+325)

## Official recall regressions: 13
- Euronext official_recall_missing: 679 -> 681 (+2)
- KOSDAQ official_recall_missing: 226 -> 230 (+4)
- KOSDAQ collision_adjusted_recall_missing: 223 -> 227 (+4)
- KRX official_recall_missing: 156 -> 160 (+4)
- KRX collision_adjusted_recall_missing: 142 -> 146 (+4)
- LSE official_recall_missing: 4312 -> 4349 (+37)
- NASDAQ official_recall_missing: 1044 -> 1049 (+5)
- NASDAQ collision_adjusted_recall_missing: 980 -> 982 (+2)
- NEO official_recall_missing: 233 -> 234 (+1)
- NYSE official_recall_missing: 1905 -> 1908 (+3)
- TSX collision_adjusted_recall_missing: 5 -> 6 (+1)
- TXSE official_recall_missing: 4 -> 5 (+1)
- TXSE collision_adjusted_recall_missing: 1 -> 2 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
