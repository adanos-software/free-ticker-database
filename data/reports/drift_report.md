# Drift / freshness report

Generated: 2026-11-09T17:41:40Z
Dataset built_at: 2026-11-09T17:08:46Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 17}

## Blocked/manual rename review rows: 17
- PAPL -> PTIV (Pineapple Financial Inc, 2026-10-26): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- GWH -> GWHT (Ess Tech Inc, 2026-10-06): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NYSE::GWH
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
- expected_missing_primary_isin: 605
- missing_etf_category: 118
- missing_stock_sector: 644
- source_gap_rows: 11123

## Quality regressions: 0

## Official recall regressions: 12
- ADX official_recall_missing: 30 -> 32 (+2)
- ADX collision_adjusted_recall_missing: 0 -> 2 (+2)
- FSX official_recall_missing: 10321 -> 10322 (+1)
- FSX collision_adjusted_recall_missing: 5888 -> 5901 (+13)
- IDX collision_adjusted_recall_missing: 18 -> 19 (+1)
- KRX official_recall_missing: 175 -> 177 (+2)
- KRX collision_adjusted_recall_missing: 161 -> 163 (+2)
- LSE collision_adjusted_recall_missing: 3501 -> 3502 (+1)
- NYSE official_recall_missing: 1869 -> 1877 (+8)
- NYSE collision_adjusted_recall_missing: 1302 -> 1310 (+8)
- TADAWUL official_recall_missing: 210 -> 211 (+1)
- XETRA official_recall_missing: 238 -> 241 (+3)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
