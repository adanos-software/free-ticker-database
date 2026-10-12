# Drift / freshness report

Generated: 2026-10-12T16:51:42Z
Dataset built_at: 2026-10-11T11:19:20Z (1.2 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 17}

## Blocked/manual rename review rows: 17
- HKIT -> BDAI (Biddance Ai Systems Inc, 2026-10-09): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
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
- allowed_warn_rows: 34
- expected_missing_primary_isin: 665
- missing_etf_category: 143
- missing_stock_sector: 669
- source_gap_rows: 11075

## Quality regressions: 0

## Official recall regressions: 16
- Borsa Italiana official_recall_missing: 2656 -> 2658 (+2)
- Borsa Italiana collision_adjusted_recall_missing: 759 -> 760 (+1)
- KOSDAQ official_recall_missing: 230 -> 233 (+3)
- KOSDAQ collision_adjusted_recall_missing: 227 -> 230 (+3)
- KRX official_recall_missing: 160 -> 161 (+1)
- KRX collision_adjusted_recall_missing: 146 -> 147 (+1)
- LSE official_recall_missing: 4349 -> 4371 (+22)
- LSE collision_adjusted_recall_missing: 3492 -> 3500 (+8)
- PSE official_recall_missing: 212 -> 214 (+2)
- PSE collision_adjusted_recall_missing: 92 -> 94 (+2)
- SET official_recall_missing: 171 -> 172 (+1)
- SET collision_adjusted_recall_missing: 37 -> 38 (+1)
- SGX official_recall_missing: 124 -> 127 (+3)
- TXSE official_recall_missing: 5 -> 16 (+11)
- TXSE collision_adjusted_recall_missing: 2 -> 7 (+5)
- XETRA official_recall_missing: 175 -> 179 (+4)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
