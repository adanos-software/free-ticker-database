# Drift / freshness report

Generated: 2026-11-02T16:50:06Z
Dataset built_at: 2026-11-02T08:16:01Z (0.4 days ago; threshold 45.0)
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
- allowed_warn_rows: 32
- expected_missing_primary_isin: 701
- missing_etf_category: 121
- missing_stock_sector: 691
- source_gap_rows: 11260

## Quality regressions: 3
- source_gap_rows: 11081 -> 11260 (+179)
- expected_missing_primary_isin: 664 -> 701 (+37)
- missing_stock_sector: 668 -> 691 (+23)

## Official recall regressions: 25
- BATS official_recall_missing: 339 -> 353 (+14)
- BATS collision_adjusted_recall_missing: 281 -> 295 (+14)
- Euronext official_recall_missing: 707 -> 708 (+1)
- Euronext collision_adjusted_recall_missing: 330 -> 333 (+3)
- FSX official_recall_missing: 10151 -> 10321 (+170)
- FSX collision_adjusted_recall_missing: 5861 -> 5888 (+27)
- KRX official_recall_missing: 161 -> 175 (+14)
- KRX collision_adjusted_recall_missing: 147 -> 161 (+14)
- LSE collision_adjusted_recall_missing: 3500 -> 3501 (+1)
- NASDAQ official_recall_missing: 1043 -> 1045 (+2)
- NASDAQ collision_adjusted_recall_missing: 984 -> 985 (+1)
- NEO official_recall_missing: 234 -> 241 (+7)
- NEO collision_adjusted_recall_missing: 166 -> 172 (+6)
- NSE_IN official_recall_missing: 72 -> 73 (+1)
- NYSE ARCA official_recall_missing: 120 -> 132 (+12)
- NYSE ARCA collision_adjusted_recall_missing: 89 -> 102 (+13)
- NYSE MKT official_recall_missing: 80 -> 83 (+3)
- NYSE MKT collision_adjusted_recall_missing: 47 -> 49 (+2)
- PSE official_recall_missing: 214 -> 215 (+1)
- SET official_recall_missing: 172 -> 173 (+1)
- SGX official_recall_missing: 127 -> 130 (+3)
- TWSE official_recall_missing: 35 -> 38 (+3)
- TXSE official_recall_missing: 16 -> 22 (+6)
- TXSE collision_adjusted_recall_missing: 7 -> 8 (+1)
- XETRA official_recall_missing: 179 -> 238 (+59)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
