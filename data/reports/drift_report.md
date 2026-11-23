# Drift / freshness report

Generated: 2026-11-23T19:22:28Z
Dataset built_at: 2026-11-23T18:03:45Z (0.1 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 21}

## Blocked/manual rename review rows: 21
- ACH -> ACCH (Accendra Health Inc, 2026-11-17): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: NYSE::ACH
- TCBI -> TXCP (Texas Capital Bancshares Inc, 2026-11-09): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- INM -> MTRI (Mentari Therapeutics Inc, 2026-11-09): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- PAPL -> PTIV (Pineapple Financial Inc, 2026-10-26): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- GWH -> GWHT (Ess Tech Inc, 2026-10-06): manual: source exchange scope is not mapped to a safe listing-keyed apply path
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
- OMI -> ACCH (Accendra Health Inc, 2026-01-02): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: TSXV::OMI
- OPT -> OPTEY (Opthea Ltd., 2025-11-20): manual: source exchange scope is not mapped to a safe listing-keyed apply path
- EBR -> AXIAY (Axia Energia SA, 2025-11-10): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::EBR
- PET -> PETXQ (Wag! Group Co., 2025-07-29): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: ASX::PET|LSE::PET|TSX::PET
- SUP -> SSUP (Superior Industries International Inc, 2025-06-25): blocked: secondary feed scope is OTC, but old symbol matches dataset listing(s) outside that scope: LSE::SUP
- WW -> WGHTQ (Ww International Inc, 2025-05-15): manual: source exchange scope is not mapped to a safe listing-keyed apply path

## Quality indicators (release-gate info counts)
- allowed_warn_rows: 36
- expected_missing_primary_isin: 645
- missing_etf_category: 66
- missing_stock_sector: 563
- source_gap_rows: 10988

## Quality regressions: 1
- expected_missing_primary_isin: 629 -> 645 (+16)

## Official recall regressions: 17
- Borsa Italiana official_recall_missing: 2732 -> 2736 (+4)
- Borsa Italiana collision_adjusted_recall_missing: 796 -> 800 (+4)
- FSX official_recall_missing: 10354 -> 10403 (+49)
- FSX collision_adjusted_recall_missing: 5922 -> 5945 (+23)
- IDX official_recall_missing: 199 -> 205 (+6)
- IDX collision_adjusted_recall_missing: 16 -> 19 (+3)
- KRX official_recall_missing: 183 -> 185 (+2)
- KRX collision_adjusted_recall_missing: 169 -> 171 (+2)
- NEO official_recall_missing: 241 -> 244 (+3)
- NEO collision_adjusted_recall_missing: 172 -> 174 (+2)
- NYSE official_recall_missing: 1875 -> 1877 (+2)
- NYSE ARCA official_recall_missing: 131 -> 135 (+4)
- NYSE ARCA collision_adjusted_recall_missing: 102 -> 106 (+4)
- OSL official_recall_missing: 7 -> 8 (+1)
- SGX official_recall_missing: 131 -> 132 (+1)
- UPCOM collision_adjusted_recall_missing: 293 -> 294 (+1)
- XETRA official_recall_missing: 242 -> 247 (+5)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
