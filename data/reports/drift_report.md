# Drift / freshness report

Generated: 2026-11-16T17:12:08Z
Dataset built_at: 2026-11-16T16:24:22Z (0.0 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 19}

## Blocked/manual rename review rows: 19
- TCBI -> TXCP (Texas Capital Bancshares Inc, 2026-11-09): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- INM -> MTRI (Mentari Therapeutics Inc, 2026-11-09): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
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
- expected_missing_primary_isin: 629
- missing_etf_category: 149
- missing_stock_sector: 594
- source_gap_rows: 11010

## Quality regressions: 2
- expected_missing_primary_isin: 605 -> 629 (+24)
- missing_etf_category: 118 -> 149 (+31)

## Official recall regressions: 15
- Borsa Italiana official_recall_missing: 2716 -> 2732 (+16)
- Borsa Italiana collision_adjusted_recall_missing: 789 -> 796 (+7)
- DFM official_recall_missing: 17 -> 18 (+1)
- Euronext official_recall_missing: 705 -> 712 (+7)
- Euronext collision_adjusted_recall_missing: 331 -> 340 (+9)
- FSX official_recall_missing: 10322 -> 10354 (+32)
- FSX collision_adjusted_recall_missing: 5901 -> 5922 (+21)
- KRX official_recall_missing: 177 -> 183 (+6)
- KRX collision_adjusted_recall_missing: 163 -> 169 (+6)
- NASDAQ official_recall_missing: 1038 -> 1039 (+1)
- NSE_IN official_recall_missing: 73 -> 74 (+1)
- SGX official_recall_missing: 130 -> 131 (+1)
- TXSE official_recall_missing: 22 -> 25 (+3)
- TXSE collision_adjusted_recall_missing: 8 -> 10 (+2)
- XETRA official_recall_missing: 241 -> 242 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
