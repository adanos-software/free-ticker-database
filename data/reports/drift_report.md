# Drift / freshness report

Generated: 2026-10-26T14:26:02Z
Dataset built_at: 2026-10-20T10:13:03Z (6.2 days ago; threshold 45.0)
**drift_detected: True**

## Pending renames (feed-detected, not yet applied): 0
- Triage sources: {'symbol_changes_review': 21}

## Blocked/manual rename review rows: 21
- LRHC -> SRTM (Stratum Holdings Corp, 2026-10-19): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- SKK -> RNTZ (Rantizo Inc, 2026-10-13): manual: current dataset row has no ISIN, so unchanged identity cannot be proven
- MIMI -> AXNX (Axonex Ai Group Ltd., 2026-10-13): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
- QNCX -> IRLA (Irulya Therapeutics Inc, 2026-10-12): Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer.
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
- expected_missing_primary_isin: 664
- missing_etf_category: 146
- missing_stock_sector: 668
- source_gap_rows: 11081

## Quality regressions: 2
- source_gap_rows: 11075 -> 11081 (+6)
- missing_etf_category: 143 -> 146 (+3)

## Official recall regressions: 6
- Borsa Italiana official_recall_missing: 2658 -> 2723 (+65)
- Borsa Italiana collision_adjusted_recall_missing: 760 -> 795 (+35)
- Euronext official_recall_missing: 681 -> 707 (+26)
- Euronext collision_adjusted_recall_missing: 312 -> 330 (+18)
- NYSE official_recall_missing: 1894 -> 1895 (+1)
- NYSE collision_adjusted_recall_missing: 1324 -> 1325 (+1)

_Detection only. Triage renames via the symbol-change review feed; apply corrections through the verified override/verify pipeline._
