#!/usr/bin/env python3
"""Apply reviewed issue #449 ISIN identity resolutions (metadata clears + same-issuer)."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METADATA = ROOT / "data/review_overrides/metadata_updates.csv"
SAME_ISSUER = ROOT / "data/review_overrides/isin_identity_same_issuer.csv"
REVIEW_CSV = ROOT / "data/reports/isin_identity_issue449_review.csv"
REVIEW_JSON = ROOT / "data/reports/isin_identity_issue449_review.json"
REVIEW_MD = ROOT / "data/reports/isin_identity_issue449_review.md"

REVIEWED_AT = "2026-10-31"
REVIEWER = "cursor-cloud-agent"
SAME_ISSUER_AT = "2026-10-31T12:00:00Z"

METADATA_CLEARS: list[tuple[str, str, str, str, str, str]] = [
    (
        "TLG",
        "VSE",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: LSE official price explorer ticker 0R3N binds TLG IMMOBILIEN AG to DE000A12B8Z4; "
        "Deutsche Börse Frankfurt directory binds FSX::TGX TALGA GROUP LTD. to AU000000TLG7. "
        "VSE::TLG is named TLG Immobilien AG and must not carry Talga's Australian ISIN AU000000TLG7. "
        "Clear only; no replacement adopted. "
        "Sources: https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer "
        "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv",
    ),
    (
        "OMAX",
        "FSX",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: Frankfurt all-tradable directory lists OMA VIVESTO AB with SE0028925701; "
        "Nasdaq Nordic Stockholm shares directory lists STO::VIVE Vivesto with SE0028925701. "
        "FSX::OMAX (Vivesto AB) must not retain legacy SE0000722365 shared with unrelated OTC::VIVE. "
        "Clear only; no replacement in this patch. "
        "Sources: https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv "
        "https://api.nasdaq.com/api/nordic/screener/shares",
    ),
    (
        "VIVE",
        "OTC",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: Vivesto AB official listings use SE0028925701 (Frankfurt OMA, LSE 0N4A, Nasdaq Nordic STO::VIVE). "
        "OTC::VIVE is Viveve Medical Inc. and must not carry Vivesto's legacy SE0000722365. Clear only. "
        "Sources: https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer "
        "https://api.nasdaq.com/api/nordic/screener/shares",
    ),
    (
        "NASO",
        "OTC",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: NYSE American official security master binds NYSE MKT::GNS Genius Group Ltd to SGXZ81555062; "
        "SEC company_tickers_exchange.json binds GNS to Genius Group Ltd. "
        "OTC::NASO Naples Soap Company must not share Genius Group's Singapore ISIN. Clear only. "
        "Sources: https://www.sec.gov/files/company_tickers_exchange.json "
        "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
    ),
    (
        "GFSTEELS",
        "NSE_IN",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: NSE EQUITY_L.csv lists TIKONA Tikona Communication Limited with INE534A01028 and has no GFSTEELS row; "
        "BSE List_Scrips binds BSE_IN::GFSTEELS Grand Foundry Ltd to INE534A01028. "
        "NSE_IN::GFSTEELS lacks listing-keyed NSE evidence. Clear only. "
        "Sources: https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv "
        "https://www.bseindia.com/corporates/List_Scrips.html",
    ),
    (
        "PTECH",
        "SET",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: SET listed-companies directory lists SET::TBSP TBSP PUBLIC COMPANY LIMITED; "
        "SET stock search API lists the same TBSP listing. PTECH is absent from official SET listing directories "
        "and must not retain ISIN TH0476010Y03 copied from the homonym. Clear only. "
        "Sources: https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls "
        "https://www.set.or.th/api/set/stock/list",
    ),
    (
        "BIGG",
        "OTC",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: TMX listed-issuers directory binds TSXV::BIGG to BIGG Digital Assets Inc.; "
        "OTC Markets screener binds BBKCF to BIGG DIGITAL ASSETS INC (CA0898041086 peer). "
        "OTC::BIGG Big Tree Group Inc. must not share US0896951003. Clear only. "
        "Sources: https://www.tsx.com/en/listings/current-market-statistics/mig-archives "
        "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV?greyAccess=false&expertAccess=false",
    ),
    (
        "BIGG",
        "TSXV",
        "isin",
        "clear",
        "",
        "Reviewed issue #449: TMX listed-issuers directory names TSXV::BIGG BIGG Digital Assets Inc.; "
        "Frankfurt directory binds FSX::7111 BIGG Digital Assets Inc. to CA0898041086. "
        "Stored US0896951003 is a ticker-collision artifact. Clear only; CA fill is a separate reviewed item. "
        "Sources: https://www.tsx.com/en/listings/current-market-statistics/mig-archives "
        "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv",
    ),
]

SAME_ISSUER_ROWS: list[tuple[str, str, str]] = [
    (
        "US0268747849",
        "FSX::AINN|LSE::0OAL|NYSE::AIG|XETRA::AINN",
        "Reviewed same issuer: American International Group cross-lists on NYSE, LSE 0OAL, XETRA/FSX AINN with US0268747849 per SEC and LSE/Deutsche Börse directories. Do not clear or reassign this ISIN.",
    ),
    (
        "US6153691059",
        "FSX::DUT|LSE::0K36|NYSE::MCO|XETRA::DUT",
        "Reviewed same issuer: Moody's Corp on NYSE MCO, LSE 0K36, XETRA/FSX DUT with US6153691059 per LSE price explorer and Deutsche Börse directories. Do not clear or reassign this ISIN.",
    ),
    (
        "GB00BNGDN821",
        "FSX::27M|LSE::MRO|XETRA::27M",
        "Reviewed same issuer: Melrose Industries PLC on LSE MRO and XETRA/FSX 27M with GB00BNGDN821 per LSE price explorer and Deutsche Börse directories. Do not clear or reassign this ISIN.",
    ),
    (
        "DE000A41YEG8",
        "XETRA::CSQ|XETRA::CSQ0",
        "Reviewed same issuer: Nexum Group AG (XETRA CSQ) and legacy creditshelf line CSQ0 share DE000A41YEG8 after rename; Deutsche Börse listed-companies subset. Do not clear or reassign this ISIN.",
    ),
    (
        "NL0000313286",
        "LSE::0DLI|XETRA::8AC",
        "Reviewed same issuer: Amsterdam Commodities / ACOMO NV on LSE 0DLI and XETRA 8AC with NL0000313286 per LSE price explorer and XETRA all-tradable directory. Do not clear or reassign this ISIN.",
    ),
    (
        "US34631F1021",
        "FSX::9FS|NYSE::FPS|XETRA::9FS",
        "Reviewed same issuer: Forgent Power Solutions on NYSE FPS and XETRA/FSX 9FS with US34631F1021 per Nasdaq/SEC and Deutsche Börse directories. Do not clear or reassign this ISIN.",
    ),
    (
        "US55825T1034",
        "FSX::MS8|NYSE::MSGK|XETRA::MS8",
        "Reviewed same issuer: MSG Knickerbockers on NYSE MSGK and XETRA/FSX MS8 with US55825T1034 per SEC/Nasdaq other listed and Deutsche Börse directories. Do not clear or reassign this ISIN.",
    ),
    (
        "US7789203061",
        "NASDAQ::SHAZ|XETRA::87P",
        "Reviewed same issuer: SharonAI Holdings on NASDAQ SHAZ and XETRA 87P with US7789203061 per Nasdaq Trader and XETRA all-tradable directory. Do not clear or reassign this ISIN.",
    ),
    (
        "CA9609085076",
        "FSX::WPI|NASDAQ::AETR|TSX::WPRT",
        "Reviewed same issuer: Westport Fuel Systems renamed to Aetherion on Nasdaq (listing transition WPRT→AETR) while TSX WPRT and Frankfurt WPI retain CA9609085076 per TMX interlisted and official symbol-change evidence. Do not clear or reassign this ISIN.",
    ),
]

REVIEW_ITEMS: list[dict[str, str]] = [
    {
        "isin": "AU000000TLG7",
        "listing_keys": "ASX::TLG|FSX::TGX|OTC::TLGRF|VSE::TLG|XSTU::TGX",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "VSE::TLG",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "AU000000CRB1",
        "listing_keys": "ASX::CRB|Euronext::CRB",
        "status": "open_needs_official_identifier_evidence",
        "changed_listing_key": "",
        "decision": "leave_open",
        "replacement_isin": "",
        "evidence_url_1": "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
        "evidence_url_2": "https://live.euronext.com/en/product_directory/data/etf-all-markets/download",
        "blocker": "Need a dated Euronext or issuer record tying legacy Euronext::CRB to an ISIN before clearing ASX Carbine or the ETF row.",
    },
    {
        "isin": "CA41752L1076",
        "listing_keys": "OTC::HBIE|TSX::HBIE",
        "status": "open_needs_official_identifier_evidence",
        "changed_listing_key": "",
        "decision": "leave_open",
        "replacement_isin": "",
        "evidence_url_1": "https://www.tsx.com/en/listings/current-market-statistics/mig-archives",
        "evidence_url_2": "https://www.tsx.com/en/news/new-company-listings?id=1751",
        "blocker": "TMX names TSX::HBIE Harvest ETF but published ETF JSON still lacks an ISIN field; need listing-keyed ISIN for TSX and independent OTC::HBIE Hai Jia identity.",
    },
    {
        "isin": "CA5910881096",
        "listing_keys": "OTC::MERG|TSXV::MERG",
        "status": "open_needs_official_identifier_evidence",
        "changed_listing_key": "",
        "decision": "leave_open",
        "replacement_isin": "",
        "evidence_url_1": "https://www.tsx.com/en/listings/current-market-statistics/mig-archives",
        "evidence_url_2": "https://www.newsfile-prod.tmx.cloud/release/270743/Metal-Energy-Provides-Corporate-Update",
        "blocker": "Metal Energy post-consolidation ISIN not yet bound to TSXV::MERG; OTC Merger Mines identity needs separate official observation.",
    },
    {
        "isin": "US0896951003",
        "listing_keys": "OTC::BIGG|TSXV::BIGG",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "OTC::BIGG;TSXV::BIGG",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.tsx.com/en/listings/current-market-statistics/mig-archives",
        "evidence_url_2": "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV?greyAccess=false&expertAccess=false",
        "blocker": "",
    },
    {
        "isin": "US0268747849",
        "listing_keys": "FSX::AINN|LSE::0OAL|NYSE::AIG|XETRA::AINN",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.sec.gov/files/company_tickers_exchange.json",
        "evidence_url_2": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "blocker": "",
    },
    {
        "isin": "US6153691059",
        "listing_keys": "FSX::DUT|LSE::0K36|NYSE::MCO|XETRA::DUT",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "GB00BNGDN821",
        "listing_keys": "FSX::27M|LSE::MRO|XETRA::27M",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "CA9609085076",
        "listing_keys": "FSX::WPI|NASDAQ::AETR|TSX::WPRT",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.tsx.com/files/trading/interlisted-companies.txt",
        "evidence_url_2": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        "blocker": "",
    },
    {
        "isin": "INE534A01028",
        "listing_keys": "BSE_IN::GFSTEELS|NSE_IN::GFSTEELS|NSE_IN::TIKONA",
        "status": "open_needs_official_identifier_evidence",
        "changed_listing_key": "NSE_IN::GFSTEELS",
        "decision": "partial_clear",
        "replacement_isin": "",
        "evidence_url_1": "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        "evidence_url_2": "https://www.bseindia.com/corporates/List_Scrips.html",
        "blocker": "Cleared NSE_IN::GFSTEELS (no NSE directory row). BSE Grand Foundry vs NSE Tikona still both claim INE534A01028; needs MCA/NSDL adjudication.",
    },
    {
        "isin": "DE000A41YEG8",
        "listing_keys": "XETRA::CSQ|XETRA::CSQ0",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.cashmarket.deutsche-boerse.com/resource/blob/67858/dd766fc6588100c79294324175f95501/data/Listed-companies.xlsx",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "NL0000313286",
        "listing_keys": "LSE::0DLI|XETRA::8AC",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "SGXZ81555062",
        "listing_keys": "NYSE MKT::GNS|OTC::NASO",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "OTC::NASO",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.sec.gov/files/company_tickers_exchange.json",
        "evidence_url_2": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
        "blocker": "",
    },
    {
        "isin": "SE0000722365",
        "listing_keys": "FSX::OMAX|OTC::VIVE",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "FSX::OMAX;OTC::VIVE",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "evidence_url_2": "https://api.nasdaq.com/api/nordic/screener/shares",
        "blocker": "",
    },
    {
        "isin": "TH0476010Y03",
        "listing_keys": "SET::PTECH|SET::TBSP",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "SET::PTECH",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls",
        "evidence_url_2": "https://www.set.or.th/api/set/stock/list",
        "blocker": "",
    },
    {
        "isin": "US34631F1021",
        "listing_keys": "NYSE::FPS|XETRA::9FS",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "US55825T1034",
        "listing_keys": "FSX::MS8|NYSE::MSGK",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/3a67eacf4ad3419344f37b88a755943f/data/t7-xfra-BF-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "US7789203061",
        "listing_keys": "NASDAQ::SHAZ|XETRA::87P",
        "status": "closed_same_issuer_reviewed",
        "changed_listing_key": "",
        "decision": "same_issuer",
        "replacement_isin": "",
        "evidence_url_1": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        "evidence_url_2": "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv",
        "blocker": "",
    },
    {
        "isin": "CA3025862010",
        "listing_keys": "LSE::FP|OTC::FPNUF|TSXV::FP",
        "status": "cleared_misassigned_isin",
        "changed_listing_key": "LSE::FP",
        "decision": "clear",
        "replacement_isin": "",
        "evidence_url_1": "https://www.fondulproprietatea.ro/files/live/sites/fondul/files/en/listing-materials/london/prospectus_final_with_electronic_disclaimer.pdf",
        "evidence_url_2": "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer",
        "blocker": "Prior review 2026-10-20; unchanged.",
    },
]


def append_metadata_clears() -> None:
    with METADATA.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        for ticker, exchange, field, decision, proposed, reason in METADATA_CLEARS:
            writer.writerow([ticker, exchange, field, decision, proposed, "0.99", reason])


def append_same_issuer() -> None:
    existing = SAME_ISSUER.read_text(encoding="utf-8")
    lines = []
    for isin, keys, reason in SAME_ISSUER_ROWS:
        if isin in existing:
            continue
        escaped = reason.replace('"', '""')
        lines.append(f'{isin},{keys},{SAME_ISSUER_AT},"{escaped}"')
    if lines:
        with SAME_ISSUER.open("a", encoding="utf-8") as handle:
            if not existing.endswith("\n"):
                handle.write("\n")
            handle.write("\n".join(lines) + "\n")


def write_review_artifacts() -> None:
    fieldnames = [
        "isin",
        "listing_keys",
        "status",
        "changed_listing_key",
        "decision",
        "replacement_isin",
        "evidence_url_1",
        "evidence_url_2",
        "source_key",
        "reviewed_at",
        "reviewer",
        "blocker",
        "direct_identifier_apply_allowed_rows",
    ]
    with REVIEW_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in REVIEW_ITEMS:
            row = {
                **item,
                "source_key": "official_source_review",
                "reviewed_at": REVIEWED_AT,
                "reviewer": REVIEWER,
                "direct_identifier_apply_allowed_rows": "0",
            }
            writer.writerow(row)

    closed = sum(1 for i in REVIEW_ITEMS if i["status"] == "closed_same_issuer_reviewed")
    cleared = sum(1 for i in REVIEW_ITEMS if i["status"] == "cleared_misassigned_isin")
    open_count = sum(1 for i in REVIEW_ITEMS if i["status"] == "open_needs_official_identifier_evidence")
    payload = {
        "issue": 449,
        "generated_at": datetime.now(UTC).isoformat(),
        "groups_reviewed": len(REVIEW_ITEMS),
        "groups_closed_same_issuer": closed,
        "groups_cleared_misassigned": cleared,
        "groups_left_open": open_count,
        "direct_identifier_apply_allowed_rows": 0,
        "items": [
            {
                **item,
                "source_key": "official_source_review",
                "reviewed_at": REVIEWED_AT,
                "reviewer": REVIEWER,
                "direct_identifier_apply_allowed_rows": 0,
            }
            for item in REVIEW_ITEMS
        ],
    }
    REVIEW_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    md_lines = [
        "# Issue 449: ISIN identity collision groups (18)",
        "",
        f"Reviewed on {REVIEWED_AT}. Same-issuer closures: {closed}. Misassigned ISIN clears: {cleared}. Open: {open_count}.",
        "",
        "| ISIN | Listing keys | Decision | Source 1 | Source 2 | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in REVIEW_ITEMS:
        notes = item["blocker"] or item["status"]
        md_lines.append(
            f"| {item['isin']} | {item['listing_keys']} | {item['decision']} | "
            f"[link]({item['evidence_url_1']}) | [link]({item['evidence_url_2']}) | {notes} |"
        )
    REVIEW_MD.write_text("\n".join(md_lines) + "\n", encoding="utf-8")


def main() -> None:
    append_metadata_clears()
    append_same_issuer()
    write_review_artifacts()
    print("Applied issue #449 batch updates.")


if __name__ == "__main__":
    main()
