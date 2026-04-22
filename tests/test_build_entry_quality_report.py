from __future__ import annotations

import csv

from scripts.build_entry_quality_report import (
    assess_entries,
    summarize,
    utc_now,
    write_csv,
    write_markdown,
)


def row(
    listing_key: str,
    ticker: str,
    exchange: str,
    name: str,
    *,
    isin: str,
    stock_sector: str = "Information Technology",
    etf_category: str = "",
    aliases: str = "",
    asset_type: str = "Stock",
    country: str = "United States",
    country_code: str = "US",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "name": name,
        "asset_type": asset_type,
        "stock_sector": stock_sector,
        "etf_category": etf_category,
        "country": country,
        "country_code": country_code,
        "isin": isin,
        "aliases": aliases,
    }


def scope(
    listing_key: str,
    ticker: str,
    exchange: str,
    *,
    isin: str,
    instrument_scope: str = "core",
    scope_reason: str = "primary_listing",
    primary_listing_key: str | None = None,
    asset_type: str = "Stock",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "asset_type": asset_type,
        "isin": isin,
        "instrument_group_key": isin or listing_key,
        "instrument_scope": instrument_scope,
        "scope_reason": scope_reason,
        "primary_listing_key": primary_listing_key or listing_key,
    }


def official_ref(
    ticker: str,
    exchange: str,
    name: str,
    isin: str,
    asset_type: str = "Stock",
    *,
    source_key: str = "test_masterfile",
    provider: str = "Test Exchange",
) -> dict[str, str]:
    return {
        "source_key": source_key,
        "provider": provider,
        "source_url": "https://example.test",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": isin,
    }


def test_entry_quality_marks_pass_source_gap_warn_and_quarantine():
    listings = [
        row("NASDAQ::MSFT", "MSFT", "NASDAQ", "Microsoft Corporation", isin="US5949181045", aliases="microsoft"),
        row(
            "TSE::7203",
            "7203",
            "TSE",
            "Toyota Motor Corp",
            isin="",
            stock_sector="Consumer Discretionary",
            aliases="toyota motor",
            country="Japan",
            country_code="JP",
        ),
        row(
            "PSX::LOADSR1",
            "LOADSR1",
            "PSX",
            "LOADS Limited",
            isin="",
            stock_sector="Consumer Discretionary",
            aliases="loads",
            country="Pakistan",
            country_code="PK",
        ),
        row("XETRA::MSF", "MSF", "XETRA", "Microsoft Corporation", isin="US5949181045", aliases="microsoft"),
    ]
    scopes = [
        scope("NASDAQ::MSFT", "MSFT", "NASDAQ", isin="US5949181045"),
        scope(
            "TSE::7203",
            "7203",
            "TSE",
            isin="",
            scope_reason="primary_listing_missing_isin",
        ),
        scope(
            "PSX::LOADSR1",
            "LOADSR1",
            "PSX",
            isin="",
            scope_reason="primary_listing_missing_isin",
        ),
        scope(
            "XETRA::MSF",
            "MSF",
            "XETRA",
            isin="US5949181045",
            instrument_scope="extended",
            scope_reason="secondary_cross_listing",
            primary_listing_key="NASDAQ::MISSING",
        ),
    ]
    identifiers = [
        {"listing_key": "NASDAQ::MSFT", "ticker": "MSFT", "exchange": "NASDAQ", "isin": "US5949181045", "wkn": "", "figi": "BBG000BPH459", "cik": "789019", "lei": "", "figi_source": "OpenFIGI", "cik_source": "SEC", "lei_source": ""},
        {"listing_key": "TSE::7203", "ticker": "7203", "exchange": "TSE", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        {"listing_key": "PSX::LOADSR1", "ticker": "LOADSR1", "exchange": "PSX", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        {"listing_key": "XETRA::MSF", "ticker": "MSF", "exchange": "XETRA", "isin": "US5949181045", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
    ]
    aliases = [
        {"ticker": "MSFT", "alias": "microsoft", "alias_type": "name"},
        {"ticker": "7203", "alias": "toyota motor", "alias_type": "name"},
        {"ticker": "LOADSR1", "alias": "loads", "alias_type": "name"},
        {"ticker": "MSF", "alias": "microsoft", "alias_type": "name"},
    ]
    coverage_report = {
        "by_exchange": [
            {"exchange": "NASDAQ", "venue_status": "official_full"},
            {"exchange": "TSE", "venue_status": "official_partial"},
            {"exchange": "PSX", "venue_status": "missing"},
            {"exchange": "XETRA", "venue_status": "official_full"},
        ]
    }

    report_rows = assess_entries(
        listings,
        tickers=[listings[0], listings[1], listings[2]],
        scopes=scopes,
        identifiers=identifiers,
        masterfiles=[official_ref("MSFT", "NASDAQ", "Microsoft Corporation", "US5949181045")],
        aliases=aliases,
        coverage_report=coverage_report,
    )
    by_key = {quality_row.listing_key: quality_row for quality_row in report_rows}

    assert by_key["NASDAQ::MSFT"].quality_status == "pass"
    assert by_key["NASDAQ::MSFT"].evidence_level == "official_reference"
    assert by_key["TSE::7203"].quality_status == "source_gap"
    assert any(issue.issue_type == "expected_missing_primary_isin" for issue in by_key["TSE::7203"].issues)
    assert by_key["PSX::LOADSR1"].quality_status == "quarantine"
    assert any(issue.issue_type == "blocked_alias_present" for issue in by_key["PSX::LOADSR1"].issues)
    assert by_key["XETRA::MSF"].quality_status == "warn"
    assert any(issue.issue_type == "primary_listing_key_missing_from_listings" for issue in by_key["XETRA::MSF"].issues)


def test_entry_quality_writes_complete_csv_and_markdown_summary(tmp_path):
    listings = [row("NASDAQ::MSFT", "MSFT", "NASDAQ", "Microsoft Corporation", isin="US5949181045", aliases="microsoft")]
    report_rows = assess_entries(
        listings,
        tickers=listings,
        scopes=[scope("NASDAQ::MSFT", "MSFT", "NASDAQ", isin="US5949181045")],
        identifiers=[{"listing_key": "NASDAQ::MSFT", "ticker": "MSFT", "exchange": "NASDAQ", "isin": "US5949181045", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("MSFT", "NASDAQ", "Microsoft Corporation", "US5949181045")],
        aliases=[{"ticker": "MSFT", "alias": "microsoft", "alias_type": "name"}],
        coverage_report={"by_exchange": [{"exchange": "NASDAQ", "venue_status": "official_full"}]},
    )
    csv_path = tmp_path / "entry_quality.csv"
    md_path = tmp_path / "entry_quality.md"
    write_csv(csv_path, report_rows)
    payload = summarize(report_rows, utc_now(), csv_path)
    write_markdown(md_path, payload)

    csv_rows = list(csv.DictReader(csv_path.open()))
    assert len(csv_rows) == 1
    assert csv_rows[0]["listing_key"] == "NASDAQ::MSFT"
    assert csv_rows[0]["quality_status"] == "pass"
    assert "Entry Quality Report" in md_path.read_text()


def test_entry_quality_does_not_flag_local_script_official_names_as_mismatch():
    listing = row(
        "SZSE::000001",
        "000001",
        "SZSE",
        "Ping An Bank Co Ltd",
        isin="",
        stock_sector="Financials",
        aliases="ping an bank",
        country="China",
        country_code="CN",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("SZSE::000001", "000001", "SZSE", isin="")],
        identifiers=[{"listing_key": "SZSE::000001", "ticker": "000001", "exchange": "SZSE", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("000001", "SZSE", "平安银行股份有限公司", "", asset_type="Stock")],
        aliases=[{"ticker": "000001", "alias": "ping an bank", "alias_type": "name"}],
        coverage_report={"by_exchange": [{"exchange": "SZSE", "venue_status": "official_partial"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_official_otc_abbreviated_names():
    listing = row(
        "OTC::ADHC",
        "ADHC",
        "OTC",
        "American Diversified Holdings Corp",
        isin="US02541R3003",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::ADHC", "ADHC", "OTC", isin="US02541R3003", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::ADHC", "ticker": "ADHC", "exchange": "OTC", "isin": "US02541R3003", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("ADHC", "OTC", "AMER DIVRSFD HLDGS CORP", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_unsponsored_adr_wrapper_names():
    listing = row(
        "OTC::AACAY",
        "AACAY",
        "OTC",
        "AAC Technologies Holdings Inc",
        isin="US0003041052",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::AACAY", "AACAY", "OTC", isin="US0003041052", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::AACAY", "ticker": "AACAY", "exchange": "OTC", "isin": "US0003041052", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("AACAY", "OTC", "AAC TECHS HLDGS UNSP/ADR", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_bank_adr_abbreviations():
    listing = row(
        "OTC::AAVMY",
        "AAVMY",
        "OTC",
        "ABN AMRO Bank N.V",
        isin="NL0011540547",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::AAVMY", "AAVMY", "OTC", isin="NL0011540547", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::AAVMY", "ticker": "AAVMY", "exchange": "OTC", "isin": "NL0011540547", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("AAVMY", "OTC", "ABN AMR BK N V UNSP/ADR", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_power_and_telephone_abbreviations():
    listing = row(
        "OTC::APTL",
        "APTL",
        "OTC",
        "Alaska Power & Telephone Company",
        isin="US0117642068",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::APTL", "APTL", "OTC", isin="US0117642068", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::APTL", "ticker": "APTL", "exchange": "OTC", "isin": "US0117642068", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("APTL", "OTC", "ALASKA PWR & TEL CO", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_berhad_abbreviation():
    listing = row(
        "OTC::BRYAF",
        "BRYAF",
        "OTC",
        "Berjaya Corporation Berhad",
        isin="",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::BRYAF", "BRYAF", "OTC", isin="", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::BRYAF", "ticker": "BRYAF", "exchange": "OTC", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("BRYAF", "OTC", "Berjaya Corporation BHD", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_mining_and_resources_abbreviations():
    listing = row(
        "OTC::BENZF",
        "BENZF",
        "OTC",
        "Benz Mining Corp",
        isin="CA08279D2081",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::BENZF", "BENZF", "OTC", isin="CA08279D2081", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::BENZF", "ticker": "BENZF", "exchange": "OTC", "isin": "CA08279D2081", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("BENZF", "OTC", "BENZ MNG CORP", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_development_wrapper_abbreviations():
    listing = row(
        "OTC::HLDCY",
        "HLDCY",
        "OTC",
        "Henderson Land Development",
        isin="US42506L1070",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::HLDCY", "HLDCY", "OTC", isin="US42506L1070", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::HLDCY", "ticker": "HLDCY", "exchange": "OTC", "isin": "US42506L1070", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("HLDCY", "OTC", "HENDERSON LD DEV S/ADR", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_management_and_share_class_abbreviations():
    listing = row(
        "OTC::AGFMF",
        "AGFMF",
        "OTC",
        "AGF Management Limited",
        isin="CA0010931027",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::AGFMF", "AGFMF", "OTC", isin="CA0010931027", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::AGFMF", "ticker": "AGFMF", "exchange": "OTC", "isin": "CA0010931027", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("AGFMF", "OTC", "AGF MGMT LTD B", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_education_and_communities_abbreviations():
    listing = row(
        "OTC::GECSF",
        "GECSF",
        "OTC",
        "Global Education Communities Corp.",
        isin="CA37960M1041",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::GECSF", "GECSF", "OTC", isin="CA37960M1041", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::GECSF", "ticker": "GECSF", "exchange": "OTC", "isin": "CA37960M1041", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("GECSF", "OTC", "GLOBAL ED CMNTYS CORP", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_properties_and_signal_abbreviations():
    listing = row(
        "OTC::GZUHF",
        "GZUHF",
        "OTC",
        "Guangzhou R&F Properties Co. Ltd",
        isin="CNE1000000N2",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::GZUHF", "GZUHF", "OTC", isin="CNE1000000N2", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::GZUHF", "ticker": "GZUHF", "exchange": "OTC", "isin": "CNE1000000N2", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("GZUHF", "OTC", "GUANGZHOU R&F PPTYS CO H", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_bancorp_and_location_abbreviations():
    listing = row(
        "OTC::CZBC",
        "CZBC",
        "OTC",
        "Citizens Bancorp",
        isin="US1729501072",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::CZBC", "CZBC", "OTC", isin="US1729501072", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::CZBC", "ticker": "CZBC", "exchange": "OTC", "isin": "US1729501072", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("CZBC", "OTC", "CITZNS BNCRP CORVALLIS OR", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_suppresses_stale_otc_name_after_reviewed_override():
    listing = row(
        "OTC::LCHTF",
        "LCHTF",
        "OTC",
        "Text S.A.",
        isin="COD04PA00014",
        country="Poland",
        country_code="PL",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::LCHTF", "LCHTF", "OTC", isin="COD04PA00014", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::LCHTF", "ticker": "LCHTF", "exchange": "OTC", "isin": "COD04PA00014", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[
            official_ref(
                "LCHTF",
                "OTC",
                "LIVECHAT SOFTWARE SA",
                "",
                asset_type="Stock",
                source_key="otc_markets_stock_screener",
                provider="OTC Markets",
            )
        ],
        metadata_updates=[
            {
                "ticker": "LCHTF",
                "exchange": "OTC",
                "field": "name",
                "decision": "update",
                "proposed_value": "Text S.A.",
                "confidence": "0.98",
                "reason": "Manual OTC issuer-by-issuer review April 2026: issuer/IR pages and OTC profile use Text S.A.; LiveChat Software is the former name.",
            }
        ],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_accepts_otc_name_when_same_isin_peer_is_officially_confirmed():
    otc_listing = row(
        "OTC::NTULF",
        "NTULF",
        "OTC",
        "BIPROGY Inc.",
        isin="JP3754200008",
        country="Japan",
        country_code="JP",
    )
    tse_listing = row(
        "TSE::8056",
        "8056",
        "TSE",
        "BIPROGY Inc.",
        isin="JP3754200008",
        country="Japan",
        country_code="JP",
    )
    report_rows = assess_entries(
        [otc_listing, tse_listing],
        tickers=[otc_listing, tse_listing],
        scopes=[
            scope("OTC::NTULF", "NTULF", "OTC", isin="JP3754200008", instrument_scope="extended", scope_reason="otc_listing"),
            scope("TSE::8056", "8056", "TSE", isin="JP3754200008"),
        ],
        identifiers=[
            {"listing_key": "OTC::NTULF", "ticker": "NTULF", "exchange": "OTC", "isin": "JP3754200008", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
            {"listing_key": "TSE::8056", "ticker": "8056", "exchange": "TSE", "isin": "JP3754200008", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        ],
        masterfiles=[
            official_ref("NTULF", "OTC", "NIHON UNISYS LTD ORD", "", asset_type="Stock"),
            official_ref("8056", "TSE", "BIPROGY Inc.", "JP3754200008", asset_type="Stock"),
        ],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}, {"exchange": "TSE", "venue_status": "official_full"}]},
    )

    by_key = {quality_row.listing_key: quality_row for quality_row in report_rows}
    assert all(issue.issue_type != "official_name_mismatch" for issue in by_key["OTC::NTULF"].issues)


def test_entry_quality_keeps_otc_false_peer_overlap_visible():
    otc_listing = row(
        "OTC::FLLLF",
        "FLLLF",
        "OTC",
        "Feel Foods Ltd",
        isin="US8322482071",
    )
    nasdaq_listing = row(
        "NASDAQ::SFD",
        "SFD",
        "NASDAQ",
        "Smithfield Foods, Inc. Common Stock",
        isin="US8322482071",
    )
    report_rows = assess_entries(
        [otc_listing, nasdaq_listing],
        tickers=[otc_listing, nasdaq_listing],
        scopes=[
            scope("OTC::FLLLF", "FLLLF", "OTC", isin="US8322482071", instrument_scope="extended", scope_reason="otc_listing"),
            scope("NASDAQ::SFD", "SFD", "NASDAQ", isin="US8322482071"),
        ],
        identifiers=[
            {"listing_key": "OTC::FLLLF", "ticker": "FLLLF", "exchange": "OTC", "isin": "US8322482071", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
            {"listing_key": "NASDAQ::SFD", "ticker": "SFD", "exchange": "NASDAQ", "isin": "US8322482071", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        ],
        masterfiles=[
            official_ref("FLLLF", "OTC", "ULTRA BRANDS LTD", "", asset_type="Stock"),
            official_ref("SFD", "NASDAQ", "Smithfield Foods, Inc.", "US8322482071", asset_type="Stock"),
        ],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}, {"exchange": "NASDAQ", "venue_status": "official_full"}]},
    )

    by_key = {quality_row.listing_key: quality_row for quality_row in report_rows}
    assert any(issue.issue_type == "official_name_mismatch" for issue in by_key["OTC::FLLLF"].issues)


def test_entry_quality_accepts_otc_name_with_ascii_legal_form_peer_corroboration():
    otc_listing = row(
        "OTC::TVFCF",
        "TVFCF",
        "OTC",
        "Télévision Française 1 Société anonyme",
        isin="FR0000054900",
        country="France",
        country_code="FR",
    )
    lse_listing = row(
        "LSE::0NQT",
        "0NQT",
        "LSE",
        "Television Francaise 1 SA",
        isin="FR0000054900",
        country="France",
        country_code="FR",
    )
    report_rows = assess_entries(
        [otc_listing, lse_listing],
        tickers=[otc_listing, lse_listing],
        scopes=[
            scope("OTC::TVFCF", "TVFCF", "OTC", isin="FR0000054900", instrument_scope="extended", scope_reason="otc_listing"),
            scope("LSE::0NQT", "0NQT", "LSE", isin="FR0000054900"),
        ],
        identifiers=[
            {"listing_key": "OTC::TVFCF", "ticker": "TVFCF", "exchange": "OTC", "isin": "FR0000054900", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
            {"listing_key": "LSE::0NQT", "ticker": "0NQT", "exchange": "LSE", "isin": "FR0000054900", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        ],
        masterfiles=[
            official_ref("TVFCF", "OTC", "TELEVISION FRANCHISE", "", asset_type="Stock"),
            official_ref("0NQT", "LSE", "Television Francaise 1 SA", "FR0000054900", asset_type="Stock"),
        ],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}, {"exchange": "LSE", "venue_status": "official_full"}]},
    )

    by_key = {quality_row.listing_key: quality_row for quality_row in report_rows}
    assert all(issue.issue_type != "official_name_mismatch" for issue in by_key["OTC::TVFCF"].issues)


def test_entry_quality_keeps_true_otc_rename_mismatch_visible():
    listing = row(
        "OTC::AECX",
        "AECX",
        "OTC",
        "CurrentC Power Corporation",
        isin="US92855W2017",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::AECX", "AECX", "OTC", isin="US92855W2017", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::AECX", "ticker": "AECX", "exchange": "OTC", "isin": "US92855W2017", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("AECX", "OTC", "ACADIA ENERGY CORP", "", asset_type="Stock")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
    )

    assert any(issue.issue_type == "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_suppresses_reviewed_otc_hold_from_active_mismatch_queue():
    listing = row(
        "OTC::HKRHF",
        "HKRHF",
        "OTC",
        "3DG Holdings (International) Limited",
        isin="BMG4587L1090",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("OTC::HKRHF", "HKRHF", "OTC", isin="BMG4587L1090", instrument_scope="extended", scope_reason="otc_listing")],
        identifiers=[{"listing_key": "OTC::HKRHF", "ticker": "HKRHF", "exchange": "OTC", "isin": "BMG4587L1090", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("HKRHF", "OTC", "HONG KONG RESOURCES HOLDINGS CO LTD", "", asset_type="Stock", source_key="otc_markets_stock_screener", provider="OTC Markets")],
        aliases=[],
        coverage_report={"by_exchange": [{"exchange": "OTC", "venue_status": "official_full"}]},
        otc_review_decisions=[{"ticker": "HKRHF", "exchange": "OTC", "decision": "keep_current_reviewed", "confidence": "high", "reason": "Reviewed stale OTC official name."}],
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_prefers_matching_official_isin_over_short_name_mismatch():
    listing = row(
        "KRX::000180",
        "000180",
        "KRX",
        "Sungchang Hold",
        isin="KR7000180000",
        stock_sector="Industrials",
        aliases="sungchang hold",
        country="South Korea",
        country_code="KR",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("KRX::000180", "000180", "KRX", isin="KR7000180000")],
        identifiers=[{"listing_key": "KRX::000180", "ticker": "000180", "exchange": "KRX", "isin": "KR7000180000", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("000180", "KRX", "SCE Holdings", "KR7000180000", asset_type="Stock")],
        aliases=[{"ticker": "000180", "alias": "sungchang hold", "alias_type": "name"}],
        coverage_report={"by_exchange": [{"exchange": "KRX", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_ignores_ticker_like_or_generic_official_names():
    rows = [
        row(
            "BMV::EDUCA18",
            "EDUCA18",
            "BMV",
            "Fideicomiso Irrevocable No. F/3277 en Banco Invex",
            isin="",
            stock_sector="Real Estate",
            aliases="educa",
            country="Mexico",
            country_code="MX",
        ),
        row(
            "BATS::TOXR",
            "TOXR",
            "BATS",
            "21Shares Core Xrp Trust",
            isin="",
            stock_sector="",
            etf_category="",
            aliases="21shares xrp",
            asset_type="ETF",
        ),
    ]
    scopes = [
        scope("BMV::EDUCA18", "EDUCA18", "BMV", isin="", scope_reason="primary_listing_missing_isin"),
        scope("BATS::TOXR", "TOXR", "BATS", isin="", scope_reason="primary_listing_missing_isin", asset_type="ETF"),
    ]
    identifiers = [
        {"listing_key": "BMV::EDUCA18", "ticker": "EDUCA18", "exchange": "BMV", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
        {"listing_key": "BATS::TOXR", "ticker": "TOXR", "exchange": "BATS", "isin": "", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""},
    ]
    refs = [
        official_ref("EDUCA18", "BMV", "EDUCA 18", "", asset_type="Stock"),
        official_ref("TOXR", "BATS", "Shares of Beneficial Interest", "", asset_type="ETF"),
    ]
    report_rows = assess_entries(
        rows,
        tickers=rows,
        scopes=scopes,
        identifiers=identifiers,
        masterfiles=refs,
        aliases=[],
        coverage_report={
            "by_exchange": [
                {"exchange": "BMV", "venue_status": "official_partial"},
                {"exchange": "BATS", "venue_status": "official_full"},
            ]
        },
    )

    assert all(
        issue.issue_type != "official_name_mismatch"
        for quality_row in report_rows
        for issue in quality_row.issues
    )


def test_entry_quality_matches_concatenated_lse_official_names():
    listing = row(
        "LSE::0IUJ",
        "0IUJ",
        "LSE",
        "Inter Parfums Inc.",
        isin="FR0004024222",
        stock_sector="Consumer Staples",
        aliases="inter parfums",
        country="France",
        country_code="FR",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("LSE::0IUJ", "0IUJ", "LSE", isin="FR0004024222")],
        identifiers=[{"listing_key": "LSE::0IUJ", "ticker": "0IUJ", "exchange": "LSE", "isin": "FR0004024222", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("0IUJ", "LSE", "INTERPARFUMS SA INTERPARFUMS ORD SHS", "", asset_type="Stock")],
        aliases=[{"ticker": "0IUJ", "alias": "inter parfums", "alias_type": "name"}],
        coverage_report={"by_exchange": [{"exchange": "LSE", "venue_status": "official_full"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)


def test_entry_quality_ignores_local_script_name_with_single_latin_acronym():
    listing = row(
        "SSE::512220",
        "512220",
        "SSE",
        "IGW China Securities TMT 150 ETF",
        isin="CNE1000028N4",
        stock_sector="",
        etf_category="",
        aliases="igw china securities tmt 150 etf",
        asset_type="ETF",
        country="China",
        country_code="CN",
    )
    report_rows = assess_entries(
        [listing],
        tickers=[listing],
        scopes=[scope("SSE::512220", "512220", "SSE", isin="CNE1000028N4", asset_type="ETF")],
        identifiers=[{"listing_key": "SSE::512220", "ticker": "512220", "exchange": "SSE", "isin": "CNE1000028N4", "wkn": "", "figi": "", "cik": "", "lei": "", "figi_source": "", "cik_source": "", "lei_source": ""}],
        masterfiles=[official_ref("512220", "SSE", "TMTETF景顺", "", asset_type="ETF")],
        aliases=[{"ticker": "512220", "alias": "igw china securities tmt 150 etf", "alias_type": "name"}],
        coverage_report={"by_exchange": [{"exchange": "SSE", "venue_status": "official_partial"}]},
    )

    assert all(issue.issue_type != "official_name_mismatch" for issue in report_rows[0].issues)
