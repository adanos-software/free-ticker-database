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


def official_ref(ticker: str, exchange: str, name: str, isin: str, asset_type: str = "Stock") -> dict[str, str]:
    return {
        "source_key": "test_masterfile",
        "provider": "Test Exchange",
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
