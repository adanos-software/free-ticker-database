from __future__ import annotations

from scripts.build_coverage_report import (
    build_country_report,
    build_exchange_report,
    build_global_summary,
    render_markdown,
)


def test_build_exchange_report_includes_masterfile_match_rates():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "IBM", "exchange": "NYSE", "isin": "", "sector": "Information Technology"},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "", "lei": ""},
        {"ticker": "IBM", "exchange": "NYSE", "cik": "0000051143", "figi": "BBG000BLNNH6", "lei": ""},
    ]
    masterfiles = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "QQQ", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "IBM", "exchange": "NYSE", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "SHOP", "exchange": "TSX", "listing_status": "active", "reference_scope": "interlisted_subset"},
    ]

    rows = build_exchange_report(tickers, identifiers, masterfiles)

    assert rows == [
        {
            "exchange": "NASDAQ",
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 0,
            "lei_coverage": 0,
            "masterfile_symbols": 2,
            "masterfile_matches": 1,
            "masterfile_match_rate": 50.0,
        },
        {
            "exchange": "NYSE",
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
            "masterfile_symbols": 1,
            "masterfile_matches": 1,
            "masterfile_match_rate": 100.0,
        },
    ]


def test_build_country_report_summarizes_identifier_coverage():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "country": "United States", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "SHOP", "exchange": "TSX", "country": "Canada", "isin": "", "sector": ""},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "BBG000B9XRY4", "lei": ""},
        {"ticker": "SHOP", "exchange": "TSX", "cik": "", "figi": "", "lei": ""},
    ]

    rows = build_country_report(tickers, identifiers)

    assert rows == [
        {
            "country": "Canada",
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 0,
            "cik_coverage": 0,
            "figi_coverage": 0,
            "lei_coverage": 0,
        },
        {
            "country": "United States",
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
        },
    ]


def test_global_summary_and_markdown_render():
    report = {
        "global": build_global_summary(
            tickers=[{"asset_type": "Stock", "isin": "X", "sector": "Y"}],
            aliases=[{"ticker": "AAA", "alias": "alpha"}],
            identifiers_extended=[{"cik": "1", "figi": "", "lei": ""}],
            listing_status_history=[{"ticker": "AAA"}],
            listing_events=[{"ticker": "AAA"}],
        ),
        "exchange_coverage": [],
        "country_coverage": [],
    }

    markdown = render_markdown(report)

    assert report["global"]["tickers"] == 1
    assert report["global"]["cik_coverage"] == 1
    assert "# Coverage Report" in markdown
