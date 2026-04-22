from __future__ import annotations

from scripts.build_override_debt_report import build_override_debt_report


def test_build_override_debt_report_tracks_open_and_canonical_metadata():
    tickers = [
        {
            "ticker": "TWSE1",
            "exchange": "TWSE",
            "name": "Leveraged ETF",
            "asset_type": "ETF",
            "stock_sector": "",
            "etf_category": "Other",
            "country": "Taiwan",
            "country_code": "TW",
            "isin": "TW0000000001",
            "aliases": "leveraged etf",
        },
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "name": "Alpha Inc.",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "etf_category": "",
            "country": "United States",
            "country_code": "US",
            "isin": "US0000000001",
            "aliases": "alpha",
        },
    ]
    metadata_updates = [
        {
            "ticker": "TWSE1",
            "exchange": "TWSE",
            "field": "etf_category",
            "decision": "update",
            "proposed_value": "Trading",
        },
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "country",
            "decision": "update",
            "proposed_value": "Canada",
        },
    ]
    remove_aliases = [
        {"ticker": "AAA", "exchange": "NASDAQ", "alias": "alpha"},
        {"ticker": "TWSE1", "exchange": "TWSE", "alias": "gone"},
    ]

    report = build_override_debt_report(
        tickers=tickers,
        metadata_updates=metadata_updates,
        remove_aliases=remove_aliases,
        generated_at="2026-04-22T00:00:00Z",
    )

    assert report["summary"]["metadata_resolved_canonical"] == 1
    assert report["summary"]["metadata_partial_canonical"] == 0
    assert report["summary"]["metadata_open"] == 1
    assert report["summary"]["alias_open"] == 1
    assert report["open_metadata_by_field"] == {"country": 1}
    assert report["open_alias_by_exchange"] == {"NASDAQ": 1}


def test_build_override_debt_report_marks_partial_alias_metadata_as_non_open():
    tickers = [
        {
            "ticker": "DGR",
            "exchange": "XETRA",
            "name": "Deutsche Grundstücksauktionen AG",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "etf_category": "",
            "country": "Germany",
            "country_code": "DE",
            "isin": "DE0005533400",
            "aliases": "deutsche grundstucksauktionen",
        }
    ]
    metadata_updates = [
        {
            "ticker": "DGR",
            "exchange": "XETRA",
            "field": "aliases",
            "decision": "update",
            "proposed_value": "deutsche grundstücksauktionen|deutsche grundstuecksauktionen",
        }
    ]

    report = build_override_debt_report(
        tickers=tickers,
        metadata_updates=metadata_updates,
        remove_aliases=[],
        generated_at="2026-04-22T00:00:00Z",
    )

    assert report["summary"]["metadata_partial_canonical"] == 1
    assert report["summary"]["metadata_open"] == 0


def test_build_override_debt_report_marks_filtered_alias_override_resolved_by_policy():
    tickers = [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "name": "Alpha Inc.",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "etf_category": "",
            "country": "United States",
            "country_code": "US",
            "isin": "US0000000001",
            "aliases": "",
        },
    ]
    metadata_updates = [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "aliases",
            "decision": "update",
            "proposed_value": "1234",
        }
    ]

    report = build_override_debt_report(
        tickers=tickers,
        metadata_updates=metadata_updates,
        remove_aliases=[],
        generated_at="2026-04-22T00:00:00Z",
    )

    assert report["summary"]["metadata_resolved_policy"] == 1
    assert report["summary"]["metadata_open"] == 0
