from __future__ import annotations

import csv
import json

from scripts.audit_dataset import (
    analyze_dataset,
    review_items_to_json,
    write_review_csv,
)


def test_analyze_dataset_flags_blocked_and_colliding_aliases():
    ticker_rows = [
        {
            "ticker": "AAA",
            "name": "Alpha Systems Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Information Technology",
            "country": "United States",
            "country_code": "US",
            "isin": "US0378331005",
            "aliases": "alpha|vision",
        },
        {
            "ticker": "BBB",
            "name": "Beta Foods Inc",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "sector": "Consumer Staples",
            "country": "United States",
            "country_code": "US",
            "isin": "US5949181045",
            "aliases": "beta|vision",
        },
        {
            "ticker": "CCC",
            "name": "Consumer Devices Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Consumer Discretionary",
            "country": "United States",
            "country_code": "US",
            "isin": "US88160R1014",
            "aliases": "consumer devices|iphone",
        },
    ]
    alias_rows = [
        {"ticker": "AAA", "alias": "alpha", "alias_type": "name"},
        {"ticker": "AAA", "alias": "vision", "alias_type": "name"},
        {"ticker": "BBB", "alias": "beta", "alias_type": "name"},
        {"ticker": "BBB", "alias": "vision", "alias_type": "name"},
        {"ticker": "CCC", "alias": "consumer devices", "alias_type": "name"},
        {"ticker": "CCC", "alias": "iphone", "alias_type": "name"},
    ]
    identifier_rows = [
        {"ticker": "AAA", "isin": "US0378331005", "wkn": ""},
        {"ticker": "BBB", "isin": "US5949181045", "wkn": ""},
        {"ticker": "CCC", "isin": "US88160R1014", "wkn": ""},
    ]

    review_items = analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=40)

    assert [item.ticker for item in review_items] == ["CCC", "AAA", "BBB"]
    assert any(f.finding_type == "blocked_alias_present" for f in review_items[0].findings)
    assert any(f.finding_type == "cross_company_alias_collision" for f in review_items[1].findings)
    assert any(f.finding_type == "cross_company_alias_collision" for f in review_items[2].findings)


def test_analyze_dataset_respects_min_score_threshold():
    ticker_rows = [
        {
            "ticker": "GOOGX",
            "name": "Alphabet Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Communication Services",
            "country": "United States",
            "country_code": "US",
            "isin": "US0231351067",
            "aliases": "google",
        }
    ]
    alias_rows = [{"ticker": "GOOGX", "alias": "google", "alias_type": "name"}]
    identifier_rows = [{"ticker": "GOOGX", "isin": "US0231351067", "wkn": ""}]

    assert analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=40) == []
    flagged = analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=10)
    assert [item.ticker for item in flagged] == ["GOOGX"]
    assert flagged[0].total_score == 15


def test_review_outputs_include_expected_metadata(tmp_path):
    ticker_rows = [
        {
            "ticker": "CCC",
            "name": "Consumer Devices Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Consumer Discretionary",
            "country": "United States",
            "country_code": "US",
            "isin": "US88160R1014",
            "aliases": "consumer devices|iphone",
        }
    ]
    alias_rows = [
        {"ticker": "CCC", "alias": "consumer devices", "alias_type": "name"},
        {"ticker": "CCC", "alias": "iphone", "alias_type": "name"},
    ]
    identifier_rows = [{"ticker": "CCC", "isin": "US88160R1014", "wkn": ""}]
    review_items = analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=40)

    payload = review_items_to_json(review_items, min_score=40)
    assert payload["_meta"]["flagged_entries"] == 1
    assert payload["summary"]["blocked_alias_present"] == 1

    csv_path = tmp_path / "review_queue.csv"
    write_review_csv(csv_path, review_items)

    rows = list(csv.DictReader(csv_path.open()))
    assert rows
    assert all(row["ticker"] == "CCC" for row in rows)
    assert any(row["finding_type"] == "blocked_alias_present" for row in rows)
    assert any(row["severity"] == "critical" for row in rows)


def test_cross_company_alias_collision_ignores_same_entity_cross_listings():
    ticker_rows = [
        {
            "ticker": "GOOGL",
            "name": "Alphabet Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Communication Services",
            "country": "United States",
            "country_code": "US",
            "isin": "US02079K3059",
            "aliases": "google",
        },
        {
            "ticker": "0RIH",
            "name": "Alphabet Inc",
            "exchange": "LSE",
            "asset_type": "Stock",
            "sector": "Communication Services",
            "country": "United States",
            "country_code": "US",
            "isin": "US02079K3059",
            "aliases": "google",
        },
    ]
    alias_rows = [
        {"ticker": "GOOGL", "alias": "google", "alias_type": "name"},
        {"ticker": "0RIH", "alias": "google", "alias_type": "name"},
    ]
    identifier_rows = [
        {"ticker": "GOOGL", "isin": "US02079K3059", "wkn": ""},
        {"ticker": "0RIH", "isin": "US02079K3059", "wkn": ""},
    ]

    assert analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=40) == []


def test_low_overlap_ignores_symbol_like_aliases():
    ticker_rows = [
        {
            "ticker": "BHF",
            "name": "Brighthouse Financial Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Financials",
            "country": "United States",
            "country_code": "US",
            "isin": "US10922N1037",
            "aliases": "BHFAN",
        }
    ]
    alias_rows = [{"ticker": "BHF", "alias": "BHFAN", "alias_type": "name"}]
    identifier_rows = [{"ticker": "BHF", "isin": "US10922N1037", "wkn": ""}]

    assert analyze_dataset(ticker_rows, alias_rows, identifier_rows, min_score=1) == []
