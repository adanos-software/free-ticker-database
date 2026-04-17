from __future__ import annotations

import json

from scripts.build_adanos_ticker_reference import build_natural_alias_rows, build_ticker_reference_rows


def test_adanos_reference_exports_only_safe_natural_language_aliases():
    tickers = [
        {
            "ticker": "MSFT",
            "name": "Microsoft Corporation",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "etf_category": "",
            "country": "United States",
            "country_code": "US",
            "isin": "US5949181045",
        }
    ]
    aliases = [
        {"ticker": "MSFT", "alias": "microsoft", "alias_type": "name"},
        {"ticker": "MSFT", "alias": "US5949181045", "alias_type": "isin"},
        {"ticker": "MSFT", "alias": "leo", "alias_type": "name"},
    ]

    natural_aliases = build_natural_alias_rows(tickers, aliases)
    reference_rows = build_ticker_reference_rows(tickers, natural_aliases)

    assert [row["alias"] for row in natural_aliases] == ["leo", "microsoft"]
    assert {row["alias"]: row["detection_policy"] for row in natural_aliases} == {
        "leo": "context_required",
        "microsoft": "safe_natural_language",
    }
    assert reference_rows[0]["sector"] == "Information Technology"
    assert json.loads(reference_rows[0]["aliases"]) == ["microsoft"]


def test_adanos_reference_uses_etf_category_as_legacy_sector():
    tickers = [
        {
            "ticker": "SPY",
            "name": "SPDR S&P 500 ETF Trust",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "stock_sector": "",
            "etf_category": "Broad Market ETF",
            "country": "United States",
            "country_code": "US",
            "isin": "US78462F1030",
        }
    ]

    reference_rows = build_ticker_reference_rows(tickers, [])

    assert reference_rows[0]["sector"] == "Broad Market ETF"
    assert json.loads(reference_rows[0]["aliases"]) == []
