from __future__ import annotations

from scripts.verify_yahoo_listings import (
    evaluate_row,
    expected_exchange_match,
    expected_quote_type_match,
    normalize_isin,
    yahoo_symbol_candidates,
)


def test_yahoo_symbol_candidates_cover_us_and_suffix_exchanges():
    assert yahoo_symbol_candidates("AAPL", "NASDAQ") == ["AAPL"]
    assert yahoo_symbol_candidates("0AD5", "LSE") == ["0AD5.L"]
    assert yahoo_symbol_candidates("PKO", "WSE") == ["PKO.WA"]
    assert yahoo_symbol_candidates("BOL", "Euronext") == []


def test_expected_exchange_and_quote_type_matching():
    assert expected_exchange_match("NASDAQ", "NMS", "NasdaqGS") is True
    assert expected_exchange_match("NYSE ARCA", "PCX", "NYSEArca") is True
    assert expected_exchange_match("NYSE", "NMS", "NasdaqGS") is False
    assert expected_exchange_match("Euronext", "PAR", "Paris") is None

    assert expected_quote_type_match("Stock", "EQUITY") is True
    assert expected_quote_type_match("ETF", "ETF") is True
    assert expected_quote_type_match("ETF", "EQUITY") is False


def test_normalize_isin_maps_dash_to_empty_string():
    assert normalize_isin("-") == ""
    assert normalize_isin("US0378331005") == "US0378331005"


def test_evaluate_row_detects_verified_and_mismatch_states():
    row = {
        "ticker": "GAL",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "SPDR SSgA Global Allocation ETF",
        "country": "United States",
        "isin": "",
    }

    verified = evaluate_row(
        row,
        {
            "exists": True,
            "symbol": "GAL",
            "quoteType": "ETF",
            "shortName": "State Street Global Allocation ",
            "longName": "State Street Global Allocation ETF",
            "exchange": "PCX",
            "fullExchangeName": "NYSEArca",
            "country": "United States",
            "sector": "",
            "isin": "US78467V4005",
            "history_rows": 5,
        },
    )
    assert verified["status"] == "verified"
    assert verified["quote_type_match"] is True
    assert verified["exchange_match"] is True
    assert verified["name_match"] is True
    assert verified["yahoo_sector"] == ""

    mismatch = evaluate_row(
        row,
        {
            "exists": True,
            "symbol": "GAL",
            "quoteType": "EQUITY",
            "shortName": "Random Company",
            "longName": "Random Company",
            "exchange": "NMS",
            "fullExchangeName": "NasdaqGS",
            "country": "Canada",
            "sector": "Technology",
            "isin": "",
            "history_rows": 5,
        },
    )
    assert mismatch["status"] == "mismatch"
    assert mismatch["quote_type_match"] is False
    assert mismatch["exchange_match"] is False
