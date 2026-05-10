from scripts.backfill_tradingview_missing_isins import TradingViewRow
from scripts.backfill_tradingview_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_tradingview_sector,
)


def tv_row(**overrides):
    values = {
        "request_symbol": "SET:PTT",
        "symbol": "PTT",
        "name": "PTT Public Co., Ltd.",
        "exchange": "SET",
        "instrument_type": "stock",
        "subtype": "common",
        "isin": "TH0646010Z00",
        "sector": "Energy Minerals",
        "industry": "Integrated Oil",
        "country": "Thailand",
        "typespecs": ("common",),
    }
    values.update(overrides)
    return TradingViewRow(**values)


def target_row(**overrides):
    values = {
        "ticker": "PTT",
        "exchange": "SET",
        "asset_type": "Stock",
        "name": "PTT Public Company Limited",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def test_map_tradingview_sector_to_canonical_stock_sector():
    assert map_tradingview_sector("Energy Minerals", "Integrated Oil") == "Energy"
    assert map_tradingview_sector("Commercial Services", "Miscellaneous Commercial Services") == "Industrials"
    assert map_tradingview_sector("Finance", "Major Banks") == "Financials"
    assert map_tradingview_sector("Finance", "Real Estate Development") == "Real Estate"
    assert map_tradingview_sector("Finance", "Financial Conglomerates") == ""
    assert map_tradingview_sector("Consumer Services", "Media Conglomerates") == "Communication Services"
    assert map_tradingview_sector("Distribution Services", "Food Distributors") == "Consumer Staples"
    assert map_tradingview_sector("Electronic Technology", "Aerospace & Defense") == "Industrials"
    assert map_tradingview_sector("Process Industries", "Agricultural Commodities/Milling") == "Consumer Staples"
    assert map_tradingview_sector("Retail Trade", "Food Retail") == "Consumer Staples"
    assert map_tradingview_sector("Retail Trade", "Specialty Stores") == "Consumer Discretionary"
    assert map_tradingview_sector("Miscellaneous", "Investment Trusts/Mutual Funds") == ""


def test_evaluate_row_accepts_clear_sector_match():
    result = evaluate_row(target_row(), tv_row())
    assert result["decision"] == "accept"
    assert result["sector_update"] == "Energy"


def test_evaluate_row_rejects_name_mismatch():
    result = evaluate_row(target_row(), tv_row(name="Unrelated Company"))
    assert result["decision"] == "name_mismatch"


def test_evaluate_row_accepts_same_isin_name_variant():
    result = evaluate_row(target_row(name="PTT PCL", isin="TH0646010Z00"), tv_row(name="PTT Public Co., Ltd."))
    assert result["decision"] == "accept"
    assert result["sector_update"] == "Energy"


def test_evaluate_row_rejects_unsupported_sector():
    result = evaluate_row(target_row(), tv_row(sector="Miscellaneous", industry="Investment Trusts/Mutual Funds"))
    assert result["decision"] == "unsupported_sector"


def test_build_metadata_updates_emits_stock_sector_update():
    updates = build_metadata_updates([evaluate_row(target_row(), tv_row())])
    assert updates[0]["field"] == "stock_sector"
    assert updates[0]["proposed_value"] == "Energy"
    assert updates[0]["confidence"] == "0.72"
