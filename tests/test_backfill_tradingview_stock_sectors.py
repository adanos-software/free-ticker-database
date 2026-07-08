import csv

from scripts.backfill_tradingview_missing_isins import TradingViewRow
from scripts.backfill_tradingview_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    load_entry_quality_missing_stock_sector_rows,
    map_tradingview_sector,
    names_match_after_security_suffix_noise,
    write_report_csv,
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


def test_names_match_after_security_suffix_noise_handles_share_class_labels():
    assert names_match_after_security_suffix_noise("DPC Holdings PLC", "DPC Holdings PLC Ordinary Shares")
    assert names_match_after_security_suffix_noise("ITG Inc. Class A", "ITG, Inc. - Class A Common Stock")
    assert not names_match_after_security_suffix_noise("ITG Energy Inc.", "ITG, Inc. - Class A Common Stock")


def test_load_entry_quality_missing_stock_sector_rows_filters_supported_stock_issues(tmp_path):
    path = tmp_path / "entry_quality.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "asset_type", "name", "issue_types"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "BHP",
                    "exchange": "ASX",
                    "asset_type": "Stock",
                    "name": "BHP Group Limited",
                    "issue_types": "official_reference_gap|missing_stock_sector",
                },
                {
                    "ticker": "ETF",
                    "exchange": "ASX",
                    "asset_type": "ETF",
                    "name": "Example ETF",
                    "issue_types": "missing_etf_category",
                },
                {
                    "ticker": "AAA",
                    "exchange": "BK",
                    "asset_type": "Stock",
                    "name": "Unsupported Exchange",
                    "issue_types": "missing_stock_sector",
                },
                {
                    "ticker": "OK",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "name": "Already Complete",
                    "issue_types": "",
                },
            ]
        )

    rows = load_entry_quality_missing_stock_sector_rows(exchanges={"ASX", "BK", "LSE"}, entry_quality_csv=path)

    assert [(row["ticker"], row["exchange"]) for row in rows] == [("BHP", "ASX")]


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


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "report.csv"
    write_report_csv(path, [evaluate_row(target_row(), tv_row())])

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert b"\n" in content
