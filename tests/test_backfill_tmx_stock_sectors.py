from io import BytesIO

from openpyxl import Workbook

from scripts.backfill_tmx_stock_sectors import (
    evaluate_rows,
    load_tmx_sector_rows,
    normalize_tmx_sector,
    ticker_root_symbol,
)


def make_tmx_workbook() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "TSXV Issuers"
    worksheet.append(["Disclaimer"])
    worksheet.append(["Co_ID", "PO ID", "Exchange", "Name", "Root\nTicker", "Market Cap", "Shares", "Sector", "Sub-Sector"])
    worksheet.append(["V-1", "1", "TSXV", "Example Mining Ltd.", "EXM", "100", "10", "Mining", "Gold"])
    worksheet.append(["V-2", "2", "TSXV", "Example CPC Corp.", "CPC.P", "100", "10", "CPC", ""])
    worksheet.append(["T-1", "3", "TSX", "Example Tech Inc.", "TECH", "100", "10", "Technology", ""])
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def test_load_tmx_sector_rows_parses_header_and_rows():
    rows = load_tmx_sector_rows(make_tmx_workbook())

    assert [(row.exchange, row.symbol, row.sector) for row in rows] == [
        ("TSXV", "EXM", "Mining"),
        ("TSXV", "CPC.P", "CPC"),
        ("TSX", "TECH", "Technology"),
    ]


def test_normalize_tmx_sector_maps_supported_official_values():
    assert normalize_tmx_sector("Mining") == "Materials"
    assert normalize_tmx_sector("Technology") == "Information Technology"
    assert normalize_tmx_sector("CPC") == ""


def test_ticker_root_symbol_handles_share_class_suffixes():
    assert ticker_root_symbol("BEK-B") == "BEK"
    assert ticker_root_symbol("CPC.P") == "CPC"


def test_evaluate_rows_accepts_exact_exchange_and_root_symbol():
    result = evaluate_rows(
        [
            {
                "ticker": "EXM-H",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Example Mining Ltd.",
            }
        ],
        load_tmx_sector_rows(make_tmx_workbook()),
    )[0]

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"


def test_evaluate_rows_rejects_unsupported_tmx_sector():
    result = evaluate_rows(
        [
            {
                "ticker": "CPC.P",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Example CPC Corp.",
            }
        ],
        load_tmx_sector_rows(make_tmx_workbook()),
    )[0]

    assert result["decision"] == "unsupported_or_ambiguous_tmx_sector"
