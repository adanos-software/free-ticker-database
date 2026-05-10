from scripts.backfill_twse_isin_sectors import (
    build_metadata_updates,
    evaluate_row,
    normalize_twse_industry_sector,
    parse_twse_isin_html,
)


HTML = """
<table>
<tr><td>Security Code & Security Name</td><td>ISIN Code</td><td>Date Listed</td><td>Market</td><td>Industrial Group</td><td>CFICode</td><td>Remarks</td></tr>
<tr><td>Stocks</td><td>Stocks</td><td>Stocks</td><td>Stocks</td><td>Stocks</td><td>Stocks</td><td>Stocks</td></tr>
<tr><td>1256¡@SUNJUICE</td><td>KYG858681003</td><td>2016/03/17</td><td>TWSE LISTED</td><td>Food</td><td>ESVUFR</td><td></td></tr>
<tr><td>00625K¡@FB SSE180+R</td><td>TW00000625K4</td><td>2016/08/08</td><td>TWSE LISTED</td><td></td><td>CEOGEU</td><td></td></tr>
</table>
"""


def test_parse_twse_isin_html_extracts_exact_security_codes():
    rows = parse_twse_isin_html(HTML)

    assert rows[0].ticker == "1256"
    assert rows[0].security_name == "SUNJUICE"
    assert rows[0].isin == "KYG858681003"
    assert rows[0].industry == "Food"
    assert rows[1].ticker == "00625K"
    assert rows[1].isin == "TW00000625K4"


def test_normalize_twse_industry_sector_maps_official_industries():
    assert normalize_twse_industry_sector("Food") == "Consumer Staples"
    assert normalize_twse_industry_sector("Electric Machinery") == "Industrials"
    assert normalize_twse_industry_sector("Biotechnology and Medical Care") == "Health Care"
    assert normalize_twse_industry_sector("") == ""


def test_evaluate_row_accepts_exact_ticker_isin_and_sector():
    source = parse_twse_isin_html(HTML)[0]
    result = evaluate_row(
        {"ticker": "1256", "exchange": "TWSE", "asset_type": "Stock", "name": "鮮活控股股份有限公司", "isin": "", "stock_sector": ""},
        source,
    )

    assert result["decision"] == "accept"
    assert result["isin_update"] == "KYG858681003"
    assert result["sector_update"] == "Consumer Staples"


def test_evaluate_row_accepts_etf_isin_without_stock_sector():
    source = parse_twse_isin_html(HTML)[1]
    result = evaluate_row(
        {"ticker": "00625K", "exchange": "TWSE", "asset_type": "ETF", "name": "Fubon SSE180 ETF", "isin": "", "stock_sector": ""},
        source,
    )

    assert result["decision"] == "accept"
    assert result["isin_update"] == "TW00000625K4"
    assert result["sector_update"] == ""


def test_build_metadata_updates_emits_separate_fields():
    source = parse_twse_isin_html(HTML)[0]
    result = evaluate_row(
        {"ticker": "1256", "exchange": "TWSE", "asset_type": "Stock", "name": "鮮活控股股份有限公司", "isin": "", "stock_sector": ""},
        source,
    )
    updates = build_metadata_updates([result])

    assert [update["field"] for update in updates] == ["isin", "stock_sector"]
