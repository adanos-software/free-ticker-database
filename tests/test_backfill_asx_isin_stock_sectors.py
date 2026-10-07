from io import BytesIO

import pandas as pd

from scripts.backfill_asx_isin_stock_sectors import (
    asx_security_is_equity,
    build_metadata_updates,
    evaluate_row,
    index_by_isin,
    parse_asx_isin_equity_rows,
    parse_asx_listed_sector_rows,
)


def target(**overrides):
    values = {
        "ticker": "1IP",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "IPH LTD",
        "isin": "AU000000IPH9",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def listed_csv() -> str:
    return (
        "ASX listed companies as at Wed Oct 07 00:16:10 AEDT 2026\n"
        "\n"
        "Company name,ASX code,GICS industry group\n"
        '"IPH LIMITED","IPH","Commercial & Professional Services"\n'
        '"WAYPOINT REIT","WPR","Equity Real Estate Investment Trusts (REITs)"\n'
        '"XPEDRA RESOURCES LIMITED","XPD",""\n'
    )


def isin_xls() -> bytes:
    buffer = BytesIO()
    dataframe = pd.DataFrame(
        [
            {
                "ASX code": "IPH",
                "Company name": "IPH LIMITED",
                "Security type": "ORDINARY FULLY PAID",
                "ISIN code": "AU000000IPH9",
            },
            {
                "ASX code": "IPH",
                "Company name": "IPH LIMITED",
                "Security type": "OPTION EXPIRING VARIOUS DATES EX VARIOUS PRICES",
                "ISIN code": "AU0000IPHOPT",
            },
            {
                "ASX code": "WPR",
                "Company name": "WAYPOINT REIT",
                "Security type": "FULLY PAID ORDINARY/UNITS STAPLED SECURITIES",
                "ISIN code": "AU0000088064",
            },
            {
                "ASX code": "XPD",
                "Company name": "XPEDRA RESOURCES LIMITED",
                "Security type": "ORDINARY FULLY PAID",
                "ISIN code": "AU0000442931",
            },
        ]
    )
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        dataframe.to_excel(writer, sheet_name="ISIN", index=False)
    return buffer.getvalue()


def indexed():
    return index_by_isin(parse_asx_isin_equity_rows(isin_xls())), parse_asx_listed_sector_rows(listed_csv())


def test_asx_security_is_equity_keeps_ordinary_and_stapled_only():
    assert asx_security_is_equity("ORDINARY FULLY PAID")
    assert asx_security_is_equity("FULLY PAID ORDINARY/UNITS STAPLED SECURITIES")
    assert not asx_security_is_equity("OPTION EXPIRING VARIOUS DATES EX VARIOUS PRICES")
    assert not asx_security_is_equity("WARRANT")
    assert not asx_security_is_equity("")


def test_parse_asx_isin_equity_rows_drops_options_and_keeps_stapled():
    rows = parse_asx_isin_equity_rows(isin_xls())
    assert {(row.ticker, row.isin) for row in rows} == {
        ("IPH", "AU000000IPH9"),
        ("WPR", "AU0000088064"),
        ("XPD", "AU0000442931"),
    }


def test_evaluate_row_accepts_exact_isin_with_unique_gics():
    by_isin, listed = indexed()
    result = evaluate_row(target(), by_isin, listed)

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Industrials"
    assert result["asx_ticker"] == "IPH"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Industrials"


def test_evaluate_row_accepts_stapled_reit_isin():
    by_isin, listed = indexed()
    result = evaluate_row(
        target(ticker="1V2", name="WAYPOINT REIT UTS", isin="AU0000088064"),
        by_isin,
        listed,
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Real Estate"
    assert result["asx_ticker"] == "WPR"


def test_evaluate_row_rejects_missing_gics_on_listed_row():
    by_isin, listed = indexed()
    result = evaluate_row(
        target(ticker="LFY", name="Xpedra Resources Limited", isin="AU0000442931"),
        by_isin,
        listed,
    )

    assert result["decision"] == "empty_asx_gics"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_unknown_isin():
    by_isin, listed = indexed()
    result = evaluate_row(target(isin="AU000000BHP4"), by_isin, listed)

    assert result["decision"] == "no_asx_isin_match"
    assert result["sector_update"] == ""
