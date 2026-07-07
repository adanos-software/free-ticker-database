from __future__ import annotations

import csv

from scripts.backfill_reviewed_source_metadata import (
    REVIEWED_SOURCE_UPDATES,
    build_metadata_updates,
    evaluate_updates,
    load_entry_quality_issue_keys,
)


def test_load_entry_quality_issue_keys_tracks_isin_and_category_rows(tmp_path):
    path = tmp_path / "entry_quality.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "issue_types"])
        writer.writeheader()
        writer.writerow({"ticker": "AAA", "exchange": "X", "issue_types": "missing_etf_category"})
        writer.writerow({"ticker": "BBB", "exchange": "X", "issue_types": "expected_missing_primary_isin"})
        writer.writerow({"ticker": "CCC", "exchange": "X", "issue_types": "missing_stock_sector"})

    assert load_entry_quality_issue_keys(path) == {("AAA", "X"), ("BBB", "X"), ("CCC", "X")}


def test_evaluate_updates_accepts_exact_reviewed_rows():
    listing_rows = {
        ("XACTC25", "CPH"): {
            "ticker": "XACTC25",
            "exchange": "CPH",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "SE0011452127",
        },
        ("WT", "NYSE"): {
            "ticker": "WT",
            "exchange": "NYSE",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "US97717P1049",
        },
        ("TTE", "NYSE"): {
            "ticker": "TTE",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "stock_sector": "Energy",
            "isin": "",
        },
        ("RCR.P", "TSXV"): {
            "ticker": "RCR.P",
            "exchange": "TSXV",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "",
        },
        ("ALRIS", "Euronext"): {
            "ticker": "ALRIS",
            "exchange": "Euronext",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "FR00140164Q1",
        },
        ("CVBP", "LSE"): {
            "ticker": "CVBP",
            "exchange": "LSE",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("AXTRART", "SET"): {
            "ticker": "AXTRART",
            "exchange": "SET",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("AGAC", "AMS"): {
            "ticker": "AGAC",
            "exchange": "AMS",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("BLTN", "AMS"): {
            "ticker": "BLTN",
            "exchange": "AMS",
            "asset_type": "ETF",
            "etf_category": "Fixed Income",
            "isin": "",
        },
        ("HYCB", "AMS"): {
            "ticker": "HYCB",
            "exchange": "AMS",
            "asset_type": "ETF",
            "etf_category": "Fixed Income",
            "isin": "",
        },
        ("BFRE", "OTC"): {
            "ticker": "BFRE",
            "exchange": "OTC",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "",
        },
        ("BNOBF", "OTC"): {
            "ticker": "BNOBF",
            "exchange": "OTC",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "",
        },
        ("BREJY", "OTC"): {
            "ticker": "BREJY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US5527501010",
        },
        ("CRTHF", "OTC"): {
            "ticker": "CRTHF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "",
        },
        ("NPPSF", "OTC"): {
            "ticker": "NPPSF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "",
        },
        ("XNJJY", "OTC"): {
            "ticker": "XNJJY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US98421K1007",
        },
        ("OPMNF", "OTC"): {
            "ticker": "OPMNF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "",
        },
        ("FOACW", "OTC"): {
            "ticker": "FOACW",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US31738L1070",
        },
        ("RPTCV", "OTC"): {
            "ticker": "RPTCV",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US38983D3008",
        },
        ("SPMCP", "OTC"): {
            "ticker": "SPMCP",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US83617A1088",
        },
        ("SHCC", "OTC"): {
            "ticker": "SHCC",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Shi Corporation",
            "stock_sector": "",
            "isin": "",
        },
        ("DTZ", "ASX"): {
            "ticker": "DTZ",
            "exchange": "ASX",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "AU000000DTZ4",
        },
        ("DTZNY", "OTC"): {
            "ticker": "DTZNY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US25857G1058",
        },
        ("DTZZF", "OTC"): {
            "ticker": "DTZZF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "AU000000DTZ4",
        },
        ("CVW", "TSXV"): {
            "ticker": "CVW",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "CA12670M1059",
        },
        ("CVWFF", "OTC"): {
            "ticker": "CVWFF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "CA23249L1067",
        },
        ("BENH", "OTC"): {
            "ticker": "BENH",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US09068S1087",
        },
        ("RZZN", "OTC"): {
            "ticker": "RZZN",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US47737A2078",
        },
        ("UVCL", "OTC"): {
            "ticker": "UVCL",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "US9133561012",
        },
        ("HLOI", "OTC"): {
            "ticker": "HLOI",
            "exchange": "OTC",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "",
        },
        ("DDNFD", "OTC"): {
            "ticker": "DDNFD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "DDNFD",
            "stock_sector": "",
            "isin": "",
        },
        ("PLYFD", "OTC"): {
            "ticker": "PLYFD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "PLYFD",
            "stock_sector": "",
            "isin": "",
        },
        ("DOWAD", "OTC"): {
            "ticker": "DOWAD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "DOWAD",
            "stock_sector": "",
            "isin": "",
        },
        ("SDPMP", "OTC"): {
            "ticker": "SDPMP",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "SDPMP",
            "stock_sector": "",
            "isin": "",
        },
        ("SDPMV", "OTC"): {
            "ticker": "SDPMV",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "SDPMV",
            "stock_sector": "",
            "isin": "",
        },
        ("MCSAV", "OTC"): {
            "ticker": "MCSAV",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "MCSAV",
            "stock_sector": "",
            "isin": "",
        },
        ("MCSYV", "OTC"): {
            "ticker": "MCSYV",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "MCSYV",
            "stock_sector": "",
            "isin": "",
        },
        ("TDSPU", "OTC"): {
            "ticker": "TDSPU",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "TDSPU",
            "stock_sector": "",
            "isin": "",
        },
        ("AMNGV", "OTC"): {
            "ticker": "AMNGV",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "AMNGV",
            "stock_sector": "",
            "isin": "",
        },
        ("HZOZD", "OTC"): {
            "ticker": "HZOZD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "HZOZD",
            "stock_sector": "",
            "etf_category": "",
            "isin": "",
        },
        ("PTRVD", "OTC"): {
            "ticker": "PTRVD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "PTRVD",
            "stock_sector": "",
            "isin": "",
        },
        ("PMSXF", "OTC"): {
            "ticker": "PMSXF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "PMSXF",
            "stock_sector": "",
            "isin": "",
        },
        ("FNMCD", "OTC"): {
            "ticker": "FNMCD",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "First Nordic Metals Corp.",
            "stock_sector": "",
            "isin": "",
        },
        ("VGRDF", "OTC"): {
            "ticker": "VGRDF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "VANGUARD FUNDS PLC FTSE DEV EUR",
            "stock_sector": "",
            "etf_category": "",
            "isin": "",
        },
        ("IRS2W", "BCBA"): {
            "ticker": "IRS2W",
            "exchange": "BCBA",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("PNIZF", "BCBA"): {
            "ticker": "PNIZF",
            "exchange": "BCBA",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("EWQQF", "OTC"): {
            "ticker": "EWQQF",
            "exchange": "OTC",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "",
        },
        ("VNCUF", "OTC"): {
            "ticker": "VNCUF",
            "exchange": "OTC",
            "asset_type": "ETF",
            "etf_category": "",
            "isin": "",
        },
        ("MFMS", "QSE"): {
            "ticker": "MFMS",
            "exchange": "QSE",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("FORE", "IDX"): {
            "ticker": "FORE",
            "exchange": "IDX",
            "asset_type": "Stock",
            "stock_sector": "Consumer Discretionary",
            "isin": "",
        },
        ("IRSX", "IDX"): {
            "ticker": "IRSX",
            "exchange": "IDX",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "isin": "",
        },
        ("ETFPESOV", "BVL"): {
            "ticker": "ETFPESOV",
            "exchange": "BVL",
            "asset_type": "ETF",
            "etf_category": "Fixed Income",
            "isin": "",
        },
        ("JTINA", "PSE_CZ"): {
            "ticker": "JTINA",
            "exchange": "PSE_CZ",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("786", "PSX"): {
            "ticker": "786",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("ACIETF", "PSX"): {
            "ticker": "ACIETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("FCEPL", "PSX"): {
            "ticker": "FCEPL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Consumer Staples",
            "isin": "",
        },
        ("FDPL", "PSX"): {
            "ticker": "FDPL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("HRPL", "PSX"): {
            "ticker": "HRPL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Consumer Staples",
            "isin": "",
        },
        ("HBLTETF", "PSX"): {
            "ticker": "HBLTETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Fixed Income",
            "isin": "",
        },
        ("IGIHL", "PSX"): {
            "ticker": "IGIHL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("IML", "PSX"): {
            "ticker": "IML",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("IPAK", "PSX"): {
            "ticker": "IPAK",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "",
        },
        ("JSGBETF", "PSX"): {
            "ticker": "JSGBETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("JSMFETF", "PSX"): {
            "ticker": "JSMFETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("LCI", "PSX"): {
            "ticker": "LCI",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "",
        },
        ("LSEFSL", "PSX"): {
            "ticker": "LSEFSL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("MCBIM", "PSX"): {
            "ticker": "MCBIM",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("MIIETF", "PSX"): {
            "ticker": "MIIETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("MZNPETF", "PSX"): {
            "ticker": "MZNPETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("NBPGETF", "PSX"): {
            "ticker": "NBPGETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("NITGETF", "PSX"): {
            "ticker": "NITGETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("PIAHCLA", "PSX"): {
            "ticker": "PIAHCLA",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("PIAHCLB", "PSX"): {
            "ticker": "PIAHCLB",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("STYLERS", "PSX"): {
            "ticker": "STYLERS",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Consumer Discretionary",
            "isin": "",
        },
        ("UBLPETF", "PSX"): {
            "ticker": "UBLPETF",
            "exchange": "PSX",
            "asset_type": "ETF",
            "name": "UBLPakistanETF XD",
            "etf_category": "Equity",
            "isin": "",
        },
        ("WAFI", "PSX"): {
            "ticker": "WAFI",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Energy",
            "isin": "",
        },
        ("WASL", "PSX"): {
            "ticker": "WASL",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("WAVESAPP", "PSX"): {
            "ticker": "WAVESAPP",
            "exchange": "PSX",
            "asset_type": "Stock",
            "stock_sector": "Consumer Discretionary",
            "isin": "",
        },
        ("WABS", "BSE_HU"): {
            "ticker": "WABS",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("ETFCETOPOT", "BSE_HU"): {
            "ticker": "ETFCETOPOT",
            "exchange": "BSE_HU",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("FORRB", "BSE_HU"): {
            "ticker": "FORRB",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("FUTUR", "BSE_HU"): {
            "ticker": "FUTUR",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "stock_sector": "Consumer Staples",
            "isin": "",
        },
        ("MBHJ", "BSE_HU"): {
            "ticker": "MBHJ",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("TROLLEY", "BK"): {
            "ticker": "TROLLEY",
            "exchange": "BK",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "KW0EQ0610414",
        },
        ("GENIUS21", "BMV"): {
            "ticker": "GENIUS21",
            "exchange": "BMV",
            "asset_type": "Stock",
            "stock_sector": "",
            "etf_category": "",
            "isin": "",
        },
        ("DX2J", "XETRA"): {
            "ticker": "DX2J",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("DXS7", "XETRA"): {
            "ticker": "DXS7",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("XFRD", "XETRA"): {
            "ticker": "XFRD",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "etf_category": "Alternative",
            "isin": "",
        },
        ("CPIP", "JSE"): {
            "ticker": "CPIP",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("DSBP", "JSE"): {
            "ticker": "DSBP",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("GNDP", "JSE"): {
            "ticker": "GNDP",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("IOC", "JSE"): {
            "ticker": "IOC",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "isin": "",
        },
        ("NTCP", "JSE"): {
            "ticker": "NTCP",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Health Care",
            "isin": "",
        },
        ("WTOP20", "JSE"): {
            "ticker": "WTOP20",
            "exchange": "JSE",
            "asset_type": "ETF",
            "etf_category": "Equity",
            "isin": "",
        },
        ("ZZD", "JSE"): {
            "ticker": "ZZD",
            "exchange": "JSE",
            "asset_type": "Stock",
            "stock_sector": "Consumer Discretionary",
            "isin": "",
        },
        ("AKTR", "ATHEX"): {
            "ticker": "AKTR",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "stock_sector": "Industrials",
            "isin": "",
        },
        ("BOCHGR", "ATHEX"): {
            "ticker": "BOCHGR",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "stock_sector": "Financials",
            "isin": "",
        },
        ("ORILINA", "ATHEX"): {
            "ticker": "ORILINA",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("SOFTWEB", "ATHEX"): {
            "ticker": "SOFTWEB",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "stock_sector": "Information Technology",
            "isin": "",
        },
        ("TRESTATES", "ATHEX"): {
            "ticker": "TRESTATES",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("EISP", "OSL"): {
            "ticker": "EISP",
            "exchange": "OSL",
            "asset_type": "Stock",
            "stock_sector": "Real Estate",
            "isin": "",
        },
        ("ELOO", "OSL"): {
            "ticker": "ELOO",
            "exchange": "OSL",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "",
        },
        ("BE09442646", "Euronext"): {
            "ticker": "BE09442646",
            "exchange": "Euronext",
            "asset_type": "Stock",
            "stock_sector": "Materials",
            "isin": "",
        },
        ("GEC", "TSX"): {
            "ticker": "GEC",
            "exchange": "TSX",
            "asset_type": "Stock",
            "stock_sector": "",
            "isin": "CA37961F1053",
        },
    }

    results = evaluate_updates(
        REVIEWED_SOURCE_UPDATES,
        listing_rows,
        issue_keys={("XACTC25", "CPH"), ("WT", "NYSE"), ("RCR.P", "TSXV")},
        exchanges={
                "AMS",
                "ASX",
                "ATHEX",
            "BCBA",
            "BSE_HU",
            "BK",
            "BMV",
            "BVL",
            "CPH",
            "Euronext",
            "IDX",
            "JSE",
            "LSE",
            "NYSE",
            "OSL",
            "OTC",
            "PSE_CZ",
            "PSX",
            "QSE",
            "SET",
            "TSX",
            "TSXV",
            "XETRA",
        },
    )

    accepted = {(row["ticker"], row["exchange"], row["field"], row["proposed_value"]) for row in results if row["decision"] == "accept"}
    assert ("XACTC25", "CPH", "etf_category", "Equity") in accepted
    assert ("WT", "NYSE", "asset_type", "Stock") in accepted
    assert ("WT", "NYSE", "stock_sector", "Financials") in accepted
    assert ("TTE", "NYSE", "isin", "FR0000120271") in accepted
    assert ("RCR.P", "TSXV", "asset_type", "Stock") in accepted
    assert ("RCR.P", "TSXV", "stock_sector", "Financials") in accepted
    assert ("ALRIS", "Euronext", "stock_sector", "Real Estate") in accepted
    assert ("CVBP", "LSE", "isin", "GB0002290764") in accepted
    assert ("AXTRART", "SET", "isin", "THC611010008") in accepted
    assert ("AGAC", "AMS", "isin", "IE000FHBZDZ8") in accepted
    assert ("AGAC", "AMS", "etf_category", "Fixed Income") in accepted
    assert ("BLTN", "AMS", "isin", "DE000A2QP4D2") in accepted
    assert ("HYCB", "AMS", "isin", "IE00098G6RH2") in accepted
    assert ("BFRE", "OTC", "etf_category", "Equity") in accepted
    assert ("BNOBF", "OTC", "etf_category", "Equity") in accepted
    assert ("BREJY", "OTC", "stock_sector", "Financials") in accepted
    assert ("CRTHF", "OTC", "stock_sector", "Health Care") in accepted
    assert ("NPPSF", "OTC", "stock_sector", "Industrials") in accepted
    assert ("XNJJY", "OTC", "stock_sector", "Industrials") in accepted
    assert ("OPMNF", "OTC", "stock_sector", "Energy") in accepted
    assert ("FOACW", "OTC", "stock_sector", "Financials") in accepted
    assert ("RPTCV", "OTC", "stock_sector", "Real Estate") in accepted
    assert ("SPMCP", "OTC", "stock_sector", "Financials") in accepted
    assert ("SHCC", "OTC", "stock_sector", "Industrials") in accepted
    assert ("DTZ", "ASX", "stock_sector", "Information Technology") in accepted
    assert ("DTZNY", "OTC", "stock_sector", "Information Technology") in accepted
    assert ("DTZZF", "OTC", "stock_sector", "Information Technology") in accepted
    assert ("CVW", "TSXV", "stock_sector", "Materials") in accepted
    assert ("CVWFF", "OTC", "stock_sector", "Materials") in accepted
    assert ("BENH", "OTC", "stock_sector", "Consumer Staples") in accepted
    assert ("RZZN", "OTC", "stock_sector", "Industrials") in accepted
    assert ("UVCL", "OTC", "stock_sector", "Communication Services") in accepted
    assert ("HLOI", "OTC", "stock_sector", "Consumer Staples") in accepted
    assert ("DDNFD", "OTC", "name", "Adamera Minerals Corp") in accepted
    assert ("DDNFD", "OTC", "stock_sector", "Materials") in accepted
    assert ("PLYFD", "OTC", "name", "Playfair Mining Ltd") in accepted
    assert ("PLYFD", "OTC", "stock_sector", "Materials") in accepted
    assert ("DOWAD", "OTC", "name", "Defeng Solife Holdings Ltd ADR") in accepted
    assert ("DOWAD", "OTC", "stock_sector", "Communication Services") in accepted
    assert ("SDPMP", "OTC", "name", "Sound Point Meridian Capital 8.5% Preferred Shares") in accepted
    assert ("SDPMV", "OTC", "name", "Sound Point Meridian Capital Inc Preferred Shares 8.5%") in accepted
    assert ("MCSAV", "OTC", "name", "MicroStrategy Inc 10% Perpetual Strife Preferred Series A") in accepted
    assert ("MCSYV", "OTC", "name", "MicroStrategy Inc 10% Perpetual Stride Preferred Series A") in accepted
    assert (
        "TDSPU",
        "OTC",
        "name",
        "Telephone and Data Systems Inc Depositary Shares 6.625% Series UU Preferred Stock",
    ) in accepted
    assert ("AMNGV", "OTC", "name", "American National Group Inc Preferred Series D") in accepted
    assert ("AMNGV", "OTC", "stock_sector", "Financials") in accepted
    assert ("HZOZD", "OTC", "name", "BetaPro Crude Oil Daily Bull ETF") in accepted
    assert ("HZOZD", "OTC", "asset_type", "ETF") in accepted
    assert ("HZOZD", "OTC", "etf_category", "Leveraged/Inverse") in accepted
    assert ("PTRVD", "OTC", "name", "Avila Energy Corporation") in accepted
    assert ("PTRVD", "OTC", "stock_sector", "Energy") in accepted
    assert ("PMSXF", "OTC", "name", "Grafton Resources Inc") in accepted
    assert ("PMSXF", "OTC", "isin", "CA38447A1084") in accepted
    assert ("PMSXF", "OTC", "stock_sector", "Materials") in accepted
    assert ("FNMCD", "OTC", "name", "Goldsky Resources") in accepted
    assert ("FNMCD", "OTC", "stock_sector", "Materials") in accepted
    assert ("VGRDF", "OTC", "name", "VANGUARD FNDS PLC EU ETF") in accepted
    assert ("VGRDF", "OTC", "asset_type", "ETF") in accepted
    assert ("VGRDF", "OTC", "etf_category", "Equity") in accepted
    assert ("IRS2W", "BCBA", "isin", "ARIRSA100224") in accepted
    assert ("PNIZF", "BCBA", "isin", "AR0140128668") in accepted
    assert ("EWQQF", "OTC", "etf_category", "Equity") in accepted
    assert ("VNCUF", "OTC", "etf_category", "Equity") in accepted
    assert ("MFMS", "QSE", "isin", "QA000YI47FK6") in accepted
    assert ("FORE", "IDX", "isin", "ID1000210008") in accepted
    assert ("IRSX", "IDX", "isin", "ID1000184401") in accepted
    assert ("ETFPESOV", "BVL", "isin", "PEP790058007") in accepted
    assert ("JTINA", "PSE_CZ", "isin", "CZ0008044856") in accepted
    assert ("786", "PSX", "isin", "PK0061101017") in accepted
    assert ("ACIETF", "PSX", "isin", "PK0088806069") in accepted
    assert ("FCEPL", "PSX", "isin", "PK0096501017") in accepted
    assert ("FDPL", "PSX", "isin", "PK0063401019") in accepted
    assert ("HRPL", "PSX", "isin", "PK0026401015") in accepted
    assert ("HBLTETF", "PSX", "isin", "PK0086206072") in accepted
    assert ("IGIHL", "PSX", "isin", "PK0032601012") in accepted
    assert ("IML", "PSX", "isin", "PK0093001011") in accepted
    assert ("IPAK", "PSX", "isin", "PK0111801012") in accepted
    assert ("JSGBETF", "PSX", "isin", "PK0082806016") in accepted
    assert ("JSMFETF", "PSX", "isin", "PK0086006035") in accepted
    assert ("LCI", "PSX", "isin", "PK0003101018") in accepted
    assert ("LSEFSL", "PSX", "isin", "PK0104901019") in accepted
    assert ("MCBIM", "PSX", "isin", "PK0082101012") in accepted
    assert ("MIIETF", "PSX", "isin", "PK0133706017") in accepted
    assert ("MZNPETF", "PSX", "isin", "PK0087306053") in accepted
    assert ("NBPGETF", "PSX", "isin", "PK0089106048") in accepted
    assert ("NITGETF", "PSX", "isin", "PK0096106015") in accepted
    assert ("PIAHCLA", "PSX", "isin", "PK0146501017") in accepted
    assert ("PIAHCLB", "PSX", "isin", "PK0146501025") in accepted
    assert ("STYLERS", "PSX", "isin", "PK0143701016") in accepted
    assert ("UBLPETF", "PSX", "name", "UBL Pakistan Enterprise ETF") in accepted
    assert ("UBLPETF", "PSX", "isin", "PK0087206139") in accepted
    assert ("WAFI", "PSX", "isin", "PK0016701010") in accepted
    assert ("WASL", "PSX", "isin", "PK0042801016") in accepted
    assert ("WAVESAPP", "PSX", "isin", "PK0068401014") in accepted
    assert ("WABS", "BSE_HU", "isin", "HU0000120720") in accepted
    assert ("ETFCETOPOT", "BSE_HU", "isin", "HU0000734454") in accepted
    assert ("FORRB", "BSE_HU", "isin", "HU0000066394") in accepted
    assert ("FUTUR", "BSE_HU", "isin", "HU0000107362") in accepted
    assert ("MBHJ", "BSE_HU", "isin", "HU0000078175") in accepted
    assert ("TROLLEY", "BK", "stock_sector", "Consumer Discretionary") in accepted
    assert ("GENIUS21", "BMV", "asset_type", "ETF") in accepted
    assert ("GENIUS21", "BMV", "etf_category", "Equity") in accepted
    assert ("DX2J", "XETRA", "isin", "LU0322253906") in accepted
    assert ("DXS7", "XETRA", "isin", "LU0322252924") in accepted
    assert ("XFRD", "XETRA", "isin", "DE000A1KJHG8") in accepted
    assert ("CPIP", "JSE", "isin", "ZAE000083838") in accepted
    assert ("DSBP", "JSE", "isin", "ZAE000158564") in accepted
    assert ("GNDP", "JSE", "isin", "ZAE000071106") in accepted
    assert ("IOC", "JSE", "isin", "ZAE000071072") in accepted
    assert ("NTCP", "JSE", "isin", "ZAE000081121") in accepted
    assert ("WTOP20", "JSE", "isin", "ZAE000320792") in accepted
    assert ("ZZD", "JSE", "isin", "ZAE000315768") in accepted
    assert ("AKTR", "ATHEX", "isin", "GRS432003028") in accepted
    assert ("BOCHGR", "ATHEX", "isin", "IE00BD5B1Y92") in accepted
    assert ("ORILINA", "ATHEX", "isin", "GRS535003008") in accepted
    assert ("SOFTWEB", "ATHEX", "isin", "GRS538003005") in accepted
    assert ("TRESTATES", "ATHEX", "isin", "GRS534003009") in accepted
    assert ("EISP", "OSL", "isin", "NO0003998700") in accepted
    assert ("ELOO", "OSL", "isin", "NO0011002586") in accepted
    assert ("BE09442646", "Euronext", "isin", "BE0944264663") in accepted
    assert ("GEC", "TSX", "stock_sector", "Consumer Staples") in accepted


def test_build_metadata_updates_includes_source_reason():
    updates = build_metadata_updates(
        [
            {
                "ticker": "XACTC25",
                "exchange": "CPH",
                "field": "etf_category",
                "proposed_value": "Equity",
                "confidence": "0.80",
                "source_url": "https://example.test/source",
                "evidence": "Exact listing source.",
                "decision": "accept",
            },
            {
                "ticker": "BAD",
                "exchange": "CPH",
                "field": "etf_category",
                "proposed_value": "Equity",
                "confidence": "0.80",
                "source_url": "https://example.test/source",
                "evidence": "Exact listing source.",
                "decision": "already_has_etf_category",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "XACTC25",
            "exchange": "CPH",
            "field": "etf_category",
            "decision": "update",
            "proposed_value": "Equity",
            "confidence": "0.80",
            "reason": (
                "Reviewed exact-listing source evidence mapped CPH::XACTC25 etf_category to Equity. "
                "Evidence: Exact listing source. Source: https://example.test/source"
            ),
        }
    ]
