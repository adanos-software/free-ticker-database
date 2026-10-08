import io

import pandas as pd

from scripts.backfill_deutsche_boerse_listed_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_deutsche_boerse_sector,
    parse_listed_companies_excel,
)


def target(**overrides):
    values = {
        "ticker": "ADC",
        "exchange": "XETRA",
        "asset_type": "Stock",
        "name": "ADCAPITAL AG",
        "isin": "DE0005214506",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def official(**overrides):
    values = {
        "ticker": "ADC",
        "name": "ADCAPITAL AG",
        "isin": "DE0005214506",
        "sector": "Automobile",
        "subsector": "Auto Parts & Equipment",
        "instrument_exchange": "FRANKFURT",
        "sheet": "Basic Board",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    by_ticker = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append(row)
    return by_ticker


def test_map_deutsche_boerse_sector_maps_unambiguous_buckets_only():
    assert map_deutsche_boerse_sector("Industrial") == "Industrials"
    assert map_deutsche_boerse_sector("Software") == "Information Technology"
    assert map_deutsche_boerse_sector("Pharma & Healthcare") == "Health Care"
    assert map_deutsche_boerse_sector("Automobile") == "Consumer Discretionary"
    assert map_deutsche_boerse_sector("Food & Beverages") == "Consumer Staples"
    assert map_deutsche_boerse_sector("Telecommunication") == "Communication Services"
    assert map_deutsche_boerse_sector("Banks") == "Financials"
    assert map_deutsche_boerse_sector("Basic resources") == "Materials"
    assert map_deutsche_boerse_sector("Consumer") == ""
    assert map_deutsche_boerse_sector("Retail") == ""
    assert map_deutsche_boerse_sector("Financial Services") == ""
    assert map_deutsche_boerse_sector("-") == ""
    assert map_deutsche_boerse_sector("Consumer", "Clothing & Footwear") == "Consumer Discretionary"
    assert map_deutsche_boerse_sector("Financial Services", "Real Estate") == "Real Estate"
    assert map_deutsche_boerse_sector("Financial Services", "Private Equity & Venture Capital") == "Financials"
    assert map_deutsche_boerse_sector("Retail", "Retail, Food & Drug") == "Consumer Staples"


def test_evaluate_row_accepts_exact_ticker_and_isin_on_xetra_and_fsx():
    xetra = evaluate_row(target(), indexed(official()))
    fsx = evaluate_row(target(exchange="FSX", name="AdCapital AG"), indexed(official()))

    assert xetra["decision"] == fsx["decision"] == "accept"
    assert xetra["sector_update"] == fsx["sector_update"] == "Consumer Discretionary"
    assert build_metadata_updates([xetra])[0]["proposed_value"] == "Consumer Discretionary"


def test_evaluate_row_rejects_xstu_and_isin_mismatch():
    xstu = evaluate_row(target(exchange="XSTU"), indexed(official()))
    mismatch = evaluate_row(target(isin="DE0007218901"), indexed(official()))

    assert xstu["decision"] == "not_deutsche_boerse_exchange"
    assert mismatch["decision"] == "isin_mismatch"
    assert xstu["sector_update"] == mismatch["sector_update"] == ""
    assert build_metadata_updates([xstu, mismatch]) == []


def test_evaluate_row_leaves_dash_and_mixed_consumer_unmapped():
    dash = evaluate_row(
        target(ticker="PEH", name="PEH Wertpapier AG", isin="DE0006201403"),
        indexed(official(ticker="PEH", name="PEH Wertpapier AG", isin="DE0006201403", sector="-", subsector="-")),
    )
    mixed = evaluate_row(
        target(ticker="ML2", name="MING LE SPORTS AG", isin="DE000A2LQ728"),
        indexed(
            official(
                ticker="ML2",
                name="MING LE SPORTS AG",
                isin="DE000A2LQ728",
                sector="Consumer",
                subsector="Consumer Goods",
            )
        ),
    )
    clothing = evaluate_row(
        target(ticker="ML2", name="MING LE SPORTS AG", isin="DE000A2LQ728"),
        indexed(
            official(
                ticker="ML2",
                name="MING LE SPORTS AG",
                isin="DE000A2LQ728",
                sector="Consumer",
                subsector="Clothing & Footwear",
            )
        ),
    )

    assert dash["decision"] == "unsupported_sector"
    assert mixed["decision"] == "unsupported_sector"
    assert clothing["decision"] == "accept"
    assert clothing["sector_update"] == "Consumer Discretionary"
    assert dash["sector_update"] == mixed["sector_update"] == ""


def test_parse_listed_companies_excel_keeps_sector_column():
    rows = [[None] * 8 for _ in range(7)]
    rows.append(
        ["ISIN", "Trading Symbol", "Company", "Sector", "Subsector", "Country", "Instrument Exchange", "Index"]
    )
    rows.append(
        [
            "DE0005214506",
            "ADC",
            "ADCAPITAL AG",
            "Automobile",
            "Auto Parts & Equipment",
            "Germany",
            "FRANKFURT",
            "-",
        ]
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Basic Board", header=False, index=False)

    parsed = parse_listed_companies_excel(buffer.getvalue())

    assert parsed == [
        {
            "ticker": "ADC",
            "name": "ADCAPITAL AG",
            "isin": "DE0005214506",
            "sector": "Automobile",
            "subsector": "Auto Parts & Equipment",
            "instrument_exchange": "FRANKFURT",
            "sheet": "Basic Board",
        }
    ]
