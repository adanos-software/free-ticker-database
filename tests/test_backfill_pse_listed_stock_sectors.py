from scripts.backfill_pse_listed_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_pse_subsector,
    parse_directory_html,
    parse_directory_items,
)


def target(**overrides):
    values = {
        "ticker": "ALI",
        "exchange": "PSE",
        "asset_type": "Stock",
        "name": "Ayala Land Inc",
        "isin": "PHY0488F1004",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def official(**overrides):
    values = {
        "SecuritySymbol": "ALI",
        "SecurityName": "Ayala Land, Inc.",
        "SecurityISIN": "PHY0488F1004",
        "SecurityType": "C",
        "SecurityStatus": "T",
        "SubsectorName": "PROPERTY",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    parsed = parse_directory_items(list(rows))
    return {row["ticker"]: row for row in parsed}


def test_map_pse_subsector_maps_unambiguous_buckets_only():
    assert map_pse_subsector("PROPERTY") == "Real Estate"
    assert map_pse_subsector("BANKS") == "Financials"
    assert map_pse_subsector("MINING") == "Materials"
    assert map_pse_subsector("OIL") == "Energy"
    assert map_pse_subsector("FOOD, BEVERAGE & TOBACCO") == "Consumer Staples"
    assert map_pse_subsector("TELECOMMUNICATIONS") == "Communication Services"
    assert map_pse_subsector("HOLDING FIRMS") == ""
    assert map_pse_subsector("SME") == ""
    assert map_pse_subsector("OTHER SERVICES") == ""
    assert map_pse_subsector("ELEC., ENERGY, POWER & WATER") == ""
    assert map_pse_subsector("RETAIL") == ""
    assert map_pse_subsector("") == ""


def test_evaluate_row_accepts_exact_ticker_and_isin():
    result = evaluate_row(target(), indexed(official()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Real Estate"
    assert result["pse_ticker"] == "ALI"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Real Estate"


def test_evaluate_row_rejects_isin_mismatch():
    result = evaluate_row(target(isin="PHY0001Z1040"), indexed(official()))

    assert result["decision"] == "isin_mismatch"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_holding_firms_and_mixed_energy_water():
    holding = evaluate_row(
        target(ticker="AC", name="Ayala Corp", isin="PHY0486V1154"),
        indexed(official(SecuritySymbol="AC", SecurityISIN="PHY0486V1154", SubsectorName="HOLDING FIRMS")),
    )
    energy = evaluate_row(
        target(ticker="AP", name="Aboitiz Power Corp", isin="PHY0005M1090"),
        indexed(
            official(
                SecuritySymbol="AP",
                SecurityISIN="PHY0005M1090",
                SubsectorName="ELEC., ENERGY, POWER & WATER",
            )
        ),
    )
    retail = evaluate_row(
        target(ticker="SEVN", name="Philippine Seven Corp", isin="PHY6955M1063"),
        indexed(
            official(
                SecuritySymbol="SEVN",
                SecurityISIN="PHY6955M1063",
                SubsectorName="RETAIL",
            )
        ),
    )

    assert holding["decision"] == "unsupported_pse_subsector"
    assert energy["decision"] == "unsupported_pse_subsector"
    assert retail["decision"] == "unsupported_pse_subsector"
    assert holding["sector_update"] == energy["sector_update"] == retail["sector_update"] == ""


def test_evaluate_row_rejects_filled_row_and_inactive_official():
    filled = evaluate_row(target(stock_sector="Real Estate"), indexed(official()))
    inactive = evaluate_row(
        target(),
        indexed(official(SecurityStatus="S")),
    )

    assert filled["decision"] == "already_has_sector"
    assert inactive["decision"] == "no_pse_match"
    assert filled["sector_update"] == inactive["sector_update"] == ""


def test_parse_directory_html_keeps_active_common_share_subsector():
    html = (
        '<input id="store-json" value="[{&quot;SecuritySymbol&quot;:&quot;ALI&quot;,'
        '&quot;SecurityName&quot;:&quot;Ayala Land, Inc.&quot;,'
        '&quot;SecurityISIN&quot;:&quot;PHY0488F1004&quot;,'
        '&quot;SecurityType&quot;:&quot;C&quot;,'
        '&quot;SecurityStatus&quot;:&quot;T&quot;,'
        '&quot;SubsectorName&quot;:&quot;PROPERTY&quot;}]">'
    )

    rows = parse_directory_html(html)

    assert rows == [
        {
            "ticker": "ALI",
            "name": "Ayala Land, Inc.",
            "isin": "PHY0488F1004",
            "asset_type": "Stock",
            "status": "T",
            "subsector": "PROPERTY",
        }
    ]



