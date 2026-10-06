from scripts.backfill_cse_listed_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_by_issuer_key,
    map_cse_sector,
    parse_listed_records,
)


def target(**overrides):
    values = {
        "ticker": "0IU",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "US Critical Metals Corp.",
        "isin": "CA90366H4089",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def cse(**overrides):
    values = {
        "listing_market": "CSE",
        "symbol": "USCM",
        "status": "Active",
        "security_name": "US Critical Metals Corp.",
        "security_type": "Equity",
        "sector": "Mining",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_issuer_key(parse_listed_records(list(rows)))


def test_map_cse_sector_maps_unambiguous_buckets_only():
    assert map_cse_sector("Mining") == "Materials"
    assert map_cse_sector("Oil and Gas") == "Energy"
    assert map_cse_sector("Technology") == "Information Technology"
    assert map_cse_sector("Diversified Industries") == ""
    assert map_cse_sector("Life Sciences") == ""
    assert map_cse_sector("CleanTech") == ""
    assert map_cse_sector("") == ""


def test_evaluate_row_accepts_unique_mining_issuer_key():
    result = evaluate_row(target(), indexed(cse()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"
    assert result["cse_symbol"] == "USCM"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Materials"


def test_evaluate_row_accepts_unique_technology_issuer_key():
    result = evaluate_row(
        target(ticker="0XM1", name="Humanoid Global Holdings Corp.", isin="CA44486R1010"),
        indexed(cse(symbol="ROBO", security_name="Humanoid Global Holdings Corp.", sector="Technology")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Information Technology"


def test_evaluate_row_rejects_diversified_industries():
    result = evaluate_row(
        target(ticker="4W70", name="SBD Capital Corp.", isin="CA78412Y3014"),
        indexed(cse(symbol="SBD", security_name="SBD Capital Corp.", sector="Diversified Industries")),
    )

    assert result["decision"] == "unsupported_cse_sector"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_missing_cse_name():
    result = evaluate_row(
        target(ticker="0PL", name="Patriot One Technologies Inc", isin="CA70339L1085"),
        indexed(cse()),
    )

    assert result["decision"] == "no_cse_match"
    assert result["sector_update"] == ""


def test_evaluate_row_accepts_suspended_mining_and_technology():
    mining = evaluate_row(
        target(ticker="A0H", name="AUXICO RESOURCES CANADA", isin="CA05334L1094"),
        indexed(cse(symbol="AUAG", security_name="Auxico Resources Canada Inc.", status="Suspended", sector="Mining")),
    )
    tech = evaluate_row(
        target(ticker="5AU", name="SPROUT AI INC.", isin="CA85209X1078"),
        indexed(cse(symbol="BYFM", security_name="Sprout AI Inc.", status="Suspended", sector="Technology")),
    )

    assert mining["decision"] == "accept"
    assert mining["sector_update"] == "Materials"
    assert tech["decision"] == "accept"
    assert tech["sector_update"] == "Information Technology"


def test_evaluate_row_rejects_delisted_and_halted():
    delisted = evaluate_row(
        target(ticker="A0H", name="AUXICO RESOURCES CANADA", isin="CA05334L1094"),
        indexed(cse(symbol="AUAG", security_name="Auxico Resources Canada Inc.", status="Delisted", sector="Mining")),
    )
    halted = evaluate_row(
        target(ticker="1WH", name="BIOCURE TECHNOLOGY INC.", isin="CA09075T1075"),
        indexed(
            cse(
                symbol="CURE.X",
                security_name="Biocure Technology Inc.",
                status="Halted Pending Fundamental Change",
                sector="Life Sciences",
            )
        ),
    )

    assert delisted["decision"] == "no_cse_match"
    assert halted["decision"] == "no_cse_match"
    assert delisted["sector_update"] == halted["sector_update"] == ""


def test_evaluate_row_rejects_one_token_key_and_non_ca_isin():
    one_token = evaluate_row(
        target(ticker="IQ70", name="1CM INC.", isin="CA68237A1093"),
        indexed(cse(symbol="EPIC", security_name="1CM Inc.", sector="Life Sciences")),
    )
    foreign = evaluate_row(
        target(ticker="LOC", name="Lion Corporation", isin="JP3965400009"),
        indexed(cse(symbol="LION", security_name="Lion Corporation", sector="Mining")),
    )

    assert one_token["decision"] == "short_issuer_key"
    assert foreign["decision"] == "not_ca_isin"
    assert one_token["sector_update"] == ""
    assert foreign["sector_update"] == ""
