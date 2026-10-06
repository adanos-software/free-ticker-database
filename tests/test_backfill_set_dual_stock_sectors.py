from scripts.backfill_set_dual_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_set_listings_by_nsin_prefix,
    thai_issuer_key,
)


def target(**overrides):
    values = {
        "ticker": "47T",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "TISCO Financial Group PCL",
        "isin": "TH0999010Z11",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def set_listing(**overrides):
    values = {
        "ticker": "TISCO",
        "exchange": "SET",
        "asset_type": "Stock",
        "name": "TISCO Financial Group Public Company Limited",
        "isin": "TH0999010Z03",
        "stock_sector": "Financials",
    }
    values.update(overrides)
    return values


def search_row(symbol: str, name_en: str, sector: str, industry: str = "", **overrides):
    values = {
        "symbol": symbol,
        "nameEN": name_en,
        "market": "SET",
        "securityType": "S",
        "sector": sector,
        "industry": industry,
    }
    values.update(overrides)
    return values


def test_thai_issuer_key_strips_legal_form_and_keeps_issuer_tokens():
    assert thai_issuer_key("TISCO Financial Group PCL") == "tisco financial group"
    assert thai_issuer_key("SCB X Public Company Limited(Alien Mkt)") == "scb x"
    assert thai_issuer_key("Bangkok Expressway and Metro PCL F") == "bangkok expressway and metro"
    assert thai_issuer_key("TPI Polene Public Company Limited") != thai_issuer_key(
        "TPI Polene Power Public Company Limited"
    )
    assert thai_issuer_key("PTT Exploration & Production PCL") == thai_issuer_key(
        "PTT Exploration and Production Public Company Limited"
    )


def test_evaluate_row_accepts_unique_nsin_stem_with_matching_issuer_key():
    indexed = index_set_listings_by_nsin_prefix([set_listing()])
    result = evaluate_row(target(), indexed, [])

    assert result["decision"] == "accept"
    assert result["match_path"] == "nsin_stem"
    assert result["sector_update"] == "Financials"
    assert result["set_ticker"] == "TISCO"


def test_evaluate_row_rejects_nsin_stem_when_issuer_key_does_not_match():
    indexed = index_set_listings_by_nsin_prefix([set_listing()])
    result = evaluate_row(target(name="Unrelated Mining PCL"), indexed, [])

    assert result["decision"] == "no_set_match"
    assert result["sector_update"] == ""


def test_evaluate_row_does_not_use_jaccard_name_match_for_bangkok_bank():
    indexed = index_set_listings_by_nsin_prefix([])
    result = evaluate_row(
        target(
            ticker="BKKF",
            name="Bangkok Bank Public Company Limited",
            isin="TH0001010014",
        ),
        indexed,
        [
            search_row("BA", "BANGKOK AIRWAYS PUBLIC COMPANY LIMITED", "TRANS", "SERVICE"),
            search_row("BBL", "BANGKOK BANK PUBLIC COMPANY LIMITED", "BANK", "FINCIAL"),
            search_row("BCH", "BANGKOK CHAIN HOSPITAL PUBLIC COMPANY LIMITED", "HELTH", "SERVICE"),
        ],
    )

    assert result["decision"] == "accept"
    assert result["set_ticker"] == "BBL"
    assert result["sector_update"] == "Financials"


def test_evaluate_row_distinguishes_tpi_polene_from_tpi_polene_power():
    indexed = index_set_listings_by_nsin_prefix([])
    result = evaluate_row(
        target(
            ticker="NVP6",
            name="TPI Polene Public Company Limited",
            isin="TH0212010R19",
        ),
        indexed,
        [
            search_row("TPIPL", "TPI POLENE PUBLIC COMPANY LIMITED", "CONMAT", "PROPCON"),
            search_row("TPIPP", "TPI POLENE POWER PUBLIC COMPANY LIMITED", "ENERG", "RESOURC"),
        ],
    )

    assert result["decision"] == "accept"
    assert result["set_ticker"] == "TPIPL"
    assert result["sector_update"] == "Materials"


def test_evaluate_row_collapses_foreign_board_share_when_gics_is_unique():
    indexed = index_set_listings_by_nsin_prefix([])
    result = evaluate_row(
        target(
            ticker="OU8",
            name="SCB X Public Company Limited(Alien Mkt)",
            isin="THA790010013",
        ),
        indexed,
        [
            search_row("SCB", "SCB X PUBLIC COMPANY LIMITED", "BANK", "FINCIAL"),
            search_row("SCB-F", "SCB X PUBLIC COMPANY LIMITED", "BANK", "FINCIAL"),
            search_row("SCB-P", "Preferred Shares of SCB X PUBLIC COMPANY LIMITED", "", ""),
        ],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Financials"


def test_evaluate_row_rejects_group_false_positives_that_names_match_would_accept():
    indexed = index_set_listings_by_nsin_prefix([])
    result = evaluate_row(
        target(
            ticker="J74",
            name="Betagro Public Company Limited",
            isin="TH0612010R10",
        ),
        indexed,
        [
            search_row("CIG", "C.I.GROUP PUBLIC COMPANY LIMITED", "INDUS", "INDUS"),
            search_row("IIG", "I&I GROUP PUBLIC COMPANY LIMITED", "TECH", "TECH"),
        ],
    )

    assert result["decision"] == "no_set_match"
    assert result["sector_update"] == ""
