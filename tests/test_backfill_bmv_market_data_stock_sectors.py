from scripts.backfill_bmv_market_data_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_bmv_sector,
)


def target(**overrides):
    values = {
        "ticker": "AC",
        "exchange": "BMV",
        "asset_type": "Stock",
        "name": "Arca Continental S.A.B. de C.V",
        "isin": "MX01AC100006",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def official(**overrides):
    values = {
        "ticker": "AC",
        "name": "ARCA CONTINENTAL, S.A.B. DE C.V.",
        "isin": "MX01AC100006",
        "sector": "PRODUCTOS DE CONSUMO FRECUENTE",
        "issuer": "AC",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    by_ticker = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append(row)
    return by_ticker


def test_map_bmv_sector_maps_unambiguous_buckets_only():
    assert map_bmv_sector("PRODUCTOS DE CONSUMO FRECUENTE") == "Consumer Staples"
    assert map_bmv_sector("SERVICIOS Y BIENES DE CONSUMO NO BASICO") == "Consumer Discretionary"
    assert map_bmv_sector("INDUSTRIAL") == "Industrials"
    assert map_bmv_sector("SERVICIOS FINANCIEROS") == "Financials"
    assert map_bmv_sector("ENERGIA") == "Energy"
    assert map_bmv_sector("MATERIALES") == "Materials"
    assert map_bmv_sector("BIENES RAICES") == "Real Estate"
    assert map_bmv_sector("SALUD") == "Health Care"
    assert map_bmv_sector("TECNOLOGIA DE LA INFORMACION") == "Information Technology"
    assert map_bmv_sector("SERVICIOS PUBLICOS") == "Utilities"
    assert map_bmv_sector("SERVICIOS DE TELECOMUNICACIONES") == "Communication Services"
    assert map_bmv_sector("HOLDING") == ""
    assert map_bmv_sector("") == ""
    assert map_bmv_sector("-") == ""


def test_evaluate_row_accepts_exact_ticker_and_isin():
    with_isin = evaluate_row(target(), indexed(official()))
    no_official_isin = evaluate_row(target(), indexed(official(isin="")))

    assert with_isin["decision"] == no_official_isin["decision"] == "accept"
    assert with_isin["sector_update"] == no_official_isin["sector_update"] == "Consumer Staples"
    assert build_metadata_updates([with_isin])[0]["proposed_value"] == "Consumer Staples"


def test_evaluate_row_rejects_isin_mismatch_and_non_bmv():
    mismatch = evaluate_row(target(isin="MXP4960P1070"), indexed(official()))
    nyse = evaluate_row(target(exchange="NYSE"), indexed(official()))

    assert mismatch["decision"] == "isin_mismatch"
    assert nyse["decision"] == "not_bmv_exchange"
    assert mismatch["sector_update"] == nyse["sector_update"] == ""
    assert build_metadata_updates([mismatch, nyse]) == []


def test_evaluate_row_leaves_unmapped_and_empty_sectors():
    holding = evaluate_row(target(), indexed(official(sector="HOLDING")))
    empty = evaluate_row(target(), indexed(official(sector="")))
    missing = evaluate_row(target(ticker="MRK", isin="US58933Y1055"), indexed())

    assert holding["decision"] == "unsupported_sector"
    assert empty["decision"] == "empty_sector"
    assert missing["decision"] == "no_official_match"
    assert holding["sector_update"] == empty["sector_update"] == missing["sector_update"] == ""
