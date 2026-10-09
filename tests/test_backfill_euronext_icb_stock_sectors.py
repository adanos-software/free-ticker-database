from scripts.backfill_euronext_icb_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_icb_industry,
    parse_icb_industry,
    parse_product_instrument,
    resolve_hits,
)


def target(**overrides):
    values = {
        "ticker": "EG7",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "FBD Holdings plc",
        "isin": "IE0003290289",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def hit(**overrides):
    values = {
        "isin": "IE0003290289",
        "mic": "XMSM",
        "name": "FBD HOLDINGS PLC",
        "symbol": "EG7",
    }
    values.update(overrides)
    return values


ICB_HTML = """
<td>Industry</td>
<td><strong>30, Financials</strong></td>
"""


def test_map_icb_industry_maps_official_buckets_only():
    assert map_icb_industry("Financials") == "Financials"
    assert map_icb_industry("Consumer Discretionary") == "Consumer Discretionary"
    assert map_icb_industry("Real Estate") == "Real Estate"
    assert map_icb_industry("Basic Materials") == "Materials"
    assert map_icb_industry("Telecommunications") == "Communication Services"
    assert map_icb_industry("Technology") == "Information Technology"
    assert map_icb_industry("Finance") == ""
    assert map_icb_industry("") == ""


def test_parse_icb_industry_reads_official_industry_cell():
    assert parse_icb_industry(ICB_HTML) == "Financials"


def test_evaluate_row_accepts_exact_isin_financials():
    result = evaluate_row(target(), [hit()], ["Financials"])

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"
    assert result["euronext_mic"] == "XMSM"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Financials"


def test_evaluate_row_accepts_ops_retail_official_media_icb():
    result = evaluate_row(
        target(ticker="DMM", name="OPS Retail S.p.A.", isin="IT0005545675"),
        [hit(isin="IT0005545675", mic="MTAA", name="OPS RETAIL", symbol="OPR")],
        ["Consumer Discretionary"],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Discretionary"


def test_evaluate_row_uses_oslo_product_when_search_misses():
    oslo = {"isin": "NO0013178616", "mic": "XOSL", "name": "PUBLIC PROPERTY IN", "symbol": "PUBLI"}
    hits = resolve_hits("NO0013178616", [], oslo)
    result = evaluate_row(
        target(ticker="6SO", name="Public Property Invest ASA", isin="NO0013178616"),
        hits,
        ["Real Estate"],
    )

    assert hits[0]["mic"] == "XOSL"
    assert result["decision"] == "accept"
    assert result["sector_update"] == "Real Estate"


def test_evaluate_row_rejects_missing_search_and_mismatched_oslo_isin():
    hits = resolve_hits("NO0013178616", [], {"isin": "NO0000000000", "mic": "XOSL", "name": "OTHER", "symbol": "X"})
    result = evaluate_row(
        target(ticker="6SO", name="Public Property Invest ASA", isin="NO0013178616"),
        hits,
        [],
    )

    assert hits == []
    assert result["decision"] == "no_euronext_match"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_accepts_leftover_osl_merk_icb():
    leftovers = [
        (
            "ADS",
            "ADS Maritime Holding Plc",
            "CY0108052115",
            "ADS MARITIME HOLD",
            "Industrials",
            "Industrials",
        ),
        (
            "GOLDR",
            "GOLD ROAD INTL",
            "MT0003060101",
            "GOLD ROAD INTL",
            "Basic Materials",
            "Materials",
        ),
        (
            "PLCAN",
            "PELICAN AQUA HLDNG",
            "CY0201461213",
            "PELICAN AQUA HLDNG",
            "Industrials",
            "Industrials",
        ),
        (
            "VLCC",
            "VOLARE SHIPPING",
            "SGXZ33616145",
            "VOLARE SHIPPING",
            "Industrials",
            "Industrials",
        ),
    ]
    for ticker, name, isin, euronext_name, icb_industry, expected in leftovers:
        result = evaluate_row(
            target(ticker=ticker, exchange="OSL", name=name, isin=isin),
            [hit(isin=isin, mic="MERK", name=euronext_name, symbol="")],
            [icb_industry],
        )
        assert result["decision"] == "accept", ticker
        assert result["sector_update"] == expected, ticker
        assert result["euronext_mic"] == "MERK", ticker
        assert build_metadata_updates([result])[0]["proposed_value"] == expected, ticker


def test_parse_product_instrument_requires_isin_and_mic():
    html = (
        '<script type="application/json" data-drupal-selector="drupal-settings-json">'
        '{"custom":{"instrument":{"type":"STOCK","isin":"NO0013178616","mic":"XOSL",'
        '"name":"PUBLIC PROPERTY IN","symbol":"PUBLI"}}}</script>'
    )
    parsed = parse_product_instrument(html)
    assert parsed["isin"] == "NO0013178616"
    assert parsed["mic"] == "XOSL"
    assert parsed["symbol"] == "PUBLI"
