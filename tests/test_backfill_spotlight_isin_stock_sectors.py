from scripts.backfill_spotlight_isin_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_by_isin,
    map_spotlight_industry,
    parse_company_records,
    parse_trade_isin,
)


def target(**overrides):
    values = {
        "ticker": "7M8",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "ECORUB AB B",
        "isin": "SE0003273531",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def capture(**overrides):
    values = {
        "name": "EcoRub AB",
        "industry": "Basic Materials",
        "instrument_id": "XSAT01000824",
        "isin": "SE0003273531",
        "detail_url": "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01000824",
        "trade_url": "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT01000824",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_isin(list(rows))


RESP_HTML = """
<span class="sl-ii__label">ISIN-Kod</span>
<span class="sl-ii__value">SE0023314315</span>
<span>XSAT01001050</span>
"""


def test_map_spotlight_industry_maps_unambiguous_labels_only():
    assert map_spotlight_industry("Basic Materials") == "Materials"
    assert map_spotlight_industry("Technology") == "Information Technology"
    assert map_spotlight_industry("Health Care") == "Health Care"
    assert map_spotlight_industry("Real Estate") == "Real Estate"
    assert map_spotlight_industry("Consumer Goods & Services") == ""
    assert map_spotlight_industry("Finance") == ""
    assert map_spotlight_industry("Financial Conglomerates") == ""
    assert map_spotlight_industry("") == ""


def test_parse_trade_isin_uses_labeled_value_not_instrument_id():
    assert parse_trade_isin(RESP_HTML) == "SE0023314315"
    assert parse_trade_isin("<span>XSAT01001050</span>") == ""


def test_parse_company_records_keeps_instrument_id():
    parsed = parse_company_records(
        [{"heading": "EcoRub AB", "industry": "Basic Materials", "url": "/sv/bolag/irabout?InstrumentId=XSAT01000824"}]
    )
    assert parsed[0]["instrument_id"] == "XSAT01000824"
    assert parsed[0]["trade_url"].endswith("irtrade?InstrumentId=XSAT01000824")


def test_evaluate_row_accepts_exact_isin_basic_materials():
    result = evaluate_row(target(), indexed(capture()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Materials"


def test_evaluate_row_accepts_labeled_respiratorius_isin():
    result = evaluate_row(
        target(ticker="HF00", name="Respiratorius AB (publ)", isin="SE0023314315"),
        indexed(capture(name="Respiratorius AB", industry="Health Care", isin="SE0023314315", instrument_id="XSAT01001050")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Health Care"


def test_evaluate_row_rejects_isin_change_and_consumer_goods_services():
    mismatch = evaluate_row(
        target(ticker="6ZR", name="MOVEBYBIKE EUROPE AB", isin="SE0015988100"),
        indexed(capture(name="MoveByBike Europe AB", industry="Industrials", isin="SE0029530138")),
    )
    unsupported = evaluate_row(target(), indexed(capture(industry="Consumer Goods & Services")))

    assert mismatch["decision"] == "no_spotlight_match"
    assert unsupported["decision"] == "unsupported_industry"
    assert mismatch["sector_update"] == unsupported["sector_update"] == ""
