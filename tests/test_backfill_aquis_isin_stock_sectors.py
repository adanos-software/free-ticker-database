from scripts.backfill_aquis_isin_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_by_isin,
    map_aquis_sector,
    parse_companies_page,
)


def target(**overrides):
    values = {
        "ticker": "48U0",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Marula Mining PLC",
        "isin": "GB00BNBS4S95",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def company(**overrides):
    values = {
        "ticker": "MARU",
        "name": "Marula Mining PLC",
        "isin": "GB00BNBS4S95",
        "sector": "Materials",
        "instrument_type": "Ordinary Shares",
        "status": "SUSPENDED",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_isin(list(rows))


MARU_HTML = """
<script id="__NEXT_DATA__" type="application/json">{"props":{"pageProps":{"companies":[{"name":"Marula Mining PLC","isin":"GB00BNBS4S95","symbol":"MARU","instrument_type":"Ordinary Shares","sector":"Materials","status":"SUSPENDED"}]}}}</script>
"""


def test_map_aquis_sector_maps_gics_labels_only():
    assert map_aquis_sector("Materials") == "Materials"
    assert map_aquis_sector("Healthcare") == "Health Care"
    assert map_aquis_sector("Financials ") == "Financials"
    assert map_aquis_sector("Finance") == ""
    assert map_aquis_sector("Financial Conglomerates") == ""
    assert map_aquis_sector("") == ""


def test_parse_companies_page_reads_isin_and_sector():
    parsed = parse_companies_page(MARU_HTML)
    assert parsed == [
        {
            "ticker": "MARU",
            "name": "Marula Mining PLC",
            "isin": "GB00BNBS4S95",
            "sector": "Materials",
            "instrument_type": "Ordinary Shares",
            "status": "SUSPENDED",
        }
    ]


def test_evaluate_row_accepts_suspended_ordinary_share():
    result = evaluate_row(target(), indexed(company()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"
    assert result["aquis_ticker"] == "MARU"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Materials"


def test_evaluate_row_rejects_isin_mismatch():
    result = evaluate_row(target(isin="GB00BF01VL55", name="Ace Liberty"), indexed(company()))

    assert result["decision"] == "no_aquis_match"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_bonds_and_finance():
    bond = evaluate_row(target(), indexed(company(instrument_type="Bonds")))
    finance = evaluate_row(target(), indexed(company(sector="Finance")))

    assert bond["decision"] == "unsupported_instrument_type"
    assert finance["decision"] == "unsupported_sector"
    assert bond["sector_update"] == finance["sector_update"] == ""
