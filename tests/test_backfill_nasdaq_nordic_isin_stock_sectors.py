from scripts.backfill_nasdaq_nordic_isin_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_by_isin,
    map_nordic_sector,
    parse_nordic_share_rows,
)


def target(**overrides):
    values = {
        "ticker": "4HY",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Cheffelo AB (publ)",
        "isin": "SE0015556873",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def share(**overrides):
    values = {
        "ticker": "CHEF",
        "name": "Cheffelo",
        "exchange": "STO",
        "isin": "SE0015556873",
        "sector": "Consumer Staples",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_isin(list(rows))


def test_map_nordic_sector_maps_synonyms_and_rejects_finance_junk():
    assert map_nordic_sector("Telecommunications") == "Communication Services"
    assert map_nordic_sector("Technology") == "Information Technology"
    assert map_nordic_sector("Basic Materials") == "Materials"
    assert map_nordic_sector("Financials") == "Financials"
    assert map_nordic_sector("Finance") == ""
    assert map_nordic_sector("Financial Conglomerates") == ""
    assert map_nordic_sector("") == ""


def test_parse_nordic_share_rows_reads_masterfile_and_raw_listing_payloads():
    parsed = parse_nordic_share_rows([share()])
    raw = parse_nordic_share_rows(
        {
            "data": {
                "instrumentListing": {
                    "rows": [
                        {
                            "symbol": "DADC",
                            "fullName": "Danish Aerospace & Defence Co.",
                            "isin": "DK0061140407",
                            "sector": "Industrials",
                        }
                    ]
                }
            }
        }
    )
    assert parsed[0]["ticker"] == "CHEF"
    assert raw[0]["ticker"] == "DADC"
    assert raw[0]["isin"] == "DK0061140407"


def test_evaluate_row_accepts_exact_isin_consumer_staples():
    result = evaluate_row(target(), indexed(share()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Staples"
    assert result["nordic_ticker"] == "CHEF"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Consumer Staples"


def test_evaluate_row_maps_telecommunications():
    result = evaluate_row(
        target(ticker="TELX", name="Telia", isin="SE0007100581"),
        indexed(share(ticker="TELIA", name="Telia Company", isin="SE0007100581", sector="Telecommunications")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Communication Services"


def test_evaluate_row_rejects_finance_junk():
    result = evaluate_row(
        target(ticker="X8K", name="Unknown Finance", isin="SE0015550009"),
        indexed(share(ticker="FIN", name="Unknown Finance", isin="SE0015550009", sector="Finance")),
    )

    assert result["decision"] == "unsupported_nordic_sector"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_missing_isin():
    result = evaluate_row(target(isin="DK0000000001"), indexed(share()))

    assert result["decision"] == "no_nordic_match"
    assert result["sector_update"] == ""
