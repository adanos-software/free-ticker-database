from scripts.backfill_nasdaq_baltic_isin_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    index_by_isin,
    map_baltic_industry,
    parse_instrument_page,
    parse_share_records,
)


def target(**overrides):
    values = {
        "ticker": "AV1",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Arco Vara AS",
        "isin": "EE3100034653",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def share(**overrides):
    values = {
        "Ticker": "ARC1T",
        "Name": "Arco Vara",
        "ISIN": "EE3100034653",
        "Industry": "Real Estate",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_isin(parse_share_records(list(rows)))


def test_map_baltic_industry_maps_telecommunications_and_strips_padding():
    assert map_baltic_industry("Telecommunications") == "Communication Services"
    assert map_baltic_industry("Consumer Staples ") == "Consumer Staples"
    assert map_baltic_industry("Basic Materials") == "Materials"
    assert map_baltic_industry("Technology") == "Information Technology"
    assert map_baltic_industry("Financials") == "Financials"
    assert map_baltic_industry("Finance") == ""
    assert map_baltic_industry("Financial Conglomerates") == ""
    assert map_baltic_industry("") == ""


def test_evaluate_row_accepts_exact_isin_real_estate():
    result = evaluate_row(target(), indexed(share()))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Real Estate"
    assert result["baltic_ticker"] == "ARC1T"


def test_evaluate_row_maps_telecommunications_to_communication_services():
    result = evaluate_row(
        target(ticker="ZWS", name="Telia Lietuva AB", isin="LT0000123911"),
        indexed(share(Ticker="TEL1L", Name="Telia Lietuva", ISIN="LT0000123911", Industry="Telecommunications")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Communication Services"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Communication Services"


def test_evaluate_row_accepts_official_financials_without_inventing_finance():
    result = evaluate_row(
        target(ticker="X8K", name="AS Infortar", isin="EE3100149394"),
        indexed(share(Ticker="INF1T", Name="Infortar", ISIN="EE3100149394", Industry="Financials")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"


def test_evaluate_row_rejects_missing_workbook_isin():
    result = evaluate_row(
        target(ticker="DYC", name="AS Ekspress Grupp", isin="EE3100016965"),
        indexed(share()),
    )

    assert result["decision"] == "no_baltic_match"
    assert result["sector_update"] == ""


def test_parse_instrument_page_reads_labeled_isin_and_top_industry():
    html = """
    <strong>EEG1T</strong>
    <span class="text-muted text-thin">&nbsp;|&nbsp;</span>
    <strong>ISIN EE3100016965</strong>
    <p class="text-grey">
    Consumer Discretionary&gt;
    Media</p>
    """
    parsed = parse_instrument_page(html)
    assert parsed["isin"] == "EE3100016965"
    assert parsed["ticker"] == "EEG1T"
    assert parsed["industry"] == "Consumer Discretionary"


def test_evaluate_row_accepts_instrument_page_consumer_discretionary():
    result = evaluate_row(
        target(ticker="DYC", name="AS Ekspress Grupp", isin="EE3100016965"),
        indexed(share(Ticker="EEG1T", Name="Ekspress Grupp", ISIN="EE3100016965", Industry="Consumer Discretionary")),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Discretionary"


def test_evaluate_row_rejects_empty_and_unsupported_industry():
    empty = evaluate_row(target(), indexed(share(Industry="")))
    junk = evaluate_row(target(), indexed(share(Industry="Financial Conglomerates")))

    assert empty["decision"] == "empty_industry"
    assert junk["decision"] == "unsupported_industry"
    assert empty["sector_update"] == ""
    assert junk["sector_update"] == ""
    assert build_metadata_updates([empty, junk]) == []
