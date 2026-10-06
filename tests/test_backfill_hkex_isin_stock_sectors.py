from scripts.backfill_hkex_isin_stock_sectors import build_metadata_updates, evaluate_row


def target(**overrides):
    values = {
        "ticker": "EPU",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Emperor Watch & Jewellery Limited",
        "isin": "HK0000047982",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def hkex_row(**overrides):
    values = {
        "ticker": "00887",
        "name": "EMPEROR WATCH&J",
        "isin": "HK0000047982",
    }
    values.update(overrides)
    return values


def capture(**overrides):
    values = {
        "ticker": "00887",
        "hkex_industry": "Consumer Discretionary - Specialty Retail - Other Retailers",
        "error": "",
    }
    values.update(overrides)
    return values


def test_evaluate_row_accepts_exact_isin_hsic():
    result = evaluate_row(target(), hkex_row(), capture())

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Discretionary"
    assert result["hkex_ticker"] == "00887"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Consumer Discretionary"


def test_evaluate_row_maps_financials_brokerage_hsic():
    result = evaluate_row(
        target(ticker="HQF", name="Emperor Capital Group Limited", isin="BMG313751015"),
        hkex_row(ticker="00717", name="EMPEROR CAPITAL", isin="BMG313751015"),
        capture(ticker="00717", hkex_industry="Financials - Other Financials - Securities & Brokerage"),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"


def test_evaluate_row_rejects_different_hkex_isin():
    result = evaluate_row(
        target(ticker="HIUC", name="China Taiping Insurance Holdings Company Limited", isin="HK0000055787"),
        None,
        capture(ticker="00966", hkex_industry="Financials - Insurance - Insurance"),
    )

    assert result["decision"] == "no_hkex_isin"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_conglomerates_hsic():
    result = evaluate_row(
        target(),
        hkex_row(),
        capture(hkex_industry="Conglomerates - Conglomerates - Conglomerates"),
    )

    assert result["decision"] == "unsupported_hsic_sector"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []
