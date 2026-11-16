from scripts.rebuild_dataset import drop_duplicate_ticker_aliases


def _etf(ticker, exchange, isin, alias, asset_type="ETF"):
    return {"ticker": ticker, "name": f"Fund {ticker}", "exchange": exchange, "asset_type": asset_type, "isin": isin, "aliases": [alias]}


def test_duplicate_etf_alias_kept_by_same_isin_listings_that_tie_each_other():
    # Two listings of one fund (same ISIN, equal scores) must not tie each other out
    # when the only other holder is a lower-scoring entity.
    rows = [
        _etf("CSPX", "LSE", "IE00B5BMR087", "core s&p 500"),
        _etf("SXR8", "XETRA", "IE00B5BMR087", "core s&p 500"),
        _etf("CSPXF", "OTC", "IE0031442068", "core s&p 500"),
    ]

    cleaned = drop_duplicate_ticker_aliases(rows)

    assert [row["aliases"] for row in cleaned] == [["core s&p 500"], ["core s&p 500"], []]


def test_duplicate_etf_alias_still_dropped_when_two_entities_tie():
    rows = [
        _etf("AAA", "LSE", "IE00B5BMR087", "shared fund"),
        _etf("BBB", "XETRA", "IE0031442068", "shared fund"),
    ]

    assert [row["aliases"] for row in drop_duplicate_ticker_aliases(rows)] == [[], []]


def test_duplicate_stock_alias_keeps_row_level_tie_rule():
    # Stock and mixed aliases keep the previous behaviour (Validator-owned rows).
    rows = [
        _etf("AAA", "LSE", "GB0000000001", "shared co", asset_type="Stock"),
        _etf("AAA", "XETRA", "GB0000000001", "shared co", asset_type="Stock"),
        _etf("BBB", "OTC", "US0000000002", "shared co", asset_type="Stock"),
    ]

    assert [row["aliases"] for row in drop_duplicate_ticker_aliases(rows)] == [[], [], []]
