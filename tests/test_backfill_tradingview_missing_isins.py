from scripts.backfill_tradingview_missing_isins import (
    TradingViewRow,
    asset_type_matches,
    build_metadata_updates,
    evaluate_row,
    ticker_variants,
)


def tv_row(**overrides):
    values = {
        "request_symbol": "TSX:SHOP",
        "symbol": "SHOP",
        "name": "Shopify, Inc. Class A",
        "exchange": "TSX",
        "instrument_type": "stock",
        "subtype": "common",
        "isin": "CA82509L1076",
        "sector": "Commercial Services",
        "industry": "Miscellaneous Commercial Services",
        "country": "Canada",
        "typespecs": ("common",),
    }
    values.update(overrides)
    return TradingViewRow(**values)


def target_row(**overrides):
    values = {
        "ticker": "SHOP",
        "exchange": "TSX",
        "asset_type": "Stock",
        "name": "Shopify Inc.",
        "isin": "",
    }
    values.update(overrides)
    return values


def test_ticker_variants_uses_dots_for_canadian_classes():
    assert ticker_variants("AD-UN", "TSX") == ["AD-UN", "AD.UN"]
    assert ticker_variants("AAA.P", "TSXV") == ["AAA.P"]
    assert ticker_variants("VOLV-B", "STO") == ["VOLV-B", "VOLV_B"]
    assert ticker_variants("PETR4", "B3") == ["PETR4"]


def test_asset_type_matches_stock_and_etf_rows():
    assert asset_type_matches(target_row(asset_type="Stock"), tv_row(instrument_type="stock"))
    assert asset_type_matches(
        target_row(asset_type="ETF"),
        tv_row(instrument_type="fund", subtype="etf", typespecs=("etf",)),
    )
    assert not asset_type_matches(target_row(asset_type="ETF"), tv_row(instrument_type="stock"))


def test_evaluate_row_accepts_strict_match():
    result = evaluate_row(target_row(), tv_row(), {})
    assert result["decision"] == "accept"
    assert result["tv_isin"] == "CA82509L1076"


def test_evaluate_row_rejects_exchange_mismatch():
    result = evaluate_row(target_row(), tv_row(exchange="NASDAQ"), {})
    assert result["decision"] == "exchange_mismatch"


def test_evaluate_row_rejects_name_mismatch():
    result = evaluate_row(target_row(), tv_row(name="Unrelated Mining Corp."), {})
    assert result["decision"] == "name_mismatch"


def test_evaluate_row_rejects_conflicting_existing_isin_peer():
    result = evaluate_row(
        target_row(),
        tv_row(),
        {"CA82509L1076": [target_row(name="Unrelated Mining Corp.", isin="CA82509L1076")]},
    )
    assert result["decision"] == "isin_peer_name_mismatch"


def test_evaluate_row_blocks_known_collision_guard():
    result = evaluate_row(
        target_row(ticker="BMR", exchange="NASDAQ", name="Beamr Imaging Ltd."),
        tv_row(
            request_symbol="NASDAQ:BMR",
            symbol="BMR",
            exchange="NASDAQ",
            name="Beamr Imaging Ltd.",
            isin="IL0011832438",
        ),
        {},
    )
    assert result["decision"] == "blocked_known_collision"


def test_build_metadata_updates_emits_review_gated_isin_update():
    updates = build_metadata_updates([evaluate_row(target_row(), tv_row(), {})])
    assert updates == [
        {
            "ticker": "SHOP",
            "exchange": "TSX",
            "field": "isin",
            "decision": "update",
            "proposed_value": "CA82509L1076",
            "confidence": "0.78",
            "reason": (
                "TradingView free scanner supplied a valid ISIN for a row without ISIN; accepted only after exact "
                "scanner exchange, symbol normalization, asset type, issuer/product-name, existing-ISIN peer, "
                "and ISIN-checksum gates matched."
            ),
        }
    ]
