from __future__ import annotations

from scripts.backfill_sector_from_isin_peers import (
    build_metadata_updates,
    evaluate_sector_peer_row,
    index_peer_sectors,
    verify_sector_peers,
)


def test_index_peer_sectors_uses_isin_and_asset_type():
    rows = [
        {"ticker": "ABC", "exchange": "XETRA", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Industrials"},
        {"ticker": "ABC", "exchange": "LSE", "asset_type": "ETF", "isin": "DE000ABC1234", "sector": "Developed Markets"},
        {"ticker": "EMPTY", "exchange": "LSE", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": ""},
    ]

    indexed = index_peer_sectors(rows)

    assert [row["sector"] for row in indexed[("DE000ABC1234", "Stock")]] == ["Industrials"]
    assert [row["sector"] for row in indexed[("DE000ABC1234", "ETF")]] == ["Developed Markets"]


def test_evaluate_sector_peer_row_accepts_single_normalized_sector():
    result = evaluate_sector_peer_row(
        {"ticker": "ABC", "exchange": "LSE", "asset_type": "Stock", "name": "ABC PLC", "isin": "DE000ABC1234", "sector": ""},
        [
            {"ticker": "ABC", "exchange": "XETRA", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Industrials"},
            {"ticker": "ABC", "exchange": "SIX", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Industrials"},
        ],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Industrials"


def test_evaluate_sector_peer_row_rejects_conflicting_peer_sectors():
    result = evaluate_sector_peer_row(
        {"ticker": "ABC", "exchange": "LSE", "asset_type": "Stock", "name": "ABC PLC", "isin": "DE000ABC1234", "sector": ""},
        [
            {"ticker": "ABC", "exchange": "XETRA", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Industrials"},
            {"ticker": "ABC", "exchange": "SIX", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Financials"},
        ],
    )

    assert result["decision"] == "conflicting_peer_sectors"


def test_verify_sector_peers_filters_missing_sector_rows():
    ticker_rows = [
        {"ticker": "ABC", "exchange": "LSE", "asset_type": "Stock", "name": "ABC PLC", "isin": "DE000ABC1234", "sector": ""},
        {"ticker": "DEF", "exchange": "LSE", "asset_type": "Stock", "name": "DEF PLC", "isin": "DE000DEF1234", "sector": "Health Care"},
        {"ticker": "ETF", "exchange": "LSE", "asset_type": "ETF", "name": "ETF", "isin": "IE000ETF1234", "sector": ""},
    ]
    listing_rows = [
        {"ticker": "ABC", "exchange": "XETRA", "asset_type": "Stock", "isin": "DE000ABC1234", "sector": "Industrials"},
        {"ticker": "ETF", "exchange": "XETRA", "asset_type": "ETF", "isin": "IE000ETF1234", "sector": "Government Bonds"},
    ]

    results = verify_sector_peers(ticker_rows, listing_rows, exchanges={"LSE"}, asset_types={"Stock"})

    assert len(results) == 1
    assert results[0]["ticker"] == "ABC"
    assert results[0]["decision"] == "accept"


def test_build_metadata_updates_emits_sector_override():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "ABC",
                "exchange": "LSE",
                "asset_type": "Stock",
                "sector_update": "Industrials",
            },
            {"decision": "no_sector_peer", "ticker": "BAD", "exchange": "LSE", "sector_update": ""},
        ]
    )

    assert updates == [
        {
            "ticker": "ABC",
            "exchange": "LSE",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Industrials",
            "confidence": "0.88",
            "reason": "Sector/category propagated from same-ISIN listing peers after requiring the primary row to be missing sector and all same-asset peer sectors to normalize to one value.",
        }
    ]
