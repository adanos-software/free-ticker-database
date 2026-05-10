from __future__ import annotations

from scripts.backfill_stockanalysis_list_sectors import (
    StockAnalysisListRow,
    build_metadata_updates,
    evaluate_row,
    normalize_source_symbol,
    stockanalysis_screener_url,
)


def test_normalize_source_symbol_strips_exchange_prefix() -> None:
    assert normalize_source_symbol("tsxv/ARTG", "tsxv/") == "ARTG"
    assert normalize_source_symbol("SSNLF", "otc/") == "SSNLF"


def test_stockanalysis_screener_url_requests_sector_columns() -> None:
    url = stockanalysis_screener_url("OTC", page=2, count=1000)

    assert "exchangeCode-is-OTC%2Csubtype-is-stock" in url
    assert "c=no%2Cs%2Cn%2Cisin%2Csector%2Cindustry%2CmarketCap" in url
    assert "p=2" in url


def test_evaluate_row_accepts_exact_symbol_name_gated_sector() -> None:
    result = evaluate_row(
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "name": "Artemis Gold Inc.",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="TSXV",
            asset_type="Stock",
            symbol="ARTG",
            name="Artemis Gold Inc.",
            isin="CA04302L1004",
            sector="Basic Materials",
            industry="Gold",
            category="",
            source_url="https://stockanalysis.com/quote/tsxv/ARTG/",
        ),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"


def test_evaluate_row_rejects_name_mismatch() -> None:
    result = evaluate_row(
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "name": "Different Issuer Inc.",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="TSXV",
            asset_type="Stock",
            symbol="ARTG",
            name="Artemis Gold Inc.",
            isin="CA04302L1004",
            sector="Basic Materials",
            industry="Gold",
            category="",
            source_url="https://stockanalysis.com/quote/tsxv/ARTG/",
        ),
    )

    assert result["decision"] == "name_mismatch"
    assert result["sector_update"] == ""


def test_build_metadata_updates_labels_stockanalysis_list_as_secondary() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "ARTG",
                "exchange": "TSXV",
                "decision": "accept",
                "isin_update": "",
                "sector_update": "Materials",
                "category_update": "",
                "source_url": "https://stockanalysis.com/quote/tsxv/ARTG/",
            }
        ]
    )

    assert updates == [
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Materials",
            "confidence": "0.74",
            "reason": (
                "StockAnalysis exchange list screener supplied sector metadata for a row missing stock_sector; "
                "accepted as reviewed secondary metadata after exact exchange/symbol match and issuer-name gate. "
                "Source: https://stockanalysis.com/quote/tsxv/ARTG/"
            ),
        }
    ]
