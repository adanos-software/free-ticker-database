from __future__ import annotations

from scripts.backfill_cboe_us_lmm_metadata import (
    build_metadata_updates,
    evaluate_rows,
    parse_lmm_csv,
)


def test_parse_lmm_csv_maps_supported_asset_classes():
    rows = parse_lmm_csv(
        "issuer,symbol,security_name,lmm,asset_class\n"
        "Cultivar Capital,CVAR,Cultivar ETF,GTS,US Equity\n"
        "Issuer,ABXB,Abacus Flexible Bond Leaders ETF,GTS,Fixed Income\n"
    )

    assert rows["CVAR"]["asset_class"] == "US Equity"
    assert rows["ABXB"]["asset_class"] == "Fixed Income"


def test_evaluate_rows_accepts_name_gated_category_and_isin():
    results = evaluate_rows(
        [
            {
                "ticker": "CVAR",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Cultivar ETF",
                "etf_category": "",
                "isin": "",
            }
        ],
        {"CVAR": {"name": "Cultivar ETF", "asset_class": "US Equity"}},
        {"CVAR": {"name": "Cultivar ETF", "isin": "US26923N8763"}},
    )

    assert results[0]["decision"] == "accept_etf_category_isin"
    assert results[0]["category_update"] == "Equity"
    assert results[0]["isin_update"] == "US26923N8763"


def test_build_metadata_updates_emits_separate_field_updates():
    updates = build_metadata_updates(
        [
            {
                "ticker": "CVAR",
                "exchange": "BATS",
                "category_update": "Equity",
                "isin_update": "US26923N8763",
            }
        ]
    )

    assert [update["field"] for update in updates] == ["etf_category", "isin"]
