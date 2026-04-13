from __future__ import annotations

import csv

from scripts.backfill_etf_categories_from_names import (
    build_metadata_updates,
    classify_etf_category,
    evaluate_etf_row,
    load_existing_classifier_update_keys,
    load_ticker_rows,
    prune_stale_classifier_updates,
    verify_etf_categories,
)


def test_load_ticker_rows_reads_csv(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "sector"])
        writer.writeheader()
        writer.writerow({"ticker": "BND", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "Example Bond ETF", "sector": ""})

    assert load_ticker_rows(path)[0]["ticker"] == "BND"


def test_classify_etf_category_uses_specific_order_before_equity_fallback():
    assert classify_etf_category("Example US Corporate Bond ETF") == ("Fixed Income", "corporate_bonds")
    assert classify_etf_category("Example S&P 500 Index Fund") == ("Equity", "large_cap")
    assert classify_etf_category("Example Dow Jones Industrial Average ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Example NY Dow Industrial Average ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Example Gold Futures ETF") == ("Commodity", "commodities")
    assert classify_etf_category("Example Equity Index Fund") == ("Equity", "equities")
    assert classify_etf_category("Example Nikkei 225 Currency-hedged ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Example Currency Basket ETF") == ("Currency", "currencies")


def test_classify_etf_category_handles_common_non_english_markers():
    assert classify_etf_category("KODEX 27-12 \ud68c\uc0ac\ucc44(AA-\uc774\uc0c1)\uc561\ud2f0\ube0c") == (
        "Fixed Income",
        "corporate_bonds",
    )
    assert classify_etf_category("TIGER \ubbf8\uad6d\ucd08\ub2e8\uae30(3\uac1c\uc6d4\uc774\ud558)\uad6d\ucc44") == (
        "Fixed Income",
        "treasury_bonds",
    )
    assert classify_etf_category("KODEX \ubbf8\uad6dS&P500\ub370\uc77c\ub9ac\ucee4\ubc84\ub4dc\ucf5cOTM") == (
        "Alternative",
        "alternative",
    )
    assert classify_etf_category("TIGER \uc5d4\ube44\ub514\uc544\ubbf8\uad6d\ucc44\ucee4\ubc84\ub4dc\ucf5c\ubc38\ub7f0\uc2a4") == (
        "Alternative",
        "alternative",
    )


def test_evaluate_etf_row_accepts_only_missing_etf_category():
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""}
    )["decision"] == "accept"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Bonds"}
    )["decision"] == "already_has_category"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "Stock", "name": "Example Corporate Bond ETF", "sector": ""}
    )["decision"] == "not_etf"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Wrapper", "sector": ""}
    )["decision"] == "no_rule_match"


def test_verify_etf_categories_filters_exchange_and_existing_category():
    results = verify_etf_categories(
        [
            {"ticker": "A", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""},
            {"ticker": "B", "exchange": "LSE", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""},
            {"ticker": "C", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Bonds"},
        ],
        exchanges={"XETRA"},
    )

    assert [result["ticker"] for result in results] == ["A"]
    assert results[0]["category_update"] == "Fixed Income"


def test_verify_etf_categories_refreshes_existing_classifier_updates():
    results = verify_etf_categories(
        [
            {"ticker": "A", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Corporate Bonds"},
            {"ticker": "B", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Corporate Bonds"},
        ],
        exchanges={"XETRA"},
        existing_classifier_update_keys={("A", "XETRA")},
    )

    assert [result["ticker"] for result in results] == ["A"]
    assert results[0]["decision"] == "accept"


def test_build_metadata_updates_emits_reviewed_etf_category_update():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "BND",
                "exchange": "NYSE ARCA",
                "category_update": "Fixed Income",
                "matched_rule": "corporate_bonds",
            },
            {"decision": "no_rule_match", "ticker": "BAD", "exchange": "NYSE ARCA"},
        ]
    )

    assert updates == [
        {
            "ticker": "BND",
            "exchange": "NYSE ARCA",
            "field": "etf_category",
            "decision": "update",
            "proposed_value": "Fixed Income",
            "confidence": "0.68",
            "reason": "Deterministic ETF-name classifier mapped the product name to 'Fixed Income' via rule 'corporate_bonds'. This is an etf_category fill, not a stock-sector assertion.",
        }
    ]


def test_load_existing_classifier_update_keys_reads_only_classifier_metadata_rows(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "KEEP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "OTHER",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Bonds",
                    "confidence": "0.88",
                    "reason": "Sector/category propagated from same-ISIN listing peers.",
                },
            ]
        )

    assert load_existing_classifier_update_keys(path) == {("KEEP", "XETRA")}


def test_prune_stale_classifier_updates_removes_legacy_classifier_rows_and_keeps_other_sources(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "KEEP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "DROP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Fixed Income",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Fixed Income' via rule 'fixed_income'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "OTHER",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Bonds",
                    "confidence": "0.88",
                    "reason": "Sector/category propagated from same-ISIN listing peers.",
                },
            ]
        )

    prune_stale_classifier_updates(
        path,
        [
            {
                "ticker": "KEEP",
                "exchange": "XETRA",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Equity",
                "confidence": "0.68",
                "reason": "new reason",
            }
        ],
    )

    rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
    assert [(row["ticker"], row["field"], row["proposed_value"]) for row in rows] == [
        ("OTHER", "sector", "Bonds"),
    ]
