from __future__ import annotations

import csv

from scripts.backfill_cboe_us_lmm_metadata import (
    build_metadata_updates,
    cboe_etf_names_match,
    evaluate_rows,
    load_entry_quality_bats_etf_residual_rows,
    parse_lmm_csv,
    write_report_csv,
)


def test_parse_lmm_csv_maps_supported_asset_classes():
    rows = parse_lmm_csv(
        "issuer,symbol,security_name,lmm,asset_class\n"
        "Cultivar Capital,CVAR,Cultivar ETF,GTS,US Equity\n"
        "Issuer,ABXB,Abacus Flexible Bond Leaders ETF,GTS,Fixed Income\n"
    )

    assert rows["CVAR"]["asset_class"] == "US Equity"
    assert rows["ABXB"]["asset_class"] == "Fixed Income"


def test_load_entry_quality_bats_etf_residual_rows_filters_current_bats_etf_gaps(tmp_path):
    path = tmp_path / "entry_quality.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "asset_type", "name", "issue_types"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "KRUZ",
                    "exchange": "BATS",
                    "asset_type": "ETF",
                    "name": "Unusual Whales Subversive Republican Trading ETF",
                    "issue_types": "missing_etf_category",
                },
                {
                    "ticker": "SPY",
                    "exchange": "NYSE ARCA",
                    "asset_type": "ETF",
                    "name": "SPDR S&P 500 ETF Trust",
                    "issue_types": "missing_etf_category",
                },
                {
                    "ticker": "ABC",
                    "exchange": "BATS",
                    "asset_type": "Stock",
                    "name": "Example Inc.",
                    "issue_types": "expected_missing_primary_isin",
                },
                {
                    "ticker": "DONE",
                    "exchange": "BATS",
                    "asset_type": "ETF",
                    "name": "Complete ETF",
                    "issue_types": "",
                },
            ]
        )

    rows = load_entry_quality_bats_etf_residual_rows(path)

    assert [(row["ticker"], row["exchange"]) for row in rows] == [("KRUZ", "BATS")]


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


def test_cboe_etf_names_match_accepts_official_product_suffix_after_trust_wrapper():
    assert cboe_etf_names_match(
        "FPA Global Equity ETF",
        "Northern Lights Fund Trust III - FPA Global Equity ETF",
    )
    assert not cboe_etf_names_match(
        "Roundhill Top WeeklyPay ETF",
        "Roundhill WeeklyPay Universe ETF",
    )


def test_evaluate_rows_accepts_cboe_isin_for_official_product_suffix_match():
    results = evaluate_rows(
        [
            {
                "ticker": "FPAG",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Northern Lights Fund Trust III - FPA Global Equity ETF",
                "etf_category": "Equity",
                "isin": "",
            }
        ],
        {},
        {"FPAG": {"name": "FPA Global Equity ETF", "isin": "US66538R6311"}},
    )

    assert results[0]["decision"] == "accept_isin"
    assert results[0]["isin_update"] == "US66538R6311"


def test_evaluate_rows_accepts_cboe_pdf_isin_when_lmm_product_name_matches():
    results = evaluate_rows(
        [
            {
                "ticker": "FPAG",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Northern Lights Fund Trust III - FPA Global Equity ETF",
                "etf_category": "Equity",
                "isin": "",
            }
        ],
        {"FPAG": {"name": "FPA Global Equity ETF", "asset_class": "Other"}},
        {"FPAG": {"name": "FPA Global Equity ETF First Pacific Advisors", "isin": "US66538R6311"}},
    )

    assert results[0]["decision"] == "accept_isin"
    assert results[0]["isin_update"] == "US66538R6311"


def test_evaluate_rows_rejects_cboe_isin_already_present():
    results = evaluate_rows(
        [
            {
                "ticker": "FPAG",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Northern Lights Fund Trust III - FPA Global Equity ETF",
                "etf_category": "Equity",
                "isin": "",
            }
        ],
        {},
        {"FPAG": {"name": "FPA Global Equity ETF", "isin": "US66538R6311"}},
        existing_isins={"US66538R6311"},
    )

    assert results[0]["decision"] == "isin_already_present"
    assert results[0]["isin_update"] == ""


def test_evaluate_rows_entry_quality_issues_authorize_only_missing_fields():
    results = evaluate_rows(
        [
            {
                "ticker": "HFSI",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Hartford Strategic Income ETF",
                "issue_types": "expected_missing_primary_isin",
            }
        ],
        {"HFSI": {"name": "Hartford Strategic Income ETF", "asset_class": "Fixed Income"}},
        {},
    )

    assert results[0]["decision"] == "no_update"
    assert results[0]["category_update"] == ""


def test_evaluate_rows_entry_quality_category_issue_allows_category_update():
    results = evaluate_rows(
        [
            {
                "ticker": "KRUZ",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Unusual Whales Subversive Republican Trading ETF",
                "issue_types": "missing_etf_category",
            }
        ],
        {"KRUZ": {"name": "Unusual Whales Subversive Republican Trading ETF", "asset_class": "US Equity"}},
        {},
    )

    assert results[0]["decision"] == "accept_etf_category"
    assert results[0]["category_update"] == "Equity"


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


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "report.csv"
    write_report_csv(
        path,
        [
            {
                "ticker": "CVAR",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Cultivar ETF",
                "cboe_name": "Cultivar ETF",
                "cboe_asset_class": "US Equity",
                "cboe_isin": "US26923N8763",
                "category_update": "Equity",
                "isin_update": "US26923N8763",
                "decision": "accept_etf_category_isin",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")
