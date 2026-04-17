from __future__ import annotations

import csv
import json
from argparse import Namespace

from scripts.fetch_financialdata_symbols import (
    FinancialDataSymbol,
    build_match_report,
    extract_api_rows,
    normalize_api_row,
    run,
    split_financialdata_symbol,
)


def test_split_financialdata_symbol_maps_supported_suffixes() -> None:
    assert split_financialdata_symbol("SHEL.L") == ("SHEL", ".L", "LSE")
    assert split_financialdata_symbol("000080.KS") == ("000080", ".KS", "KRX")
    assert split_financialdata_symbol("SHOP.TO") == ("SHOP", ".TO", "TSX")
    assert split_financialdata_symbol("7203.T") == ("7203", ".T", "TSE")
    assert split_financialdata_symbol("RELIANCE.NS") == ("RELIANCE", ".NS", "NSE_IN")
    assert split_financialdata_symbol("ABC.UNKNOWN") == ("ABC", ".UNKNOWN", "")


def test_extract_api_rows_accepts_list_or_wrapped_payload() -> None:
    rows = [{"trading_symbol": "SHEL.L", "registrant_name": "Shell plc"}]

    assert extract_api_rows(rows) == rows
    assert extract_api_rows({"data": rows}) == rows
    assert extract_api_rows({"results": rows}) == rows


def test_normalize_api_row_labels_source_as_secondary_review() -> None:
    row = normalize_api_row(
        {"trading_symbol": " shel.l ", "registrant_name": " Shell   plc "},
        observed_at="2026-04-17T00:00:00Z",
    )

    assert row.financialdata_symbol == "SHEL.L"
    assert row.base_ticker == "SHEL"
    assert row.suffix == ".L"
    assert row.mapped_exchange == "LSE"
    assert row.registrant_name == "Shell plc"
    assert row.source_confidence == "secondary_review"
    assert row.review_needed == "true"


def test_build_match_report_classifies_exact_missing_and_name_mismatch() -> None:
    symbols = [
        FinancialDataSymbol("SHEL.L", "SHEL", ".L", "LSE", "Shell plc", "secondary_review", "true", "now"),
        FinancialDataSymbol("000080.KS", "000080", ".KS", "KRX", "HiteJinro Co., Ltd.", "secondary_review", "true", "now"),
        FinancialDataSymbol("SHOP.TO", "SHOP", ".TO", "TSX", "Shopify Inc.", "secondary_review", "true", "now"),
        FinancialDataSymbol("ABC.UNKNOWN", "ABC", ".UNKNOWN", "", "ABC Ltd", "secondary_review", "true", "now"),
        FinancialDataSymbol("MISSING.L", "MISSING", ".L", "LSE", "Missing plc", "secondary_review", "true", "now"),
        FinancialDataSymbol("RELIANCE.NS", "RELIANCE", ".NS", "NSE_IN", "Reliance Industries", "secondary_review", "true", "now"),
    ]
    listings = [
        {"listing_key": "LSE::SHEL", "ticker": "SHEL", "exchange": "LSE", "asset_type": "Stock", "name": "Shell plc"},
        {"listing_key": "KRX::000080", "ticker": "000080", "exchange": "KRX", "asset_type": "Stock", "name": "Other Corp"},
        {"listing_key": "NYSE::SHOP", "ticker": "SHOP", "exchange": "NYSE", "asset_type": "Stock", "name": "Shopify Inc."},
    ]

    by_symbol = {match.symbol.financialdata_symbol: match for match in build_match_report(symbols, listings)}

    assert by_symbol["SHEL.L"].match_status == "matched_exchange_name_ok"
    assert by_symbol["000080.KS"].match_status == "matched_exchange_name_mismatch"
    assert by_symbol["SHOP.TO"].match_status == "ticker_present_other_exchange"
    assert by_symbol["ABC.UNKNOWN"].match_status == "unmapped_suffix"
    assert by_symbol["MISSING.L"].match_status == "missing_from_database"
    assert by_symbol["MISSING.L"].review_scope == "current_exchange_gap"
    assert by_symbol["MISSING.L"].quality_gate == "needs_official_source_or_isin"
    assert by_symbol["RELIANCE.NS"].review_scope == "global_expansion_candidate"
    assert by_symbol["RELIANCE.NS"].quality_gate == "needs_venue_onboarding_and_isin"


def test_run_writes_outputs_with_mocked_fetch(tmp_path, monkeypatch) -> None:
    from scripts import fetch_financialdata_symbols as module

    listings_csv = tmp_path / "listings.csv"
    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange", "asset_type", "name"])
        writer.writeheader()
        writer.writerow(
            {
                "listing_key": "LSE::SHEL",
                "ticker": "SHEL",
                "exchange": "LSE",
                "asset_type": "Stock",
                "name": "Shell plc",
            }
        )

    def fake_fetch_all_symbols(**_kwargs):
        return (
            [
                FinancialDataSymbol(
                    "SHEL.L",
                    "SHEL",
                    ".L",
                    "LSE",
                    "Shell plc",
                    "secondary_review",
                    "true",
                    "2026-04-17T00:00:00Z",
                )
            ],
            {
                "endpoint": "https://example.test",
                "page_size": 500,
                "max_requests": 300,
                "requests_used": 1,
                "raw_rows": 1,
                "truncated_by_request_limit": False,
            },
        )

    monkeypatch.setenv("FINANCIALDATA_API_KEY", "test-key")
    monkeypatch.setattr(module, "fetch_all_symbols", fake_fetch_all_symbols)

    args = Namespace(
        endpoint="https://example.test",
        api_key="",
        api_key_env="FINANCIALDATA_API_KEY",
        page_size=500,
        max_requests=300,
        sleep_seconds=0,
        timeout_seconds=1,
        listings_csv=listings_csv,
        symbols_csv=tmp_path / "symbols.csv",
        symbols_json=tmp_path / "symbols.json",
        report_csv=tmp_path / "report.csv",
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
        current_gaps_csv=tmp_path / "current_gaps.csv",
        global_expansion_csv=tmp_path / "global_expansion.csv",
    )

    summary = run(args)

    assert summary["deduped_rows"] == 1
    assert summary["match_status_counts"] == {"matched_exchange_name_ok": 1}
    assert summary["review_scope_counts"] == {"secondary_reference": 1}
    assert json.loads(args.report_json.read_text(encoding="utf-8"))["summary"]["requests_used"] == 1
    assert "# FinancialData Symbol Match" in args.report_md.read_text(encoding="utf-8")
