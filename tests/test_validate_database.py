from __future__ import annotations

import json
from pathlib import Path

from scripts.validate_database import (
    build_validation_report,
    parse_adanos_aliases,
    write_json,
    write_markdown,
)


def ticker(
    symbol: str = "MSFT",
    *,
    asset_type: str = "Stock",
    stock_sector: str = "Information Technology",
    etf_category: str = "",
    isin: str = "US5949181045",
) -> dict[str, str]:
    return {
        "ticker": symbol,
        "name": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "asset_type": asset_type,
        "stock_sector": stock_sector,
        "etf_category": etf_category,
        "country": "United States",
        "country_code": "US",
        "isin": isin,
        "aliases": "microsoft",
    }


def listing(row: dict[str, str] | None = None) -> dict[str, str]:
    row = row or ticker()
    return {"listing_key": f"{row['exchange']}::{row['ticker']}", **row}


def scope(row: dict[str, str] | None = None, *, scope_reason: str = "primary_listing") -> dict[str, str]:
    row = row or ticker()
    key = f"{row['exchange']}::{row['ticker']}"
    return {
        "listing_key": key,
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "instrument_scope": "core",
        "scope_reason": scope_reason,
        "primary_listing_key": key,
    }


def adanos_reference(row: dict[str, str] | None = None, aliases: list[str] | None = None) -> dict[str, str]:
    row = row or ticker()
    return {
        "ticker": row["ticker"],
        "name": row["name"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "sector": row["stock_sector"] or row["etf_category"],
        "country": row["country"],
        "country_code": row["country_code"],
        "isin": row["isin"],
        "aliases": json.dumps(["microsoft"] if aliases is None else aliases),
    }


def entry_quality(row: dict[str, str] | None = None, quality_status: str = "pass") -> dict[str, str]:
    row = row or ticker()
    return {
        "listing_key": f"{row['exchange']}::{row['ticker']}",
        "quality_status": quality_status,
        "issue_types": "",
    }


def test_validation_report_passes_clean_minimal_dataset():
    tickers = [ticker()]
    report = build_validation_report(
        tickers=tickers,
        listings=[listing(tickers[0])],
        instrument_scopes=[scope(tickers[0])],
        adanos_reference=[adanos_reference(tickers[0])],
        entry_quality=[entry_quality(tickers[0])],
        allowed_warns=set(),
        adanos_alias_findings=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    assert report["passed"] is True
    assert report["summary"]["failed_error_gates"] == 0


def test_validation_report_fails_hard_release_gates():
    bad = ticker(
        "MSFT",
        stock_sector="Information Technology",
        etf_category="Equity ETF",
        isin="US0000000000",
    )
    duplicate = ticker("MSFT")
    report = build_validation_report(
        tickers=[bad, duplicate],
        listings=[listing(bad)],
        instrument_scopes=[scope(bad, scope_reason="secondary_cross_listing")],
        adanos_reference=[adanos_reference(bad, aliases=["bank"])],
        entry_quality=[
            {
                "listing_key": "NASDAQ::MSFT",
                "quality_status": "warn",
                "issue_types": "official_name_mismatch",
            }
        ],
        allowed_warns=set(),
        adanos_alias_findings=[
            {
                "ticker": "MSFT",
                "alias": "bank",
                "issue_type": "common_single_word_alias",
            }
        ],
        coverage_report={"global": {"tickers": 99, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["duplicate_primary_ticker_count"]["actual"] == 1
    assert gates["invalid_isin_rows"]["actual"] == 2
    assert gates["stock_rows_with_etf_category"]["actual"] == 1
    assert gates["primary_rows_that_are_known_secondary_cross_listings"]["actual"] == 2
    assert gates["entry_quality_unexpected_warn_count"]["actual"] == 1
    assert gates["adanos_alias_findings"]["actual"] == 1
    assert gates["adanos_alias_common_word_count"]["actual"] == 1
    assert gates["coverage_report_tickers_mismatch"]["actual"] == 1


def test_parse_adanos_aliases_rejects_non_json_or_non_string_items():
    assert parse_adanos_aliases("not-json") == ([], False)
    assert parse_adanos_aliases(json.dumps(["microsoft", 123])) == (["microsoft"], False)
    assert parse_adanos_aliases(json.dumps(["microsoft"])) == (["microsoft"], True)


def test_validation_report_accepts_custom_required_column_paths(tmp_path: Path):
    custom_tickers = tmp_path / "custom_tickers.csv"
    tickers = [ticker()]

    report = build_validation_report(
        tickers=tickers,
        listings=[listing(tickers[0])],
        instrument_scopes=[scope(tickers[0])],
        adanos_reference=[adanos_reference(tickers[0])],
        entry_quality=[entry_quality(tickers[0])],
        allowed_warns=set(),
        adanos_alias_findings=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        path_to_columns={custom_tickers: set(tickers[0])},
        required_columns_by_path={custom_tickers: set(tickers[0])},
        generated_at="2026-04-18T00:00:00Z",
    )

    assert report["passed"] is True


def test_validation_report_writers(tmp_path: Path):
    tickers = [ticker()]
    report = build_validation_report(
        tickers=tickers,
        listings=[listing(tickers[0])],
        instrument_scopes=[scope(tickers[0])],
        adanos_reference=[adanos_reference(tickers[0])],
        entry_quality=[entry_quality(tickers[0])],
        allowed_warns=set(),
        adanos_alias_findings=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    json_out = tmp_path / "validation_report.json"
    md_out = tmp_path / "validation_report.md"
    write_json(json_out, report)
    write_markdown(md_out, report)

    assert json.loads(json_out.read_text())["passed"] is True
    assert "Status: `PASS`" in md_out.read_text()
