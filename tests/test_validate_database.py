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


def listing_index(row: dict[str, str]) -> dict[str, str]:
    listed = listing(row)
    return {
        "listing_key": listed["listing_key"],
        "ticker": listed["ticker"],
        "exchange": listed["exchange"],
        "name": listed["name"],
        "asset_type": listed["asset_type"],
        "country": listed["country"],
        "country_code": listed["country_code"],
        "isin": listed["isin"],
        "wkn": "",
        "figi": "",
        "cik": "",
        "lei": "",
    }


def core_listing(row: dict[str, str]) -> dict[str, str]:
    listed = listing(row)
    return {
        **listed,
        "instrument_group_key": listed["isin"] or listed["listing_key"],
        "scope_reason": "primary_listing" if listed["isin"] else "primary_listing_missing_isin",
    }


def cross_listing(row: dict[str, str], *, is_primary: str) -> dict[str, str]:
    listed = listing(row)
    return {
        "isin": listed["isin"],
        "listing_key": listed["listing_key"],
        "ticker": listed["ticker"],
        "exchange": listed["exchange"],
        "is_primary": is_primary,
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
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    assert report["passed"] is True
    assert report["summary"]["failed_error_gates"] == 0


def test_validation_report_passes_listing_key_scope_and_cross_listing_gates():
    msft = ticker("MSFT")
    msf = ticker("MSF")
    msf["exchange"] = "XETRA"

    report = build_validation_report(
        tickers=[msft],
        listings=[listing(msft), listing(msf)],
        core_listings=[core_listing(msft)],
        listing_index=[listing_index(msft), listing_index(msf)],
        instrument_scopes=[
            scope(msft),
            {
                **scope(msf, scope_reason="secondary_cross_listing"),
                "instrument_scope": "extended",
                "primary_listing_key": "NASDAQ::MSFT",
            },
        ],
        cross_listings=[cross_listing(msft, is_primary="1"), cross_listing(msf, is_primary="0")],
        adanos_reference=[adanos_reference(msft)],
        entry_quality=[entry_quality(msft), entry_quality(msf)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 2}},
        generated_at="2026-04-18T00:00:00Z",
    )

    assert report["passed"] is True


def test_validation_report_fails_listing_key_scope_and_cross_listing_gates():
    msft = ticker("MSFT")
    msf = ticker("MSF")
    msf["exchange"] = "XETRA"
    bad_scope = {
        **scope(msf, scope_reason="secondary_cross_listing"),
        "instrument_scope": "extended",
        "primary_listing_key": "XETRA::MSF",
    }

    report = build_validation_report(
        tickers=[msft],
        listings=[listing(msft), listing(msf)],
        core_listings=[core_listing(msft), core_listing(msf)],
        listing_index=[listing_index(msft)],
        instrument_scopes=[scope(msft), bad_scope],
        cross_listings=[cross_listing(msft, is_primary="1")],
        adanos_reference=[adanos_reference(msft)],
        entry_quality=[entry_quality(msft), entry_quality(msf)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 2}},
        generated_at="2026-04-18T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["core_listing_scope_mismatch_count"]["actual"] == 1
    assert gates["listing_index_key_mismatch_count"]["actual"] == 1
    assert gates["invalid_scope_primary_link_rows"]["actual"] == 1
    assert gates["cross_listing_pair_mismatch_count"]["actual"] == 1


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
        review_remove_aliases=[{"ticker": "MSFT", "exchange": "NASDAQ", "alias": "microsoft"}],
        coverage_report={"global": {"tickers": 99, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["duplicate_primary_ticker_count"]["actual"] == 1
    assert gates["invalid_isin_rows"]["actual"] == 2
    assert gates["stock_rows_with_etf_category"]["actual"] == 2
    assert gates["primary_rows_that_are_known_secondary_cross_listings"]["actual"] == 2
    assert gates["entry_quality_unexpected_warn_count"]["actual"] == 1
    assert gates["adanos_alias_findings"]["actual"] == 1
    assert gates["adanos_alias_common_word_count"]["actual"] == 1
    assert gates["review_alias_removals_open_count"]["actual"] == 1
    assert gates["coverage_report_tickers_mismatch"]["actual"] == 1


def test_validation_report_fails_duplicate_public_aliases():
    msft = ticker("MSFT")
    apple = ticker("AAPL")
    apple["aliases"] = "microsoft"
    apple["isin"] = "US0378331005"

    report = build_validation_report(
        tickers=[msft, apple],
        listings=[listing(msft), listing(apple)],
        instrument_scopes=[scope(msft), scope(apple)],
        adanos_reference=[adanos_reference(msft), adanos_reference(apple)],
        entry_quality=[entry_quality(msft), entry_quality(apple)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 2, "listing_keys": 2}},
        generated_at="2026-04-18T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["duplicate_public_alias_count"]["actual"] == 1
    assert "NASDAQ::AAPL" in gates["duplicate_public_alias_count"]["details"][0]
    assert "NASDAQ::MSFT" in gates["duplicate_public_alias_count"]["details"][0]


def test_validation_report_fails_entry_quality_coverage_gaps():
    msft = ticker("MSFT")
    apple = ticker("AAPL")
    apple["isin"] = "US0378331005"
    stale = entry_quality(msft)
    stale["listing_key"] = "NYSE::STALE"

    report = build_validation_report(
        tickers=[msft, apple],
        listings=[listing(msft), listing(apple)],
        instrument_scopes=[scope(msft), scope(apple)],
        adanos_reference=[adanos_reference(msft), adanos_reference(apple)],
        entry_quality=[entry_quality(msft), entry_quality(msft), stale],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 2, "listing_keys": 2}},
        generated_at="2026-04-18T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["duplicate_entry_quality_listing_key_count"]["actual"] == 1
    assert gates["listing_rows_missing_entry_quality"]["actual"] == 1
    assert gates["entry_quality_rows_missing_listing"]["actual"] == 1


def test_validation_report_fails_rows_missing_country_metadata_despite_isin():
    bad = ticker()
    bad["country"] = ""
    bad["country_code"] = ""

    report = build_validation_report(
        tickers=[bad],
        listings=[listing(bad)],
        instrument_scopes=[scope(bad)],
        adanos_reference=[adanos_reference(bad)],
        entry_quality=[entry_quality(bad)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["rows_missing_country_metadata_despite_isin"]["actual"] == 2


def test_validation_report_checks_typed_metadata_across_listing_outputs():
    bad_listing = listing(ticker(asset_type="Stock", stock_sector="Financials", etf_category="Equity"))
    bad_core = {**bad_listing, "instrument_group_key": "US0000000001", "scope_reason": "primary_listing"}

    report = build_validation_report(
        tickers=[ticker()],
        listings=[bad_listing],
        core_listings=[bad_core],
        instrument_scopes=[scope(ticker())],
        adanos_reference=[adanos_reference(ticker())],
        entry_quality=[entry_quality(ticker())],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["stock_rows_with_etf_category"]["actual"] == 2


def test_validation_report_checks_metadata_override_typing_and_canonical_values():
    report = build_validation_report(
        tickers=[ticker("BND", asset_type="ETF", stock_sector="", etf_category="Fixed Income")],
        listings=[listing(ticker("BND", asset_type="ETF", stock_sector="", etf_category="Fixed Income"))],
        instrument_scopes=[scope(ticker("BND", asset_type="ETF", stock_sector="", etf_category="Fixed Income"))],
        adanos_reference=[adanos_reference(ticker("BND", asset_type="ETF", stock_sector="", etf_category="Fixed Income"))],
        entry_quality=[entry_quality(ticker("BND", asset_type="ETF", stock_sector="", etf_category="Fixed Income"))],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        review_metadata_updates=[
            {
                "ticker": "BND",
                "exchange": "NASDAQ",
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": "Technology",
                "reason": "bad typed field",
            },
            {
                "ticker": "BND",
                "exchange": "NASDAQ",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Trading",
                "reason": "noncanonical ETF category",
            },
        ],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["metadata_updates_typed_leakage"]["actual"] == 1
    assert gates["metadata_updates_noncanonical_typed_values"]["actual"] == 2


def test_validation_report_allows_review_gated_country_isin_prefix_mismatch():
    adr = ticker()
    adr["country"] = "Belgium"
    adr["country_code"] = "BE"
    adr["isin"] = "US03524A1088"

    report = build_validation_report(
        tickers=[adr],
        listings=[listing(adr)],
        instrument_scopes=[scope(adr)],
        adanos_reference=[adanos_reference(adr)],
        entry_quality=[entry_quality(adr)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        review_metadata_updates=[
            {
                "ticker": "MSFT",
                "exchange": "NASDAQ",
                "field": "country",
                "decision": "update",
                "proposed_value": "Belgium",
                "reason": "reviewed ADR country override",
            }
        ],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert gates["country_isin_prefix_mismatch_without_review"]["actual"] == 0


def test_validation_report_fails_unreviewed_country_isin_prefix_mismatch():
    bad = ticker()
    bad["country"] = "Belgium"
    bad["country_code"] = "BE"
    bad["isin"] = "US03524A1088"

    report = build_validation_report(
        tickers=[bad],
        listings=[listing(bad)],
        instrument_scopes=[scope(bad)],
        adanos_reference=[adanos_reference(bad)],
        entry_quality=[entry_quality(bad)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["country_isin_prefix_mismatch_without_review"]["actual"] == 2


def test_validation_report_ignores_blank_country_when_isin_prefix_is_not_mappable():
    bad = ticker(isin="XS2691037282")
    bad["country"] = ""
    bad["country_code"] = ""

    report = build_validation_report(
        tickers=[bad],
        listings=[listing(bad)],
        instrument_scopes=[scope(bad)],
        adanos_reference=[adanos_reference(bad)],
        entry_quality=[entry_quality(bad)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert gates["rows_missing_country_metadata_despite_isin"]["actual"] == 0


def test_validation_report_fails_rows_with_mojibake_names():
    bad = ticker()
    bad["name"] = "Grupo AeromÃ©xico, S.A.B. de C.V."

    report = build_validation_report(
        tickers=[bad],
        listings=[listing(bad)],
        instrument_scopes=[scope(bad)],
        adanos_reference=[adanos_reference(bad)],
        entry_quality=[entry_quality(bad)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert report["passed"] is False
    assert gates["rows_with_mojibake_names"]["actual"] == 2


def test_validation_report_allows_legitimate_non_ascii_names():
    good = ticker()
    good["name"] = "ATOM EDUCAÇÃO E EDITORA S.A."

    report = build_validation_report(
        tickers=[good],
        listings=[listing(good)],
        instrument_scopes=[scope(good)],
        adanos_reference=[adanos_reference(good)],
        entry_quality=[entry_quality(good)],
        allowed_warns=set(),
        adanos_alias_findings=[],
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-22T00:00:00Z",
    )

    gates = {gate["name"]: gate for gate in report["gates"]}
    assert gates["rows_with_mojibake_names"]["actual"] == 0


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
        review_remove_aliases=[],
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
        review_remove_aliases=[],
        coverage_report={"global": {"tickers": 1, "listing_keys": 1}},
        generated_at="2026-04-18T00:00:00Z",
    )

    json_out = tmp_path / "validation_report.json"
    md_out = tmp_path / "validation_report.md"
    write_json(json_out, report)
    write_markdown(md_out, report)

    assert json.loads(json_out.read_text())["passed"] is True
    assert "Status: `PASS`" in md_out.read_text()
