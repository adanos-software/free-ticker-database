from __future__ import annotations

from scripts.backfill_official_name_mismatches import (
    build_updates,
    choose_candidate_name,
    clean_reference_name,
    format_display_name,
)


def test_clean_reference_name_strips_nasdaq_listing_suffix() -> None:
    assert clean_reference_name("Pathward Financial, Inc. - Common Stock") == "Pathward Financial, Inc."


def test_format_display_name_downcases_all_caps_while_preserving_acronyms() -> None:
    assert format_display_name("GABELLI MERCHANT PARTNERS PLC") == "Gabelli Merchant Partners PLC"
    assert format_display_name("KONTROL TECHNOLOGIES CORP.") == "Kontrol Technologies Corp."


def test_choose_candidate_name_prefers_mixed_case_variant() -> None:
    row = {"ticker": "CAG", "name": "ConAgra Foods Inc"}
    refs = [
        {
            "provider": "SEC",
            "source_key": "sec_company_tickers_exchange",
            "name": "CONAGRA BRANDS INC.",
        },
        {
            "provider": "Nasdaq Trader",
            "source_key": "nasdaq_other_listed",
            "name": "ConAgra Brands, Inc. Common Stock",
        },
    ]
    proposed, supporting = choose_candidate_name(row, refs)
    assert proposed == "ConAgra Brands, Inc."
    assert len(supporting) == 2


def test_build_updates_emits_non_otc_name_updates() -> None:
    entry_quality_rows = [
        {
            "listing_key": "NASDAQ::BRRR",
            "ticker": "BRRR",
            "exchange": "NASDAQ",
            "asset_type": "ETF",
            "name": "Valkyrie Bitcoin Fund",
            "quality_status": "warn",
            "issue_types": "official_name_mismatch",
        },
        {
            "listing_key": "NYSE ARCA::BILD",
            "ticker": "BILD",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "name": "Macquarie ETF Trust",
            "quality_status": "warn",
            "issue_types": "official_name_mismatch|missing_etf_category",
        },
        {
            "listing_key": "OTC::FOO",
            "ticker": "FOO",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Foo Corp",
            "quality_status": "warn",
            "issue_types": "official_name_mismatch",
        },
    ]
    masterfile_rows = [
        {
            "ticker": "BRRR",
            "exchange": "NASDAQ",
            "asset_type": "ETF",
            "provider": "SEC",
            "source_key": "sec_company_tickers_exchange",
            "name": "CoinShares Bitcoin ETF",
            "official": "true",
            "listing_status": "active",
        },
        {
            "ticker": "BILD",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "provider": "Nasdaq Trader",
            "source_key": "nasdaq_other_listed",
            "name": "Nomura Global Listed Infrastructure ETF",
            "official": "true",
            "listing_status": "active",
        },
        {
            "ticker": "FOO",
            "exchange": "OTC",
            "asset_type": "Stock",
            "provider": "SEC",
            "source_key": "sec_company_tickers_exchange",
            "name": "Foo Holdings Inc.",
            "official": "true",
            "listing_status": "active",
        },
    ]

    updates, report_rows = build_updates(
        entry_quality_rows,
        masterfile_rows,
        exchanges={"NASDAQ", "NYSE ARCA", "OTC"},
    )

    assert updates == [
        {
            "ticker": "BRRR",
            "exchange": "NASDAQ",
            "field": "name",
            "decision": "update",
            "proposed_value": "CoinShares Bitcoin ETF",
            "confidence": "0.93",
            "reason": "Official active reference name mismatch backfill from sec_company_tickers_exchange; replaced stale or generic listing name with the current reference name.",
        },
        {
            "ticker": "BILD",
            "exchange": "NYSE ARCA",
            "field": "name",
            "decision": "update",
            "proposed_value": "Nomura Global Listed Infrastructure ETF",
            "confidence": "0.93",
            "reason": "Official active reference name mismatch backfill from nasdaq_other_listed; replaced stale or generic listing name with the current reference name.",
        },
    ]
    assert {row["listing_key"] for row in report_rows if row["decision"] == "accept"} == {
        "NASDAQ::BRRR",
        "NYSE ARCA::BILD",
    }
