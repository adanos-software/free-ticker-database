from __future__ import annotations

import socket

from scripts.backfill_yahoo_generic_etf_names import (
    build_metadata_updates,
    evaluate_generic_etf_row,
    has_foreign_metadata_contamination,
    is_generic_etf_wrapper_name,
    looks_like_specific_fund_name,
    normalized_exchange_from_yahoo,
    parse_args,
    socket_timeout,
)


def test_generic_etf_wrapper_name_detection():
    assert is_generic_etf_wrapper_name("iShares Trust")
    assert is_generic_etf_wrapper_name("T. Rowe Price Exchange-Traded Funds, Inc.")
    assert not is_generic_etf_wrapper_name("iShares iBonds Dec 2035 Term Corporate ETF")


def test_specific_fund_name_detection():
    assert looks_like_specific_fund_name("iShares iBonds Dec 2035 Term Corporate ETF")
    assert looks_like_specific_fund_name("SPDR MSCI World StrategicFactors UCITS ETF")
    assert not looks_like_specific_fund_name("iShares Trust")
    assert not looks_like_specific_fund_name("Global X Funds")


def test_foreign_metadata_contamination_detection():
    assert has_foreign_metadata_contamination({"country_code": "CA", "isin": ""})
    assert has_foreign_metadata_contamination({"country_code": "US", "isin": "CA1234567890"})
    assert not has_foreign_metadata_contamination({"country_code": "US", "isin": "US1234567890"})


def test_normalized_exchange_from_yahoo_maps_us_venues():
    assert normalized_exchange_from_yahoo("NYSEArca", "PCX") == "NYSE ARCA"
    assert normalized_exchange_from_yahoo("NYSE", "NYQ") == "NYSE"
    assert normalized_exchange_from_yahoo("NasdaqGS", "NMS") == "NASDAQ"


def test_evaluate_generic_etf_row_accepts_specific_yahoo_name():
    row = {
        "ticker": "IBCA",
        "exchange": "NYSE ARCA",
        "name": "iShares Trust",
        "isin": "",
        "country_code": "US",
    }
    yahoo_result = {
        "exists": True,
        "symbol": "IBCA",
        "quoteType": "ETF",
        "longName": "iShares iBonds Dec 2035 Term Corporate ETF",
        "shortName": "ignored",
        "exchange": "PCX",
        "fullExchangeName": "NYSEArca",
        "isin": "US46438G3728",
        "history_rows": 5,
    }
    result = evaluate_generic_etf_row(row, yahoo_result)
    assert result["decision"] == "accept"
    assert result["specific_name"] is True
    assert result["exchange_match"] is True


def test_evaluate_generic_etf_row_accepts_contaminated_foreign_metadata():
    row = {
        "ticker": "AGGH",
        "exchange": "NYSE ARCA",
        "name": "Simplify Exchange Traded Funds",
        "isin": "IE00BDBRDM35",
        "country_code": "IE",
    }
    yahoo_result = {
        "exists": True,
        "symbol": "AGGH",
        "quoteType": "ETF",
        "longName": "Simplify Aggregate Bond ETF",
        "shortName": "ignored",
        "exchange": "PCX",
        "fullExchangeName": "NYSEArca",
        "isin": "US82889N7232",
        "history_rows": 5,
    }
    result = evaluate_generic_etf_row(row, yahoo_result)
    assert result["decision"] == "accept"
    assert result["foreign_contamination"] is True


def test_evaluate_generic_etf_row_accepts_compatible_us_exchange_remap():
    row = {
        "ticker": "NETZ",
        "exchange": "NASDAQ",
        "name": "TCW ETF Trust",
        "isin": "CA14116K4046",
        "country_code": "CA",
    }
    yahoo_result = {
        "exists": True,
        "symbol": "NETZ",
        "quoteType": "ETF",
        "longName": "TCW Transform Systems ETF",
        "shortName": "ignored",
        "exchange": "NYQ",
        "fullExchangeName": "NYSE",
        "isin": "",
        "history_rows": 5,
    }
    result = evaluate_generic_etf_row(row, yahoo_result)
    assert result["decision"] == "accept"
    assert result["normalized_yahoo_exchange"] == "NYSE"


def test_build_metadata_updates_emits_name_and_missing_isin():
    updates = build_metadata_updates(
        [
            {
                "ticker": "IBCA",
                "exchange": "NYSE ARCA",
                "decision": "accept",
                "current_isin": "",
                "yahoo_name": "iShares iBonds Dec 2035 Term Corporate ETF",
                "yahoo_isin": "US46438G3728",
            },
            {
                "ticker": "AVTM",
                "exchange": "NASDAQ",
                "decision": "exchange_mismatch",
                "current_isin": "",
                "yahoo_name": "Avantis Total Equity Markets ETF",
                "yahoo_isin": "",
            },
        ]
    )
    assert updates == [
        {
            "ticker": "IBCA",
            "exchange": "NYSE ARCA",
            "field": "name",
            "decision": "update",
            "proposed_value": "iShares iBonds Dec 2035 Term Corporate ETF",
            "confidence": "0.95",
            "reason": "Yahoo Finance verified a specific ETF product name for a row that previously only carried a generic trust/fund wrapper name.",
        },
        {
            "ticker": "IBCA",
            "exchange": "NYSE ARCA",
            "field": "isin",
            "decision": "update",
            "proposed_value": "US46438G3728",
            "confidence": "0.9",
            "reason": "Yahoo Finance provided an ISIN for an ETF row that previously had no ISIN.",
        },
    ]


def test_build_metadata_updates_handles_foreign_contamination_and_exchange_remap():
    updates = build_metadata_updates(
        [
            {
                "ticker": "NETZ",
                "exchange": "NASDAQ",
                "decision": "accept",
                "current_isin": "CA14116K4046",
                "foreign_contamination": True,
                "sector": "Information Technology",
                "normalized_yahoo_exchange": "NYSE",
                "yahoo_name": "TCW Transform Systems ETF",
                "yahoo_isin": "",
            }
        ]
    )
    assert updates == [
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "name",
            "decision": "update",
            "proposed_value": "TCW Transform Systems ETF",
            "confidence": "0.95",
            "reason": "Yahoo Finance verified a specific ETF product name for a row that previously only carried a generic trust/fund wrapper name.",
        },
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "exchange",
            "decision": "update",
            "proposed_value": "NYSE",
            "confidence": "0.9",
            "reason": "Yahoo Finance verified the ETF on a different but compatible US exchange venue than the contaminated row currently records.",
        },
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "country",
            "decision": "update",
            "proposed_value": "United States",
            "confidence": "0.9",
            "reason": "Generic ETF wrapper row carried foreign issuer metadata, but Yahoo verified a US-listed ETF for this symbol.",
        },
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "country_code",
            "decision": "update",
            "proposed_value": "US",
            "confidence": "0.9",
            "reason": "Must match corrected US ETF domicile.",
        },
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "isin",
            "decision": "clear",
            "proposed_value": "",
            "confidence": "0.85",
            "reason": "Current ISIN points to a foreign issuer, and Yahoo did not confirm a replacement ISIN for the verified US ETF.",
        },
        {
            "ticker": "NETZ",
            "exchange": "NASDAQ",
            "field": "sector",
            "decision": "clear",
            "proposed_value": "",
            "confidence": "0.8",
            "reason": "Existing sector likely came from foreign issuer contamination and is cleared until a verified ETF category is available.",
        },
    ]


def test_socket_timeout_restores_previous_default():
    previous = socket.getdefaulttimeout()
    with socket_timeout(7.5):
        assert socket.getdefaulttimeout() == 7.5
    assert socket.getdefaulttimeout() == previous


def test_parse_args_supports_offset_and_timeout():
    args = parse_args(["--offset", "40", "--timeout-seconds", "12", "--limit", "25"])
    assert args.offset == 40
    assert args.timeout_seconds == 12
    assert args.limit == 25
