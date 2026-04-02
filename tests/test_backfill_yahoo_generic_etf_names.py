from __future__ import annotations

import socket

from scripts.backfill_yahoo_generic_etf_names import (
    build_metadata_updates,
    evaluate_generic_etf_row,
    is_generic_etf_wrapper_name,
    looks_like_specific_fund_name,
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


def test_evaluate_generic_etf_row_accepts_specific_yahoo_name():
    row = {
        "ticker": "IBCA",
        "exchange": "NYSE ARCA",
        "name": "iShares Trust",
        "isin": "",
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


def test_evaluate_generic_etf_row_rejects_exchange_mismatch():
    row = {
        "ticker": "AVTM",
        "exchange": "NASDAQ",
        "name": "American Century ETF Trust",
        "isin": "",
    }
    yahoo_result = {
        "exists": True,
        "symbol": "AVTM",
        "quoteType": "ETF",
        "longName": "Avantis Total Equity Markets ETF",
        "shortName": "ignored",
        "exchange": "PCX",
        "fullExchangeName": "NYSEArca",
        "isin": "",
        "history_rows": 5,
    }
    result = evaluate_generic_etf_row(row, yahoo_result)
    assert result["decision"] == "exchange_mismatch"


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
