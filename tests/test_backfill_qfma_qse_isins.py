from __future__ import annotations

from scripts.backfill_qfma_qse_isins import (
    build_metadata_updates,
    evaluate_rows,
    parse_qfma_main_market_rows,
)


def qfma_payload() -> dict:
    return {
        "results": [
            {
                "Sector": "Banks & Financial Services",
                "Items": [
                    {
                        "Active": True,
                        "CompanyCode": "QNBK",
                        "CompanyName": "Qatar National Bank",
                        "ISIN": "ISIN: QA0006929895",
                        "Sector": "Banks & Financial Services",
                    },
                    {
                        "Active": True,
                        "CompanyCode": "BAD",
                        "CompanyName": "Bad Isin Co",
                        "ISIN": "ISIN: QA0000000000",
                    },
                    {
                        "Active": False,
                        "CompanyCode": "OLD",
                        "CompanyName": "Old Co",
                        "ISIN": "ISIN: QA0006929895",
                    },
                ],
            }
        ]
    }


def test_parse_qfma_main_market_rows_extracts_clean_isins() -> None:
    rows = parse_qfma_main_market_rows(qfma_payload(), "https://example.test/qfma")

    assert rows[0] == {
        "ticker": "QNBK",
        "name": "Qatar National Bank",
        "sector": "Banks & Financial Services",
        "isin": "QA0006929895",
        "active": "true",
        "source_url": "https://example.test/qfma",
    }
    assert rows[2]["active"] == "false"


def test_evaluate_rows_accepts_exact_active_qfma_code_with_valid_isin() -> None:
    qfma_rows = parse_qfma_main_market_rows(qfma_payload(), "https://example.test/qfma")
    results = evaluate_rows(
        [
            {
                "ticker": "QNBK",
                "exchange": "QSE",
                "asset_type": "Stock",
                "name": "QNB",
                "isin": "",
            },
            {
                "ticker": "BAD",
                "exchange": "QSE",
                "asset_type": "Stock",
                "name": "Bad Isin Co",
                "isin": "",
            },
            {
                "ticker": "OLD",
                "exchange": "QSE",
                "asset_type": "Stock",
                "name": "Old Co",
                "isin": "",
            },
        ],
        qfma_rows,
    )

    assert results[0]["decision"] == "accept"
    assert results[0]["qfma_isin"] == "QA0006929895"
    assert "ticker_exact_match=true" in results[0]["identity_gate_context"]
    assert "valid_isin_checksum=true" in results[0]["identity_gate_context"]
    assert results[1]["decision"] == "invalid_qfma_isin"
    assert results[2]["decision"] == "no_qfma_company_code_match"


def test_build_metadata_updates_uses_official_qfma_reason() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "QNBK",
                "exchange": "QSE",
                "decision": "accept",
                "qfma_isin": "QA0006929895",
                "source_url": "https://example.test/qfma",
            },
            {
                "ticker": "BAD",
                "exchange": "QSE",
                "decision": "invalid_qfma_isin",
                "qfma_isin": "QA0000000000",
                "source_url": "https://example.test/qfma",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "QNBK",
            "exchange": "QSE",
            "field": "isin",
            "decision": "update",
            "proposed_value": "QA0006929895",
            "confidence": "0.92",
            "reason": (
                "Official QFMA security source supplied a valid ISIN for the exact QSE company code; "
                "accepted only after exact ticker/company-code match and repo ISIN checksum gate. "
                "Source: https://example.test/qfma"
            ),
        }
    ]
