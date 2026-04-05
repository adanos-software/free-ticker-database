from __future__ import annotations

import csv
from scripts.build_stock_verification_overrides import (
    build_generated_updates,
    clean_official_name,
    is_excluded_security_reference,
    is_strong_rename_candidate,
    write_csv,
)


def test_clean_official_name_strips_security_suffixes() -> None:
    assert clean_official_name("WaFd, Inc. - Common Stock") == "WaFd, Inc."
    assert clean_official_name("Aurelion Inc.  - Class A Ordinary Shares") == "Aurelion Inc."


def test_is_strong_rename_candidate_accepts_plain_common_stock() -> None:
    row = {
        "status": "name_mismatch",
        "exchange": "NASDAQ",
        "official_reference_source": "nasdaq_listed",
        "official_reference_name": "WaFd, Inc. - Common Stock",
        "name": "Washington Federal Inc",
    }
    assert is_strong_rename_candidate(row)


def test_is_strong_rename_candidate_rejects_warrants_and_units() -> None:
    warrant_row = {
        "status": "name_mismatch",
        "exchange": "NASDAQ",
        "official_reference_source": "nasdaq_listed",
        "official_reference_name": "SunPower Inc. - Warrant",
        "name": "Complete Solaria, Inc.",
    }
    unit_row = {
        "status": "name_mismatch",
        "exchange": "NASDAQ",
        "official_reference_source": "nasdaq_listed",
        "official_reference_name": "ProCap Acquisition Corp - Unit",
        "name": "PCAPU",
    }
    assert not is_strong_rename_candidate(warrant_row)
    assert not is_strong_rename_candidate(unit_row)


def test_is_excluded_security_reference_accepts_non_common_lines() -> None:
    row = {
        "status": "name_mismatch",
        "exchange": "NASDAQ",
        "official_reference_name": "CDT Equity Inc. - Warrant",
    }
    assert is_excluded_security_reference(row)


def test_build_generated_updates_creates_conservative_rows() -> None:
    findings = [
        {
            "ticker": "ZWZZT",
            "exchange": "NASDAQ",
            "status": "non_active_official",
            "reason": "Official exchange directory marks this symbol as test.",
            "official_reference_name": "NASDAQ TEST STOCK",
        },
        {
            "ticker": "PALU",
            "exchange": "NASDAQ",
            "status": "asset_type_mismatch",
            "reason": "Dataset asset_type=Stock but official reference says ETF.",
            "official_reference_name": "Direxion Daily PANW Bull 2X ETF",
            "name": "Direxion Daily PANW Bull 2X Shares",
        },
        {
            "ticker": "WAFD",
            "exchange": "NASDAQ",
            "status": "name_mismatch",
            "official_reference_source": "nasdaq_listed",
            "official_reference_name": "WaFd, Inc. - Common Stock",
            "name": "Washington Federal Inc",
        },
        {
            "ticker": "MSAIW",
            "exchange": "NASDAQ",
            "status": "name_mismatch",
            "official_reference_source": "nasdaq_listed",
            "official_reference_name": "MultiSensor AI Holdings, Inc. - Warrants",
            "name": "Infrared Cameras Holdings Inc",
        },
    ]
    metadata, drops = build_generated_updates(findings)
    assert ("ZWZZT", "NASDAQ") == (drops[0]["ticker"], drops[0]["exchange"])
    assert any(row["ticker"] == "MSAIW" and row["exchange"] == "NASDAQ" for row in drops)
    assert any(row["ticker"] == "PALU" and row["field"] == "asset_type" and row["proposed_value"] == "ETF" for row in metadata)
    assert any(row["ticker"] == "PALU" and row["field"] == "name" and row["proposed_value"] == "Direxion Daily PANW Bull 2X ETF" for row in metadata)
    assert any(row["ticker"] == "WAFD" and row["field"] == "name" and row["proposed_value"] == "WaFd, Inc." for row in metadata)


def test_write_csv_ignores_unexpected_keys(tmp_path) -> None:
    output = tmp_path / "metadata_updates.csv"
    rows = [
        {
            "ticker": "RTX",
            "exchange": "NYSE",
            "field": "name",
            "decision": "update",
            "proposed_value": "RTX Corporation",
            "confidence": "0.95",
            "reason": "Official rename",
            "unexpected": "ignored",
        }
    ]
    write_csv(
        output,
        ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        rows,
    )
    with output.open(newline="", encoding="utf-8") as handle:
        written = list(csv.DictReader(handle))
    assert written == [
        {
            "ticker": "RTX",
            "exchange": "NYSE",
            "field": "name",
            "decision": "update",
            "proposed_value": "RTX Corporation",
            "confidence": "0.95",
            "reason": "Official rename",
        }
    ]
