from __future__ import annotations

from scripts.backfill_sginvestors_sgx_sectors import (
    build_metadata_updates,
    evaluate_rows,
    parse_sginvestors_sector_page,
)


def test_parse_sginvestors_sector_page_extracts_sector_rows():
    rows = parse_sginvestors_sector_page(
        """
        <h3>SGX Listed Information Technology Sector Companies</h3>
        <li>SERIAL SYSTEM LTD ( SGX: S69 ) 1</li>
        <h3>SGX Listed Consumer Staples Sector Companies</h3>
        <li>WILMAR INTERNATIONAL LIMITED ( SGX: F34 ) 1</li>
        """
    )

    assert [(row.ticker, row.name, row.sector) for row in rows] == [
        ("S69", "SERIAL SYSTEM LTD", "Information Technology"),
        ("F34", "WILMAR INTERNATIONAL LIMITED", "Consumer Staples"),
    ]


def test_evaluate_rows_accepts_exact_ticker_and_name_match():
    sector_rows = parse_sginvestors_sector_page(
        "<h3>SGX Listed Information Technology Sector Companies</h3>"
        "<li>SERIAL SYSTEM LTD ( SGX: S69 ) 1</li>"
    )

    results = evaluate_rows(
        [
            {
                "ticker": "S69",
                "exchange": "SGX",
                "asset_type": "Stock",
                "name": "Serial System Ltd",
                "stock_sector": "",
            },
            {
                "ticker": "BAD",
                "exchange": "SGX",
                "asset_type": "Stock",
                "name": "Other Co",
                "stock_sector": "",
            },
        ],
        sector_rows,
    )

    assert results[0]["decision"] == "accept"
    assert results[0]["sector_update"] == "Information Technology"
    assert results[1]["decision"] == "no_sginvestors_match"


def test_build_metadata_updates_emits_review_gated_sector_update():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "S69",
                "exchange": "SGX",
                "sector_update": "Information Technology",
            },
            {"decision": "name_mismatch", "ticker": "BAD", "exchange": "SGX", "sector_update": ""},
        ]
    )

    assert updates[0]["ticker"] == "S69"
    assert updates[0]["field"] == "stock_sector"
    assert updates[0]["proposed_value"] == "Information Technology"
