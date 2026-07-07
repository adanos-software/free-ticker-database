from __future__ import annotations

import csv

from scripts.backfill_financedatabase_metadata import (
    FinanceDatabaseRow,
    build_metadata_updates,
    evaluate_financedatabase_row,
    expected_isin_prefix_match,
    finance_base_ticker,
    load_previously_cleared_isin_candidates,
    normalized_ticker_key,
    write_report_csv,
)


def test_ticker_normalization_handles_suffix_and_class_separator():
    assert finance_base_ticker("7203.T") == "7203"
    assert finance_base_ticker("BRK-B") == "BRK-B"
    assert normalized_ticker_key("BRK-B") == "BRK.B"


def test_expected_isin_prefix_match_gates_known_venues():
    assert expected_isin_prefix_match("TSE", "JP3309000002")
    assert not expected_isin_prefix_match("TSE", "US3309000005")
    assert expected_isin_prefix_match("LSE", "IE00B4L5Y983")


def test_evaluate_financedatabase_row_accepts_sector_and_isin_updates():
    result = evaluate_financedatabase_row(
        {
            "ticker": "1893",
            "exchange": "TSE",
            "asset_type": "Stock",
            "name": "PENTA-OCEAN CONSTRUCTION CO.,LTD.",
            "sector": "",
            "isin": "",
        },
        [
            FinanceDatabaseRow(
                symbol="1893.T",
                base_ticker="1893",
                name="Penta-Ocean Construction Co., Ltd.",
                exchange="JPX",
                asset_type="Stock",
                sector="Industrials",
                isin="JP3309000002",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Industrials"
    assert result["isin_update"] == "JP3309000002"


def test_evaluate_financedatabase_row_can_disable_isin_updates():
    result = evaluate_financedatabase_row(
        {
            "ticker": "1893",
            "exchange": "TSE",
            "asset_type": "Stock",
            "name": "PENTA-OCEAN CONSTRUCTION CO.,LTD.",
            "sector": "",
            "isin": "",
        },
        [
            FinanceDatabaseRow(
                symbol="1893.T",
                base_ticker="1893",
                name="Penta-Ocean Construction Co., Ltd.",
                exchange="JPX",
                asset_type="Stock",
                sector="Industrials",
                isin="JP3309000002",
            )
        ],
        include_isin=False,
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Industrials"
    assert result["isin_update"] == ""


def test_evaluate_financedatabase_row_rejects_name_and_country_mismatch():
    row = {
        "ticker": "1893",
        "exchange": "TSE",
        "asset_type": "Stock",
        "name": "PENTA-OCEAN CONSTRUCTION CO.,LTD.",
        "sector": "",
        "isin": "",
    }

    assert evaluate_financedatabase_row(
        row,
        [
            FinanceDatabaseRow(
                symbol="1893.T",
                base_ticker="1893",
                name="Different Company Inc.",
                exchange="JPX",
                asset_type="Stock",
                sector="Industrials",
                isin="JP3309000002",
            )
        ],
    )["decision"] == "name_mismatch"
    assert evaluate_financedatabase_row(
        row,
        [
            FinanceDatabaseRow(
                symbol="1893.T",
                base_ticker="1893",
                name="Penta-Ocean Construction Co., Ltd.",
                exchange="JPX",
                asset_type="Stock",
                sector="Industrials",
                isin="US0378331005",
            )
        ],
    )["decision"] == "isin_country_mismatch"


def test_evaluate_financedatabase_row_rejects_existing_isin_peer_name_mismatch():
    result = evaluate_financedatabase_row(
        {
            "ticker": "2531",
            "exchange": "TSE",
            "asset_type": "Stock",
            "name": "TAKARA HOLDINGS INC.",
            "sector": "",
            "isin": "",
        },
        [
            FinanceDatabaseRow(
                symbol="2531.T",
                base_ticker="2531",
                name="Takara Holdings Inc.",
                exchange="JPX",
                asset_type="Stock",
                sector="Consumer Defensive",
                isin="JP3459600007",
            )
        ],
        existing_isin_rows_by_isin={
            "JP3459600007": [
                {"ticker": "TAX", "exchange": "NASDAQ", "name": "Cambria Tax Aware ETF"}
            ]
        },
    )

    assert result["decision"] == "isin_peer_name_mismatch"


def test_load_previously_cleared_isin_candidates_reads_isins_from_clear_reasons(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "RVRC",
                "exchange": "OTC",
                "field": "isin",
                "decision": "clear",
                "proposed_value": "",
                "confidence": "high",
                "reason": "wrong-ISIN collision; SE0015962485 belongs to RVRC Holding AB.",
            }
        )
        writer.writerow(
            {
                "ticker": "KEEP",
                "exchange": "OTC",
                "field": "isin",
                "decision": "update",
                "proposed_value": "SE0015962485",
                "confidence": "0.74",
                "reason": "not a clear row",
            }
        )

    assert load_previously_cleared_isin_candidates(path) == {("RVRC", "OTC"): {"SE0015962485"}}


def test_evaluate_financedatabase_row_rejects_previously_cleared_isin_candidate():
    result = evaluate_financedatabase_row(
        {
            "ticker": "RVRC",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Revium Recovery Inc",
            "sector": "Health Care",
            "isin": "",
        },
        [
            FinanceDatabaseRow(
                symbol="RVRC",
                base_ticker="RVRC",
                name="Revium Recovery Inc.",
                exchange="PNK",
                asset_type="Stock",
                sector="Financials",
                isin="SE0015962485",
            )
        ],
        include_sector=False,
        previously_cleared_isin_candidates={("RVRC", "OTC"): {"SE0015962485"}},
    )

    assert result["decision"] == "isin_candidate_previously_cleared"


def test_build_metadata_updates_emits_one_row_per_field():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "1893",
                "exchange": "TSE",
                "asset_type": "Stock",
                "sector_update": "Industrials",
                "isin_update": "JP3309000002",
            },
            {
                "decision": "name_mismatch",
                "ticker": "BAD",
                "exchange": "TSE",
                "sector_update": "",
                "isin_update": "",
            },
        ]
    )

    assert [update["field"] for update in updates] == ["stock_sector", "isin"]
    assert updates[0]["proposed_value"] == "Industrials"
    assert updates[1]["proposed_value"] == "JP3309000002"


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "metadata_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "1893",
                "exchange": "TSE",
                "asset_type": "Stock",
                "name": "PENTA-OCEAN CONSTRUCTION CO.,LTD.",
                "fd_symbol": "1893.T",
                "fd_exchange": "JPX",
                "fd_name": "Penta-Ocean Construction Co., Ltd.",
                "fd_sector": "Industrials",
                "fd_isin": "JP3309000002",
                "sector_update": "Industrials",
                "isin_update": "",
                "decision": "accept",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")
