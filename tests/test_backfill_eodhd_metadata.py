from __future__ import annotations

import csv

from scripts.backfill_eodhd_metadata import (
    EodhdSymbol,
    build_metadata_updates,
    evaluate_eodhd_row,
    index_eodhd_symbols,
    load_missing_isin_rows,
    main,
    normalized_ticker_key,
    replay_eodhd_report_accepts,
    strict_names_match,
    write_report_csv,
)


def test_load_missing_isin_rows_filters_exchange_asset_and_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc.", "isin": ""},
                {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc.", "isin": "CA05277B2093"},
                {"ticker": "ACQ", "exchange": "TSXV", "asset_type": "Stock", "name": "AutoCanada Inc.", "isin": ""},
                {"ticker": "BTCY", "exchange": "TSX", "asset_type": "ETF", "name": "Purpose Bitcoin Yield ETF", "isin": ""},
            ]
        )

    rows = load_missing_isin_rows(exchanges={"TSX"}, asset_types={"Stock"}, tickers_csv=path)

    assert [row["ticker"] for row in rows] == ["ACQ"]


def test_index_eodhd_symbols_filters_internal_market_code():
    indexed = index_eodhd_symbols(
        {
            "US": [
                {"Code": "DIV", "Name": "Global X SuperDividend U.S. ETF", "Exchange": "NYSE ARCA", "Type": "ETF", "Isin": "US37950E2919"},
                {"Code": "DIV", "Name": "Diversified Holdings", "Exchange": "NYSE", "Type": "Common Stock", "Isin": "US25400X1019"},
            ]
        },
        {"NYSE ARCA"},
    )

    assert indexed[("NYSE ARCA", "DIV")] == [
        EodhdSymbol(
            code="DIV",
            name="Global X SuperDividend U.S. ETF",
            exchange="NYSE ARCA",
            asset_type="ETF",
            isin="US37950E2919",
        )
    ]


def test_evaluate_eodhd_row_accepts_valid_match():
    result = evaluate_eodhd_row(
        {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc."},
        [
            EodhdSymbol(
                code="ACQ",
                name="Autocanada Inc",
                exchange="TO",
                asset_type="Common Stock",
                isin="CA05277B2093",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["eodhd_isin"] == "CA05277B2093"


def test_evaluate_eodhd_row_rejects_existing_isin_by_default():
    result = evaluate_eodhd_row(
        {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc."},
        [
            EodhdSymbol(
                code="ACQ",
                name="Autocanada Inc",
                exchange="TO",
                asset_type="Common Stock",
                isin="CA05277B2093",
            )
        ],
        existing_isins={"CA05277B2093"},
    )

    assert result["decision"] == "isin_already_present"


def test_evaluate_eodhd_row_can_allow_existing_isin_for_reviewed_batches():
    result = evaluate_eodhd_row(
        {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc."},
        [
            EodhdSymbol(
                code="ACQ",
                name="Autocanada Inc",
                exchange="TO",
                asset_type="Common Stock",
                isin="CA05277B2093",
            )
        ],
        existing_isins={"CA05277B2093"},
        allow_existing_isin=True,
    )

    assert result["decision"] == "accept"


def test_evaluate_eodhd_row_rejects_number_token_mismatch():
    result = evaluate_eodhd_row(
        {"ticker": "BOND", "exchange": "TSX", "asset_type": "ETF", "name": "Example Bond ETF 2026"},
        [
            EodhdSymbol(
                code="BOND",
                name="Example Bond ETF 2028",
                exchange="TO",
                asset_type="ETF",
                isin="CA05277B2093",
            )
        ],
    )

    assert result["decision"] == "number_token_mismatch"


def test_evaluate_eodhd_row_rejects_asset_type_and_country_mismatch():
    row = {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc."}

    assert evaluate_eodhd_row(
        row,
        [
            EodhdSymbol(
                code="ACQ",
                name="Autocanada Inc",
                exchange="TO",
                asset_type="ETF",
                isin="CA05277B2093",
            )
        ],
    )["decision"] == "asset_type_mismatch"
    assert evaluate_eodhd_row(
        row,
        [
            EodhdSymbol(
                code="ACQ",
                name="Autocanada Inc",
                exchange="TO",
                asset_type="Common Stock",
                isin="US0378331005",
            )
        ],
    )["decision"] == "isin_country_mismatch"


def test_build_metadata_updates_emits_isin_override():
    updates = build_metadata_updates(
        [
            {"decision": "accept", "ticker": "ACQ", "exchange": "TSX", "eodhd_isin": "CA05277B2093"},
            {"decision": "name_mismatch", "ticker": "BAD", "exchange": "TSX", "eodhd_isin": "CA0000000000"},
        ]
    )

    assert updates == [
        {
            "ticker": "ACQ",
            "exchange": "TSX",
            "field": "isin",
            "decision": "update",
            "proposed_value": "CA05277B2093",
            "confidence": "0.82",
            "reason": "EODHD exchange-symbol-list returned a valid ISIN for a row without ISIN, accepted only after ticker, EODHD exchange/subvenue, asset type, expected ISIN prefix, strict issuer/product-name, numeric-token, and checksum gates matched.",
        }
    ]


def test_replay_eodhd_report_accepts_revalidates_current_missing_row():
    results = replay_eodhd_report_accepts(
        report_rows=[
            {
                "ticker": "ACQ",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "AutoCanada Inc.",
                "eodhd_code": "ACQ",
                "eodhd_exchange": "TO",
                "eodhd_name": "Autocanada Inc",
                "eodhd_type": "Common Stock",
                "eodhd_isin": "CA05277B2093",
                "number_tokens_match": "True",
                "name_match": "True",
                "decision": "accept",
            }
        ],
        ticker_rows=[
            {"ticker": "ACQ", "exchange": "TSX", "asset_type": "Stock", "name": "AutoCanada Inc.", "isin": ""}
        ],
        exchanges={"TSX"},
        asset_types={"Stock"},
        existing_isins=set(),
    )

    assert results[0]["decision"] == "accept"
    assert results[0]["eodhd_isin"] == "CA05277B2093"


def test_replay_eodhd_report_rejects_isin_now_present_elsewhere():
    results = replay_eodhd_report_accepts(
        report_rows=[
            {
                "ticker": "BEBE",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "TGE Value Creative Solutions Corp",
                "eodhd_code": "BEBE",
                "eodhd_exchange": "NYSE",
                "eodhd_name": "TGE Value Creative Solutions Corp",
                "eodhd_type": "Common Stock",
                "eodhd_isin": "US0755712082",
                "number_tokens_match": "True",
                "name_match": "True",
                "decision": "accept",
            }
        ],
        ticker_rows=[
            {
                "ticker": "BEBE",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "TGE Value Creative Solutions Corp",
                "isin": "",
            },
            {
                "ticker": "BDST",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "bebe stores, inc.",
                "isin": "US0755712082",
            },
        ],
        exchanges={"NYSE"},
        asset_types={"Stock"},
        existing_isins={"US0755712082"},
    )

    assert results[0]["decision"] == "isin_already_present"


def test_replay_cli_uses_full_existing_isin_file_for_core_scan(tmp_path):
    tickers_csv = tmp_path / "core_listings.csv"
    existing_isins_csv = tmp_path / "listings.csv"
    replay_csv = tmp_path / "eodhd_report.csv"
    output_csv = tmp_path / "replay_out.csv"
    output_json = tmp_path / "replay_out.json"

    with tickers_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "BEBE",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "TGE Value Creative Solutions Corp",
                "isin": "",
            }
        )

    with existing_isins_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "BDST",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "bebe stores, inc.",
                "isin": "US0755712082",
            }
        )

    with replay_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "exchange",
                "asset_type",
                "name",
                "eodhd_code",
                "eodhd_exchange",
                "eodhd_name",
                "eodhd_type",
                "eodhd_isin",
                "number_tokens_match",
                "name_match",
                "decision",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "BEBE",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "TGE Value Creative Solutions Corp",
                "eodhd_code": "BEBE",
                "eodhd_exchange": "NYSE",
                "eodhd_name": "TGE Value Creative Solutions Corp",
                "eodhd_type": "Common Stock",
                "eodhd_isin": "US0755712082",
                "number_tokens_match": "True",
                "name_match": "True",
                "decision": "accept",
            }
        )

    main(
        [
            "--tickers-csv",
            str(tickers_csv),
            "--existing-isins-csv",
            str(existing_isins_csv),
            "--exchange",
            "NYSE",
            "--asset-type",
            "Stock",
            "--replay-report-csv",
            str(replay_csv),
            "--csv-out",
            str(output_csv),
            "--json-out",
            str(output_json),
        ]
    )

    with output_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["decision"] == "isin_already_present"
    assert output_json.read_text(encoding="utf-8") == "[]"


def test_name_and_ticker_normalization_are_strict_enough():
    assert normalized_ticker_key("BRK-B") == "BRK.B"
    assert strict_names_match("Example Bond ETF 2026", "Example Bond ETF 2026")
    assert not strict_names_match("Example Bond ETF 2028", "Example Bond ETF 2026")


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "metadata_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "ACQ",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "AutoCanada Inc.",
                "eodhd_code": "ACQ",
                "eodhd_exchange": "TO",
                "eodhd_name": "Autocanada Inc",
                "eodhd_type": "Common Stock",
                "eodhd_isin": "CA05277B2093",
                "number_tokens_match": True,
                "name_match": True,
                "decision": "accept",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")
