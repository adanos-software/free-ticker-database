import csv

from scripts.build_twelvedata_review_queues import (
    build_gap_candidates,
    build_rename_candidates,
    build_stale_candidates,
)


def write_rows(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_gap_candidates_filters_to_allowed_stock_types_and_prioritizes_us(tmp_path) -> None:
    path = tmp_path / "missing.csv"
    write_rows(
        path,
        [
            "symbol",
            "name",
            "exchange",
            "mic_code",
            "country",
            "currency",
            "type",
            "figi_code",
            "stock_like",
            "same_symbol_local_exchanges",
        ],
        [
            {
                "symbol": "AAA",
                "name": "AAA Corp",
                "exchange": "NASDAQ",
                "mic_code": "XNMS",
                "country": "United States",
                "currency": "USD",
                "type": "Common Stock",
                "figi_code": "BBGTEST",
                "stock_like": "True",
                "same_symbol_local_exchanges": "",
            },
            {
                "symbol": "BBB",
                "name": "BBB Warrant",
                "exchange": "NASDAQ",
                "mic_code": "XNMS",
                "country": "United States",
                "currency": "USD",
                "type": "Warrant",
                "figi_code": "",
                "stock_like": "False",
                "same_symbol_local_exchanges": "",
            },
            {
                "symbol": "CCC",
                "name": "CCC AG",
                "exchange": "XSTU",
                "mic_code": "XSTU",
                "country": "Germany",
                "currency": "EUR",
                "type": "Common Stock",
                "figi_code": "",
                "stock_like": "True",
                "same_symbol_local_exchanges": "XETRA",
            },
        ],
    )

    rows = build_gap_candidates(path)

    assert [row["ticker"] for row in rows] == ["AAA", "CCC"]
    assert rows[0]["priority"] == "P1"
    assert rows[0]["review_batch"] == "batch_a_us_core"
    assert rows[0]["candidate_action"] == "validate_new_listing_with_second_source"
    assert rows[1]["priority"] == "P4"
    assert "likely_secondary_german_venue" in rows[1]["reason_code"]
    assert "same_symbol_exists_elsewhere" in rows[1]["reason_code"]


def test_build_rename_candidates_prioritizes_low_score_us_mismatches(tmp_path) -> None:
    path = tmp_path / "names.csv"
    write_rows(
        path,
        [
            "listing_key",
            "ticker",
            "local_exchange",
            "twelvedata_exchange",
            "twelvedata_mic",
            "local_name",
            "twelvedata_name",
            "name_ratio",
            "twelvedata_type",
        ],
        [
            {
                "listing_key": "NASDAQ::AAA",
                "ticker": "AAA",
                "local_exchange": "NASDAQ",
                "twelvedata_exchange": "NASDAQ",
                "twelvedata_mic": "XNMS",
                "local_name": "Old Company",
                "twelvedata_name": "New Company",
                "name_ratio": "0.2",
                "twelvedata_type": "Common Stock",
            },
            {
                "listing_key": "NASDAQ::BBB",
                "ticker": "BBB",
                "local_exchange": "NASDAQ",
                "twelvedata_exchange": "NASDAQ",
                "twelvedata_mic": "XNMS",
                "local_name": "BBB Corp",
                "twelvedata_name": "BBB Warrant",
                "name_ratio": "0.1",
                "twelvedata_type": "Warrant",
            },
        ],
    )

    rows = build_rename_candidates(path)

    assert len(rows) == 1
    assert rows[0]["priority"] == "P0"
    assert rows[0]["candidate_action"] == "deepseek_classify_rename_vs_different_security"
    assert "severe_name_mismatch" in rows[0]["reason_code"]


def test_build_stale_candidates_marks_local_batch_without_deleting(tmp_path) -> None:
    path = tmp_path / "local_unmatched.csv"
    write_rows(
        path,
        ["listing_key", "ticker", "exchange", "name", "asset_type", "country", "isin"],
        [
            {
                "listing_key": "NYSE::AAA",
                "ticker": "AAA",
                "exchange": "NYSE",
                "name": "AAA Corp",
                "asset_type": "Stock",
                "country": "United States",
                "isin": "US0000000001",
            }
        ],
    )

    rows = build_stale_candidates(path)

    assert rows == [
        {
            "reason_code": "local_listing_not_seen_in_twelvedata|batch_a_us_core",
            "priority": "P1",
            "ticker": "AAA",
            "exchange": "NYSE",
            "mic_code": "",
            "local_name": "AAA Corp",
            "twelvedata_name": "",
            "twelvedata_type": "",
            "name_score": "",
            "candidate_action": "validate_stale_or_mapping_gap",
            "validation_status": "pending_second_source",
            "listing_key": "NYSE::AAA",
            "asset_type": "Stock",
            "country": "United States",
            "isin": "US0000000001",
            "review_batch": "batch_a_us_core",
        }
    ]
