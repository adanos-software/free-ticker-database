import csv

from scripts.build_twelvedata_deepseek_batches import split_rows, summarize


def test_split_rows_groups_known_review_batches() -> None:
    rows = [
        {"ticker": "AAA", "review_batch": "batch_a_us_core", "priority": "P1", "exchange": "NASDAQ"},
        {"ticker": "BBB", "review_batch": "batch_b_canada", "priority": "P1", "exchange": "TSX"},
        {"ticker": "CCC", "review_batch": "batch_c_high_value_international", "priority": "P2", "exchange": "LSE"},
        {"ticker": "DDD", "review_batch": "later_global_review", "priority": "P3", "exchange": "XSTU"},
    ]

    grouped = split_rows(rows)

    assert [row["ticker"] for row in grouped["batch_a_us_core"]] == ["AAA"]
    assert [row["ticker"] for row in grouped["batch_b_canada"]] == ["BBB"]
    assert [row["ticker"] for row in grouped["batch_c_high_value_international"]] == ["CCC"]
    assert [row["ticker"] for row in grouped["later_global_review"]] == ["DDD"]


def test_summarize_reports_batch_counts(tmp_path) -> None:
    grouped = split_rows(
        [
            {"ticker": "AAA", "review_batch": "batch_a_us_core", "priority": "P1", "exchange": "NASDAQ"},
            {"ticker": "BBB", "review_batch": "batch_a_us_core", "priority": "P2", "exchange": "OTC"},
        ]
    )

    summary = summarize(grouped, tmp_path)

    assert summary["total_rows"] == 2
    batch_a = [batch for batch in summary["batches"] if batch["review_batch"] == "batch_a_us_core"][0]
    assert batch_a["rows"] == 2
    assert batch_a["priority_counts"] == [("P1", 1), ("P2", 1)]
