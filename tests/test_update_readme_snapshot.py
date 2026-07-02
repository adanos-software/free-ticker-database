from __future__ import annotations

from collections import Counter

from scripts.update_readme_snapshot import update_snapshot_table, update_top_exchange_table


def test_update_snapshot_table_replaces_first_metric_number_only():
    readme = "\n".join(
        [
            "# Test",
            "",
            "## Snapshot",
            "",
            "| Metric | Value | Meaning |",
            "|---|---:|---|",
            "| Core listings | 1,000 | Rows in `data/core_listings.csv`; one row. |",
            "| ISIN coverage | 900 (90.0%) | Primary ticker rows with ISIN. |",
            "",
            "## Next",
            "",
        ]
    )

    updated = update_snapshot_table(
        readme,
        {"Core listings": 1001, "ISIN coverage": 901, "Primary tickers": 1001},
    )

    assert "| Core listings | 1,001 | Rows in `data/core_listings.csv`; one row. |" in updated
    assert "| ISIN coverage | 901 (90.0%) | Primary ticker rows with ISIN. |" in updated


def test_update_top_exchange_table_replaces_existing_counts():
    readme = "\n".join(
        [
            "## Coverage",
            "",
            "Top exchanges by primary ticker count:",
            "",
            "| Exchange | Tickers |",
            "|---|---:|",
            "| NASDAQ | 4,538 |",
            "| NYSE | 1,949 |",
            "",
            "For full exchange coverage, use reports.",
            "",
        ]
    )

    updated = update_top_exchange_table(readme, Counter({"NASDAQ": 4553, "NYSE": 1960}))

    assert "| NASDAQ | 4,553 |" in updated
    assert "| NYSE | 1,960 |" in updated
