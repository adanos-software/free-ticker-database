from __future__ import annotations

from collections import Counter

from scripts.update_readme_snapshot import update_snapshot_table, update_sources_status_paragraph, update_top_exchange_table


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


def test_update_sources_status_paragraph_uses_source_inventory_summary():
    readme = "\n".join(
        [
            "# Test",
            "",
            "## Sources",
            "",
            "Implemented sources.",
            "",
            "Official source candidates and reconciled source gaps are tracked in [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) and summarized by [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md). Current source inventory status: `0` missing current-scope sources, `0` parser todo rows, `0` real global-expansion candidates, `30` official-full rows, and `34` official-partial rows. Remaining work is now field-completion and taxonomy coverage, not undiscovered exchange-source inventory.",
            "",
            "Secondary sources.",
            "",
        ]
    )
    source_inventory = {
        "summary": {
            "current_status_counts": {
                "missing": 2,
                "official_full": 30,
                "official_partial": 33,
            },
            "global_expansion_candidates": 0,
            "todo_rows": 1,
        }
    }

    updated = update_sources_status_paragraph(readme, source_inventory)

    assert "`2` missing current-scope sources" in updated
    assert "`1` parser todo rows" in updated
    assert "`30` official-full rows" in updated
    assert "`33` official-partial rows" in updated
    assert "not undiscovered exchange-source inventory" not in updated
