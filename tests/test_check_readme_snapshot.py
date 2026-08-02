from __future__ import annotations

import json
from pathlib import Path

from scripts.check_readme_snapshot import check_readme_snapshot, expected_snapshot_values


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_expected_snapshot_uses_all_coverage_venues_for_source_status_counts():
    coverage = {
        "global": {
            "core_listings": 1,
            "tickers": 1,
            "listing_keys": 1,
            "stocks": 1,
            "etfs": 0,
            "aliases": 1,
            "isin_coverage": 1,
            "figi_coverage": 1,
            "sector_coverage": 1,
            "stock_sector_coverage": 1,
            "etf_category_coverage": 0,
            "instrument_scope_core": 1,
            "instrument_scope_primary_listing": 1,
            "instrument_scope_primary_listing_missing_isin": 0,
            "instrument_scope_extended": 0,
        },
        "by_exchange": [
            {"exchange": "FULL_A", "venue_status": "official_full"},
            {"exchange": "FULL_B", "venue_status": "official_full"},
            {"exchange": "PARTIAL", "venue_status": "official_partial"},
        ],
    }
    candidate_inventory = {
        "summary": {
            "current_status_counts": {
                "official_full": 1,
                "official_partial": 1,
            }
        }
    }

    expected = expected_snapshot_values(
        coverage,
        candidate_inventory,
        {"summary": {"status_counts": {}}},
    )

    assert expected["Official full exchanges"] == 2
    assert expected["Official partial exchanges"] == 1
    assert expected["Missing current-scope exchanges"] == 0


def test_check_readme_snapshot_rejects_stale_sources_missing_count(tmp_path: Path):
    readme = tmp_path / "README.md"
    coverage = tmp_path / "coverage_report.json"
    source_inventory = tmp_path / "source_inventory_gap.json"
    entry_quality = tmp_path / "entry_quality.json"

    readme.write_text(
        "\n".join(
            [
                "# Test",
                "",
                "## Snapshot",
                "",
                "| Metric | Value | Meaning |",
                "|---|---:|---|",
                "| Core listings | 10 | Rows in `data/core_listings.csv`. |",
                "| Primary tickers | 10 | Rows in `data/tickers.csv`. |",
                "| Full listing rows | 10 | Rows in `data/listings.csv`. |",
                "| Stocks | 8 | Stock rows. |",
                "| ETFs | 2 | ETF rows. |",
                "| Aliases | 3 | Alias rows. |",
                "| ISIN coverage | 9 (90.0%) | Rows with ISIN. |",
                "| FIGI coverage | 7 | FIGI rows. |",
                "| Sector/category coverage | 9 (90.0%) | Rows with sector/category. |",
                "| Stock sector coverage | 8 | Stock-sector rows. |",
                "| ETF category coverage | 1 | ETF-category rows. |",
                "| Core listing-scope rows | 10 | Core rows. |",
                "| Core primary rows with ISIN | 9 | Core ISIN rows. |",
                "| Core primary rows missing ISIN | 1 | Core missing ISIN rows. |",
                "| Extended listing-scope rows | 0 | Extended rows. |",
                "| Official full exchanges | 30 | Official full. |",
                "| Official partial exchanges | 33 | Official partial. |",
                "| Missing current-scope exchanges | 2 | Missing. |",
                "| Entry quality source-gap rows | 4 | Source gaps. |",
                "| Entry quality warn rows | 1 | Warnings. |",
                "",
                "Snapshot values use `source_inventory_gap.md` is authoritative for current-scope source gaps.",
                "",
                "## Data Model",
                "",
                "- `tickers.csv` is a compatibility export.",
                "- `listings.csv` and `listing_key` are the venue-level source of truth.",
                "",
                "## Sources",
                "",
                "Official source candidates and reconciled source gaps are tracked in [`data/masterfiles/source_candidates.json`](data/masterfiles/source_candidates.json) and summarized by [`data/reports/source_inventory_gap.md`](data/reports/source_inventory_gap.md). Current source inventory status: `0` missing current-scope sources, `1` parser todo row, `0` real global-expansion candidates, `30` official-full rows, and `33` official-partial rows. Remaining work is now field-completion and taxonomy coverage, not undiscovered exchange-source inventory.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    write_json(
        coverage,
        {
            "global": {
                "core_listings": 10,
                "tickers": 10,
                "listing_keys": 10,
                "stocks": 8,
                "etfs": 2,
                "aliases": 3,
                "isin_coverage": 9,
                "figi_coverage": 7,
                "sector_coverage": 9,
                "stock_sector_coverage": 8,
                "etf_category_coverage": 1,
                "instrument_scope_core": 10,
                "instrument_scope_primary_listing": 9,
                "instrument_scope_primary_listing_missing_isin": 1,
                "instrument_scope_extended": 0,
            },
            "by_exchange": [
                {"exchange": "MISSING_A", "venue_status": "missing"},
                {"exchange": "MISSING_B", "venue_status": "missing"},
            ],
        },
    )
    write_json(
        source_inventory,
        {
            "summary": {
                "current_status_counts": {
                    "missing": 2,
                    "official_full": 30,
                    "official_partial": 33,
                },
                "global_expansion_candidates": 0,
                "todo_rows": 1,
            }
        },
    )
    write_json(entry_quality, {"summary": {"status_counts": {"source_gap": 4, "warn": 1}}})

    errors = check_readme_snapshot(
        readme_path=readme,
        coverage_report_json=coverage,
        source_inventory_json=source_inventory,
        entry_quality_json=entry_quality,
    )

    assert any("README Sources status 'missing' is stale" in error for error in errors)
