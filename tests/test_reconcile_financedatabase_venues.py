from __future__ import annotations

import bz2
import csv
from pathlib import Path

from scripts.reconcile_financedatabase_venues import reconcile, write_reports


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_reconcile_is_read_only_and_gates_venue_candidates(tmp_path: Path) -> None:
    snapshot = tmp_path / "finance-database"
    (snapshot / "compression").mkdir(parents=True)
    fd_fields = ["symbol", "name", "summary", "sector", "exchange", "isin", "delisted"]
    fd_rows = [
        {
            "symbol": "AAA.AX",
            "name": "Alpha Holdings Limited",
            "summary": "Alpha operating company",
            "sector": "Industrials",
            "exchange": "ASX",
            "isin": "AU000000AAA1",
            "delisted": "False",
        },
        {
            "symbol": "BBB.N",
            "name": "Beta Technologies Inc",
            "summary": "Beta operating company",
            "sector": "Technology",
            "exchange": "NYQ",
            "isin": "US000000BBB2",
            "delisted": "False",
        },
        {
            "symbol": "CCC.N",
            "name": "Gamma Industries Inc",
            "summary": "Gamma operating company",
            "sector": "Industrials",
            "exchange": "NYQ",
            "isin": "US000000CCC3",
            "delisted": "False",
        },
    ]
    with bz2.open(snapshot / "compression" / "equities.bz2", "wt", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fd_fields)
        writer.writeheader()
        writer.writerows(fd_rows)

    reference = tmp_path / "reference.csv"
    reference_fields = [
        "source_key",
        "ticker",
        "name",
        "exchange",
        "asset_type",
        "listing_status",
        "official",
        "isin",
        "cfi",
        "sector",
    ]
    write_csv(
        reference,
        reference_fields,
        [
            {
                "source_key": "asx",
                "ticker": "AAA",
                "name": "ALPHA HOLDINGS LIMITED",
                "exchange": "ASX",
                "asset_type": "Stock",
                "listing_status": "active",
                "official": "true",
                "isin": "AU000000AAA1",
                "cfi": "",
                "sector": "Industrials",
            },
            {
                "source_key": "nyse",
                "ticker": "BBB",
                "name": "BETA TECHNOLOGIES INC",
                "exchange": "NYSE",
                "asset_type": "ETF",
                "listing_status": "active",
                "official": "true",
                "isin": "US000000BBB2",
                "cfi": "",
                "sector": "Technology",
            },
            {
                "source_key": "nyse",
                "ticker": "BBB",
                "name": "BETA TECHNOLOGIES INC",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "official": "true",
                "isin": "US000000BBB2",
                "cfi": "",
                "sector": "Technology",
            },
            {
                "source_key": "nyse",
                "ticker": "CCC",
                "name": "GAMMA INDUSTRIES INC",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "official": "true",
                "isin": "US000000CCC3",
                "cfi": "",
                "sector": "Industrials",
            },
        ],
    )
    listings = tmp_path / "listings.csv"
    write_csv(
        listings,
        ["ticker", "name", "exchange", "asset_type", "isin"],
        [
            {
                "ticker": "CCC",
                "name": "Gamma Industries Inc",
                "exchange": "NYSE",
                "asset_type": "ETF",
                "isin": "US000000CCC3",
            }
        ],
    )
    supplemental = tmp_path / "supplemental.csv"
    write_csv(supplemental, ["ticker", "name", "exchange", "asset_type", "isin"], [])

    result = reconcile(snapshot, reference, listings, supplemental)
    assert result["metadata"]["apply_performed"] is False
    assert len(result["candidates"]) == 3
    assert len(result["review_rows"]) == 3
    decisions = {row["official_ticker"]: row["dry_run_decision"] for row in result["review_rows"]}
    assert decisions == {
        "AAA": "review_security_type_required",
        "BBB": "manual_reference_asset_type_conflict",
        "CCC": "existing_local_other_asset_type",
    }
    assert {row["mapped_exchange"] for row in result["venue_summary"]} == {"ASX", "NYSE"}

    output_dir = tmp_path / "reports"
    paths = write_reports(result, output_dir)
    assert paths["summary_json"].exists()
    assert paths["summary_md"].exists()
    assert "Apply performed: `False`" in paths["summary_md"].read_text(encoding="utf-8")
