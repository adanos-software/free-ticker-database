from __future__ import annotations

import csv
from pathlib import Path

from scripts.build_masterfile_diff_report import build_diff


FIELDS = [
    "source_key",
    "provider",
    "source_url",
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "listing_status",
    "reference_scope",
    "official",
    "isin",
    "sector",
]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def row(ticker: str, name: str, isin: str = "US1", source_key: str = "nasdaq_listed") -> dict[str, str]:
    return {
        "source_key": source_key,
        "provider": "Official",
        "source_url": "https://example.com",
        "ticker": ticker,
        "name": name,
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": isin,
        "sector": "",
    }


def test_build_masterfile_diff_report_classifies_batch_changes(tmp_path: Path) -> None:
    previous = tmp_path / "previous.csv"
    current = tmp_path / "current.csv"
    write_rows(previous, [row("OLD", "Old Inc"), row("KEEP", "Keep Inc"), row("NAME", "Before Inc", "US2")])
    write_rows(current, [row("KEEP", "Keep Inc"), row("NAME", "After Inc", "US3"), row("NEW", "New Inc")])

    report = build_diff(
        previous_reference_csv=previous,
        current_reference_csv=current,
        report_json=tmp_path / "diff.json",
        report_md=tmp_path / "diff.md",
    )

    assert report["summary"]["new_rows"] == 1
    assert report["summary"]["vanished_rows"] == 1
    assert report["summary"]["changed_rows"] == 1
    assert report["changed"][0]["change_types"] == ["name_change", "isin_change"]
    assert report["summary"]["vanished_policy"] == "feed_delisting_classifier_not_direct_deletion"
