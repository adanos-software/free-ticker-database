from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import apply_new_listings


REFERENCE_FIELDS = [
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
LISTING_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
]
SUPPLEMENT_FIELDS = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
    "source_key",
    "source_url",
    "reference_scope",
]


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def reference_row(
    ticker: str,
    name: str,
    exchange: str = "NYSE",
    asset_type: str = "Stock",
    status: str = "active",
    source_key: str = "nasdaq_other_listed",
) -> dict[str, str]:
    return {
        "source_key": source_key,
        "provider": "Nasdaq Trader",
        "source_url": "https://example.com/source.txt",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": status,
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "",
        "sector": "",
    }


def listing_row(ticker: str, exchange: str = "NASDAQ") -> dict[str, str]:
    return {
        "listing_key": f"{exchange}::{ticker}",
        "ticker": ticker,
        "exchange": exchange,
        "name": "Existing Inc.",
        "asset_type": "Stock",
        "stock_sector": "",
        "etf_category": "",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "aliases": "",
    }


def test_apply_new_listings_accepts_new_common_stock(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    report_json = tmp_path / "report.json"
    report_md = tmp_path / "report.md"

    write_rows(previous_reference, REFERENCE_FIELDS, [reference_row("OLD", "Old Co Common Stock")])
    write_rows(
        current_reference,
        REFERENCE_FIELDS,
        [
            reference_row("OLD", "Old Co Common Stock"),
            reference_row("MBGL", "Mobility Global Inc. Common Stock"),
        ],
    )
    write_rows(listings, LISTING_FIELDS, [listing_row("OLD")])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        report_json=report_json,
        report_md=report_md,
    )

    assert report["summary"]["accepted_rows"] == 1
    assert report["accepted"][0]["ticker"] == "MBGL"
    with supplements.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "ticker": "MBGL",
            "name": "Mobility Global Inc. Common Stock",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "United States",
            "country_code": "US",
            "isin": "",
            "aliases": "",
            "source_key": "nasdaq_other_listed",
            "source_url": "https://example.com/source.txt",
            "reference_scope": "exchange_directory",
        }
    ]
    assert json.loads(report_json.read_text(encoding="utf-8"))["summary"]["accepted_rows"] == 1
    assert "MBGL" in report_md.read_text(encoding="utf-8")


def test_apply_new_listings_skips_backlog_and_non_common_rows(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"

    write_rows(
        previous_reference,
        REFERENCE_FIELDS,
        [
            reference_row("BACKLOG", "Backlog Co Common Stock"),
            reference_row("UNIT", "Unit Acquisition Corp Unit"),
        ],
    )
    write_rows(
        current_reference,
        REFERENCE_FIELDS,
        [
            reference_row("BACKLOG", "Backlog Co Common Stock"),
            reference_row("UNIT", "Unit Acquisition Corp Unit"),
            reference_row("NEWU", "New Acquisition Corp Unit"),
            reference_row("MIDDV", "Middleby Corp. - Common Stock Ex-Distribution When Issued"),
            reference_row("DSC", "DSC Holdings Ltd. - American Depositary Shares"),
            reference_row("NEW", "New Corp Common Stock"),
        ],
    )
    write_rows(listings, LISTING_FIELDS, [listing_row("BACKLOG")])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert [row["ticker"] for row in report["accepted"]] == ["NEW"]
    assert report["summary"]["skipped_by_reason"] == {
        "excluded_non_common_stock": 1,
        "not_stock_like_name": 1,
        "temporary_when_issued_line": 1,
    }


def test_apply_new_listings_skips_ticker_collisions(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ABC", "ABC Inc. Common Stock")])
    write_rows(listings, LISTING_FIELDS, [listing_row("ABC", exchange="ASX")])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["accepted"] == []
    assert report["summary"]["skipped_by_reason"] == {"ticker_collision": 1}


def test_apply_new_listings_accepts_new_us_etf_by_default(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ETFZ", "Example ETF", asset_type="ETF")])
    write_rows(listings, LISTING_FIELDS, [])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["supported_asset_types"] == ["ETF", "Stock"]
    assert [row["ticker"] for row in report["accepted"]] == ["ETFZ"]
