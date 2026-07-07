from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.apply_symbol_changes import apply_symbol_changes


CHANGE_FIELDS = [
    "change_id",
    "effective_date",
    "old_symbol",
    "new_symbol",
    "new_company_name",
    "source",
    "source_url",
    "new_symbol_url",
    "source_exchange_hint",
    "source_confidence",
    "review_needed",
    "observed_at",
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
INDEX_FIELDS = ["listing_key", "ticker", "exchange", "name", "asset_type", "country", "country_code", "isin", "wkn", "figi", "cik", "lei"]
IDENTIFIER_FIELDS = ["listing_key", "ticker", "exchange", "isin", "wkn", "figi", "cik", "lei", "figi_source", "cik_source", "lei_source"]
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


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fieldnames} for row in rows)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def change(old: str = "OLD", new: str = "NEW") -> dict[str, str]:
    return {
        "change_id": "c1",
        "effective_date": "2026-07-01",
        "old_symbol": old,
        "new_symbol": new,
        "new_company_name": "Renamed Inc",
        "source": "stockanalysis_symbol_changes",
        "source_url": "https://example.com",
        "new_symbol_url": "/stocks/new/",
        "source_exchange_hint": "US_LISTED",
        "source_confidence": "secondary_review",
        "review_needed": "true",
        "observed_at": "2026-07-02T00:00:00Z",
    }


def listing(ticker: str = "OLD", isin: str = "US0000000001") -> dict[str, str]:
    return {
        "listing_key": f"NASDAQ::{ticker}",
        "ticker": ticker,
        "exchange": "NASDAQ",
        "name": "Renamed Inc",
        "asset_type": "Stock",
        "stock_sector": "Technology",
        "etf_category": "",
        "country": "United States",
        "country_code": "US",
        "isin": isin,
        "aliases": "Renamed",
    }


def reference(ticker: str = "NEW", isin: str = "US0000000001", status: str = "active") -> dict[str, str]:
    return {
        "source_key": "test_official",
        "provider": "Official",
        "source_url": "https://exchange.example/master",
        "ticker": ticker,
        "name": "Renamed Inc",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "listing_status": status,
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": isin,
        "sector": "",
    }


def test_apply_symbol_changes_rekeys_required_csvs(tmp_path: Path) -> None:
    changes = tmp_path / "symbol_changes.csv"
    listings = tmp_path / "listings.csv"
    listing_index = tmp_path / "listing_index.csv"
    identifiers = tmp_path / "identifiers_extended.csv"
    supplemental = tmp_path / "supplemental_listings.csv"
    reference_csv = tmp_path / "reference.csv"

    write_rows(changes, CHANGE_FIELDS, [change()])
    write_rows(listings, LISTING_FIELDS, [listing()])
    write_rows(listing_index, INDEX_FIELDS, [{**listing(), "wkn": "", "figi": "", "cik": "", "lei": ""}])
    write_rows(identifiers, IDENTIFIER_FIELDS, [{"listing_key": "NASDAQ::OLD", "ticker": "OLD", "exchange": "NASDAQ", "isin": "US0000000001"}])
    write_rows(supplemental, SUPPLEMENT_FIELDS, [{**listing(), "sector": "Technology", "source_key": "test", "source_url": "https://example.com", "reference_scope": "exchange_directory"}])
    write_rows(reference_csv, REFERENCE_FIELDS, [reference()])

    report = apply_symbol_changes(
        changes_csv=changes,
        listings_csv=listings,
        listing_index_csv=listing_index,
        identifiers_extended_csv=identifiers,
        supplemental_csv=supplemental,
        reference_csv=reference_csv,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["accepted_rows"] == 1
    assert read_rows(listings)[0]["listing_key"] == "NASDAQ::NEW"
    assert read_rows(listings)[0]["aliases"] == "Renamed|OLD"
    assert read_rows(listing_index)[0]["ticker"] == "NEW"
    assert read_rows(identifiers)[0]["listing_key"] == "NASDAQ::NEW"
    assert read_rows(supplemental)[0]["ticker"] == "NEW"
    assert read_rows(supplemental)[0]["aliases"] == "Renamed|OLD"
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["accepted"][0]["isin"] == "US0000000001"


def test_apply_symbol_changes_blocks_missing_isin_and_collisions(tmp_path: Path) -> None:
    changes = tmp_path / "symbol_changes.csv"
    listings = tmp_path / "listings.csv"
    listing_index = tmp_path / "listing_index.csv"
    identifiers = tmp_path / "identifiers_extended.csv"
    supplemental = tmp_path / "supplemental_listings.csv"
    reference_csv = tmp_path / "reference.csv"

    write_rows(changes, CHANGE_FIELDS, [change(old="OLD", new="NEW")])
    write_rows(listings, LISTING_FIELDS, [listing("OLD", isin=""), listing("NEW")])
    write_rows(listing_index, INDEX_FIELDS, [])
    write_rows(identifiers, IDENTIFIER_FIELDS, [])
    write_rows(supplemental, SUPPLEMENT_FIELDS, [])
    write_rows(reference_csv, REFERENCE_FIELDS, [reference()])

    report = apply_symbol_changes(
        changes_csv=changes,
        listings_csv=listings,
        listing_index_csv=listing_index,
        identifiers_extended_csv=identifiers,
        supplemental_csv=supplemental,
        reference_csv=reference_csv,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["accepted"] == []
    assert report["summary"]["blocked_by_status"] == {"blocked_new_symbol_collision": 1}
