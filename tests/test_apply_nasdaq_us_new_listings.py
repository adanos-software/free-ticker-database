from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import apply_new_listings, resolved_listing_metadata


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
    coverage = tmp_path / "coverage.csv"
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
        coverage_expansion_csv=coverage,
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
    coverage = tmp_path / "coverage.csv"

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
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert [row["ticker"] for row in report["accepted"]] == ["NEW"]
    assert report["summary"]["skipped_by_reason"] == {
        "excluded_non_common_stock": 1,
        "not_stock_like_name": 1,
        "temporary_when_issued_line": 1,
    }


def test_apply_new_listings_routes_distinct_ticker_collisions_to_coverage(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ABC", "ABC Inc. Common Stock")])
    write_rows(listings, LISTING_FIELDS, [listing_row("ABC", exchange="ASX")])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["accepted_by_target"] == {"coverage_expansion": 1}
    assert report["skipped"] == []
    with coverage.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "listing_key": "NYSE::ABC",
            "ticker": "ABC",
            "exchange": "NYSE",
            "name": "ABC Inc. Common Stock",
            "asset_type": "Stock",
            "stock_sector": "",
            "etf_category": "",
            "country": "United States",
            "country_code": "US",
            "isin": "",
            "aliases": "",
        }
    ]


def test_apply_new_listings_accepts_new_us_etf_by_default(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ETFZ", "Example ETF", asset_type="ETF")])
    write_rows(listings, LISTING_FIELDS, [])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["supported_asset_types"] == ["ETF", "Stock"]
    assert [row["ticker"] for row in report["accepted"]] == ["ETFZ"]


def test_apply_new_listings_routes_same_security_new_venue_to_supplement(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    previous_sec = reference_row(
        "QNBC",
        "QNB CORP.",
        "OTC",
        source_key="sec_company_tickers_exchange",
    )
    current_sec = reference_row(
        "QNBC",
        "QNB CORP.",
        "NASDAQ",
        source_key="sec_company_tickers_exchange",
    )
    write_rows(previous_reference, REFERENCE_FIELDS, [previous_sec])
    write_rows(
        current_reference,
        REFERENCE_FIELDS,
        [reference_row("QNBC", "QNB Corp. - Common Stock", "NASDAQ"), current_sec],
    )
    existing = listing_row("QNBC", exchange="OTC")
    existing.update({"name": "QNB Corp", "stock_sector": "Financials", "isin": "US74726N1072"})
    write_rows(listings, LISTING_FIELDS, [existing])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["accepted_by_target"] == {"supplement": 1}
    with supplements.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["ticker"] == "QNBC"
    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["sector"] == "Financials"
    assert rows[0]["isin"] == "US74726N1072"
    assert list(csv.DictReader(coverage.open(newline="", encoding="utf-8"))) == []


def test_apply_new_listings_carries_metadata_across_exact_name_ticker_rename(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(
        current_reference,
        REFERENCE_FIELDS,
        [reference_row("SEPQ", "STF Tactical Growth & Income ETF", "NASDAQ", asset_type="ETF")],
    )
    renamed = listing_row("TUGN", exchange="NASDAQ")
    renamed.update(
        {
            "name": "STF Tactical Growth & Income ETF",
            "asset_type": "ETF",
            "stock_sector": "",
            "etf_category": "Equity",
            "isin": "US53656F1690",
        }
    )
    write_rows(listings, LISTING_FIELDS, [renamed])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    with supplements.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["ticker"] == "SEPQ"
    assert rows[0]["sector"] == "Equity"
    assert rows[0]["isin"] == ""


def test_apply_new_listings_does_not_assume_us_domicile_for_foreign_share_wording(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(
        current_reference,
        REFERENCE_FIELDS,
        [reference_row("FOREIGN", "Foreign Holdings - Class A Ordinary Shares", "NASDAQ")],
    )
    write_rows(listings, LISTING_FIELDS, [])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    with supplements.open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["country"] == ""
    assert row["country_code"] == ""


def test_apply_new_listings_does_not_let_old_ticker_identity_bypass_current_ticker_collision(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ABC", "Beta Corp Common Stock")])
    current_occupant = listing_row("ABC", exchange="ASX")
    current_occupant["name"] = "Alpha Corp"
    old_identity = listing_row("OLD", exchange="NASDAQ")
    old_identity["name"] = "Beta Corp Common Stock"
    write_rows(listings, LISTING_FIELDS, [current_occupant, old_identity])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["accepted_by_target"] == {"coverage_expansion": 1}
    with coverage.open(newline="", encoding="utf-8") as handle:
        assert next(csv.DictReader(handle))["listing_key"] == "NYSE::ABC"


def test_resolved_listing_metadata_keeps_country_pairs_coherent():
    row = reference_row("ABC", "ABC Corp Common Stock")

    assert resolved_listing_metadata(
        row,
        [{"country": "", "country_code": "CA", "asset_type": "Stock"}],
    )[1:3] == ("Canada", "CA")
    assert resolved_listing_metadata(
        row,
        [
            {"country": "Canada", "country_code": "", "asset_type": "Stock"},
            {"country": "", "country_code": "US", "asset_type": "Stock"},
        ],
    )[1:3] == ("", "")


def test_apply_new_listings_requires_security_evidence_for_same_ticker_name_match(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ABC", "ABC Corp Common Stock")])
    occupant = listing_row("ABC", exchange="OTC")
    occupant["name"] = "ABC Corp Common Stock"
    write_rows(listings, LISTING_FIELDS, [occupant])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    write_rows(coverage, LISTING_FIELDS, [])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["summary"]["accepted_by_target"] == {"coverage_expansion": 1}


def test_apply_new_listings_skips_keys_already_in_coverage(tmp_path):
    previous_reference = tmp_path / "previous.csv"
    current_reference = tmp_path / "current.csv"
    listings = tmp_path / "listings.csv"
    supplements = tmp_path / "supplements.csv"
    coverage = tmp_path / "coverage.csv"

    write_rows(previous_reference, REFERENCE_FIELDS, [])
    write_rows(current_reference, REFERENCE_FIELDS, [reference_row("ABC", "ABC Corp Common Stock")])
    write_rows(listings, LISTING_FIELDS, [])
    write_rows(supplements, SUPPLEMENT_FIELDS, [])
    existing = listing_row("ABC", exchange="NYSE")
    existing["name"] = "Existing ABC Corp"
    write_rows(coverage, LISTING_FIELDS, [existing])

    report = apply_new_listings(
        previous_reference_csv=previous_reference,
        current_reference_csv=current_reference,
        listings_csv=listings,
        supplement_csv=supplements,
        coverage_expansion_csv=coverage,
        report_json=tmp_path / "report.json",
        report_md=tmp_path / "report.md",
    )

    assert report["accepted"] == []
    assert report["summary"]["skipped_by_reason"] == {"already_in_coverage": 1}
    with coverage.open(newline="", encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 1
