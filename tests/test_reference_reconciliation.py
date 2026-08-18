from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.build_reference_reconciliation import (
    MAPPING_FIELDS,
    ReconciliationIndex,
    build as build_reconciliation,
    identity_compatible,
    listing_key,
    load_mapping_overrides,
    reconcile_references,
)


def listing(exchange: str, ticker: str, name: str, *, isin: str = "", asset_type: str = "Stock") -> dict[str, str]:
    return {"exchange": exchange, "ticker": ticker, "listing_key": listing_key(exchange, ticker), "name": name, "isin": isin, "asset_type": asset_type}


def reference(source: str, exchange: str, ticker: str, name: str, *, isin: str = "", asset_type: str = "Stock") -> dict[str, str]:
    return {"source_key": source, "exchange": exchange, "ticker": ticker, "name": name, "isin": isin, "asset_type": asset_type, "official": "true", "listing_status": "active", "reference_scope": "exchange_directory"}


def test_exact_key_requires_identity_compatibility() -> None:
    rows = reconcile_references(
        [listing("NYSE", "AAA", "Alpha Power Inc", isin="US0378331005")],
        [reference("nyse", "NYSE", "AAA", "Another Foods Inc", isin="US5949181045")],
    )
    assert rows[0]["classification"] == "exact_identity_conflict"
    assert rows[0]["coverage_credit"] == "false"



def test_exact_key_same_valid_isin_allows_official_rename_drift() -> None:
    ok, reason = identity_compatible(
        {"asset_type": "ETF", "name": "Lunate S&P Germany UCITS ETF", "isin": "IE000EK4H397"},
        {"asset_type": "ETF", "name": "Chimera S&P Germany UCITS ETF - Share Class D", "isin": "IE000EK4H397"},
    )
    assert ok
    assert "checksum-valid ISIN" in reason


def test_invalid_isin_never_establishes_identity() -> None:
    ok, reason = identity_compatible(
        {"asset_type": "Stock", "name": "Alpha Power Inc", "isin": "US0378331006"},
        {"asset_type": "Stock", "name": "Alpha Power Inc", "isin": "US0378331006"},
    )
    assert not ok
    assert "invalid" in reason

def test_cross_venue_isin_does_not_cover_missing_venue_line() -> None:
    rows = reconcile_references(
        [listing("LSE", "AAA", "Alpha Power PLC", isin="GB0002634946")],
        [reference("nyse", "NYSE", "AAA", "Alpha Power PLC", isin="GB0002634946")],
    )
    assert rows[0]["classification"] == "missing_from_database"


def test_same_venue_unique_isin_requires_reviewed_symbol_mapping_for_credit() -> None:
    rows = reconcile_references(
        [listing("LSE", "ALPH", "Alpha Power PLC", isin="GB0002634946")],
        [reference("lse", "LSE", "0ALP", "Alpha Power PLC", isin="GB0002634946")],
    )
    assert rows[0]["classification"] == "alternate_listing_line"
    assert rows[0]["coverage_credit"] == "false"


def test_symbol_normalization_is_discovery_only() -> None:
    rows = reconcile_references(
        [listing("NYSE", "BRK-B", "Berkshire Hathaway Inc", isin="US0378331005")],
        [reference("nyse", "NYSE", "BRKB", "Berkshire Hathaway Inc", isin="")],
    )
    assert rows[0]["classification"] == "normalization_candidate"
    assert rows[0]["coverage_credit"] == "false"


def test_reviewed_mapping_requires_existing_same_venue_identity(tmp_path: Path) -> None:
    path = tmp_path / "mappings.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDS)
        writer.writeheader()
        writer.writerow({
            "source_key": "nyse", "reference_key": "NYSE::BRKB", "listing_key": "NYSE::BRK-B",
            "evidence_url": "https://example.test/evidence", "reviewed_at": "2026-08-17T00:00:00Z",
            "reviewer": "reviewer", "reason": "Official venue punctuation mapping",
        })
    mappings = load_mapping_overrides(path, as_of=datetime(2026, 8, 17, tzinfo=timezone.utc))
    rows = reconcile_references(
        [listing("NYSE", "BRK-B", "Berkshire Hathaway Inc", isin="US0378331005")],
        [reference("nyse", "NYSE", "BRKB", "Berkshire Hathaway Inc", isin="US0378331005")],
        mapping_overrides=mappings,
    )
    assert rows[0]["classification"] == "reviewed_mapping"
    assert rows[0]["coverage_credit"] == "true"


def test_mapping_file_is_one_to_one(tmp_path: Path) -> None:
    path = tmp_path / "mappings.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDS)
        writer.writeheader()
        base = {"source_key": "s", "listing_key": "X::A", "evidence_url": "https://example.test", "reviewed_at": "2026-08-17T00:00:00Z", "reviewer": "r", "reason": "review"}
        writer.writerow({**base, "reference_key": "X::1"})
        writer.writerow({**base, "reference_key": "X::2"})
    with pytest.raises(ValueError, match="more than one reference"):
        load_mapping_overrides(path, as_of=datetime(2026, 8, 17, tzinfo=timezone.utc))


def test_generic_single_token_does_not_establish_identity() -> None:
    ok, _ = identity_compatible(
        {"asset_type": "Stock", "name": "Global Power Inc", "isin": ""},
        {"asset_type": "Stock", "name": "Global Foods Inc", "isin": ""},
    )
    assert not ok


def test_duplicate_current_listing_keys_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate current listing keys"):
        ReconciliationIndex([listing("X", "A", "Alpha Inc"), listing("X", "A", "Alpha Inc")])


def test_out_of_scope_reference_never_gets_exact_key_credit() -> None:
    rows = reconcile_references(
        [listing("NYSE", "ABCW", "ABC Warrants", isin="US0378331005")],
        [reference("nyse", "NYSE", "ABCW", "ABC Warrants", isin="US0378331005")],
    )
    assert rows[0]["classification"] == "out_of_scope"
    assert rows[0]["coverage_credit"] == "false"


def test_mixed_scope_reference_group_is_a_conflict() -> None:
    refs = [
        reference("nyse", "NYSE", "ABC", "ABC Holdings Inc", isin="US0378331005"),
        reference("nyse", "NYSE", "ABC", "ABC Holdings Warrants", isin="US0378331005"),
    ]
    rows = reconcile_references(
        [listing("NYSE", "ABC", "ABC Holdings Inc", isin="US0378331005")], refs
    )
    assert rows[0]["classification"] == "mixed_scope_conflict"
    assert rows[0]["coverage_credit"] == "false"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def test_future_mapping_review_timestamp_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "mappings.csv"
    _write_csv(path, MAPPING_FIELDS, [{
        "source_key": "s", "reference_key": "X::A", "listing_key": "X::A",
        "evidence_url": "https://example.test/evidence",
        "reviewed_at": "2026-08-18T00:00:00Z", "reviewer": "r", "reason": "review",
    }])
    with pytest.raises(ValueError, match="not in the future"):
        load_mapping_overrides(path, as_of=datetime(2026, 8, 17, tzinfo=timezone.utc))


def test_orphan_mapping_override_is_rejected_by_build(tmp_path: Path) -> None:
    listings = tmp_path / "listings.csv"
    references = tmp_path / "reference.csv"
    mappings = tmp_path / "mappings.csv"
    _write_csv(listings, ["exchange", "ticker", "name", "isin", "asset_type"], [
        listing("X", "A", "Alpha Inc", isin="US0378331005")
    ])
    _write_csv(
        references,
        ["source_key", "exchange", "ticker", "name", "isin", "asset_type", "official", "listing_status", "reference_scope"],
        [reference("s", "X", "A", "Alpha Inc", isin="US0378331005")],
    )
    _write_csv(mappings, MAPPING_FIELDS, [{
        "source_key": "s", "reference_key": "X::MISSING", "listing_key": "X::A",
        "evidence_url": "https://example.test/evidence",
        "reviewed_at": "2026-08-17T00:00:00Z", "reviewer": "r", "reason": "review",
    }])
    with pytest.raises(ValueError, match="inactive or missing official observations"):
        build_reconciliation(
            listings_csv=listings, reference_csv=references, mapping_overrides_csv=mappings,
            out_csv=tmp_path / "out.csv", out_json=tmp_path / "out.json", out_md=tmp_path / "out.md",
            as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
        )
