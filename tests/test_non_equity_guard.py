import csv
from collections import defaultdict
from pathlib import Path

from scripts.lib.non_equity_guard import classify_non_equity_leakage, is_blocked_non_common_stock


ROOT = Path(__file__).resolve().parents[1]


def stock_row(**overrides: str) -> dict[str, str]:
    row = {
        "ticker": "GOOD",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Good Company Common Stock",
    }
    row.update(overrides)
    return row


def test_guard_blocks_known_non_common_stock_name_classes() -> None:
    examples = [
        stock_row(ticker="PREF", name="Issuer Preferred Stock"),
        stock_row(ticker="WRT", name="Issuer Warrants"),
        stock_row(ticker="RGT", name="Issuer Rights"),
        stock_row(ticker="UNIT", name="Issuer Acquisition Corp Units"),
        stock_row(ticker="NOTE", name="Issuer Senior Notes due 2030"),
    ]

    for row in examples:
        result = classify_non_equity_leakage(row)
        assert result["guard_decision"] == "blocked_non_common_stock"
        assert result["verification_evidence_required"] == (
            "official_security_type_cfi_openfigi_or_exchange_listing_evidence"
        )


def test_guard_blocks_unit_names_even_when_name_mentions_ordinary_shares() -> None:
    result = classify_non_equity_leakage(
        stock_row(
            ticker="SPACU",
            name="Example Acquisition Corp Units, each consisting of one Class A ordinary share and one warrant",
        )
    )

    assert result["guard_decision"] == "blocked_non_common_stock"
    assert result["leakage_class"] == "unit_name_pattern"


def test_guard_blocks_official_security_type_and_cfi_evidence() -> None:
    preferred = classify_non_equity_leakage(stock_row(securityType="Preferred Stock"))
    debt = classify_non_equity_leakage(stock_row(cfi="DBFUFR"))

    assert preferred["leakage_class"] == "preferred_security_type"
    assert preferred["confidence"] == "official"
    assert debt["leakage_class"] == "debt_instrument_cfi"
    assert debt["confidence"] == "official"


def test_guard_keeps_valid_common_stock_and_etfs_accepted() -> None:
    assert not is_blocked_non_common_stock(stock_row())
    assert classify_non_equity_leakage(stock_row(cfi="ESVUFR"))["guard_decision"] == "accepted_or_not_applicable"
    assert (
        classify_non_equity_leakage(
            {"ticker": "ETFZ", "asset_type": "ETF", "name": "Example ETF"}
        )["guard_decision"]
        == "accepted_or_not_applicable"
    )


def test_guard_queues_ambiguous_closed_end_funds_without_auto_block() -> None:
    result = classify_non_equity_leakage(stock_row(name="Example Municipal Fund"))

    assert result["guard_decision"] == "manual_review_ambiguous_stock_classification"
    assert result["source_gate"] == (
        "Queue for manual classification; name shape alone does not authorize a Stock or ETF change."
    )


def test_rebuilt_listings_do_not_retain_officially_blocked_stock_rows() -> None:
    """Official exchange evidence must reach the rebuild's exclusion seam."""

    with (ROOT / "data" / "listings.csv").open(newline="", encoding="utf-8") as handle:
        listings = list(csv.DictReader(handle))
    with (ROOT / "data" / "masterfiles" / "reference.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        references = list(csv.DictReader(handle))
    with (ROOT / "data" / "review_overrides" / "preferred_allowlist.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        preferred_allowlist = {
            (row["ticker"].strip(), row["exchange"].strip())
            for row in csv.DictReader(handle)
            if row.get("ticker", "").strip() and row.get("exchange", "").strip()
        }

    official_by_key: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for reference in references:
        if (
            reference.get("official") == "true"
            and reference.get("listing_status") == "active"
        ):
            official_by_key[
                (reference.get("ticker", ""), reference.get("exchange", ""), reference.get("asset_type", ""))
            ].append(reference)

    blocked_listing_keys: list[str] = []
    for listing in listings:
        if listing.get("asset_type") != "Stock":
            continue
        if (listing.get("ticker", ""), listing.get("exchange", "")) in preferred_allowlist:
            continue

        evidence = dict(listing)
        references_for_listing = official_by_key.get(
            (listing.get("ticker", ""), listing.get("exchange", ""), "Stock"),
            [],
        )
        if references_for_listing:
            evidence.update({key: value for key, value in references_for_listing[0].items() if value})

        if is_blocked_non_common_stock(evidence):
            blocked_listing_keys.append(f"{listing['exchange']}::{listing['ticker']}")

    assert blocked_listing_keys == []


def test_rebuild_uses_same_venue_official_evidence_for_exclusion(monkeypatch) -> None:
    """A generic issuer name must not hide an official warrant classification."""

    from scripts import rebuild_dataset

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_evidence_rows",
        lambda: {
            ("WRTW", "NASDAQ", "Stock"): (
                {
                    "ticker": "WRTW",
                    "exchange": "NASDAQ",
                    "asset_type": "Stock",
                    "name": "Example Holdings, Inc. - Warrant",
                    "official": "true",
                    "listing_status": "active",
                },
            )
        },
    )

    assert rebuild_dataset.should_exclude_row(
        stock_row(ticker="WRTW", name="Example Holdings, Inc.")
    )
