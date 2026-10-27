from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import load_csv
from scripts.rebuild_dataset import (
    OTC_EXCHANGES,
    cleaned_rows,
    duplicate_alias_owner_score,
    normalize_alias_text,
    strip_legal_alias_suffixes,
)


ROOT = Path(__file__).resolve().parents[1]
LISTINGS = ROOT / "data/listings.csv"
TRANSITIONS = ROOT / "data/review_overrides/listing_transitions.csv"
METADATA = ROOT / "data/review_overrides/metadata_updates.csv"


def _listings() -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in load_csv(LISTINGS)}


def _aliases(row: dict[str, str]) -> set[str]:
    return {alias for alias in row.get("aliases", "").split("|") if alias}


def test_spym_keeps_reviewed_splg_historical_ticker_alias() -> None:
    alias_row = [
        row
        for row in load_csv(METADATA)
        if row.get("ticker") == "SPYM"
        and row.get("exchange") == "NYSE ARCA"
        and row.get("field") == "aliases"
        and row.get("decision") == "update"
    ]
    assert len(alias_row) == 1
    assert alias_row[0]["proposed_value"] == "splg"
    listings = _listings()
    assert "splg" in _aliases(listings["NYSE ARCA::SPYM"])


def test_ethe_nyse_stale_row_dropped_for_nyse_arca_listing() -> None:
    listings = _listings()
    assert "NYSE::ETHE" not in listings
    assert listings["NYSE ARCA::ETHE"]["isin"] == "US3896381072"
    transition = [
        row for row in load_csv(TRANSITIONS) if row.get("old_listing_key") == "NYSE::ETHE"
    ]
    assert len(transition) == 1
    assert transition[0]["new_listing_key"] == "NYSE ARCA::ETHE"
    assert transition[0]["event_type"] == "venue_changed"


def test_us_etf_primary_exchanges_share_duplicate_alias_bonus() -> None:
    base = {
        "ticker": "ZZZZ",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "Example ETF",
        "isin": "US1234567890",
    }
    arca_score = duplicate_alias_owner_score(base, "example etf")
    bats_score = duplicate_alias_owner_score({**base, "exchange": "BATS"}, "example etf")
    assert arca_score == bats_score
    assert arca_score > duplicate_alias_owner_score({**base, "exchange": "OTC"}, "example etf")


def _allowed_with_bonus(rows: list[tuple[int, dict[str, str]]], alias: str, bonus_exchanges: set[str]) -> set[int]:
    entity_keys = {
        f"isin:{row['isin']}" if row.get("isin") else f"ticker:{row['ticker']}"
        for _, row in rows
    }
    if len(entity_keys) <= 1:
        return {index for index, _ in rows}

    def score(row: dict[str, str]) -> int:
        total = 0
        if row.get("exchange") in OTC_EXCHANGES:
            total -= 30
        else:
            total += 10
        if row.get("asset_type") == "Stock":
            total += 5
        if normalize_alias_text(row.get("name", "")).lower() == alias:
            total += 40
        if strip_legal_alias_suffixes(normalize_alias_text(row.get("name", "")).lower()) == alias:
            total += 30
        base = {"XETRA", "LSE", "Euronext", "SIX", "NYSE", "NASDAQ", "TSX", "TSE"}
        if row.get("exchange") in base | bonus_exchanges:
            total += 3
        return total

    scored = [(score(row), index) for index, row in rows]
    scored.sort(reverse=True)
    if len(scored) < 2 or scored[0][0] <= scored[1][0]:
        return set()
    return {scored[0][1]}


def test_duplicate_alias_owner_bonus_change_count_is_zero_on_current_dataset() -> None:
    rows, _ = cleaned_rows()
    alias_to_entries: dict[str, list[tuple[int, dict[str, str]]]] = {}
    for index, row in enumerate(rows):
        for alias in row.get("aliases", []):
            normalized = normalize_alias_text(alias).lower()
            if normalized:
                alias_to_entries.setdefault(normalized, []).append((index, row))

    changed = 0
    for alias, entries in alias_to_entries.items():
        if len({index for index, _ in entries}) <= 1:
            continue
        before = _allowed_with_bonus(entries, alias, set())
        after = _allowed_with_bonus(entries, alias, {"NYSE ARCA", "BATS"})
        if before != after:
            changed += 1
    assert changed == 0
