from __future__ import annotations

import json
from pathlib import Path

import socket

from scripts.lib.dataio import display_path, load_csv, merge_metadata_updates, read_json, write_csv, write_json
from scripts.lib.http import socket_timeout
from scripts.lib.keys import listing_key, row_listing_key, split_listing_key
from scripts.lib.normalize import names_match, normalize_bool, normalize_listing_symbol, normalize_symbol


def test_csv_and_json_helpers_round_trip(tmp_path: Path) -> None:
    csv_path = tmp_path / "nested" / "rows.csv"
    json_path = tmp_path / "nested" / "payload.json"

    write_csv(csv_path, ["a", "b"], [{"a": "1", "b": "2", "extra": "ignored"}])
    write_json(json_path, {"rows": [1]})

    assert load_csv(csv_path) == [{"a": "1", "b": "2"}]
    assert read_json(json_path) == {"rows": [1]}
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"rows": [1]}
    assert read_json(tmp_path / "missing.json", default={}) == {}
    assert display_path(csv_path, tmp_path) == "nested/rows.csv"


def test_key_and_normalization_helpers() -> None:
    assert listing_key("NASDAQ", "AAPL") == "NASDAQ::AAPL"
    assert split_listing_key("NYSE::IBM") == ("NYSE", "IBM")
    assert split_listing_key("IBM") == ("", "IBM")
    assert row_listing_key({"exchange": "ASX", "ticker": "BHP"}) == "ASX::BHP"
    assert row_listing_key({"listing_key": "TSE::7203", "exchange": "X", "ticker": "Y"}) == "TSE::7203"
    assert normalize_symbol(" brk b ") == "BRKB"
    assert normalize_listing_symbol("brk-b") == "BRKB"
    assert normalize_bool("yes") is True
    assert normalize_bool("no") is False
    assert names_match("Antero Midstream Corp", "Antero Midstream Partners LP")
    assert not names_match("Credit Agricole SA", "Arcosa Inc")


def test_merge_metadata_updates_and_socket_timeout(tmp_path: Path) -> None:
    path = tmp_path / "metadata_updates.csv"
    write_csv(
        path,
        ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        [{"ticker": "ABC", "exchange": "NYSE", "field": "isin", "decision": "update", "proposed_value": "OLD", "confidence": "0.8", "reason": "old"}],
    )
    merge_metadata_updates(
        path,
        [{"ticker": "ABC", "exchange": "NYSE", "field": "isin", "decision": "update", "proposed_value": "NEW", "confidence": "0.9", "reason": "new"}],
    )
    assert load_csv(path)[0]["proposed_value"] == "NEW"

    previous = socket.getdefaulttimeout()
    with socket_timeout(3.0):
        assert socket.getdefaulttimeout() == 3.0
    assert socket.getdefaulttimeout() == previous
