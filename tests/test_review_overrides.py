from __future__ import annotations

import csv

from scripts import rebuild_dataset


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_load_review_overrides_and_apply_metadata(tmp_path, monkeypatch):
    remove_aliases = tmp_path / "remove_aliases.csv"
    metadata_updates = tmp_path / "metadata_updates.csv"
    drop_entries = tmp_path / "drop_entries.csv"

    write_csv(
        remove_aliases,
        ["ticker", "exchange", "alias", "confidence", "reason"],
        [{"ticker": "AAA", "exchange": "NASDAQ", "alias": "legacy", "confidence": "0.9", "reason": "bad"}],
    )
    write_csv(
        metadata_updates,
        ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        [
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "country",
                "decision": "update",
                "proposed_value": "United States",
                "confidence": "0.9",
                "reason": "fix",
            },
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "country_code",
                "decision": "update",
                "proposed_value": "US",
                "confidence": "0.9",
                "reason": "fix",
            },
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "isin",
                "decision": "clear",
                "proposed_value": "",
                "confidence": "0.9",
                "reason": "wrong",
            },
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "aliases",
                "decision": "clear",
                "proposed_value": "",
                "confidence": "0.9",
                "reason": "contaminated",
            },
            {
                "ticker": "BBB",
                "exchange": "SIX",
                "field": "ticker",
                "decision": "update",
                "proposed_value": "BMAG",
                "confidence": "0.99",
                "reason": "official rename",
            },
            {
                "ticker": "BBB",
                "exchange": "SIX",
                "field": "name",
                "decision": "update",
                "proposed_value": "Bajaj Mobility AG",
                "confidence": "0.99",
                "reason": "official rename",
            },
        ],
    )
    write_csv(
        drop_entries,
        ["ticker", "exchange", "confidence", "reason"],
        [{"ticker": "BBB", "exchange": "NYSE", "confidence": "0.95", "reason": "drop"}],
    )

    monkeypatch.setattr(rebuild_dataset, "REVIEW_REMOVE_ALIASES_CSV", remove_aliases)
    monkeypatch.setattr(rebuild_dataset, "REVIEW_METADATA_UPDATES_CSV", metadata_updates)
    monkeypatch.setattr(rebuild_dataset, "REVIEW_DROP_ENTRIES_CSV", drop_entries)

    alias_removals, metadata_overrides, drop_keys = rebuild_dataset.load_review_overrides()

    assert alias_removals[("AAA", "NASDAQ")] == {"legacy"}
    assert metadata_overrides[("AAA", "NASDAQ")]["country"]["proposed_value"] == "United States"
    assert drop_keys == {("BBB", "NYSE")}

    updated_input = rebuild_dataset.apply_input_metadata_overrides(
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "country": "Australia",
            "country_code": "AU",
            "isin": "AU0000000001",
            "aliases": ["legacy", "wrong"],
        },
        metadata_overrides[("AAA", "NASDAQ")],
    )
    assert updated_input["country"] == "United States"
    assert updated_input["isin"] == ""
    assert updated_input["aliases"] == []

    updated_output = rebuild_dataset.apply_output_metadata_overrides(
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "country": "United States",
            "country_code": "",
            "isin": "",
            "aliases": ["legacy"],
        },
        metadata_overrides[("AAA", "NASDAQ")],
    )
    assert updated_output["country_code"] == "US"
    assert updated_output["aliases"] == []

    renamed_input = rebuild_dataset.apply_input_metadata_overrides(
        {
            "ticker": "BBB",
            "exchange": "SIX",
            "name": "PIERER Mobility AG",
            "aliases": [],
        },
        metadata_overrides[("BBB", "SIX")],
    )
    assert renamed_input["ticker"] == "BMAG"
    assert renamed_input["name"] == "Bajaj Mobility AG"
