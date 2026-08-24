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


def test_transition_drop_activates_only_after_replacement_survives_cleaning(tmp_path, monkeypatch):
    transitions = tmp_path / "listing_transitions.csv"
    write_csv(
        transitions,
        ["old_listing_key", "new_listing_key"],
        [{"old_listing_key": "NASDAQ::OLD", "new_listing_key": "NASDAQ::NEW"}],
    )
    monkeypatch.setattr(rebuild_dataset, "REVIEW_LISTING_TRANSITIONS_CSV", transitions)
    drops = {("OLD", "NASDAQ"), ("STALE", "NYSE")}
    new_row = {"ticker": "NEW", "exchange": "NASDAQ"}

    assert rebuild_dataset.active_review_transition_drops(drops, []) == set()
    assert rebuild_dataset.active_review_transition_drops(drops, [new_row]) == {
        ("OLD", "NASDAQ")
    }

    drops.add(("NEW", "NASDAQ"))
    assert rebuild_dataset.active_review_transition_drops(drops, [new_row]) == set()


def test_split_aliases_accepts_review_override_lists():
    assert rebuild_dataset.split_aliases(["hotel fast sse", "", " stockholm "]) == [
        "hotel fast sse",
        "stockholm",
    ]


def test_apply_output_metadata_overrides_reinfers_country_from_isin_after_clear():
    updated = rebuild_dataset.apply_output_metadata_overrides(
        {
            "ticker": "MFG",
            "exchange": "NYSE",
            "country": "",
            "country_code": "",
            "isin": "US60687Y1091",
            "aliases": [],
        },
        {
            "country": {
                "decision": "clear",
                "proposed_value": "",
                "confidence": "0.9",
                "reason": "clear contaminated country before ISIN inference",
            }
        },
    )

    assert updated["country"] == "United States"
    assert updated["country_code"] == "US"


def test_apply_review_alias_removals_removes_name_derived_aliases_too():
    aliases = ["alten", "alten sa", "legacy"]
    removals = {"alten", "legacy"}

    assert rebuild_dataset.apply_review_alias_removals(aliases, removals) == ["alten sa"]


def test_apply_official_listing_asset_type_uses_jpx_listed_issues(tmp_path, monkeypatch):
    reference = tmp_path / "reference.csv"
    write_csv(
        reference,
        [
            "source_key",
            "ticker",
            "exchange",
            "asset_type",
            "listing_status",
            "reference_scope",
            "official",
        ],
        [
            {
                "source_key": "jpx_listed_issues",
                "ticker": "462A",
                "exchange": "TSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
            },
            {
                "source_key": "jpx_tse_stock_detail",
                "ticker": "462A",
                "exchange": "TSE",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "security_identifier_registry_subset",
                "official": "true",
            },
        ],
    )
    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_REFERENCE_CSV", reference)
    rebuild_dataset.load_active_jpx_listed_issue_asset_types.cache_clear()

    corrected = rebuild_dataset.apply_official_listing_asset_type(
        {
            "ticker": "462A",
            "exchange": "TSE",
            "asset_type": "ETF",
            "stock_sector": "",
            "etf_category": "",
        },
        {},
    )

    assert corrected["asset_type"] == "Stock"
    assert corrected["etf_category"] == ""


def test_apply_official_listing_asset_type_keeps_review_override(tmp_path, monkeypatch):
    reference = tmp_path / "reference.csv"
    write_csv(
        reference,
        [
            "source_key",
            "ticker",
            "exchange",
            "asset_type",
            "listing_status",
            "reference_scope",
            "official",
        ],
        [
            {
                "source_key": "jpx_listed_issues",
                "ticker": "462A",
                "exchange": "TSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
            },
        ],
    )
    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_REFERENCE_CSV", reference)
    rebuild_dataset.load_active_jpx_listed_issue_asset_types.cache_clear()

    corrected = rebuild_dataset.apply_official_listing_asset_type(
        {"ticker": "462A", "exchange": "TSE", "asset_type": "ETF"},
        {"asset_type": {"decision": "update", "proposed_value": "ETF"}},
    )

    assert corrected["asset_type"] == "ETF"
