from __future__ import annotations

import json

from scripts.apply_review_batches import apply_operations, load_operations, resolve_batch_files


def test_apply_operations_updates_source_rows():
    ticker_rows = [
        {
            "ticker": "AAA",
            "name": "Alpha Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Information Technology",
            "country": "United States",
            "country_code": "US",
            "isin": "US0378331005",
            "aliases": "alpha|legacy",
        },
        {
            "ticker": "BBB",
            "name": "Beta Inc",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "sector": "Industrials",
            "country": "United States",
            "country_code": "US",
            "isin": "US5949181045",
            "aliases": "beta",
        },
    ]
    alias_rows = [
        {"ticker": "AAA", "alias": "alpha", "alias_type": "name"},
        {"ticker": "AAA", "alias": "legacy", "alias_type": "name"},
        {"ticker": "BBB", "alias": "beta", "alias_type": "name"},
    ]
    identifier_rows = [
        {"ticker": "AAA", "isin": "US0378331005", "wkn": ""},
        {"ticker": "BBB", "isin": "US5949181045", "wkn": ""},
    ]
    operations = [
        {"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NASDAQ", "alias": "legacy"},
        {
            "operation_type": "update_metadata",
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "country",
            "decision": "update",
            "proposed_value": "Canada",
        },
        {
            "operation_type": "update_metadata",
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "isin",
            "decision": "clear",
            "proposed_value": "",
        },
        {"operation_type": "drop_entry", "ticker": "BBB", "exchange": "NYSE"},
    ]

    updated_tickers, updated_aliases, updated_identifiers, summary = apply_operations(
        ticker_rows,
        alias_rows,
        identifier_rows,
        operations,
    )

    assert [row["ticker"] for row in updated_tickers] == ["AAA"]
    assert updated_tickers[0]["aliases"] == "alpha"
    assert updated_tickers[0]["country"] == "Canada"
    assert updated_tickers[0]["isin"] == ""
    assert updated_identifiers == [{"ticker": "AAA", "isin": "", "wkn": ""}]
    assert updated_aliases == [{"ticker": "AAA", "alias": "alpha", "alias_type": "name"}]
    assert summary["applied_by_type"] == {
        "drop_entry": 1,
        "remove_alias": 1,
        "update_metadata": 2,
    }


def test_apply_operations_tracks_skips():
    ticker_rows = [
        {
            "ticker": "AAA",
            "name": "Alpha Inc",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "sector": "Information Technology",
            "country": "United States",
            "country_code": "US",
            "isin": "US0378331005",
            "aliases": "alpha",
        }
    ]
    alias_rows = [{"ticker": "AAA", "alias": "alpha", "alias_type": "name"}]
    identifier_rows = [{"ticker": "AAA", "isin": "US0378331005", "wkn": ""}]
    operations = [
        {"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NYSE", "alias": "alpha"},
        {"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NASDAQ", "alias": "missing"},
        {"operation_type": "unknown", "ticker": "AAA", "exchange": "NASDAQ"},
    ]

    _, _, _, summary = apply_operations(ticker_rows, alias_rows, identifier_rows, operations)

    assert summary["applied_operations"] == 0
    assert summary["skipped_by_reason"] == {
        "alias_not_found": 1,
        "exchange_mismatch": 1,
        "unknown_operation_type": 1,
    }


def test_resolve_batch_files_and_load_operations(tmp_path):
    batch_dir = tmp_path / "batches"
    batch_dir.mkdir()
    batch1 = batch_dir / "pr-batch-0001.json"
    batch2 = batch_dir / "pr-batch-0002.json"
    batch1.write_text(json.dumps({"operations": [{"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NASDAQ", "alias": "foo"}]}))
    batch2.write_text(json.dumps({"operations": [{"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NASDAQ", "alias": "foo"}, {"operation_type": "drop_entry", "ticker": "BBB", "exchange": "NYSE"}]}))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "batches": [
                    {"file": str(batch1)},
                    {"file": str(batch2)},
                ]
            }
        )
    )

    resolved = resolve_batch_files(manifest_path=manifest, batch_file=None, batch_dir=batch_dir)
    operations, sources = load_operations(resolved)

    assert resolved == [batch1, batch2]
    assert len(operations) == 2
    assert [operation["operation_type"] for operation in operations] == ["remove_alias", "drop_entry"]
    assert len(sources) == 2
