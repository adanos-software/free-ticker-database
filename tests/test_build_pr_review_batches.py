from __future__ import annotations

from scripts.build_pr_review_batches import build_operations, chunk_operations, write_batches


def test_build_operations_splits_actionable_and_manual_items():
    normalized_payload = {
        "items": [
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "entry_decision": "fix_metadata",
                "confidence": 0.95,
                "summary": "Remove a bad alias.",
                "alias_actions": [{"alias": "foo", "decision": "remove", "reason": "bad alias"}],
                "metadata_actions": [],
                "source_file": "responses-1.jsonl",
            },
            {
                "ticker": "BBB",
                "exchange": "NYSE",
                "entry_decision": "needs_human",
                "confidence": 0.99,
                "summary": "Unsure.",
                "alias_actions": [],
                "metadata_actions": [],
                "source_file": "responses-1.jsonl",
            },
            {
                "ticker": "CCC",
                "exchange": "NYSE",
                "entry_decision": "drop_entry",
                "confidence": 0.75,
                "summary": "Likely invalid.",
                "alias_actions": [],
                "metadata_actions": [],
                "source_file": "responses-1.jsonl",
            },
        ]
    }

    operations, manual_items = build_operations(normalized_payload, min_confidence=0.8)

    assert operations == [
        {
            "operation_type": "remove_alias",
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "alias": "foo",
            "confidence": 0.95,
            "reason": "bad alias",
            "source_file": "responses-1.jsonl",
        }
    ]
    assert [item["ticker"] for item in manual_items] == ["BBB", "CCC"]


def test_chunk_operations_and_write_batches(tmp_path):
    operations = [
        {"operation_type": "drop_entry", "ticker": "BBB", "exchange": "NYSE"},
        {"operation_type": "remove_alias", "ticker": "AAA", "exchange": "NASDAQ"},
        {"operation_type": "remove_alias", "ticker": "AAC", "exchange": "NASDAQ"},
    ]

    batches = chunk_operations(operations, max_operations_per_batch=1)
    assert [[op["ticker"] for op in batch] for batch in batches] == [["BBB"], ["AAA"], ["AAC"]]

    entries = write_batches(tmp_path, batches)
    assert len(entries) == 3
    assert entries[0]["operation_types"] == {"drop_entry": 1}
    assert entries[1]["operation_types"] == {"remove_alias": 1}
