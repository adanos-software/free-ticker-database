from __future__ import annotations

from scripts.review_utils import (
    build_normalized_payload,
    build_queue_lookup,
    validate_review_response,
    write_normalized_csv,
)


def test_validate_review_response_accepts_schema_conforming_payload():
    schema = {
        "required": [
            "ticker",
            "exchange",
            "entry_decision",
            "ticker_exists",
            "name_matches_listing",
            "alias_actions",
            "metadata_actions",
            "confidence",
            "summary",
        ],
        "properties": {
            "entry_decision": {"enum": ["keep", "drop_entry", "fix_metadata", "needs_human"]},
            "ticker_exists": {"enum": ["yes", "no", "unknown"]},
            "name_matches_listing": {"enum": ["yes", "no", "unknown"]},
        },
    }
    payload = {
        "ticker": "AAA",
        "exchange": "NASDAQ",
        "entry_decision": "keep",
        "ticker_exists": "yes",
        "name_matches_listing": "yes",
        "alias_actions": [{"alias": "example", "decision": "keep", "reason": "ok"}],
        "metadata_actions": [],
        "confidence": 0.9,
        "summary": "Looks correct.",
    }
    assert validate_review_response(payload, schema) == []


def test_build_queue_lookup_and_normalized_payload(tmp_path):
    queue_payload = {
        "items": [
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "total_score": 90,
                "findings": [{"finding_type": "blocked_alias_present"}],
                "aliases": ["example"],
            }
        ]
    }
    lookup = build_queue_lookup(queue_payload)
    assert lookup[("AAA", "NASDAQ")]["total_score"] == 90

    normalized_items = [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "entry_decision": "keep",
            "ticker_exists": "yes",
            "name_matches_listing": "yes",
            "alias_actions": [{"alias": "example", "decision": "remove", "reason": "bad alias"}],
            "metadata_actions": [{"field": "country", "decision": "needs_human", "reason": "check"}],
            "confidence": 0.9,
            "summary": "Looks mostly good.",
            "queue_total_score": 90,
            "source_file": "responses.jsonl",
        }
    ]
    payload = build_normalized_payload(
        normalized_items,
        [],
        responses_path=tmp_path,
        queue_path=tmp_path / "review_queue.json",
        schema_path=tmp_path / "schema.json",
    )
    assert payload["_meta"]["normalized_reviews"] == 1
    assert payload["summary"]["entry_decisions"] == {"keep": 1}


def test_write_normalized_csv_outputs_summary_columns(tmp_path):
    normalized_items = [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "entry_decision": "keep",
            "ticker_exists": "yes",
            "name_matches_listing": "yes",
            "alias_actions": [{"alias": "example", "decision": "remove", "reason": "bad alias"}],
            "metadata_actions": [{"field": "country", "decision": "needs_human", "reason": "check"}],
            "confidence": 0.9,
            "summary": "Looks mostly good.",
            "queue_total_score": 90,
            "source_file": "responses.jsonl",
        }
    ]
    csv_path = tmp_path / "normalized.csv"
    write_normalized_csv(csv_path, normalized_items)
    rows = csv_path.read_text().splitlines()
    assert len(rows) == 2
    assert "alias_remove_count" in rows[0]
