from __future__ import annotations

import json

from scripts.run_claude_review_queue import (
    batch_cost_from_output,
    build_batches,
    build_batch_schema,
    build_claude_command,
    build_claude_batch_prompt,
    chunk_items,
    coerce_review_payload,
    coerce_batch_payload,
    extract_structured_output,
    filter_items,
    normalize_claude_result,
)


def test_build_claude_command_includes_local_execution_flags(tmp_path):
    command = build_claude_command(
        prompt="Review this item",
        schema={"type": "object"},
        model="sonnet",
        cwd=tmp_path,
    )
    assert command[:4] == ["claude", "--dangerously-skip-permissions", "-p", "--output-format"]
    assert "--append-system-prompt" in command


def test_extract_structured_output_accepts_direct_and_wrapped_payloads():
    direct = json.dumps(
        {
            "reviews": [
                {
                    "ticker": "AAA",
                    "exchange": "NASDAQ",
                    "recommended_action": "needs_human",
                    "ticker_exists": "unknown",
                    "listing_valid": "unknown",
                    "explanation": "Need manual review.",
                    "alias_actions": [{"alias": "foo", "action": "remove", "reason": "bad"}],
                }
            ]
        }
    )
    wrapped = json.dumps({"result": direct})
    structured = json.dumps({"structured_output": json.loads(direct)})
    assert extract_structured_output(direct, {"reviews"})["reviews"][0]["ticker"] == "AAA"
    assert extract_structured_output(wrapped, {"reviews"})["reviews"][0]["exchange"] == "NASDAQ"
    assert extract_structured_output(structured, {"reviews"})["reviews"][0]["entry_decision"] == "needs_human"
    assert extract_structured_output(direct, {"reviews"})["reviews"][0]["summary"] == "Need manual review."


def test_filter_items_supports_score_offset_limit_and_skip():
    items = [
        {"ticker": "AAA", "exchange": "NASDAQ", "total_score": 10},
        {"ticker": "BBB", "exchange": "NYSE", "total_score": 90},
        {"ticker": "CCC", "exchange": "NYSE", "total_score": 95},
    ]
    selected = filter_items(
        items,
        min_score=50,
        offset=0,
        limit=1,
        include_tickers=set(),
        processed_pairs={("BBB", "NYSE")},
    )
    assert selected == [{"ticker": "CCC", "exchange": "NYSE", "total_score": 95}]


def test_normalize_claude_result_matches_existing_normalized_shape(tmp_path):
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
    item = {"ticker": "AAA", "exchange": "NASDAQ"}
    queue_lookup = {
        ("AAA", "NASDAQ"): {
            "total_score": 75,
            "findings": [{"finding_type": "cross_company_alias_collision"}],
            "aliases": ["example"],
        }
    }
    normalized, error = normalize_claude_result(
        item=item,
        parsed_payload={
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "entry_decision": "fix_metadata",
            "ticker_exists": "yes",
            "name_matches_listing": "yes",
            "alias_actions": [{"alias": "example", "decision": "remove", "reason": "bad alias"}],
            "metadata_actions": [],
            "confidence": 0.91,
            "summary": "Remove alias.",
        },
        raw_output_path=tmp_path / "raw.jsonl",
        queue_lookup=queue_lookup,
        schema=schema,
    )
    assert error is None
    assert normalized["ticker"] == "AAA"
    assert normalized["queue_total_score"] == 75
    assert normalized["alias_actions"][0]["decision"] == "remove"


def test_build_claude_batch_prompt_embeds_exact_batch_constraints():
    items = [
        {"ticker": "AAA", "exchange": "NASDAQ", "asset_type": "Stock", "country": "United States", "total_score": 50, "findings": [], "aliases": []},
        {"ticker": "BBB", "exchange": "NYSE", "asset_type": "Stock", "country": "United States", "total_score": 60, "findings": [], "aliases": []},
    ]
    prompt = build_claude_batch_prompt(items, "Review conservatively.")
    assert "exactly one key: reviews" in prompt
    assert "Return exactly 2 review objects." in prompt
    assert "Entry 1" in prompt
    assert '"ticker": "AAA"' in prompt
    assert '"ticker": "BBB"' in prompt


def test_coerce_review_payload_maps_common_claude_alt_fields():
    payload = coerce_review_payload(
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "recommended_action": "needs_human",
            "ticker_exists": "unknown",
            "listing_valid": "unknown",
            "explanation": "Need manual review.",
            "alias_actions": [{"alias": "foo", "action": "remove", "reason": "bad"}],
            "fields_requiring_verification": ["isin"],
        }
    )
    assert payload["entry_decision"] == "needs_human"
    assert payload["name_matches_listing"] == "unknown"
    assert payload["summary"] == "Need manual review."
    assert payload["confidence"] == 0.3
    assert payload["alias_actions"][0]["decision"] == "remove"
    assert payload["metadata_actions"][0]["field"] == "isin"


def test_batch_helpers_support_chunking_and_batch_schema():
    items = [{"ticker": str(index)} for index in range(23)]
    chunks = chunk_items(items, 10)
    assert [len(chunk) for chunk in chunks] == [10, 10, 3]

    schema = build_batch_schema({"type": "object"}, 10)
    assert schema["properties"]["reviews"]["minItems"] == 10
    assert schema["properties"]["reviews"]["maxItems"] == 10


def test_build_batches_defers_partial_tail_by_default_and_can_allow_it():
    items = [{"ticker": str(index)} for index in range(23)]
    batches, deferred = build_batches(items, 10, allow_partial_batch=False)
    assert [len(batch) for batch in batches] == [10, 10]
    assert [item["ticker"] for item in deferred] == ["20", "21", "22"]

    batches, deferred = build_batches(items, 10, allow_partial_batch=True)
    assert [len(batch) for batch in batches] == [10, 10, 3]
    assert deferred == []


def test_coerce_batch_payload_accepts_reviews_array_and_plain_list():
    payload = coerce_batch_payload(
        {
            "reviews": [
                {
                    "ticker": "AAA",
                    "exchange": "NASDAQ",
                    "recommended_action": "needs_human",
                    "ticker_exists": "unknown",
                    "listing_valid": "unknown",
                    "explanation": "Need manual review.",
                    "alias_actions": [{"alias": "foo", "action": "remove", "reason": "bad"}],
                }
            ]
        }
    )
    assert payload["reviews"][0]["entry_decision"] == "needs_human"
    assert payload["reviews"][0]["summary"] == "Need manual review."

    payload = coerce_batch_payload(
        [
            {
                "ticker": "BBB",
                "exchange": "NYSE",
                "entry_decision": "keep",
                "ticker_exists": "yes",
                "name_matches_listing": "yes",
                "alias_actions": [],
                "metadata_actions": [],
                "confidence": 0.9,
                "summary": "ok",
            }
        ]
    )
    assert payload["reviews"][0]["ticker"] == "BBB"


def test_batch_cost_from_output_extracts_total_cost():
    raw_output = json.dumps({"total_cost_usd": 1.25})
    assert batch_cost_from_output(raw_output) == 1.25
