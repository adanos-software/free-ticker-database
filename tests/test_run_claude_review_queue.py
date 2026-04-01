from __future__ import annotations

import json

from scripts.run_claude_review_queue import (
    build_claude_command,
    build_claude_prompt,
    coerce_review_payload,
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
    required = {
        "ticker",
        "exchange",
        "entry_decision",
        "ticker_exists",
        "name_matches_listing",
        "alias_actions",
        "metadata_actions",
        "confidence",
        "summary",
    }
    direct = json.dumps(
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "entry_decision": "keep",
            "ticker_exists": "yes",
            "name_matches_listing": "yes",
            "alias_actions": [],
            "metadata_actions": [],
            "confidence": 0.9,
            "summary": "ok",
        }
    )
    wrapped = json.dumps({"result": direct})
    structured = json.dumps({"structured_output": json.loads(direct)})
    assert extract_structured_output(direct, required)["ticker"] == "AAA"
    assert extract_structured_output(wrapped, required)["exchange"] == "NASDAQ"
    assert extract_structured_output(structured, required)["entry_decision"] == "keep"


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


def test_build_claude_prompt_embeds_entry():
    prompt = build_claude_prompt(
        {"ticker": "AAA", "exchange": "NASDAQ", "asset_type": "Stock", "country": "United States", "total_score": 50, "findings": [], "aliases": []},
        "Use Claude conservatively.",
    )
    assert "Use Claude conservatively." in prompt
    assert "Required top-level fields exactly:" in prompt
    assert '"ticker": "AAA"' in prompt


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
