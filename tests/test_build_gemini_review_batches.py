from __future__ import annotations

import json

from scripts.build_gemini_review_batches import (
    build_manifest,
    build_prompt_text,
    build_request_line,
    chunk_items,
    group_items_by_primary_finding,
    write_jsonl,
)


def sample_item(
    ticker: str,
    *,
    exchange: str = "NASDAQ",
    score: int = 90,
    finding_type: str = "blocked_alias_present",
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "name": f"{ticker} Holdings Inc",
        "exchange": exchange,
        "asset_type": "Stock",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "aliases": ["example"],
        "total_score": score,
        "findings": [
            {
                "finding_type": finding_type,
                "severity": "high",
                "score": score,
                "field": "alias",
                "value": "example",
                "reason": "Example finding",
            }
        ],
    }


def test_build_request_line_uses_gemini_batch_shape(tmp_path):
    item = sample_item("AAA")
    schema = {"type": "object", "required": ["ticker"]}
    prompt_path = tmp_path / "prompt.md"
    schema_path = tmp_path / "schema.json"
    prompt_path.write_text("prompt")
    schema_path.write_text("{}")

    request_line = build_request_line(
        item,
        response_schema=schema,
        prompt_path=prompt_path,
        schema_path=schema_path,
    )

    assert request_line["key"].startswith("blocked-alias-present--AAA--NASDAQ")
    assert request_line["metadata"]["ticker"] == "AAA"
    assert request_line["request"]["generation_config"]["response_mime_type"] == "application/json"
    assert request_line["request"]["generation_config"]["response_json_schema"] == schema
    prompt_text = request_line["request"]["contents"][0]["parts"][0]["text"]
    assert '"ticker": "AAA"' in prompt_text
    assert "Return JSON only." in prompt_text


def test_chunk_items_respects_size_and_groups_stably():
    items = [
        sample_item("CCC", score=50, finding_type="low_company_name_overlap"),
        sample_item("AAA", score=100, finding_type="blocked_alias_present"),
        sample_item("BBB", score=90, finding_type="blocked_alias_present"),
    ]
    ordered = group_items_by_primary_finding(items)
    assert [item["ticker"] for item in ordered] == ["AAA", "BBB", "CCC"]

    batches = chunk_items(
        ordered,
        max_items_per_batch=2,
        max_prompt_chars_per_batch=10_000,
    )
    assert [[item["ticker"] for item in batch] for batch in batches] == [["AAA", "BBB"], ["CCC"]]


def test_write_jsonl_and_manifest(tmp_path):
    prompt_path = tmp_path / "prompt.md"
    schema_path = tmp_path / "schema.json"
    prompt_path.write_text("prompt")
    schema_path.write_text("{}")

    items = [
        sample_item("AAA", score=90, finding_type="blocked_alias_present"),
        sample_item("BBB", score=45, finding_type="cross_company_alias_collision"),
    ]
    batches = [[items[0]], [items[1]]]
    request_lines = [
        build_request_line(items[0], {"type": "object"}, prompt_path, schema_path),
    ]
    jsonl_path = tmp_path / "batch-0001.jsonl"
    write_jsonl(jsonl_path, request_lines)

    written_lines = jsonl_path.read_text().strip().splitlines()
    assert len(written_lines) == 1
    assert json.loads(written_lines[0])["metadata"]["ticker"] == "AAA"

    queue_payload = {
        "_meta": {"flagged_entries": 2, "min_score": 40},
        "summary": {
            "blocked_alias_present": 1,
            "cross_company_alias_collision": 1,
        },
    }
    manifest = build_manifest(
        queue_payload,
        batches,
        tmp_path,
        prompt_path,
        schema_path,
        max_items_per_batch=100,
        max_prompt_chars_per_batch=200_000,
    )

    assert manifest["_meta"]["total_batches"] == 2
    assert manifest["_meta"]["total_requests"] == 2
    assert manifest["batches"][0]["item_count"] == 1
    assert manifest["batches"][1]["finding_types"] == {"cross_company_alias_collision": 1}
