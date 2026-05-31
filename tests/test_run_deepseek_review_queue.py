import json

from scripts.run_deepseek_review_queue import (
    build_prompt,
    compact_row,
    normalize_payload,
    parse_json_object,
    run,
    parse_args,
)


def test_compact_row_keeps_review_kind_fields_without_apply_values() -> None:
    row = {
        "listing_key": "OTC::ABCD",
        "ticker": "ABCD",
        "exchange": "OTC",
        "asset_type": "Stock",
        "name": "Example Corp",
        "instrument_scope": "extended",
        "scope_reason": "otc_listing",
        "source_gap_class": "needs_official_exchange_evidence",
        "irrelevant": "ignored",
    }

    compacted = compact_row(row, "otc_scope")

    assert compacted["listing_key"] == "OTC::ABCD"
    assert compacted["source_gap_class"] == "needs_official_exchange_evidence"
    assert "irrelevant" not in compacted


def test_build_prompt_forbids_invented_data_and_requires_exact_count() -> None:
    prompt = build_prompt(
        [
            {
                "listing_key": "OTC::ABCD",
                "ticker": "ABCD",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "Example Corp",
            }
        ],
        review_kind="otc_scope",
    )

    assert "do not invent ISINs" in prompt
    assert "Never output a value that should be applied to the database" in prompt
    assert "safe_action" in prompt
    assert "Return exactly 1 review objects" in prompt


def test_parse_json_object_accepts_fenced_json() -> None:
    assert parse_json_object('```json\n{"reviews":[]}\n```') == {"reviews": []}


def test_normalize_payload_blocks_invalid_decisions() -> None:
    normalized = normalize_payload(
        {
            "reviews": [
                {
                    "listing_key": "OTC::ABCD",
                    "ticker": "ABCD",
                    "exchange": "OTC",
                    "review_kind": "otc_scope",
                    "decision_candidate": "apply_sector",
                    "safe_action": "apply_sector",
                    "confidence": 7,
                    "evidence_needed": "Official filing",
                    "rationale": "Looks likely",
                    "do_not_apply_reason": "Not official",
                }
            ]
        },
        [{"listing_key": "OTC::ABCD", "ticker": "ABCD", "exchange": "OTC"}],
        "otc_scope",
    )

    assert normalized[0]["decision_candidate"] == "uncertain"
    assert normalized[0]["safe_action"] == "needs_official_evidence"
    assert normalized[0]["confidence"] == 1.0


def test_run_dry_run_writes_normalized_outputs(tmp_path) -> None:
    input_csv = tmp_path / "queue.csv"
    input_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,instrument_scope,scope_reason\n"
        "OTC::ABCD,ABCD,OTC,Stock,Example Corp,extended,otc_listing\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "out"
    args = parse_args(
        [
            "--input-csv",
            str(input_csv),
            "--output-dir",
            str(output_dir),
            "--raw-responses-jsonl",
            str(output_dir / "raw.jsonl"),
            "--normalized-json",
            str(output_dir / "normalized.json"),
            "--normalized-csv",
            str(output_dir / "normalized.csv"),
            "--errors-json",
            str(output_dir / "errors.json"),
            "--limit",
            "1",
            "--dry-run",
        ]
    )

    assert run(args) == 0
    payload = json.loads((output_dir / "normalized.json").read_text(encoding="utf-8"))

    assert payload["_meta"]["dry_run"] is True
    assert payload["items"][0]["decision_candidate"] == "needs_official_evidence"
    assert payload["items"][0]["safe_action"] == "needs_official_evidence"
