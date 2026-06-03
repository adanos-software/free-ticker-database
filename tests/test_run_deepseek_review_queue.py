import json

import pytest

from scripts.run_deepseek_review_queue import (
    build_prompt,
    compact_row,
    dry_run_would_overwrite_non_dry_run_output,
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


def test_compact_row_supports_otc_name_mismatch_review_fields() -> None:
    row = {
        "listing_key": "OTC::ABCD",
        "ticker": "ABCD",
        "exchange": "OTC",
        "asset_type": "Stock",
        "current_name": "Old Name Corp",
        "official_name": "Official Name Inc",
        "isin": "US0000000000",
        "review_class": "probable_otc_rename_or_symbol_reuse",
        "review_priority": "high",
        "recommended_next_source": "Official issuer-history source.",
        "source_gate": "Do not change the name until identity evidence is reviewed.",
        "suggested_name": "Must Not Be Included",
    }

    compacted = compact_row(row, "otc_name_mismatch")

    assert compacted["listing_key"] == "OTC::ABCD"
    assert compacted["current_name"] == "Old Name Corp"
    assert compacted["official_name"] == "Official Name Inc"
    assert compacted["review_class"] == "probable_otc_rename_or_symbol_reuse"
    assert "suggested_name" not in compacted


def test_compact_row_supports_source_gap_review_fields() -> None:
    row = {
        "field": "missing_isin_primary",
        "target_field": "isin",
        "listing_key": "ASX::ABCD",
        "ticker": "ABCD",
        "exchange": "ASX",
        "asset_type": "Stock",
        "name": "Example Ltd",
        "gap_class": "official_identifier_not_exposed_source_gap",
        "recommended_next_source": "Official CSD/security registry.",
        "source_gate": "Do not infer from issuer name.",
        "source_gap_context": "listing_key=ASX::ABCD;field=missing_isin_primary",
        "inferred_isin": "Must Not Be Included",
    }

    compacted = compact_row(row, "source_gap")

    assert compacted["listing_key"] == "ASX::ABCD"
    assert compacted["target_field"] == "isin"
    assert compacted["gap_class"] == "official_identifier_not_exposed_source_gap"
    assert compacted["source_gap_context"] == "listing_key=ASX::ABCD;field=missing_isin_primary"
    assert "inferred_isin" not in compacted


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


def test_parse_args_rejects_env_file_secret_source() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--env-file", ".env"])


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


def test_normalize_payload_clamps_incompatible_safe_actions() -> None:
    normalized = normalize_payload(
        {
            "reviews": [
                {
                    "listing_key": "OTC::ABCD",
                    "ticker": "ABCD",
                    "exchange": "OTC",
                    "review_kind": "otc_scope",
                    "decision_candidate": "keep_source_gap",
                    "safe_action": "likely_same_issuer_review",
                    "confidence": 0.8,
                }
            ]
        },
        [{"listing_key": "OTC::ABCD", "ticker": "ABCD", "exchange": "OTC"}],
        "otc_scope",
    )

    assert normalized[0]["decision_candidate"] == "keep_source_gap"
    assert normalized[0]["safe_action"] == "source_gap_accept"


def test_normalize_payload_strips_key_fields_and_falls_back_to_source_row() -> None:
    normalized = normalize_payload(
        {
            "reviews": [
                {
                    "listing_key": "   ",
                    "ticker": " ABCD ",
                    "exchange": "   ",
                    "review_kind": " otc_scope ",
                    "decision_candidate": " needs_official_evidence ",
                    "safe_action": " needs_official_evidence ",
                }
            ]
        },
        [{"listing_key": " OTC::ABCD ", "ticker": "SRC", "exchange": " OTC "}],
        "otc_scope",
    )

    assert normalized[0]["listing_key"] == "OTC::ABCD"
    assert normalized[0]["ticker"] == "ABCD"
    assert normalized[0]["exchange"] == "OTC"
    assert normalized[0]["review_kind"] == "otc_scope"
    assert normalized[0]["decision_candidate"] == "needs_official_evidence"
    assert normalized[0]["safe_action"] == "needs_official_evidence"


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


def test_run_creates_parent_dirs_for_custom_output_paths(tmp_path) -> None:
    input_csv = tmp_path / "queue.csv"
    input_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,instrument_scope,scope_reason\n"
        "OTC::ABCD,ABCD,OTC,Stock,Example Corp,extended,otc_listing\n",
        encoding="utf-8",
    )
    args = parse_args(
        [
            "--input-csv",
            str(input_csv),
            "--output-dir",
            str(tmp_path / "base-out"),
            "--raw-responses-jsonl",
            str(tmp_path / "custom" / "raw" / "raw.jsonl"),
            "--normalized-json",
            str(tmp_path / "custom" / "json" / "normalized.json"),
            "--normalized-csv",
            str(tmp_path / "custom" / "csv" / "normalized.csv"),
            "--errors-json",
            str(tmp_path / "custom" / "errors" / "errors.json"),
            "--limit",
            "1",
            "--dry-run",
        ]
    )

    assert run(args) == 0
    assert (tmp_path / "custom" / "raw" / "raw.jsonl").exists()
    assert (tmp_path / "custom" / "json" / "normalized.json").exists()
    assert (tmp_path / "custom" / "csv" / "normalized.csv").exists()
    assert (tmp_path / "custom" / "errors" / "errors.json").exists()


def test_dry_run_overwrite_guard_detects_existing_live_output(tmp_path) -> None:
    output_json = tmp_path / "normalized.json"
    output_json.write_text('{"_meta":{"dry_run":false},"items":[]}', encoding="utf-8")

    assert dry_run_would_overwrite_non_dry_run_output(output_json) is True

    output_json.write_text('{"_meta":{"dry_run":true},"items":[]}', encoding="utf-8")
    assert dry_run_would_overwrite_non_dry_run_output(output_json) is False


def test_run_dry_run_refuses_to_clobber_live_output(tmp_path) -> None:
    input_csv = tmp_path / "queue.csv"
    input_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,instrument_scope,scope_reason\n"
        "OTC::ABCD,ABCD,OTC,Stock,Example Corp,extended,otc_listing\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    normalized_json = output_dir / "normalized.json"
    normalized_json.write_text('{"_meta":{"dry_run":false},"items":[]}', encoding="utf-8")
    args = parse_args(
        [
            "--input-csv",
            str(input_csv),
            "--output-dir",
            str(output_dir),
            "--raw-responses-jsonl",
            str(output_dir / "raw.jsonl"),
            "--normalized-json",
            str(normalized_json),
            "--normalized-csv",
            str(output_dir / "normalized.csv"),
            "--errors-json",
            str(output_dir / "errors.json"),
            "--limit",
            "1",
            "--dry-run",
        ]
    )

    with pytest.raises(SystemExit) as excinfo:
        run(args)

    assert "would overwrite or append to existing DeepSeek review outputs" in str(excinfo.value)


def test_run_dry_run_refuses_to_append_existing_raw_responses(tmp_path) -> None:
    input_csv = tmp_path / "queue.csv"
    input_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,instrument_scope,scope_reason\n"
        "OTC::ABCD,ABCD,OTC,Stock,Example Corp,extended,otc_listing\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    raw_jsonl = output_dir / "raw.jsonl"
    raw_jsonl.write_text('{"batch_index":1}\n', encoding="utf-8")
    args = parse_args(
        [
            "--input-csv",
            str(input_csv),
            "--output-dir",
            str(output_dir),
            "--raw-responses-jsonl",
            str(raw_jsonl),
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

    with pytest.raises(SystemExit) as excinfo:
        run(args)

    assert "would overwrite or append to existing DeepSeek review outputs" in str(excinfo.value)
