import json

from scripts.build_deepseek_review_summary import build_payload, render_markdown


def test_build_payload_summarizes_raw_deepseek_batches(tmp_path) -> None:
    raw = tmp_path / "raw.jsonl"
    raw.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "batch_index": 1,
                        "review_kind": "otc_scope",
                        "response": {
                            "reviews": [
                                {
                                    "listing_key": "OTC::ABCD",
                                    "ticker": "ABCD",
                                    "exchange": "OTC",
                                    "review_kind": "otc_scope",
                                    "decision_candidate": "needs_official_evidence",
                                    "confidence": 0.3,
                                    "evidence_needed": "Official source",
                                    "rationale": "No official source attached",
                                    "do_not_apply_reason": "Missing evidence",
                                }
                            ]
                        },
                    }
                ),
                json.dumps(
                    {
                        "batch_index": 2,
                        "review_kind": "masterfile_collision",
                        "response": {
                            "reviews": [
                                {
                                    "listing_key": "ADX::AGIX",
                                    "ticker": "AGIX",
                                    "exchange": "ADX",
                                    "review_kind": "masterfile_collision",
                                    "decision_candidate": "possible_duplicate_or_cross_listing",
                                    "confidence": 0.8,
                                    "evidence_needed": "Listing status",
                                    "rationale": "Same ISIN",
                                    "do_not_apply_reason": "Manual identity review required",
                                }
                            ]
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    payload = build_payload(raw)

    assert payload["summary"]["rows"] == 2
    assert payload["summary"]["review_kind_totals"] == {"masterfile_collision": 1, "otc_scope": 1}
    assert payload["summary"]["decision_totals"] == {
        "needs_official_evidence": 1,
        "possible_duplicate_or_cross_listing": 1,
    }
    assert payload["errors"] == []


def test_render_markdown_marks_deepseek_as_triage_only(tmp_path) -> None:
    raw = tmp_path / "raw.jsonl"
    raw.write_text(
        json.dumps(
            {
                "batch_index": 1,
                "review_kind": "weak_sector",
                "response": {
                    "reviews": [
                        {
                            "listing_key": "NGX::ABC",
                            "ticker": "ABC",
                            "exchange": "NGX",
                            "review_kind": "weak_sector",
                            "decision_candidate": "keep_source_gap",
                            "confidence": 0.2,
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    markdown = render_markdown(build_payload(raw))

    assert "triage only" in markdown
    assert "| weak_sector | keep_source_gap | 1 |" in markdown
    assert "does not authorize data application" in markdown
