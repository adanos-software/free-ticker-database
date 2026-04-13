from __future__ import annotations

import json

from scripts import llm_plausibility_review as review
from scripts.llm_plausibility_review import (
    build_prompt,
    chunk_rows,
    compact_row,
    load_checkpoint,
    normalize_findings,
    parse_json_object,
    write_error_report,
)


def test_parse_json_object_accepts_fenced_and_embedded_json():
    assert parse_json_object('```json\n{"findings":[]}\n```') == {"findings": []}
    assert parse_json_object('text before {"findings": []} text after') == {"findings": []}


def test_compact_row_trims_aliases_and_keeps_review_fields():
    row = {
        "ticker": "AAA",
        "exchange": "NASDAQ",
        "name": "Example Inc",
        "aliases": "x" * 300,
        "ignored": "value",
    }
    compact = compact_row(row)
    assert compact["ticker"] == "AAA"
    assert compact["name"] == "Example Inc"
    assert compact["aliases"].endswith("...")
    assert "ignored" not in compact


def test_normalize_findings_filters_hallucinated_rows_and_defaults_fields():
    rows = [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "name": "Example Inc",
            "asset_type": "Stock",
            "isin": "US0000000001",
        }
    ]
    payload = {
        "findings": [
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "unknown_field",
                "severity": "critical",
                "issue": "Country conflicts with ISIN prefix.",
                "confidence": 2,
            },
            {
                "ticker": "BBB",
                "exchange": "NASDAQ",
                "field": "isin",
                "issue": "Hallucinated row.",
            },
        ]
    }
    findings = normalize_findings(payload, rows)
    assert len(findings) == 1
    assert findings[0]["field"] == "name"
    assert findings[0]["severity"] == "low"
    assert findings[0]["confidence"] == 1.0


def test_build_prompt_is_conservative_about_missing_fields():
    prompt = build_prompt(
        [
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "name": "Example Inc",
                "asset_type": "Stock",
                "isin": "",
                "sector": "",
            }
        ]
    )
    assert "Do NOT flag missing ISIN" in prompt
    assert '"ticker": "AAA"' in prompt
    entries = prompt.split("Entries:\n", maxsplit=1)[1]
    assert json.loads(entries)[0]["ticker"] == "AAA"


def test_chunk_rows_requires_positive_batch_size():
    rows = [{"ticker": str(index), "exchange": "X"} for index in range(5)]
    assert [len(chunk) for chunk in chunk_rows(rows, 2)] == [2, 2, 1]


def test_review_batch_with_fallback_splits_unparseable_batches(monkeypatch):
    rows = [
        {"ticker": "AAA", "exchange": "NASDAQ", "name": "Example A"},
        {"ticker": "BBB", "exchange": "NYSE", "name": "Example B"},
    ]
    call_sizes: list[int] = []

    def fake_call_ollama(**kwargs):
        entries = json.loads(kwargs["prompt"].split("Entries:\n", maxsplit=1)[1])
        call_sizes.append(len(entries))
        if len(entries) > 1:
            raise json.JSONDecodeError("bad json", "{", 0)
        return {"findings": []}

    monkeypatch.setattr(review, "call_ollama", fake_call_ollama)

    results = review.review_batch_with_fallback(
        rows,
        url="http://example.test",
        model="gemma4:26b",
        timeout_seconds=1,
        temperature=0,
        num_predict=256,
    )

    assert [len(batch) for batch, _ in results] == [1, 1]
    assert call_sizes == [2, 1, 1]


def test_review_batch_with_fallback_records_single_row_parse_errors(monkeypatch):
    rows = [{"ticker": "AAA", "exchange": "NASDAQ", "name": "Example A"}]

    def fake_call_ollama(**kwargs):
        raise json.JSONDecodeError("bad json", "{", 0)

    monkeypatch.setattr(review, "call_ollama", fake_call_ollama)

    results = review.review_batch_with_fallback(
        rows,
        url="http://example.test",
        model="gemma4:26b",
        timeout_seconds=1,
        temperature=0,
        num_predict=256,
    )

    assert results[0][0] == rows
    assert results[0][1][0]["_review_error"] is True
    assert results[0][1][0]["ticker"] == "AAA"


def test_load_checkpoint_marks_legacy_parse_findings_as_review_errors(tmp_path):
    checkpoint = tmp_path / "findings.jsonl"
    checkpoint.write_text(
        json.dumps(
            {
                "keys": [["AAA", "NASDAQ"]],
                "findings": [
                    {
                        "ticker": "AAA",
                        "exchange": "NASDAQ",
                        "field": "name",
                        "severity": "low",
                        "issue": "LLM response could not be parsed: bad json",
                        "suggestion": "Retry this row or inspect manually.",
                        "confidence": 0.0,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    processed, findings = load_checkpoint(checkpoint)

    assert processed == {("AAA", "NASDAQ")}
    assert findings[0]["_review_error"] is True


def test_write_error_report_excludes_legacy_parse_errors_from_data_findings(tmp_path):
    output = tmp_path / "error.txt"

    write_error_report(
        output,
        model="gemma4:26b",
        input_path=tmp_path / "tickers.csv",
        checkpoint_path=tmp_path / "findings.jsonl",
        reviewed_count=1,
        total_selected=1,
        findings=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "name",
                "severity": "low",
                "issue": "LLM response could not be parsed: bad json",
                "suggestion": "Retry this row or inspect manually.",
                "confidence": 0.0,
                "row": {"name": "Example A", "isin": ""},
            }
        ],
    )

    report = output.read_text(encoding="utf-8")
    assert "findings: 0" in report
    assert "review_errors: 1" in report
    assert "[REVIEW_ERROR] AAA NASDAQ" in report


def test_write_error_report_reconciles_stale_review_errors(tmp_path):
    output = tmp_path / "error.txt"

    write_error_report(
        output,
        model="gemma4:26b",
        input_path=tmp_path / "tickers.csv",
        checkpoint_path=tmp_path / "findings.jsonl",
        reviewed_count=1,
        total_selected=1,
        findings=[
            {
                "_review_error": True,
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "name",
                "severity": "low",
                "issue": "LLM response could not be parsed: bad json",
                "suggestion": "Retry this row or inspect manually.",
                "confidence": 0.0,
                "row": {"ticker": "AAA", "exchange": "NASDAQ", "name": "Example A", "isin": "AU0000000000"},
            }
        ],
        current_rows=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "name": "Example A",
                "isin": "US0000000001",
            }
        ],
    )

    report = output.read_text(encoding="utf-8")
    assert "review_errors: 0" in report
    assert "resolved_or_removed_review_errors: 1" in report
    assert "[REVIEW_ERROR] AAA NASDAQ" not in report


def test_write_error_report_reconciles_resolved_checkpoint_findings(tmp_path):
    output = tmp_path / "error.txt"

    write_error_report(
        output,
        model="gemma4:26b",
        input_path=tmp_path / "tickers.csv",
        checkpoint_path=tmp_path / "findings.jsonl",
        reviewed_count=1,
        total_selected=1,
        findings=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "isin",
                "severity": "medium",
                "issue": "Country conflicts with ISIN prefix.",
                "suggestion": "Check ISIN.",
                "confidence": 0.8,
                "row": {"isin": "AU0000000000", "name": "Example A"},
            }
        ],
        current_rows=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "isin": "US0000000001",
                "name": "Example A",
            }
        ],
    )

    report = output.read_text(encoding="utf-8")
    assert "findings: 0" in report
    assert "resolved_or_removed_findings: 1" in report
    assert "Country conflicts with ISIN prefix" not in report


def test_write_error_report_reconciles_legacy_name_field_isin_findings(tmp_path):
    output = tmp_path / "error.txt"

    write_error_report(
        output,
        model="gemma4:26b",
        input_path=tmp_path / "tickers.csv",
        checkpoint_path=tmp_path / "findings.jsonl",
        reviewed_count=1,
        total_selected=1,
        findings=[
            {
                "ticker": "AAA",
                "exchange": "NYSE",
                "field": "name",
                "severity": "high",
                "issue": "ISIN prefix mismatch: NYSE row has TH ISIN.",
                "suggestion": "Check ISIN.",
                "confidence": 0.8,
                "row": {"name": "Example A", "isin": "TH0000000000", "country": "Thailand"},
            }
        ],
        current_rows=[
            {
                "ticker": "AAA",
                "exchange": "NYSE",
                "name": "Example A",
                "isin": "US0000000001",
                "country": "United States",
            }
        ],
    )

    report = output.read_text(encoding="utf-8")
    assert "findings: 0" in report
    assert "resolved_or_removed_findings: 1" in report
    assert "ISIN prefix mismatch" not in report


def test_write_error_report_excludes_accepted_false_positives(tmp_path):
    output = tmp_path / "error.txt"

    write_error_report(
        output,
        model="gemma4:26b",
        input_path=tmp_path / "tickers.csv",
        checkpoint_path=tmp_path / "findings.jsonl",
        reviewed_count=1,
        total_selected=1,
        findings=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "field": "isin",
                "severity": "low",
                "issue": "LLM thinks this valid ISIN is suspicious.",
                "suggestion": "Check ISIN.",
                "confidence": 0.1,
                "row": {"isin": "US0000000001", "name": "Example A"},
            }
        ],
        current_rows=[
            {
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "isin": "US0000000001",
                "name": "Example A",
            }
        ],
        accepted_findings={("AAA", "NASDAQ", "isin", "US0000000001")},
    )

    report = output.read_text(encoding="utf-8")
    assert "findings: 0" in report
    assert "accepted_false_positive_findings: 1" in report
    assert "valid ISIN is suspicious" not in report
