from __future__ import annotations

import json
from pathlib import Path

from scripts.build_adanos_alias_audit import build_report, write_json


def reference_row(ticker: str, name: str, aliases: list[str]) -> dict[str, str]:
    return {
        "ticker": ticker,
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": name,
        "aliases": json.dumps(aliases),
    }


def test_alias_audit_json_includes_release_traceability_metadata(tmp_path: Path) -> None:
    findings = build_report([reference_row("BANK", "Bank Holdings Inc", ["bank"])])
    json_out = tmp_path / "adanos_alias_audit.json"

    write_json(
        json_out,
        findings,
        "2026-05-24T00:00:00Z",
        Path("/repo/data/adanos/ticker_reference.csv"),
    )

    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["adanos_reference_csv"].endswith("ticker_reference.csv")
    assert "do not infer aliases from unverified names" in payload["_meta"]["policy"]
    assert payload["summary"]["rows"] == len(findings)
    assert payload["summary"]["blocking_issue_count"] == 1
    assert payload["findings"][0]["issue_type"] == "common_single_word_alias"
