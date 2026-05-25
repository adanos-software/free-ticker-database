from __future__ import annotations

import json

from scripts.check_entry_quality_gate import check_entry_quality_gate, main


def row(listing_key: str, quality_status: str) -> dict[str, str]:
    return {"listing_key": listing_key, "quality_status": quality_status}


def test_entry_quality_gate_passes_allowed_warns():
    result = check_entry_quality_gate(
        [
            row("NASDAQ::MSFT", "pass"),
            row("OTC::AECX", "warn"),
        ],
        {"OTC::AECX"},
    )

    assert result["passed"] is True
    assert result["unexpected_warn_count"] == 0


def test_entry_quality_gate_fails_new_warns_and_quarantine():
    result = check_entry_quality_gate(
        [
            row("OTC::NEW", "warn"),
            row("NYSE::BAD", "quarantine"),
        ],
        {"OTC::OLD"},
    )

    assert result["passed"] is False
    assert result["unexpected_warns"] == ["OTC::NEW"]
    assert result["quarantined"] == ["NYSE::BAD"]
    assert result["stale_allowlist"] == ["OTC::OLD"]


def test_entry_quality_gate_cli_writes_json_report(tmp_path):
    entry_quality_csv = tmp_path / "entry_quality.csv"
    allowlist_csv = tmp_path / "allowlist.csv"
    json_out = tmp_path / "entry_quality_gate.json"
    entry_quality_csv.write_text(
        "listing_key,quality_status\nNASDAQ::MSFT,pass\nOTC::AECX,warn\n",
        encoding="utf-8",
    )
    allowlist_csv.write_text("listing_key\nOTC::AECX\n", encoding="utf-8")

    exit_code = main(
        [
            "--entry-quality-csv",
            str(entry_quality_csv),
            "--warn-allowlist-csv",
            str(allowlist_csv),
            "--json-out",
            str(json_out),
        ]
    )

    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["passed"] is True
    assert payload["warn_count"] == 1
    assert payload["_meta"]["generated_at"].endswith("Z")
    assert payload["_meta"]["policy"].startswith("Command evidence")
