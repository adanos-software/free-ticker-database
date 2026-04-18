from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_adanos_detection import (
    build_alias_index,
    build_simulation_report,
    detect_tickers,
    write_json,
    write_markdown,
)


def reference_row(ticker: str, aliases: list[str]) -> dict[str, str]:
    return {
        "ticker": ticker,
        "name": f"{ticker} Corp",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "sector": "Information Technology",
        "country": "United States",
        "country_code": "US",
        "isin": "US5949181045",
        "aliases": json.dumps(aliases),
    }


def test_detection_uses_word_boundaries_and_normalized_aliases():
    alias_index = build_alias_index([reference_row("MSFT", ["Microsoft"])])

    assert detect_tickers("Microsoft reported earnings.", alias_index) == [
        {"ticker": "MSFT", "alias": "microsoft"}
    ]
    assert detect_tickers("The micromicrosoftian token should not match.", alias_index) == []


def test_simulation_report_counts_positive_misses_and_negative_hits():
    report = build_simulation_report(
        reference_rows=[reference_row("MSFT", ["Microsoft"]), reference_row("BANK", ["bank"])],
        positive_probes=[
            {"id": "hit", "text": "Microsoft rallied.", "expected_ticker": "MSFT"},
            {"id": "miss", "text": "No known issuer here.", "expected_ticker": "TSLA"},
        ],
        negative_probes=[{"id": "negative_bank", "text": "The bank held rates steady."}],
        generated_at="2026-04-18T00:00:00Z",
    )

    assert report["summary"]["positive_misses"] == 1
    assert report["summary"]["negative_hits"] == 1
    assert report["positive_misses"][0]["id"] == "miss"
    assert report["negative_hits"][0]["matches"] == [{"ticker": "BANK", "alias": "bank"}]


def test_simulation_report_writers(tmp_path: Path):
    report = build_simulation_report(
        reference_rows=[reference_row("MSFT", ["Microsoft"])],
        positive_probes=[{"id": "hit", "text": "Microsoft rallied.", "expected_ticker": "MSFT"}],
        negative_probes=[{"id": "negative", "text": "The market closed higher."}],
        generated_at="2026-04-18T00:00:00Z",
    )
    json_out = tmp_path / "simulation.json"
    md_out = tmp_path / "simulation.md"

    write_json(json_out, report)
    write_markdown(md_out, report)

    assert json.loads(json_out.read_text())["summary"]["positive_misses"] == 0
    assert "Negative Hits" in md_out.read_text()
