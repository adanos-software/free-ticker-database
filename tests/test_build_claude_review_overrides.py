from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_build_claude_review_overrides_filters_by_confidence_and_dedupes(tmp_path):
    raw = tmp_path / "raw.jsonl"
    raw.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ticker": "AAA",
                        "exchange": "NASDAQ",
                        "status": "ok",
                        "response": {
                            "entry_decision": "fix_metadata",
                            "confidence": 0.85,
                            "alias_actions": [
                                {"alias": "bad alias", "decision": "remove", "reason": "bad"},
                            ],
                            "metadata_actions": [
                                {"field": "country", "decision": "update", "proposed_value": "United States", "reason": "fix"},
                                {"field": "isin", "decision": "clear", "reason": "wrong"},
                            ],
                        },
                    }
                ),
                json.dumps(
                    {
                        "ticker": "AAA",
                        "exchange": "NASDAQ",
                        "status": "ok",
                        "response": {
                            "entry_decision": "drop_entry",
                            "confidence": 0.9,
                            "alias_actions": [
                                {"alias": "bad alias", "decision": "remove", "reason": "bad again"},
                            ],
                            "metadata_actions": [],
                            "summary": "drop it",
                        },
                    }
                ),
                json.dumps(
                    {
                        "ticker": "BBB",
                        "exchange": "NYSE",
                        "status": "ok",
                        "response": {
                            "entry_decision": "fix_metadata",
                            "confidence": 0.4,
                            "alias_actions": [
                                {"alias": "ignore me", "decision": "remove", "reason": "low confidence"},
                            ],
                            "metadata_actions": [],
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "review_overrides"
    remove_aliases = output_dir / "remove_aliases.csv"
    metadata_updates = output_dir / "metadata_updates.csv"
    drop_entries = output_dir / "drop_entries.csv"
    summary = output_dir / "summary.json"

    completed = subprocess.run(
        [
            "python3",
            "scripts/build_claude_review_overrides.py",
            "--raw-responses",
            str(raw),
            "--output-dir",
            str(output_dir),
            "--remove-aliases-out",
            str(remove_aliases),
            "--metadata-updates-out",
            str(metadata_updates),
            "--drop-entries-out",
            str(drop_entries),
            "--summary-out",
            str(summary),
            "--min-confidence",
            "0.8",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert read_csv(remove_aliases) == [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "alias": "bad alias",
            "confidence": "0.9",
            "reason": "bad again",
        }
    ]
    assert read_csv(metadata_updates) == [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "country",
            "decision": "update",
            "proposed_value": "United States",
            "confidence": "0.85",
            "reason": "fix",
        },
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "field": "isin",
            "decision": "clear",
            "proposed_value": "",
            "confidence": "0.85",
            "reason": "wrong",
        },
    ]
    assert read_csv(drop_entries) == [
        {
            "ticker": "AAA",
            "exchange": "NASDAQ",
            "confidence": "0.9",
            "reason": "drop it",
        }
    ]
    summary_payload = json.loads(summary.read_text(encoding="utf-8"))
    assert summary_payload["counts"] == {
        "remove_aliases": 1,
        "metadata_updates": 2,
        "drop_entries": 1,
    }
