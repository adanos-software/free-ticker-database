from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scripts.lib.review_adjudications import FIELDS, keep_listing_keys_by_isin, validate_rows


def valid_row() -> dict[str, str]:
    return {
        "isin": "US0378331005",
        "listing_key": "NASDAQ::AAPL",
        "decision": "keep",
        "evidence_source_key": "nasdaq_listed",
        "evidence_url": "https://example.test/evidence",
        "reviewed_at": "2026-08-17T00:00:00Z",
        "reviewer": "reviewer",
        "reason": "Exact listing-keyed official evidence.",
    }


def test_valid_adjudication_is_loaded(tmp_path: Path) -> None:
    path = tmp_path / "adjudications.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerow(valid_row())
    assert keep_listing_keys_by_isin(path) == {"US0378331005": {"NASDAQ::AAPL"}}


def test_missing_evidence_or_unknown_decision_is_rejected() -> None:
    row = valid_row()
    row["evidence_url"] = ""
    with pytest.raises(ValueError, match="misses"):
        validate_rows([row])
    row = valid_row()
    row["decision"] = "clear"
    with pytest.raises(ValueError, match="unsupported"):
        validate_rows([row])


def test_duplicate_adjudication_is_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        validate_rows([valid_row(), valid_row()])


def test_http_evidence_url_is_rejected() -> None:
    row = valid_row()
    row["evidence_url"] = "http://example.test/evidence"
    with pytest.raises(ValueError, match="HTTPS"):
        validate_rows([row])
