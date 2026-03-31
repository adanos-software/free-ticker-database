from __future__ import annotations

import csv
from pathlib import Path

from scripts.enrich_isins import extract_isin, parse_args, validate_isin_checksum, write_rows_atomic


def test_validate_isin_checksum_accepts_valid_and_rejects_invalid_values():
    assert validate_isin_checksum("US0378331005") is True
    assert validate_isin_checksum("US0378331006") is False
    assert validate_isin_checksum("not-an-isin") is False


def test_extract_isin_returns_first_valid_isin():
    result = {
        "data": [
            {"idIsin": "invalid"},
            {"idIsin": "US0378331005"},
            {"idIsin": "US5949181045"},
        ]
    }

    assert extract_isin(result) == "US0378331005"
    assert extract_isin({"error": "missing"}) is None


def test_parse_args_handles_apply_and_exchange_flags():
    args = parse_args(["--apply", "--exchange", "NASDAQ"])

    assert args.apply is True
    assert args.exchange == "NASDAQ"


def test_write_rows_atomic_replaces_target_file(tmp_path: Path):
    path = tmp_path / "tickers.csv"
    fieldnames = ["ticker", "isin"]
    rows = [{"ticker": "AAPL", "isin": "US0378331005"}]

    write_rows_atomic(path, fieldnames, rows)

    with path.open(newline="", encoding="utf-8") as handle:
        written_rows = list(csv.DictReader(handle))

    assert written_rows == rows
