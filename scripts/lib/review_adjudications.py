"""Strict loader for human-reviewed identifier adjudications."""

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

FIELDS = [
    "isin",
    "listing_key",
    "decision",
    "evidence_source_key",
    "evidence_url",
    "reviewed_at",
    "reviewer",
    "reason",
]
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def valid_isin(value: str) -> bool:
    value = value.strip().upper()
    if not _ISIN_RE.fullmatch(value):
        return False
    expanded = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in value)
    total = 0
    for index, digit in enumerate(reversed(expanded)):
        number = int(digit)
        if index % 2 == 1:
            number *= 2
        total += number // 10 + number % 10
    return total % 10 == 0


def _valid_timestamp(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    validated: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for line_number, source in enumerate(rows, 2):
        row = {field: str(source.get(field, "") or "").strip() for field in FIELDS}
        missing = [field for field in FIELDS if not row[field]]
        if missing:
            raise ValueError(
                f"identifier adjudication row {line_number} misses {', '.join(missing)}"
            )
        row["isin"] = row["isin"].upper()
        if not valid_isin(row["isin"]):
            raise ValueError(f"identifier adjudication row {line_number} has invalid ISIN")
        if "::" not in row["listing_key"]:
            raise ValueError(f"identifier adjudication row {line_number} has invalid listing_key")
        if row["decision"] != "keep":
            raise ValueError(
                f"identifier adjudication row {line_number} has unsupported decision {row['decision']!r}"
            )
        if not row["evidence_url"].startswith("https://"):
            raise ValueError(f"identifier adjudication row {line_number} requires an HTTPS evidence URL")
        if not _valid_timestamp(row["reviewed_at"]):
            raise ValueError(f"identifier adjudication row {line_number} has invalid reviewed_at")
        key = (row["isin"], row["listing_key"])
        if key in seen:
            raise ValueError(f"duplicate identifier adjudication for {key[0]} {key[1]}")
        seen.add(key)
        validated.append(row)
    return validated


def load(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FIELDS:
            raise ValueError(
                f"{path} header must be exactly {','.join(FIELDS)}"
            )
        return validate_rows(reader)


def keep_listing_keys_by_isin(path: Path) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for row in load(path):
        result.setdefault(row["isin"], set()).add(row["listing_key"])
    return result
