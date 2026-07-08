from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def display_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


METADATA_UPDATE_FIELDS = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
METADATA_UPDATE_REQUIRED_FIELDS = ["ticker", "exchange", "field", "decision"]
METADATA_UPDATE_ALLOWED_TARGET_FIELDS = {
    "aliases",
    "asset_type",
    "country",
    "country_code",
    "etf_category",
    "exchange",
    "isin",
    "name",
    "stock_sector",
    "ticker",
}


def is_well_formed_metadata_update(row: dict[Any, Any]) -> bool:
    if row.get(None):
        return False
    if not all(row.get(field) for field in METADATA_UPDATE_REQUIRED_FIELDS):
        return False
    return row.get("field") in METADATA_UPDATE_ALLOWED_TARGET_FIELDS


def merge_metadata_updates(path: Path, updates: list[dict[str, str]]) -> None:
    rows = [
        {field: row.get(field, "") for field in METADATA_UPDATE_FIELDS}
        for row in load_csv(path)
        if is_well_formed_metadata_update(row)
    ]
    by_key: dict[tuple[str, str, str], dict[str, str]] = {
        (row["ticker"], row["exchange"], row["field"]): row
        for row in rows
    }
    for update in updates:
        by_key[(update["ticker"], update["exchange"], update["field"])] = update
    merged_rows = sorted(by_key.values(), key=lambda row: (row["ticker"], row["exchange"], row["field"]))
    write_csv(path, METADATA_UPDATE_FIELDS, merged_rows)
