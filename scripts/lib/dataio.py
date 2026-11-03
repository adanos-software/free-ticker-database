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
COVERAGE_METADATA_FIELDS = frozenset({"etf_category", "sector", "stock_sector"})
# A reviewed `overwrite` replaces a non-blank value. It is limited to the ETF
# category of ETF rows; every other coverage update only fills blanks.
METADATA_OVERWRITE_FIELDS = frozenset({"etf_category"})
METADATA_OVERWRITE_ASSET_TYPES = frozenset({"ETF"})


def load_metadata_field_overrides(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    return {
        (row["ticker"], row["exchange"], row["field"]): row
        for row in load_csv(path)
        if is_well_formed_metadata_update(row)
    }


def reviewed_metadata_backfill_blocked(
    overrides: dict[tuple[str, str, str], dict[str, str]],
    *,
    ticker: str,
    exchange: str,
    field: str,
) -> bool:
    """Automated backfills must not overwrite an explicit reviewed clear."""
    row = overrides.get((ticker, exchange, field))
    return bool(row and row.get("decision") == "clear")


def blocked_metadata_backfill_fields(
    overrides: dict[tuple[str, str, str], dict[str, str]],
    *,
    ticker: str,
    exchange: str,
) -> frozenset[str]:
    fields = ("isin", "stock_sector", "etf_category", "country", "country_code")
    return frozenset(
        field
        for field in fields
        if reviewed_metadata_backfill_blocked(overrides, ticker=ticker, exchange=exchange, field=field)
    )


def should_apply_metadata_field_override(
    field: str,
    current_value: object,
    decision: str,
    *,
    asset_type: str = "",
) -> bool:
    if decision == "overwrite":
        return field in METADATA_OVERWRITE_FIELDS and asset_type in METADATA_OVERWRITE_ASSET_TYPES
    if field not in COVERAGE_METADATA_FIELDS:
        return True
    if decision == "clear":
        return True
    if decision != "update":
        return False
    current = current_value if isinstance(current_value, str) else ""
    return not current.strip()


def is_well_formed_metadata_update(row: dict[Any, Any]) -> bool:
    if row.get(None):
        return False
    if not all(row.get(field) for field in METADATA_UPDATE_REQUIRED_FIELDS):
        return False
    if row.get("decision") == "overwrite" and (
        row.get("field") not in METADATA_OVERWRITE_FIELDS or not row.get("proposed_value")
    ):
        return False
    return row.get("field") in METADATA_UPDATE_ALLOWED_TARGET_FIELDS


def merge_metadata_updates(
    path: Path,
    updates: list[dict[str, str]],
    *,
    replace_clears: bool = False,
) -> None:
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
        key = (update["ticker"], update["exchange"], update["field"])
        existing = by_key.get(key)
        if (
            not replace_clears
            and existing
            and existing.get("decision") in {"clear", "overwrite"}
            and update.get("decision") == "update"
        ):
            continue
        by_key[key] = {field: update.get(field, "") for field in METADATA_UPDATE_FIELDS}
    merged_rows = sorted(by_key.values(), key=lambda row: (row["ticker"], row["exchange"], row["field"]))
    write_csv(path, METADATA_UPDATE_FIELDS, merged_rows)
