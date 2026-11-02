from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.lib.dataio import load_csv, write_csv


def _normalize_etf_category(value: str) -> str:
    from scripts.rebuild_dataset import normalize_sector

    return normalize_sector(value, "ETF")

ETF_CATEGORY_OVERRIDE_FIELDS = [
    "ticker",
    "exchange",
    "old_value",
    "new_value",
    "reason",
    "source_1",
    "source_2",
]
ETF_CATEGORY_OVERRIDE_REQUIRED_FIELDS = [
    "ticker",
    "exchange",
    "old_value",
    "new_value",
    "reason",
    "source_1",
    "source_2",
]


def is_well_formed_etf_category_override(row: dict[Any, Any]) -> bool:
    if row.get(None):
        return False
    if not all(str(row.get(field) or "").strip() for field in ETF_CATEGORY_OVERRIDE_REQUIRED_FIELDS):
        return False
    for field in ("old_value", "new_value"):
        if _normalize_etf_category(str(row.get(field) or "")) != str(row.get(field) or "").strip():
            return False
    if str(row.get("old_value") or "").strip() == str(row.get("new_value") or "").strip():
        return False
    return True


def load_etf_category_overrides(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    overrides: dict[tuple[str, str], dict[str, str]] = {}
    for row in load_csv(path):
        if not is_well_formed_etf_category_override(row):
            continue
        key = (str(row["ticker"]).strip(), str(row["exchange"]).strip())
        overrides[key] = {field: str(row.get(field, "") or "").strip() for field in ETF_CATEGORY_OVERRIDE_FIELDS}
    return overrides


def apply_reviewed_etf_category_override(
    row: dict[str, str],
    override: dict[str, str] | None,
) -> tuple[dict[str, str], str | None]:
    """Apply a reviewed etf_category overwrite when guards pass.

    Returns (updated_row, skip_reason). skip_reason is set when the override does not apply.
    """
    if not override:
        return row, None
    if row.get("asset_type") != "ETF":
        return row, "non_etf"
    current = _normalize_etf_category(row.get("etf_category", "") or row.get("sector", ""))
    expected_old = _normalize_etf_category(override["old_value"])
    new_value = _normalize_etf_category(override["new_value"])
    if current != expected_old:
        return row, "stale_old_value"
    if current == new_value:
        return row, "already_correct"
    updated = dict(row)
    updated["etf_category"] = new_value
    updated["sector"] = new_value
    return updated, None


def stale_etf_category_override_keys(
    overrides: list[dict[str, str]],
    listings: list[dict[str, str]],
) -> list[str]:
    by_key = {(row.get("ticker", ""), row.get("exchange", "")): row for row in listings}
    stale: list[str] = []
    for override in overrides:
        if not is_well_formed_etf_category_override(override):
            continue
        key = (override["ticker"], override["exchange"])
        listing_key = f"{override['exchange']}::{override['ticker']}"
        listing = by_key.get(key)
        if not listing:
            stale.append(f"missing_listing|{listing_key}")
            continue
        if listing.get("asset_type") != "ETF":
            stale.append(f"non_etf|{listing_key}")
            continue
        current = _normalize_etf_category(listing.get("etf_category", "") or listing.get("sector", ""))
        expected_old = _normalize_etf_category(override["old_value"])
        expected_new = _normalize_etf_category(override["new_value"])
        if current == expected_new:
            continue
        if current == expected_old:
            stale.append(f"override_not_applied|{listing_key}|current={current}|expected_new={expected_new}")
            continue
        stale.append(
            f"stale_old_value|{listing_key}|current={current or 'none'}|expected_old={override['old_value']}|expected_new={expected_new}"
        )
    return stale


def malformed_etf_category_override_rows(overrides: list[dict[Any, Any]]) -> list[str]:
    invalid: list[str] = []
    for index, row in enumerate(overrides, start=2):
        if is_well_formed_etf_category_override(row):
            continue
        ticker = row.get("ticker") or ""
        exchange = row.get("exchange") or ""
        extra_columns = row.get(None) or []
        suffix = f" extra_columns={len(extra_columns)}" if extra_columns else ""
        invalid.append(f"row {index}: {exchange}::{ticker}{suffix}")
    return invalid


def merge_etf_category_overrides(path: Path, rows: list[dict[str, str]]) -> None:
    existing = [
        {field: row.get(field, "") for field in ETF_CATEGORY_OVERRIDE_FIELDS}
        for row in load_csv(path)
        if is_well_formed_etf_category_override(row)
    ]
    by_key = {(row["ticker"], row["exchange"]): row for row in existing}
    for row in rows:
        by_key[(row["ticker"], row["exchange"])] = {field: row.get(field, "") for field in ETF_CATEGORY_OVERRIDE_FIELDS}
    merged = sorted(by_key.values(), key=lambda row: (row["ticker"], row["exchange"]))
    write_csv(path, ETF_CATEGORY_OVERRIDE_FIELDS, merged)
