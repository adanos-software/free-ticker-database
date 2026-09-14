"""Apply listing-keyed official ISIN replacements when identity is stable.

Official directory ISIN changes draft rotation unless they are either
directory-only (no listing) or evidenced. This lane writes metadata_updates for
the ALNRG-shaped case: same listing_key, listing still has the previous official
ISIN, the new official ISIN is valid, and the listing name still matches the
current official name.

Name changes, sector recodes, and ticker-reuse pairs stay fail-closed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from scripts.build_entry_quality_report import names_match
    from scripts.lib.dataio import load_csv, merge_metadata_updates
    from scripts.rebuild_dataset import is_valid_isin
except ModuleNotFoundError:  # pragma: no cover
    from build_entry_quality_report import names_match
    from lib.dataio import load_csv, merge_metadata_updates
    from rebuild_dataset import is_valid_isin

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_ROTATION_DIFF = DATA_DIR / "reports" / "masterfile_rotation_diff.json"
DEFAULT_LISTINGS = DATA_DIR / "listings.csv"
DEFAULT_REFERENCE = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_METADATA_UPDATES = DATA_DIR / "review_overrides" / "metadata_updates.csv"
IDENTITY_BLOCKING_FIELDS = {
    "asset_type",
    "listing_status",
    "official",
    "reference_scope",
    "sector",
}


def _listing_key(exchange: str, ticker: str) -> str:
    return f"{exchange}::{ticker}"


def _official_reference_row(
    reference_rows: list[dict[str, str]],
    *,
    exchange: str,
    ticker: str,
    source_key: str,
    isin: str,
) -> dict[str, str] | None:
    matches: list[dict[str, str]] = []
    for row in reference_rows:
        if row.get("exchange") != exchange or row.get("ticker") != ticker:
            continue
        if str(row.get("listing_status") or "").strip() not in {"", "active"}:
            continue
        if str(row.get("official") or "").strip().lower() != "true":
            continue
        if str(row.get("reference_scope") or "").strip() != "exchange_directory":
            continue
        if source_key and str(row.get("source_key") or "").strip() != source_key:
            continue
        if str(row.get("isin") or "").strip().upper() != isin:
            continue
        matches.append(row)
    if len(matches) != 1:
        return None
    return matches[0]


def build_official_isin_updates(
    rotation_diff: dict[str, Any],
    listings: list[dict[str, str]],
    reference_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    listing_by_key = {
        str(row.get("listing_key") or _listing_key(row.get("exchange", ""), row.get("ticker", ""))): row
        for row in listings
        if row.get("exchange") and row.get("ticker")
    }
    updates: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rotation_diff.get("changed") or []:
        if not isinstance(row, dict) or not isinstance(row.get("changes"), dict):
            continue
        changes = row["changes"]
        if "isin" not in changes:
            continue
        if IDENTITY_BLOCKING_FIELDS.intersection(changes):
            continue
        isin_change = changes["isin"]
        if not isinstance(isin_change, dict):
            continue
        before = str(isin_change.get("before") or "").strip().upper()
        after = str(isin_change.get("after") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip()
        ticker = str(row.get("ticker") or "").strip()
        source_key = str(row.get("source_key") or "").strip()
        source_url = str(row.get("source_url") or "").strip()
        if not exchange or not ticker or not before or not is_valid_isin(after):
            continue
        listing = listing_by_key.get(_listing_key(exchange, ticker))
        if listing is None:
            continue
        current_isin = str(listing.get("isin") or "").strip().upper()
        if current_isin != before or current_isin == after:
            continue
        official_row = _official_reference_row(
            reference_rows,
            exchange=exchange,
            ticker=ticker,
            source_key=source_key,
            isin=after,
        )
        if official_row is None:
            continue
        official_name = str(official_row.get("name") or "").strip()
        listing_name = str(listing.get("name") or "").strip()
        if not listing_name or not official_name or not names_match(listing_name, official_name):
            continue
        key = (ticker, exchange)
        if key in seen:
            continue
        seen.add(key)
        reason = (
            f"Official {source_key or 'exchange directory'} ISIN replacement for "
            f"{_listing_key(exchange, ticker)}: {before} -> {after}. "
            "Listing name still matches the current official directory name; "
            "same listing_key identity is retained."
        )
        evidence_url = source_url or str(official_row.get("source_url") or "").strip()
        if evidence_url:
            reason = f"{reason} Source: {evidence_url}"
        updates.append(
            {
                "ticker": ticker,
                "exchange": exchange,
                "field": "isin",
                "decision": "update",
                "proposed_value": after,
                "confidence": "0.99",
                "reason": reason,
            }
        )
    return updates


def apply_official_identity_updates(
    *,
    rotation_diff: dict[str, Any],
    listings: list[dict[str, str]],
    reference_rows: list[dict[str, str]],
    metadata_updates_path: Path,
    execute: bool,
) -> dict[str, Any]:
    updates = build_official_isin_updates(rotation_diff, listings, reference_rows)
    if execute and updates:
        merge_metadata_updates(metadata_updates_path, updates)
    return {
        "candidate_count": len(updates),
        "executed": bool(execute and updates),
        "updates": updates,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rotation-diff", type=Path, default=DEFAULT_ROTATION_DIFF)
    parser.add_argument("--listings", type=Path, default=DEFAULT_LISTINGS)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--metadata-updates", type=Path, default=DEFAULT_METADATA_UPDATES)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write accepted ISIN replacements to metadata_updates.csv.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rotation_diff = json.loads(args.rotation_diff.read_text(encoding="utf-8"))
    result = apply_official_identity_updates(
        rotation_diff=rotation_diff,
        listings=load_csv(args.listings),
        reference_rows=load_csv(args.reference),
        metadata_updates_path=args.metadata_updates,
        execute=args.execute,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
