"""Classify vanished official-reference rows without dropping listings.

Rotation diffs mark rows that left an official feed. Absence from a feed is not
delisting evidence: ticker reuse, rights lines, dual-currency strings, and
refresh-only extras all look the same. This report joins vanished rows to
current listings and the weekly delisting report, then reuses the delisting
classifier. Drop overrides stay on ``scripts/apply_delistings.py``.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.apply_delistings import classify_candidate
    from scripts.lib.dataio import display_path, load_csv, read_json, write_json
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from apply_delistings import classify_candidate
    from lib.dataio import display_path, load_csv, read_json, write_json


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_ROTATION_DIFF_JSON = DATA_DIR / "reports" / "masterfile_rotation_diff.json"
DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_DELISTING_REPORT_JSON = DATA_DIR / "reports" / "delisting_report.json"
DEFAULT_REPORT_JSON = DATA_DIR / "reports" / "masterfile_vanished_delisting_review.json"
DEFAULT_REPORT_MD = DATA_DIR / "reports" / "masterfile_vanished_delisting_review.md"
POLICY = "feed_delisting_classifier_not_direct_deletion"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def listing_lookup_key(exchange: str, ticker: str) -> tuple[str, str]:
    """Presence key for listings.csv, not instrument identity.

    A vanished feed row is still in the database when the listing key exists,
    including official ISIN recodes on the same ticker. Delisting evidence must
    not use this key alone; ``matching_delisting_candidate`` requires the ISIN.
    """
    return exchange, ticker


def vanished_identity(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("source_key", "")),
        str(row.get("exchange", "")),
        str(row.get("ticker", "")),
    )


def listings_by_key(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {listing_lookup_key(row.get("exchange", ""), row.get("ticker", "")): row for row in rows}


def delisting_candidates_by_key(report: dict[str, Any]) -> dict[tuple[str, str], list[dict[str, str]]]:
    out: dict[tuple[str, str], list[dict[str, str]]] = {}
    for candidate in report.get("candidates", []):
        key = listing_lookup_key(str(candidate.get("exchange", "")), str(candidate.get("ticker", "")))
        out.setdefault(key, []).append(candidate)
    return out


def listing_isin(listing: dict[str, str] | None) -> str:
    if listing is None:
        return ""
    return str(listing.get("isin", "")).strip()


def matching_delisting_candidate(
    candidates: list[dict[str, str]],
    *,
    listing: dict[str, str] | None,
) -> dict[str, str] | None:
    """Bind weekly delisting evidence to the current listing ISIN, not ticker reuse."""
    isin = listing_isin(listing)
    if not isin:
        return None
    matches = [candidate for candidate in candidates if str(candidate.get("isin", "")).strip() == isin]
    if not matches:
        return None
    for candidate in matches:
        if classify_candidate(candidate) == "apply_drop_override":
            return candidate
    return matches[0]


def reference_from_previous(row: dict[str, Any]) -> dict[str, str]:
    return {
        "source_key": str(row.get("source_key", "")),
        "source_url": str(row.get("source_url", "")),
        "ticker": str(row.get("ticker", "")),
        "exchange": str(row.get("exchange", "")),
        "name": str(row.get("name") or row.get("official_name") or ""),
        "isin": str(row.get("isin", "")),
    }


def collect_vanished_references(
    rotation_vanished: list[dict[str, Any]],
    rotation_new: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
) -> list[tuple[str, dict[str, str]]]:
    collected: list[tuple[str, dict[str, str]]] = []
    seen: set[tuple[str, str, str]] = set()
    reappeared = {vanished_identity(ref) for ref in rotation_new}
    for ref in rotation_vanished:
        identity = vanished_identity(ref)
        if identity in seen:
            continue
        seen.add(identity)
        collected.append(("rotation", {str(key): str(value or "") for key, value in ref.items()}))
    for previous in previous_rows:
        if not previous.get("still_in_database"):
            continue
        identity = vanished_identity(previous)
        if identity in seen or identity in reappeared:
            continue
        seen.add(identity)
        collected.append(("backlog", reference_from_previous(previous)))
    return collected


def classify_vanished_row(
    *,
    ref: dict[str, str],
    listing: dict[str, str] | None,
    delisting_candidates: list[dict[str, str]] | None,
    origin: str,
) -> dict[str, Any]:
    still_in_database = listing is not None
    matched = (
        matching_delisting_candidate(delisting_candidates or [], listing=listing)
        if still_in_database
        else None
    )
    if still_in_database:
        if matched:
            candidate = dict(matched)
        else:
            candidate = {
                "ticker": ref.get("ticker", ""),
                "exchange": ref.get("exchange", ""),
                "name": ref.get("name", ""),
                "isin": ref.get("isin", ""),
                "classification": "master_absent",
                "source_key": ref.get("source_key", ""),
                "source_url": ref.get("source_url", ""),
            }
        action = classify_candidate(candidate)
        classification = str(candidate.get("classification", "master_absent"))
    else:
        action = "not_in_database"
        classification = "master_absent"
    return {
        "ticker": ref.get("ticker", ""),
        "exchange": ref.get("exchange", ""),
        "name": ref.get("name", ""),
        "isin": ref.get("isin", ""),
        "classification": classification,
        "source_key": ref.get("source_key", ""),
        "source_url": ref.get("source_url", ""),
        "evidence_source_key": "" if matched is None else str(matched.get("source_key", "")),
        "evidence_source_url": "" if matched is None else str(matched.get("source_url", "")),
        "evidence_observation_id": "" if matched is None else str(matched.get("observation_id", "")),
        "evidence_observed_at": "" if matched is None else str(matched.get("observed_at", "")),
        "official_name": ref.get("name", ""),
        "listing_name": "" if listing is None else listing.get("name", ""),
        "listing_isin": "" if listing is None else listing.get("isin", ""),
        "still_in_database": still_in_database,
        "classifier_action": action,
        "would_apply_drop": action == "apply_drop_override",
        "origin": origin,
    }


def build_notes(rows: list[dict[str, Any]], *, rotation_count: int, backlog_count: int) -> list[str]:
    still = sum(1 for row in rows if row["still_in_database"])
    notes = [
        "Vanished official-reference rows are classified; listings are not dropped from this report.",
        f"Rotation vanished rows: {rotation_count}; still-in-database backlog carried: {backlog_count}.",
        f"Still in database: {still}; not in database: {len(rows) - still}.",
        "Applied drops from this classifier: 0.",
    ]
    if any(row["would_apply_drop"] for row in rows):
        notes.append(
            "Eligible official delisting evidence is reported only; "
            "scripts/apply_delistings.py remains the apply path."
        )
    return notes


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    counts = report["classifier_counts"]
    lines = [
        "# Masterfile Vanished Delisting Review",
        f"- Generated at: `{report['generated_at']}`",
        f"- Policy: `{report['policy']}`",
        f"- Vanished reference rows: `{report['vanished_reference_rows']}`",
        f"- Rotation vanished rows: `{report['rotation_vanished_rows']}`",
        f"- Backlog rows: `{report['backlog_rows']}`",
        f"- Still in database: `{report['still_in_database']}`",
        f"- Applied drops: `{report['applied_drops']}`",
        "",
        "## Classifier counts",
        "",
        "| Action | Rows |",
        "|---|---:|",
    ]
    for action, count in counts.items():
        lines.append(f"| {action} | {count} |")
    lines.extend(
        [
            "",
            "## Rows still in the database",
            "",
            "| Exchange | Ticker | Source | Action | Origin |",
            "|---|---|---|---|---|",
        ]
    )
    still_rows = [row for row in report["rows"] if row["still_in_database"]]
    still_rows.sort(key=lambda row: (row["exchange"], row["ticker"], row["source_key"]))
    for row in still_rows:
        lines.append(
            f"| {row['exchange']} | {row['ticker']} | {row['source_key']} | "
            f"{row['classifier_action']} | {row['origin']} |"
        )
    if not still_rows:
        lines.append("| | | | | |")
    lines.extend(["", "## Notes", ""])
    for note in report["notes"]:
        lines.append(f"- {note}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_review(
    *,
    rotation_diff_json: Path = DEFAULT_ROTATION_DIFF_JSON,
    listings_csv: Path = DEFAULT_LISTINGS_CSV,
    delisting_report_json: Path = DEFAULT_DELISTING_REPORT_JSON,
    previous_review_json: Path | None = None,
    report_json: Path = DEFAULT_REPORT_JSON,
    report_md: Path = DEFAULT_REPORT_MD,
) -> dict[str, Any]:
    diff = read_json(rotation_diff_json, default=None)
    if not isinstance(diff, dict):
        raise FileNotFoundError(f"rotation diff is missing or invalid: {rotation_diff_json}")
    previous_path = previous_review_json if previous_review_json is not None else report_json
    previous = read_json(previous_path, default={}) or {}
    rotation_vanished = list(diff.get("vanished", []))
    rotation_new = list(diff.get("new", []))
    collected = collect_vanished_references(
        rotation_vanished,
        rotation_new,
        list(previous.get("rows", [])),
    )
    listings = listings_by_key(load_csv(listings_csv))
    delisting_by_key = delisting_candidates_by_key(read_json(delisting_report_json, default={}) or {})

    rows = [
        classify_vanished_row(
            ref=ref,
            listing=listings.get(listing_lookup_key(ref.get("exchange", ""), ref.get("ticker", ""))),
            delisting_candidates=delisting_by_key.get(
                listing_lookup_key(ref.get("exchange", ""), ref.get("ticker", "")),
                [],
            ),
            origin=origin,
        )
        for origin, ref in collected
    ]
    rows.sort(key=lambda row: (row["exchange"], row["ticker"], row["source_key"], row["origin"]))
    backlog_count = sum(1 for origin, _ in collected if origin == "backlog")
    report = {
        "generated_at": utc_now_iso(),
        "policy": POLICY,
        "rotation_diff_json": display_path(rotation_diff_json, ROOT),
        "vanished_reference_rows": len(rows),
        "rotation_vanished_rows": len(rotation_vanished),
        "backlog_rows": backlog_count,
        "still_in_database": sum(1 for row in rows if row["still_in_database"]),
        "classifier_counts": dict(sorted(Counter(row["classifier_action"] for row in rows).items())),
        "applied_drops": 0,
        "notes": build_notes(
            rows,
            rotation_count=len(rotation_vanished),
            backlog_count=backlog_count,
        ),
        "rows": rows,
    }
    write_json(report_json, report)
    write_markdown(report_md, report)
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify vanished official-reference rows without dropping listings."
    )
    parser.add_argument("--rotation-diff-json", type=Path, default=DEFAULT_ROTATION_DIFF_JSON)
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--delisting-report-json", type=Path, default=DEFAULT_DELISTING_REPORT_JSON)
    parser.add_argument(
        "--previous-review-json",
        type=Path,
        default=None,
        help="Backlog source. Defaults to the output review JSON.",
    )
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build_review(
        rotation_diff_json=args.rotation_diff_json,
        listings_csv=args.listings_csv,
        delisting_report_json=args.delisting_report_json,
        previous_review_json=args.previous_review_json,
        report_json=args.report_json,
        report_md=args.report_md,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
