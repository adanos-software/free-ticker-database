from __future__ import annotations

import argparse
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.lib.dataio import display_path, load_csv, write_csv, write_json
    from scripts.lib.keys import listing_key, row_listing_key
    from scripts.lib.normalize import normalize_bool, normalize_symbol
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import display_path, load_csv, write_csv, write_json
    from lib.keys import listing_key, row_listing_key
    from lib.normalize import normalize_bool, normalize_symbol


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
LISTING_INDEX_CSV = DATA_DIR / "listing_index.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
MASTERFILE_SUPPLEMENTAL_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
SYMBOL_CHANGES_CSV = DATA_DIR / "corporate_actions" / "symbol_changes.csv"
REPORT_JSON = DATA_DIR / "reports" / "symbol_changes_apply.json"
REPORT_MD = DATA_DIR / "reports" / "symbol_changes_apply.md"

US_LISTED_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
REKEY_FILES = [LISTINGS_CSV, LISTING_INDEX_CSV, IDENTIFIERS_EXTENDED_CSV, MASTERFILE_SUPPLEMENTAL_CSV]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def active_official_reference_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row.get("listing_status") == "active"
        and normalize_bool(row.get("official", ""))
        and row.get("reference_scope") == "exchange_directory"
    ]


def lookup_by_exchange_symbol(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (row.get("exchange", ""), normalize_symbol(row.get("ticker", "")))
        if key[0] and key[1]:
            lookup.setdefault(key, []).append(row)
    return lookup


def has_shell_or_transition_name(value: str) -> bool:
    tokens = normalize_symbol(value.replace("-", " "))
    return any(marker in tokens for marker in ("SPAC", "ACQUISITION", "MERGER", "UNIT", "RIGHT", "WARRANT"))


def classify_rename_candidate(
    change: dict[str, str],
    *,
    listings: list[dict[str, str]],
    reference_lookup: dict[tuple[str, str], list[dict[str, str]]],
) -> tuple[str, dict[str, str]]:
    old_symbol = normalize_symbol(change.get("old_symbol", ""))
    new_symbol = normalize_symbol(change.get("new_symbol", ""))
    if not old_symbol or not new_symbol:
        return "blocked_missing_symbol", {}
    if change.get("source_exchange_hint") != "US_LISTED":
        return "manual_non_us_or_unscoped_source", {}
    if has_shell_or_transition_name(change.get("new_company_name", "")):
        return "manual_transition_or_shell_name", {}

    old_matches = [
        row
        for row in listings
        if normalize_symbol(row.get("ticker", "")) == old_symbol
        and row.get("exchange") in US_LISTED_EXCHANGES
    ]
    new_matches = [row for row in listings if normalize_symbol(row.get("ticker", "")) == new_symbol]
    if len(old_matches) != 1:
        return "blocked_old_symbol_not_unique_in_us_scope", {}
    if new_matches:
        return "blocked_new_symbol_collision", {}

    old_listing = old_matches[0]
    exchange = old_listing.get("exchange", "")
    old_key = row_listing_key(old_listing)
    new_key = listing_key(exchange, new_symbol)
    if any(row_listing_key(row) == new_key for row in listings):
        return "blocked_listing_key_collision", {}

    old_isin = old_listing.get("isin", "").strip()
    if not old_isin:
        return "manual_missing_isin", {}

    new_reference_rows = reference_lookup.get((exchange, new_symbol), [])
    if not new_reference_rows:
        return "blocked_new_symbol_not_active_in_official_master", {}
    same_isin_rows = [row for row in new_reference_rows if row.get("isin", "").strip() == old_isin]
    if not same_isin_rows:
        return "manual_isin_not_proven_unchanged", {}

    if reference_lookup.get((exchange, old_symbol)):
        return "blocked_old_symbol_still_active_in_official_master", {}

    evidence = same_isin_rows[0]
    return "apply", {
        "exchange": exchange,
        "old_symbol": old_symbol,
        "new_symbol": new_symbol,
        "old_listing_key": old_key,
        "new_listing_key": new_key,
        "isin": old_isin,
        "source_key": evidence.get("source_key", ""),
        "source_url": evidence.get("source_url", ""),
        "evidence": "official_master_new_active_old_absent_same_exchange_same_isin",
    }


def rekey_rows(rows: list[dict[str, str]], accepted: list[dict[str, str]]) -> list[dict[str, str]]:
    by_old_key = {row["old_listing_key"]: row for row in accepted}
    by_exchange_symbol = {
        (row["exchange"], row["old_symbol"]): row
        for row in accepted
    }
    updated: list[dict[str, str]] = []
    for row in rows:
        current_key = row_listing_key(row)
        rename = by_old_key.get(current_key)
        if rename is None:
            rename = by_exchange_symbol.get((row.get("exchange", ""), normalize_symbol(row.get("ticker", ""))))
        if rename is None:
            updated.append(row)
            continue
        next_row = dict(row)
        if "ticker" in next_row:
            next_row["ticker"] = rename["new_symbol"]
        if "listing_key" in next_row:
            next_row["listing_key"] = rename["new_listing_key"]
        if "aliases" in next_row:
            next_row["aliases"] = add_alias(next_row.get("aliases", ""), rename["old_symbol"])
        updated.append(next_row)
    return updated


def add_alias(value: str, alias: str) -> str:
    alias = alias.strip()
    if not alias:
        return value
    parts = [part.strip() for part in value.split("|") if part.strip()]
    normalized = {part.upper() for part in parts}
    if alias.upper() not in normalized:
        parts.append(alias)
    return "|".join(parts)


def apply_symbol_changes(
    *,
    changes_csv: Path = SYMBOL_CHANGES_CSV,
    listings_csv: Path = LISTINGS_CSV,
    listing_index_csv: Path = LISTING_INDEX_CSV,
    identifiers_extended_csv: Path = IDENTIFIERS_EXTENDED_CSV,
    supplemental_csv: Path = MASTERFILE_SUPPLEMENTAL_CSV,
    reference_csv: Path = MASTERFILE_REFERENCE_CSV,
    report_json: Path = REPORT_JSON,
    report_md: Path = REPORT_MD,
    dry_run: bool = False,
) -> dict[str, Any]:
    changes = load_csv(changes_csv)
    listings = load_csv(listings_csv)
    active_reference = active_official_reference_rows(load_csv(reference_csv))
    reference_lookup = lookup_by_exchange_symbol(active_reference)

    accepted: list[dict[str, str]] = []
    blocked: list[dict[str, str]] = []
    seen_old_keys: set[str] = set()
    for change in changes:
        status, evidence = classify_rename_candidate(
            change,
            listings=listings,
            reference_lookup=reference_lookup,
        )
        row = {
            "change_id": change.get("change_id", ""),
            "effective_date": change.get("effective_date", ""),
            "old_symbol": normalize_symbol(change.get("old_symbol", "")),
            "new_symbol": normalize_symbol(change.get("new_symbol", "")),
            "new_company_name": change.get("new_company_name", ""),
            "status": status,
        }
        if status == "apply":
            if evidence["old_listing_key"] in seen_old_keys:
                blocked.append({**row, **evidence, "status": "blocked_duplicate_old_listing_key_candidate"})
                continue
            seen_old_keys.add(evidence["old_listing_key"])
            accepted.append({**row, **evidence})
        else:
            blocked.append(row)

    if accepted and not dry_run:
        for path in [listings_csv, listing_index_csv, identifiers_extended_csv, supplemental_csv]:
            rows = load_csv(path)
            if not rows:
                continue
            write_csv(path, list(rows[0].keys()), rekey_rows(rows, accepted))

    summary = {
        "generated_at": utc_now_iso(),
        "dry_run": dry_run,
        "changes_csv": display_path(changes_csv, ROOT),
        "reference_csv": display_path(reference_csv, ROOT),
        "accepted_rows": len(accepted),
        "blocked_rows": len(blocked),
        "blocked_by_status": dict(sorted(Counter(row["status"] for row in blocked).items())),
    }
    report = {"summary": summary, "accepted": accepted, "blocked": blocked}
    write_json(report_json, report)
    write_markdown(report_md, report)
    set_github_output("symbol_changes_applied", "true" if accepted and not dry_run else "false")
    set_github_output("accepted_rows", str(len(accepted)))
    return report


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    summary = report["summary"]
    lines = [
        "# Symbol Changes Apply",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Dry run: `{str(summary['dry_run']).lower()}`",
        f"- Accepted rows: `{summary['accepted_rows']}`",
        f"- Blocked/manual rows: `{summary['blocked_rows']}`",
        "",
        "## Accepted",
        "",
    ]
    if report["accepted"]:
        lines.extend(["| Old key | New key | ISIN | Evidence |", "|---|---|---|---|"])
        for row in report["accepted"]:
            lines.append(
                f"| {row['old_listing_key']} | {row['new_listing_key']} | {row['isin']} | {row['evidence']} |"
            )
    else:
        lines.append("No rename rows satisfied the official-evidence apply gate.")
    lines.extend(["", "## Blocked / Manual", "", "| Status | Rows |", "|---|---:|"])
    for status, count in summary["blocked_by_status"].items():
        lines.append(f"| {status} | {count} |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def set_github_output(key: str, value: str) -> None:
    output_path = Path(os.environ["GITHUB_OUTPUT"]) if "GITHUB_OUTPUT" in os.environ else None
    if output_path is None:
        return
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{key}={value}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply high-confidence, officially verified US ticker renames.")
    parser.add_argument("--changes-csv", type=Path, default=SYMBOL_CHANGES_CSV)
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--listing-index-csv", type=Path, default=LISTING_INDEX_CSV)
    parser.add_argument("--identifiers-extended-csv", type=Path, default=IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--supplemental-csv", type=Path, default=MASTERFILE_SUPPLEMENTAL_CSV)
    parser.add_argument("--reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--report-json", type=Path, default=REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=REPORT_MD)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    apply_symbol_changes(
        changes_csv=args.changes_csv,
        listings_csv=args.listings_csv,
        listing_index_csv=args.listing_index_csv,
        identifiers_extended_csv=args.identifiers_extended_csv,
        supplemental_csv=args.supplemental_csv,
        reference_csv=args.reference_csv,
        report_json=args.report_json,
        report_md=args.report_md,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
