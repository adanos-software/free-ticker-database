from __future__ import annotations

import argparse
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.lib.dataio import display_path, load_csv, write_csv, write_json
    from scripts.lib.identity_integrity import names_refer_to_same_identity
    from scripts.lib.keys import listing_key, row_listing_key
    from scripts.lib.normalize import normalize_bool, normalize_symbol
    from scripts.lib.official_change_evidence import is_valid_isin
    from scripts.lib.us_exchange_directory import (
        active_directory_keys,
        load_live_us_directory_reference_rows,
        load_sec_exchange_payload,
        merge_us_directory_reference_rows,
        official_directory_row,
        sec_cik_mismatch,
        shared_sec_cik,
    )
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import display_path, load_csv, write_csv, write_json
    from lib.identity_integrity import names_refer_to_same_identity
    from lib.keys import listing_key, row_listing_key
    from lib.normalize import normalize_bool, normalize_symbol
    from lib.official_change_evidence import is_valid_isin
    from lib.us_exchange_directory import (
        active_directory_keys,
        load_live_us_directory_reference_rows,
        load_sec_exchange_payload,
        merge_us_directory_reference_rows,
        official_directory_row,
        sec_cik_mismatch,
        shared_sec_cik,
    )


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
LISTING_INDEX_CSV = DATA_DIR / "listing_index.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
MASTERFILE_SUPPLEMENTAL_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
LISTING_TRANSITIONS_CSV = DATA_DIR / "review_overrides" / "listing_transitions.csv"
DROP_ENTRIES_CSV = DATA_DIR / "review_overrides" / "drop_entries.csv"
SYMBOL_CHANGES_CSV = DATA_DIR / "corporate_actions" / "symbol_changes.csv"
REPORT_JSON = DATA_DIR / "reports" / "symbol_changes_apply.json"
REPORT_MD = DATA_DIR / "reports" / "symbol_changes_apply.md"
TRANSITION_FIELDS = [
    "old_listing_key",
    "new_listing_key",
    "event_type",
    "identity_type",
    "identity_value",
    "confidence",
    "source_key",
    "source_url",
    "reason",
]
DROP_FIELDS = ["ticker", "exchange", "confidence", "reason"]

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


def resolve_rename_identity(
    *,
    old_symbol: str,
    new_symbol: str,
    old_isin: str,
    sec_payload: dict[str, Any] | None,
) -> tuple[str, str, str]:
    """Return (identity_type, identity_value, block_reason). block_reason empty when ok."""

    old_isin = old_isin.strip().upper()
    if sec_cik_mismatch(old_symbol, new_symbol, sec_payload):
        return "", "", "blocked_sec_cik_mismatch"
    if is_valid_isin(old_isin):
        return "same_isin", old_isin, ""
    cik = shared_sec_cik(old_symbol, new_symbol, sec_payload)
    if cik:
        return "same_cik", cik, ""
    return "", "", "manual_missing_identity_evidence"


def build_verified_rename_evidence(
    *,
    exchange: str,
    old_symbol: str,
    new_symbol: str,
    old_listing_key: str,
    new_listing_key: str,
    old_isin: str,
    identity_type: str,
    identity_value: str,
    reference_rows: list[dict[str, str]],
    change: dict[str, str],
) -> dict[str, str]:
    directory_row = official_directory_row(reference_rows, exchange, new_symbol)
    source_key = directory_row.get("source_key", "") if directory_row else "nasdaq_listed"
    source_url = directory_row.get("source_url", "") if directory_row else ""
    new_official_name = (directory_row or {}).get("name", "").strip()
    return {
        "exchange": exchange,
        "old_symbol": old_symbol,
        "new_symbol": new_symbol,
        "old_listing_key": old_listing_key,
        "new_listing_key": new_listing_key,
        "isin": old_isin.strip().upper(),
        "identity_type": identity_type,
        "identity_value": identity_value,
        "transition_event_type": "symbol_changed",
        "source_key": source_key,
        "source_url": source_url,
        "new_official_name": new_official_name,
        "evidence": "live_official_directory_old_inactive_new_active_same_issuer",
        "feed_source_key": change.get("source", "stockanalysis_symbol_changes"),
        "feed_source_url": change.get("source_url", ""),
    }


def classify_rename_candidate(
    change: dict[str, str],
    *,
    listings: list[dict[str, str]],
    reference_rows: list[dict[str, str]],
    reference_lookup: dict[tuple[str, str], list[dict[str, str]]],
    directory_keys: set[tuple[str, str]],
    sec_payload: dict[str, Any] | None,
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
    if len(old_matches) != 1:
        return "blocked_old_symbol_not_unique_in_us_scope", {}

    old_listing = old_matches[0]
    exchange = old_listing.get("exchange", "")
    same_exchange_successors = [
        row
        for row in listings
        if normalize_symbol(row.get("ticker", "")) == new_symbol and row.get("exchange") == exchange
    ]
    if same_exchange_successors:
        old_key = row_listing_key(old_listing)
        old_isin = old_listing.get("isin", "").strip()
        if len(same_exchange_successors) != 1:
            return "blocked_new_symbol_collision", {}
        successor = same_exchange_successors[0]
        if (exchange, old_symbol) in directory_keys:
            return "blocked_new_symbol_collision", {}
        if (exchange, new_symbol) not in directory_keys:
            return "blocked_new_symbol_collision", {}
        identity_type, identity_value, block = resolve_rename_identity(
            old_symbol=old_symbol,
            new_symbol=new_symbol,
            old_isin=old_isin,
            sec_payload=sec_payload,
        )
        if block:
            return block, {}
        successor_isin = successor.get("isin", "").strip()
        if successor_isin and successor_isin != old_isin:
            return "manual_successor_isin_mismatch", {}
        asset_type = old_listing.get("asset_type", "") or successor.get("asset_type", "")
        if not names_refer_to_same_identity(
            old_listing.get("name", ""),
            successor.get("name", ""),
            asset_type,
        ):
            return "manual_ticker_reuse_name_mismatch", {}
        official_new_rows = reference_lookup.get((exchange, new_symbol), [])
        if official_new_rows and not any(
            names_refer_to_same_identity(old_listing.get("name", ""), row.get("name", ""), asset_type)
            for row in official_new_rows
        ):
            return "manual_ticker_reuse_name_mismatch", {}
        directory_row = official_directory_row(reference_rows, exchange, new_symbol)
        return "apply_drop_predecessor", {
            "exchange": exchange,
            "old_symbol": old_symbol,
            "new_symbol": new_symbol,
            "old_listing_key": old_key,
            "new_listing_key": listing_key(exchange, new_symbol),
            "isin": old_isin,
            "identity_type": identity_type,
            "identity_value": identity_value,
            "source_key": (directory_row or {}).get("source_key", "nasdaq_listed"),
            "source_url": (directory_row or {}).get("source_url", ""),
            "evidence": "official_master_old_absent_new_active_successor_already_listed",
        }

    cross_venue_successors = [
        row
        for row in listings
        if normalize_symbol(row.get("ticker", "")) == new_symbol
        and row.get("exchange") in US_LISTED_EXCHANGES
        and row.get("exchange") != exchange
    ]
    if cross_venue_successors:
        return "manual_cross_venue_ticker_reuse", {
            "exchange": exchange,
            "old_symbol": old_symbol,
            "new_symbol": new_symbol,
            "old_listing_key": row_listing_key(old_listing),
            "new_listing_key": row_listing_key(cross_venue_successors[0]),
            "isin": old_listing.get("isin", "").strip(),
            "source_key": change.get("source", "stockanalysis_symbol_changes"),
            "source_url": change.get("source_url", ""),
            "evidence": "new_symbol_already_listed_on_another_us_venue",
        }

    old_key = row_listing_key(old_listing)
    new_key = listing_key(exchange, new_symbol)
    if any(row_listing_key(row) == new_key for row in listings):
        return "blocked_listing_key_collision", {}

    old_isin = old_listing.get("isin", "").strip()
    if (exchange, new_symbol) not in directory_keys:
        return "blocked_new_symbol_not_active_in_official_master", {}
    if (exchange, old_symbol) in directory_keys:
        return "blocked_old_symbol_still_active_in_official_master", {}

    identity_type, identity_value, block = resolve_rename_identity(
        old_symbol=old_symbol,
        new_symbol=new_symbol,
        old_isin=old_isin,
        sec_payload=sec_payload,
    )
    if block:
        return block, {}

    return "apply", build_verified_rename_evidence(
        exchange=exchange,
        old_symbol=old_symbol,
        new_symbol=new_symbol,
        old_listing_key=old_key,
        new_listing_key=new_key,
        old_isin=old_isin,
        identity_type=identity_type,
        identity_value=identity_value,
        reference_rows=reference_rows,
        change=change,
    )


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
        official_name = rename.get("new_official_name", "").strip()
        if official_name and "name" in next_row:
            next_row["name"] = official_name
        updated.append(next_row)
    return updated


def persist_applied_symbol_renames(
    accepted: list[dict[str, str]],
    *,
    listing_transitions_csv: Path,
) -> None:
    if not accepted:
        return
    existing = load_csv(listing_transitions_csv)
    keys = {
        (row.get("old_listing_key", ""), row.get("new_listing_key", ""))
        for row in existing
    }
    for row in accepted:
        old_key = row["old_listing_key"]
        new_key = row["new_listing_key"]
        if (old_key, new_key) in keys:
            continue
        identity_type = row.get("identity_type", "same_isin")
        identity_value = row.get("identity_value", row.get("isin", ""))
        source_url = row.get("source_url", "")
        if not str(source_url).startswith("https://"):
            continue
        existing.append(
            {
                "old_listing_key": old_key,
                "new_listing_key": new_key,
                "event_type": row.get("transition_event_type", "symbol_changed"),
                "identity_type": identity_type,
                "identity_value": identity_value,
                "confidence": "0.99",
                "source_key": row.get("source_key", "nasdaq_listed"),
                "source_url": source_url,
                "reason": (
                    f"Official US exchange directory lists {new_key} active and no longer lists "
                    f"{old_key}; {identity_type} {identity_value} links the predecessor to the "
                    "successor. Applied automatically from the symbol-change feed after venue "
                    f"confirmation ({row.get('feed_source_key', '')})."
                ),
            }
        )
        keys.add((old_key, new_key))
    write_csv(listing_transitions_csv, TRANSITION_FIELDS, existing)


def persist_dropped_predecessors(
    dropped: list[dict[str, str]],
    *,
    listing_transitions_csv: Path,
    drop_entries_csv: Path,
) -> None:
    existing_transitions = load_csv(listing_transitions_csv)
    existing_drops = load_csv(drop_entries_csv)
    transition_keys = {
        (row.get("old_listing_key", ""), row.get("new_listing_key", ""))
        for row in existing_transitions
    }
    drop_keys = {(row.get("ticker", ""), row.get("exchange", "")) for row in existing_drops}
    for row in dropped:
        old_key = row["old_listing_key"]
        transition = {
            "old_listing_key": old_key,
            "new_listing_key": "",
            "event_type": "delisted",
            "identity_type": "exact_isin",
            "identity_value": row["isin"],
            "confidence": "0.99",
            "source_key": row.get("source_key", "") or "stockanalysis_symbol_changes",
            "source_url": row.get("source_url", ""),
            "reason": (
                f"Official US directory no longer lists {old_key} and already lists "
                f"{row['new_listing_key']}; the symbol-change feed identifies the successor. "
                "The existing successor row is not rewritten."
            ),
        }
        if (old_key, "") not in transition_keys:
            existing_transitions.append(transition)
            transition_keys.add((old_key, ""))
        exchange, _, ticker = old_key.partition("::")
        if (ticker, exchange) not in drop_keys:
            existing_drops.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "confidence": "0.99",
                    "reason": (
                        f"Reviewed predecessor drop {old_key} after successor "
                        f"{row['new_listing_key']} was already listed; exact evidence is recorded "
                        "in listing_transitions.csv."
                    ),
                }
            )
            drop_keys.add((ticker, exchange))
    if existing_transitions:
        write_csv(listing_transitions_csv, TRANSITION_FIELDS, existing_transitions)
    if existing_drops:
        write_csv(drop_entries_csv, DROP_FIELDS, existing_drops)


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
    listing_transitions_csv: Path = LISTING_TRANSITIONS_CSV,
    drop_entries_csv: Path = DROP_ENTRIES_CSV,
    report_json: Path = REPORT_JSON,
    report_md: Path = REPORT_MD,
    dry_run: bool = False,
    live_reference_rows: list[dict[str, str]] | None = None,
    sec_exchange_payload: dict[str, Any] | None = None,
    fetch_live_us_directory: bool = True,
) -> dict[str, Any]:
    changes = load_csv(changes_csv)
    listings = load_csv(listings_csv)
    static_reference = active_official_reference_rows(load_csv(reference_csv))
    if live_reference_rows is None and fetch_live_us_directory:
        live_reference_rows = load_live_us_directory_reference_rows()
    active_reference = merge_us_directory_reference_rows(static_reference, live_reference_rows or [])
    reference_lookup = lookup_by_exchange_symbol(active_reference)
    directory_keys = active_directory_keys(active_reference)
    sec_payload = (
        load_sec_exchange_payload()
        if sec_exchange_payload is None and fetch_live_us_directory
        else sec_exchange_payload
    )

    accepted: list[dict[str, str]] = []
    dropped_predecessors: list[dict[str, str]] = []
    blocked: list[dict[str, str]] = []
    seen_old_keys: set[str] = set()
    for change in changes:
        status, evidence = classify_rename_candidate(
            change,
            listings=listings,
            reference_rows=active_reference,
            reference_lookup=reference_lookup,
            directory_keys=directory_keys,
            sec_payload=sec_payload,
        )
        row = {
            "change_id": change.get("change_id", ""),
            "effective_date": change.get("effective_date", ""),
            "old_symbol": normalize_symbol(change.get("old_symbol", "")),
            "new_symbol": normalize_symbol(change.get("new_symbol", "")),
            "new_company_name": change.get("new_company_name", ""),
            "status": status,
        }
        if status in {"apply", "apply_drop_predecessor"}:
            if evidence["old_listing_key"] in seen_old_keys:
                blocked.append({**row, **evidence, "status": "blocked_duplicate_old_listing_key_candidate"})
                continue
            if status == "apply_drop_predecessor" and not str(evidence.get("source_url", "")).startswith("https://"):
                blocked.append({**row, **evidence, "status": "manual_successor_exists_missing_source_url"})
                continue
            seen_old_keys.add(evidence["old_listing_key"])
            if status == "apply":
                accepted.append({**row, **evidence})
            else:
                dropped_predecessors.append({**row, **evidence})
        else:
            blocked.append(row)

    if accepted and not dry_run:
        persist_applied_symbol_renames(
            accepted,
            listing_transitions_csv=listing_transitions_csv,
        )
    if accepted and not dry_run:
        for path in [listings_csv, listing_index_csv, identifiers_extended_csv, supplemental_csv]:
            rows = load_csv(path)
            if not rows:
                continue
            write_csv(path, list(rows[0].keys()), rekey_rows(rows, accepted))
    if dropped_predecessors and not dry_run:
        persist_dropped_predecessors(
            dropped_predecessors,
            listing_transitions_csv=listing_transitions_csv,
            drop_entries_csv=drop_entries_csv,
        )

    summary = {
        "generated_at": utc_now_iso(),
        "dry_run": dry_run,
        "changes_csv": display_path(changes_csv, ROOT),
        "reference_csv": display_path(reference_csv, ROOT),
        "accepted_rows": len(accepted),
        "dropped_predecessor_rows": len(dropped_predecessors),
        "blocked_rows": len(blocked),
        "blocked_by_status": dict(sorted(Counter(row["status"] for row in blocked).items())),
    }
    report = {
        "summary": summary,
        "accepted": accepted,
        "dropped_predecessors": dropped_predecessors,
        "blocked": blocked,
    }
    write_json(report_json, report)
    write_markdown(report_md, report)
    set_github_output(
        "symbol_changes_applied",
        "true" if (accepted or dropped_predecessors) and not dry_run else "false",
    )
    set_github_output("accepted_rows", str(len(accepted) + len(dropped_predecessors)))
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
