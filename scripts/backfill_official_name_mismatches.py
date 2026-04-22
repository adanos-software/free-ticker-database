from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.build_entry_quality_report import (
    DEFAULT_CSV_OUT as ENTRY_QUALITY_CSV,
    MASTERFILE_REFERENCE_CSV,
    company_compact_key,
    names_match,
    official_name_is_informative,
)

DEFAULT_REPORT_JSON = ROOT / "data" / "reports" / "official_name_mismatch_backfill.json"
DEFAULT_REPORT_CSV = ROOT / "data" / "reports" / "official_name_mismatch_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

SUPPORTED_EXCHANGES = {
    "ASX",
    "BATS",
    "IDX",
    "JSE",
    "LSE",
    "NASDAQ",
    "NEO",
    "NYSE",
    "NYSE ARCA",
    "NYSE MKT",
    "PSX",
    "SET",
    "SSE_CL",
    "TSX",
    "TSXV",
}

PROVIDER_PRIORITY = {
    "Nasdaq Trader": 50,
    "TMX": 45,
    "Cboe Canada": 45,
    "LSE": 45,
    "JSE": 45,
    "ASX": 45,
    "SEC": 40,
}

SOURCE_KEY_PRIORITY = {
    "lse_price_explorer": 30,
    "lse_instrument_search": 20,
    "lse_company_reports": 10,
}

DISPLAY_ACRONYMS = {
    "ads",
    "adr",
    "ai",
    "clo",
    "dr",
    "eur",
    "etf",
    "etn",
    "etp",
    "lp",
    "l.p",
    "llc",
    "ltd",
    "ltd.",
    "n.v",
    "n.a",
    "o.n",
    "ord",
    "ord.",
    "plc",
    "reit",
    "s.a",
    "se",
    "spac",
    "ucits",
    "usd",
    "usa",
    "u.s",
}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def build_official_lookup(
    masterfile_rows: list[dict[str, str]],
) -> dict[tuple[str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in masterfile_rows:
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        key = (row.get("ticker", ""), row.get("exchange", ""))
        lookup[key].append(row)
    return dict(lookup)


def clean_reference_name(name: str) -> str:
    cleaned = re.sub(r"\s+-\s+(common|class|ordinary|american depository|depositary).*$", "", name, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+ord\s+[a-z]{3}\d*[\d.]*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\s+class\s+[a-z0-9-]+\s+common\s+stock.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+(common|ordinary|preferred)\s+(stock|shares?|units?).*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+american\s+depositary\s+shares.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+common\s+shares\s+of\s+beneficial\s+interest.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip(" ,")


def format_display_name(name: str) -> str:
    if not name or any(character.islower() for character in name):
        return name

    titled = name.title()
    parts = re.split(r"(\W+)", titled)
    fixed: list[str] = []
    for part in parts:
        compact = part.lower().strip(".")
        if compact in DISPLAY_ACRONYMS:
            fixed.append(part.upper())
            continue
        if compact == "n" and part in {"N", "n"}:
            fixed.append(part)
            continue
        fixed.append(part)
    return "".join(fixed)


def candidate_score(ref: dict[str, str], candidate: str) -> tuple[int, int, int, int]:
    provider_score = PROVIDER_PRIORITY.get(ref.get("provider", ""), 0)
    source_score = SOURCE_KEY_PRIORITY.get(ref.get("source_key", ""), 0)
    mixed_case_bonus = 1 if any(character.islower() for character in ref.get("name", "")) else 0
    return (provider_score + source_score, mixed_case_bonus, -len(candidate), -len(ref.get("name", "")))


def choose_candidate_name(
    row: dict[str, str],
    refs: list[dict[str, str]],
) -> tuple[str, list[dict[str, str]]]:
    grouped: dict[str, list[tuple[dict[str, str], str]]] = defaultdict(list)
    for ref in refs:
        name = ref.get("name", "")
        if not official_name_is_informative(name, row.get("ticker", "")):
            continue
        candidate = format_display_name(clean_reference_name(name))
        if not candidate:
            continue
        group_key = company_compact_key(candidate) or candidate.casefold()
        grouped[group_key].append((ref, candidate))

    if not grouped:
        return "", []

    best_group = max(
        grouped.values(),
        key=lambda entries: (
            len(entries),
            max(candidate_score(ref, candidate) for ref, candidate in entries),
        ),
    )
    chosen_ref, chosen_candidate = max(best_group, key=lambda item: candidate_score(item[0], item[1]))
    supporting_refs = [ref for ref, _candidate in best_group]

    if names_match(row.get("name", ""), chosen_candidate):
        return "", supporting_refs
    return chosen_candidate, supporting_refs


def build_updates(
    entry_quality_rows: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    *,
    exchanges: set[str],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    official_lookup = build_official_lookup(masterfile_rows)
    updates: list[dict[str, str]] = []
    report_rows: list[dict[str, Any]] = []

    for row in entry_quality_rows:
        if row.get("exchange") not in exchanges:
            continue
        if row.get("exchange") == "OTC":
            continue
        if row.get("quality_status") != "warn":
            continue
        if "official_name_mismatch" not in row.get("issue_types", "").split("|"):
            continue

        refs = official_lookup.get((row.get("ticker", ""), row.get("exchange", "")), [])
        proposed_name, supporting_refs = choose_candidate_name(row, refs)
        decision = "skip"
        reason = ""
        confidence = ""

        if proposed_name:
            decision = "accept"
            confidence_value = 0.96 if len(supporting_refs) >= 2 else 0.93
            confidence = f"{confidence_value:.2f}"
            sources = "|".join(sorted({ref.get("source_key", "") for ref in supporting_refs if ref.get("source_key")}))
            reason = (
                "Official active reference name mismatch backfill from "
                f"{sources or 'official exchange references'}; replaced stale or generic listing name with the current reference name."
            )
            updates.append(
                {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "field": "name",
                    "decision": "update",
                    "proposed_value": proposed_name,
                    "confidence": confidence,
                    "reason": reason,
                }
            )

        report_rows.append(
            {
                "listing_key": row["listing_key"],
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "current_name": row["name"],
                "proposed_name": proposed_name,
                "decision": decision,
                "official_sources": "|".join(sorted({ref.get("source_key", "") for ref in refs if ref.get("source_key")})),
                "supporting_sources": "|".join(
                    sorted({ref.get("source_key", "") for ref in supporting_refs if ref.get("source_key")})
                ),
            }
        )

    return updates, report_rows


def build_summary(report_rows: list[dict[str, Any]], updates: list[dict[str, str]], exchanges: set[str]) -> dict[str, Any]:
    accepted = [row for row in report_rows if row["decision"] == "accept"]
    return {
        "summary": {
            "supported_exchanges": sorted(exchanges),
            "rows_reviewed": len(report_rows),
            "updates_emitted": len(updates),
            "accepted_by_exchange": dict(Counter(row["exchange"] for row in accepted).most_common()),
            "skipped_by_exchange": dict(
                Counter(row["exchange"] for row in report_rows if row["decision"] != "accept").most_common()
            ),
        },
        "items": report_rows,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill non-OTC official name mismatches from active official references.")
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", dest="exchanges")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchanges or SUPPORTED_EXCHANGES)
    updates, report_rows = build_updates(
        load_csv(args.entry_quality_csv),
        load_csv(args.masterfile_reference_csv),
        exchanges=exchanges,
    )
    summary = build_summary(report_rows, updates, exchanges)
    write_csv(args.csv_out, report_rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)
    print(
        json.dumps(
            {
                "rows_reviewed": len(report_rows),
                "updates_emitted": len(updates),
                "accepted_by_exchange": summary["summary"]["accepted_by_exchange"],
                "applied": bool(args.apply and updates),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
