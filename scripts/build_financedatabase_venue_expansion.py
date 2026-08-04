"""Build and optionally apply a gated FinanceDatabase venue expansion plan.

The venue reconciliation report identifies rows that are absent from the local
dataset.  This command narrows that queue further: the target must have an
active official Stock reference and an independent OpenFIGI Common Stock result
whose name matches the official issuer.  It writes a reviewable plan first and
only changes ``coverage_expansion_listings.csv`` when ``--execute`` is given.

The command deliberately does not promote rows to a primary listing, infer
sectors from FinanceDatabase, or copy an unverified FinanceDatabase ISIN.
Those remain separate metadata/source gaps.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.non_equity_guard import classify_non_equity_leakage
from scripts.listing_keys import row_listing_key
from scripts.rebuild_dataset import (
    COUNTRY_TO_ISO,
    country_from_isin,
    is_valid_isin,
    normalize_sector,
)

DEFAULT_REVIEW = ROOT / "data" / "reports" / "finance_database_venue_review.csv"
DEFAULT_RECONCILIATION = ROOT / "data" / "reports" / "finance_database_venue_reconciliation.json"
DEFAULT_REFERENCE = ROOT / "data" / "masterfiles" / "reference.csv"
DEFAULT_LISTINGS = ROOT / "data" / "listings.csv"
DEFAULT_COVERAGE = ROOT / "data" / "coverage_expansion_listings.csv"
DEFAULT_DROP_ENTRIES = ROOT / "data" / "review_overrides" / "drop_entries.csv"
DEFAULT_PROBE = ROOT / "data" / "reports" / "finance_database_venue_openfigi_probe.json"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "reports"

COVERAGE_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
]

PLAN_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "fd_symbol",
    "fd_name",
    "fd_isin",
    "official_ticker",
    "official_name",
    "official_isin",
    "official_source_key",
    "official_sector",
    "official_reference_asset_type",
    "reference_asset_type_conflict",
    "local_same_isin_any_venue",
    "local_same_isin_venues",
    "openfigi_query_type",
    "openfigi_query_value",
    "openfigi_common_stock_match_count",
    "openfigi_common_stock_figis",
    "openfigi_common_stock_names",
    "openfigi_common_stock_exchange_codes",
    "openfigi_security_types",
    "openfigi_match_status",
    "apply_action",
    "reason",
    "stock_sector_action",
    "isin_action",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_probe(path: Path) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    responses: dict[tuple[str, str], dict[str, Any]] = {}
    for item in payload.get("unique_jobs", []):
        key = (str(item.get("id_type", "")), str(item.get("id_value", "")))
        response = item.get("response")
        if key[0] and key[1] and isinstance(response, dict):
            responses[key] = response
    errors = payload.get("errors", [])
    return responses, errors if isinstance(errors, list) else []


def _reference_index(references: Iterable[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    indexed: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in references:
        if row.get("official", "").strip().lower() != "true":
            continue
        if row.get("listing_status", "").strip().lower() != "active":
            continue
        indexed[(row.get("exchange", ""), row.get("ticker", ""), row.get("source_key", ""))].append(row)
    return indexed


def _listing_index(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    indexed: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[(row.get("exchange", ""), row.get("ticker", ""))].append(row)
    return indexed


def _common_stock(row: dict[str, Any]) -> bool:
    return str(row.get("securityType", "")).strip().lower() == "common stock" or str(
        row.get("securityType2", "")
    ).strip().lower() == "common stock"


def _openfigi_matches(
    review: dict[str, str],
    responses: dict[tuple[str, str], dict[str, Any]],
) -> tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    query_type = "ID_ISIN" if review.get("official_isin", "").strip() else "TICKER"
    query_value = review.get("official_isin", "").strip() or review.get("official_ticker", "").strip()
    response = responses.get((query_type, query_value), {})
    data = response.get("data", []) if isinstance(response, dict) else []
    if not isinstance(data, list):
        data = []
    common = [item for item in data if isinstance(item, dict) and _common_stock(item)]
    matching = [
        item
        for item in common
        if _names_match(review.get("official_name", ""), str(item.get("name", "")))
    ]
    return query_type, query_value, common, matching


def _names_match(left: str, right: str) -> bool:
    from scripts.rebuild_dataset import alias_matches_company

    return alias_matches_company(left, right)


def _unique_items(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (
            str(item.get("figi", "")),
            str(item.get("name", "")),
            str(item.get("exchCode", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _coverage_row(review: dict[str, str]) -> dict[str, str]:
    isin = review.get("official_isin", "").strip().upper()
    if not is_valid_isin(isin):
        isin = ""
    country = country_from_isin(isin) or ""
    sector = normalize_sector(review.get("official_sector", "").strip(), "Stock")
    return {
        "listing_key": f"{review['mapped_exchange']}::{review['official_ticker']}",
        "ticker": review["official_ticker"],
        "exchange": review["mapped_exchange"],
        "name": review["official_name"],
        "asset_type": "Stock",
        "stock_sector": sector,
        "etf_category": "",
        "country": country,
        "country_code": COUNTRY_TO_ISO.get(country, ""),
        "isin": isin,
        "aliases": "",
    }


def build_plan(
    review_rows: list[dict[str, str]],
    references: list[dict[str, str]],
    listings: list[dict[str, str]],
    coverage_rows: list[dict[str, str]],
    responses: dict[tuple[str, str], dict[str, Any]],
    probe_errors: list[dict[str, Any]] | None = None,
    drop_entries: set[tuple[str, str]] | None = None,
) -> list[dict[str, str]]:
    reference_index = _reference_index(references)
    listing_index = _listing_index(listings)
    coverage_index = _listing_index(coverage_rows)
    errors = probe_errors or []
    drop_entries = drop_entries or set()
    plan: list[dict[str, str]] = []

    for review in review_rows:
        exchange = review.get("mapped_exchange", "")
        ticker = review.get("official_ticker", "")
        source_key = review.get("official_source_key", "")
        key = (exchange, ticker)
        drop_key = (ticker, exchange)
        query_type = "ID_ISIN" if review.get("official_isin", "").strip() else "TICKER"
        query_value = review.get("official_isin", "").strip() or ticker
        common, matching = [], []
        if query_value:
            _query_type, _query_value, common, matching = _openfigi_matches(review, responses)
            query_type, query_value = _query_type, _query_value
        common = _unique_items(common)
        matching = _unique_items(matching)
        reference_rows = reference_index.get((exchange, ticker, source_key), [])
        source_asset_types = sorted({row.get("asset_type", "") for row in reference_rows if row.get("asset_type", "")})
        local_rows = listing_index.get(key, [])
        coverage_present = bool(coverage_index.get(key))

        if drop_key in drop_entries:
            action = "blocked_review_drop_entry"
            reason = "The repository has an explicit reviewed drop entry for this ticker/venue; coverage expansion must not reintroduce it."
        elif review.get("dry_run_decision") != "review_security_type_required":
            action = "blocked_reconciliation_decision"
            reason = "The venue reconciliation classified this row as a conflict, name signal, or existing other asset type."
        elif not reference_rows or source_asset_types != ["Stock"]:
            action = "blocked_reference_source"
            reason = "The selected active official source is not an unambiguous Stock reference."
        elif local_rows:
            action = "already_local"
            reason = "The exact venue/ticker listing key is already present locally."
        elif coverage_present:
            action = "already_covered"
            reason = "The exact venue/ticker listing key is already in coverage_expansion_listings.csv."
        elif not common:
            action = "blocked_openfigi_no_common_stock"
            reason = "OpenFIGI returned no Common Stock result for the query."
        elif not matching:
            action = "blocked_openfigi_name_mismatch"
            reason = "OpenFIGI returned Common Stock results, but none matched the official issuer name."
        else:
            guard = classify_non_equity_leakage(
                {
                    "asset_type": "Stock",
                    "ticker": ticker,
                    "name": review.get("official_name", ""),
                    "cfi": "",
                }
            )
            if guard["guard_decision"] in {
                "blocked_non_common_stock",
                "manual_review_ambiguous_stock_classification",
            }:
                action = "blocked_non_equity_guard"
                reason = str(guard.get("reason") or guard["guard_decision"])
            else:
                action = "add_coverage_expansion"
                reason = "Active official Stock reference plus OpenFIGI Common Stock issuer-name match."

        coverage = _coverage_row(review)
        normalized_sector = coverage["stock_sector"]
        official_isin = coverage["isin"]
        plan.append(
            {
                "listing_key": coverage["listing_key"],
                "ticker": coverage["ticker"],
                "exchange": coverage["exchange"],
                "name": coverage["name"],
                "fd_symbol": review.get("fd_symbol", ""),
                "fd_name": review.get("fd_name", ""),
                "fd_isin": review.get("fd_isin", ""),
                "official_ticker": ticker,
                "official_name": review.get("official_name", ""),
                "official_isin": review.get("official_isin", ""),
                "official_source_key": source_key,
                "official_sector": review.get("official_sector", ""),
                "official_reference_asset_type": "|".join(source_asset_types),
                "reference_asset_type_conflict": review.get("reference_asset_type_conflict", ""),
                "local_same_isin_any_venue": review.get("local_same_isin_any_venue", ""),
                "local_same_isin_venues": review.get("local_same_isin_venues", ""),
                "openfigi_query_type": query_type,
                "openfigi_query_value": query_value,
                "openfigi_common_stock_match_count": str(len(matching)),
                "openfigi_common_stock_figis": "|".join(str(item.get("figi", "")) for item in matching if item.get("figi")),
                "openfigi_common_stock_names": "|".join(str(item.get("name", "")) for item in matching if item.get("name")),
                "openfigi_common_stock_exchange_codes": "|".join(
                    sorted({str(item.get("exchCode", "")) for item in matching if item.get("exchCode")})
                ),
                "openfigi_security_types": "|".join(
                    sorted(
                        {
                            str(item.get("securityType", ""))
                            for item in common
                            if item.get("securityType")
                        }
                    )
                ),
                "openfigi_match_status": (
                    "common_stock_name_match"
                    if matching
                    else "common_stock_name_mismatch"
                    if common
                    else "no_common_stock"
                ),
                "apply_action": action,
                "reason": reason,
                "stock_sector_action": "use_official_reference" if normalized_sector else "leave_blank_until_official_sector",
                "isin_action": "use_official_reference" if official_isin else "leave_blank_until_official_identifier",
            }
        )

    # The probe may contain transport errors.  Keep those visible in the plan
    # rather than silently presenting an incomplete queue as complete.
    if errors:
        for row in plan:
            if row["apply_action"] == "add_coverage_expansion" and not row["openfigi_common_stock_match_count"]:
                row["apply_action"] = "blocked_openfigi_probe_error"
                row["reason"] = "The OpenFIGI probe recorded transport errors; no row is applied without a complete probe."
    return sorted(plan, key=lambda row: (row["exchange"], row["ticker"], row["listing_key"]))


def write_plan_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    output_dir: Path,
    plan: list[dict[str, str]],
    *,
    probe_path: Path,
    reconciliation_path: Path,
    execute: bool,
    coverage_rows_added: int,
    probe_errors: list[dict[str, Any]],
) -> dict[str, Path]:
    decisions = Counter(row["apply_action"] for row in plan)
    venue_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in plan:
        venue_counts[row["exchange"]][row["apply_action"]] += 1
    reconciliation = {}
    if reconciliation_path.exists():
        try:
            reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            reconciliation = {}
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "apply_performed": execute,
        "coverage_rows_added": coverage_rows_added,
        "plan_rows": len(plan),
        "apply_action_counts": dict(sorted(decisions.items())),
        "venue_action_counts": {
            venue: dict(sorted(counts.items())) for venue, counts in sorted(venue_counts.items())
        },
        "openfigi_probe_sha256": sha256_file(probe_path),
        "openfigi_probe_errors": len(probe_errors),
        "reconciliation_summary": {
            "finance_database_commit": reconciliation.get("finance_database_commit", ""),
            "reference_sha256": reconciliation.get("reference_sha256", ""),
            "candidate_rows": reconciliation.get("candidate_rows", ""),
            "selected_review_rows": reconciliation.get("selected_review_rows", ""),
        },
        "policy": {
            "selection": "Only active official Stock references with an OpenFIGI Common Stock issuer-name match are applied.",
            "primary_listing": "Coverage rows never override an existing primary ticker owner.",
            "sector": "Only canonical sectors already present in the official reference are copied; otherwise blank.",
            "isin": "Only a valid official-reference ISIN is copied; FinanceDatabase-only ISINs remain blank.",
            "blocked_rows": "Explicit review drop entries, conflicts, fund/trust name signals, missing OpenFIGI Common Stock identity, and name mismatches remain review-only.",
        },
        "artifacts": {
            "plan_csv": "finance_database_venue_apply_plan.csv",
            "summary_json": "finance_database_venue_apply_plan.json",
            "summary_md": "finance_database_venue_apply_plan.md",
        },
    }
    paths = {
        "plan_csv": output_dir / "finance_database_venue_apply_plan.csv",
        "summary_json": output_dir / "finance_database_venue_apply_plan.json",
        "summary_md": output_dir / "finance_database_venue_apply_plan.md",
    }
    write_plan_csv(paths["plan_csv"], plan)
    paths["summary_json"].write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    table = [
        "| Venue | Add | Already covered/local | OpenFIGI name mismatch | No Common Stock | Other blocked |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for venue, counts in sorted(venue_counts.items()):
        table.append(
            f"| {venue} | {counts['add_coverage_expansion']} | "
            f"{counts['already_local'] + counts['already_covered']} | "
            f"{counts['blocked_openfigi_name_mismatch']} | {counts['blocked_openfigi_no_common_stock']} | "
            f"{sum(value for key, value in counts.items() if key not in {'add_coverage_expansion', 'already_local', 'already_covered', 'blocked_openfigi_name_mismatch', 'blocked_openfigi_no_common_stock'})} |"
        )
    markdown = "\n".join(
        [
            "# FinanceDatabase venue expansion apply plan",
            "",
            "This plan is review-gated. It is generated from the venue reconciliation and a frozen OpenFIGI probe.",
            "",
            f"- Plan rows: `{len(plan)}`",
            f"- Rows authorized by the evidence gate: `{decisions['add_coverage_expansion']}`",
            f"- Rows written to coverage expansion in this run: `{coverage_rows_added}`",
            f"- Apply performed: `{execute}`",
            f"- OpenFIGI probe SHA-256: `{summary['openfigi_probe_sha256']}`",
            f"- OpenFIGI probe errors: `{len(probe_errors)}`",
            "",
            *table,
            "",
            "Only the `add_coverage_expansion` rows are written. The output remains collision-safe and does not replace primary ticker owners.",
            "",
        ]
    )
    paths["summary_md"].write_text(markdown, encoding="utf-8")
    return paths


def apply_coverage_rows(path: Path, plan: list[dict[str, str]]) -> int:
    original_bytes = path.read_bytes() if path.exists() else b""
    existing = read_csv(path) if path.exists() else []
    by_key: dict[str, dict[str, str]] = {}
    for row in existing:
        key = row_listing_key(row)
        if key in by_key:
            raise ValueError(f"duplicate existing coverage listing key: {key}")
        by_key[key] = {field: row.get(field, "") for field in COVERAGE_FIELDS}
    additions = 0
    new_rows: list[dict[str, str]] = []
    for row in plan:
        if row["apply_action"] != "add_coverage_expansion":
            continue
        key = row["listing_key"]
        if key in by_key:
            continue
        by_key[key] = {
            "listing_key": key,
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "name": row["name"],
            "asset_type": "Stock",
            "stock_sector": row["stock_sector_action"] == "use_official_reference" and normalize_sector(row["official_sector"], "Stock") or "",
            "etf_category": "",
            "country": country_from_isin(row["official_isin"]) or "",
            "country_code": COUNTRY_TO_ISO.get(country_from_isin(row["official_isin"]) or "", ""),
            "isin": row["official_isin"] if row["isin_action"] == "use_official_reference" else "",
            "aliases": "",
        }
        new_rows.append(by_key[key])
        additions += 1
    if additions == 0:
        return 0

    # Preserve the established source bytes and line endings so an incremental
    # expansion does not rewrite thousands of unrelated existing rows.  New
    # rows are stable and sorted by listing key at the end of the source file.
    # Use LF for new rows so ``git diff --check`` does not report CR as
    # trailing whitespace, while the original source bytes remain untouched.
    line_ending = "\n"
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COVERAGE_FIELDS, lineterminator=line_ending)
    writer.writerows(sorted(new_rows, key=lambda row: row["listing_key"]))
    suffix = buffer.getvalue().encode("utf-8")
    if original_bytes:
        if not original_bytes.endswith((b"\n", b"\r")):
            original_bytes += line_ending.encode("ascii")
        path.write_bytes(original_bytes + suffix)
    else:
        header = (",".join(COVERAGE_FIELDS) + line_ending).encode("utf-8")
        path.write_bytes(header + suffix)
    return additions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--reconciliation", type=Path, default=DEFAULT_RECONCILIATION)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--listings", type=Path, default=DEFAULT_LISTINGS)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--drop-entries", type=Path, default=DEFAULT_DROP_ENTRIES)
    parser.add_argument("--probe", type=Path, default=DEFAULT_PROBE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--execute", action="store_true", help="Write approved rows to coverage_expansion_listings.csv.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    review_rows = read_csv(args.review)
    references = read_csv(args.reference)
    listings = read_csv(args.listings)
    coverage_rows = read_csv(args.coverage) if args.coverage.exists() else []
    drop_entries = {
        (row.get("ticker", "").strip(), row.get("exchange", "").strip())
        for row in read_csv(args.drop_entries)
        if row.get("ticker", "").strip() and row.get("exchange", "").strip()
    }
    responses, probe_errors = load_probe(args.probe)
    plan = build_plan(
        review_rows,
        references,
        listings,
        coverage_rows,
        responses,
        probe_errors,
        drop_entries,
    )
    coverage_rows_added = apply_coverage_rows(args.coverage, plan) if args.execute else 0
    paths = write_summary(
        args.output_dir,
        plan,
        probe_path=args.probe,
        reconciliation_path=args.reconciliation,
        execute=args.execute,
        coverage_rows_added=coverage_rows_added,
        probe_errors=probe_errors,
    )
    print(
        json.dumps(
            {
                "plan_rows": len(plan),
                "apply_action_counts": dict(Counter(row["apply_action"] for row in plan)),
                "coverage_rows_added": coverage_rows_added,
                "apply_performed": args.execute,
                "summary": str(paths["summary_json"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
