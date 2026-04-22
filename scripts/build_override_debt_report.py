from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import normalize_alias_text
from scripts.rebuild_dataset import (
    allowed_duplicate_alias_rows,
    build_alias_context,
    clean_aliases,
    country_from_isin,
    normalize_sector,
    split_aliases,
    strip_legal_alias_suffixes,
)

DATA_DIR = ROOT / "data"
REVIEW_OVERRIDES_DIR = DATA_DIR / "review_overrides"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_METADATA_UPDATES_CSV = REVIEW_OVERRIDES_DIR / "metadata_updates.csv"
DEFAULT_REMOVE_ALIASES_CSV = REVIEW_OVERRIDES_DIR / "remove_aliases.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "override_debt_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "override_debt_report.md"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def canonical_value(field: str, value: str, row: dict[str, str]) -> str:
    if field == "aliases":
        return "|".join(split_aliases(value))
    if field == "stock_sector":
        return normalize_sector(value, "Stock")
    if field == "etf_category":
        return normalize_sector(value, "ETF")
    if field == "country_code":
        return value.upper()
    return value


def canonical_alias_set(value: str) -> set[str]:
    aliases = split_aliases(value)
    return {
        normalize_alias_text(alias).lower()
        for alias in aliases
        if normalize_alias_text(alias).strip()
    }


def build_policy_rows(tickers: list[dict[str, str]]) -> list[dict[str, str]]:
    policy_rows: list[dict[str, str]] = []
    for row in tickers:
        policy_rows.append(
            {
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "name": row["name"],
                "asset_type": row["asset_type"],
                "country": row.get("country", ""),
                "country_code": row.get("country_code", ""),
                "isin": row.get("isin", ""),
                "aliases": split_aliases(row.get("aliases", "")),
                "wkn": row.get("wkn", ""),
            }
        )
    return policy_rows


def project_aliases_under_current_policy(
    *,
    row: dict[str, str],
    target_value: str,
    policy_rows: list[dict[str, str]],
    alias_context: dict[str, dict[str, set[str]]],
) -> set[str]:
    proposed_aliases = split_aliases(target_value)
    _, cleaned_aliases = clean_aliases(row, proposed_aliases, set(), alias_context)
    surviving_aliases: list[str] = []

    for alias in cleaned_aliases:
        normalized_alias = normalize_alias_text(alias).lower()
        candidate_row = dict(row)
        candidate_row["aliases"] = [alias]
        candidate_entries: list[tuple[int, dict[str, str]]] = [(-1, candidate_row)]

        for index, peer in enumerate(policy_rows):
            if (peer["ticker"], peer["exchange"]) == (row["ticker"], row["exchange"]):
                continue
            peer_aliases = {
                normalize_alias_text(existing_alias).lower()
                for existing_alias in peer.get("aliases", [])
                if normalize_alias_text(existing_alias).strip()
            }
            normalized_name = normalize_alias_text(peer["name"]).lower()
            stripped_name = strip_legal_alias_suffixes(normalized_name)
            if (
                normalized_alias in peer_aliases
                or normalized_name == normalized_alias
                or stripped_name == normalized_alias
            ):
                candidate_entries.append((index, peer))

        allowed_entries = allowed_duplicate_alias_rows(candidate_entries, normalized_alias)
        if -1 in allowed_entries:
            surviving_aliases.append(alias)

    return canonical_alias_set("|".join(surviving_aliases))


def build_override_debt_report(
    *,
    tickers: list[dict[str, str]],
    metadata_updates: list[dict[str, str]],
    remove_aliases: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or utc_now()
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers}
    policy_rows = build_policy_rows(tickers)
    policy_rows_by_key = {(row["ticker"], row["exchange"]): row for row in policy_rows}
    alias_context = build_alias_context(policy_rows, {})

    metadata_rows: list[dict[str, str]] = []
    for review in metadata_updates:
        row = by_key.get((review["ticker"], review["exchange"]))
        if not row:
            continue
        field = review["field"]
        if field not in row:
            continue
        actual = row[field]
        target = review.get("proposed_value", "")
        decision = review["decision"]
        resolved_exact = (actual == "" if decision == "clear" else actual == target)
        status = "open"
        if resolved_exact:
            status = "resolved_exact"
        elif field == "aliases":
            actual_aliases = canonical_alias_set(actual)
            target_aliases = canonical_alias_set(target)
            if actual_aliases == target_aliases:
                status = "resolved_canonical"
            elif actual_aliases and actual_aliases.issubset(target_aliases):
                status = "partial_canonical"
            else:
                policy_row = policy_rows_by_key.get((review["ticker"], review["exchange"]))
                if policy_row:
                    projected_aliases = project_aliases_under_current_policy(
                        row=policy_row,
                        target_value=target,
                        policy_rows=policy_rows,
                        alias_context=alias_context,
                    )
                    if projected_aliases == actual_aliases:
                        status = "resolved_policy"
                    elif projected_aliases and actual_aliases == projected_aliases and actual_aliases.issubset(target_aliases):
                        status = "partial_policy"
        elif field in {"country", "country_code"} and decision == "clear":
            inferred_country = country_from_isin(row.get("isin", ""))
            inferred_country_code = row.get("country_code", "")
            if field == "country" and inferred_country and actual == inferred_country:
                status = "superseded_by_isin_inference"
            elif field == "country_code" and inferred_country:
                inferred_country_code = canonical_value("country_code", row.get("country_code", ""), row)
                if actual == inferred_country_code:
                    status = "superseded_by_isin_inference"
        else:
            canonical_actual = canonical_value(field, actual, row)
            canonical_target = "" if decision == "clear" else canonical_value(field, target, row)
            if canonical_actual == canonical_target:
                status = "resolved_canonical"
        metadata_rows.append(
            {
                "ticker": review["ticker"],
                "exchange": review["exchange"],
                "field": field,
                "decision": decision,
                "target": target,
                "actual": actual,
                "status": status,
            }
        )

    alias_rows: list[dict[str, str]] = []
    for review in remove_aliases:
        row = by_key.get((review["ticker"], review["exchange"]))
        if not row:
            continue
        aliases = set(split_aliases(row.get("aliases", "")))
        alias = review["alias"]
        alias_rows.append(
            {
                "ticker": review["ticker"],
                "exchange": review["exchange"],
                "alias": alias,
                "status": "open" if alias in aliases else "resolved",
            }
        )

    metadata_status_counts = Counter(row["status"] for row in metadata_rows)
    alias_status_counts = Counter(row["status"] for row in alias_rows)
    open_metadata_by_field = Counter(row["field"] for row in metadata_rows if row["status"] == "open")
    open_metadata_by_exchange = Counter(row["exchange"] for row in metadata_rows if row["status"] == "open")
    open_alias_by_exchange = Counter(row["exchange"] for row in alias_rows if row["status"] == "open")

    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "tickers_csv": display_path(DEFAULT_TICKERS_CSV),
                "metadata_updates_csv": display_path(DEFAULT_METADATA_UPDATES_CSV),
                "remove_aliases_csv": display_path(DEFAULT_REMOVE_ALIASES_CSV),
            },
        },
        "summary": {
            "metadata_present_rows": len(metadata_rows),
            "metadata_resolved_exact": metadata_status_counts["resolved_exact"],
            "metadata_resolved_canonical": metadata_status_counts["resolved_canonical"],
            "metadata_partial_canonical": metadata_status_counts["partial_canonical"],
            "metadata_resolved_policy": metadata_status_counts["resolved_policy"],
            "metadata_partial_policy": metadata_status_counts["partial_policy"],
            "metadata_superseded_by_isin_inference": metadata_status_counts["superseded_by_isin_inference"],
            "metadata_open": metadata_status_counts["open"],
            "alias_present_rows": len(alias_rows),
            "alias_resolved": alias_status_counts["resolved"],
            "alias_open": alias_status_counts["open"],
        },
        "open_metadata_by_field": dict(open_metadata_by_field.most_common()),
        "open_metadata_by_exchange": dict(open_metadata_by_exchange.most_common()),
        "open_alias_by_exchange": dict(open_alias_by_exchange.most_common()),
        "open_metadata_samples": [row for row in metadata_rows if row["status"] == "open"][:50],
        "open_alias_samples": [row for row in alias_rows if row["status"] == "open"][:50],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    lines = [
        "# Override Debt Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in summary.items():
        lines.append(f"| {key} | {value:,} |")

    lines.extend(["", "## Open Metadata By Field", ""])
    for field, count in payload["open_metadata_by_field"].items():
        lines.append(f"- `{field}`: `{count}`")
    if not payload["open_metadata_by_field"]:
        lines.append("- None")

    lines.extend(["", "## Open Alias Removals By Exchange", ""])
    for exchange, count in payload["open_alias_by_exchange"].items():
        lines.append(f"- `{exchange}`: `{count}`")
    if not payload["open_alias_by_exchange"]:
        lines.append("- None")

    lines.extend(["", "## Open Metadata Samples", ""])
    for row in payload["open_metadata_samples"][:20]:
        lines.append(
            f"- `{row['exchange']}::{row['ticker']}` `{row['field']}` target=`{row['target']}` actual=`{row['actual']}`"
        )
    if not payload["open_metadata_samples"]:
        lines.append("- None")

    lines.extend(["", "## Open Alias Samples", ""])
    for row in payload["open_alias_samples"][:20]:
        lines.append(f"- `{row['exchange']}::{row['ticker']}` alias=`{row['alias']}`")
    if not payload["open_alias_samples"]:
        lines.append("- None")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build report for unresolved review-override debt.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--remove-aliases-csv", type=Path, default=DEFAULT_REMOVE_ALIASES_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_override_debt_report(
        tickers=load_csv(args.tickers_csv),
        metadata_updates=load_csv(args.metadata_updates_csv) if args.metadata_updates_csv.exists() else [],
        remove_aliases=load_csv(args.remove_aliases_csv) if args.remove_aliases_csv.exists() else [],
    )
    if not args.no_write:
        write_json(args.json_out, payload)
        write_markdown(args.md_out, payload)
    print(json.dumps({"summary": payload["summary"], "json_out": display_path(args.json_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
