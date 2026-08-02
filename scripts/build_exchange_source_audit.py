from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
COVERAGE_JSON = DATA_DIR / "reports" / "coverage_report.json"
LISTINGS_CSV = DATA_DIR / "listings.csv"
REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
CSV_OUT = DATA_DIR / "reports" / "exchange_source_audit.csv"
JSON_OUT = DATA_DIR / "reports" / "exchange_source_audit.json"
MD_OUT = DATA_DIR / "reports" / "exchange_source_audit.md"


FIELDNAMES = [
    "exchange",
    "venue_status",
    "reference_scopes",
    "current_stock_rows",
    "current_etf_rows",
    "official_active_stock_rows",
    "official_active_etf_rows",
    "missing_product_classes",
    "official_source_keys",
    "unavailable_source_keys",
    "nonfresh_source_keys",
    "source_blocker_classes",
    "official_denominator",
    "official_matches",
    "official_collisions",
    "official_missing",
    "official_recall_pct",
    "collision_adjusted_recall_pct",
    "denominator_status",
    "audit_outcome",
    "promotion_readiness",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def pipe(values: Iterable[str]) -> str:
    return "|".join(sorted({value for value in values if value}))


def source_blocker_class(source: dict[str, Any]) -> str:
    error = str(source.get("last_error", "")).lower()
    if "timed out" in error or "timeout" in error:
        return "timeout"
    if any(token in error for token in ("challenge", "cloudflare", "incapsula", "http 403")):
        return "access_challenge"
    if any(token in error for token in ("certificate", "ssl", "tls")):
        return "tls"
    if "empty refresh" in error:
        return "empty_refresh"
    if source.get("mode") == "unavailable":
        return "unavailable"
    if source.get("mode") == "cache":
        return "cache_only"
    if source.get("freshness_status") in {"old", "stale"}:
        return "stale_artifact"
    return ""


def build_exchange_source_audit(
    coverage: dict[str, Any],
    listings: Iterable[dict[str, str]],
    references: Iterable[dict[str, str]],
) -> list[dict[str, Any]]:
    listing_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in listings:
        listing_counts[row.get("exchange", "")][row.get("asset_type", "")] += 1

    official_counts: dict[str, Counter[str]] = defaultdict(Counter)
    source_keys: dict[str, set[str]] = defaultdict(set)
    for row in references:
        if row.get("official") != "true":
            continue
        exchange = row.get("exchange", "")
        source_keys[exchange].add(row.get("source_key", ""))
        if row.get("listing_status") == "active":
            official_counts[exchange][row.get("asset_type", "")] += 1

    source_coverage = {
        row.get("key", ""): row for row in coverage.get("source_coverage", []) if row.get("key")
    }
    rows: list[dict[str, Any]] = []
    for venue in sorted(coverage.get("by_exchange", []), key=lambda row: row.get("exchange", "")):
        exchange = venue.get("exchange", "")
        keys = sorted(source_keys.get(exchange, set()))
        unavailable = [key for key in keys if source_coverage.get(key, {}).get("mode") == "unavailable"]
        nonfresh = [
            key
            for key in keys
            if source_coverage.get(key, {}).get("freshness_status") not in {"fresh", None}
        ]
        blocker_classes = [source_blocker_class(source_coverage.get(key, {})) for key in keys]
        missing_products = [
            asset_type
            for asset_type in ("Stock", "ETF")
            if listing_counts[exchange][asset_type] and not official_counts[exchange][asset_type]
        ]
        denominator = int(venue.get("masterfile_symbols") or 0)
        denominator_status = "available" if denominator else "denominator_missing"
        status = venue.get("venue_status", "missing")
        if unavailable:
            outcome = "refresh_unavailable"
        elif status == "official_full" and nonfresh:
            outcome = "refresh_required"
        elif status == "official_full" and denominator:
            outcome = "maintain"
        elif status == "official_partial" and not denominator:
            outcome = "denominator_missing"
        elif status == "official_partial":
            outcome = "promotion_evidence_required"
        else:
            outcome = "official_source_required"
        if status != "official_partial":
            promotion_readiness = "not_applicable"
        elif unavailable:
            promotion_readiness = "blocked_source_unavailable"
        elif nonfresh:
            promotion_readiness = "blocked_nonfresh_source"
        elif missing_products:
            promotion_readiness = "blocked_product_class_gap"
        elif not denominator:
            promotion_readiness = "blocked_denominator_missing"
        elif float(venue.get("collision_adjusted_recall_pct") or 0) < 99.5:
            promotion_readiness = "blocked_recall_below_99_5"
        else:
            promotion_readiness = "ready_for_manual_scope_review"
        rows.append(
            {
                "exchange": exchange,
                "venue_status": status,
                "reference_scopes": pipe(venue.get("reference_scopes", [])),
                "current_stock_rows": listing_counts[exchange]["Stock"],
                "current_etf_rows": listing_counts[exchange]["ETF"],
                "official_active_stock_rows": official_counts[exchange]["Stock"],
                "official_active_etf_rows": official_counts[exchange]["ETF"],
                "missing_product_classes": pipe(missing_products),
                "official_source_keys": pipe(keys),
                "unavailable_source_keys": pipe(unavailable),
                "nonfresh_source_keys": pipe(nonfresh),
                "source_blocker_classes": pipe(blocker_classes),
                "official_denominator": denominator,
                "official_matches": int(venue.get("masterfile_matches") or 0),
                "official_collisions": int(venue.get("masterfile_collisions") or 0),
                "official_missing": int(venue.get("masterfile_missing") or 0),
                "official_recall_pct": venue.get("official_recall_pct"),
                "collision_adjusted_recall_pct": venue.get("collision_adjusted_recall_pct"),
                "denominator_status": denominator_status,
                "audit_outcome": outcome,
                "promotion_readiness": promotion_readiness,
            }
        )
    return rows


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "venues": len(rows),
        "venue_status_counts": dict(sorted(Counter(row["venue_status"] for row in rows).items())),
        "audit_outcome_counts": dict(sorted(Counter(row["audit_outcome"] for row in rows).items())),
        "unavailable_venues": sum(bool(row["unavailable_source_keys"]) for row in rows),
        "nonfresh_venues": sum(bool(row["nonfresh_source_keys"]) for row in rows),
        "denominator_missing_venues": sum(row["denominator_status"] == "denominator_missing" for row in rows),
        "promotion_ready_venues": sum(
            row["promotion_readiness"] == "ready_for_manual_scope_review" for row in rows
        ),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(rows: list[dict[str, Any]], summary: dict[str, Any], generated_at: str) -> str:
    lines = [
        "# Exchange Source Audit",
        "",
        f"Generated at: `{generated_at}`",
        "",
        f"- Venues: `{summary['venues']}`",
        f"- Venue status: `{json.dumps(summary['venue_status_counts'], sort_keys=True)}`",
        f"- Audit outcomes: `{json.dumps(summary['audit_outcome_counts'], sort_keys=True)}`",
        "",
        "| Exchange | Status | Sources | Missing products | Denominator | Recall | Nonfresh | Outcome | Promotion |",
        "|---|---|---|---|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['exchange']} | {row['venue_status']} | {row['official_source_keys']} | "
            f"{row['missing_product_classes']} | {row['official_denominator']} | "
            f"{row['official_recall_pct'] if row['official_recall_pct'] is not None else ''} | "
            f"{row['nonfresh_source_keys']} | {row['audit_outcome']} | {row['promotion_readiness']} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build one official-source audit row per covered exchange.")
    parser.add_argument("--coverage", type=Path, default=COVERAGE_JSON)
    parser.add_argument("--listings", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--reference", type=Path, default=REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=MD_OUT)
    args = parser.parse_args(argv)
    generated_at = utc_now_iso()
    rows = build_exchange_source_audit(load_json(args.coverage), load_csv(args.listings), load_csv(args.reference))
    summary = summarize(rows)
    write_csv(args.csv_out, rows)
    args.json_out.write_text(
        json.dumps({"generated_at": generated_at, "summary": summary, "rows": rows}, indent=2),
        encoding="utf-8",
    )
    args.md_out.write_text(render_markdown(rows, summary, generated_at), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
