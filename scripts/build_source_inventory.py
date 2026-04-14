from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
REPORTS_DIR = DATA_DIR / "reports"
SOURCES_JSON = MASTERFILES_DIR / "sources.json"
SOURCE_CANDIDATES_JSON = MASTERFILES_DIR / "source_candidates.json"
COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "source_inventory_gap.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "source_inventory_gap.json"
DEFAULT_MD_OUT = REPORTS_DIR / "source_inventory_gap.md"

STATUS_RANK = {
    "missing": 0,
    "official_partial": 1,
    "manual_only": 2,
    "not_in_current_universe": 3,
    "official_full": 4,
}
PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2}

CSV_FIELDNAMES = [
    "priority_rank",
    "exchange",
    "venue_name",
    "country",
    "current_status",
    "tickers",
    "missing_isin",
    "missing_sector_or_category",
    "unresolved_findings",
    "official_source_count",
    "reference_scopes",
    "candidate_key",
    "candidate_scope",
    "provider",
    "asset_types",
    "expected_format",
    "source_url",
    "implementation_status",
    "priority",
    "review_needed",
    "blocker",
    "notes",
]


@dataclass(frozen=True)
class SourceInventoryRow:
    priority_rank: int
    exchange: str
    venue_name: str
    country: str
    current_status: str
    tickers: int
    missing_isin: int
    missing_sector_or_category: int
    unresolved_findings: int
    official_source_count: int
    reference_scopes: str
    candidate_key: str
    candidate_scope: str
    provider: str
    asset_types: str
    expected_format: str
    source_url: str
    implementation_status: str
    priority: str
    review_needed: bool
    blocker: str
    notes: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_coverage_lookup(coverage_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = coverage_report.get("by_exchange") or coverage_report.get("exchange_coverage") or []
    return {row["exchange"]: row for row in rows if row.get("exchange")}


def candidate_asset_types(candidate: dict[str, Any]) -> str:
    value = candidate.get("asset_types", [])
    if isinstance(value, list):
        return "|".join(str(item) for item in value)
    return str(value)


def coverage_metrics(exchange: str, coverage_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    coverage = coverage_lookup.get(exchange, {})
    tickers = int_value(coverage.get("tickers"))
    missing_isin = max(tickers - int_value(coverage.get("isin_coverage")), 0)
    missing_sector = max(tickers - int_value(coverage.get("sector_coverage")), 0)
    return {
        "current_status": coverage.get("venue_status", "not_in_current_universe"),
        "tickers": tickers,
        "missing_isin": missing_isin,
        "missing_sector_or_category": missing_sector,
        "unresolved_findings": int_value(coverage.get("unresolved_count")),
        "official_source_count": int_value(coverage.get("official_source_count")),
        "reference_scopes": "|".join(coverage.get("reference_scopes", []) or []),
    }


def row_sort_key(row: SourceInventoryRow) -> tuple[int, int, int, int, str, str]:
    return (
        STATUS_RANK.get(row.current_status, 9),
        PRIORITY_RANK.get(row.priority, 9),
        -(row.unresolved_findings or row.missing_isin or row.missing_sector_or_category or row.tickers),
        -row.tickers,
        row.exchange,
        row.candidate_key,
    )


def build_source_inventory(
    sources: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    coverage_report: dict[str, Any],
) -> list[SourceInventoryRow]:
    source_keys = {source.get("key") for source in sources}
    coverage_lookup = build_coverage_lookup(coverage_report)
    rows: list[SourceInventoryRow] = []
    candidate_exchanges: set[str] = set()

    for candidate in candidates:
        exchange = candidate.get("exchange", "")
        if not exchange:
            continue
        candidate_exchanges.add(exchange)
        metrics = coverage_metrics(exchange, coverage_lookup)
        implemented = candidate.get("key") in source_keys
        implementation_status = "implemented" if implemented else candidate.get("implementation_status", "todo")
        review_needed = (
            candidate.get("review_needed")
            if "review_needed" in candidate
            else candidate.get("candidate_scope") != "normalization_alias" and implementation_status != "implemented"
        )
        rows.append(
            SourceInventoryRow(
                priority_rank=0,
                exchange=exchange,
                venue_name=candidate.get("venue_name", ""),
                country=candidate.get("country", ""),
                current_status=metrics["current_status"],
                tickers=metrics["tickers"],
                missing_isin=metrics["missing_isin"],
                missing_sector_or_category=metrics["missing_sector_or_category"],
                unresolved_findings=metrics["unresolved_findings"],
                official_source_count=metrics["official_source_count"],
                reference_scopes=metrics["reference_scopes"],
                candidate_key=candidate.get("key", ""),
                candidate_scope=candidate.get("candidate_scope", ""),
                provider=candidate.get("provider", ""),
                asset_types=candidate_asset_types(candidate),
                expected_format=candidate.get("expected_format", ""),
                source_url=candidate.get("source_url", ""),
                implementation_status=implementation_status,
                priority=candidate.get("priority", "medium"),
                review_needed=review_needed,
                blocker=candidate.get("blocker", ""),
                notes=candidate.get("notes", ""),
            )
        )

    for exchange, coverage in coverage_lookup.items():
        status = coverage.get("venue_status", "")
        if status not in {"missing", "official_partial", "manual_only"} or exchange in candidate_exchanges:
            continue
        metrics = coverage_metrics(exchange, coverage_lookup)
        priority = "high" if metrics["unresolved_findings"] >= 100 or metrics["tickers"] >= 1000 else "medium"
        rows.append(
            SourceInventoryRow(
                priority_rank=0,
                exchange=exchange,
                venue_name="",
                country="",
                current_status=metrics["current_status"],
                tickers=metrics["tickers"],
                missing_isin=metrics["missing_isin"],
                missing_sector_or_category=metrics["missing_sector_or_category"],
                unresolved_findings=metrics["unresolved_findings"],
                official_source_count=metrics["official_source_count"],
                reference_scopes=metrics["reference_scopes"],
                candidate_key="",
                candidate_scope="needs_source_research",
                provider="",
                asset_types="Stock|ETF",
                expected_format="unknown",
                source_url="",
                implementation_status="todo",
                priority=priority,
                review_needed=True,
                blocker="candidate source not curated yet",
                notes="Existing current-scope venue still needs a more complete official source candidate.",
            )
        )

    ranked = sorted(rows, key=row_sort_key)
    return [SourceInventoryRow(**{**asdict(row), "priority_rank": index}) for index, row in enumerate(ranked, start=1)]


def summarize(rows: list[SourceInventoryRow], generated_at: str) -> dict[str, Any]:
    status_counts = Counter(row.current_status for row in rows)
    scope_counts = Counter(row.candidate_scope for row in rows)
    global_expansion_rows = [row for row in rows if is_global_expansion_candidate(row)]
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "current_status_counts": dict(sorted(status_counts.items())),
        "candidate_scope_counts": dict(sorted(scope_counts.items())),
        "todo_rows": sum(row.implementation_status != "implemented" for row in rows),
        "current_scope_candidates": sum(row.current_status != "not_in_current_universe" for row in rows),
        "global_expansion_candidates": len(global_expansion_rows),
        "high_priority_rows": sum(row.priority == "high" for row in rows),
    }


def rows_to_dicts(rows: list[SourceInventoryRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def write_csv(path: Path, rows: list[SourceInventoryRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["review_needed"] = "true" if row.review_needed else "false"
            writer.writerow(payload)


def format_table(rows: list[SourceInventoryRow], *, status: str | None = None, limit: int = 20) -> str:
    selected = [row for row in rows if status is None or row.current_status == status][:limit]
    if not selected:
        return "_No rows._\n"
    lines = [
        "| Rank | Exchange | Status | Tickers | ISIN gap | Metadata gap | Candidate | Provider | Blocker |",
        "|---|---|---|---:|---:|---:|---|---|---|",
    ]
    for row in selected:
        candidate = row.candidate_key or row.candidate_scope
        lines.append(
            f"| {row.priority_rank} | {row.exchange} | {row.current_status} | {row.tickers} | "
            f"{row.missing_isin} | {row.missing_sector_or_category} | {candidate} | {row.provider} | {row.blocker} |"
        )
    return "\n".join(lines) + "\n"


def is_global_expansion_candidate(row: SourceInventoryRow) -> bool:
    return row.current_status == "not_in_current_universe" and row.candidate_scope != "normalization_alias"


def render_markdown(rows: list[SourceInventoryRow], summary: dict[str, Any]) -> str:
    status_counts = ", ".join(f"{key}: `{value}`" for key, value in summary["current_status_counts"].items())
    scope_counts = ", ".join(f"{key}: `{value}`" for key, value in summary["candidate_scope_counts"].items())
    completed = [row for row in rows if row.implementation_status == "implemented"]
    return "\n".join(
        [
            "# Source Inventory Gap",
            "",
            f"Generated at: `{summary['generated_at']}`",
            "",
            "## Summary",
            "",
            f"- Rows: `{summary['rows']}`",
            f"- Current-scope candidates: `{summary['current_scope_candidates']}`",
            f"- Global expansion candidates: `{summary['global_expansion_candidates']}`",
            f"- Todo rows: `{summary['todo_rows']}`",
            f"- High-priority rows: `{summary['high_priority_rows']}`",
            f"- Status counts: {status_counts}",
            f"- Scope counts: {scope_counts}",
            "",
            "## Missing Current-Scope Sources",
            "",
            format_table(rows, status="missing", limit=30),
            "## Partial Current-Scope Sources",
            "",
            format_table(rows, status="official_partial", limit=30),
            "## Global Expansion Candidates",
            "",
            format_table([row for row in rows if is_global_expansion_candidate(row)], limit=30),
            "## Completed Or Reconciled Candidates",
            "",
            format_table(completed, limit=30),
            "## Policy",
            "",
            "- `sources.json` remains limited to implemented source feeds.",
            "- `source_candidates.json` tracks official candidates that still need endpoint discovery, parsing, or venue-code reconciliation.",
            "- Candidate rows are not authoritative exchange data until a parser writes audited `reference.csv` rows and coverage reports mark the venue as covered.",
            "",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the source inventory and missing-source gap report.")
    parser.add_argument("--sources-json", type=Path, default=SOURCES_JSON)
    parser.add_argument("--source-candidates-json", type=Path, default=SOURCE_CANDIDATES_JSON)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    generated_at = utc_now_iso()
    rows = build_source_inventory(
        load_json(args.sources_json, []),
        load_json(args.source_candidates_json, []),
        load_json(args.coverage_report_json, {}),
    )
    summary = summarize(rows, generated_at)

    write_csv(args.csv_out, rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": rows_to_dicts(rows)}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(render_markdown(rows, summary), encoding="utf-8")

    print(
        json.dumps(
            {
                **summary,
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
