from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_COVERAGE_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_ENTRY_QUALITY_GATE_JSON = REPORTS_DIR / "entry_quality_gate.json"
DEFAULT_IMPROVEMENT_CAMPAIGNS_JSON = REPORTS_DIR / "improvement_campaigns.json"
DEFAULT_RELEASE_ACCEPTANCE_JSON = REPORTS_DIR / "release_acceptance.json"
DEFAULT_SOURCE_GAP_CLASSIFICATION_JSON = REPORTS_DIR / "source_gap_classification.json"
DEFAULT_VALIDATION_JSON = REPORTS_DIR / "validation_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "pr_review_summary.md"

REVIEW_FILES = [
    "data/reports/release_acceptance.md",
    "data/reports/improvement_campaigns.md",
    "data/reports/improvement_deltas.md",
    "data/reports/coverage_report.md",
    "data/reports/source_gap_classification.md",
    "data/reports/source_of_truth_decisions.md",
    "data/reports/masterfile_collision_review.md",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def int_value(value: Any, default: int = 0) -> int:
    if value in {"", None}:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def format_int(value: Any) -> str:
    return f"{int_value(value):,}"


def summary(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("summary", {})
    return value if isinstance(value, dict) else {}


def campaign_rows(campaigns: dict[str, Any]) -> list[dict[str, Any]]:
    rows = campaigns.get("next_review_batches", [])
    return rows if isinstance(rows, list) else []


def campaign_names(campaigns: dict[str, Any]) -> dict[str, str]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get("campaign_key", "")): str(row.get("name", ""))
        for row in rows
        if row.get("campaign_key") and row.get("name")
    }


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def passed_failed(passed: bool) -> str:
    return "passed" if passed else "failed"


def entry_quality_gate_result(entry_quality_gate: dict[str, Any]) -> str:
    unexpected_warn_count = int_value(entry_quality_gate.get("unexpected_warn_count"))
    quarantine_count = int_value(entry_quality_gate.get("quarantine_count"))
    passed = unexpected_warn_count == 0 and quarantine_count == 0
    return (
        f"{passed_failed(passed)}; "
        f"`unexpected_warn_count={unexpected_warn_count}`, "
        f"`quarantine_count={quarantine_count}`"
    )


def render_markdown(
    *,
    coverage: dict[str, Any],
    campaigns: dict[str, Any],
    entry_quality_gate: dict[str, Any],
    release_acceptance: dict[str, Any],
    source_gap_classification: dict[str, Any],
    validation: dict[str, Any],
    generated_at: str,
) -> str:
    global_summary = coverage.get("global", {})
    freshness = coverage.get("source_freshness_summary", {})
    release_summary = summary(release_acceptance)
    validation_summary = summary(validation)
    source_gap_summary = summary(source_gap_classification)
    campaign_name_by_key = campaign_names(campaigns)

    lines = [
        "# PR Review Summary",
        "",
        f"Generated: `{generated_at}`",
        "",
        "This PR improves the ticker database through source-gated review workflows, refreshed official masterfile evidence, and release acceptance checks. It does not authorize guessed ISINs, sectors, ETF categories, names, listings, or symbol changes.",
        "",
        "## Scope",
        "",
        "- Added and updated review workflows for B3 residuals, OTC scope, Canada FIGI/ISIN review, ASX residuals, weak-sector venues, masterfile collisions, symbol-change review, OHLCV plausibility, source freshness, and release acceptance.",
        "- Regenerated listing-keyed review artifacts under `data/reports/` so unresolved gaps remain explicit source gaps instead of inferred data fills.",
        "- Refreshed selected official exchange-directory sources through controlled network refreshes with generated-at and row-count evidence.",
        "- Added refresh safety for partial masterfile updates so an unavailable selected source does not silently delete existing reference rows.",
        "",
        "## Data Safety",
        "",
        "- No uncertain identifier, sector, category, name, listing, or symbol value is filled from symbol shape, issuer-name shape, peer instruments, or stale secondary evidence.",
        "- Review artifacts are gates, not automatic apply instructions. Rows remain blank unless exact listing-keyed official evidence passes the relevant source gate.",
        "- OHLCV evidence is plausibility-only and never authorizes canonical data changes.",
        "- Freshness evidence only proves source age and row count; it does not authorize data changes by itself.",
        "",
        "## Current Evidence",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Tickers | {format_int(global_summary.get('tickers'))} |",
        f"| Listing keys | {format_int(global_summary.get('listing_keys'))} |",
        f"| Official masterfile symbols | {format_int(global_summary.get('official_masterfile_symbols'))} |",
        f"| Official masterfile matches | {format_int(global_summary.get('official_masterfile_matches'))} |",
        f"| Official masterfile collisions queued | {format_int(global_summary.get('official_masterfile_collisions'))} |",
        f"| Official masterfile missing queued | {format_int(global_summary.get('official_masterfile_missing'))} |",
        f"| Source gaps | {format_int(source_gap_summary.get('rows'))} |",
        f"| Entry-quality warnings | {format_int(entry_quality_gate.get('warn_count'))} |",
        f"| Quarantine rows | {format_int(entry_quality_gate.get('quarantine_count'))} |",
        "",
        "## Acceptance",
        "",
        "| Gate | Result |",
        "|---|---|",
        "| `python -m pytest tests/ -q` | run before release; not captured by generated report JSON |",
        f"| `python scripts/check_entry_quality_gate.py` | {entry_quality_gate_result(entry_quality_gate)} |",
        f"| `python scripts/validate_database.py` | {passed_failed(bool(validation.get('passed')))}; `failed_error_gates={int_value(validation_summary.get('failed_error_gates'))}` |",
        f"| `python scripts/build_release_acceptance_report.py` | {passed_failed(bool(release_acceptance.get('passed')))}; `{int_value(release_summary.get('passed_criteria'))}/{int_value(release_summary.get('criteria'))}` |",
        "| CRLF-aware `git diff --check` | run before release; not captured by generated report JSON |",
        "",
        "## Freshness",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Fresh sources | {format_int(freshness.get('freshness_status_totals', {}).get('fresh'))} |",
        f"| Old sources | {format_int(freshness.get('freshness_status_totals', {}).get('old'))} |",
        f"| Remaining old P1 exchange-directory sources | {format_int(freshness.get('old_official_exchange_directory_count'))} |",
        "",
        "Remaining P1 exchange-directory refresh work:",
        "",
    ]
    for row in freshness.get("top_old_official_exchange_directories", []):
        key = row.get("key", "")
        if key:
            lines.append(f"- `{key}`")
    lines.extend(
        [
            "",
            "## Review Backlog",
            "",
            "| Campaign | Rows | Status |",
            "|---|---:|---|",
        ]
    )
    for row in campaign_rows(campaigns):
        campaign_key = str(row.get("campaign_key", ""))
        campaign = campaign_name_by_key.get(campaign_key, campaign_key.replace("_", " ").title())
        lines.append(
            f"| {campaign} | {format_int(row.get('artifact_rows'))} | {row.get('status', '')} |"
        )
    lines.extend(
        [
            "",
            "## Primary Review Files",
            "",
        ]
    )
    for path in REVIEW_FILES:
        lines.append(f"- `{path}`")
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a compact PR review summary from generated report JSON files.")
    parser.add_argument("--coverage-json", type=Path, default=DEFAULT_COVERAGE_JSON)
    parser.add_argument("--entry-quality-gate-json", type=Path, default=DEFAULT_ENTRY_QUALITY_GATE_JSON)
    parser.add_argument("--improvement-campaigns-json", type=Path, default=DEFAULT_IMPROVEMENT_CAMPAIGNS_JSON)
    parser.add_argument("--release-acceptance-json", type=Path, default=DEFAULT_RELEASE_ACCEPTANCE_JSON)
    parser.add_argument("--source-gap-classification-json", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_JSON)
    parser.add_argument("--validation-json", type=Path, default=DEFAULT_VALIDATION_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    md = render_markdown(
        coverage=load_json(args.coverage_json),
        campaigns=load_json(args.improvement_campaigns_json),
        entry_quality_gate=load_json(args.entry_quality_gate_json),
        release_acceptance=load_json(args.release_acceptance_json),
        source_gap_classification=load_json(args.source_gap_classification_json),
        validation=load_json(args.validation_json),
        generated_at=utc_now_iso(),
    )
    args.md_out.write_text(md, encoding="utf-8")
    print(json.dumps({"md_out": display_path(args.md_out)}, indent=2))


if __name__ == "__main__":
    main()
