from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_CANADA_RESIDUAL_CSV = REPORTS_DIR / "canada_residual_review.csv"
DEFAULT_CANADA_SCOPE_CSV = REPORTS_DIR / "canada_scope_review_queue.csv"
DEFAULT_CANADA_FIGI_QUEUE_JSON = REPORTS_DIR / "canada_figi_queue.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_improvement_action_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_improvement_action_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_improvement_action_queue.md"

CSV_FIELDNAMES = [
    "campaign",
    "exchange",
    "review_queue",
    "official_sources",
    "rows",
    "action_queue",
    "evidence_required",
    "review_strategy",
    "recommended_next_source",
    "source_gate",
    "example_listing_keys",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def residual_campaign(row: dict[str, str]) -> str:
    queue = row.get("canada_resolution_queue", "")
    if queue.startswith("core_exclusion_candidate"):
        return "canada_scope_blocker"
    if queue.startswith("missing_isin"):
        return "canada_isin_source_gap"
    if queue.startswith("reviewed_openfigi"):
        return "canada_figi_reviewed_source_gap"
    if queue.startswith("metadata"):
        return "canada_metadata_source_gap"
    return "canada_manual_review"


def action_for(campaign: str, queue: str) -> str:
    if campaign == "canada_scope_blocker":
        return "decide_scope_before_identifier_or_metadata_enrichment"
    if campaign == "canada_isin_source_gap":
        return "seek_official_csd_issuer_or_transfer_agent_isin_source"
    if campaign == "canada_figi_reviewed_source_gap":
        return "keep_figi_blank_until_stronger_reviewed_figi_source"
    if campaign == "canada_metadata_source_gap":
        return "keep_metadata_blank_until_stronger_official_source"
    return "manual_canada_review"


def join_unique(rows: list[dict[str, str]], field: str) -> str:
    return " | ".join(sorted({row.get(field, "") for row in rows if row.get(field, "")}))


def build_action_rows(residual_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in residual_rows:
        campaign = residual_campaign(row)
        queue = row.get("canada_resolution_queue", "")
        grouped[
            (
                campaign,
                row.get("exchange", ""),
                queue,
                row.get("official_masterfile_sources", "") or "none",
            )
        ].append(row)

    action_rows: list[dict[str, str]] = []
    for (campaign, exchange, queue, official_sources), group in grouped.items():
        examples = sorted({row.get("listing_key", "") for row in group if row.get("listing_key")})[:10]
        action_rows.append(
            {
                "campaign": campaign,
                "exchange": exchange,
                "review_queue": queue,
                "official_sources": official_sources,
                "rows": str(len(group)),
                "action_queue": action_for(campaign, queue),
                "evidence_required": join_unique(group, "queue_evidence_required")
                or join_unique(group, "verification_evidence_required"),
                "review_strategy": join_unique(group, "review_strategy"),
                "recommended_next_source": join_unique(group, "recommended_next_source"),
                "source_gate": join_unique(group, "source_gate"),
                "example_listing_keys": "|".join(examples),
            }
        )
    return sorted(
        action_rows,
        key=lambda row: (
            action_rank(row["campaign"]),
            row["exchange"],
            row["review_queue"],
            row["official_sources"],
        ),
    )


def action_rank(campaign: str) -> int:
    return {
        "canada_scope_blocker": 0,
        "canada_isin_source_gap": 1,
        "canada_figi_reviewed_source_gap": 2,
        "canada_metadata_source_gap": 3,
    }.get(campaign, 9)


def count_rows(action_rows: list[dict[str, str]], field: str) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for row in action_rows:
        totals[row[field]] += int(row["rows"])
    return dict(sorted(totals.items()))


def summarize(
    *,
    action_rows: list[dict[str, str]],
    scope_rows: list[dict[str, str]],
    figi_queue_payload: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    figi_summary = figi_queue_payload.get("summary", {})
    return {
        "generated_at": generated_at,
        "batches": len(action_rows),
        "underlying_review_rows": sum(int(row["rows"]) for row in action_rows),
        "campaign_totals": count_rows(action_rows, "campaign"),
        "exchange_totals": count_rows(action_rows, "exchange"),
        "action_queue_totals": count_rows(action_rows, "action_queue"),
        "scope_queue_rows": len(scope_rows),
        "figi_queue_rows": figi_summary.get("rows", figi_summary.get("queue_rows", 0)),
        "figi_excluded_reviewed_source_gaps": figi_summary.get(
            "excluded_openfigi_gap_rows",
            figi_summary.get("excluded_reviewed_source_gaps", 0),
        ),
        "direct_identifier_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "policy": {
            "tmx_first": "TMX/Cboe Canada official listing context is the first gate; issuer, CSD, transfer-agent, prospectus, or reviewed identifier sources are required for missing ISINs.",
            "figi_no_repeat_probe": "Reviewed OpenFIGI no-match or cross-ISIN rows stay excluded until stronger reviewed FIGI evidence exists.",
            "scope_before_fill": "Core-exclusion candidates must be decided as core, extended, or excluded before ISIN, FIGI, sector, or ETF-category enrichment.",
        },
    }


def build_payload(
    *,
    residual_csv: Path,
    scope_csv: Path,
    figi_queue_json: Path,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    residual_rows = load_csv(residual_csv)
    scope_rows = load_csv(scope_csv)
    figi_payload = load_json(figi_queue_json)
    action_rows = build_action_rows(residual_rows)
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "residual_csv": display_path(residual_csv),
                "scope_csv": display_path(scope_csv),
                "figi_queue_json": display_path(figi_queue_json),
            },
        },
        "summary": summarize(
            action_rows=action_rows,
            scope_rows=scope_rows,
            figi_queue_payload=figi_payload,
            generated_at=generated_at,
        ),
        "items": action_rows,
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def markdown_escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# Canada Improvement Action Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: this report does not apply ISIN, FIGI, sector, category, name, or scope changes. It groups TSX/TSXV/NEO residuals into official-evidence batches.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Batches | {summary['batches']} |",
        f"| Underlying review rows | {summary['underlying_review_rows']} |",
        f"| Scope queue rows | {summary['scope_queue_rows']} |",
        f"| Active FIGI queue rows | {summary['figi_queue_rows']} |",
        f"| Excluded reviewed FIGI source gaps | {summary['figi_excluded_reviewed_source_gaps']} |",
        f"| Direct identifier apply allowed rows | {summary['direct_identifier_apply_allowed_rows']} |",
        "",
        "## Campaigns",
        "",
        "| Campaign | Rows |",
        "| --- | ---: |",
    ]
    for campaign, count in summary["campaign_totals"].items():
        lines.append(f"| {campaign} | {count} |")
    lines.extend(["", "## Exchanges", "", "| Exchange | Rows |", "| --- | ---: |"])
    for exchange, count in summary["exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(
        [
            "",
            "## Batches",
            "",
            "| Campaign | Exchange | Queue | Source | Rows | Action | Evidence required |",
            "| --- | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for row in payload["items"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["campaign"]),
                    markdown_escape(row["exchange"]),
                    markdown_escape(row["review_queue"]),
                    markdown_escape(row["official_sources"]),
                    row["rows"],
                    markdown_escape(row["action_queue"]),
                    markdown_escape(row["evidence_required"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Gates",
            "",
            "- Direct identifier apply allowed rows: `0`.",
            "- Active Canada FIGI queue rows: `0`; reviewed OpenFIGI gaps stay excluded.",
            "- Scope blockers remain blocked before identifier or metadata enrichment.",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], csv_out: Path, json_out: Path, md_out: Path) -> None:
    write_csv(csv_out, payload["items"])
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(render_markdown(payload), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a consolidated Canada improvement action queue.")
    parser.add_argument("--residual-csv", type=Path, default=DEFAULT_CANADA_RESIDUAL_CSV)
    parser.add_argument("--scope-csv", type=Path, default=DEFAULT_CANADA_SCOPE_CSV)
    parser.add_argument("--figi-queue-json", type=Path, default=DEFAULT_CANADA_FIGI_QUEUE_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(
        residual_csv=args.residual_csv,
        scope_csv=args.scope_csv,
        figi_queue_json=args.figi_queue_json,
    )
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        f"Wrote {len(payload['items'])} Canada action batches "
        f"for {payload['summary']['underlying_review_rows']} review rows."
    )


if __name__ == "__main__":
    main()
