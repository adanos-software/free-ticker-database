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

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import normalize_sector


DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_WEAK_SECTOR_REVIEW_CSV = REPORTS_DIR / "weak_sector_residual_review.csv"
DEFAULT_METADATA_UPDATES_CSV = DATA_DIR / "review_overrides" / "metadata_updates.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "ngx_official_sector_apply_report.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "ngx_official_sector_apply_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "ngx_official_sector_apply_report.md"

NGX_OFFICIAL_SECTOR_MAP = {
    "UTILITIES": "Utilities",
}

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "official_sector",
    "sector_update",
    "decision",
    "reason",
    "official_source_context",
    "mapping_review_context",
    "apply_gate_context",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def official_source_context_for(row: dict[str, str]) -> str:
    return (
        f"exchange={row.get('exchange', '') or 'none'};"
        f"official_sector={row.get('official_sector', '') or 'none'}"
    )


def mapping_review_context_for(row: dict[str, str]) -> str:
    return (
        f"official_sector={row.get('official_sector', '') or 'none'};"
        f"sector_update={row.get('sector_update', '') or 'none'};"
        f"mapping_supported={'true' if row.get('sector_update') else 'false'}"
    )


def apply_gate_context_for(row: dict[str, str]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'}"
    )


def build_apply_rows(review_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in review_rows:
        if row.get("exchange") != "NGX" or row.get("residual_decision") != "official_sector_available_review_apply":
            continue
        official_sector = row.get("official_masterfile_sector_values", "").strip()
        mapped = NGX_OFFICIAL_SECTOR_MAP.get(official_sector, "")
        sector_update = normalize_sector(mapped, "Stock") if mapped else ""
        decision = "apply" if sector_update else "skip"
        reason = "official_ngx_sector_mapped_to_canonical" if sector_update else "unsupported_broad_ngx_sector_label"
        report_row = {
            "listing_key": row.get("listing_key", ""),
            "ticker": row.get("ticker", ""),
            "exchange": row.get("exchange", ""),
            "name": row.get("name", ""),
            "official_sector": official_sector,
            "sector_update": sector_update,
            "decision": decision,
            "reason": reason,
        }
        report_row["official_source_context"] = official_source_context_for(report_row)
        report_row["mapping_review_context"] = mapping_review_context_for(report_row)
        report_row["apply_gate_context"] = apply_gate_context_for(report_row)
        rows.append(report_row)
    return sorted(rows, key=lambda row: (row["decision"], row["ticker"], row["listing_key"]))


def build_metadata_updates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for row in rows:
        if row.get("decision") != "apply" or not row.get("sector_update"):
            continue
        updates.append(
            {
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": row["sector_update"],
                "confidence": "0.97",
                "reason": (
                    "Official NGX company profile sector mapped to canonical stock_sector; accepted only for "
                    "explicit one-to-one venue-specific sector mappings after listing_key and official-source gates."
                ),
            }
        )
    return updates


def summarize(rows: list[dict[str, str]], generated_at: str, applied: bool, written_updates: int) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "applied": applied,
        "rows": len(rows),
        "written_updates": written_updates,
        "decision_totals": dict(sorted(Counter(row["decision"] for row in rows).items())),
        "official_sector_totals": dict(sorted(Counter(row["official_sector"] for row in rows).items())),
        "sector_update_totals": dict(sorted(Counter(row["sector_update"] for row in rows if row["sector_update"]).items())),
        "policy": {
            "source": "Only weak_sector_residual_review rows with exchange=NGX and residual_decision=official_sector_available_review_apply are eligible.",
            "mapping": "Only explicit one-to-one NGX official sector mappings are applied; broad labels remain blank.",
            "no_guessing": "No sector is inferred from ticker, issuer name, or broad NGX category labels.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# NGX Official Sector Apply Report",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report applies only explicit one-to-one canonical mappings from official NGX sector values. It leaves broad labels blank.",
        "",
        "## Summary",
        "",
        f"- Rows: `{summary['rows']}`",
        f"- Applied: `{summary['applied']}`",
        f"- Written updates: `{summary['written_updates']}`",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Official Sectors", "", "| Official sector | Rows |", "|---|---:|"])
    for sector, count in summary["official_sector_totals"].items():
        lines.append(f"| {sector} | {count} |")
    lines.extend(["", "## Applied Canonical Sectors", "", "| Canonical sector | Rows |", "|---|---:|"])
    for sector, count in summary["sector_update_totals"].items():
        lines.append(f"| {sector} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Broad NGX labels such as SERVICES, CONGLOMERATES, and INVESTMENT are not mapped.",
            "- Only direct official venue sector evidence with an explicit canonical mapping is eligible.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply safe NGX official sector mappings from weak sector review.")
    parser.add_argument("--weak-sector-review-csv", type=Path, default=DEFAULT_WEAK_SECTOR_REVIEW_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_apply_rows(load_csv(args.weak_sector_review_csv))
    updates = build_metadata_updates(rows)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)
    summary = summarize(rows, utc_now_iso(), args.apply, len(updates) if args.apply else 0)
    write_csv(args.csv_out, rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": rows}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.md_out, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "written_updates": summary["written_updates"],
                "decision_totals": summary["decision_totals"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
