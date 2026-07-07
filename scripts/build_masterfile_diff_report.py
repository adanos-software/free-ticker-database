from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.lib.dataio import display_path, load_csv, write_json
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import display_path, load_csv, write_json


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CURRENT_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
REPORT_JSON = DATA_DIR / "reports" / "masterfile_rotation_diff.json"
REPORT_MD = DATA_DIR / "reports" / "masterfile_rotation_diff.md"
IDENTITY_FIELDS = ["source_key", "exchange", "ticker"]
COMPARE_FIELDS = ["name", "isin", "asset_type", "listing_status", "reference_scope", "official", "sector"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def row_key(row: dict[str, str]) -> tuple[str, str, str]:
    return tuple(row.get(field, "") for field in IDENTITY_FIELDS)  # type: ignore[return-value]


def build_diff(
    *,
    previous_reference_csv: Path,
    current_reference_csv: Path = CURRENT_REFERENCE_CSV,
    report_json: Path = REPORT_JSON,
    report_md: Path = REPORT_MD,
) -> dict[str, Any]:
    previous = {row_key(row): row for row in load_csv(previous_reference_csv)}
    current = {row_key(row): row for row in load_csv(current_reference_csv)}

    new_rows = [current[key] for key in sorted(current.keys() - previous.keys())]
    vanished_rows = [previous[key] for key in sorted(previous.keys() - current.keys())]
    changed_rows: list[dict[str, Any]] = []
    for key in sorted(previous.keys() & current.keys()):
        before = previous[key]
        after = current[key]
        changes = {
            field: {"before": before.get(field, ""), "after": after.get(field, "")}
            for field in COMPARE_FIELDS
            if before.get(field, "") != after.get(field, "")
        }
        if changes:
            change_types = []
            if "name" in changes:
                change_types.append("name_change")
            if "isin" in changes:
                change_types.append("isin_change")
            other_fields = sorted(set(changes) - {"name", "isin"})
            if other_fields:
                change_types.append("field_change")
            changed_rows.append(
                {
                    "source_key": key[0],
                    "exchange": key[1],
                    "ticker": key[2],
                    "change_types": change_types,
                    "changes": changes,
                }
            )

    summary = {
        "generated_at": utc_now_iso(),
        "previous_reference_csv": display_path(previous_reference_csv, ROOT),
        "current_reference_csv": display_path(current_reference_csv, ROOT),
        "new_rows": len(new_rows),
        "vanished_rows": len(vanished_rows),
        "changed_rows": len(changed_rows),
        "new_by_source": dict(sorted(Counter(row.get("source_key", "") for row in new_rows).items())),
        "vanished_by_source": dict(sorted(Counter(row.get("source_key", "") for row in vanished_rows).items())),
        "changed_by_type": dict(sorted(Counter(change_type for row in changed_rows for change_type in row["change_types"]).items())),
        "vanished_policy": "feed_delisting_classifier_not_direct_deletion",
    }
    report = {"summary": summary, "new": new_rows, "vanished": vanished_rows, "changed": changed_rows}
    write_json(report_json, report)
    write_markdown(report_md, report)
    return report


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    summary = report["summary"]
    lines = [
        "# Masterfile Rotation Diff",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- New rows: `{summary['new_rows']}`",
        f"- Vanished rows: `{summary['vanished_rows']}`",
        f"- Changed rows: `{summary['changed_rows']}`",
        f"- Vanished policy: `{summary['vanished_policy']}`",
        "",
        "## Changed By Type",
        "",
        "| Type | Rows |",
        "|---|---:|",
    ]
    for change_type, count in summary["changed_by_type"].items():
        lines.append(f"| {change_type} | {count} |")
    lines.extend(["", "## New By Source", "", "| Source | Rows |", "|---|---:|"])
    for source_key, count in summary["new_by_source"].items():
        lines.append(f"| {source_key} | {count} |")
    lines.extend(["", "## Vanished By Source", "", "| Source | Rows |", "|---|---:|"])
    for source_key, count in summary["vanished_by_source"].items():
        lines.append(f"| {source_key} | {count} |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build before/after diffs for a masterfile refresh batch.")
    parser.add_argument("--previous-reference", type=Path, required=True)
    parser.add_argument("--current-reference", type=Path, default=CURRENT_REFERENCE_CSV)
    parser.add_argument("--report-json", type=Path, default=REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=REPORT_MD)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build_diff(
        previous_reference_csv=args.previous_reference,
        current_reference_csv=args.current_reference,
        report_json=args.report_json,
        report_md=args.report_md,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
