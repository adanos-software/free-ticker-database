"""Queue collision-adjusted recall work for official_full still_actionable venues.

Does not invent listings. It ranks official_full venues that remain below the
99.5% collision-adjusted recall target and records whether collision-free
masterfile supplements are already allowed for that venue.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

try:
    from scripts.build_masterfile_supplements import SUPPLEMENT_EXCHANGES, SUPPLEMENT_REFRESH_ONLY_EXCHANGES
except ModuleNotFoundError:  # pragma: no cover
    from build_masterfile_supplements import SUPPLEMENT_EXCHANGES, SUPPLEMENT_REFRESH_ONLY_EXCHANGES

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COVERAGE = ROOT / "data" / "reports" / "coverage_report.json"
DEFAULT_JSON = ROOT / "data" / "reports" / "official_full_recall_action_queue.json"
DEFAULT_CSV = ROOT / "data" / "reports" / "official_full_recall_action_queue.csv"
DEFAULT_MD = ROOT / "data" / "reports" / "official_full_recall_action_queue.md"

CSV_FIELDS = [
    "rank",
    "exchange",
    "collision_adjusted_recall_pct",
    "true_missing_excluding_collisions",
    "collision_hidden_missing",
    "official_recall_pct",
    "supplement_lane",
    "next_action",
]


def supplement_lane(exchange: str) -> str:
    if exchange in SUPPLEMENT_REFRESH_ONLY_EXCHANGES:
        return "refresh_only"
    if exchange in SUPPLEMENT_EXCHANGES:
        return "collision_free_supplement"
    return "not_in_supplement_allowlist"


def build_queue(coverage_report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in coverage_report.get("by_exchange") or []:
        if row.get("official_recall_decision") != "still_actionable":
            continue
        if row.get("venue_status") != "official_full":
            continue
        exchange = str(row.get("exchange") or "")
        if not exchange:
            continue
        true_missing = int(row.get("official_recall_true_missing_excluding_collisions") or 0)
        collision_hidden = int(row.get("official_recall_collision_hidden_missing") or 0)
        rows.append(
            {
                "exchange": exchange,
                "collision_adjusted_recall_pct": row.get("collision_adjusted_recall_pct"),
                "true_missing_excluding_collisions": true_missing,
                "collision_hidden_missing": collision_hidden,
                "official_recall_pct": row.get("official_recall_pct"),
                "supplement_lane": supplement_lane(exchange),
                "next_action": str(row.get("official_recall_next_action") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["true_missing_excluding_collisions"]),
            str(row["exchange"]),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows


def write_reports(queue: list[dict[str, Any]], *, json_out: Path, csv_out: Path, md_out: Path) -> None:
    json_out.write_text(json.dumps({"rows": queue, "count": len(queue)}, indent=2) + "\n", encoding="utf-8")
    with csv_out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(queue)
    lines = [
        "# Official-full recall action queue",
        "",
        "Official_full venues still below 99.5% collision-adjusted recall.",
        "Collision-hidden symbols are listed separately and must not be added as a second primary.",
        "",
        f"Rows: `{len(queue)}`",
        "",
        "| Rank | Exchange | Collision-adjusted recall | True missing | Collision-hidden | Supplement lane |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in queue:
        lines.append(
            f"| {row['rank']} | {row['exchange']} | {row['collision_adjusted_recall_pct']} | "
            f"{row['true_missing_excluding_collisions']} | {row['collision_hidden_missing']} | "
            f"{row['supplement_lane']} |"
        )
    lines.append("")
    md_out.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-report", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD)
    args = parser.parse_args(argv)
    coverage = json.loads(args.coverage_report.read_text(encoding="utf-8"))
    queue = build_queue(coverage)
    write_reports(queue, json_out=args.json_out, csv_out=args.csv_out, md_out=args.md_out)
    print(json.dumps({"count": len(queue), "md_out": str(args.md_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
