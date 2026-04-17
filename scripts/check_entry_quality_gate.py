from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ENTRY_QUALITY_CSV = ROOT / "data" / "reports" / "entry_quality.csv"
WARN_ALLOWLIST_CSV = ROOT / "data" / "reports" / "entry_quality_warn_allowlist.csv"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def allowed_warn_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {row["listing_key"] for row in load_csv(path) if row.get("listing_key")}


def check_entry_quality_gate(
    entry_quality_rows: list[dict[str, str]],
    allowed_warns: set[str],
) -> dict[str, object]:
    quarantined = [row for row in entry_quality_rows if row.get("quality_status") == "quarantine"]
    warn_rows = [row for row in entry_quality_rows if row.get("quality_status") == "warn"]
    warn_keys = {row["listing_key"] for row in warn_rows}
    unexpected_warns = sorted(warn_keys - allowed_warns)
    stale_allowlist = sorted(allowed_warns - warn_keys)

    return {
        "passed": not quarantined and not unexpected_warns,
        "quarantine_count": len(quarantined),
        "warn_count": len(warn_rows),
        "allowed_warn_count": len(allowed_warns),
        "unexpected_warn_count": len(unexpected_warns),
        "stale_allowlist_count": len(stale_allowlist),
        "unexpected_warns": unexpected_warns[:50],
        "quarantined": [row["listing_key"] for row in quarantined[:50]],
        "stale_allowlist": stale_allowlist[:50],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail CI when new entry-quality hard warnings appear.")
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--warn-allowlist-csv", type=Path, default=WARN_ALLOWLIST_CSV)
    parser.add_argument(
        "--fail-on-stale-allowlist",
        action="store_true",
        help="Also fail when allowlisted keys are no longer current warnings.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = check_entry_quality_gate(
        load_csv(args.entry_quality_csv),
        allowed_warn_keys(args.warn_allowlist_csv),
    )
    if args.fail_on_stale_allowlist and result["stale_allowlist_count"]:
        result["passed"] = False
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
