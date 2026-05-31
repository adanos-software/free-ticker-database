from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ENTRY_QUALITY_CSV = ROOT / "data" / "reports" / "entry_quality.csv"
WARN_ALLOWLIST_CSV = ROOT / "data" / "reports" / "entry_quality_warn_allowlist.csv"
DEFAULT_JSON_OUT = ROOT / "data" / "reports" / "entry_quality_gate.json"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def write_json_report(path: Path, result: dict[str, object], *, entry_quality_csv: Path, warn_allowlist_csv: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_meta": {
            "generated_at": utc_now_iso(),
            "entry_quality_csv": str(entry_quality_csv.relative_to(ROOT) if entry_quality_csv.is_relative_to(ROOT) else entry_quality_csv),
            "warn_allowlist_csv": str(
                warn_allowlist_csv.relative_to(ROOT) if warn_allowlist_csv.is_relative_to(ROOT) else warn_allowlist_csv
            ),
            "policy": "Command evidence for the entry-quality release gate. New warn rows or quarantine rows fail the gate.",
        },
        **result,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail CI when new entry-quality hard warnings appear.")
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--warn-allowlist-csv", type=Path, default=WARN_ALLOWLIST_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--no-json-out", action="store_true", help="Print the gate result without writing a JSON report.")
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
    if not args.no_json_out:
        write_json_report(
            args.json_out,
            result,
            entry_quality_csv=args.entry_quality_csv,
            warn_allowlist_csv=args.warn_allowlist_csv,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
