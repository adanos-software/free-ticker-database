from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import classify_alias_for_natural_language, duplicate_alias_counts

DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
ALIASES_CSV = DATA_DIR / "aliases.csv"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_CSV_OUT = REPORTS_DIR / "alias_quality.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "alias_quality.json"
DEFAULT_MD_OUT = REPORTS_DIR / "alias_quality.md"

FIELDNAMES = [
    "ticker",
    "alias",
    "alias_type",
    "status",
    "detection_policy",
    "confidence",
    "reason",
    "duplicate_ticker_count",
]


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


def ticker_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["ticker"]: row for row in rows}


def build_alias_quality_rows(
    tickers: list[dict[str, str]],
    aliases: list[dict[str, str]],
) -> list[dict[str, str]]:
    tickers_by_symbol = ticker_lookup(tickers)
    duplicate_counts = duplicate_alias_counts(aliases)
    rows: list[dict[str, str]] = []

    for alias_row in aliases:
        ticker = alias_row["ticker"]
        ticker_row = tickers_by_symbol.get(ticker, {})
        wkns = {ticker_row.get("wkn", "")} if ticker_row.get("wkn") else set()
        decision = classify_alias_for_natural_language(
            alias=alias_row["alias"],
            alias_type=alias_row["alias_type"],
            ticker=ticker,
            duplicate_ticker_count=duplicate_counts.get(alias_row["alias"], 1),
            isin=ticker_row.get("isin", ""),
            wkns=wkns,
        )
        rows.append(
            {
                "ticker": ticker,
                "alias": alias_row["alias"],
                "alias_type": alias_row["alias_type"],
                "status": decision.status,
                "detection_policy": decision.detection_policy,
                "confidence": decision.confidence,
                "reason": decision.reason,
                "duplicate_ticker_count": str(duplicate_counts.get(alias_row["alias"], 1)),
            }
        )

    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, object]:
    status_counts = Counter(row["status"] for row in rows)
    policy_counts = Counter(row["detection_policy"] for row in rows)
    reason_counts = Counter(row["reason"] for row in rows)
    alias_type_status: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        alias_type_status[row["alias_type"]][row["status"]] += 1

    return {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(rows),
            "source_files": {
                "tickers_csv": display_path(TICKERS_CSV),
                "aliases_csv": display_path(ALIASES_CSV),
            },
        },
        "summary": {
            "status_counts": dict(status_counts.most_common()),
            "detection_policy_counts": dict(policy_counts.most_common()),
            "reason_counts": dict(reason_counts.most_common()),
            "alias_type_status_counts": {
                alias_type: dict(counter.most_common())
                for alias_type, counter in sorted(alias_type_status.items())
            },
        },
        "examples": {
            status: [row for row in rows if row["status"] == status][:25]
            for status in ("reject", "review", "accept")
        },
    }


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, object]) -> None:
    summary = payload["summary"]  # type: ignore[index]
    lines = [
        "# Alias Quality Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",  # type: ignore[index]
        "",
        "This report classifies `data/aliases.csv` for Natural-Language detection safety.",
        "Identifier aliases remain useful for lookup, but are rejected for mention detection.",
        "",
        "## Status Counts",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["status_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {status} | {count:,} |")

    lines.extend(["", "## Detection Policies", "", "| Policy | Rows |", "|---|---:|"])
    for policy, count in summary["detection_policy_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {policy} | {count:,} |")

    lines.extend(["", "## Top Reasons", "", "| Reason | Rows |", "|---|---:|"])
    for reason, count in summary["reason_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {reason} | {count:,} |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build alias quality report for mention-detection safety.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = build_alias_quality_rows(load_csv(args.tickers_csv), load_csv(args.aliases_csv))
    generated_at = utc_now()
    payload = summarize(rows, generated_at)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "status_counts": payload["summary"]["status_counts"],  # type: ignore[index]
                "detection_policy_counts": payload["summary"]["detection_policy_counts"],  # type: ignore[index]
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
