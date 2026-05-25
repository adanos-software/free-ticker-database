from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.enrich_global_identifiers import fetch_openfigi_by_isin

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_CANADA_FIGI_QUEUE_CSV = REPORTS_DIR / "canada_figi_queue.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_figi_batch_probe.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_figi_batch_probe.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_figi_batch_probe.md"

CSV_FIELDNAMES = [
    "batch_id",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "requested_exchange_hint",
    "openfigi_figi",
    "openfigi_ticker",
    "openfigi_exch_code",
    "openfigi_name",
    "openfigi_security_type",
    "candidate_count",
    "decision",
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


def select_queue_rows(rows: list[dict[str, str]], batch_id: str, limit: int | None) -> list[dict[str, str]]:
    selected = [row for row in rows if row.get("batch_id") == batch_id]
    selected.sort(key=lambda row: int(row.get("batch_position") or "0"))
    return selected[:limit] if limit is not None else selected


def select_candidate(row: dict[str, str], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    target_ticker = row.get("ticker", "").upper()
    hint = row.get("openfigi_exchange_hint", "")
    hinted = [candidate for candidate in candidates if str(candidate.get("exchCode", "")) == hint]
    ticker_and_hint = [candidate for candidate in hinted if str(candidate.get("ticker", "")).upper() == target_ticker]
    if ticker_and_hint:
        return ticker_and_hint[0]
    if hinted:
        return hinted[0]
    exact_ticker = [candidate for candidate in candidates if str(candidate.get("ticker", "")).upper() == target_ticker]
    if exact_ticker:
        return exact_ticker[0]
    return None


def decision_for(row: dict[str, str], candidate: dict[str, Any] | None) -> str:
    if candidate is None:
        return "no_openfigi_match"
    if str(candidate.get("exchCode", "")) != row.get("openfigi_exchange_hint", ""):
        return "reject_exchange_hint_mismatch"
    if not str(candidate.get("figi", "")).strip():
        return "reject_missing_figi"
    return "accept"


def build_probe_rows(
    queue_rows: list[dict[str, str]],
    candidates_by_isin: dict[str, list[dict[str, Any]]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in queue_rows:
        candidates = candidates_by_isin.get(row.get("isin", ""), [])
        candidate = select_candidate(row, candidates)
        rows.append(
            {
                "batch_id": row.get("batch_id", ""),
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "exchange": row.get("exchange", ""),
                "asset_type": row.get("asset_type", ""),
                "name": row.get("name", ""),
                "isin": row.get("isin", ""),
                "requested_exchange_hint": row.get("openfigi_exchange_hint", ""),
                "openfigi_figi": str(candidate.get("figi", "")) if candidate else "",
                "openfigi_ticker": str(candidate.get("ticker", "")) if candidate else "",
                "openfigi_exch_code": str(candidate.get("exchCode", "")) if candidate else "",
                "openfigi_name": str(candidate.get("name", "")) if candidate else "",
                "openfigi_security_type": str(candidate.get("securityType", "")) if candidate else "",
                "candidate_count": str(len(candidates)),
                "decision": decision_for(row, candidate),
            }
        )
    return rows


def summarize(rows: list[dict[str, str]], generated_at: str, errors: list[str]) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "decision_totals": dict(sorted(Counter(row["decision"] for row in rows).items())),
        "exchange_totals": dict(sorted(Counter(row["exchange"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "accepted_rows": sum(row["decision"] == "accept" for row in rows),
        "errors": errors,
        "policy": {
            "probe_only": "This report does not write identifiers_extended.csv.",
            "accept_gate": "A probe row accepts only when OpenFIGI returns a non-empty FIGI on the requested Canada exchange hint.",
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
        "# Canada FIGI Batch Probe",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report probes one Canada FIGI queue slice against OpenFIGI. It does not fill values.",
        "",
        "## Summary",
        "",
        f"- Probe rows: `{summary['rows']}`",
        f"- Accepted rows: `{summary['accepted_rows']}`",
        f"- Errors: `{len(summary['errors'])}`",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- This probe is read-only for dataset identifiers.",
            "- Accepted rows still need the normal FIGI collision gate before apply.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe a Canada FIGI queue batch against OpenFIGI without applying values.")
    parser.add_argument("--canada-figi-queue-csv", type=Path, default=DEFAULT_CANADA_FIGI_QUEUE_CSV)
    parser.add_argument("--batch-id", default="canada-figi-0001")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--openfigi-api-key", default="")
    parser.add_argument("--figi-delay-seconds", type=float, default=0.0)
    parser.add_argument("--figi-batch-size", type=int, default=10)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    queue_rows = select_queue_rows(load_csv(args.canada_figi_queue_csv), args.batch_id, args.limit)
    session = requests.Session()
    candidates_by_isin, errors = fetch_openfigi_by_isin(
        sorted({row["isin"] for row in queue_rows if row.get("isin")}),
        session=session,
        api_key=args.openfigi_api_key or os.environ.get("OPENFIGI_API_KEY") or None,
        delay_seconds=args.figi_delay_seconds,
        batch_size=args.figi_batch_size,
    )
    rows = build_probe_rows(queue_rows, candidates_by_isin)
    summary = summarize(rows, utc_now_iso(), errors)
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
                "accepted_rows": summary["accepted_rows"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "decision_totals": summary["decision_totals"],
                "errors": errors,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
