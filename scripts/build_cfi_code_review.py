from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.non_equity_guard import classify_non_equity_leakage

DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "cfi_code_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "cfi_code_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "cfi_code_review.md"

CSV_FIELDNAMES = [
    "source_key",
    "exchange",
    "ticker",
    "asset_type",
    "listing_status",
    "isin",
    "cfi",
    "cfi_prefix",
    "cfi_review_decision",
    "guard_decision",
    "leakage_class",
    "evidence_source",
    "source_gate",
    "recommended_action",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def cfi_review_decision(row: dict[str, str], guard: dict[str, str]) -> str:
    cfi = cfi_evidence_value(row)
    asset_type = row.get("asset_type", "")
    if guard.get("guard_decision") == "blocked_non_common_stock":
        return "blocked_non_common_stock_review"
    if asset_type == "Stock" and cfi.startswith("ES"):
        return "accepted_common_stock_cfi_evidence"
    if asset_type == "ETF" and cfi:
        return "review_gated_fund_or_etf_cfi_evidence"
    if cfi:
        return "review_gated_cfi_evidence"
    return "no_cfi_evidence"


def cfi_evidence_value(row: dict[str, str]) -> str:
    cfi = row.get("cfi", "").strip().upper()
    if cfi:
        return cfi
    parsed = urlparse(row.get("source_url", ""))
    query_values = parse_qs(parsed.query).get("cfi", [])
    if not query_values:
        return ""
    return str(query_values[0]).strip().upper()


def build_rows(reference_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in reference_rows:
        cfi = cfi_evidence_value(row)
        if not cfi:
            continue
        guard = classify_non_equity_leakage({**row, "cfi": cfi})
        rows.append(
            {
                "source_key": row.get("source_key", ""),
                "exchange": row.get("exchange", ""),
                "ticker": row.get("ticker", ""),
                "asset_type": row.get("asset_type", ""),
                "listing_status": row.get("listing_status", ""),
                "isin": row.get("isin", ""),
                "cfi": cfi,
                "cfi_prefix": cfi[:2],
                "cfi_review_decision": cfi_review_decision(row, guard),
                "guard_decision": guard.get("guard_decision", ""),
                "leakage_class": guard.get("leakage_class", ""),
                "evidence_source": guard.get("evidence_source", "cfi") or "cfi",
                "source_gate": (
                    "Official CFI evidence is a review gate only; it never fills identifiers, names, "
                    "sectors, categories, or listings without exact listing identity, checksum, and no-collision gates."
                ),
                "recommended_action": guard.get("recommended_action", "") or "keep_as_review_evidence",
            }
        )
    return sorted(rows, key=lambda item: (item["exchange"], item["ticker"], item["source_key"]))


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items()))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "cfi_evidence_rows": len(rows),
        "source_count": len({row["source_key"] for row in rows if row["source_key"]}),
        "exchange_count": len({row["exchange"] for row in rows if row["exchange"]}),
        "blocked_non_common_stock_review_rows": sum(
            1 for row in rows if row["cfi_review_decision"] == "blocked_non_common_stock_review"
        ),
        "decision_totals": counter_dict(Counter(row["cfi_review_decision"] for row in rows)),
        "asset_type_totals": counter_dict(Counter(row["asset_type"] for row in rows)),
        "cfi_prefix_totals": counter_dict(Counter(row["cfi_prefix"] for row in rows)),
        "guard_decision_totals": counter_dict(Counter(row["guard_decision"] for row in rows)),
        "policy": {
            "official_or_reviewed_only": "Rows come from official masterfile reference rows that expose CFI codes.",
            "no_auto_apply": "This report never applies data changes and never fills identifiers, sectors, categories, names, listings, or symbols.",
            "identity_gate": "Any future use of CFI evidence must still pass exact listing identity, valid identifier checksum where applicable, and no-collision gates.",
        },
    }


def build_payload(reference_csv: Path) -> dict[str, Any]:
    generated_at = utc_now_iso()
    rows = build_rows(load_csv(reference_csv))
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "reference_csv": display_path(reference_csv),
                "non_equity_guard": "scripts/lib/non_equity_guard.py",
            },
            "policy": "CFI evidence is review-only and cannot authorize automatic data fills.",
        },
        "summary": summarize(rows, generated_at),
        "rows": rows,
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def markdown_escape(value: Any) -> str:
    return str(value).replace("|", "\\|")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    rows = payload["rows"]
    lines = [
        "# CFI Code Review",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "This report surfaces official CFI evidence from masterfile rows as a review gate. It does not apply data changes.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| CFI evidence rows | {summary['cfi_evidence_rows']:,} |",
        f"| Sources | {summary['source_count']:,} |",
        f"| Exchanges | {summary['exchange_count']:,} |",
        f"| Blocked non-common-stock review rows | {summary['blocked_non_common_stock_review_rows']:,} |",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| `{markdown_escape(decision)}` | {count:,} |")
    lines.extend(
        [
            "",
            "## Top Rows",
            "",
            "| Source | Exchange | Ticker | Asset type | CFI | Decision |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in rows[:25]:
        lines.append(
            "| "
            f"{markdown_escape(row['source_key'])} | "
            f"{markdown_escape(row['exchange'])} | "
            f"{markdown_escape(row['ticker'])} | "
            f"{markdown_escape(row['asset_type'])} | "
            f"`{markdown_escape(row['cfi'])}` | "
            f"`{markdown_escape(row['cfi_review_decision'])}` |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- CFI evidence is official or review-gated evidence only.",
            "- This report does not fill identifiers, sectors, categories, names, listings, or symbols.",
            "- Any future apply path still requires exact listing identity, checksum where applicable, and no-collision gates.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build review-only CFI evidence report from official masterfiles.")
    parser.add_argument("--reference-csv", type=Path, default=DEFAULT_REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    payload = build_payload(args.reference_csv)
    write_csv(args.csv_out, payload["rows"])
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "cfi_evidence_rows": payload["summary"]["cfi_evidence_rows"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
