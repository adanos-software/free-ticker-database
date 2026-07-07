"""Anti-drift freshness report.

Companion to the daily symbol-changes feed: summarises, on a schedule, the
signals that indicate the dataset is drifting out of date — dataset staleness,
renames detected by the feed but not yet applied, and the current data-quality
indicator counts from the release gates. Detection only; nothing is auto-applied
(corrections stay on the verified override/verify path).

A naive source-vs-dataset symbol set-diff is intentionally NOT used: exchange
symbol universes differ in scope (BDRs, share classes, units), so it produces
thousands of false new/delisting candidates rather than real drift.

Writes data/reports/drift_report.{json,md}; in GitHub Actions also writes
``drift_detected=true|false`` to ``$GITHUB_OUTPUT``.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import TICKERS_CSV

COVERAGE_REPORT_JSON = ROOT / "data" / "reports" / "coverage_report.json"
SYMBOL_CHANGES_CSV = ROOT / "data" / "corporate_actions" / "symbol_changes.csv"
VALIDATION_JSON = ROOT / "data" / "reports" / "validation_report.json"
REPORT_JSON = ROOT / "data" / "reports" / "drift_report.json"
REPORT_MD = ROOT / "data" / "reports" / "drift_report.md"
SAMPLE_CAP = 30
QUALITY_KEYS = (
    "source_gap_rows", "missing_stock_sector", "missing_etf_category",
    "expected_missing_primary_isin", "country_isin_mismatch",
    "official_name_mismatch", "allowed_warn_rows",
)


def dataset_built_at() -> str | None:
    try:
        return json.loads(COVERAGE_REPORT_JSON.read_text(encoding="utf-8"))["_meta"]["generated_at"]
    except Exception:
        return None


def staleness_days(built_at: str | None, *, now: datetime) -> float | None:
    if not built_at:
        return None
    try:
        ts = datetime.fromisoformat(built_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((now - ts).total_seconds() / 86400.0, 1)


def pending_renames() -> list[dict[str, str]]:
    """symbol_changes whose OLD symbol is still present in tickers.csv and whose
    NEW symbol is absent — i.e., a rename the feed found but we have not applied."""
    if not SYMBOL_CHANGES_CSV.exists():
        return []
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        symbols = {r["ticker"].strip().upper() for r in csv.DictReader(handle)}
    pending: list[dict[str, str]] = []
    with SYMBOL_CHANGES_CSV.open(newline="", encoding="utf-8") as handle:
        for ch in csv.DictReader(handle):
            old = (ch.get("old_symbol") or "").strip().upper()
            new = (ch.get("new_symbol") or "").strip().upper()
            if old and old in symbols and new and new not in symbols:
                pending.append({
                    "old_symbol": old, "new_symbol": new,
                    "new_company_name": ch.get("new_company_name", ""),
                    "effective_date": ch.get("effective_date", ""),
                })
    return pending


def quality_indicators() -> dict[str, int]:
    """Current info-gate counts from the validation report (data-quality drift)."""
    out: dict[str, int] = {}
    if not VALIDATION_JSON.exists():
        return out
    data = json.loads(VALIDATION_JSON.read_text(encoding="utf-8"))

    def walk(node):
        if isinstance(node, dict):
            name = node.get("name") or node.get("gate")
            if name in QUALITY_KEYS and ("actual" in node or "count" in node):
                val = node.get("actual", node.get("count"))
                if isinstance(val, int):
                    out[name] = val
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)
    return out


def build_markdown(s: dict) -> str:
    L = ["# Drift / freshness report", "",
         f"Generated: {s['generated_at']}",
         f"Dataset built_at: {s['built_at']} ({s['staleness_days']} days ago; "
         f"threshold {s['stale_threshold_days']})",
         f"**drift_detected: {s['drift_detected']}**", "",
         f"## Pending renames (feed-detected, not yet applied): {s['pending_renames_count']}"]
    for r in s["pending_renames_sample"]:
        L.append(f"- {r['old_symbol']} -> {r['new_symbol']} ({r['new_company_name']}, {r['effective_date']})")
    if s["pending_renames_count"] > len(s["pending_renames_sample"]):
        L.append(f"- ... and {s['pending_renames_count'] - len(s['pending_renames_sample'])} more")
    L += ["", "## Quality indicators (release-gate info counts)"]
    for k, v in sorted(s["quality_indicators"].items()):
        L.append(f"- {k}: {v}")
    L += ["", "_Detection only. Triage renames via the symbol-change review feed; "
          "apply corrections through the verified override/verify pipeline._"]
    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-days", type=float, default=45.0,
                        help="Staleness threshold (days) that counts as drift.")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    built = dataset_built_at()
    stale = staleness_days(built, now=now)
    pend = pending_renames()
    quality = quality_indicators()

    drift_detected = bool(pend) or (stale is not None and stale > args.stale_days)

    summary = {
        "generated_at": now.isoformat(timespec="seconds"),
        "built_at": built,
        "staleness_days": stale,
        "stale_threshold_days": args.stale_days,
        "pending_renames_count": len(pend),
        "pending_renames_sample": pend[:SAMPLE_CAP],
        "quality_indicators": quality,
        "drift_detected": drift_detected,
    }
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT_MD.write_text(build_markdown(summary), encoding="utf-8")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as handle:
            handle.write(f"drift_detected={'true' if drift_detected else 'false'}\n")

    print(json.dumps({k: summary[k] for k in (
        "built_at", "staleness_days", "pending_renames_count",
        "quality_indicators", "drift_detected")}, indent=2))


if __name__ == "__main__":
    main()
