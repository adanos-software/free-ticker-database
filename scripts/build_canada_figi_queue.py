from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_CANADA_RESIDUAL_REVIEW_CSV = REPORTS_DIR / "canada_residual_review.csv"
DEFAULT_OPENFIGI_GAP_CSV = DATA_DIR / "review_overrides" / "canada_figi_openfigi_gaps.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_figi_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_figi_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_figi_queue.md"

CSV_FIELDNAMES = [
    "batch_id",
    "batch_position",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "figi_decision",
    "openfigi_exchange_hint",
    "review_gate",
    "apply_eligibility",
    "verification_evidence_required",
]

OPENFIGI_EXCHANGE_HINTS = {
    "NEO": "CN",
    "TSX": "CN",
    "TSXV": "CN",
}

QUEUE_APPLY_ELIGIBILITY = "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate"
QUEUE_VERIFICATION_EVIDENCE = "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review"
EMPTY_QUEUE_APPLY_ELIGIBILITY = "no_active_openfigi_probe_rows"
EXCLUDED_GAP_APPLY_ELIGIBILITY = "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source"
EMPTY_QUEUE_VERIFICATION_EVIDENCE = "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps"
EXCLUDED_GAP_VERIFICATION_EVIDENCE = "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision"
QUEUE_REVIEW_STRATEGY = "probe_openfigi_by_valid_isin_with_canada_exchange_hint_then_collision_review"
EMPTY_QUEUE_REVIEW_STRATEGY = "no_active_openfigi_probe_rows_after_gates"
EXCLUDED_GAP_REVIEW_STRATEGY = "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source"
QUEUE_RECOMMENDED_NEXT_SOURCE = "OpenFIGI ID_ISIN request with Canada exchange hint CN, followed by collision and cross-ISIN review."
EXCLUDED_GAP_RECOMMENDED_NEXT_SOURCE = "Stronger FIGI source or reviewed OpenFIGI re-check evidence for the existing reviewed source gap."
QUEUE_SOURCE_GATE = "OpenFIGI result is a candidate only; apply only after listing-keyed collision and cross-ISIN gates pass."
EXCLUDED_GAP_SOURCE_GATE = "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."


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


def openfigi_gap_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        row.get("listing_key", ""),
        row.get("isin", ""),
        row.get("requested_exchange_hint") or row.get("openfigi_exchange_hint", ""),
    )


def reviewed_openfigi_gap_keys(rows: list[dict[str, str]]) -> set[tuple[str, str, str]]:
    return {
        openfigi_gap_key(row)
        for row in rows
        if row.get("decision") in {"no_openfigi_match", "reject"}
        and row.get("review_status")
        in {
            "accepted_source_gap_no_openfigi_match",
            "accepted_source_gap_figi_cross_isin_collision",
        }
        and all(openfigi_gap_key(row))
    }


def select_candidates(rows: list[dict[str, str]], reviewed_gap_keys: set[tuple[str, str, str]] | None = None) -> list[dict[str, str]]:
    reviewed_gap_keys = reviewed_gap_keys or set()
    return sorted(
        [
            row
            for row in rows
            if row.get("figi_decision") == "missing_figi_openfigi_candidate"
            and row.get("isin", "").strip()
            and row.get("missing_figi") == "true"
            and row.get("exchange") in OPENFIGI_EXCHANGE_HINTS
            and (
                row.get("listing_key", ""),
                row.get("isin", ""),
                OPENFIGI_EXCHANGE_HINTS[row.get("exchange", "")],
            )
            not in reviewed_gap_keys
        ],
        key=lambda row: (row.get("exchange", ""), row.get("asset_type", ""), row.get("ticker", ""), row.get("listing_key", "")),
    )


def build_queue_rows(
    rows: list[dict[str, str]],
    batch_size: int,
    reviewed_gap_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    candidates = select_candidates(rows, reviewed_openfigi_gap_keys(reviewed_gap_rows or []))
    queue: list[dict[str, str]] = []
    for index, row in enumerate(candidates):
        queue.append(
            {
                "batch_id": f"canada-figi-{index // batch_size + 1:04d}",
                "batch_position": str(index % batch_size + 1),
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "exchange": row.get("exchange", ""),
                "asset_type": row.get("asset_type", ""),
                "name": row.get("name", ""),
                "isin": row.get("isin", ""),
                "figi_decision": row.get("figi_decision", ""),
                "openfigi_exchange_hint": OPENFIGI_EXCHANGE_HINTS[row.get("exchange", "")],
                "review_gate": "valid_isin_and_missing_figi",
                "apply_eligibility": QUEUE_APPLY_ELIGIBILITY,
                "verification_evidence_required": QUEUE_VERIFICATION_EVIDENCE,
            }
        )
    return queue


def summarize(rows: list[dict[str, str]], generated_at: str, batch_size: int, excluded_openfigi_gap_rows: int = 0) -> dict[str, Any]:
    apply_eligibility_totals = Counter(row["apply_eligibility"] for row in rows)
    verification_evidence_required_totals = Counter(row["verification_evidence_required"] for row in rows)
    review_strategy_totals: Counter[str] = Counter()
    top_batch_counter: Counter[tuple[str, str, str, str]] = Counter()
    for row in rows:
        review_strategy_totals[QUEUE_REVIEW_STRATEGY] += 1
        top_batch_counter[
            (
                row.get("exchange", ""),
                row.get("asset_type", "") or "unknown",
                row.get("openfigi_exchange_hint", ""),
                QUEUE_REVIEW_STRATEGY,
            )
        ] += 1
    if not rows:
        apply_eligibility_totals[EMPTY_QUEUE_APPLY_ELIGIBILITY] += 1
        verification_evidence_required_totals[EMPTY_QUEUE_VERIFICATION_EVIDENCE] += 1
        review_strategy_totals[EMPTY_QUEUE_REVIEW_STRATEGY] += 1
    if excluded_openfigi_gap_rows:
        apply_eligibility_totals[EXCLUDED_GAP_APPLY_ELIGIBILITY] += excluded_openfigi_gap_rows
        verification_evidence_required_totals[EXCLUDED_GAP_VERIFICATION_EVIDENCE] += excluded_openfigi_gap_rows
        review_strategy_totals[EXCLUDED_GAP_REVIEW_STRATEGY] += excluded_openfigi_gap_rows
        top_batch_counter[
            (
                "reviewed_openfigi_source_gap",
                "all",
                "CN",
                EXCLUDED_GAP_REVIEW_STRATEGY,
            )
        ] += excluded_openfigi_gap_rows
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "batch_size": batch_size,
        "batches": len({row["batch_id"] for row in rows}),
        "excluded_openfigi_gap_rows": excluded_openfigi_gap_rows,
        "exchange_totals": dict(sorted(Counter(row["exchange"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "openfigi_exchange_hint_totals": dict(sorted(Counter(row["openfigi_exchange_hint"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(apply_eligibility_totals.items())),
        "verification_evidence_required_totals": dict(sorted(verification_evidence_required_totals.items())),
        "review_strategy_totals": dict(sorted(review_strategy_totals.items())),
        "top_canada_figi_queue_review_batches": [
            {
                "exchange": exchange,
                "asset_type": asset_type,
                "openfigi_exchange_hint": exchange_hint,
                "rows": count,
                "review_strategy": review_strategy,
                "apply_eligibility": (
                    EXCLUDED_GAP_APPLY_ELIGIBILITY
                    if review_strategy == EXCLUDED_GAP_REVIEW_STRATEGY
                    else QUEUE_APPLY_ELIGIBILITY
                ),
                "verification_evidence_required": (
                    EXCLUDED_GAP_VERIFICATION_EVIDENCE
                    if review_strategy == EXCLUDED_GAP_REVIEW_STRATEGY
                    else QUEUE_VERIFICATION_EVIDENCE
                ),
                "recommended_next_source": (
                    EXCLUDED_GAP_RECOMMENDED_NEXT_SOURCE
                    if review_strategy == EXCLUDED_GAP_REVIEW_STRATEGY
                    else QUEUE_RECOMMENDED_NEXT_SOURCE
                ),
                "source_gate": (
                    EXCLUDED_GAP_SOURCE_GATE
                    if review_strategy == EXCLUDED_GAP_REVIEW_STRATEGY
                    else QUEUE_SOURCE_GATE
                ),
            }
            for (exchange, asset_type, exchange_hint, review_strategy), count in sorted(
                top_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3]),
            )[:10]
        ],
        "policy": {
            "input_gate": "Only canada_residual_review rows with valid ISIN, missing FIGI, and figi_decision=missing_figi_openfigi_candidate are queued.",
            "reviewed_gap_gate": "Rows with an accepted OpenFIGI no-match or FIGI-collision review are excluded until a stronger source is available.",
            "no_symbol_guessing": "Rows without ISIN are excluded; OpenFIGI candidate selection must still prefer the Canada exchange hint and ticker.",
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
        "# Canada FIGI Queue",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report batches TSX/TSXV/NEO rows that can be sent to OpenFIGI because they already have valid ISINs and no FIGI. It does not call OpenFIGI and does not fill values.",
        "",
        "## Summary",
        "",
        f"- Queue rows: `{summary['rows']}`",
        f"- Batch size: `{summary['batch_size']}`",
        f"- Batches: `{summary['batches']}`",
        f"- Excluded reviewed OpenFIGI source gaps/conflicts: `{summary['excluded_openfigi_gap_rows']}`",
        "",
        "## Exchanges",
        "",
        "| Exchange | Rows |",
        "|---|---:|",
    ]
    for exchange, count in summary["exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## Asset Types", "", "| Asset type | Rows |", "|---|---:|"])
    for asset_type, count in summary["asset_type_totals"].items():
        lines.append(f"| {asset_type} | {count} |")
    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Verification Evidence", "", "| Evidence Required | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(["", "## Review Strategies", "", "| Strategy | Rows |", "|---|---:|"])
    for strategy, count in summary["review_strategy_totals"].items():
        lines.append(f"| {strategy} | {count} |")
    lines.extend(
        [
            "",
            "## Top Review Batches",
            "",
            "| Exchange | Asset type | OpenFIGI hint | Rows | Strategy | Eligibility | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---:|---|---|---|---|---|",
        ]
    )
    for batch in summary["top_canada_figi_queue_review_batches"]:
        lines.append(
            f"| {batch['exchange']} | {batch['asset_type']} | {batch['openfigi_exchange_hint']} | {batch['rows']} | "
            f"{batch['review_strategy']} | {batch['apply_eligibility']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Rows without ISIN are excluded from the FIGI queue.",
            "- Rows already reviewed as OpenFIGI no-match or FIGI-collision source gaps are excluded from repeat probing.",
            "- NEO, TSX, and TSXV use the OpenFIGI Canada exchange hint `CN`.",
            "- Applying FIGIs still requires the existing collision gate in `scripts/enrich_global_identifiers.py`.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a batch queue for Canada OpenFIGI enrichment candidates.")
    parser.add_argument("--canada-residual-review-csv", type=Path, default=DEFAULT_CANADA_RESIDUAL_REVIEW_CSV)
    parser.add_argument("--openfigi-gap-csv", type=Path, default=DEFAULT_OPENFIGI_GAP_CSV)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    gap_rows = load_csv(args.openfigi_gap_csv)
    rows = build_queue_rows(load_csv(args.canada_residual_review_csv), args.batch_size, gap_rows)
    summary = summarize(rows, utc_now_iso(), args.batch_size, excluded_openfigi_gap_rows=len(reviewed_openfigi_gap_keys(gap_rows)))
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
                "batches": summary["batches"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
