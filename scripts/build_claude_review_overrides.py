from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_RESPONSES = ROOT / "data" / "claude_review_jobs" / "raw_responses.jsonl"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "review_overrides"
DEFAULT_REMOVE_ALIASES = DEFAULT_OUTPUT_DIR / "remove_aliases.csv"
DEFAULT_METADATA_UPDATES = DEFAULT_OUTPUT_DIR / "metadata_updates.csv"
DEFAULT_DROP_ENTRIES = DEFAULT_OUTPUT_DIR / "drop_entries.csv"
DEFAULT_SUMMARY = DEFAULT_OUTPUT_DIR / "summary.json"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive conservative override files from local Claude review results.")
    parser.add_argument("--raw-responses", type=Path, default=DEFAULT_RAW_RESPONSES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--remove-aliases-out", type=Path, default=DEFAULT_REMOVE_ALIASES)
    parser.add_argument("--metadata-updates-out", type=Path, default=DEFAULT_METADATA_UPDATES)
    parser.add_argument("--drop-entries-out", type=Path, default=DEFAULT_DROP_ENTRIES)
    parser.add_argument("--summary-out", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--min-confidence", type=float, default=0.8)
    return parser.parse_args()


def best_operation_key(*parts: str) -> tuple[str, ...]:
    return tuple(parts)


def main() -> None:
    args = parse_args()
    rows = load_jsonl(args.raw_responses)

    remove_aliases: dict[tuple[str, ...], dict[str, object]] = {}
    metadata_updates: dict[tuple[str, ...], dict[str, object]] = {}
    drop_entries: dict[tuple[str, ...], dict[str, object]] = {}
    skipped = Counter()

    for row in rows:
        if row.get("status") != "ok":
            skipped["non_ok_status"] += 1
            continue

        payload = row.get("response")
        if not isinstance(payload, dict):
            skipped["missing_response"] += 1
            continue

        confidence = float(payload.get("confidence", 0) or 0)
        if confidence < args.min_confidence:
            skipped["confidence_below_threshold"] += 1
            continue

        ticker = str(row["ticker"])
        exchange = str(row["exchange"])

        if payload.get("entry_decision") == "drop_entry":
            key = best_operation_key(ticker, exchange)
            existing = drop_entries.get(key)
            candidate = {
                "ticker": ticker,
                "exchange": exchange,
                "confidence": confidence,
                "reason": payload.get("summary", ""),
            }
            if existing is None or confidence > float(existing["confidence"]):
                drop_entries[key] = candidate

        for action in payload.get("alias_actions", []):
            if not isinstance(action, dict) or action.get("decision") != "remove":
                continue
            alias = str(action.get("alias", ""))
            if not alias:
                continue
            key = best_operation_key(ticker, exchange, alias)
            existing = remove_aliases.get(key)
            candidate = {
                "ticker": ticker,
                "exchange": exchange,
                "alias": alias,
                "confidence": confidence,
                "reason": str(action.get("reason", "")),
            }
            if existing is None or confidence > float(existing["confidence"]):
                remove_aliases[key] = candidate

        for action in payload.get("metadata_actions", []):
            if not isinstance(action, dict):
                continue
            decision = action.get("decision")
            if decision not in {"update", "clear"}:
                continue
            field = str(action.get("field", ""))
            if not field:
                continue
            key = best_operation_key(ticker, exchange, field)
            existing = metadata_updates.get(key)
            candidate = {
                "ticker": ticker,
                "exchange": exchange,
                "field": field,
                "decision": str(decision),
                "proposed_value": str(action.get("proposed_value", "") or ""),
                "confidence": confidence,
                "reason": str(action.get("reason", "")),
            }
            if existing is None or confidence > float(existing["confidence"]):
                metadata_updates[key] = candidate

    remove_alias_rows = sorted(remove_aliases.values(), key=lambda row: (row["ticker"], row["exchange"], row["alias"]))
    metadata_update_rows = sorted(
        metadata_updates.values(),
        key=lambda row: (row["ticker"], row["exchange"], row["field"]),
    )
    drop_entry_rows = sorted(drop_entries.values(), key=lambda row: (row["ticker"], row["exchange"]))

    write_csv(
        args.remove_aliases_out,
        ["ticker", "exchange", "alias", "confidence", "reason"],
        remove_alias_rows,
    )
    write_csv(
        args.metadata_updates_out,
        ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        metadata_update_rows,
    )
    write_csv(
        args.drop_entries_out,
        ["ticker", "exchange", "confidence", "reason"],
        drop_entry_rows,
    )

    summary = {
        "_meta": {
            "raw_responses": display_path(args.raw_responses),
            "min_confidence": args.min_confidence,
        },
        "counts": {
            "remove_aliases": len(remove_alias_rows),
            "metadata_updates": len(metadata_update_rows),
            "drop_entries": len(drop_entry_rows),
        },
        "skipped": dict(sorted(skipped.items())),
    }
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "remove_aliases_out": display_path(args.remove_aliases_out),
                "metadata_updates_out": display_path(args.metadata_updates_out),
                "drop_entries_out": display_path(args.drop_entries_out),
                "summary_out": display_path(args.summary_out),
                "counts": summary["counts"],
                "skipped": summary["skipped"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
