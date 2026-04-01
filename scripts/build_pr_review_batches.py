from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.review_utils import DEFAULT_NORMALIZED_REVIEWS_JSON, display_path


DEFAULT_PR_BATCH_DIR = ROOT / "data" / "pr_review_batches"


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_operations(
    normalized_payload: dict[str, object],
    *,
    min_confidence: float,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    operations: list[dict[str, object]] = []
    manual_items: list[dict[str, object]] = []

    for item in normalized_payload.get("items", []):
        if float(item["confidence"]) < min_confidence:
            manual_items.append(
                {
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "reason": "confidence_below_threshold",
                    "confidence": item["confidence"],
                    "entry_decision": item["entry_decision"],
                    "summary": item["summary"],
                }
            )
            continue

        if item["entry_decision"] == "needs_human":
            manual_items.append(
                {
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "reason": "entry_decision_needs_human",
                    "confidence": item["confidence"],
                    "entry_decision": item["entry_decision"],
                    "summary": item["summary"],
                }
            )
            continue

        if item["entry_decision"] == "drop_entry":
            operations.append(
                {
                    "operation_type": "drop_entry",
                    "ticker": item["ticker"],
                    "exchange": item["exchange"],
                    "confidence": item["confidence"],
                    "reason": item["summary"],
                    "source_file": item["source_file"],
                }
            )

        for action in item["alias_actions"]:
            if action["decision"] == "remove":
                operations.append(
                    {
                        "operation_type": "remove_alias",
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "alias": action["alias"],
                        "confidence": item["confidence"],
                        "reason": action["reason"],
                        "source_file": item["source_file"],
                    }
                )
            elif action["decision"] == "needs_human":
                manual_items.append(
                    {
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "reason": f"alias_needs_human:{action['alias']}",
                        "confidence": item["confidence"],
                        "entry_decision": item["entry_decision"],
                        "summary": action["reason"],
                    }
                )

        for action in item["metadata_actions"]:
            if action["decision"] in {"update", "clear"}:
                operations.append(
                    {
                        "operation_type": "update_metadata",
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "field": action["field"],
                        "decision": action["decision"],
                        "proposed_value": action.get("proposed_value", ""),
                        "confidence": item["confidence"],
                        "reason": action["reason"],
                        "source_file": item["source_file"],
                    }
                )
            elif action["decision"] == "needs_human":
                manual_items.append(
                    {
                        "ticker": item["ticker"],
                        "exchange": item["exchange"],
                        "reason": f"metadata_needs_human:{action['field']}",
                        "confidence": item["confidence"],
                        "entry_decision": item["entry_decision"],
                        "summary": action["reason"],
                    }
                )

    operations.sort(key=lambda operation: (operation["operation_type"], operation["ticker"], operation["exchange"]))
    manual_items.sort(key=lambda item: (item["ticker"], item["exchange"], item["reason"]))
    return operations, manual_items


def chunk_operations(
    operations: list[dict[str, object]],
    *,
    max_operations_per_batch: int,
) -> list[list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for operation in operations:
        grouped[operation["operation_type"]].append(operation)

    batches: list[list[dict[str, object]]] = []
    for operation_type in sorted(grouped):
        current: list[dict[str, object]] = []
        for operation in grouped[operation_type]:
            if current and len(current) >= max_operations_per_batch:
                batches.append(current)
                current = []
            current.append(operation)
        if current:
            batches.append(current)
    return batches


def write_batches(output_dir: Path, batches: list[list[dict[str, object]]]) -> list[dict[str, object]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = []
    for index, batch_operations in enumerate(batches, start=1):
        batch_id = f"pr-batch-{index:04d}"
        batch_path = output_dir / f"{batch_id}.json"
        operation_counts = Counter(operation["operation_type"] for operation in batch_operations)
        batch_payload = {
            "batch_id": batch_id,
            "suggested_branch": f"codex/review-{batch_id}",
            "suggested_title": f"Apply review actions from {batch_id}",
            "operations": batch_operations,
        }
        batch_path.write_text(json.dumps(batch_payload, indent=2), encoding="utf-8")
        entries.append(
            {
                "batch_id": batch_id,
                "file": display_path(batch_path),
                "item_count": len(batch_operations),
                "operation_types": dict(sorted(operation_counts.items())),
                "tickers": sorted({operation["ticker"] for operation in batch_operations}),
            }
        )
    return entries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build actionable PR review batches from normalized review decisions.")
    parser.add_argument("--normalized-reviews-json", type=Path, default=DEFAULT_NORMALIZED_REVIEWS_JSON)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_PR_BATCH_DIR)
    parser.add_argument("--manual-out", type=Path, default=DEFAULT_PR_BATCH_DIR / "manual_backlog.json")
    parser.add_argument("--manifest-out", type=Path, default=DEFAULT_PR_BATCH_DIR / "manifest.json")
    parser.add_argument("--min-confidence", type=float, default=0.8)
    parser.add_argument("--max-operations-per-batch", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    normalized_payload = load_json(args.normalized_reviews_json)
    operations, manual_items = build_operations(
        normalized_payload,
        min_confidence=args.min_confidence,
    )
    batches = chunk_operations(
        operations,
        max_operations_per_batch=args.max_operations_per_batch,
    )
    batch_entries = write_batches(args.output_dir, batches)
    args.manual_out.parent.mkdir(parents=True, exist_ok=True)
    args.manual_out.write_text(json.dumps(manual_items, indent=2), encoding="utf-8")

    manifest = {
        "_meta": {
            "source_normalized_reviews": display_path(args.normalized_reviews_json),
            "min_confidence": args.min_confidence,
            "max_operations_per_batch": args.max_operations_per_batch,
            "total_batches": len(batch_entries),
            "total_operations": len(operations),
            "manual_backlog": len(manual_items),
        },
        "batches": batch_entries,
    }
    args.manifest_out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "manifest": display_path(args.manifest_out),
                "manual_out": display_path(args.manual_out),
                "total_batches": len(batch_entries),
                "total_operations": len(operations),
                "manual_backlog": len(manual_items),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
