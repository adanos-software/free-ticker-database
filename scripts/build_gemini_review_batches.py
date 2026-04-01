from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_dataset import DEFAULT_JSON_OUT as DEFAULT_REVIEW_QUEUE_JSON


DEFAULT_BATCH_DIR = ROOT / "data" / "gemini_review_jobs"
DEFAULT_PROMPT_PATH = ROOT / "docs" / "gemini_review_prompt.md"
DEFAULT_SCHEMA_PATH = ROOT / "docs" / "gemini_review_response.schema.json"


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "review"


def primary_finding_type(item: dict[str, object]) -> str:
    findings = item.get("findings", [])
    if not findings:
        return "unclassified"
    return str(findings[0]["finding_type"])


def build_prompt_text(item: dict[str, object]) -> str:
    return (
        "Review this flagged ticker-database entry.\n\n"
        "Return JSON only.\n"
        "Follow the provided response schema exactly.\n"
        "Be conservative: if evidence is insufficient, use needs_human or unknown.\n"
        "Treat exchange-specific evidence and ISIN-based evidence as stronger than fuzzy name matches.\n"
        "Remove aliases that are obvious products, celebrities, wrapper terms, or generic/common words.\n"
        "Keep aliases that are strong lexical matches to the issuer or fund name, or clear listing identifiers.\n"
        "If ticker existence cannot be validated from the provided evidence, set ticker_exists to \"unknown\".\n"
        "If the entry looks like a wrapper, receipt, note, warrant, right, or preferred instrument that should not "
        "be in the stock universe, prefer drop_entry.\n\n"
        "Entry:\n"
        f"{json.dumps(item, indent=2, ensure_ascii=True)}\n\n"
        "External evidence:\n"
        "[]\n\n"
        "No external evidence is attached in this batch request. If uncertain, prefer needs_human or unknown.\n"
    )


def build_request_line(
    item: dict[str, object],
    response_schema: dict[str, object],
    prompt_path: Path,
    schema_path: Path,
) -> dict[str, object]:
    key = f"{slugify(primary_finding_type(item))}--{item['ticker']}--{item['exchange']}"
    return {
        "key": key,
        "metadata": {
            "ticker": item["ticker"],
            "exchange": item["exchange"],
            "asset_type": item["asset_type"],
            "country": item["country"],
            "total_score": item["total_score"],
            "primary_finding_type": primary_finding_type(item),
            "finding_count": len(item["findings"]),
            "prompt_template": display_path(prompt_path),
            "response_schema": display_path(schema_path),
        },
        "request": {
            "contents": [
                {
                    "parts": [
                        {
                            "text": build_prompt_text(item),
                        }
                    ]
                }
            ],
            "generation_config": {
                "temperature": 0,
                "response_mime_type": "application/json",
                "response_json_schema": response_schema,
            },
        },
    }


def chunk_items(
    items: list[dict[str, object]],
    *,
    max_items_per_batch: int,
    max_prompt_chars_per_batch: int,
) -> list[list[dict[str, object]]]:
    batches: list[list[dict[str, object]]] = []
    current_batch: list[dict[str, object]] = []
    current_chars = 0

    for item in items:
        item_chars = len(build_prompt_text(item))
        should_split = (
            current_batch
            and (
                len(current_batch) >= max_items_per_batch
                or current_chars + item_chars > max_prompt_chars_per_batch
            )
        )
        if should_split:
            batches.append(current_batch)
            current_batch = []
            current_chars = 0

        current_batch.append(item)
        current_chars += item_chars

    if current_batch:
        batches.append(current_batch)

    return batches


def group_items_by_primary_finding(items: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in items:
        grouped[primary_finding_type(item)].append(item)

    ordered: list[dict[str, object]] = []
    for finding_type in sorted(grouped):
        ordered.extend(
            sorted(
                grouped[finding_type],
                key=lambda item: (-int(item["total_score"]), str(item["ticker"]), str(item["exchange"])),
            )
        )
    return ordered


def write_jsonl(path: Path, lines: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(json.dumps(line, ensure_ascii=True) + "\n")


def build_manifest(
    queue_payload: dict[str, object],
    batches: list[list[dict[str, object]]],
    output_dir: Path,
    prompt_path: Path,
    schema_path: Path,
    *,
    max_items_per_batch: int,
    max_prompt_chars_per_batch: int,
) -> dict[str, object]:
    batch_entries: list[dict[str, object]] = []
    total_requests = 0
    for index, batch_items in enumerate(batches, start=1):
        batch_id = f"batch-{index:04d}"
        finding_counts = Counter(primary_finding_type(item) for item in batch_items)
        batch_entries.append(
            {
                "batch_id": batch_id,
                "file": display_path(output_dir / f"{batch_id}.jsonl"),
                "item_count": len(batch_items),
                "finding_types": dict(sorted(finding_counts.items())),
                "tickers": [item["ticker"] for item in batch_items],
                "total_score": sum(int(item["total_score"]) for item in batch_items),
            }
        )
        total_requests += len(batch_items)

    return {
        "_meta": {
            "source_review_queue": display_path(DEFAULT_REVIEW_QUEUE_JSON),
            "prompt_template": display_path(prompt_path),
            "response_schema": display_path(schema_path),
            "max_items_per_batch": max_items_per_batch,
            "max_prompt_chars_per_batch": max_prompt_chars_per_batch,
            "total_batches": len(batch_entries),
            "total_requests": total_requests,
            "source_flagged_entries": queue_payload["_meta"]["flagged_entries"],
            "source_min_score": queue_payload["_meta"]["min_score"],
        },
        "finding_summary": queue_payload.get("summary", {}),
        "batches": batch_entries,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split review_queue.json into Gemini Batch API JSONL jobs.")
    parser.add_argument("--review-queue-json", type=Path, default=DEFAULT_REVIEW_QUEUE_JSON)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_BATCH_DIR)
    parser.add_argument("--prompt-path", type=Path, default=DEFAULT_PROMPT_PATH)
    parser.add_argument("--schema-path", type=Path, default=DEFAULT_SCHEMA_PATH)
    parser.add_argument("--max-items-per-batch", type=int, default=100)
    parser.add_argument("--max-prompt-chars-per-batch", type=int, default=200_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    queue_payload = load_json(args.review_queue_json)
    response_schema = load_json(args.schema_path)
    items = list(queue_payload.get("items", []))
    ordered_items = group_items_by_primary_finding(items)
    batches = chunk_items(
        ordered_items,
        max_items_per_batch=args.max_items_per_batch,
        max_prompt_chars_per_batch=args.max_prompt_chars_per_batch,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    batch_ids: list[str] = []
    for index, batch_items in enumerate(batches, start=1):
        batch_id = f"batch-{index:04d}"
        batch_ids.append(batch_id)
        request_lines = [
            build_request_line(
                item,
                response_schema=response_schema,
                prompt_path=args.prompt_path,
                schema_path=args.schema_path,
            )
            for item in batch_items
        ]
        write_jsonl(args.output_dir / f"{batch_id}.jsonl", request_lines)

    manifest = build_manifest(
        queue_payload,
        batches,
        args.output_dir,
        args.prompt_path,
        args.schema_path,
        max_items_per_batch=args.max_items_per_batch,
        max_prompt_chars_per_batch=args.max_prompt_chars_per_batch,
    )
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": display_path(args.output_dir),
                "manifest": display_path(manifest_path),
                "total_batches": manifest["_meta"]["total_batches"],
                "total_requests": manifest["_meta"]["total_requests"],
                "batch_files": batch_ids[:10],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
