from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"
JOBS_DIR = ROOT / "data" / "deepseek_review_jobs"

DEFAULT_REVIEW_SUMMARY_CSV = REPORTS_DIR / "deepseek_review_summary.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "deepseek_batch_plan.json"
DEFAULT_MD_OUT = REPORTS_DIR / "deepseek_batch_plan.md"
DEFAULT_BATCH_SIZE = 5
DEFAULT_LIMIT = 50


@dataclass(frozen=True)
class QueueConfig:
    queue: str
    review_kind: str
    source_csv: Path
    key_field: str
    priority: int
    reason: str


DEFAULT_QUEUES = [
    QueueConfig(
        queue="masterfile_collision",
        review_kind="masterfile_collision",
        source_csv=REPORTS_DIR / "masterfile_collision_review.csv",
        key_field="target_listing_key",
        priority=1,
        reason="Largest official-masterfile identity queue; DeepSeek can triage likely cross-listing vs. still-needs-evidence cases without applying data.",
    ),
    QueueConfig(
        queue="otc_scope",
        review_kind="otc_scope",
        source_csv=REPORTS_DIR / "otc_scope_review.csv",
        key_field="listing_key",
        priority=2,
        reason="Large OTC warning/source-gap queue; DeepSeek can summarize evidence gaps while OTC names and metadata remain blocked.",
    ),
    QueueConfig(
        queue="weak_sector",
        review_kind="weak_sector",
        source_csv=REPORTS_DIR / "weak_sector_residual_review.csv",
        key_field="listing_key",
        priority=3,
        reason="Official-sector candidate queue; DeepSeek can prioritize normalization review, not sector application.",
    ),
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def reviewed_keys_by_kind(path: Path) -> dict[str, set[str]]:
    if not path.exists():
        return {}
    rows, _ = read_csv(path)
    result: dict[str, set[str]] = {}
    for row in rows:
        kind = row.get("review_kind", "")
        listing_key = row.get("listing_key", "")
        if kind and listing_key:
            result.setdefault(kind, set()).add(listing_key)
    return result


def queue_status(config: QueueConfig, reviewed_by_kind: dict[str, set[str]]) -> dict[str, Any]:
    rows, _ = read_csv(config.source_csv)
    reviewed = reviewed_by_kind.get(config.review_kind, set())
    keys = [row.get(config.key_field, "") for row in rows]
    unreviewed = [key for key in keys if key and key not in reviewed]
    return {
        "queue": config.queue,
        "review_kind": config.review_kind,
        "source_csv": display_path(config.source_csv),
        "key_field": config.key_field,
        "priority": config.priority,
        "reason": config.reason,
        "rows": len(rows),
        "already_deepseek_reviewed": len([key for key in keys if key in reviewed]),
        "unreviewed_rows": len(unreviewed),
    }


def select_queue(
    configs: list[QueueConfig],
    reviewed_by_kind: dict[str, set[str]],
    requested_queue: str,
) -> QueueConfig:
    statuses = {config.queue: queue_status(config, reviewed_by_kind) for config in configs}
    if requested_queue != "auto":
        for config in configs:
            if config.queue == requested_queue:
                if statuses[config.queue]["unreviewed_rows"] <= 0:
                    raise ValueError(f"Queue {requested_queue} has no unreviewed rows.")
                return config
        raise ValueError(f"Unknown queue: {requested_queue}")
    candidates = [config for config in configs if statuses[config.queue]["unreviewed_rows"] > 0]
    if not candidates:
        raise ValueError("No unreviewed DeepSeek-supported queue rows remain.")
    return sorted(candidates, key=lambda config: config.priority)[0]


def select_rows(config: QueueConfig, reviewed_by_kind: dict[str, set[str]], limit: int) -> tuple[list[dict[str, str]], list[str]]:
    rows, fieldnames = read_csv(config.source_csv)
    reviewed = reviewed_by_kind.get(config.review_kind, set())
    selected = [row for row in rows if row.get(config.key_field, "") and row.get(config.key_field, "") not in reviewed]
    return selected[:limit], fieldnames


def write_batch_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def runner_command(config: QueueConfig, batch_csv: Path, *, limit: int, batch_size: int, dry_run: bool = False) -> list[str]:
    stem = f"{config.queue}_next"
    command = [
        "python",
        "scripts/run_deepseek_review_queue.py",
        "--input-csv",
        display_path(batch_csv),
        "--review-kind",
        config.review_kind,
        "--limit",
        str(limit),
        "--batch-size",
        str(batch_size),
        "--raw-responses-jsonl",
        display_path(JOBS_DIR / "raw_responses.jsonl"),
        "--normalized-json",
        display_path(JOBS_DIR / f"{stem}_normalized_reviews.json"),
        "--normalized-csv",
        display_path(JOBS_DIR / f"{stem}_normalized_reviews.csv"),
        "--errors-json",
        display_path(JOBS_DIR / f"{stem}_errors.json"),
    ]
    if dry_run:
        command.append("--dry-run")
    return command


def build_plan(
    *,
    configs: list[QueueConfig],
    review_summary_csv: Path,
    requested_queue: str,
    limit: int,
    batch_size: int,
    batch_csv: Path | None = None,
) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    reviewed = reviewed_keys_by_kind(review_summary_csv)
    statuses = [queue_status(config, reviewed) for config in configs]
    selected_config = select_queue(configs, reviewed, requested_queue)
    selected_rows, fieldnames = select_rows(selected_config, reviewed, limit)
    selected_batch_csv = batch_csv or JOBS_DIR / f"next_{selected_config.queue}_batch.csv"
    payload = {
        "_meta": {
            "generated_at": utc_now_iso(),
            "review_summary_csv": display_path(review_summary_csv),
            "policy": (
                "This plan prepares DeepSeek advisory triage only. DeepSeek output must not authorize inferred "
                "identifiers, sectors, categories, names, symbol changes, scope changes, or direct data application."
            ),
        },
        "queues": statuses,
        "selected_batch": {
            "queue": selected_config.queue,
            "review_kind": selected_config.review_kind,
            "source_csv": display_path(selected_config.source_csv),
            "batch_csv": display_path(selected_batch_csv),
            "rows": len(selected_rows),
            "batch_size": batch_size,
            "reason": selected_config.reason,
            "run_command": runner_command(selected_config, selected_batch_csv, limit=len(selected_rows), batch_size=batch_size),
            "dry_run_command": runner_command(
                selected_config,
                selected_batch_csv,
                limit=len(selected_rows),
                batch_size=batch_size,
                dry_run=True,
            ),
            "required_env": "DEEPSEEK_API_KEY",
            "secret_policy": "Read the API key only from DEEPSEEK_API_KEY. Never write it to files, reports, logs, commits, or prompts.",
        },
    }
    return payload, selected_rows, fieldnames


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    selected = payload["selected_batch"]
    lines = [
        "# DeepSeek Batch Plan",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.",
        "",
        "## Queue Backlog",
        "",
        "| Queue | Rows | Already Reviewed | Unreviewed | Priority |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for queue in payload["queues"]:
        lines.append(
            f"| {queue['queue']} | {queue['rows']} | {queue['already_deepseek_reviewed']} | "
            f"{queue['unreviewed_rows']} | {queue['priority']} |"
        )
    lines.extend(
        [
            "",
            "## Selected Batch",
            "",
            f"- Queue: `{selected['queue']}`",
            f"- Review kind: `{selected['review_kind']}`",
            f"- Rows: `{selected['rows']}`",
            f"- Batch CSV: `{selected['batch_csv']}`",
            f"- Reason: {selected['reason']}",
            "",
            "Run when `DEEPSEEK_API_KEY` is set:",
            "",
            "```bash",
            " ".join(selected["run_command"]),
            "```",
            "",
            "Schema-only dry run:",
            "",
            "```bash",
            " ".join(selected["dry_run_command"]),
            "```",
            "",
            "Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build the next token-efficient DeepSeek advisory review batch.")
    parser.add_argument("--review-summary-csv", type=Path, default=DEFAULT_REVIEW_SUMMARY_CSV)
    parser.add_argument("--queue", choices=["auto", *(config.queue for config in DEFAULT_QUEUES)], default="auto")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-csv", type=Path)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    payload, rows, fieldnames = build_plan(
        configs=DEFAULT_QUEUES,
        review_summary_csv=args.review_summary_csv,
        requested_queue=args.queue,
        limit=args.limit,
        batch_size=args.batch_size,
        batch_csv=args.batch_csv,
    )
    batch_csv = ROOT / payload["selected_batch"]["batch_csv"]
    write_batch_csv(batch_csv, rows, fieldnames)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "selected_queue": payload["selected_batch"]["queue"],
                "rows": payload["selected_batch"]["rows"],
                "batch_csv": payload["selected_batch"]["batch_csv"],
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
