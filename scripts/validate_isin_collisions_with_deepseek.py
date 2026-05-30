"""Validate ISIN identity collisions with DeepSeek v4 Pro (triage only).

The deterministic collision detector proves that an ISIN is shared by distinct
issuer names. DeepSeek is used here purely as an independent triage cross-check:
for each collision group it classifies same-issuer vs distinct-issuers and, when
distinct, points at the listing(s) that most likely carry the wrong ISIN, with a
confidence and the official evidence still required.

DeepSeek output is advisory triage only. It never authorizes a data change: the
ISIN/country/name remain unchanged until an official national-numbering-agency,
issuer, or exchange security master keyed to the exact listing confirms the
holder. Run with ``--dry-run`` to validate batching and schema without a network
call, or provide ``DEEPSEEK_API_KEY`` for a live run.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import sys
import time
import urllib.error
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_deepseek_review_queue import call_deepseek, parse_json_object

REPORTS_DIR = ROOT / "data" / "reports"
DEFAULT_QUEUE_JSON = REPORTS_DIR / "isin_identity_collision_review_queue.json"
DEFAULT_JSON_OUT = REPORTS_DIR / "deepseek_isin_collision_validation.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "deepseek_isin_collision_validation.csv"
DEFAULT_MD_OUT = REPORTS_DIR / "deepseek_isin_collision_validation.md"
DEFAULT_MODEL = "deepseek-v4-pro"
DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_LIMIT = 12
DEFAULT_BATCH_SIZE = 4

VALID_CLASSIFICATIONS = {"distinct_issuers", "same_issuer", "uncertain"}

CSV_FIELDNAMES = [
    "isin",
    "registered_country",
    "deepseek_classification",
    "deepseek_confidence",
    "likely_correct_listing_keys",
    "likely_misassigned_listing_keys",
    "agrees_with_detector",
    "evidence_needed",
    "rationale",
    "review_gate",
]

REVIEW_GATE = (
    "Triage only. Do not change, blank, or reassign any ISIN, country, or name "
    "until official listing-keyed identifier evidence confirms the holder."
)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def select_top_groups(payload: dict[str, Any], limit: int, offset: int = 0) -> list[dict[str, Any]]:
    """Highest-risk collision groups first (ticker-collision, then most listings).

    The queue is already sorted that way, so this takes a contiguous ``limit``
    window starting at ``offset``. ``offset`` lets a large sweep be split into
    disjoint chunks that can run in parallel and be merged afterwards.
    """

    items = payload.get("items", [])
    selected = [item for item in items if isinstance(item, dict) and item.get("_members")]
    start = max(0, offset)
    return selected[start : start + limit]


def compact_group(group: dict[str, Any]) -> dict[str, Any]:
    return {
        "isin": group.get("isin", ""),
        "registered_country": group.get("registered_country", ""),
        "members": [
            {
                "listing_key": member.get("listing_key", ""),
                "ticker": member.get("ticker", ""),
                "exchange": member.get("exchange", ""),
                "name": member.get("name", ""),
            }
            for member in group.get("_members", [])
        ],
    }


def chunk(groups: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [groups[index : index + batch_size] for index in range(0, len(groups), batch_size)]


def build_prompt(groups: list[dict[str, Any]]) -> str:
    compacted = [compact_group(group) for group in groups]
    return (
        "You are a conservative identity-reconciliation assistant for a truth-sensitive "
        "global securities database.\n"
        "Each group lists two or more listings that currently share ONE ISIN. An ISIN "
        "identifies exactly one security/issuer, so if the listings belong to different "
        "issuers, at least one listing carries a wrong ISIN.\n"
        "For each group decide whether the listings are the SAME issuer (variants/cross-"
        "listings) or DISTINCT issuers. If distinct, name which listing_key(s) most likely "
        "hold the ISIN legitimately and which are likely misassigned.\n"
        "Do not invent ISINs, names, or identifiers. This is triage only; an official "
        "source must confirm before any change. Set confidence low when unsure.\n"
        "Return JSON only with this exact shape:\n"
        '{"verdicts":[{"isin":"...","classification":"distinct_issuers|same_issuer|uncertain",'
        '"likely_correct_listing_keys":["..."],"likely_misassigned_listing_keys":["..."],'
        '"confidence":0.0,"evidence_needed":"...","rationale":"..."}]}\n'
        f"Return exactly {len(compacted)} verdict objects in the same order as the groups.\n\n"
        f"Groups:\n{json.dumps(compacted, ensure_ascii=True, indent=2)}"
    )


def _as_key_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def normalize_verdict(raw: dict[str, Any], group: dict[str, Any]) -> dict[str, Any]:
    classification = str(raw.get("classification") or "uncertain")
    if classification not in VALID_CLASSIFICATIONS:
        classification = "uncertain"
    confidence = raw.get("confidence", 0)
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        confidence = 0
    confidence = max(0.0, min(1.0, float(confidence)))
    member_keys = {member.get("listing_key", "") for member in group.get("_members", [])}
    correct = [key for key in _as_key_list(raw.get("likely_correct_listing_keys")) if key in member_keys]
    misassigned = [key for key in _as_key_list(raw.get("likely_misassigned_listing_keys")) if key in member_keys]
    detector_distinct = group.get("decision_candidate", "") in {
        "isin_shared_by_distinct_issuers",
        "ticker_collision_isin_misassignment_suspected",
    }
    agrees = (classification == "distinct_issuers") if detector_distinct else (classification == "same_issuer")
    return {
        "isin": group.get("isin", ""),
        "registered_country": group.get("registered_country", ""),
        "deepseek_classification": classification,
        "deepseek_confidence": confidence,
        "likely_correct_listing_keys": "|".join(correct),
        "likely_misassigned_listing_keys": "|".join(misassigned),
        "agrees_with_detector": "true" if agrees else "false",
        "evidence_needed": str(raw.get("evidence_needed", ""))[:500],
        "rationale": str(raw.get("rationale", ""))[:500],
        "review_gate": REVIEW_GATE,
    }


def normalize_batch(payload: dict[str, Any], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    verdicts = payload.get("verdicts")
    if not isinstance(verdicts, list):
        raise ValueError("DeepSeek response is missing verdicts array.")
    if len(verdicts) != len(groups):
        raise ValueError(f"Expected {len(groups)} verdicts, got {len(verdicts)}.")
    normalized = []
    for raw, group in zip(verdicts, groups, strict=True):
        if not isinstance(raw, dict):
            raise ValueError("Each verdict must be a JSON object.")
        normalized.append(normalize_verdict(raw, group))
    return normalized


def summarize(verdicts: list[dict[str, Any]], errors: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    classifications: dict[str, int] = {}
    agree = 0
    for verdict in verdicts:
        classifications[verdict["deepseek_classification"]] = (
            classifications.get(verdict["deepseek_classification"], 0) + 1
        )
        if verdict["agrees_with_detector"] == "true":
            agree += 1
    return {
        "generated_at": generated_at,
        "validated_groups": len(verdicts),
        "errors": len(errors),
        "classification_totals": dict(sorted(classifications.items())),
        "agrees_with_detector": agree,
        "disagrees_with_detector": len(verdicts) - agree,
        "policy": (
            "DeepSeek triage is advisory only and authorizes no ISIN, country, or name "
            "change. Official listing-keyed identifier evidence remains required."
        ),
    }


def dry_run_payload(groups: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "verdicts": [
            {
                "isin": group.get("isin", ""),
                "classification": "uncertain",
                "likely_correct_listing_keys": [],
                "likely_misassigned_listing_keys": [],
                "confidence": 0.1,
                "evidence_needed": "Dry run; no DeepSeek API request was made.",
                "rationale": "Dry run validates batching and output schema only.",
            }
            for group in groups
        ]
    }


def run_validation(
    *,
    queue_payload: dict[str, Any],
    limit: int,
    batch_size: int,
    call_fn: Callable[[str], dict[str, Any]] | None,
    sleep_seconds: float = 0.0,
    max_attempts: int = 2,
    retry_backoff_seconds: float = 0.0,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups = select_top_groups(queue_payload, limit, offset=offset)
    verdicts: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for batch_index, batch in enumerate(chunk(groups, batch_size), start=1):
        prompt = build_prompt(batch)
        last_exc: Exception | None = None
        for attempt in range(1, max(1, max_attempts) + 1):
            try:
                payload = dry_run_payload(batch) if call_fn is None else call_fn(prompt)
                verdicts.extend(normalize_batch(payload, batch))
                last_exc = None
                if call_fn is not None and sleep_seconds:
                    time.sleep(sleep_seconds)
                break
            except (
                ValueError,
                KeyError,
                json.JSONDecodeError,
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                ConnectionError,
                OSError,
                RuntimeError,
            ) as exc:
                # A single flaky batch (transient read/connection error, malformed
                # response) must not abort the whole run. Retry a few times, then
                # record and continue so the rest of the sweep still completes.
                last_exc = exc
                if call_fn is not None and attempt < max_attempts and retry_backoff_seconds:
                    time.sleep(retry_backoff_seconds)
        if last_exc is not None:
            errors.append(
                {
                    "batch_index": batch_index,
                    "error": f"{type(last_exc).__name__}: {last_exc}",
                    "attempts": max(1, max_attempts),
                }
            )
    return verdicts, errors


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def markdown_escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# DeepSeek ISIN Identity Collision Validation",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "Policy: DeepSeek triage is advisory only and authorizes no ISIN, country, or name change.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Validated groups | {summary['validated_groups']} |",
        f"| Agrees with detector | {summary['agrees_with_detector']} |",
        f"| Disagrees with detector | {summary['disagrees_with_detector']} |",
        f"| Errors | {summary['errors']} |",
        "",
        "## Verdicts",
        "",
        "| ISIN | Country | Classification | Conf | Likely misassigned | Rationale |",
        "| --- | --- | --- | ---: | --- | --- |",
    ]
    for row in payload["items"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["isin"]),
                    markdown_escape(row["registered_country"] or "unknown"),
                    markdown_escape(row["deepseek_classification"]),
                    f"{row['deepseek_confidence']:.2f}",
                    markdown_escape(row["likely_misassigned_listing_keys"] or "-"),
                    markdown_escape(row["rationale"][:120]),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], csv_out: Path, json_out: Path, md_out: Path) -> None:
    write_csv(csv_out, payload["items"])
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(render_markdown(payload), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ISIN identity collisions with DeepSeek (triage only).")
    parser.add_argument("--queue-json", type=Path, default=DEFAULT_QUEUE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--offset", type=int, default=0, help="Skip this many groups before validating (for chunked runs).")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    parser.add_argument("--max-attempts", type=int, default=2, help="Attempts per batch before recording an error.")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    queue_payload = load_json(args.queue_json)

    call_fn: Callable[[str], dict[str, Any]] | None
    if args.dry_run:
        call_fn = None
    else:
        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        if not api_key:
            raise SystemExit("DEEPSEEK_API_KEY is required unless --dry-run is used.")

        def call_fn(prompt: str) -> dict[str, Any]:
            return call_deepseek(
                api_key=api_key,
                prompt=prompt,
                model=args.model,
                base_url=args.base_url,
                timeout=args.timeout,
            )

    verdicts, errors = run_validation(
        queue_payload=queue_payload,
        limit=args.limit,
        batch_size=args.batch_size,
        call_fn=call_fn,
        sleep_seconds=args.sleep_seconds,
        max_attempts=args.max_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
        offset=args.offset,
    )
    payload = {
        "_meta": {
            "generated_at": utc_now_iso(),
            "queue_json": display_path(args.queue_json),
            "model": args.model,
            "dry_run": args.dry_run,
            "policy": (
                "DeepSeek triage is advisory only. It authorizes no ISIN, country, name, or "
                "scope change without official listing-keyed evidence."
            ),
        },
        "summary": summarize(verdicts, errors, generated_at=utc_now_iso()),
        "items": verdicts,
        "errors": errors,
    }
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        json.dumps(
            {"summary": payload["summary"], "json_out": display_path(args.json_out)},
            indent=2,
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
