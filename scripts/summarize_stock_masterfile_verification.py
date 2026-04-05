from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT / "data" / "stock_verification"
DEFAULT_SUMMARY_JSON = DEFAULT_INPUT_DIR / "summary.json"
DEFAULT_FINDINGS_CSV = DEFAULT_INPUT_DIR / "findings.csv"
BAD_STATUSES = {
    "asset_type_mismatch",
    "name_mismatch",
    "missing_from_official",
    "non_active_official",
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def list_chunk_summary_paths(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("chunk-*-of-*.summary.json"))


def aggregate_chunk_summaries(input_dir: Path) -> dict[str, Any]:
    chunk_paths = list_chunk_summary_paths(input_dir)
    status_counts: dict[str, int] = {}
    chunks: list[dict[str, Any]] = []
    chunk_count = None
    for path in chunk_paths:
        payload = load_json(path)
        chunks.append(payload)
        chunk_count = payload["chunk_count"]
        for status, count in payload.get("status_counts", {}).items():
            status_counts[status] = status_counts.get(status, 0) + int(count)

    observed = {chunk["chunk_index"] for chunk in chunks}
    missing_chunks = [index for index in range(1, (chunk_count or 0) + 1) if index not in observed]
    return {
        "chunk_count": chunk_count or 0,
        "completed_chunks": len(chunks),
        "missing_chunks": missing_chunks,
        "items": sum(int(chunk.get("items", 0)) for chunk in chunks),
        "status_counts": dict(sorted(status_counts.items())),
        "chunks": chunks,
    }


def collect_findings(input_dir: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for path in sorted(input_dir.glob("chunk-*-of-*.json")):
        if path.name.endswith(".summary.json"):
            continue
        for row in load_json(path):
            if row.get("status") in BAD_STATUSES:
                findings.append(row)
    return findings


def write_findings_csv(path: Path, findings: list[dict[str, Any]]) -> None:
    if not findings:
        path.write_text("", encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(findings[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(findings)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate chunked stock masterfile verification outputs.")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--findings-csv", type=Path, default=DEFAULT_FINDINGS_CSV)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = aggregate_chunk_summaries(args.input_dir)
    findings = collect_findings(args.input_dir)
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.findings_csv.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **summary,
        "finding_rows": len(findings),
        "finding_examples": findings[:25],
        "summary_json": display_path(args.summary_json),
        "findings_csv": display_path(args.findings_csv),
    }
    args.summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_findings_csv(args.findings_csv, findings)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
