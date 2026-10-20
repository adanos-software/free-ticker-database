"""Pinned inputs and report-only outputs for independent reference comparisons."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from scripts.lib.dataio import write_csv, write_json


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def snapshot_metadata(path: Path, url: str) -> dict[str, Any]:
    digest = sha256(path)
    sidecar = path.with_suffix(path.suffix + ".meta.json")
    metadata = json.loads(sidecar.read_text()) if sidecar.exists() else {}
    if metadata.get("sha256", digest) != digest:
        raise ValueError(f"Snapshot hash disagrees with metadata: {path}")
    return {**metadata, "url": metadata.get("url", url), "sha256": digest,
            "bytes": path.stat().st_size, "last_modified": metadata.get("last_modified", "")}


def fetch_snapshot(url: str, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    try:
        with requests.get(url, headers={"User-Agent": "Adanos research contact@adanos-software.com"},
                          timeout=60, stream=True) as response:
            response.raise_for_status()
            with temporary.open("wb") as handle:
                for chunk in response.iter_content(1024 * 1024):
                    handle.write(chunk)
            metadata = {"url": url, "last_modified": response.headers.get("Last-Modified", ""),
                        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"), "sha256": sha256(temporary)}
        temporary.replace(path)
        write_json(path.with_suffix(path.suffix + ".meta.json"), metadata)
    finally:
        temporary.unlink(missing_ok=True)
    return snapshot_metadata(path, url)


def write_report(prefix: Path, title: str, summary: dict[str, Any],
                 rows: list[dict[str, Any]], fields: list[str], sources: list[dict[str, Any]],
                 limitations: list[str], *, inputs: tuple[Path, ...] = ()) -> dict[str, Any]:
    payload = {"generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"), "summary": summary,
               "sources": sources, "direct_identifier_apply_allowed_rows": 0,
               "inputs": [{"file": path.name, "sha256": sha256(path)} for path in inputs],
               "limitations": limitations}
    write_csv(prefix.with_suffix(".csv"), fields, rows)
    write_json(prefix.with_suffix(".json"), payload)
    lines = [f"# {title}", "", "Report only. No listing or identifier changes.", "",
             "| Metric | Value |", "| --- | ---: |"]
    lines.extend(f"| {key} | {value} |" for key, value in summary.items())
    lines.extend(["", "## Sources", ""])
    lines.extend(f"- {source.get('url', '')}; SHA-256 `{source['sha256']}`; last-modified `{source.get('last_modified', '') or 'not supplied'}`."
                 for source in sources)
    lines.extend(["", "## Limits", "", *[f"- {item}" for item in limitations], ""])
    prefix.with_suffix(".md").write_text("\n".join(lines), encoding="utf-8")
    return payload
