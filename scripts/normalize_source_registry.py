"""Normalize source governance fields without inventing legal conclusions."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
SOURCES_JSON = ROOT / "data" / "masterfiles" / "sources.json"
SOURCE_LICENSING_JSON = ROOT / "data" / "reports" / "source_licensing.json"

SOURCE_FIELDS = [
    "key", "provider", "description", "source_url", "format", "reference_scope", "official",
    "authority_level", "license_status", "license_name", "license_url",
    "derived_facts_redistribution_status", "raw_redistribution_allowed",
    "attribution_required", "commercial_use_status", "terms_version", "terms_sha256",
    "license_reviewed_at", "freshness_sla_days", "enabled",
]


def _text(row: Mapping[str, Any], key: str, default: str = "") -> str:
    value = row.get(key, default)
    return str(value).strip() if value is not None else default


def normalize_source(row: Mapping[str, Any]) -> dict[str, Any]:
    official = bool(row.get("official", False))
    scope = _text(row, "reference_scope")
    source_url = _text(row, "source_url")
    internal = source_url.startswith("internal://")
    license_status = _text(row, "license_status", "internal" if internal else "review_required")
    if license_status not in {"internal", "verified_open", "verified_restricted", "review_required"}:
        license_status = "review_required"
    normalized = {
        "key": _text(row, "key"),
        "provider": _text(row, "provider"),
        "description": _text(row, "description"),
        "source_url": source_url,
        "format": _text(row, "format"),
        "reference_scope": scope,
        "official": official,
        "authority_level": _text(row, "authority_level", "official" if official else "secondary"),
        "license_status": license_status,
        "license_name": _text(row, "license_name"),
        "license_url": _text(row, "license_url"),
        "derived_facts_redistribution_status": _text(
            row, "derived_facts_redistribution_status", "allowed" if internal else "review_required"
        ),
        "raw_redistribution_allowed": bool(row.get("raw_redistribution_allowed", False)),
        "attribution_required": _text(row, "attribution_required", "none" if internal else "review_required"),
        "commercial_use_status": _text(row, "commercial_use_status", "allowed" if internal else "review_required"),
        "terms_version": _text(row, "terms_version"),
        "terms_sha256": _text(row, "terms_sha256").lower(),
        "license_reviewed_at": _text(row, "license_reviewed_at"),
        "freshness_sla_days": int(row.get("freshness_sla_days") or (7 if official and scope == "exchange_directory" else 30)),
        "enabled": bool(row.get("enabled", True)),
    }
    if not normalized["key"]:
        raise ValueError("source registry entry is missing key")
    if normalized["freshness_sla_days"] <= 0:
        raise ValueError(f"source {normalized['key']} has invalid freshness_sla_days")
    return normalized


def normalize_registry(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [normalize_source(row) for row in rows]
    keys = [row["key"] for row in normalized]
    duplicates = sorted(key for key, count in Counter(keys).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate source keys: {duplicates}")
    return normalized


def build(*, sources_json: Path = SOURCES_JSON, licensing_json: Path = SOURCE_LICENSING_JSON) -> dict[str, Any]:
    payload = json.loads(sources_json.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("sources.json must contain a list")
    rows = normalize_registry(payload)
    sources_json.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    counts = Counter(row["license_status"] for row in rows)
    report = {
        "sources": len(rows),
        "license_status_counts": dict(sorted(counts.items())),
        "raw_redistribution_allowed_sources": sum(bool(row["raw_redistribution_allowed"]) for row in rows),
        "review_required_sources": sum(row["license_status"] == "review_required" for row in rows),
        "verified_restricted_sources": sum(row["license_status"] == "verified_restricted" for row in rows),
        "sources_with_terms_hash": sum(len(row["terms_sha256"]) == 64 for row in rows),
        "policy": "Unknown legal terms remain review_required; normalization never infers permission.",
    }
    licensing_json.parent.mkdir(parents=True, exist_ok=True)
    licensing_json.write_text(json.dumps({"summary": report, "sources": rows}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources-json", type=Path, default=SOURCES_JSON)
    parser.add_argument("--licensing-json", type=Path, default=SOURCE_LICENSING_JSON)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build(sources_json=args.sources_json, licensing_json=args.licensing_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
