from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATH = ROOT / "data" / "masterfiles" / "commercial_source_options.csv"

FIELDNAMES = [
    "option_key",
    "exchanges",
    "provider",
    "product",
    "coverage_claim",
    "access_model",
    "redistribution_status",
    "evidence_url",
    "decision",
    "next_action",
    "reviewed_at",
]

REQUIRED_OPTION_KEYS = {
    "asx_referencepoint",
    "athex_reference_data_service",
    "bursa_information_services",
    "jse_reference_data",
    "nasdaq_nordic_reference_data",
    "six_reference_data",
}
ALLOWED_ACCESS_MODELS = {"commercial_subscription", "licensed_feed", "sales_contact_required"}
ALLOWED_REDISTRIBUTION_STATUSES = {
    "contract_review_required",
    "prior_written_permission_required",
}
ALLOWED_DECISIONS = {"evaluate_contract", "hold_public_subset"}


def _valid_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc)


def validate(path: Path, required_option_keys: Iterable[str] = REQUIRED_OPTION_KEYS) -> list[str]:
    errors: list[str] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FIELDNAMES:
            errors.append(f"unexpected columns: {reader.fieldnames!r}")
        rows = list(reader)

    keys = [row.get("option_key", "") for row in rows]
    duplicates = sorted({key for key in keys if key and keys.count(key) > 1})
    if duplicates:
        errors.append(f"duplicate option_key values: {', '.join(duplicates)}")
    missing_keys = sorted(set(required_option_keys) - set(keys))
    if missing_keys:
        errors.append(f"missing required options: {', '.join(missing_keys)}")

    for line_number, row in enumerate(rows, start=2):
        for field in FIELDNAMES:
            if not row.get(field, "").strip():
                errors.append(f"line {line_number}: {field} is required")
        if row.get("access_model") not in ALLOWED_ACCESS_MODELS:
            errors.append(f"line {line_number}: unsupported access_model")
        if row.get("redistribution_status") not in ALLOWED_REDISTRIBUTION_STATUSES:
            errors.append(f"line {line_number}: redistribution_status must remain review-gated")
        if row.get("decision") not in ALLOWED_DECISIONS:
            errors.append(f"line {line_number}: unsupported decision")
        if not _valid_http_url(row.get("evidence_url", "")):
            errors.append(f"line {line_number}: evidence_url must be an HTTPS URL")
        try:
            date.fromisoformat(row.get("reviewed_at", ""))
        except ValueError:
            errors.append(f"line {line_number}: reviewed_at must be YYYY-MM-DD")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate commercial reference-data options.")
    parser.add_argument("path", nargs="?", type=Path, default=DEFAULT_PATH)
    args = parser.parse_args()
    errors = validate(args.path)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"Commercial source options valid: {args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
