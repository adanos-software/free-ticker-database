from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DECISIONS = ROOT / "data" / "masterfiles" / "exchange_scope_decisions.csv"
DEFAULT_AUDIT = ROOT / "data" / "reports" / "exchange_source_audit.csv"

FIELDNAMES = [
    "exchange",
    "current_venue_status",
    "public_scope",
    "decision",
    "reason_code",
    "required_evidence",
    "commercial_option_key",
    "reviewed_at",
]
ALLOWED_PUBLIC_SCOPES = {
    "official_public_subset",
    "official_security_lookup_subset",
    "official_full_licensed_internal",
    "unofficial_source_gap",
}
BLOCKING_REASON_CODES = {
    "blocked_denominator_missing",
    "blocked_nonfresh_source",
    "blocked_product_class_gap",
    "blocked_recall_below_99_5",
    "blocked_source_unavailable",
}
MISSING_REASON_CODES = {
    "no_free_official_directory",
}


def _read(path: Path) -> tuple[list[str] | None, list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


def validate(decisions_path: Path, audit_path: Path) -> list[str]:
    errors: list[str] = []
    decision_fields, decisions = _read(decisions_path)
    _, audit_rows = _read(audit_path)
    if decision_fields != FIELDNAMES:
        errors.append(f"unexpected columns: {decision_fields!r}")

    partial = {
        row["exchange"]: row for row in audit_rows if row.get("venue_status") == "official_partial"
    }
    missing_venues = {
        row["exchange"]: row for row in audit_rows if row.get("venue_status") == "missing"
    }
    required = set(partial) | set(missing_venues)
    decision_exchanges = [row.get("exchange", "") for row in decisions]
    duplicates = sorted(
        {exchange for exchange in decision_exchanges if exchange and decision_exchanges.count(exchange) > 1}
    )
    if duplicates:
        errors.append(f"duplicate decisions: {', '.join(duplicates)}")
    missing = sorted(required - set(decision_exchanges))
    extra = sorted(set(decision_exchanges) - required)
    if missing:
        errors.append(f"missing venue-scope decisions: {', '.join(missing)}")
    if extra:
        errors.append(f"decisions for venues that are not official_partial or missing: {', '.join(extra)}")

    for line_number, row in enumerate(decisions, start=2):
        exchange = row.get("exchange", "")
        audit = partial.get(exchange) or missing_venues.get(exchange)
        if audit and row.get("current_venue_status") != audit.get("venue_status"):
            errors.append(
                f"line {line_number}: current_venue_status {row.get('current_venue_status')!r} "
                f"does not match audit venue_status {audit.get('venue_status')!r}"
            )
        for field in FIELDNAMES:
            if field != "commercial_option_key" and not row.get(field, "").strip():
                errors.append(f"line {line_number}: {field} is required")
        venue_status = row.get("current_venue_status")
        public_scope = row.get("public_scope")
        if public_scope == "official_full_public":
            errors.append(f"line {line_number}: official_full_public requires promotion evidence")
        elif public_scope not in ALLOWED_PUBLIC_SCOPES:
            errors.append(f"line {line_number}: unsupported public_scope")
        reviewed_reason = row.get("reason_code", "").strip()
        if venue_status == "missing":
            if row.get("decision") != "retain_missing":
                errors.append(f"line {line_number}: missing venues must retain_missing")
            if public_scope != "unofficial_source_gap":
                errors.append(f"line {line_number}: missing venues must use unofficial_source_gap")
            if reviewed_reason not in MISSING_REASON_CODES:
                errors.append(
                    f"line {line_number}: {exchange} has unsupported missing reason_code "
                    f"{reviewed_reason!r}"
                )
        else:
            if venue_status != "official_partial":
                errors.append(f"line {line_number}: current_venue_status must be official_partial or missing")
            if row.get("decision") != "retain_official_partial":
                errors.append(f"line {line_number}: decision must retain official_partial")
            if reviewed_reason and reviewed_reason not in BLOCKING_REASON_CODES:
                errors.append(
                    f"line {line_number}: {exchange} has unsupported reviewed reason_code "
                    f"{reviewed_reason!r}"
                )
            if audit and audit.get("venue_status") == "official_partial":
                current_readiness = audit.get("promotion_readiness", "").strip()
                if current_readiness == "ready_for_manual_scope_review":
                    errors.append(
                        f"line {line_number}: {exchange} current audit has no promotion blocker; "
                        "explicit scope review is required"
                    )
                elif current_readiness not in BLOCKING_REASON_CODES:
                    errors.append(
                        f"line {line_number}: {exchange} current audit has unsupported "
                        f"promotion_readiness {current_readiness!r}"
                    )
            if audit and "security_lookup_subset" in audit.get("reference_scopes", ""):
                if public_scope != "official_security_lookup_subset":
                    errors.append(f"line {line_number}: security lookup evidence needs lookup subset scope")
        if public_scope == "official_full_licensed_internal" and not row.get("commercial_option_key"):
            errors.append(f"line {line_number}: licensed internal scope requires a commercial option")
        try:
            date.fromisoformat(row.get("reviewed_at", ""))
        except ValueError:
            errors.append(f"line {line_number}: reviewed_at must be YYYY-MM-DD")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate explicit scope decisions for partial venues.")
    parser.add_argument("--decisions", type=Path, default=DEFAULT_DECISIONS)
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    args = parser.parse_args()
    errors = validate(args.decisions, args.audit)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"Exchange scope decisions valid: {args.decisions}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
