import csv
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_exchange_scope_decisions.py"
SPEC = importlib.util.spec_from_file_location("check_exchange_scope_decisions", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def audit_row(exchange: str = "AAA") -> dict[str, object]:
    return {
        "exchange": exchange,
        "venue_status": "official_partial",
        "reference_scopes": "listed_companies_subset",
        "promotion_readiness": "blocked_denominator_missing",
    }


def decision_row(exchange: str = "AAA") -> dict[str, object]:
    return {
        "exchange": exchange,
        "current_venue_status": "official_partial",
        "public_scope": "official_public_subset",
        "decision": "retain_official_partial",
        "reason_code": "blocked_denominator_missing",
        "required_evidence": "Fresh full exchange directory and >=99.5% collision-adjusted recall",
        "commercial_option_key": "",
        "reviewed_at": "2026-08-02",
    }


def test_validate_requires_one_decision_per_partial_exchange(tmp_path: Path) -> None:
    audit = tmp_path / "audit.csv"
    decisions = tmp_path / "decisions.csv"
    write_csv(audit, list(audit_row().keys()), [audit_row("AAA"), audit_row("BBB")])
    write_csv(decisions, MODULE.FIELDNAMES, [decision_row("AAA")])

    errors = MODULE.validate(decisions, audit)

    assert any("BBB" in error for error in errors)


def test_validate_rejects_full_public_claim_for_blocked_exchange(tmp_path: Path) -> None:
    audit = tmp_path / "audit.csv"
    decisions = tmp_path / "decisions.csv"
    write_csv(audit, list(audit_row().keys()), [audit_row()])
    row = decision_row()
    row["public_scope"] = "official_full_public"
    write_csv(decisions, MODULE.FIELDNAMES, [row])

    errors = MODULE.validate(decisions, audit)

    assert any("official_full_public" in error for error in errors)


def test_validate_accepts_review_gated_subset(tmp_path: Path) -> None:
    audit = tmp_path / "audit.csv"
    decisions = tmp_path / "decisions.csv"
    write_csv(audit, list(audit_row().keys()), [audit_row()])
    write_csv(decisions, MODULE.FIELDNAMES, [decision_row()])

    assert MODULE.validate(decisions, audit) == []


def test_validate_accepts_reason_code_drift_while_current_audit_remains_blocked(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "audit.csv"
    decisions_path = tmp_path / "decisions.csv"
    row = audit_row("SIX")
    row["promotion_readiness"] = "blocked_nonfresh_source"
    write_csv(audit_path, list(row.keys()), [row])
    write_csv(decisions_path, MODULE.FIELDNAMES, [decision_row("SIX")])

    assert MODULE.validate(decisions_path, audit_path) == []


def test_validate_rejects_retained_scope_when_current_audit_has_no_blocker(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "audit.csv"
    decisions_path = tmp_path / "decisions.csv"
    row = audit_row("SIX")
    row["promotion_readiness"] = "ready_for_manual_scope_review"
    write_csv(audit_path, list(row.keys()), [row])
    write_csv(decisions_path, MODULE.FIELDNAMES, [decision_row("SIX")])

    errors = MODULE.validate(decisions_path, audit_path)

    assert errors == [
        "line 2: SIX current audit has no promotion blocker; explicit scope review is required"
    ]


def test_validate_rejects_unknown_current_audit_readiness(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.csv"
    decisions_path = tmp_path / "decisions.csv"
    row = audit_row("SIX")
    row["promotion_readiness"] = "blocked_new_unreviewed_condition"
    write_csv(audit_path, list(row.keys()), [row])
    write_csv(decisions_path, MODULE.FIELDNAMES, [decision_row("SIX")])

    errors = MODULE.validate(decisions_path, audit_path)

    assert errors == [
        "line 2: SIX current audit has unsupported promotion_readiness "
        "'blocked_new_unreviewed_condition'"
    ]


def test_validate_rejects_unknown_reviewed_reason_code(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.csv"
    decisions_path = tmp_path / "decisions.csv"
    row = decision_row("SIX")
    row["reason_code"] = "blocked_new_unreviewed_condition"
    write_csv(audit_path, list(audit_row().keys()), [audit_row("SIX")])
    write_csv(decisions_path, MODULE.FIELDNAMES, [row])

    errors = MODULE.validate(decisions_path, audit_path)

    assert errors == [
        "line 2: SIX has unsupported reviewed reason_code 'blocked_new_unreviewed_condition'"
    ]
