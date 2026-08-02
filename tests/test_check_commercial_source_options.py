import csv
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_commercial_source_options.py"
SPEC = importlib.util.spec_from_file_location("check_commercial_source_options", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MODULE.FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def valid_row() -> dict[str, str]:
    return {
        "option_key": "asx_referencepoint",
        "exchanges": "ASX",
        "provider": "ASX",
        "product": "ReferencePoint Master List",
        "coverage_claim": "Complete ASX reference data product",
        "access_model": "commercial_subscription",
        "redistribution_status": "contract_review_required",
        "evidence_url": "https://www.asx.com.au/connectivity-and-data/information-services/reference-data",
        "decision": "evaluate_contract",
        "next_action": "Obtain quote and explicit public redistribution terms",
        "reviewed_at": "2026-08-02",
    }


def test_validate_requires_explicit_redistribution_review(tmp_path: Path) -> None:
    row = valid_row()
    row["redistribution_status"] = ""
    path = tmp_path / "options.csv"
    write_rows(path, [row])

    errors = MODULE.validate(path, required_option_keys={"asx_referencepoint"})

    assert any("redistribution_status" in error for error in errors)


def test_validate_accepts_complete_review_gated_matrix(tmp_path: Path) -> None:
    path = tmp_path / "options.csv"
    write_rows(path, [valid_row()])

    assert MODULE.validate(path, required_option_keys={"asx_referencepoint"}) == []
