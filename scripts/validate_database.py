from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import is_common_single_word_alias, normalize_alias_text
from scripts.check_entry_quality_gate import allowed_warn_keys, check_entry_quality_gate
from scripts.rebuild_dataset import country_from_isin, is_valid_isin

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_ADANOS_REFERENCE_CSV = DATA_DIR / "adanos" / "ticker_reference.csv"
DEFAULT_ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
DEFAULT_WARN_ALLOWLIST_CSV = REPORTS_DIR / "entry_quality_warn_allowlist.csv"
DEFAULT_ADANOS_ALIAS_AUDIT_CSV = REPORTS_DIR / "adanos_alias_audit.csv"
DEFAULT_REVIEW_REMOVE_ALIASES_CSV = DATA_DIR / "review_overrides" / "remove_aliases.csv"
DEFAULT_COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_JSON_OUT = REPORTS_DIR / "validation_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "validation_report.md"

ASSET_TYPES = {"Stock", "ETF"}
PRIMARY_SCOPE_REASONS = {"primary_listing", "primary_listing_missing_isin", "otc_listing"}

TICKERS_COLUMNS = {
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
}
LISTINGS_COLUMNS = {"listing_key", *TICKERS_COLUMNS}
INSTRUMENT_SCOPE_COLUMNS = {
    "listing_key",
    "ticker",
    "exchange",
    "instrument_scope",
    "scope_reason",
    "primary_listing_key",
}
ADANOS_COLUMNS = {
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
}
ENTRY_QUALITY_COLUMNS = {"listing_key", "quality_status"}
MOJIBAKE_RE = re.compile(r"(?:Ã[\u0080-\u00BF]|Â[\u0080-\u00BF]|â[\u0080-\u00BF]{1,2}|�|\\x[0-9a-fA-F]{2})")


@dataclass(frozen=True)
class Gate:
    name: str
    severity: str
    passed: bool
    actual: int
    limit: int | None = 0
    details: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "severity": self.severity,
            "passed": self.passed,
            "actual": self.actual,
            "limit": self.limit,
        }
        if self.details:
            payload["details"] = self.details[:50]
        return payload


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def csv_columns(path: Path) -> set[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return set(reader.fieldnames or [])


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def fail_gate(name: str, actual: int, details: list[str] | None = None, limit: int = 0) -> Gate:
    return Gate(name=name, severity="error", passed=actual <= limit, actual=actual, limit=limit, details=details)


def info_gate(name: str, actual: int, details: list[str] | None = None) -> Gate:
    return Gate(name=name, severity="info", passed=True, actual=actual, limit=None, details=details)


def duplicate_values(rows: list[dict[str, str]], field: str) -> list[str]:
    counts = Counter(row.get(field, "") for row in rows if row.get(field, ""))
    return sorted(value for value, count in counts.items() if count > 1)


def invalid_asset_type_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    return [row.get(id_field, "") for row in rows if row.get("asset_type") not in ASSET_TYPES]


def invalid_isin_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        if isin and not is_valid_isin(isin):
            invalid.append(row.get(id_field, ""))
    return invalid


def invalid_country_code_rows(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        code = row.get("country_code", "").strip()
        if row.get("country") and (len(code) != 2 or not code.isalpha() or code.upper() != code):
            invalid.append(row.get(id_field, ""))
    return invalid


def rows_missing_country_metadata_despite_isin(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        if not isin:
            continue
        if not country_from_isin(isin):
            continue
        country = row.get("country", "").strip()
        country_code = row.get("country_code", "").strip()
        if not country or not country_code:
            invalid.append(row.get(id_field, ""))
    return invalid


def rows_with_mojibake_names(rows: list[dict[str, str]], id_field: str) -> list[str]:
    invalid: list[str] = []
    for row in rows:
        name = row.get("name", "")
        if name and MOJIBAKE_RE.search(name):
            invalid.append(row.get(id_field, ""))
    return invalid


def unresolved_review_alias_removals(
    tickers: list[dict[str, str]],
    review_remove_aliases: list[dict[str, str]],
) -> list[str]:
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers}
    unresolved: list[str] = []
    for review in review_remove_aliases:
        row = by_key.get((review.get("ticker", ""), review.get("exchange", "")))
        if not row:
            continue
        aliases = {alias for alias in row.get("aliases", "").split("|") if alias}
        alias = review.get("alias", "")
        if alias and alias in aliases:
            unresolved.append(f"{review.get('exchange', '')}::{review.get('ticker', '')}:{alias}")
    return unresolved


def listing_key(row: dict[str, str]) -> str:
    return f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def parse_adanos_aliases(value: str) -> tuple[list[str], bool]:
    try:
        parsed = json.loads(value or "[]")
    except json.JSONDecodeError:
        return [], False
    if not isinstance(parsed, list):
        return [], False
    aliases = [alias for alias in parsed if isinstance(alias, str) and alias.strip()]
    return aliases, len(aliases) == len(parsed)


def required_column_gates(
    path_to_columns: dict[Path, set[str]],
    required_by_path: dict[Path, set[str]] | None = None,
) -> list[Gate]:
    gates: list[Gate] = []
    required_by_path = required_by_path or {
        DEFAULT_TICKERS_CSV: TICKERS_COLUMNS,
        DEFAULT_LISTINGS_CSV: LISTINGS_COLUMNS,
        DEFAULT_INSTRUMENT_SCOPES_CSV: INSTRUMENT_SCOPE_COLUMNS,
        DEFAULT_ADANOS_REFERENCE_CSV: ADANOS_COLUMNS,
        DEFAULT_ENTRY_QUALITY_CSV: ENTRY_QUALITY_COLUMNS,
    }
    for path, required in required_by_path.items():
        columns = path_to_columns.get(path, set())
        missing = sorted(required - columns)
        gates.append(fail_gate(f"required_columns:{display_path(path)}", len(missing), missing))
    return gates


def compare_coverage_report(
    coverage_report: dict[str, Any] | None,
    tickers: list[dict[str, str]],
    listings: list[dict[str, str]],
) -> list[Gate]:
    if not coverage_report:
        return [info_gate("coverage_report_missing", 1, ["coverage report was not provided"])]

    global_metrics = coverage_report.get("global") or coverage_report.get("summary") or {}
    gates: list[Gate] = []
    expected = {
        "tickers": len(tickers),
        "listing_keys": len(listings),
    }
    for metric, actual_count in expected.items():
        report_count = global_metrics.get(metric)
        mismatch = 0 if report_count == actual_count else 1
        gates.append(
            fail_gate(
                f"coverage_report_{metric}_mismatch",
                mismatch,
                [] if not mismatch else [f"report={report_count} actual={actual_count}"],
            )
        )
    return gates


def build_validation_report(
    *,
    tickers: list[dict[str, str]],
    listings: list[dict[str, str]],
    instrument_scopes: list[dict[str, str]],
    adanos_reference: list[dict[str, str]],
    entry_quality: list[dict[str, str]],
    allowed_warns: set[str],
    adanos_alias_findings: list[dict[str, str]],
    review_remove_aliases: list[dict[str, str]] | None = None,
    coverage_report: dict[str, Any] | None = None,
    path_to_columns: dict[Path, set[str]] | None = None,
    required_columns_by_path: dict[Path, set[str]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or utc_now()
    path_to_columns = path_to_columns or {}
    review_remove_aliases = review_remove_aliases or []

    listing_keys = {row["listing_key"] for row in listings if row.get("listing_key")}
    instrument_scope_by_key = {
        row["listing_key"]: row for row in instrument_scopes if row.get("listing_key")
    }
    ticker_listing_keys = [listing_key(row) for row in tickers]
    entry_gate = check_entry_quality_gate(entry_quality, allowed_warns)

    adanos_alias_parse_errors: list[str] = []
    adanos_common_word_aliases: list[str] = []
    for row in adanos_reference:
        aliases, parsed = parse_adanos_aliases(row.get("aliases", ""))
        if not parsed:
            adanos_alias_parse_errors.append(row.get("ticker", ""))
            continue
        for alias in aliases:
            normalized = normalize_alias_text(alias).lower()
            if normalized and is_common_single_word_alias(normalized):
                adanos_common_word_aliases.append(f"{row.get('ticker', '')}:{alias}")

    ticker_scope_rows = [instrument_scope_by_key.get(key, {}) for key in ticker_listing_keys]
    secondary_primary_rows = [
        key
        for key, scope_row in zip(ticker_listing_keys, ticker_scope_rows, strict=False)
        if scope_row.get("scope_reason") == "secondary_cross_listing"
    ]
    invalid_primary_scope_rows = [
        key
        for key, scope_row in zip(ticker_listing_keys, ticker_scope_rows, strict=False)
        if scope_row and scope_row.get("scope_reason") not in PRIMARY_SCOPE_REASONS
    ]

    gates: list[Gate] = []
    if path_to_columns:
        gates.extend(required_column_gates(path_to_columns, required_columns_by_path))
    gates.extend(
        [
            fail_gate("duplicate_primary_ticker_count", len(duplicate_values(tickers, "ticker"))),
            fail_gate("duplicate_listing_key_count", len(duplicate_values(listings, "listing_key"))),
            fail_gate("duplicate_adanos_ticker_count", len(duplicate_values(adanos_reference, "ticker"))),
            fail_gate(
                "invalid_ticker_asset_type_rows",
                len(invalid_asset_type_rows(tickers, "ticker")),
                invalid_asset_type_rows(tickers, "ticker"),
            ),
            fail_gate(
                "invalid_listing_asset_type_rows",
                len(invalid_asset_type_rows(listings, "listing_key")),
                invalid_asset_type_rows(listings, "listing_key"),
            ),
            fail_gate(
                "invalid_adanos_asset_type_rows",
                len(invalid_asset_type_rows(adanos_reference, "ticker")),
                invalid_asset_type_rows(adanos_reference, "ticker"),
            ),
            fail_gate(
                "invalid_isin_rows",
                len(invalid_isin_rows(tickers, "ticker")) + len(invalid_isin_rows(listings, "listing_key")),
                invalid_isin_rows(tickers, "ticker") + invalid_isin_rows(listings, "listing_key"),
            ),
            fail_gate(
                "invalid_country_code_rows",
                len(invalid_country_code_rows(tickers, "ticker"))
                + len(invalid_country_code_rows(listings, "listing_key")),
                invalid_country_code_rows(tickers, "ticker") + invalid_country_code_rows(listings, "listing_key"),
            ),
            fail_gate(
                "rows_missing_country_metadata_despite_isin",
                len(rows_missing_country_metadata_despite_isin(tickers, "ticker"))
                + len(rows_missing_country_metadata_despite_isin(listings, "listing_key")),
                rows_missing_country_metadata_despite_isin(tickers, "ticker")
                + rows_missing_country_metadata_despite_isin(listings, "listing_key"),
            ),
            fail_gate(
                "rows_with_mojibake_names",
                len(rows_with_mojibake_names(tickers, "ticker")) + len(rows_with_mojibake_names(listings, "listing_key")),
                rows_with_mojibake_names(tickers, "ticker") + rows_with_mojibake_names(listings, "listing_key"),
            ),
            fail_gate(
                "ticker_rows_missing_listing",
                len([key for key in ticker_listing_keys if key not in listing_keys]),
                [key for key in ticker_listing_keys if key not in listing_keys],
            ),
            fail_gate(
                "listing_rows_missing_instrument_scope",
                len([key for key in listing_keys if key not in instrument_scope_by_key]),
                [key for key in listing_keys if key not in instrument_scope_by_key],
            ),
            fail_gate(
                "primary_rows_that_are_known_secondary_cross_listings",
                len(secondary_primary_rows),
                secondary_primary_rows,
            ),
            fail_gate("primary_rows_with_invalid_scope_reason", len(invalid_primary_scope_rows), invalid_primary_scope_rows),
            fail_gate(
                "stock_rows_with_etf_category",
                len([row["ticker"] for row in tickers if row.get("asset_type") == "Stock" and row.get("etf_category")]),
            ),
            fail_gate(
                "etf_rows_with_stock_sector",
                len([row["ticker"] for row in tickers if row.get("asset_type") == "ETF" and row.get("stock_sector")]),
            ),
            fail_gate(
                "adanos_reference_row_count_mismatch",
                0 if len(adanos_reference) == len(tickers) else abs(len(adanos_reference) - len(tickers)),
                [] if len(adanos_reference) == len(tickers) else [f"adanos={len(adanos_reference)} tickers={len(tickers)}"],
            ),
            fail_gate(
                "entry_quality_quarantine_count",
                int(entry_gate["quarantine_count"]),
                list(entry_gate.get("quarantined", [])),
            ),
            fail_gate(
                "entry_quality_unexpected_warn_count",
                int(entry_gate["unexpected_warn_count"]),
                list(entry_gate.get("unexpected_warns", [])),
            ),
            fail_gate(
                "adanos_alias_findings",
                len(adanos_alias_findings),
                [
                    f"{row.get('ticker', '')}:{row.get('alias', '')}:{row.get('issue_type', '')}"
                    for row in adanos_alias_findings
                ],
            ),
            fail_gate("adanos_alias_parse_errors", len(adanos_alias_parse_errors), adanos_alias_parse_errors),
            fail_gate("adanos_alias_common_word_count", len(adanos_common_word_aliases), adanos_common_word_aliases),
            fail_gate(
                "review_alias_removals_open_count",
                len(unresolved_review_alias_removals(tickers, review_remove_aliases)),
                unresolved_review_alias_removals(tickers, review_remove_aliases),
            ),
            info_gate(
                "expected_missing_primary_isin",
                sum(1 for row in entry_quality if "expected_missing_primary_isin" in row.get("issue_types", "")),
            ),
            info_gate(
                "missing_stock_sector",
                sum(1 for row in entry_quality if "missing_stock_sector" in row.get("issue_types", "")),
            ),
            info_gate(
                "missing_etf_category",
                sum(1 for row in entry_quality if "missing_etf_category" in row.get("issue_types", "")),
            ),
            info_gate("source_gap_rows", sum(1 for row in entry_quality if row.get("quality_status") == "source_gap")),
            info_gate("allowed_warn_rows", int(entry_gate["allowed_warn_count"])),
        ]
    )
    gates.extend(compare_coverage_report(coverage_report, tickers, listings))

    failed_error_gates = [gate for gate in gates if gate.severity == "error" and not gate.passed]
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "tickers_csv": display_path(DEFAULT_TICKERS_CSV),
                "listings_csv": display_path(DEFAULT_LISTINGS_CSV),
                "instrument_scopes_csv": display_path(DEFAULT_INSTRUMENT_SCOPES_CSV),
                "adanos_reference_csv": display_path(DEFAULT_ADANOS_REFERENCE_CSV),
                "entry_quality_csv": display_path(DEFAULT_ENTRY_QUALITY_CSV),
                "adanos_alias_audit_csv": display_path(DEFAULT_ADANOS_ALIAS_AUDIT_CSV),
                "review_remove_aliases_csv": display_path(DEFAULT_REVIEW_REMOVE_ALIASES_CSV),
                "coverage_report_json": display_path(DEFAULT_COVERAGE_REPORT_JSON),
            },
        },
        "passed": not failed_error_gates,
        "summary": {
            "ticker_rows": len(tickers),
            "listing_rows": len(listings),
            "adanos_reference_rows": len(adanos_reference),
            "entry_quality_rows": len(entry_quality),
            "error_gates": sum(1 for gate in gates if gate.severity == "error"),
            "failed_error_gates": len(failed_error_gates),
            "info_gates": sum(1 for gate in gates if gate.severity == "info"),
        },
        "gates": [gate.to_dict() for gate in gates],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    status = "PASS" if payload["passed"] else "FAIL"
    lines = [
        "# Database Validation Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        f"Status: `{status}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in payload["summary"].items():
        lines.append(f"| {key} | {value:,} |" if isinstance(value, int) else f"| {key} | {value} |")

    lines.extend(["", "## Gates", "", "| Gate | Severity | Status | Actual | Limit |", "|---|---|---|---:|---:|"])
    for gate in payload["gates"]:
        gate_status = "PASS" if gate["passed"] else "FAIL"
        limit = "" if gate["limit"] is None else str(gate["limit"])
        lines.append(f"| {gate['name']} | {gate['severity']} | {gate_status} | {gate['actual']} | {limit} |")

    failed = [gate for gate in payload["gates"] if gate["severity"] == "error" and not gate["passed"]]
    lines.extend(["", "## Failed Gate Details", ""])
    if not failed:
        lines.append("_No failed error gates._")
    for gate in failed:
        lines.extend([f"### {gate['name']}", "", f"- Actual: `{gate['actual']}`"])
        for detail in gate.get("details", [])[:25]:
            lines.append(f"- `{detail}`")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ticker database release gates.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=DEFAULT_INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--adanos-reference-csv", type=Path, default=DEFAULT_ADANOS_REFERENCE_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=DEFAULT_ENTRY_QUALITY_CSV)
    parser.add_argument("--warn-allowlist-csv", type=Path, default=DEFAULT_WARN_ALLOWLIST_CSV)
    parser.add_argument("--adanos-alias-audit-csv", type=Path, default=DEFAULT_ADANOS_ALIAS_AUDIT_CSV)
    parser.add_argument("--review-remove-aliases-csv", type=Path, default=DEFAULT_REVIEW_REMOVE_ALIASES_CSV)
    parser.add_argument("--coverage-report-json", type=Path, default=DEFAULT_COVERAGE_REPORT_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--no-write", action="store_true", help="Print summary only; do not write report files.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = [
        args.tickers_csv,
        args.listings_csv,
        args.instrument_scopes_csv,
        args.adanos_reference_csv,
        args.entry_quality_csv,
    ]
    path_to_columns = {path: csv_columns(path) for path in paths}
    required_columns_by_path = {
        args.tickers_csv: TICKERS_COLUMNS,
        args.listings_csv: LISTINGS_COLUMNS,
        args.instrument_scopes_csv: INSTRUMENT_SCOPE_COLUMNS,
        args.adanos_reference_csv: ADANOS_COLUMNS,
        args.entry_quality_csv: ENTRY_QUALITY_COLUMNS,
    }
    coverage_report = load_json(args.coverage_report_json) if args.coverage_report_json.exists() else None
    payload = build_validation_report(
        tickers=load_csv(args.tickers_csv),
        listings=load_csv(args.listings_csv),
        instrument_scopes=load_csv(args.instrument_scopes_csv),
        adanos_reference=load_csv(args.adanos_reference_csv),
        entry_quality=load_csv(args.entry_quality_csv),
        allowed_warns=allowed_warn_keys(args.warn_allowlist_csv),
        adanos_alias_findings=load_csv(args.adanos_alias_audit_csv) if args.adanos_alias_audit_csv.exists() else [],
        review_remove_aliases=(
            load_csv(args.review_remove_aliases_csv) if args.review_remove_aliases_csv.exists() else []
        ),
        coverage_report=coverage_report,
        path_to_columns=path_to_columns,
        required_columns_by_path=required_columns_by_path,
    )
    if not args.no_write:
        write_json(args.json_out, payload)
        write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "passed": payload["passed"],
                "summary": payload["summary"],
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
