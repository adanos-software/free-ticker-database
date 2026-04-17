from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import is_common_single_word_alias, normalize_alias_text
from scripts.rebuild_dataset import (
    LEGAL_ALIAS_SUFFIX_PATTERNS,
    alias_matches_company,
    etf_alias_candidate,
    stock_alias_candidates,
)

DATA_DIR = ROOT / "data"
DEFAULT_INPUT = DATA_DIR / "adanos" / "ticker_reference.csv"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_CSV_OUT = REPORTS_DIR / "adanos_alias_audit.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "adanos_alias_audit.json"
DEFAULT_MD_OUT = REPORTS_DIR / "adanos_alias_audit.md"

FIELDNAMES = [
    "row_index",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "alias",
    "issue_type",
    "severity",
    "suggested_fix_or_rule",
]

MOJIBAKE_RE = re.compile(r"(?:Ã|Â|â|�|\\x[0-9a-fA-F]{2})")
ETF_LONG_ALIAS_RE = re.compile(
    r"\b(?:ucits|etf|etn|fund|trust|index|daily|shares?|series|portfolio)\b",
    re.IGNORECASE,
)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parse_aliases(value: str) -> list[str]:
    try:
        aliases = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    return [alias for alias in aliases if isinstance(alias, str) and alias.strip()]


def has_legal_suffix(alias: str) -> bool:
    return any(pattern.search(alias) for pattern in LEGAL_ALIAS_SUFFIX_PATTERNS)


def expected_aliases(row: dict[str, str], alias: str) -> set[str]:
    if row["asset_type"] == "ETF":
        candidate = etf_alias_candidate(alias)
        return {candidate} if candidate else set()
    return set(stock_alias_candidates(alias))


def issue(
    row: dict[str, str],
    *,
    row_index: int,
    alias: str,
    issue_type: str,
    severity: str,
    suggested_fix_or_rule: str,
) -> dict[str, str]:
    return {
        "row_index": str(row_index),
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "alias": alias,
        "issue_type": issue_type,
        "severity": severity,
        "suggested_fix_or_rule": suggested_fix_or_rule,
    }


def audit_row(row: dict[str, str], row_index: int) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    aliases = parse_aliases(row.get("aliases", "[]"))
    for alias in aliases:
        normalized = normalize_alias_text(alias).lower()
        if alias != normalized:
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="alias_not_normalized",
                    severity="medium",
                    suggested_fix_or_rule=f"Use normalized alias `{normalized}`.",
                )
            )
        if MOJIBAKE_RE.search(alias):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="mojibake_or_replacement_character",
                    severity="high",
                    suggested_fix_or_rule="Fix source encoding or remove alias.",
                )
            )
        if any(ord(character) > 127 for character in alias):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="non_ascii_alias",
                    severity="medium",
                    suggested_fix_or_rule="Normalize alias to ASCII for sentiment API lookup.",
                )
            )
        if has_legal_suffix(alias):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="legal_or_security_suffix",
                    severity="medium",
                    suggested_fix_or_rule="Strip legal/security suffix via alias normalization.",
                )
            )
        if is_common_single_word_alias(alias):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="common_single_word_alias",
                    severity="high",
                    suggested_fix_or_rule="Remove from natural-language alias export.",
                )
            )
        if row["asset_type"] == "ETF" and len(alias.split()) >= 5 and ETF_LONG_ALIAS_RE.search(alias):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="long_etf_product_alias",
                    severity="medium",
                    suggested_fix_or_rule="Use shortened ETF theme/index alias instead of full product name.",
                )
            )
        if normalized and normalized not in expected_aliases(row, alias) and not alias_matches_company(alias, row["name"]):
            findings.append(
                issue(
                    row,
                    row_index=row_index,
                    alias=alias,
                    issue_type="alias_name_mismatch",
                    severity="high",
                    suggested_fix_or_rule="Remove as likely cross-issuer contamination unless manually trusted.",
                )
            )
    return findings


def build_report(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for row_index, row in enumerate(rows, start=1):
        findings.extend(audit_row(row, row_index))
    return findings


def write_csv(path: Path, findings: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(findings)


def write_json(path: Path, findings: list[dict[str, str]], generated_at: str) -> None:
    issue_counts = Counter(finding["issue_type"] for finding in findings)
    severity_counts = Counter(finding["severity"] for finding in findings)
    payload = {
        "generated_at": generated_at,
        "rows": len(findings),
        "issue_counts": dict(issue_counts.most_common()),
        "severity_counts": dict(severity_counts.most_common()),
        "examples": findings[:100],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, findings: list[dict[str, str]], generated_at: str) -> None:
    issue_counts = Counter(finding["issue_type"] for finding in findings)
    lines = [
        "# Adanos Alias Audit",
        "",
        f"Generated at: `{generated_at}`",
        "",
        f"Findings: `{len(findings):,}`",
        "",
        "## Issue Counts",
        "",
        "| Issue | Rows |",
        "|---|---:|",
    ]
    for issue_type, count in issue_counts.most_common():
        lines.append(f"| {issue_type} | {count:,} |")
    lines.extend(["", "## Examples", "", "| Row | Ticker | Alias | Issue | Suggested Fix |", "|---:|---|---|---|---|"])
    for finding in findings[:50]:
        lines.append(
            "| {row_index} | {ticker} | {alias} | {issue_type} | {suggested_fix_or_rule} |".format(
                **finding
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Adanos ticker_reference aliases for sentiment API safety.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    findings = build_report(load_rows(args.input))
    generated_at = utc_now()
    write_csv(args.csv_out, findings)
    write_json(args.json_out, findings, generated_at)
    write_markdown(args.md_out, findings, generated_at)
    print(
        json.dumps(
            {
                "findings": len(findings),
                "csv_out": str(args.csv_out.relative_to(ROOT)),
                "json_out": str(args.json_out.relative_to(ROOT)),
                "md_out": str(args.md_out.relative_to(ROOT)),
                "issue_counts": Counter(finding["issue_type"] for finding in findings),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
