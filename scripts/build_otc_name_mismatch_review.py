from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_entry_quality_report import (
    DEFAULT_CSV_OUT as ENTRY_QUALITY_CSV,
    MASTERFILE_REFERENCE_CSV,
    abbreviated_official_name_matches,
    informative_name_tokens,
    is_valid_isin,
    names_match,
    official_name_is_informative,
)

REPORTS_DIR = ROOT / "data" / "reports"
DEFAULT_CSV_OUT = REPORTS_DIR / "otc_name_mismatch_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "otc_name_mismatch_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "otc_name_mismatch_review.md"


@dataclass(frozen=True)
class OtcNameMismatchReview:
    listing_key: str
    ticker: str
    exchange: str
    asset_type: str
    current_name: str
    official_name: str
    isin: str
    country: str
    official_sources: str
    token_overlap: int
    current_token_count: int
    official_token_count: int
    review_class: str
    review_priority: str
    recommended_action: str


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[OtcNameMismatchReview]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(OtcNameMismatchReview.__dataclass_fields__)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def build_official_lookup(masterfile_rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in masterfile_rows:
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        key = (row.get("ticker", ""), row.get("exchange", ""), row.get("asset_type", ""))
        grouped[key].append(row)
    return dict(grouped)


def choose_official_name(current_name: str, ticker: str, refs: list[dict[str, str]]) -> str:
    names = [
        ref.get("name", "")
        for ref in refs
        if official_name_is_informative(ref.get("name", ""), ticker)
    ]
    if not names:
        return ""
    return sorted(names, key=lambda name: (names_match(current_name, name), len(name)), reverse=True)[0]


def classify_name_mismatch(current_name: str, official_name: str, isin: str) -> tuple[str, str, str]:
    current_tokens = set(informative_name_tokens(current_name))
    official_tokens = set(informative_name_tokens(official_name))
    overlap = current_tokens & official_tokens

    if current_tokens and current_tokens == official_tokens:
        return (
            "matcher_false_positive",
            "low",
            "tighten_name_matcher_or_ignore_current_pair",
        )
    if abbreviated_official_name_matches(current_name, official_name) or overlap:
        return (
            "weak_abbreviation_or_truncation_review",
            "medium",
            "review_before_metadata_update",
        )
    if is_valid_isin(isin):
        return (
            "probable_otc_rename_or_symbol_reuse",
            "high",
            "verify_current_issuer_with_isin_source_before_name_update",
        )
    return (
        "stale_or_symbol_reuse_without_isin",
        "critical",
        "verify_or_quarantine_before_trusting_listing",
    )


def build_review_rows(
    entry_quality_rows: Iterable[dict[str, str]],
    masterfile_rows: Iterable[dict[str, str]],
) -> list[OtcNameMismatchReview]:
    official_lookup = build_official_lookup(masterfile_rows)
    review_rows: list[OtcNameMismatchReview] = []

    for row in entry_quality_rows:
        if row.get("exchange") != "OTC":
            continue
        if row.get("quality_status") != "warn":
            continue
        if "official_name_mismatch" not in row.get("issue_types", "").split("|"):
            continue

        refs = official_lookup.get((row["ticker"], row["exchange"], row["asset_type"]), [])
        official_name = choose_official_name(row.get("name", ""), row.get("ticker", ""), refs)
        current_tokens = set(informative_name_tokens(row.get("name", "")))
        official_tokens = set(informative_name_tokens(official_name))
        review_class, review_priority, recommended_action = classify_name_mismatch(
            row.get("name", ""),
            official_name,
            row.get("isin", ""),
        )
        review_rows.append(
            OtcNameMismatchReview(
                listing_key=row["listing_key"],
                ticker=row["ticker"],
                exchange=row["exchange"],
                asset_type=row["asset_type"],
                current_name=row["name"],
                official_name=official_name,
                isin=row.get("isin", ""),
                country=row.get("country", ""),
                official_sources="|".join(sorted({ref.get("source_key", "") for ref in refs if ref.get("source_key")})),
                token_overlap=len(current_tokens & official_tokens),
                current_token_count=len(current_tokens),
                official_token_count=len(official_tokens),
                review_class=review_class,
                review_priority=review_priority,
                recommended_action=recommended_action,
            )
        )

    priority_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(
        review_rows,
        key=lambda item: (
            priority_rank.get(item.review_priority, 9),
            item.review_class,
            item.ticker,
        ),
    )


def summarize(rows: list[OtcNameMismatchReview], generated_at: str, csv_out: Path) -> dict[str, object]:
    try:
        csv_display_path = str(csv_out.relative_to(ROOT))
    except ValueError:
        csv_display_path = str(csv_out)

    return {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(rows),
            "csv_out": csv_display_path,
            "source_files": {
                "entry_quality_csv": str(ENTRY_QUALITY_CSV.relative_to(ROOT)),
                "masterfile_reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)),
            },
        },
        "summary": {
            "review_class_counts": dict(Counter(row.review_class for row in rows).most_common()),
            "review_priority_counts": dict(Counter(row.review_priority for row in rows).most_common()),
            "with_isin": sum(1 for row in rows if row.isin),
            "without_isin": sum(1 for row in rows if not row.isin),
            "official_source_counts": dict(
                Counter(
                    source
                    for row in rows
                    for source in row.official_sources.split("|")
                    if source
                ).most_common()
            ),
        },
        "items": [asdict(row) for row in rows[:1000]],
    }


def write_markdown(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]  # type: ignore[index]
    lines = [
        "# OTC Name Mismatch Review",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",  # type: ignore[index]
        "",
        "This report is a deterministic review queue for OTC `official_name_mismatch` warnings.",
        "It does not apply metadata updates automatically.",
        "",
        "## Summary",
        "",
        f"- Rows: {payload['_meta']['rows']:,}",  # type: ignore[index]
        f"- With ISIN: {summary['with_isin']:,}",  # type: ignore[index]
        f"- Without ISIN: {summary['without_isin']:,}",  # type: ignore[index]
        "",
        "## Review Classes",
        "",
        "| Class | Rows |",
        "|---|---:|",
    ]
    for review_class, count in summary["review_class_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {review_class} | {count:,} |")

    lines.extend(["", "## Priority", "", "| Priority | Rows |", "|---|---:|"])
    for priority, count in summary["review_priority_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {priority} | {count:,} |")

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `probable_otc_rename_or_symbol_reuse` needs an ISIN-bearing issuer/source check before applying a name update.",
            "- `stale_or_symbol_reuse_without_isin` is the highest-risk bucket because ticker reuse cannot be disambiguated locally.",
            "- `weak_abbreviation_or_truncation_review` should improve the matcher only when the official OTC abbreviation is clearly the same issuer.",
            "- `matcher_false_positive` means the deterministic matcher should be tightened if the row still appears in `entry_quality.csv`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OTC name mismatch review queue from entry quality warnings.")
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = build_review_rows(
        load_csv(args.entry_quality_csv),
        load_csv(args.masterfile_reference_csv),
    )
    generated_at = utc_now()
    payload = summarize(rows, generated_at, args.csv_out)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "csv_out": str(args.csv_out.relative_to(ROOT)),
                "json_out": str(args.json_out.relative_to(ROOT)),
                "md_out": str(args.md_out.relative_to(ROOT)),
                "review_class_counts": payload["summary"]["review_class_counts"],  # type: ignore[index]
                "review_priority_counts": payload["summary"]["review_priority_counts"],  # type: ignore[index]
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
