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
    OTC_REVIEW_DECISIONS_CSV,
    abbreviated_official_name_matches,
    build_otc_review_decision_lookup,
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
    apply_eligibility: str
    verification_evidence_required: str
    review_strategy: str
    recommended_next_source: str
    source_gate: str
    official_source_context: str
    identity_review_context: str
    decision_review_context: str
    review_decision: str
    decision_reason: str
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
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
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


def apply_eligibility_for(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "keep_current_until_stronger_issuer_history_source"
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "blocked_until_official_issuer_identity_source_or_quarantine_decision"
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "blocked_until_isin_anchored_issuer_history_review"
    if review_class == "weak_abbreviation_or_truncation_review":
        return "matcher_tuning_only_no_metadata_apply_until_exact_identity_review"
    if review_class == "matcher_false_positive":
        return "no_metadata_change_authorized_tighten_matcher_only"
    return "manual_review_required"


def verification_evidence_for(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "stronger_official_or_reviewed_issuer_history_source_before_any_name_change"
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action"
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name"
    if review_class == "weak_abbreviation_or_truncation_review":
        return "official_alias_or_abbreviation_evidence_with_exact_listing_identity_match"
    if review_class == "matcher_false_positive":
        return "entry_quality_matcher_regression_evidence_no_dataset_metadata_change"
    return "manual_review_required"


def recommended_next_source_for(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "Stronger official or reviewed issuer-history source matching the OTC listing key."
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "Official OTC profile, registry, exchange notice, or issuer-history source matching the listing key."
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
    if review_class == "weak_abbreviation_or_truncation_review":
        return "Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity."
    if review_class == "matcher_false_positive":
        return "Entry-quality matcher regression evidence; no issuer metadata source is needed for a data change."
    return "Manual reviewed OTC identity source matching the listing key."


def source_gate_for(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "Keep current name until stronger issuer-history evidence resolves the ambiguity."
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "Do not rename or trust reused OTC symbol without listing-keyed official identity or quarantine evidence."
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "Do not change the name until ISIN-anchored evidence proves the same current issuer."
    if review_class == "weak_abbreviation_or_truncation_review":
        return "Tune matcher only after official alias evidence; do not change metadata from abbreviation alone."
    if review_class == "matcher_false_positive":
        return "Matcher-only fix; no ticker, listing, name, sector, or category change is authorized."
    return "Manual review required before any OTC name or metadata change."


def review_strategy_for(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "keep_current_until_stronger_issuer_history_source"
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "resolve_or_quarantine_with_official_otc_profile_or_issuer_history"
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "verify_isin_anchored_issuer_history_before_name_change"
    if review_class == "weak_abbreviation_or_truncation_review":
        return "review_official_alias_or_abbreviation_before_matcher_tuning"
    if review_class == "matcher_false_positive":
        return "tighten_matcher_without_dataset_metadata_change"
    return "manual_otc_name_review"


def official_source_context_for(*, official_sources: str, official_name: str) -> str:
    return (
        f"official_sources={official_sources or 'none'};"
        f"official_name_present={'true' if official_name else 'false'}"
    )


def identity_review_context_for(
    *,
    token_overlap: int,
    current_token_count: int,
    official_token_count: int,
    isin: str,
    country: str,
) -> str:
    return (
        f"token_overlap={token_overlap};"
        f"current_token_count={current_token_count};"
        f"official_token_count={official_token_count};"
        f"isin_presence={'with_isin' if isin else 'without_isin'};"
        f"country={country or 'none'}"
    )


def decision_review_context_for(
    *,
    review_class: str,
    apply_eligibility: str,
    verification_evidence_required: str,
    review_decision: str,
) -> str:
    return (
        f"review_class={review_class or 'none'};"
        f"apply_eligibility={apply_eligibility or 'none'};"
        f"verification_evidence_required={verification_evidence_required or 'none'};"
        f"review_decision={review_decision or 'none'}"
    )


def build_review_rows(
    entry_quality_rows: Iterable[dict[str, str]],
    masterfile_rows: Iterable[dict[str, str]],
    otc_review_decision_rows: Iterable[dict[str, str]] | None = None,
) -> list[OtcNameMismatchReview]:
    official_lookup = build_official_lookup(masterfile_rows)
    otc_review_decision_lookup = build_otc_review_decision_lookup(otc_review_decision_rows or [])
    review_rows: list[OtcNameMismatchReview] = []

    for row in entry_quality_rows:
        if row.get("exchange") != "OTC":
            continue
        if row.get("quality_status") != "warn":
            continue
        if "official_name_mismatch" not in row.get("issue_types", "").split("|"):
            continue

        review_decision = otc_review_decision_lookup.get((row["ticker"], row["exchange"]), {})
        if review_decision.get("decision") == "keep_current_reviewed":
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
        if review_decision.get("decision") == "hold_unresolved":
            review_class = "hold_unresolved"
            review_priority = "held"
            recommended_action = "source_needed_for_resolution"
        official_sources = "|".join(sorted({ref.get("source_key", "") for ref in refs if ref.get("source_key")}))
        token_overlap = len(current_tokens & official_tokens)
        current_token_count = len(current_tokens)
        official_token_count = len(official_tokens)
        apply_eligibility = apply_eligibility_for(review_class)
        verification_evidence_required = verification_evidence_for(review_class)
        decision = review_decision.get("decision", "")
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
                official_sources=official_sources,
                token_overlap=token_overlap,
                current_token_count=current_token_count,
                official_token_count=official_token_count,
                review_class=review_class,
                review_priority=review_priority,
                apply_eligibility=apply_eligibility,
                verification_evidence_required=verification_evidence_required,
                review_strategy=review_strategy_for(review_class),
                recommended_next_source=recommended_next_source_for(review_class),
                source_gate=source_gate_for(review_class),
                official_source_context=official_source_context_for(
                    official_sources=official_sources,
                    official_name=official_name,
                ),
                identity_review_context=identity_review_context_for(
                    token_overlap=token_overlap,
                    current_token_count=current_token_count,
                    official_token_count=official_token_count,
                    isin=row.get("isin", ""),
                    country=row.get("country", ""),
                ),
                decision_review_context=decision_review_context_for(
                    review_class=review_class,
                    apply_eligibility=apply_eligibility,
                    verification_evidence_required=verification_evidence_required,
                    review_decision=decision,
                ),
                review_decision=decision,
                decision_reason=review_decision.get("reason", ""),
                recommended_action=recommended_action,
            )
        )

    priority_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3, "held": 4}
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
            "policy": "Review queue only. OTC names are not changed unless an official or reviewed issuer-history source matches the listing_key and required identity evidence.",
            "source_files": {
                "entry_quality_csv": str(ENTRY_QUALITY_CSV.relative_to(ROOT)),
                "masterfile_reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)),
            },
        },
        "summary": {
            "review_class_counts": dict(Counter(row.review_class for row in rows).most_common()),
            "review_priority_counts": dict(Counter(row.review_priority for row in rows).most_common()),
            "apply_eligibility_counts": dict(Counter(row.apply_eligibility for row in rows).most_common()),
            "verification_evidence_required_counts": dict(
                Counter(row.verification_evidence_required for row in rows).most_common()
            ),
            "review_strategy_counts": dict(Counter(review_strategy_for(row.review_class) for row in rows).most_common()),
            "top_otc_name_mismatch_review_batches": [
                {
                    "review_priority": priority,
                    "review_class": review_class,
                    "isin_presence": isin_presence,
                    "official_sources": official_sources,
                    "rows": count,
                    "review_strategy": review_strategy_for(review_class),
                    "verification_evidence_required": verification_evidence_for(review_class),
                    "recommended_next_source": recommended_next_source_for(review_class),
                    "source_gate": source_gate_for(review_class),
                }
                for (priority, review_class, isin_presence, official_sources), count in sorted(
                    Counter(
                        (
                            row.review_priority,
                            row.review_class,
                            "with_isin" if row.isin else "without_isin",
                            row.official_sources or "missing_official_source",
                        )
                        for row in rows
                    ).items(),
                    key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3]),
                )[:10]
            ],
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
        "Reviewed `keep_current_reviewed` decisions are excluded from the active queue.",
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

    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["apply_eligibility_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {eligibility} | {count:,} |")

    lines.extend(["", "## Verification Evidence", "", "| Evidence Required | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {evidence} | {count:,} |")

    lines.extend(["", "## Review Strategies", "", "| Strategy | Rows |", "|---|---:|"])
    for strategy, count in summary["review_strategy_counts"].items():  # type: ignore[index,union-attr]
        lines.append(f"| {strategy} | {count:,} |")

    lines.extend(
        [
            "",
            "## Top Review Batches",
            "",
            "| Priority | Class | ISIN | Official sources | Rows | Strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_otc_name_mismatch_review_batches"]:  # type: ignore[index,union-attr]
        lines.append(
            f"| {batch['review_priority']} | {batch['review_class']} | {batch['isin_presence']} | "
            f"{batch['official_sources']} | {batch['rows']:,} | {batch['review_strategy']} | "
            f"{batch['verification_evidence_required']} | {batch['recommended_next_source']} | "
            f"{batch['source_gate']} |"
        )

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `keep_current_reviewed` suppresses already-reviewed stale OTC naming noise where the current canonical dataset name is intentionally retained.",
            "- `hold_unresolved` marks source-limited ambiguities that remain intentionally open until a stronger issuer-history source is available.",
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
    parser.add_argument("--otc-review-decisions-csv", type=Path, default=OTC_REVIEW_DECISIONS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = build_review_rows(
        load_csv(args.entry_quality_csv),
        load_csv(args.masterfile_reference_csv),
        load_csv(args.otc_review_decisions_csv) if args.otc_review_decisions_csv.exists() else [],
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
                "apply_eligibility_counts": payload["summary"]["apply_eligibility_counts"],  # type: ignore[index]
                "verification_evidence_required_counts": payload["summary"]["verification_evidence_required_counts"],  # type: ignore[index]
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
