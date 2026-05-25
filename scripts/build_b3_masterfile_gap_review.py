from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_masterfile_gap_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_masterfile_gap_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_masterfile_gap_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "current_etf_category",
    "b3_gap_category",
    "source_presence",
    "candidate_sources",
    "candidate_source_urls",
    "candidate_names",
    "candidate_asset_types",
    "candidate_isins",
    "candidate_sectors",
    "candidate_category_review_decision",
    "official_subset_review_decision",
    "official_subset_closure_eligibility",
    "active_exchange_directory_match",
    "any_official_b3_source_match",
    "b3_resolution_queue",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "b3_source_gap_evidence_path",
    "source_gap_resolution_gate",
    "recommended_next_source",
    "source_gate",
    "b3_listing_context",
    "official_candidate_context",
    "review_gate_context",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def listing_identity(row: dict[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def classify_b3_gap(row: dict[str, str]) -> str:
    ticker = row.get("ticker", "")
    asset_type = row.get("asset_type", "")
    if ticker.endswith(("31", "32", "33", "34", "35", "39")):
        return "bdr_or_foreign_receipt"
    if asset_type == "ETF" and (ticker.endswith("11") or ticker.endswith("11B") or ticker[-2:].isdigit()):
        return "unit_or_fund_line"
    if ticker.endswith("F"):
        return "fractional_line"
    if ticker.endswith("11"):
        return "unit_or_fund_line"
    if ticker and ticker[-1:].isdigit():
        return "local_share_line"
    return "other"


def active_exchange_directory_row(row: dict[str, str]) -> bool:
    return (
        row.get("exchange") == "B3"
        and row.get("listing_status") == "active"
        and row.get("reference_scope") == "exchange_directory"
        and bool(row.get("ticker"))
    )


def b3_masterfile_row(row: dict[str, str]) -> bool:
    return row.get("exchange") == "B3" and bool(row.get("ticker"))


def source_presence_for(candidate_rows: list[dict[str, str]]) -> str:
    if candidate_rows:
        return "present_only_in_non_exchange_directory_source"
    return "absent_from_all_b3_masterfile_sources"


def residual_decision_for(source_presence: str) -> str:
    if source_presence == "present_only_in_non_exchange_directory_source":
        return "official_b3_non_directory_source_requires_scope_or_parser_review"
    return "accepted_source_gap_not_in_any_current_b3_masterfile_source"


def review_bucket_for(residual_decision: str) -> str:
    if residual_decision == "official_b3_non_directory_source_requires_scope_or_parser_review":
        return "official_b3_non_directory_source_review"
    return "missing_from_all_b3_masterfile_sources_source_gap"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket == "official_b3_non_directory_source_review":
        return "P2"
    return "P3"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "official_b3_non_directory_source_review":
        return "review_scope_or_parser_before_any_data_change"
    return "source_gap_keep_existing_dataset_row_until_official_active_source_evidence"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "official_b3_non_directory_source_review":
        return "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
    return "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key"


def source_gap_evidence_path_for(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "current_b3_exchange_directory_or_cvm_issuer_listing_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "current_b3_product_registry_or_issuer_sponsor_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "current_b3_directory_or_official_registry_evidence"
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "official_b3_bdr_subset_listing_evidence_category_source_gap"
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "official_b3_subset_listing_evidence_category_already_reflected"
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "official_b3_subset_listing_evidence_category_source_gap"
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "official_b3_subset_category_apply_evidence"
    return "manual_b3_source_evidence_review"


def source_gap_resolution_gate_for(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "do_not_delete_or_rename_until_current_b3_cvm_or_issuer_listing_evidence_is_reviewed"
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "do_not_delete_or_rename_until_current_product_registry_or_issuer_sponsor_evidence_is_reviewed"
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "do_not_change_until_current_official_registry_evidence_is_reviewed"
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "close_directory_gap_only_keep_identifier_and_category_unchanged"
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "close_directory_gap_only_after_scope_or_parser_review"
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "keep_identifier_and_category_unchanged_until_category_or_scope_evidence_exists"
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "apply_only_after_listing_keyed_category_review"
    return "manual_review_required_before_any_data_change"


def review_strategy_for(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap"
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "close_bdr_subset_gap_without_data_change_keep_category_source_gap"
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "review_official_subset_scope_before_category_or_parser_change"
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "review_official_subset_category_and_scope_before_apply_gate"
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "keep_local_share_gap_until_current_official_b3_or_issuer_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "manual_b3_source_gap_review_before_any_data_change"
    return "manual_b3_masterfile_gap_review"


def recommended_next_source_for(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "Current active B3 exchange directory or reviewed parser/scope evidence for the listed ETF/fund subset."
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them."
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "Current active B3 exchange directory, B3 ETF/BDR subset source, or issuer product page with exact listing match."
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry."
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "Current B3 directory or issuer/registry evidence with exact ticker, name, and instrument type."
    return "Manual B3 source review with exact listing-key evidence."


def source_gate_for(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "Close the directory gap only after confirming the subset is intentionally outside the active directory or parser-scoped."
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "No B3 category, ISIN, name, symbol, or scope change is authorized; the official BDR subset evidence only closes the active-directory gap."
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "No category or scope change until the official subset row is reviewed against the dataset listing."
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "Apply category only after official subset category, listing key, and current dataset category are reviewed."
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing."
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "No data change without exact current official evidence for ticker, name, and instrument type."
    return "Manual review required before any data change."


def b3_listing_context_for(row: dict[str, str]) -> str:
    return (
        f"listing_key={row.get('listing_key', '') or 'none'};"
        f"ticker={row.get('ticker', '') or 'none'};"
        f"asset_type={row.get('asset_type', '') or 'none'};"
        f"b3_gap_category={row.get('b3_gap_category', '') or 'none'};"
        f"current_etf_category={row.get('current_etf_category', '') or 'none'}"
    )


def official_candidate_context_for(row: dict[str, str]) -> str:
    return (
        f"source_presence={row.get('source_presence', '') or 'none'};"
        f"candidate_sources={row.get('candidate_sources', '') or 'none'};"
        f"candidate_isins_present={'true' if row.get('candidate_isins') else 'false'};"
        f"candidate_sectors_present={'true' if row.get('candidate_sectors') else 'false'};"
        f"active_exchange_directory_match={row.get('active_exchange_directory_match', '') or 'none'};"
        f"any_official_b3_source_match={row.get('any_official_b3_source_match', '') or 'none'}"
    )


def review_gate_context_for(row: dict[str, str]) -> str:
    return (
        f"b3_resolution_queue={row.get('b3_resolution_queue', '') or 'none'};"
        f"residual_decision={row.get('residual_decision', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"official_subset_review_decision={row.get('official_subset_review_decision', '') or 'none'};"
        f"official_subset_closure_eligibility={row.get('official_subset_closure_eligibility', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def open_review_example_for(row: dict[str, str]) -> dict[str, str]:
    return {
        "listing_key": row["listing_key"],
        "ticker": row["ticker"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "b3_gap_category": row["b3_gap_category"],
        "b3_resolution_queue": row["b3_resolution_queue"],
        "review_priority": row["review_priority"],
        "review_strategy": row["review_strategy"],
        "verification_evidence_required": row["verification_evidence_required"],
        "b3_source_gap_evidence_path": row["b3_source_gap_evidence_path"],
        "source_gap_resolution_gate": row["source_gap_resolution_gate"],
        "recommended_next_source": row["recommended_next_source"],
        "source_gate": row["source_gate"],
    }


def joined_unique(candidate_rows: list[dict[str, str]], field: str) -> str:
    return "|".join(sorted({row.get(field, "") for row in candidate_rows if row.get(field, "")}))


def candidate_category_review_decision(current_category: str, candidate_categories: str) -> str:
    if not candidate_categories:
        return "no_official_candidate_category"
    candidates = {value.strip() for value in candidate_categories.split("|") if value.strip()}
    if len(candidates) != 1:
        return "ambiguous_official_candidate_category_requires_review"
    candidate = next(iter(candidates))
    if current_category == candidate:
        return "official_candidate_category_already_reflected"
    if current_category:
        return "official_candidate_category_differs_from_current_requires_review"
    return "official_candidate_category_available_requires_apply_gate"


def b3_resolution_queue_for(
    *,
    source_presence: str,
    b3_gap_category: str,
    candidate_category_decision: str,
) -> str:
    if source_presence == "present_only_in_non_exchange_directory_source":
        if candidate_category_decision == "official_candidate_category_already_reflected":
            return "official_subset_category_already_reflected_scope_review"
        if (
            candidate_category_decision == "no_official_candidate_category"
            and b3_gap_category == "bdr_or_foreign_receipt"
        ):
            return "official_bdr_subset_without_category_source_gap_closed"
        if candidate_category_decision == "no_official_candidate_category":
            return "official_subset_without_category_scope_review"
        return "official_subset_category_requires_review"
    if b3_gap_category == "local_share_line":
        return "absent_from_all_b3_sources_local_share_source_gap"
    if b3_gap_category in {"unit_or_fund_line", "bdr_or_foreign_receipt", "other"}:
        return "absent_from_all_b3_sources_fund_or_receipt_source_gap"
    return "absent_from_all_b3_sources_unclassified_source_gap"


def official_subset_review_decision_for(
    *,
    source_presence: str,
    candidate_category_decision: str,
    b3_gap_category: str,
) -> str:
    if source_presence != "present_only_in_non_exchange_directory_source":
        return "not_official_subset_source_gap"
    if candidate_category_decision == "official_candidate_category_already_reflected":
        return "official_subset_category_already_reflected_no_data_change"
    if (
        candidate_category_decision == "no_official_candidate_category"
        and b3_gap_category == "bdr_or_foreign_receipt"
    ):
        return "official_subset_bdr_without_category_no_data_change"
    if candidate_category_decision == "no_official_candidate_category":
        return "official_subset_without_category_keep_current_source_gap"
    return "official_subset_category_mismatch_requires_apply_gate"


def official_subset_closure_eligibility_for(review_decision: str) -> str:
    if review_decision == "official_subset_category_already_reflected_no_data_change":
        return "closure_ready_official_subset_category_already_reflected"
    if review_decision == "official_subset_bdr_without_category_no_data_change":
        return "closure_ready_official_subset_bdr_without_category_source_gap"
    if review_decision == "official_subset_without_category_keep_current_source_gap":
        return "blocked_until_official_category_or_scope_evidence"
    if review_decision == "official_subset_category_mismatch_requires_apply_gate":
        return "blocked_until_category_apply_gate"
    return "blocked_until_current_official_active_source_evidence"


def build_review_rows(
    *,
    listings: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    active_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    all_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in masterfile_rows:
        if active_exchange_directory_row(row):
            active_by_ticker[row["ticker"]].append(row)
        if b3_masterfile_row(row):
            all_by_ticker[row["ticker"]].append(row)

    rows: list[dict[str, str]] = []
    for listing in listings:
        if listing.get("exchange") != "B3" or not listing.get("ticker"):
            continue
        ticker = listing["ticker"]
        if active_by_ticker.get(ticker):
            continue
        candidate_rows = all_by_ticker.get(ticker, [])
        candidate_sectors = joined_unique(candidate_rows, "sector")
        source_presence = source_presence_for(candidate_rows)
        residual_decision = residual_decision_for(source_presence)
        review_bucket = review_bucket_for(residual_decision)
        current_etf_category = listing.get("etf_category", "")
        category_decision = candidate_category_review_decision(current_etf_category, candidate_sectors)
        gap_category = classify_b3_gap(listing)
        b3_resolution_queue = b3_resolution_queue_for(
            source_presence=source_presence,
            b3_gap_category=gap_category,
            candidate_category_decision=category_decision,
        )
        official_subset_review_decision = official_subset_review_decision_for(
            source_presence=source_presence,
            candidate_category_decision=category_decision,
            b3_gap_category=gap_category,
        )
        row = {
            "listing_key": listing_identity(listing),
            "ticker": ticker,
            "exchange": "B3",
            "asset_type": listing.get("asset_type", ""),
            "name": listing.get("name", ""),
            "current_etf_category": current_etf_category,
            "b3_gap_category": gap_category,
            "source_presence": source_presence,
            "candidate_sources": "|".join(
                sorted({row.get("source_key", "") for row in candidate_rows if row.get("source_key")})
            ),
            "candidate_source_urls": "|".join(
                sorted({row.get("source_url", "") for row in candidate_rows if row.get("source_url")})
            ),
            "candidate_names": joined_unique(candidate_rows, "name"),
            "candidate_asset_types": joined_unique(candidate_rows, "asset_type"),
            "candidate_isins": joined_unique(candidate_rows, "isin"),
            "candidate_sectors": candidate_sectors,
            "candidate_category_review_decision": category_decision,
            "official_subset_review_decision": official_subset_review_decision,
            "official_subset_closure_eligibility": official_subset_closure_eligibility_for(
                official_subset_review_decision
            ),
            "active_exchange_directory_match": "false",
            "any_official_b3_source_match": "true" if candidate_rows else "false",
            "b3_resolution_queue": b3_resolution_queue,
            "residual_decision": residual_decision,
            "review_bucket": review_bucket,
            "review_priority": review_priority_for(review_bucket),
            "review_strategy": review_strategy_for(b3_resolution_queue),
            "apply_eligibility": apply_eligibility_for(review_bucket),
            "verification_evidence_required": verification_evidence_for(review_bucket),
            "b3_source_gap_evidence_path": source_gap_evidence_path_for(b3_resolution_queue),
            "source_gap_resolution_gate": source_gap_resolution_gate_for(b3_resolution_queue),
            "recommended_next_source": recommended_next_source_for(b3_resolution_queue),
            "source_gate": source_gate_for(b3_resolution_queue),
        }
        row["b3_listing_context"] = b3_listing_context_for(row)
        row["official_candidate_context"] = official_candidate_context_for(row)
        row["review_gate_context"] = review_gate_context_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["b3_gap_category"], row["ticker"]))


def build_b3_coverage_snapshot(
    *,
    listings: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    review_rows: list[dict[str, str]],
) -> dict[str, Any]:
    b3_listing_tickers_by_key = {
        listing_identity(row): row["ticker"]
        for row in listings
        if row.get("exchange") == "B3" and row.get("ticker")
    }
    active_directory_tickers = {
        row["ticker"]
        for row in masterfile_rows
        if active_exchange_directory_row(row)
    }
    all_official_b3_tickers = {
        row["ticker"]
        for row in masterfile_rows
        if b3_masterfile_row(row)
    }
    active_matched = sum(
        1
        for ticker in b3_listing_tickers_by_key.values()
        if ticker in active_directory_tickers
    )
    any_official_matched = sum(
        1
        for ticker in b3_listing_tickers_by_key.values()
        if ticker in all_official_b3_tickers
    )
    dataset_rows = len(b3_listing_tickers_by_key)
    active_missing = dataset_rows - active_matched
    any_official_missing = dataset_rows - any_official_matched
    active_sources = sorted(
        {
            row.get("source_key", "")
            for row in masterfile_rows
            if active_exchange_directory_row(row) and row.get("source_key")
        }
    )
    non_directory_sources = sorted(
        {
            row.get("source_key", "")
            for row in masterfile_rows
            if b3_masterfile_row(row)
            and not active_exchange_directory_row(row)
            and row.get("source_key")
        }
    )
    return {
        "dataset_rows": dataset_rows,
        "active_exchange_directory_rows": sum(1 for row in masterfile_rows if active_exchange_directory_row(row)),
        "all_b3_masterfile_rows": sum(1 for row in masterfile_rows if b3_masterfile_row(row)),
        "active_directory_matched_dataset_rows": active_matched,
        "active_directory_missing_dataset_rows": active_missing,
        "active_directory_match_rate": round(active_matched / dataset_rows * 100, 2) if dataset_rows else 0.0,
        "official_any_source_matched_dataset_rows": any_official_matched,
        "official_any_source_missing_dataset_rows": any_official_missing,
        "official_any_source_match_rate": round(any_official_matched / dataset_rows * 100, 2) if dataset_rows else 0.0,
        "active_exchange_directory_source_keys": active_sources,
        "official_non_directory_source_keys": non_directory_sources,
        "active_directory_gap_rows": len(review_rows),
        "official_non_directory_gap_rows": sum(
            row["source_presence"] == "present_only_in_non_exchange_directory_source"
            for row in review_rows
        ),
        "absent_from_all_b3_source_gap_rows": sum(
            row["source_presence"] == "absent_from_all_b3_masterfile_sources"
            for row in review_rows
        ),
        "diagnosis": (
            "Active B3 exchange-directory coverage is measured against b3_instruments_equities; rows found only in "
            "official ETF/BDR subset sources remain parser/scope review cases, and rows absent from all B3 sources "
            "remain source gaps."
        ),
    }


def build_b3_coverage_diagnosis(
    *,
    rows: list[dict[str, str]],
    coverage_snapshot: dict[str, Any],
) -> dict[str, Any]:
    official_subset_rows = [
        row for row in rows if row["source_presence"] == "present_only_in_non_exchange_directory_source"
    ]
    absent_rows = [
        row for row in rows if row["source_presence"] == "absent_from_all_b3_masterfile_sources"
    ]
    closure_ready_rows = sum(
        row["official_subset_closure_eligibility"].startswith("closure_ready_")
        for row in rows
    )
    open_review_rows = len(rows) - closure_ready_rows
    candidate_isin_rows = sum(bool(row["candidate_isins"]) for row in official_subset_rows)
    candidate_sector_rows = sum(bool(row["candidate_sectors"]) for row in official_subset_rows)
    active_match_rate = coverage_snapshot.get("active_directory_match_rate", 0.0)
    if isinstance(active_match_rate, (int, float)) and active_match_rate >= 95:
        status = "active_directory_coverage_high_with_reviewable_residuals"
    elif official_subset_rows:
        status = "active_directory_coverage_has_official_subset_parser_or_scope_gap"
    else:
        status = "active_directory_coverage_has_source_gaps"
    return {
        "status": status,
        "dataset_rows": coverage_snapshot.get("dataset_rows", 0),
        "active_directory_match_rate": active_match_rate,
        "active_directory_missing_dataset_rows": coverage_snapshot.get("active_directory_missing_dataset_rows", 0),
        "open_review_rows": open_review_rows,
        "closed_no_data_change_rows": closure_ready_rows,
        "official_non_directory_gap_rows": len(official_subset_rows),
        "absent_from_all_b3_source_gap_rows": len(absent_rows),
        "official_subset_candidate_isin_rows": candidate_isin_rows,
        "official_subset_candidate_sector_rows": candidate_sector_rows,
        "rows_requiring_parser_or_scope_review": len(official_subset_rows) - closure_ready_rows,
        "rows_requiring_external_active_evidence": len(absent_rows),
        "data_change_authorized": False,
        "root_cause": (
            "Residual B3 coverage gaps split between official B3 subset rows outside the active exchange-directory "
            "parser scope and listings absent from all current B3 masterfile sources."
        ),
        "source_gate": (
            "No B3 ISIN, sector, category, name, symbol, or scope change is authorized until the exact listing-keyed "
            "official source evidence and apply gate are reviewed."
        ),
    }


def summarize(
    rows: list[dict[str, str]],
    generated_at: str,
    coverage_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_batch_counter: Counter[tuple[str, str, str, str, str]] = Counter(
        (
            row["review_priority"],
            row["b3_resolution_queue"],
            row["asset_type"] or "unknown",
            row["b3_gap_category"] or "unknown",
            row["source_presence"],
        )
        for row in rows
    )
    open_rows = [
        row
        for row in rows
        if not row["official_subset_closure_eligibility"].startswith("closure_ready_")
    ]
    top_open_batch_counter: Counter[tuple[str, str, str, str, str]] = Counter(
        (
            row["review_priority"],
            row["b3_resolution_queue"],
            row["asset_type"] or "unknown",
            row["b3_gap_category"] or "unknown",
            row["source_presence"],
        )
        for row in open_rows
    )
    coverage_snapshot = coverage_snapshot or {}
    coverage_diagnosis = build_b3_coverage_diagnosis(
        rows=rows,
        coverage_snapshot=coverage_snapshot,
    )
    official_subset_closure_eligibility_totals = dict(
        sorted(Counter(row["official_subset_closure_eligibility"] for row in rows).items())
    )
    official_subset_closure_ready_rows = sum(
        count
        for eligibility, count in official_subset_closure_eligibility_totals.items()
        if eligibility.startswith("closure_ready_")
    )
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "open_review_rows": len(rows) - official_subset_closure_ready_rows,
        "closed_no_data_change_rows": official_subset_closure_ready_rows,
        "open_review_source_presence_totals": dict(
            sorted(Counter(row["source_presence"] for row in open_rows).items())
        ),
        "open_review_resolution_queue_totals": dict(
            sorted(Counter(row["b3_resolution_queue"] for row in open_rows).items())
        ),
        "open_review_next_source_totals": dict(
            sorted(Counter(row["recommended_next_source"] for row in open_rows).items())
        ),
        "open_review_evidence_path_totals": dict(
            sorted(Counter(row["b3_source_gap_evidence_path"] for row in open_rows).items())
        ),
        "source_gap_resolution_gate_totals": dict(
            sorted(Counter(row["source_gap_resolution_gate"] for row in rows).items())
        ),
        "top_open_b3_masterfile_review_rows": [
            open_review_example_for(row)
            for row in sorted(
                open_rows,
                key=lambda row: (
                    row["review_priority"],
                    row["b3_resolution_queue"],
                    row["b3_gap_category"],
                    row["ticker"],
                ),
            )[:25]
        ],
        "source_presence_totals": dict(sorted(Counter(row["source_presence"] for row in rows).items())),
        "b3_gap_category_totals": dict(sorted(Counter(row["b3_gap_category"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] or "unknown" for row in rows).items())),
        "candidate_source_totals": dict(
            sorted(
                Counter(
                    source
                    for row in rows
                    for source in row["candidate_sources"].split("|")
                    if source
                ).items()
            )
        ),
        "candidate_sector_present_rows": sum(bool(row["candidate_sectors"]) for row in rows),
        "candidate_isin_present_rows": sum(bool(row["candidate_isins"]) for row in rows),
        "candidate_category_review_decision_totals": dict(
            sorted(Counter(row["candidate_category_review_decision"] for row in rows).items())
        ),
        "official_subset_review_decision_totals": dict(
            sorted(Counter(row["official_subset_review_decision"] for row in rows).items())
        ),
        "official_subset_closure_eligibility_totals": official_subset_closure_eligibility_totals,
        "official_subset_closure_ready_rows": official_subset_closure_ready_rows,
        "b3_resolution_queue_totals": dict(sorted(Counter(row["b3_resolution_queue"] for row in rows).items())),
        "b3_resolution_queue_asset_type_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row["asset_type"] or "unknown"
                        for row in rows
                        if row["b3_resolution_queue"] == queue
                    ).items()
                )
            )
            for queue in sorted({row["b3_resolution_queue"] for row in rows})
        },
        "b3_resolution_queue_gap_category_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row["b3_gap_category"] or "unknown"
                        for row in rows
                        if row["b3_resolution_queue"] == queue
                    ).items()
                )
            )
            for queue in sorted({row["b3_resolution_queue"] for row in rows})
        },
        "candidate_category_mismatch_rows": sum(
            row["candidate_category_review_decision"]
            == "official_candidate_category_differs_from_current_requires_review"
            for row in rows
        ),
        "candidate_category_mismatch_examples": [
            {
                "listing_key": row["listing_key"],
                "ticker": row["ticker"],
                "current_etf_category": row["current_etf_category"],
                "candidate_sectors": row["candidate_sectors"],
                "candidate_sources": row["candidate_sources"],
            }
            for row in rows
            if row["candidate_category_review_decision"]
            == "official_candidate_category_differs_from_current_requires_review"
        ][:10],
        "coverage_snapshot": coverage_snapshot,
        "coverage_diagnosis": coverage_diagnosis,
        "residual_decision_totals": dict(sorted(Counter(row["residual_decision"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "review_strategy_totals": dict(sorted(Counter(row["review_strategy"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(Counter(row["apply_eligibility"] for row in rows).items())),
        "verification_evidence_required_totals": dict(
            sorted(Counter(row["verification_evidence_required"] for row in rows).items())
        ),
        "top_b3_masterfile_gap_review_batches": [
            {
                "review_priority": priority,
                "b3_resolution_queue": queue,
                "asset_type": asset_type,
                "b3_gap_category": gap_category,
                "source_presence": source_presence,
                "rows": count,
                "review_strategy": review_strategy_for(queue),
                "verification_evidence_required": verification_evidence_for(
                    "official_b3_non_directory_source_review"
                    if source_presence == "present_only_in_non_exchange_directory_source"
                    else "missing_from_all_b3_masterfile_sources_source_gap"
                ),
                "b3_source_gap_evidence_path": source_gap_evidence_path_for(queue),
                "source_gap_resolution_gate": source_gap_resolution_gate_for(queue),
                "recommended_next_source": recommended_next_source_for(queue),
                "source_gate": source_gate_for(queue),
            }
            for (priority, queue, asset_type, gap_category, source_presence), count in sorted(
                top_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3], item[0][4]),
            )[:10]
        ],
        "top_open_b3_masterfile_review_batches": [
            {
                "review_priority": priority,
                "b3_resolution_queue": queue,
                "asset_type": asset_type,
                "b3_gap_category": gap_category,
                "source_presence": source_presence,
                "rows": count,
                "review_strategy": review_strategy_for(queue),
                "verification_evidence_required": verification_evidence_for(
                    "official_b3_non_directory_source_review"
                    if source_presence == "present_only_in_non_exchange_directory_source"
                    else "missing_from_all_b3_masterfile_sources_source_gap"
                ),
                "b3_source_gap_evidence_path": source_gap_evidence_path_for(queue),
                "source_gap_resolution_gate": source_gap_resolution_gate_for(queue),
                "recommended_next_source": recommended_next_source_for(queue),
                "source_gate": source_gate_for(queue),
            }
            for (priority, queue, asset_type, gap_category, source_presence), count in sorted(
                top_open_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3], item[0][4]),
            )[:10]
        ],
        "policy": {
            "no_guessing": "This review does not authorize inferred identifiers, sectors, categories, names, scope changes, or symbol changes.",
            "listing_keyed_review": "Every row is keyed by listing_key and tied to the B3 dataset row plus official B3 masterfile source presence.",
            "source_gap_handling": "Rows absent from all current B3 masterfile sources remain source gaps until stronger official evidence exists.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# B3 Masterfile Gap Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks B3 listings absent from the active B3 exchange-directory source. It does not fill or delete data.",
        "",
        "## Summary",
        "",
        f"- Active-directory missing B3 listings: `{summary['rows']}`",
        "",
        "## Coverage Snapshot",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    coverage_snapshot = summary.get("coverage_snapshot", {})
    if isinstance(coverage_snapshot, dict):
        for key in (
            "dataset_rows",
            "active_exchange_directory_rows",
            "all_b3_masterfile_rows",
            "active_directory_matched_dataset_rows",
            "active_directory_missing_dataset_rows",
            "active_directory_match_rate",
            "official_any_source_matched_dataset_rows",
            "official_any_source_missing_dataset_rows",
            "official_any_source_match_rate",
            "official_non_directory_gap_rows",
            "absent_from_all_b3_source_gap_rows",
        ):
            lines.append(f"| {key} | {coverage_snapshot.get(key, '')} |")
        lines.extend(
            [
                "",
                f"- Active directory sources: `{', '.join(coverage_snapshot.get('active_exchange_directory_source_keys', []))}`",
                f"- Official non-directory sources: `{', '.join(coverage_snapshot.get('official_non_directory_source_keys', []))}`",
                f"- Diagnosis: {coverage_snapshot.get('diagnosis', '')}",
                "",
            ]
        )
    coverage_diagnosis = summary.get("coverage_diagnosis", {})
    if isinstance(coverage_diagnosis, dict):
        lines.extend(
            [
                "## Coverage Diagnosis",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for key in (
            "status",
            "dataset_rows",
            "active_directory_match_rate",
            "active_directory_missing_dataset_rows",
            "open_review_rows",
            "closed_no_data_change_rows",
            "official_non_directory_gap_rows",
            "absent_from_all_b3_source_gap_rows",
            "official_subset_candidate_isin_rows",
            "official_subset_candidate_sector_rows",
            "rows_requiring_parser_or_scope_review",
            "rows_requiring_external_active_evidence",
            "data_change_authorized",
        ):
            lines.append(f"| {key} | {coverage_diagnosis.get(key, '')} |")
        lines.extend(
            [
                "",
                f"- Root cause: {coverage_diagnosis.get('root_cause', '')}",
                f"- Source gate: {coverage_diagnosis.get('source_gate', '')}",
                "",
            ]
        )
    lines.extend(
        [
            "## Source Presence",
            "",
            "| Source presence | Rows |",
            "|---|---:|",
        ]
    )
    for source_presence, count in summary["source_presence_totals"].items():
        lines.append(f"| {source_presence} | {count} |")
    lines.extend(["", "## B3 Resolution Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["b3_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "## Open B3 Resolution Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["open_review_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "## Open B3 Next Sources", "", "| Recommended next source | Rows |", "|---|---:|"])
    for next_source, count in summary["open_review_next_source_totals"].items():
        lines.append(f"| {next_source} | {count} |")
    lines.extend(["", "## Open B3 Evidence Paths", "", "| Evidence path | Rows |", "|---|---:|"])
    for evidence_path, count in summary["open_review_evidence_path_totals"].items():
        lines.append(f"| {evidence_path} | {count} |")
    lines.extend(["", "## Source Gap Resolution Gates", "", "| Resolution gate | Rows |", "|---|---:|"])
    for gate, count in summary["source_gap_resolution_gate_totals"].items():
        lines.append(f"| {gate} | {count} |")
    lines.extend(["", "## B3 Resolution Queue By Asset Type", "", "| Queue | Asset Type | Rows |", "|---|---|---:|"])
    for queue, asset_type_totals in summary["b3_resolution_queue_asset_type_totals"].items():
        for asset_type, count in asset_type_totals.items():
            lines.append(f"| {queue} | {asset_type} | {count} |")
    lines.extend(["", "## B3 Resolution Queue By Gap Category", "", "| Queue | Gap category | Rows |", "|---|---|---:|"])
    for queue, gap_category_totals in summary["b3_resolution_queue_gap_category_totals"].items():
        for gap_category, count in gap_category_totals.items():
            lines.append(f"| {queue} | {gap_category} | {count} |")
    lines.extend(["", "## Review Buckets", "", "| Bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["review_bucket_totals"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend(["", "## Review Strategies", "", "| Strategy | Rows |", "|---|---:|"])
    for strategy, count in summary["review_strategy_totals"].items():
        lines.append(f"| {strategy} | {count} |")
    lines.extend(["", "## Candidate Evidence", "", "| Metric | Rows |", "|---|---:|"])
    lines.append(f"| Candidate sector present | {summary['candidate_sector_present_rows']} |")
    lines.append(f"| Candidate ISIN present | {summary['candidate_isin_present_rows']} |")
    lines.append(f"| Candidate category mismatch review rows | {summary['candidate_category_mismatch_rows']} |")
    lines.extend(["", "## Candidate Category Review Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["candidate_category_review_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Official Subset Review Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["official_subset_review_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Official Subset Closure Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["official_subset_closure_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Candidate Sources", "", "| Source | Rows |", "|---|---:|"])
    for source, count in summary["candidate_source_totals"].items():
        lines.append(f"| {source} | {count} |")
    lines.extend(
        [
            "",
            "## Top Review Batches",
            "",
            "| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_b3_masterfile_gap_review_batches"]:
        lines.append(
            f"| {batch['review_priority']} | {batch['b3_resolution_queue']} | {batch['asset_type']} | "
            f"{batch['b3_gap_category']} | {batch['source_presence']} | {batch['rows']} | "
            f"{batch['review_strategy']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Top Open Review Batches",
            "",
            "| Priority | Queue | Asset type | Gap category | Source presence | Rows | Strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_open_b3_masterfile_review_batches"]:
        lines.append(
            f"| {batch['review_priority']} | {batch['b3_resolution_queue']} | {batch['asset_type']} | "
            f"{batch['b3_gap_category']} | {batch['source_presence']} | {batch['rows']} | "
            f"{batch['review_strategy']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Top Open Review Rows",
            "",
            "| Priority | Listing key | Ticker | Asset type | Gap category | Queue | Name | Evidence path | Resolution gate | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in summary["top_open_b3_masterfile_review_rows"]:
        lines.append(
            f"| {row['review_priority']} | {row['listing_key']} | {row['ticker']} | {row['asset_type']} | "
            f"{row['b3_gap_category']} | {row['b3_resolution_queue']} | {row['name']} | "
            f"{row['b3_source_gap_evidence_path']} | {row['source_gap_resolution_gate']} | "
            f"{row['verification_evidence_required']} | {row['recommended_next_source']} | {row['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Listing key | Priority | Category | Current ETF category | Candidate sectors | Candidate category decision | Candidate sources | Name |",
            "|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['review_priority']} | {row['b3_gap_category']} | "
            f"{row['current_etf_category']} | {row['candidate_sectors']} | "
            f"{row['candidate_category_review_decision']} | {row['candidate_sources']} | {row['name']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No B3 data change is authorized by this report alone.",
            "- Rows present only in non-directory B3 sources require scope or parser review before reclassifying the gap.",
            "- Rows absent from all current B3 masterfile sources remain source gaps until official listing evidence exists.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed review report for B3 masterfile coverage gaps.")
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    listings = load_csv(args.listings_csv)
    masterfile_rows = load_csv(args.masterfile_reference_csv)
    rows = build_review_rows(
        listings=listings,
        masterfile_rows=masterfile_rows,
    )
    summary = summarize(
        rows,
        utc_now_iso(),
        build_b3_coverage_snapshot(
            listings=listings,
            masterfile_rows=masterfile_rows,
            review_rows=rows,
        ),
    )
    write_csv(args.csv_out, rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": rows}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.md_out, rows, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "source_presence_totals": summary["source_presence_totals"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
