from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CORPORATE_ACTIONS_DIR = DATA_DIR / "corporate_actions"
REPORTS_DIR = DATA_DIR / "reports"
LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_URL = "https://stockanalysis.com/actions/changes/"
DEFAULT_CHANGES_CSV = CORPORATE_ACTIONS_DIR / "symbol_changes.csv"
DEFAULT_CHANGES_JSON = CORPORATE_ACTIONS_DIR / "symbol_changes.json"
DEFAULT_REVIEW_CSV = REPORTS_DIR / "symbol_changes_review.csv"
DEFAULT_REVIEW_JSON = REPORTS_DIR / "symbol_changes_review.json"
DEFAULT_REVIEW_MD = REPORTS_DIR / "symbol_changes_review.md"

CHANGE_FIELDS = [
    "change_id",
    "effective_date",
    "old_symbol",
    "new_symbol",
    "new_company_name",
    "source",
    "source_url",
    "new_symbol_url",
    "source_exchange_hint",
    "source_confidence",
    "review_needed",
    "observed_at",
]
REVIEW_FIELDS = [
    *CHANGE_FIELDS,
    "match_status",
    "symbol_change_workflow_queue",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "effective_age_days",
    "recency_bucket",
    "apply_eligibility",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "recommended_action",
    "exchange_scope_status",
    "old_listing_keys",
    "new_listing_keys",
    "old_scoped_listing_keys",
    "new_scoped_listing_keys",
    "listing_key_review_status",
    "scoped_listing_names",
    "issuer_name_review_status",
    "source_review_context",
    "scope_match_context",
    "workflow_review_context",
    "old_match_count",
    "new_match_count",
    "old_scoped_match_count",
    "new_scoped_match_count",
]

US_LISTED_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
EXCHANGE_HINT_SCOPES = {
    "US_LISTED": US_LISTED_EXCHANGES,
    "OTC": {"OTC"},
}


@dataclass(frozen=True)
class SymbolChange:
    change_id: str
    effective_date: str
    old_symbol: str
    new_symbol: str
    new_company_name: str
    source: str
    source_url: str
    new_symbol_url: str
    source_exchange_hint: str
    source_confidence: str
    review_needed: str
    observed_at: str


@dataclass(frozen=True)
class SymbolChangeReview:
    change: SymbolChange
    match_status: str
    symbol_change_workflow_queue: str
    review_bucket: str
    review_priority: str
    review_strategy: str
    effective_age_days: int
    recency_bucket: str
    apply_eligibility: str
    verification_evidence_required: str
    recommended_next_source: str
    source_gate: str
    recommended_action: str
    exchange_scope_status: str
    old_listing_keys: str
    new_listing_keys: str
    old_scoped_listing_keys: str
    new_scoped_listing_keys: str
    listing_key_review_status: str
    scoped_listing_names: str
    issuer_name_review_status: str
    source_review_context: str
    scope_match_context: str
    workflow_review_context: str
    old_match_count: int
    new_match_count: int
    old_scoped_match_count: int
    new_scoped_match_count: int


class StockAnalysisChangeTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[dict[str, str]]] = []
        self._in_table = False
        self._table_done = False
        self._in_row = False
        self._in_cell = False
        self._current_row: list[dict[str, str]] = []
        self._current_text: list[str] = []
        self._current_href = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._table_done:
            return
        if tag == "table" and not self._in_table:
            self._in_table = True
            return
        if not self._in_table:
            return
        if tag == "tr":
            self._in_row = True
            self._current_row = []
        elif tag == "td" and self._in_row:
            self._in_cell = True
            self._current_text = []
            self._current_href = ""
        elif tag == "a" and self._in_cell:
            attrs_dict = dict(attrs)
            self._current_href = attrs_dict.get("href") or self._current_href

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._in_cell:
            text = " ".join(part.strip() for part in self._current_text if part.strip()).strip()
            self._current_row.append({"text": text, "href": self._current_href})
            self._in_cell = False
            return
        if tag == "tr" and self._in_row:
            if len(self._current_row) >= 4:
                self.rows.append(self._current_row[:4])
            self._in_row = False
            return
        if tag == "table" and self._in_table:
            self._in_table = False
            self._table_done = True


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace(" ", "")


def parse_effective_date(value: str) -> str:
    parsed = datetime.strptime(value.strip(), "%b %d, %Y")
    return parsed.date().isoformat()


def source_exchange_hint(href: str) -> str:
    href = href.lower()
    if "/quote/otc/" in href:
        return "OTC"
    if "/stocks/" in href:
        return "US_LISTED"
    return ""


def change_id_for(*, effective_date: str, old_symbol: str, new_symbol: str, source_url: str) -> str:
    value = "|".join([effective_date, old_symbol, new_symbol, source_url])
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def parse_stockanalysis_changes(html: str, *, source_url: str, observed_at: str) -> list[SymbolChange]:
    parser = StockAnalysisChangeTableParser()
    parser.feed(html)
    changes: list[SymbolChange] = []
    for cells in parser.rows:
        date_text, old_cell, new_cell, company_cell = cells[:4]
        old_symbol = normalize_symbol(old_cell["text"])
        new_symbol = normalize_symbol(new_cell["text"])
        if not new_symbol:
            continue
        effective_date = parse_effective_date(date_text["text"])
        new_symbol_url = new_cell["href"]
        changes.append(
            SymbolChange(
                change_id=change_id_for(
                    effective_date=effective_date,
                    old_symbol=old_symbol,
                    new_symbol=new_symbol,
                    source_url=source_url,
                ),
                effective_date=effective_date,
                old_symbol=old_symbol,
                new_symbol=new_symbol,
                new_company_name=company_cell["text"].strip(),
                source="stockanalysis_symbol_changes",
                source_url=source_url,
                new_symbol_url=new_symbol_url,
                source_exchange_hint=source_exchange_hint(new_symbol_url),
                source_confidence="secondary_review",
                review_needed="true",
                observed_at=observed_at,
            )
        )
    return changes


def fetch_html(url: str, timeout_seconds: float) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": "free-ticker-database/symbol-change-sync"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def merge_changes(existing_rows: list[dict[str, str]], fetched_changes: list[SymbolChange]) -> list[SymbolChange]:
    merged: dict[str, SymbolChange] = {}
    for row in existing_rows:
        if not row.get("change_id"):
            continue
        merged[row["change_id"]] = SymbolChange(**{field: row.get(field, "") for field in CHANGE_FIELDS})
    for change in fetched_changes:
        merged[change.change_id] = change
    return sorted(
        merged.values(),
        key=lambda change: (change.effective_date, change.old_symbol, change.new_symbol),
        reverse=True,
    )


def build_listing_lookup(listings_rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    lookup: dict[str, list[dict[str, str]]] = {}
    for row in listings_rows:
        ticker = normalize_symbol(row.get("ticker", ""))
        if ticker:
            lookup.setdefault(ticker, []).append(row)
    return lookup


def listing_keys(rows: list[dict[str, str]]) -> str:
    return "|".join(sorted(row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}" for row in rows))


def listing_names(rows: list[dict[str, str]]) -> str:
    return "|".join(sorted({row.get("name", "").strip() for row in rows if row.get("name", "").strip()}))


def normalize_name_for_review(value: str) -> str:
    return " ".join("".join(char if char.isalnum() else " " for char in value.upper()).split())


def issuer_name_review_status_for(*, feed_name: str, scoped_matches: list[dict[str, str]]) -> str:
    if not feed_name.strip():
        return "no_feed_company_name"
    names = [row.get("name", "") for row in scoped_matches if row.get("name", "").strip()]
    if not names:
        return "no_scoped_listing_name_available"
    normalized_feed_name = normalize_name_for_review(feed_name)
    normalized_listing_names = {normalize_name_for_review(name) for name in names}
    if normalized_feed_name in normalized_listing_names:
        return "feed_name_exactly_matches_scoped_listing_name"
    feed_tokens = set(normalized_feed_name.split())
    for normalized_name in normalized_listing_names:
        listing_tokens = set(normalized_name.split())
        if feed_tokens and listing_tokens and len(feed_tokens & listing_tokens) >= 2:
            return "feed_name_partially_overlaps_scoped_listing_name"
    return "feed_name_differs_from_scoped_listing_name"


def source_review_context_for(change: SymbolChange) -> str:
    return (
        f"source={change.source or 'none'};"
        f"source_exchange_hint={change.source_exchange_hint or 'none'};"
        f"source_confidence={change.source_confidence or 'none'};"
        f"source_url={change.source_url or 'none'}"
    )


def scope_match_context_for(
    *,
    exchange_scope_status: str,
    old_matches: list[dict[str, str]],
    new_matches: list[dict[str, str]],
    old_scoped_matches: list[dict[str, str]],
    new_scoped_matches: list[dict[str, str]],
) -> str:
    return (
        f"exchange_scope_status={exchange_scope_status or 'none'};"
        f"old_match_count={len(old_matches)};"
        f"new_match_count={len(new_matches)};"
        f"old_scoped_match_count={len(old_scoped_matches)};"
        f"new_scoped_match_count={len(new_scoped_matches)};"
        f"old_scoped_listing_keys={listing_keys(old_scoped_matches) or 'none'};"
        f"new_scoped_listing_keys={listing_keys(new_scoped_matches) or 'none'}"
    )


def workflow_review_context_for(
    *,
    workflow_queue: str,
    review_bucket: str,
    apply_eligibility: str,
    verification_evidence_required: str,
) -> str:
    return (
        f"workflow_queue={workflow_queue or 'none'};"
        f"review_bucket={review_bucket or 'none'};"
        f"apply_eligibility={apply_eligibility or 'none'};"
        f"verification_evidence_required={verification_evidence_required or 'none'}"
    )


def listing_key_review_status_for(
    *,
    old_scoped_matches: list[dict[str, str]],
    new_scoped_matches: list[dict[str, str]],
) -> str:
    if old_scoped_matches and new_scoped_matches:
        return "old_and_new_scoped_listing_keys_present"
    if old_scoped_matches:
        return "old_scoped_listing_key_only"
    if new_scoped_matches:
        return "new_scoped_listing_key_only"
    return "no_scoped_listing_key_match"


def scoped_rows(rows: list[dict[str, str]], source_exchange_hint_value: str) -> list[dict[str, str]]:
    allowed_exchanges = EXCHANGE_HINT_SCOPES.get(source_exchange_hint_value)
    if not allowed_exchanges:
        return rows
    return [row for row in rows if row.get("exchange", "") in allowed_exchanges]


def exchange_scope_status_for(
    *,
    old_matches: list[dict[str, str]],
    new_matches: list[dict[str, str]],
    old_scoped_matches: list[dict[str, str]],
    new_scoped_matches: list[dict[str, str]],
    source_exchange_hint_value: str,
) -> str:
    if source_exchange_hint_value not in EXCHANGE_HINT_SCOPES:
        return "unscoped_source_hint"
    if (old_matches and not old_scoped_matches) or (new_matches and not new_scoped_matches):
        return "global_symbol_collision_outside_source_scope"
    return "matches_within_source_scope"


def review_bucket_for(match_status: str, exchange_scope_status: str) -> str:
    if exchange_scope_status == "unscoped_source_hint":
        return "manual_scope_mapping_required"
    if match_status == "old_symbol_present_new_symbol_missing" and exchange_scope_status == "matches_within_source_scope":
        return "action_required_possible_rename_or_delisting"
    if match_status == "old_and_new_symbols_present" and exchange_scope_status == "matches_within_source_scope":
        return "action_required_duplicate_or_cross_listing"
    if match_status == "new_symbol_present_old_symbol_missing" and exchange_scope_status == "matches_within_source_scope":
        return "already_reflected_in_source_scope"
    if match_status == "new_symbol_present_old_symbol_missing" and exchange_scope_status == "global_symbol_collision_outside_source_scope":
        return "already_reflected_in_scope_with_global_symbol_collision"
    if match_status == "symbol_present_only_outside_source_scope":
        return "hold_out_of_scope_symbol_collision"
    if match_status == "no_matching_listing":
        return "no_dataset_match_for_source_scope"
    if exchange_scope_status == "global_symbol_collision_outside_source_scope":
        return "manual_review_due_to_out_of_scope_collision"
    return "manual_review_required"


def symbol_change_workflow_queue_for(review_bucket: str) -> str:
    if review_bucket == "action_required_possible_rename_or_delisting":
        return "review_verified_rename_or_delisting"
    if review_bucket == "action_required_duplicate_or_cross_listing":
        return "review_duplicate_or_cross_listing"
    if review_bucket in {
        "already_reflected_in_source_scope",
        "already_reflected_in_scope_with_global_symbol_collision",
    }:
        return "audit_already_reflected"
    if review_bucket in {
        "manual_review_due_to_out_of_scope_collision",
        "hold_out_of_scope_symbol_collision",
    }:
        return "blocked_out_of_scope_symbol_collision"
    if review_bucket == "manual_scope_mapping_required":
        return "blocked_missing_source_scope_mapping"
    if review_bucket == "no_dataset_match_for_source_scope":
        return "document_no_dataset_match"
    return "manual_review_required"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "action_required_possible_rename_or_delisting",
        "action_required_duplicate_or_cross_listing",
    }:
        return "P1"
    if review_bucket in {
        "manual_scope_mapping_required",
        "manual_review_due_to_out_of_scope_collision",
        "hold_out_of_scope_symbol_collision",
    }:
        return "P2"
    if review_bucket == "no_dataset_match_for_source_scope":
        return "P3"
    if review_bucket in {
        "already_reflected_in_source_scope",
        "already_reflected_in_scope_with_global_symbol_collision",
    }:
        return "P4"
    return "P2"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket in {
        "action_required_possible_rename_or_delisting",
        "action_required_duplicate_or_cross_listing",
    }:
        return "requires_official_venue_confirmation"
    if review_bucket in {
        "manual_scope_mapping_required",
        "manual_review_due_to_out_of_scope_collision",
        "hold_out_of_scope_symbol_collision",
    }:
        return "blocked_until_exchange_scope_resolved"
    if review_bucket == "no_dataset_match_for_source_scope":
        return "no_dataset_action_without_scope_mapping"
    if review_bucket in {
        "already_reflected_in_source_scope",
        "already_reflected_in_scope_with_global_symbol_collision",
    }:
        return "audit_only_no_apply"
    return "manual_review_required"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "action_required_possible_rename_or_delisting":
        return "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer"
    if review_bucket == "action_required_duplicate_or_cross_listing":
        return "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition"
    if review_bucket == "manual_scope_mapping_required":
        return "source_exchange_mapping_before_any_symbol_change_review"
    if review_bucket in {
        "manual_review_due_to_out_of_scope_collision",
        "hold_out_of_scope_symbol_collision",
    }:
        return "official_exchange_scope_and_non_symbol_identity_evidence_before_apply"
    if review_bucket == "no_dataset_match_for_source_scope":
        return "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event"
    if review_bucket in {
        "already_reflected_in_source_scope",
        "already_reflected_in_scope_with_global_symbol_collision",
    }:
        return "audit_only_confirm_no_canonical_change_needed"
    return "manual_review_required"


def workflow_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "review_verified_rename_or_delisting":
        return (
            "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
        )
    if queue == "review_duplicate_or_cross_listing":
        return (
            "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
            "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
        )
    if queue == "audit_already_reflected":
        return (
            "audit_already_reflected_no_canonical_change",
            "audit_only_confirm_no_canonical_change_needed",
        )
    if queue == "blocked_out_of_scope_symbol_collision":
        return (
            "block_until_source_scope_and_non_symbol_identity_resolved",
            "official_exchange_scope_and_non_symbol_identity_evidence_before_apply",
        )
    if queue == "blocked_missing_source_scope_mapping":
        return (
            "map_source_exchange_scope_before_symbol_review",
            "source_exchange_mapping_before_any_symbol_change_review",
        )
    if queue == "document_no_dataset_match":
        return (
            "document_no_dataset_match_without_canonical_action",
            "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
        )
    return ("manual_symbol_change_review_required", "manual_review_required")


def workflow_recommended_next_source_for(queue: str, exchange_scope_status: str) -> str:
    if queue == "review_verified_rename_or_delisting":
        return "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer."
    if queue == "review_duplicate_or_cross_listing":
        return "Official exchange directory records plus listing-key review for both symbols."
    if queue == "blocked_out_of_scope_symbol_collision":
        return "Official source exchange scope mapping plus non-symbol identity evidence before any symbol action."
    if queue == "blocked_missing_source_scope_mapping":
        return "Documented source-to-exchange scope mapping before symbol-change review."
    if queue == "document_no_dataset_match":
        return "Official exchange scope mapping, or document the event as outside the dataset."
    if queue == "audit_already_reflected":
        if exchange_scope_status == "global_symbol_collision_outside_source_scope":
            return "Audit-only comparison against official scoped exchange evidence; no canonical change."
        return "Audit-only confirmation from scoped listing records; no canonical change."
    return "Manual official symbol-change evidence."


def workflow_source_gate_for(queue: str) -> str:
    if queue == "review_verified_rename_or_delisting":
        return "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
    if queue == "review_duplicate_or_cross_listing":
        return "Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key."
    if queue == "blocked_out_of_scope_symbol_collision":
        return "Block apply; global symbol collision outside source scope is not symbol-change evidence."
    if queue == "blocked_missing_source_scope_mapping":
        return "Block review until the secondary feed event is mapped to an exchange scope."
    if queue == "document_no_dataset_match":
        return "No dataset action without scoped official mapping to an existing or intended listing."
    if queue == "audit_already_reflected":
        return "Audit only; no ticker, listing, or name change is authorized."
    return "Manual review required; secondary-feed evidence alone is insufficient."


def apply_readiness_for(review: SymbolChangeReview) -> str:
    if review.apply_eligibility == "requires_official_venue_confirmation":
        return "blocked_until_listing_keyed_official_symbol_change_evidence"
    if review.apply_eligibility == "blocked_until_exchange_scope_resolved":
        return "blocked_until_source_exchange_scope_and_non_symbol_identity_evidence"
    if review.apply_eligibility == "no_dataset_action_without_scope_mapping":
        return "document_or_ignore_until_scoped_official_dataset_match"
    if review.apply_eligibility == "audit_only_no_apply":
        return "audit_only_no_canonical_change"
    return "manual_review_required_no_apply"


def effective_age_days_for(effective_date: str, observed_at: str) -> int:
    try:
        effective = datetime.fromisoformat(effective_date).date()
        observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00")).date()
    except ValueError:
        return -1
    return (observed - effective).days


def recency_bucket_for(age_days: int) -> str:
    if age_days < 0:
        return "unknown_or_future_effective_date"
    if age_days <= 7:
        return "recent_7d"
    if age_days <= 30:
        return "recent_30d"
    if age_days <= 90:
        return "recent_90d"
    return "older_than_90d"


def review_for_change(change: SymbolChange, listing_lookup: dict[str, list[dict[str, str]]]) -> SymbolChangeReview:
    old_matches = listing_lookup.get(change.old_symbol, []) if change.old_symbol else []
    new_matches = listing_lookup.get(change.new_symbol, [])
    old_scoped_matches = scoped_rows(old_matches, change.source_exchange_hint)
    new_scoped_matches = scoped_rows(new_matches, change.source_exchange_hint)
    exchange_scope_status = exchange_scope_status_for(
        old_matches=old_matches,
        new_matches=new_matches,
        old_scoped_matches=old_scoped_matches,
        new_scoped_matches=new_scoped_matches,
        source_exchange_hint_value=change.source_exchange_hint,
    )
    if not change.old_symbol:
        match_status = "informational_no_old_symbol"
        recommended_action = "review_new_symbol_only"
    elif old_scoped_matches and not new_scoped_matches:
        match_status = "old_symbol_present_new_symbol_missing"
        recommended_action = "review_possible_rename_or_delisting_in_source_scope"
    elif not old_scoped_matches and new_scoped_matches:
        match_status = "new_symbol_present_old_symbol_missing"
        recommended_action = "already_reflected_or_new_symbol_added_in_source_scope"
    elif old_scoped_matches and new_scoped_matches:
        match_status = "old_and_new_symbols_present"
        recommended_action = "review_duplicate_or_cross_listing_state_in_source_scope"
    elif old_matches or new_matches:
        match_status = "symbol_present_only_outside_source_scope"
        recommended_action = "do_not_apply_from_symbol_match_review_exchange_scope_first"
    else:
        match_status = "no_matching_listing"
        recommended_action = "ignore_or_map_exchange_scope_before_applying"
    scoped_name_rows = [*old_scoped_matches, *new_scoped_matches]
    review_bucket = review_bucket_for(match_status, exchange_scope_status)
    workflow_queue = symbol_change_workflow_queue_for(review_bucket)
    apply_eligibility = apply_eligibility_for(review_bucket)
    verification_evidence_required = verification_evidence_for(review_bucket)
    effective_age_days = effective_age_days_for(change.effective_date, change.observed_at)
    return SymbolChangeReview(
        change=change,
        match_status=match_status,
        symbol_change_workflow_queue=workflow_queue,
        review_bucket=review_bucket,
        review_priority=review_priority_for(review_bucket),
        review_strategy=workflow_strategy_for(workflow_queue)[0],
        effective_age_days=effective_age_days,
        recency_bucket=recency_bucket_for(effective_age_days),
        apply_eligibility=apply_eligibility,
        verification_evidence_required=verification_evidence_required,
        recommended_next_source=workflow_recommended_next_source_for(workflow_queue, exchange_scope_status),
        source_gate=workflow_source_gate_for(workflow_queue),
        recommended_action=recommended_action,
        exchange_scope_status=exchange_scope_status,
        old_listing_keys=listing_keys(old_matches),
        new_listing_keys=listing_keys(new_matches),
        old_scoped_listing_keys=listing_keys(old_scoped_matches),
        new_scoped_listing_keys=listing_keys(new_scoped_matches),
        listing_key_review_status=listing_key_review_status_for(
            old_scoped_matches=old_scoped_matches,
            new_scoped_matches=new_scoped_matches,
        ),
        scoped_listing_names=listing_names(scoped_name_rows),
        issuer_name_review_status=issuer_name_review_status_for(
            feed_name=change.new_company_name,
            scoped_matches=scoped_name_rows,
        ),
        source_review_context=source_review_context_for(change),
        scope_match_context=scope_match_context_for(
            exchange_scope_status=exchange_scope_status,
            old_matches=old_matches,
            new_matches=new_matches,
            old_scoped_matches=old_scoped_matches,
            new_scoped_matches=new_scoped_matches,
        ),
        workflow_review_context=workflow_review_context_for(
            workflow_queue=workflow_queue,
            review_bucket=review_bucket,
            apply_eligibility=apply_eligibility,
            verification_evidence_required=verification_evidence_required,
        ),
        old_match_count=len(old_matches),
        new_match_count=len(new_matches),
        old_scoped_match_count=len(old_scoped_matches),
        new_scoped_match_count=len(new_scoped_matches),
    )


def build_reviews(changes: list[SymbolChange], listings_rows: list[dict[str, str]]) -> list[SymbolChangeReview]:
    lookup = build_listing_lookup(listings_rows)
    return [review_for_change(change, lookup) for change in changes]


def review_to_record(review: SymbolChangeReview) -> dict[str, Any]:
    return {
        **asdict(review.change),
        "match_status": review.match_status,
        "symbol_change_workflow_queue": review.symbol_change_workflow_queue,
        "review_bucket": review.review_bucket,
        "review_priority": review.review_priority,
        "review_strategy": review.review_strategy,
        "effective_age_days": review.effective_age_days,
        "recency_bucket": review.recency_bucket,
        "apply_eligibility": review.apply_eligibility,
        "verification_evidence_required": review.verification_evidence_required,
        "recommended_next_source": review.recommended_next_source,
        "source_gate": review.source_gate,
        "recommended_action": review.recommended_action,
        "exchange_scope_status": review.exchange_scope_status,
        "old_listing_keys": review.old_listing_keys,
        "new_listing_keys": review.new_listing_keys,
        "old_scoped_listing_keys": review.old_scoped_listing_keys,
        "new_scoped_listing_keys": review.new_scoped_listing_keys,
        "listing_key_review_status": review.listing_key_review_status,
        "scoped_listing_names": review.scoped_listing_names,
        "issuer_name_review_status": review.issuer_name_review_status,
        "source_review_context": review.source_review_context,
        "scope_match_context": review.scope_match_context,
        "workflow_review_context": review.workflow_review_context,
        "old_match_count": review.old_match_count,
        "new_match_count": review.new_match_count,
        "old_scoped_match_count": review.old_scoped_match_count,
        "new_scoped_match_count": review.new_scoped_match_count,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    lines = [
        "# Symbol Changes Review",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.",
        "",
        "## Summary",
        "",
        "| Metric | Rows |",
        "|---|---:|",
        f"| Fetched rows | {summary['fetched_rows']:,} |",
        f"| Merged history rows | {summary['merged_history_rows']:,} |",
        f"| Review rows | {summary['review_rows']:,} |",
        f"| Direct symbol-change apply allowed rows | {summary['symbol_change_backlog']['direct_symbol_change_apply_allowed_rows']:,} |",
        "",
        "## Symbol-Change Backlog",
        "",
        f"- Status: `{summary['symbol_change_backlog']['status']}`",
        f"- Rows: `{summary['symbol_change_backlog']['rows']:,}`",
        f"- Rename/delisting review rows: `{summary['symbol_change_backlog']['verified_rename_or_delisting_review_rows']:,}`",
        f"- Duplicate/cross-listing review rows: `{summary['symbol_change_backlog']['duplicate_or_cross_listing_review_rows']:,}`",
        f"- Already reflected audit rows: `{summary['symbol_change_backlog']['already_reflected_audit_rows']:,}`",
        f"- Out-of-scope collision blocked rows: `{summary['symbol_change_backlog']['out_of_scope_collision_blocked_rows']:,}`",
        f"- Missing source-scope mapping rows: `{summary['symbol_change_backlog']['missing_source_scope_mapping_rows']:,}`",
        f"- No-dataset-match documentation rows: `{summary['symbol_change_backlog']['no_dataset_match_documentation_rows']:,}`",
        f"- Time-sensitive review rows: `{summary['symbol_change_backlog']['time_sensitive_review_rows']:,}`",
        f"- Secondary feed apply authorized: `{str(summary['symbol_change_backlog']['secondary_feed_apply_authorized']).lower()}`",
        f"- Source gate: {summary['symbol_change_backlog']['source_gate']}",
        "",
        "## Match Status",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["match_status_counts"].items():
        lines.append(f"| {status} | {count:,} |")

    lines.extend(["", "## Workflow Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["symbol_change_workflow_queue_counts"].items():
        lines.append(f"| {queue} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Recency", "", "| Queue / Recency | Rows |", "|---|---:|"])
    for bucket, count in summary["workflow_queue_recency_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Priority", "", "| Queue / Priority | Rows |", "|---|---:|"])
    for bucket, count in summary["workflow_queue_priority_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Exchange Scope", "", "| Queue / Scope Status | Rows |", "|---|---:|"])
    for bucket, count in summary["workflow_queue_scope_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Match Status", "", "| Queue / Match Status | Rows |", "|---|---:|"])
    for bucket, count in summary["workflow_queue_match_status_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Listing-Key Review", "", "| Queue | Listing-Key Status | Rows |", "|---|---|---:|"])
    for queue, status_counts in summary["workflow_queue_listing_key_review_counts"].items():
        for status, count in status_counts.items():
            lines.append(f"| {queue} | {status} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Source Hint", "", "| Queue | Source Hint | Rows |", "|---|---|---:|"])
    for queue, hint_counts in summary["workflow_queue_source_hint_counts"].items():
        for hint, count in hint_counts.items():
            lines.append(f"| {queue} | {hint} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Source Confidence", "", "| Queue | Source Confidence | Rows |", "|---|---|---:|"])
    for queue, confidence_counts in summary["workflow_queue_source_confidence_counts"].items():
        for confidence, count in confidence_counts.items():
            lines.append(f"| {queue} | {confidence} | {count:,} |")

    lines.extend(["", "## Workflow Queue By Review Strategy", "", "| Queue | Strategy | Rows |", "|---|---|---:|"])
    for queue, strategy_counts in summary["workflow_queue_review_strategy_counts"].items():
        for strategy, count in strategy_counts.items():
            lines.append(f"| {queue} | {strategy} | {count:,} |")

    lines.extend(
        [
            "",
            "## Top Workflow Batches",
            "",
            "| Queue | Priority | Recency | Scope status | Strategy | Evidence required | Recommended next source | Source gate | Rows |",
            "|---|---|---|---|---|---|---|---|---:|",
        ]
    )
    for batch in summary["top_symbol_change_workflow_batches"]:
        lines.append(
            f"| {batch['symbol_change_workflow_queue']} | {batch['review_priority']} | "
            f"{batch['recency_bucket']} | {batch['exchange_scope_status']} | {batch['review_strategy']} | "
            f"{batch['evidence_required']} | {batch['recommended_next_source']} | {batch['source_gate']} | "
            f"{batch['rows']:,} |"
        )

    lines.extend(["", "## Review Buckets", "", "| Priority | Bucket | Rows |", "|---|---|---:|"])
    for bucket, count in summary["review_bucket_counts"].items():
        priority = summary["review_bucket_priorities"].get(bucket, "")
        lines.append(f"| {priority} | {bucket} | {count:,} |")

    lines.extend(["", "## Review Priorities", "", "| Priority | Rows |", "|---|---:|"])
    for priority, count in summary["review_priority_counts"].items():
        lines.append(f"| {priority} | {count:,} |")

    lines.extend(["", "## Recency", "", "| Recency bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["recency_bucket_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Time-Sensitive P1 Review", "", "| Workflow queue | Rows |", "|---|---:|"])
    for queue, count in summary["time_sensitive_workflow_queue_counts"].items():
        lines.append(f"| {queue} | {count:,} |")
    lines.extend(["", "| Recency bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["time_sensitive_recency_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")
    lines.extend(
        [
            "",
            "### Top Time-Sensitive Symbol-Change Batches",
            "",
            "| Queue | Recency | Scope status | Match status | Listing-key status | Strategy | Evidence required | Source gate | Rows |",
            "|---|---|---|---|---|---|---|---|---:|",
        ]
    )
    for batch in summary["top_time_sensitive_symbol_change_batches"]:
        lines.append(
            f"| {batch['symbol_change_workflow_queue']} | {batch['recency_bucket']} | "
            f"{batch['exchange_scope_status']} | {batch['match_status']} | "
            f"{batch['listing_key_review_status']} | {batch['review_strategy']} | "
            f"{batch['evidence_required']} | {batch['source_gate']} | {batch['rows']:,} |"
        )

    lines.extend(["", "## Priority By Recency", "", "| Priority / Recency | Rows |", "|---|---:|"])
    for bucket, count in summary["review_priority_recency_counts"].items():
        lines.append(f"| {bucket} | {count:,} |")

    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["apply_eligibility_counts"].items():
        lines.append(f"| {eligibility} | {count:,} |")

    lines.extend(["", "## Apply Readiness", "", "| Readiness | Rows |", "|---|---:|"])
    for readiness, count in summary["symbol_change_apply_readiness_counts"].items():
        lines.append(f"| {readiness} | {count:,} |")

    lines.extend(["", "## Time-Sensitive Apply Readiness", "", "| Readiness | Rows |", "|---|---:|"])
    for readiness, count in summary["time_sensitive_apply_readiness_counts"].items():
        lines.append(f"| {readiness} | {count:,} |")

    lines.extend(["", "## Verification Evidence", "", "| Evidence Gate | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_counts"].items():
        lines.append(f"| {evidence} | {count:,} |")

    lines.extend(["", "## Recommended Actions", "", "| Action | Rows |", "|---|---:|"])
    for action, count in summary["recommended_action_counts"].items():
        lines.append(f"| {action} | {count:,} |")

    lines.extend(["", "## Exchange Scope", "", "| Scope Status | Rows |", "|---|---:|"])
    for status, count in summary["exchange_scope_status_counts"].items():
        lines.append(f"| {status} | {count:,} |")

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `source_confidence=secondary_review`: do not auto-merge as official exchange data.",
            "- `review_needed=true`: apply only after exchange/listing-key validation.",
            "- `review_priority=P1`: start here; these are in-scope rename/delisting or duplicate/cross-listing candidates, still not automatic updates.",
            "- `review_priority=P4`: generally already reflected; keep only as audit evidence unless an official venue source contradicts it.",
            "- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_payload(
    *,
    fetched_changes: list[SymbolChange],
    merged_changes: list[SymbolChange],
    reviews: list[SymbolChangeReview],
    generated_at: str,
    url: str,
    source_files: dict[str, Path],
) -> dict[str, Any]:
    match_status_counts = Counter(review.match_status for review in reviews)
    review_bucket_counts = Counter(review.review_bucket for review in reviews)
    workflow_queue_counts = Counter(review.symbol_change_workflow_queue for review in reviews)
    review_priority_counts = Counter(review.review_priority for review in reviews)
    recency_bucket_counts = Counter(review.recency_bucket for review in reviews)
    review_priority_recency_counts = Counter(f"{review.review_priority}:{review.recency_bucket}" for review in reviews)
    workflow_queue_recency_counts = Counter(
        f"{review.symbol_change_workflow_queue}:{review.recency_bucket}" for review in reviews
    )
    workflow_queue_priority_counts = Counter(
        f"{review.symbol_change_workflow_queue}:{review.review_priority}" for review in reviews
    )
    workflow_queue_scope_counts = Counter(
        f"{review.symbol_change_workflow_queue}:{review.exchange_scope_status}" for review in reviews
    )
    workflow_queue_match_status_counts = Counter(
        f"{review.symbol_change_workflow_queue}:{review.match_status}" for review in reviews
    )
    workflow_queue_source_hint_counts: dict[str, Counter[str]] = {}
    workflow_queue_source_confidence_counts: dict[str, Counter[str]] = {}
    workflow_queue_strategy_counts: dict[str, Counter[str]] = {}
    workflow_queue_issuer_name_review_counts: dict[str, Counter[str]] = {}
    workflow_queue_listing_key_review_counts: dict[str, Counter[str]] = {}
    workflow_batches: Counter[tuple[str, str, str, str]] = Counter()
    time_sensitive_batches: Counter[tuple[str, str, str, str, str]] = Counter()
    for review in reviews:
        queue = review.symbol_change_workflow_queue
        workflow_queue_source_hint_counts.setdefault(queue, Counter())[review.change.source_exchange_hint or "missing"] += 1
        workflow_queue_source_confidence_counts.setdefault(queue, Counter())[review.change.source_confidence or "missing"] += 1
        workflow_queue_issuer_name_review_counts.setdefault(queue, Counter())[review.issuer_name_review_status] += 1
        workflow_queue_listing_key_review_counts.setdefault(queue, Counter())[review.listing_key_review_status] += 1
        strategy, _evidence_required = workflow_strategy_for(queue)
        workflow_queue_strategy_counts.setdefault(queue, Counter())[strategy] += 1
        workflow_batches[
            (
                queue,
                review.review_priority,
                review.recency_bucket,
                review.exchange_scope_status,
            )
        ] += 1
    apply_eligibility_counts = Counter(review.apply_eligibility for review in reviews)
    apply_readiness_counts = Counter(apply_readiness_for(review) for review in reviews)
    verification_evidence_required_counts = Counter(review.verification_evidence_required for review in reviews)
    recommended_action_counts = Counter(review.recommended_action for review in reviews)
    exchange_scope_status_counts = Counter(review.exchange_scope_status for review in reviews)
    time_sensitive_reviews = [
        review
        for review in reviews
        if review.review_priority == "P1" and review.recency_bucket in {"recent_7d", "recent_30d"}
    ]
    for review in time_sensitive_reviews:
        time_sensitive_batches[
            (
                review.symbol_change_workflow_queue,
                review.recency_bucket,
                review.exchange_scope_status,
                review.match_status,
                review.listing_key_review_status,
            )
        ] += 1
    time_sensitive_workflow_queue_counts = Counter(
        review.symbol_change_workflow_queue for review in time_sensitive_reviews
    )
    time_sensitive_recency_counts = Counter(review.recency_bucket for review in time_sensitive_reviews)
    time_sensitive_apply_readiness_counts = Counter(apply_readiness_for(review) for review in time_sensitive_reviews)
    symbol_change_backlog = {
        "status": "listing_keyed_symbol_change_review_queue_open",
        "rows": len(reviews),
        "verified_rename_or_delisting_review_rows": workflow_queue_counts.get(
            "review_verified_rename_or_delisting", 0
        ),
        "duplicate_or_cross_listing_review_rows": workflow_queue_counts.get("review_duplicate_or_cross_listing", 0),
        "already_reflected_audit_rows": workflow_queue_counts.get("audit_already_reflected", 0),
        "out_of_scope_collision_blocked_rows": workflow_queue_counts.get("blocked_out_of_scope_symbol_collision", 0),
        "missing_source_scope_mapping_rows": workflow_queue_counts.get("blocked_missing_source_scope_mapping", 0),
        "no_dataset_match_documentation_rows": workflow_queue_counts.get("document_no_dataset_match", 0),
        "time_sensitive_review_rows": len(time_sensitive_reviews),
        "direct_symbol_change_apply_allowed_rows": 0,
        "secondary_feed_apply_authorized": False,
        "source_gate": (
            "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes require "
            "listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
        ),
    }
    review_bucket_priorities = {
        bucket: review_priority_for(bucket)
        for bucket in review_bucket_counts
    }
    priority_order = {"P1": 1, "P2": 2, "P3": 3, "P4": 4}
    recency_order = {
        "recent_7d": 1,
        "recent_30d": 2,
        "recent_90d": 3,
        "older_than_90d": 4,
        "unknown_or_future_effective_date": 5,
    }
    top_workflow_batches = []
    for (queue, priority, recency, exchange_scope_status), count in sorted(
        workflow_batches.items(),
        key=lambda item: (
            priority_order.get(item[0][1], 99),
            recency_order.get(item[0][2], 99),
            -item[1],
            item[0][0],
            item[0][3],
        ),
    )[:25]:
        strategy, evidence_required = workflow_strategy_for(queue)
        top_workflow_batches.append(
            {
                "symbol_change_workflow_queue": queue,
                "review_priority": priority,
                "recency_bucket": recency,
                "exchange_scope_status": exchange_scope_status,
                "rows": count,
                "review_strategy": strategy,
                "evidence_required": evidence_required,
                "recommended_next_source": workflow_recommended_next_source_for(queue, exchange_scope_status),
                "source_gate": workflow_source_gate_for(queue),
            }
        )
    top_time_sensitive_batches = []
    for (queue, recency, exchange_scope_status, match_status, listing_key_review_status), count in sorted(
        time_sensitive_batches.items(),
        key=lambda item: (
            recency_order.get(item[0][1], 99),
            -item[1],
            item[0][0],
            item[0][2],
            item[0][3],
            item[0][4],
        ),
    )[:25]:
        strategy, evidence_required = workflow_strategy_for(queue)
        top_time_sensitive_batches.append(
            {
                "symbol_change_workflow_queue": queue,
                "recency_bucket": recency,
                "exchange_scope_status": exchange_scope_status,
                "match_status": match_status,
                "listing_key_review_status": listing_key_review_status,
                "rows": count,
                "review_strategy": strategy,
                "evidence_required": evidence_required,
                "recommended_next_source": workflow_recommended_next_source_for(queue, exchange_scope_status),
                "source_gate": workflow_source_gate_for(queue),
            }
        )
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_url": url,
            "source_files": {key: display_path(path) for key, path in source_files.items()},
            "source_policy": "secondary_review_only",
            "policy": "Secondary symbol-change feed for review only. No ticker, name, listing, or corporate-action data is applied without listing-keyed official exchange or issuer evidence.",
        },
        "summary": {
            "fetched_rows": len(fetched_changes),
            "merged_history_rows": len(merged_changes),
            "review_rows": len(reviews),
            "match_status_counts": dict(sorted(match_status_counts.items())),
            "symbol_change_workflow_queue_counts": dict(sorted(workflow_queue_counts.items())),
            "symbol_change_backlog": symbol_change_backlog,
            "review_bucket_counts": dict(sorted(review_bucket_counts.items())),
            "review_priority_counts": dict(sorted(review_priority_counts.items())),
            "review_bucket_priorities": dict(sorted(review_bucket_priorities.items())),
            "recency_bucket_counts": dict(sorted(recency_bucket_counts.items())),
            "review_priority_recency_counts": dict(sorted(review_priority_recency_counts.items())),
            "workflow_queue_recency_counts": dict(sorted(workflow_queue_recency_counts.items())),
            "workflow_queue_priority_counts": dict(sorted(workflow_queue_priority_counts.items())),
            "workflow_queue_scope_counts": dict(sorted(workflow_queue_scope_counts.items())),
            "workflow_queue_match_status_counts": dict(sorted(workflow_queue_match_status_counts.items())),
            "workflow_queue_source_hint_counts": {
                queue: dict(sorted(counter.items()))
                for queue, counter in sorted(workflow_queue_source_hint_counts.items())
            },
            "workflow_queue_source_confidence_counts": {
                queue: dict(sorted(counter.items()))
                for queue, counter in sorted(workflow_queue_source_confidence_counts.items())
            },
            "workflow_queue_issuer_name_review_counts": {
                queue: dict(sorted(counter.items()))
                for queue, counter in sorted(workflow_queue_issuer_name_review_counts.items())
            },
            "workflow_queue_listing_key_review_counts": {
                queue: dict(sorted(counter.items()))
                for queue, counter in sorted(workflow_queue_listing_key_review_counts.items())
            },
            "workflow_queue_review_strategy_counts": {
                queue: dict(sorted(counter.items()))
                for queue, counter in sorted(workflow_queue_strategy_counts.items())
            },
            "top_symbol_change_workflow_batches": top_workflow_batches,
            "apply_eligibility_counts": dict(sorted(apply_eligibility_counts.items())),
            "symbol_change_apply_readiness_counts": dict(sorted(apply_readiness_counts.items())),
            "verification_evidence_required_counts": dict(sorted(verification_evidence_required_counts.items())),
            "time_sensitive_review_rows": len(time_sensitive_reviews),
            "time_sensitive_workflow_queue_counts": dict(sorted(time_sensitive_workflow_queue_counts.items())),
            "time_sensitive_recency_counts": dict(sorted(time_sensitive_recency_counts.items())),
            "time_sensitive_apply_readiness_counts": dict(sorted(time_sensitive_apply_readiness_counts.items())),
            "top_time_sensitive_symbol_change_batches": top_time_sensitive_batches,
            "recommended_action_counts": dict(sorted(recommended_action_counts.items())),
            "exchange_scope_status_counts": dict(sorted(exchange_scope_status_counts.items())),
        },
        "review_items": [review_to_record(review) for review in reviews[:1000]],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and process recent stock ticker symbol changes.")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--html-in", type=Path, default=None, help="Read already downloaded HTML instead of fetching the URL.")
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--changes-csv", type=Path, default=DEFAULT_CHANGES_CSV)
    parser.add_argument("--changes-json", type=Path, default=DEFAULT_CHANGES_JSON)
    parser.add_argument("--review-csv", type=Path, default=DEFAULT_REVIEW_CSV)
    parser.add_argument("--review-json", type=Path, default=DEFAULT_REVIEW_JSON)
    parser.add_argument("--review-md", type=Path, default=DEFAULT_REVIEW_MD)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--observed-at", default=None, help="Override observation timestamp for reproducible review reports.")
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    observed_at = getattr(args, "observed_at", None) or utc_now()
    html = args.html_in.read_text(encoding="utf-8") if args.html_in else fetch_html(args.url, args.timeout_seconds)
    fetched_changes = parse_stockanalysis_changes(html, source_url=args.url, observed_at=observed_at)
    merged_changes = merge_changes(load_csv(args.changes_csv), fetched_changes)
    reviews = build_reviews(merged_changes, load_csv(args.listings_csv))
    payload = build_payload(
        fetched_changes=fetched_changes,
        merged_changes=merged_changes,
        reviews=reviews,
        generated_at=observed_at,
        url=args.url,
        source_files={
            "listings_csv": args.listings_csv,
            "changes_csv": args.changes_csv,
        },
    )
    write_csv(args.changes_csv, CHANGE_FIELDS, [asdict(change) for change in merged_changes])
    write_json(
        args.changes_json,
        {
            "_meta": payload["_meta"],
            "summary": payload["summary"],
            "symbol_changes": [asdict(change) for change in merged_changes],
        },
    )
    write_csv(args.review_csv, REVIEW_FIELDS, [review_to_record(review) for review in reviews])
    write_json(args.review_json, payload)
    write_markdown(args.review_md, payload)
    return payload


def main(argv: list[str] | None = None) -> None:
    payload = run(parse_args(argv))
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
