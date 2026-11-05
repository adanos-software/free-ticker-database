"""Evidence primitives shared by history and destructive-change gates."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Mapping

CRITICAL_FIELDS = (
    "name", "asset_type", "country", "country_code", "isin", "stock_sector", "etf_category",
)
REMOVAL_EVENT_TYPES = {
    "delisted", "listing_changed", "symbol_changed", "venue_changed", "venue_reconciled", "merged",
    "out_of_scope",
}
# A reviewed `out_of_scope` transition removes a still-trading row that is not an
# exchange-traded product (closed-end fund, mutual-fund class, operating company)
# from the ETF bucket. It names no successor and needs a reason and an https source.
OUT_OF_SCOPE_EVENT_TYPE = "out_of_scope"
OUT_OF_SCOPE_ASSET_TYPES = frozenset({"ETF"})
OUT_OF_SCOPE_IDENTITY_TYPES = frozenset({"exact_isin", "exact_listing"})
FIELD_EVENT_TYPES = {
    "name": {"renamed", "name_changed"},
    "asset_type": {"reclassified"},
    "country": {"country_changed", "reclassified"},
    "country_code": {"country_changed", "reclassified"},
    "isin": {"identifier_changed", "identifier_removed", "reclassified"},
    "stock_sector": {"taxonomy_changed", "reclassified"},
    "etf_category": {"taxonomy_changed", "reclassified"},
}


def listing_key(row: Mapping[str, str]) -> str:
    return (
        row.get("listing_key")
        or f"{row.get('exchange', '')}::{row.get('ticker', '')}"
    ).strip()


def canonical_row_payload(row: Mapping[str, str]) -> dict[str, str]:
    return {
        "listing_key": listing_key(row),
        **{field: str(row.get(field, "") or "").strip() for field in CRITICAL_FIELDS},
    }


def row_fingerprint(row: Mapping[str, str]) -> str:
    payload = json.dumps(
        canonical_row_payload(row), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def event_timestamp(row: Mapping[str, str]) -> str:
    return str(row.get("effective_at") or row.get("observed_at") or "").strip()


def event_timestamp_is_valid(row: Mapping[str, str]) -> bool:
    value = event_timestamp(row)
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return False
    return parsed.astimezone(timezone.utc) <= datetime.now(timezone.utc) + timedelta(minutes=5)


def event_has_provenance(row: Mapping[str, str]) -> bool:
    status = str(row.get("evidence_status", "") or "").strip().lower()
    source_key = str(row.get("source_key", "") or "").strip()
    source_url = str(row.get("source_url", "") or "").strip()
    source_report = str(row.get("source_report", "") or "").strip()
    observation_id = str(row.get("observation_id", "") or "").strip()
    if status not in {"official", "reviewed", "verified"} or not source_key:
        return False
    secure_url = source_url.startswith("https://")
    if status == "official":
        return bool(observation_id and (secure_url or source_report))
    if status == "reviewed":
        return bool(source_report and observation_id and (not source_url or secure_url))
    return bool(source_report and observation_id)


def out_of_scope_transition_problems(row: Mapping[str, str]) -> list[str]:
    """Return why an `out_of_scope` transition row is malformed (empty when valid)."""
    old_key = str(row.get("old_listing_key", "") or "").strip()
    identity_type = str(row.get("identity_type", "") or "").strip()
    identity_value = str(row.get("identity_value", "") or "").strip()
    problems: list[str] = []
    exchange, separator, ticker = old_key.partition("::")
    if not (separator and exchange and ticker):
        problems.append("old_listing_key")
    if str(row.get("new_listing_key", "") or "").strip():
        problems.append("new_listing_key")
    if identity_type not in OUT_OF_SCOPE_IDENTITY_TYPES or not identity_value:
        problems.append("identity_type")
    elif identity_type == "exact_listing" and identity_value != old_key:
        problems.append("identity_value")
    try:
        confidence = float(str(row.get("confidence", "") or "0"))
    except ValueError:
        confidence = 0.0
    if not 0.95 <= confidence <= 1.0:
        problems.append("confidence")
    if not str(row.get("source_key", "") or "").strip():
        problems.append("source_key")
    if not str(row.get("source_url", "") or "").strip().startswith("https://"):
        problems.append("source_url")
    if not str(row.get("reason", "") or "").strip():
        problems.append("reason")
    return problems
