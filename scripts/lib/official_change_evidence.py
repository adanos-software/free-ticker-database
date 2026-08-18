"""Generate narrow, source-backed evidence for safe canonical field changes.

This module deliberately supports only one automatic evidence path:

* an exact active official reference row supplies a valid ISIN for the same
  venue, ticker, asset type and name;
* the baseline ISIN is empty;
* the candidate ISIN is that unique official ISIN; and
* any accompanying country changes are the deterministic consequence of the
  ISIN prefix and a country-code/name mapping already present in the trusted
  baseline dataset.

Conflicting references, ticker-only matches, non-official rows, stale rows,
name mismatches and replacement of a non-empty ISIN are all rejected.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Iterable, Mapping

try:
    from scripts.lib.merge_evidence import listing_key, row_fingerprint
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from lib.merge_evidence import listing_key, row_fingerprint

ISIN_FORMAT_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
DIRECT_IDENTIFIER_SCOPES = {
    "exchange_directory",
    "security_identifier_registry",
    "security_identifier_registry_subset",
    "security_lookup_subset",
    "listed_companies_subset",
}


def is_valid_isin(value: str) -> bool:
    """Validate ISIN format and its Luhn check digit without heavy dependencies."""

    isin = str(value or "").strip().upper()
    if not ISIN_FORMAT_RE.fullmatch(isin):
        return False
    digits = "".join(char if char.isdigit() else str(ord(char) - 55) for char in isin[:-1])
    total = 0
    for index, digit in enumerate(reversed(digits)):
        number = int(digit)
        if index % 2 == 0:
            number *= 2
            if number > 9:
                number -= 9
        total += number
    return (10 - total % 10) % 10 == int(isin[-1])


def _text(value: str) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _country_name_by_code(rows: Iterable[Mapping[str, str]]) -> dict[str, str]:
    names: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        code = str(row.get("country_code", "") or "").strip().upper()
        country = str(row.get("country", "") or "").strip()
        if code and country:
            names[code].add(country)
    return {
        code: next(iter(values))
        for code, values in names.items()
        if len(values) == 1
    }


def _reference_key(row: Mapping[str, str]) -> str:
    exchange = str(row.get("exchange", "") or "").strip()
    ticker = str(row.get("ticker", "") or "").strip().upper()
    return f"{exchange}::{ticker}" if exchange and ticker else ""


def _observation_id(
    *, source_key: str, source_url: str, key: str, field: str, old: str, new: str
) -> str:
    payload = "|".join((source_key, source_url, key, field, old, new))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def build_official_change_evidence(
    before_rows: list[dict[str, str]],
    after_rows: list[dict[str, str]],
    reference_rows: list[dict[str, str]],
    *,
    observed_at: str,
    source_report: str,
) -> list[dict[str, str]]:
    """Return exact field-change events justified by active official references.

    The result is suitable for the safe-merge gate. It never authorizes a
    replacement of an existing ISIN and never treats a cross-venue identifier as
    evidence for the requested listing.
    """

    if not observed_at:
        return []

    before = {listing_key(row): row for row in before_rows if listing_key(row)}
    after = {listing_key(row): row for row in after_rows if listing_key(row)}
    baseline_country_by_code = _country_name_by_code(before_rows)

    references_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    for reference in reference_rows:
        if str(reference.get("official", "")).strip().casefold() != "true":
            continue
        if str(reference.get("listing_status", "")).strip().casefold() != "active":
            continue
        if str(reference.get("reference_scope", "")).strip() not in DIRECT_IDENTIFIER_SCOPES:
            continue
        source_key = str(reference.get("source_key", "") or "").strip()
        source_url = str(reference.get("source_url", "") or "").strip()
        isin = str(reference.get("isin", "") or "").strip().upper()
        key = _reference_key(reference)
        if not key or not source_key or not source_url or not is_valid_isin(isin):
            continue
        references_by_key[key].append(dict(reference))

    evidence: list[dict[str, str]] = []
    for key in sorted(set(before) & set(after)):
        old_row = before[key]
        new_row = after[key]
        old_isin = str(old_row.get("isin", "") or "").strip().upper()
        new_isin = str(new_row.get("isin", "") or "").strip().upper()

        # Automatic evidence is intentionally limited to filling a blank ISIN.
        if old_isin or not is_valid_isin(new_isin):
            continue

        matching = []
        for reference in references_by_key.get(key, []):
            if str(reference.get("isin", "") or "").strip().upper() != new_isin:
                continue
            if str(reference.get("asset_type", "") or "").strip() != str(
                new_row.get("asset_type", "") or ""
            ).strip():
                continue
            if _text(reference.get("name", "")) != _text(new_row.get("name", "")):
                continue
            matching.append(reference)

        # A unique identifier family is required. Multiple sources may agree,
        # but any distinct official ISIN for the exact listing blocks automation.
        all_official_isins = {
            str(reference.get("isin", "") or "").strip().upper()
            for reference in references_by_key.get(key, [])
            if str(reference.get("asset_type", "") or "").strip()
            == str(new_row.get("asset_type", "") or "").strip()
        }
        if not matching or all_official_isins != {new_isin}:
            continue

        reference = sorted(
            matching,
            key=lambda row: (
                str(row.get("source_key", "")),
                str(row.get("source_url", "")),
            ),
        )[0]
        source_key = str(reference.get("source_key", "") or "").strip()
        source_url = str(reference.get("source_url", "") or "").strip()
        before_hash = row_fingerprint(old_row)

        def append_event(
            field: str,
            old: str,
            new: str,
            *,
            event_type: str,
            evidence_status: str,
        ) -> None:
            evidence.append(
                {
                    "listing_key": key,
                    "ticker": str(new_row.get("ticker", "") or ""),
                    "exchange": str(new_row.get("exchange", "") or ""),
                    "event_type": event_type,
                    "field_name": field,
                    "old_value": old,
                    "new_value": new,
                    "before_row_sha256": before_hash,
                    "effective_at": "",
                    "observed_at": observed_at,
                    "source_key": source_key,
                    "source_url": source_url,
                    "source_report": source_report,
                    "observation_id": _observation_id(
                        source_key=source_key,
                        source_url=source_url,
                        key=key,
                        field=field,
                        old=old,
                        new=new,
                    ),
                    "evidence_status": evidence_status,
                }
            )

        append_event(
            "isin",
            old_isin,
            new_isin,
            event_type="identifier_changed",
            evidence_status="official",
        )

        prefix = new_isin[:2]
        inferred_country = baseline_country_by_code.get(prefix, "")
        candidate_code = str(new_row.get("country_code", "") or "").strip().upper()
        candidate_country = str(new_row.get("country", "") or "").strip()
        country_pair_is_consistent = bool(
            inferred_country
            and candidate_code == prefix
            and candidate_country == inferred_country
        )
        if country_pair_is_consistent:
            old_country = str(old_row.get("country", "") or "").strip()
            old_code = str(old_row.get("country_code", "") or "").strip().upper()
            if old_country != candidate_country:
                append_event(
                    "country",
                    old_country,
                    candidate_country,
                    event_type="country_changed",
                    evidence_status="verified",
                )
            if old_code != candidate_code:
                append_event(
                    "country_code",
                    old_code,
                    candidate_code,
                    event_type="country_changed",
                    evidence_status="verified",
                )

    return evidence
