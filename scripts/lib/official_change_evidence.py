"""Generate narrow, source-backed evidence for safe canonical field changes.

This module deliberately supports only two automatic evidence paths:

* an exact active official reference row supplies a valid ISIN for the same
  venue, ticker, asset type and name;
* the baseline ISIN is empty;
* the candidate ISIN is that unique official ISIN; and
* any accompanying country changes are the deterministic consequence of the
  ISIN prefix and a country-code/name mapping already present in the trusted
  baseline dataset.

It also recognizes an exact venue migration when the same active official
source observation moves from one venue to one other venue. For US listings it
can reconcile a vanished Nasdaq Trader directory row to a stable SEC venue
assignment. Both paths require an unchanged canonical security; ambiguous
observations or candidate listings remain blocked.

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
    from scripts.lib.normalize import names_match
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from lib.merge_evidence import listing_key, row_fingerprint
    from lib.normalize import names_match

ISIN_FORMAT_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
DIRECT_IDENTIFIER_SCOPES = {
    "exchange_directory",
    "security_identifier_registry",
    "security_identifier_registry_subset",
    "security_lookup_subset",
    "listed_companies_subset",
}
NASDAQ_US_DIRECTORY_SOURCES = {"nasdaq_listed", "nasdaq_other_listed"}
SEC_VENUE_SOURCE = "sec_company_tickers_exchange"


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


def _active_official_reference(row: Mapping[str, str]) -> bool:
    return (
        str(row.get("official", "")).strip().casefold() == "true"
        and str(row.get("listing_status", "")).strip().casefold() == "active"
        and str(row.get("reference_scope", "")).strip() in DIRECT_IDENTIFIER_SCOPES
        and bool(str(row.get("source_key", "") or "").strip())
        and bool(str(row.get("source_url", "") or "").strip())
        and bool(_reference_key(row))
    )


def _reference_identity(row: Mapping[str, str]) -> tuple[str, ...]:
    """Identify one security observation while deliberately excluding venue."""

    return (
        str(row.get("source_key", "") or "").strip(),
        str(row.get("source_url", "") or "").strip(),
        str(row.get("ticker", "") or "").strip().upper(),
        _text(str(row.get("name", "") or "")),
        str(row.get("asset_type", "") or "").strip(),
        str(row.get("reference_scope", "") or "").strip(),
        str(row.get("isin", "") or "").strip().upper(),
    )


def _reference_matches_canonical(
    reference: Mapping[str, str], canonical: Mapping[str, str]
) -> bool:
    reference_isin = str(reference.get("isin", "") or "").strip().upper()
    canonical_isin = str(canonical.get("isin", "") or "").strip().upper()
    return (
        str(reference.get("ticker", "") or "").strip().upper()
        == str(canonical.get("ticker", "") or "").strip().upper()
        and str(reference.get("asset_type", "") or "").strip()
        == str(canonical.get("asset_type", "") or "").strip()
        and names_match(
            str(reference.get("name", "") or ""),
            str(canonical.get("name", "") or ""),
        )
        and (not reference_isin or reference_isin == canonical_isin)
    )


def _reference_claims_canonical_security(
    reference: Mapping[str, str], canonical: Mapping[str, str]
) -> bool:
    if (
        str(reference.get("ticker", "") or "").strip().upper()
        != str(canonical.get("ticker", "") or "").strip().upper()
    ):
        return False
    reference_isin = str(reference.get("isin", "") or "").strip().upper()
    canonical_isin = str(canonical.get("isin", "") or "").strip().upper()
    return bool(reference_isin and reference_isin == canonical_isin) or (
        _reference_matches_canonical(reference, canonical)
    )


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
    previous_reference_rows: list[dict[str, str]] | None = None,
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
        if not _active_official_reference(reference):
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

    previous_references = [
        dict(row) for row in (previous_reference_rows or []) if _active_official_reference(row)
    ]
    current_references = [dict(row) for row in reference_rows if _active_official_reference(row)]
    previous_by_identity: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    current_by_identity: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    previous_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    current_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    current_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for reference in previous_references:
        previous_by_identity[_reference_identity(reference)].append(reference)
        previous_by_key[_reference_key(reference)].append(reference)
    for reference in current_references:
        current_by_identity[_reference_identity(reference)].append(reference)
        current_by_key[_reference_key(reference)].append(reference)
        ticker = str(reference.get("ticker", "") or "").strip().upper()
        current_by_ticker[ticker].append(reference)

    removed_keys = sorted(set(before) - set(after))
    for old_key in removed_keys:
        old_row = before[old_key]
        ticker = str(old_row.get("ticker", "") or "").strip().upper()
        candidate_rows = [
            row
            for key, row in after.items()
            if key != old_key
            and str(row.get("ticker", "") or "").strip().upper() == ticker
            and all(
                str(row.get(field, "") or "").strip()
                == str(old_row.get(field, "") or "").strip()
                for field in (
                    "name", "asset_type", "country", "country_code", "isin",
                    "stock_sector", "etf_category",
                )
            )
        ]
        if len(candidate_rows) != 1:
            continue
        new_row = candidate_rows[0]
        new_key = listing_key(new_row)

        matching_pairs: list[tuple[dict[str, str], dict[str, str]]] = []
        evidence_event_type = "venue_changed"
        for old_reference in previous_by_key.get(old_key, []):
            identity = _reference_identity(old_reference)
            old_references = previous_by_identity[identity]
            new_references = current_by_identity.get(identity, [])
            if len(old_references) != 1 or len(new_references) != 1:
                continue
            new_reference = new_references[0]
            if (
                _reference_key(old_reference) == old_key
                and _reference_key(new_reference) == new_key
                and old_key != new_key
                and _reference_matches_canonical(old_reference, old_row)
                and _reference_matches_canonical(new_reference, new_row)
            ):
                matching_pairs.append((old_reference, new_reference))
        if not matching_pairs and is_valid_isin(str(old_row.get("isin", "") or "")):
            disappeared_old_references = [
                reference
                for reference in previous_by_key.get(old_key, [])
                if not current_by_identity.get(_reference_identity(reference))
                and str(reference.get("source_key", "") or "").strip()
                in NASDAQ_US_DIRECTORY_SOURCES
                and _reference_matches_canonical(reference, old_row)
            ]
            stable_new_references = [
                reference
                for reference in current_by_key.get(new_key, [])
                if len(previous_by_identity.get(_reference_identity(reference), [])) == 1
                and _reference_key(
                    previous_by_identity[_reference_identity(reference)][0]
                ) == new_key
                and str(reference.get("source_key", "") or "").strip() == SEC_VENUE_SOURCE
                and _reference_matches_canonical(reference, new_row)
            ]
            if (
                len(disappeared_old_references) == 1
                and len(stable_new_references) == 1
                and names_match(
                    str(disappeared_old_references[0].get("name", "") or ""),
                    str(stable_new_references[0].get("name", "") or ""),
                )
            ):
                matching_pairs.append((disappeared_old_references[0], stable_new_references[0]))
                evidence_event_type = "venue_reconciled"
        if len(matching_pairs) != 1:
            continue

        current_claimed_venues = {
            _reference_key(reference)
            for reference in current_by_ticker[ticker]
            if _reference_claims_canonical_security(reference, new_row)
        }
        if current_claimed_venues != {new_key}:
            continue

        previous_reference, reference = matching_pairs[0]
        source_key = str(reference.get("source_key", "") or "").strip()
        source_url = str(reference.get("source_url", "") or "").strip()
        evidence.append(
            {
                "listing_key": old_key,
                "ticker": str(old_row.get("ticker", "") or ""),
                "exchange": str(old_row.get("exchange", "") or ""),
                "event_type": evidence_event_type,
                "field_name": "exchange",
                "old_value": str(old_row.get("exchange", "") or ""),
                "new_value": str(new_row.get("exchange", "") or ""),
                "before_row_sha256": row_fingerprint(old_row),
                "effective_at": "",
                "observed_at": observed_at,
                "source_key": source_key,
                "source_url": source_url,
                "source_report": source_report,
                "previous_source_key": str(previous_reference.get("source_key", "") or ""),
                "previous_source_url": str(previous_reference.get("source_url", "") or ""),
                "observation_id": _observation_id(
                    source_key=source_key,
                    source_url=source_url,
                    key=f"{old_key}->{new_key}",
                    field="exchange",
                    old=str(old_row.get("exchange", "") or ""),
                    new=str(new_row.get("exchange", "") or ""),
                ),
                "evidence_status": "official",
            }
        )

    return evidence
