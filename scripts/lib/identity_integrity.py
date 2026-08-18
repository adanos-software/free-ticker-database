"""Conservative, listing-keyed instrument identity adjudication.

The module never assigns a new identifier. It retains an existing ISIN only
when one coherent identity family has exact listing-keyed evidence. All other
assertions are reported or quarantined; destructive clears require explicit
apply mode.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

_TRANSLITERATIONS = str.maketrans(
    {
        "Ø": "O", "ø": "o", "Æ": "AE", "æ": "ae", "Å": "A", "å": "a",
        "ß": "ss", "Đ": "D", "đ": "d", "Ð": "D", "ð": "d",
        "Þ": "TH", "þ": "th", "Ł": "L", "ł": "l", "Œ": "OE", "œ": "oe",
        "Ä": "AE", "ä": "ae", "Ö": "OE", "ö": "oe", "Ü": "UE", "ü": "ue",
    }
)
_SUFFIX_RE = re.compile(
    r"\b(?:INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO|PLC|LLC|LP|LTD|LIMITED|"
    r"AG|SA|SE|NV|BV|OYJ|ASA|AB|SPA|BHD|PCL|PJSC|GMBH|KGAA|GROUP|HOLDINGS?|"
    r"TRUST|FUND|ETF|ETN|ETP|REIT|UCITS|ORDINARY|COMMON|STOCK|SHARES?|ADR|GDR|"
    r"DEPOSITARY|RECEIPTS?|UNITS?)\b",
    re.IGNORECASE,
)
_CURRENCY_RE = re.compile(
    r"(?:\s|^)(?:USD|EUR|GBP|GBX|CHF|HKD|SGD|JPY|CNY|CNH|AUD|CAD|NZD|SEK|NOK|"
    r"DKK|ZAR|INR|KRW|TWD|THB|MYR|IDR|BRL|MXN|ILS|AED|SAR|PLN|HUF|CZK)\s*$",
    re.IGNORECASE,
)
_GENERIC_TOKENS = {
    "a", "b", "c", "d", "h", "n", "and", "of", "de", "da", "do", "el", "la",
    "global", "international", "capital", "financial", "finance", "investment",
    "investments", "industries", "industrial", "energy", "resources", "technology",
    "technologies", "services", "solutions", "systems", "enterprise", "enterprises",
}
_ETF_TOKENS = {
    "amundi", "core", "daily", "etf", "etn", "etp", "franklin", "fund", "hsbc",
    "index", "invesco", "ishare", "ishares", "lyxor", "proshares", "spdr", "strategy",
    "ucits", "vanguard", "wisdomtree", "xtrackers",
}


@dataclass(frozen=True)
class IdentityConflict:
    isin: str
    rows: tuple[dict[str, str], ...]
    asset_types: tuple[str, ...]
    family_count: int
    signals: tuple[str, ...]


@dataclass(frozen=True)
class ResolutionDecision:
    conflict_id: str
    isin: str
    listing_key: str
    ticker: str
    exchange: str
    name: str
    asset_type: str
    action: str
    reason: str
    evidence_status: str
    retained_isin: str

    def as_dict(self) -> dict[str, str]:
        return self.__dict__.copy()


def listing_key(row: Mapping[str, Any]) -> str:
    existing = str(row.get("listing_key", "") or "").strip()
    if existing:
        return existing
    return f"{str(row.get('exchange', '') or '').strip()}::{str(row.get('ticker', '') or '').strip()}"


def _fold(value: str) -> str:
    value = (value or "").translate(_TRANSLITERATIONS)
    value = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in value if not unicodedata.combining(ch))


def _clean_name(value: str) -> str:
    value = _CURRENCY_RE.sub(" ", value or "")
    value = _SUFFIX_RE.sub(" ", _fold(value).upper())
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9]+", " ", value)).strip()


def name_tokens(value: str, asset_type: str = "") -> frozenset[str]:
    stop = _GENERIC_TOKENS | (_ETF_TOKENS if asset_type == "ETF" else set())
    return frozenset(
        token.lower() for token in _clean_name(value).split()
        if len(token) > 1 and token.lower() not in stop
    )


def compact_name(value: str, asset_type: str = "") -> str:
    return "".join(sorted(name_tokens(value, asset_type)))


def is_full_identity_name(value: str) -> bool:
    value = (value or "").strip()
    if not value:
        return False
    if _SUFFIX_RE.search(value):
        return True
    words = re.findall(r"[\wÀ-ÿ]+", value, flags=re.UNICODE)
    return len(words) >= 2 and any(ch.islower() for ch in value)


def names_refer_to_same_identity(left: str, right: str, asset_type: str = "") -> bool:
    left_clean = _clean_name(left)
    right_clean = _clean_name(right)
    if left_clean and left_clean == right_clean:
        return True
    left_tokens = name_tokens(left, asset_type)
    right_tokens = name_tokens(right, asset_type)
    if not left_tokens or not right_tokens:
        return False
    shared = left_tokens & right_tokens
    if not shared or not any(len(token) >= 4 for token in shared):
        return False
    union = left_tokens | right_tokens
    if len(shared) >= 2 or len(shared) / len(union) >= 0.5:
        return True
    left_compact = compact_name(left, asset_type)
    right_compact = compact_name(right, asset_type)
    short, long = sorted((left_compact, right_compact), key=len)
    return bool(short and len(short) >= 7 and short in long and len(short) / len(long) >= 0.72)




def identity_group_is_coherent(rows: Sequence[Mapping[str, Any]]) -> bool:
    """Return whether every row is provably one instrument identity.

    This is a complete-linkage rule: one shared or bridging name is not enough.
    Every pair must agree and every row must carry the same asset type.
    """

    if not rows:
        return False
    asset_types = {str(row.get("asset_type", "") or "").strip() for row in rows}
    if len(asset_types) != 1:
        return False
    asset_type = next(iter(asset_types))
    return all(
        left is right
        or names_refer_to_same_identity(
            str(left.get("name", "") or ""),
            str(right.get("name", "") or ""),
            asset_type,
        )
        for left in rows
        for right in rows
    )


def _complete_linkage_family_assignments(
    rows: Sequence[dict[str, str]],
    families: Sequence[Sequence[dict[str, str]]],
) -> dict[str, int | None]:
    """Assign non-family rows without weakening complete-linkage semantics."""

    assigned: dict[str, int | None] = {listing_key(row): None for row in rows}
    expanded: dict[int, list[dict[str, str]]] = {}
    for index, family in enumerate(families):
        members = list(family)
        expanded[index] = members
        for row in members:
            assigned[listing_key(row)] = index

    for row in sorted(rows, key=listing_key):
        key = listing_key(row)
        if assigned[key] is not None:
            continue
        candidates = [
            index
            for index, family in expanded.items()
            if family
            and family[0].get("asset_type", "") == row.get("asset_type", "")
            and all(
                names_refer_to_same_identity(
                    row.get("name", ""), peer.get("name", ""), row.get("asset_type", "")
                )
                for peer in family
            )
        ]
        if len(candidates) == 1:
            index = candidates[0]
            assigned[key] = index
            expanded[index].append(row)
    return assigned


def _complete_linkage_families(
    rows: Sequence[dict[str, str]], asset_type: str
) -> list[list[dict[str, str]]]:
    families: list[list[dict[str, str]]] = []
    for row in sorted(rows, key=listing_key):
        candidates = [
            family for family in families
            if all(
                names_refer_to_same_identity(row.get("name", ""), peer.get("name", ""), asset_type)
                for peer in family
            )
        ]
        if len(candidates) == 1:
            candidates[0].append(row)
        else:
            families.append([row])
    return families


def identity_families(rows: Sequence[dict[str, str]]) -> list[list[dict[str, str]]]:
    """Build deterministic complete-linkage families including short venue names.

    Earlier versions clustered only long names and then attached every short name
    independently. That allowed two mutually incompatible abbreviations to bridge
    through the same long name. Including every row with discriminative tokens in
    the complete-linkage pass makes pairwise compatibility a true invariant.
    """

    by_asset: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        asset_type = str(row.get("asset_type", "") or "").strip()
        tokens = name_tokens(row.get("name", ""), asset_type)
        if tokens and (is_full_identity_name(row.get("name", "")) or len(tokens) >= 2):
            by_asset[asset_type].append(row)
    families: list[list[dict[str, str]]] = []
    for asset_type in sorted(by_asset):
        families.extend(_complete_linkage_families(by_asset[asset_type], asset_type))
    return families


def _family_for_row(
    row: dict[str, str], families: Sequence[Sequence[dict[str, str]]]
) -> int | None:
    key = listing_key(row)
    matches = [
        index
        for index, family in enumerate(families)
        if any(listing_key(peer) == key for peer in family)
    ]
    return matches[0] if len(matches) == 1 else None


def find_identity_conflicts(rows: Iterable[Mapping[str, Any]]) -> list[IdentityConflict]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for source in rows:
        isin = str(source.get("isin", "") or "").strip().upper()
        if not isin:
            continue
        row = {str(key): str(value or "") for key, value in source.items() if not isinstance(value, (list, dict, set, tuple))}
        row["isin"] = isin
        grouped[isin].append(row)

    conflicts: list[IdentityConflict] = []
    for isin, members in sorted(grouped.items()):
        if len(members) < 2:
            continue
        asset_types = tuple(sorted({row.get("asset_type", "") for row in members}))
        families = identity_families(members)
        assignments = _complete_linkage_family_assignments(members, families)
        unresolved = [row for row in members if assignments[listing_key(row)] is None]
        signals: list[str] = []
        if len(asset_types) > 1:
            signals.append("asset_type_conflict")
        if not identity_group_is_coherent(members):
            signals.append("incomplete_identity_linkage")
        assigned_families = {value for value in assignments.values() if value is not None}
        if len(assigned_families) > 1:
            signals.append("disjoint_identity_families")
        if unresolved:
            distinct = {_clean_name(row.get("name", "")) for row in unresolved if row.get("name", "")}
            if families or len(distinct) > 1:
                signals.append("unresolved_identity_rows")
        if signals:
            conflicts.append(
                IdentityConflict(
                    isin=isin,
                    rows=tuple(sorted(members, key=listing_key)),
                    asset_types=asset_types,
                    family_count=len(families),
                    signals=tuple(signals),
                )
            )
    return conflicts


def _official_isins(
    official: Mapping[str, str | Sequence[str] | set[str]], row: Mapping[str, Any]
) -> set[str]:
    value = official.get(listing_key(row), "")
    if isinstance(value, str):
        return {value.strip().upper()} if value.strip() else set()
    return {str(item).strip().upper() for item in value if str(item).strip()}


def _conflict_id(isin: str, rows: Sequence[Mapping[str, Any]]) -> str:
    payload = f"{isin}|" + "|".join(sorted(listing_key(row) for row in rows))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def resolve_identity_conflicts(
    rows: Sequence[MutableMapping[str, Any]],
    *,
    official_isin_by_listing: Mapping[str, str | Sequence[str] | set[str]] | None = None,
    reviewed_keep_listing_keys: Mapping[str, Sequence[str] | set[str]] | None = None,
    apply_resolved_clears: bool = False,
) -> tuple[list[dict[str, Any]], list[ResolutionDecision]]:
    official_isin_by_listing = official_isin_by_listing or {}
    reviewed_keep_listing_keys = reviewed_keep_listing_keys or {}
    cleaned = [dict(row) for row in rows]
    key_counts = Counter(listing_key(row) for row in cleaned)
    duplicates = sorted(key for key, count in key_counts.items() if key and count > 1)
    if duplicates:
        raise ValueError(f"duplicate listing keys in identity resolver: {duplicates[:5]}")
    row_by_key = {listing_key(row): row for row in cleaned}
    decisions: list[ResolutionDecision] = []

    for conflict in find_identity_conflicts(cleaned):
        members = [row_by_key[listing_key(row)] for row in conflict.rows]
        families = identity_families(members)
        family_by_key = _complete_linkage_family_assignments(members, families)
        family_has_full_identity = {
            index: any(is_full_identity_name(member.get("name", "")) for member in family)
            for index, family in enumerate(families)
        }
        supported: set[int] = set()
        direct_official: set[str] = set()
        for row in members:
            if conflict.isin in _official_isins(official_isin_by_listing, row):
                key = listing_key(row)
                direct_official.add(key)
                family = family_by_key[key]
                if family is not None:
                    supported.add(family)

        reviewed = {str(key) for key in reviewed_keep_listing_keys.get(conflict.isin, ())}
        unknown = reviewed - set(family_by_key)
        if unknown:
            raise ValueError(
                f"reviewed ISIN override {conflict.isin} references unknown listing keys: {sorted(unknown)}"
            )
        wrong_isin = {
            key for key in reviewed
            if str(row_by_key[key].get("isin", "") or "").strip().upper() != conflict.isin
        }
        if wrong_isin:
            raise ValueError(f"reviewed ISIN override {conflict.isin} does not match {sorted(wrong_isin)}")
        reviewed_families = {family_by_key[key] for key in reviewed}
        if None in reviewed_families:
            raise ValueError(f"reviewed ISIN override {conflict.isin} includes unresolved identity rows")
        if len(reviewed_families) > 1:
            raise ValueError(f"reviewed ISIN override {conflict.isin} spans multiple identity families")

        keep_family: int | None = None
        evidence_status = "no_decisive_listing_keyed_evidence"
        if reviewed:
            keep_family = next(iter(reviewed_families))
            evidence_status = "reviewed_listing_keyed_resolution"
        elif len(supported) == 1:
            keep_family = next(iter(supported))
            evidence_status = "official_exact_listing_match"
        elif len(supported) > 1:
            evidence_status = "conflicting_official_exact_matches"

        conflict_id = _conflict_id(conflict.isin, members)
        for row in sorted(members, key=listing_key):
            key = listing_key(row)
            family = family_by_key[key]
            keep = keep_family is not None and family == keep_family
            if keep:
                action = "kept_listing_keyed_identifier"
                reason = "One coherent identity family has decisive listing-keyed evidence."
                retained = conflict.isin
            elif (
                keep_family is not None
                and family is not None
                and family != keep_family
                and family_has_full_identity.get(family, False)
            ):
                if apply_resolved_clears:
                    row["isin"] = ""
                    action = "cleared_conflicting_identifier"
                    retained = ""
                    reason = "The listing belongs to a different full-name identity family than the decisively supported family."
                else:
                    action = "proposed_clear_conflicting_identifier"
                    retained = conflict.isin
                    reason = "A different full-name identity family is detected, but destructive changes require explicit apply mode."
            else:
                action = "quarantined_unresolved_identifier"
                retained = conflict.isin
                if family is None:
                    reason = "The listing identity is unresolved; the existing assertion is quarantined without mutation."
                elif keep_family is not None and family != keep_family and not family_has_full_identity.get(family, False):
                    reason = "The conflicting family lacks a full identity name, so destructive action is not justified."
                else:
                    reason = "No single identity family has decisive listing-keyed evidence; the existing assertion is quarantined without mutation."
            decisions.append(
                ResolutionDecision(
                    conflict_id=conflict_id,
                    isin=conflict.isin,
                    listing_key=key,
                    ticker=str(row.get("ticker", "") or ""),
                    exchange=str(row.get("exchange", "") or ""),
                    name=str(row.get("name", "") or ""),
                    asset_type=str(row.get("asset_type", "") or ""),
                    action=action,
                    reason=reason,
                    evidence_status=evidence_status,
                    retained_isin=retained,
                )
            )

    decisions.sort(key=lambda item: (item.isin, item.listing_key, item.action))
    return cleaned, decisions


def decision_rows(decisions: Iterable[ResolutionDecision]) -> list[dict[str, str]]:
    return [decision.as_dict() for decision in decisions]
