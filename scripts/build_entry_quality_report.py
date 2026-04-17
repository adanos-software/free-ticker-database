from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import (
    COUNTRY_TO_ISO,
    EXCHANGE_TICKER_RE,
    IDENTIFIERS_EXTENDED_CSV,
    LISTINGS_CSV,
    MASTERFILE_REFERENCE_CSV,
    TICKERS_CSV,
    VALID_ETF_SECTORS,
    VALID_STOCK_SECTORS,
    alias_matches_company,
    country_from_isin,
    has_wrapper_term,
    is_blocked_alias,
    is_valid_isin,
    looks_like_identifier,
    normalize_tokens,
    split_aliases,
)

REPORTS_DIR = ROOT / "data" / "reports"
INSTRUMENT_SCOPES_CSV = ROOT / "data" / "instrument_scopes.csv"
COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "entry_quality.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "entry_quality.json"
DEFAULT_MD_OUT = REPORTS_DIR / "entry_quality.md"

VALID_ASSET_TYPES = {"Stock", "ETF"}
ISSUE_PENALTIES = {
    "critical": 100,
    "high": 60,
    "medium": 35,
    "low": 15,
    "source_gap": 10,
}


@dataclass(frozen=True)
class EntryIssue:
    issue_type: str
    severity: str
    field: str
    value: str
    message: str

    @property
    def penalty(self) -> int:
        return ISSUE_PENALTIES[self.severity]


@dataclass(frozen=True)
class EntryQualityRow:
    listing_key: str
    ticker: str
    exchange: str
    asset_type: str
    name: str
    country: str
    country_code: str
    isin: str
    instrument_scope: str
    scope_reason: str
    primary_listing_key: str
    venue_status: str
    evidence_level: str
    evidence_sources: list[str]
    quality_status: str
    quality_score: int
    issues: list[EntryIssue]
    recommended_action: str


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def listing_key_for(row: dict[str, str]) -> str:
    return f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def build_scope_lookup(scope_rows: Iterable[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in scope_rows}


def build_identifier_lookup(identifier_rows: Iterable[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in identifier_rows if row.get("listing_key")}


def build_venue_lookup(coverage_report: dict[str, Any]) -> dict[str, str]:
    return {
        row["exchange"]: row.get("venue_status", "")
        for row in coverage_report.get("by_exchange", [])
    }


def build_official_reference_lookup(
    masterfile_rows: Iterable[dict[str, str]],
) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in masterfile_rows:
        if row.get("official") != "true":
            continue
        if row.get("listing_status") != "active":
            continue
        key = (row.get("ticker", ""), row.get("exchange", ""), row.get("asset_type", ""))
        lookup[key].append(row)
    return dict(lookup)


def alias_entity_key(row: dict[str, str]) -> str:
    isin = row.get("isin", "")
    if isin and is_valid_isin(isin):
        return f"isin:{isin}"

    name_tokens = "-".join(sorted(normalize_tokens(row.get("name", ""))))
    if name_tokens:
        return f"name:{row.get('asset_type', '')}:{name_tokens}"
    return f"listing:{row.get('listing_key', '')}"


def build_alias_context(
    listings: Iterable[dict[str, str]],
) -> dict[str, set[str]]:
    context: dict[str, set[str]] = defaultdict(set)
    for row in listings:
        for alias in split_aliases(row.get("aliases", "")):
            if alias:
                context[alias.lower()].add(alias_entity_key(row))
    return dict(context)


def add_issue(
    issues: list[EntryIssue],
    issue_type: str,
    severity: str,
    field: str,
    value: str,
    message: str,
) -> None:
    issues.append(
        EntryIssue(
            issue_type=issue_type,
            severity=severity,
            field=field,
            value=value,
            message=message,
        )
    )


def looks_like_symbol_alias(alias: str) -> bool:
    if EXCHANGE_TICKER_RE.match(alias):
        return True
    compact = "".join(character for character in alias.upper() if character.isalnum())
    return bool(compact and compact == alias.upper() and len(compact) >= 3)


def is_company_style_alias(alias: str, company_name: str) -> bool:
    if alias_matches_company(alias, company_name):
        return True
    return bool(normalize_tokens(alias) & normalize_tokens(company_name))


def has_latin_letter(value: str) -> bool:
    return any(("a" <= character <= "z") or ("A" <= character <= "Z") for character in value)


def has_non_latin_alnum(value: str) -> bool:
    return any(character.isalnum() and not character.isascii() for character in value)


def meaningful_latin_tokens(value: str) -> set[str]:
    ignored = {
        "adr",
        "beneficial",
        "class",
        "common",
        "etf",
        "fund",
        "interest",
        "ky",
        "ord",
        "shares",
        "shs",
        "stock",
        "trust",
    }
    return {
        token.lower()
        for token in re.findall(r"[A-Za-z]{3,}", value)
        if token.lower() not in ignored
    }


def names_are_comparable(left: str, right: str) -> bool:
    left_tokens = meaningful_latin_tokens(left)
    right_tokens = meaningful_latin_tokens(right)
    if left_tokens or right_tokens:
        return bool(left_tokens and right_tokens)
    return has_latin_letter(left) == has_latin_letter(right)


def unicode_compact(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return "".join(character for character in normalized if character.isalnum())


def company_compact_key(value: str) -> str:
    compact = unicode_compact(value)
    suffixes = (
        "commonstock",
        "corporation",
        "incorporated",
        "limited",
        "company",
        "corp",
        "inc",
        "ltd",
        "plc",
        "ord",
        "shs",
        "classa",
        "classb",
        "sa",
        "se",
        "ag",
        "ab",
        "nv",
    )
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if compact.endswith(suffix) and len(compact) > len(suffix) + 3:
                compact = compact[: -len(suffix)]
                changed = True
    return compact


OTC_NAME_NOISE_TOKENS = {
    "ab",
    "adr",
    "ag",
    "as",
    "b",
    "co",
    "corp",
    "corporation",
    "cv",
    "de",
    "inc",
    "ltd",
    "limited",
    "new",
    "nv",
    "ord",
    "pa",
    "plc",
    "publ",
    "s",
    "sa",
    "se",
    "shs",
    "spa",
    "uns",
}


def ascii_tokens(value: str) -> list[str]:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return [token.lower() for token in re.findall(r"[A-Za-z0-9]+", normalized)]


def informative_name_tokens(value: str) -> list[str]:
    return [
        token
        for token in ascii_tokens(value)
        if token not in OTC_NAME_NOISE_TOKENS and len(token) > 1
    ]


def consonant_skeleton(token: str) -> str:
    return "".join(character for character in token if character not in "aeiouy")


def abbreviation_token_matches(left: str, right: str) -> bool:
    if left == right:
        return True
    if len(left) >= 4 and len(right) >= 4 and (left.startswith(right[:4]) or right.startswith(left[:4])):
        return True

    left_skeleton = consonant_skeleton(left)
    right_skeleton = consonant_skeleton(right)
    return bool(
        len(left_skeleton) >= 3
        and len(right_skeleton) >= 3
        and (left_skeleton.startswith(right_skeleton) or right_skeleton.startswith(left_skeleton))
    )


def abbreviated_official_name_matches(left: str, right: str) -> bool:
    left_tokens = informative_name_tokens(left)
    right_tokens = informative_name_tokens(right)
    if not left_tokens or not right_tokens:
        return False

    left_matches = sum(any(abbreviation_token_matches(left_token, right_token) for right_token in right_tokens) for left_token in left_tokens)
    right_matches = sum(any(abbreviation_token_matches(right_token, left_token) for left_token in left_tokens) for right_token in right_tokens)
    return left_matches / len(left_tokens) >= 0.6 and right_matches / len(right_tokens) >= 0.6


def names_match(left: str, right: str) -> bool:
    if alias_matches_company(left, right):
        return True
    if abbreviated_official_name_matches(left, right):
        return True
    left_compact = company_compact_key(left)
    right_compact = company_compact_key(right)
    return bool(
        left_compact
        and right_compact
        and (left_compact in right_compact or right_compact in left_compact)
    )


def official_name_is_informative(name: str, ticker: str) -> bool:
    compact = unicode_compact(name)
    ticker_compact = unicode_compact(ticker)
    if compact and ticker_compact and compact == ticker_compact:
        return False

    tokens = meaningful_latin_tokens(name)
    if tokens:
        if has_non_latin_alnum(name) and len(tokens) <= 1:
            return False
        if len(tokens) == 1 and ticker_compact and next(iter(tokens)) in ticker_compact:
            return False
        return True
    return not has_latin_letter(name)


def assess_aliases(
    row: dict[str, str],
    identifier: dict[str, str],
    alias_context: dict[str, set[str]],
    issues: list[EntryIssue],
) -> None:
    isin = row.get("isin", "")
    wkns = {identifier.get("wkn", "")} - {""}
    row_aliases = split_aliases(row.get("aliases", ""))

    for alias in row_aliases:
        if is_blocked_alias(alias):
            add_issue(
                issues,
                "blocked_alias_present",
                "critical",
                "aliases",
                alias,
                "Alias matches a blocked common-word, contaminated, generic fund-wrapper, or obviously ambiguous term.",
            )
        if has_wrapper_term(alias):
            add_issue(
                issues,
                "wrapper_alias_present",
                "high",
                "aliases",
                alias,
                "Alias contains wrapper terminology such as CDR, DRC, or CEDEAR.",
            )
        if len(alias) <= 2 and not looks_like_identifier(alias, wkns, isin):
            add_issue(
                issues,
                "short_name_alias",
                "high",
                "aliases",
                alias,
                "Very short name alias is too ambiguous for high-quality lookup.",
            )
        if alias.isdigit() and not looks_like_identifier(alias, wkns, isin):
            add_issue(
                issues,
                "numeric_name_alias",
                "high",
                "aliases",
                alias,
                "Pure numeric alias should be stored only as a validated identifier.",
            )
        if (
            alias
            and not looks_like_identifier(alias, wkns, isin)
            and not looks_like_symbol_alias(alias)
            and not is_company_style_alias(alias, row.get("name", ""))
        ):
            add_issue(
                issues,
                "low_company_name_overlap",
                "low",
                "aliases",
                alias,
                "Alias has weak lexical overlap with the company or fund name.",
            )

        alias_peer_count = len(alias_context.get(alias.lower(), set()))
        if alias_peer_count > 1 and not looks_like_identifier(alias, wkns, isin):
            add_issue(
                issues,
                "shared_name_alias",
                "low",
                "aliases",
                alias,
                f"Alias is shared by {alias_peer_count} entity keys; verify it is not a false-positive trigger.",
            )


def assess_reference_match(
    row: dict[str, str],
    official_refs: list[dict[str, str]],
    issues: list[EntryIssue],
) -> None:
    if not official_refs:
        return

    row_isin = row.get("isin", "")
    official_isins = {ref.get("isin", "") for ref in official_refs if ref.get("isin")}
    if row_isin and official_isins and row_isin not in official_isins:
        add_issue(
            issues,
            "official_isin_mismatch",
            "high",
            "isin",
            row_isin,
            f"Listing ISIN disagrees with official active reference ISIN(s): {'|'.join(sorted(official_isins))}.",
        )
    if row_isin and row_isin in official_isins:
        return

    official_names = [
        ref.get("name", "")
        for ref in official_refs
        if official_name_is_informative(ref.get("name", ""), row.get("ticker", ""))
    ]
    comparable_names = [name for name in official_names if names_are_comparable(row.get("name", ""), name)]
    if comparable_names and not any(names_match(row.get("name", ""), name) for name in comparable_names):
        add_issue(
            issues,
            "official_name_mismatch",
            "medium",
            "name",
            row.get("name", ""),
            "Listing name has weak overlap with official active reference name(s).",
        )


def build_evidence(
    row: dict[str, str],
    scope: dict[str, str],
    identifier: dict[str, str],
    official_refs: list[dict[str, str]],
) -> tuple[str, list[str]]:
    sources: list[str] = []
    if official_refs:
        for ref in official_refs[:3]:
            sources.append(f"official:{ref.get('provider', '')}:{ref.get('source_key', '')}")
        return "official_reference", sorted(set(sources))

    for field, label in (("figi", "OpenFIGI"), ("cik", "SEC"), ("lei", "GLEIF")):
        if identifier.get(field):
            sources.append(f"{label}:{field}")

    if identifier.get("wkn"):
        sources.append("identifier:wkn")
    if row.get("isin"):
        sources.append("identifier:valid_isin" if is_valid_isin(row["isin"]) else "identifier:invalid_isin")

    if scope:
        sources.append(f"scope:{scope.get('instrument_scope', '')}:{scope.get('scope_reason', '')}")

    if identifier.get("figi") or identifier.get("cik") or identifier.get("lei"):
        return "identifier_enriched", sorted(set(sources))
    if row.get("isin") and is_valid_isin(row["isin"]):
        return "valid_isin", sorted(set(sources))
    if scope:
        return "scoped_source_gap", sorted(set(sources))
    return "dataset_only", sorted(set(sources))


def status_for_issues(issues: list[EntryIssue]) -> str:
    severities = {issue.severity for issue in issues}
    if "critical" in severities:
        return "quarantine"
    if severities & {"high", "medium"}:
        return "warn"
    if "low" in severities:
        return "notice"
    if "source_gap" in severities:
        return "source_gap"
    return "pass"


def score_for_issues(issues: list[EntryIssue]) -> int:
    return max(0, 100 - min(100, sum(issue.penalty for issue in issues)))


def action_for_status(status: str) -> str:
    return {
        "pass": "none",
        "notice": "review_when_tuning_aliases",
        "source_gap": "find_source_or_keep_scoped_gap",
        "warn": "review_or_backfill",
        "quarantine": "fix_before_release_or_quarantine",
    }[status]


def assess_entry(
    row: dict[str, str],
    *,
    duplicate_listing_keys: set[str],
    listing_keys: set[str],
    primary_listing_keys: set[str],
    scope: dict[str, str],
    identifier: dict[str, str],
    official_refs: list[dict[str, str]],
    venue_status: str,
    alias_context: dict[str, set[str]],
) -> EntryQualityRow:
    issues: list[EntryIssue] = []
    listing_key = row.get("listing_key", "")

    for field in ("listing_key", "ticker", "exchange", "name", "asset_type"):
        if not row.get(field, "").strip():
            add_issue(
                issues,
                "missing_required_field",
                "critical",
                field,
                "",
                f"{field} is required for a usable listing row.",
            )

    expected_listing_key = listing_key_for(row)
    if listing_key and expected_listing_key != listing_key:
        add_issue(
            issues,
            "listing_key_mismatch",
            "critical",
            "listing_key",
            listing_key,
            f"listing_key should be {expected_listing_key}.",
        )

    if listing_key in duplicate_listing_keys:
        add_issue(
            issues,
            "duplicate_listing_key",
            "critical",
            "listing_key",
            listing_key,
            "listing_key appears more than once in data/listings.csv.",
        )

    if row.get("asset_type") not in VALID_ASSET_TYPES:
        add_issue(
            issues,
            "invalid_asset_type",
            "critical",
            "asset_type",
            row.get("asset_type", ""),
            "asset_type must be Stock or ETF.",
        )

    if not scope:
        add_issue(
            issues,
            "missing_instrument_scope",
            "high",
            "listing_key",
            listing_key,
            "Listing row has no matching instrument_scopes.csv row.",
        )
    else:
        primary_key = scope.get("primary_listing_key", "")
        if scope.get("instrument_scope") == "core" and not primary_key:
            add_issue(
                issues,
                "missing_primary_listing_key",
                "high",
                "primary_listing_key",
                "",
                "Core scope row should point at its primary listing key.",
            )
        if primary_key and primary_key not in listing_keys:
            add_issue(
                issues,
                "primary_listing_key_missing_from_listings",
                "high",
                "primary_listing_key",
                primary_key,
                "primary_listing_key does not exist in data/listings.csv.",
            )
        if scope.get("scope_reason") == "secondary_cross_listing" and primary_key == listing_key:
            add_issue(
                issues,
                "secondary_listing_points_to_self",
                "high",
                "primary_listing_key",
                primary_key,
                "Secondary cross-listing should point to a different primary listing.",
            )
        if scope.get("scope_reason") in {"primary_listing", "primary_listing_missing_isin"} and listing_key not in primary_listing_keys:
            add_issue(
                issues,
                "primary_scope_missing_from_primary_export",
                "medium",
                "listing_key",
                listing_key,
                "Primary listing scope is absent from data/tickers.csv.",
            )

    isin = row.get("isin", "")
    if isin and not is_valid_isin(isin):
        add_issue(
            issues,
            "invalid_isin",
            "critical",
            "isin",
            isin,
            "ISIN fails format or Luhn checksum validation.",
        )
    if not isin and scope.get("scope_reason") == "primary_listing_missing_isin":
        add_issue(
            issues,
            "expected_missing_primary_isin",
            "source_gap",
            "isin",
            "",
            "Core primary listing is explicitly scoped as missing ISIN.",
        )
    inferred_country = country_from_isin(isin) if isin else None
    if inferred_country and row.get("country") and inferred_country != row["country"]:
        add_issue(
            issues,
            "country_isin_mismatch",
            "high",
            "country",
            row.get("country", ""),
            f"Country disagrees with ISIN prefix country {inferred_country}.",
        )

    country = row.get("country", "")
    country_code = row.get("country_code", "")
    if country and not country_code:
        add_issue(
            issues,
            "missing_country_code",
            "high",
            "country_code",
            "",
            "Country is present but ISO country_code is missing.",
        )
    expected_country_code = COUNTRY_TO_ISO.get(country)
    if expected_country_code and country_code and country_code != expected_country_code:
        add_issue(
            issues,
            "country_code_mismatch",
            "high",
            "country_code",
            country_code,
            f"country_code should be {expected_country_code} for {country}.",
        )

    asset_type = row.get("asset_type", "")
    stock_sector = row.get("stock_sector", "")
    etf_category = row.get("etf_category", "")
    if asset_type == "Stock":
        if etf_category:
            add_issue(
                issues,
                "stock_has_etf_category",
                "high",
                "etf_category",
                etf_category,
                "Stock rows must not carry ETF category metadata.",
            )
        if stock_sector and stock_sector not in VALID_STOCK_SECTORS:
            add_issue(
                issues,
                "invalid_stock_sector",
                "high",
                "stock_sector",
                stock_sector,
                "stock_sector is not a canonical stock sector.",
            )
        if not stock_sector and scope.get("instrument_scope") == "core":
            add_issue(
                issues,
                "missing_stock_sector",
                "source_gap",
                "stock_sector",
                "",
                "Core stock listing has no stock_sector.",
            )
    if asset_type == "ETF":
        if stock_sector:
            add_issue(
                issues,
                "etf_has_stock_sector",
                "high",
                "stock_sector",
                stock_sector,
                "ETF rows must not carry stock sector metadata.",
            )
        if etf_category and etf_category not in VALID_ETF_SECTORS:
            add_issue(
                issues,
                "invalid_etf_category",
                "high",
                "etf_category",
                etf_category,
                "etf_category is not a standardized ETF category.",
            )
        if not etf_category and scope.get("instrument_scope") == "core":
            add_issue(
                issues,
                "missing_etf_category",
                "source_gap",
                "etf_category",
                "",
                "Core ETF listing has no etf_category.",
            )

    if not official_refs:
        if venue_status in {"official_full", "official_partial"}:
            add_issue(
                issues,
                "official_reference_gap",
                "source_gap",
                "exchange",
                row.get("exchange", ""),
                "Venue has official source coverage, but this row lacks an active official reference match.",
            )
        elif venue_status == "missing":
            add_issue(
                issues,
                "venue_missing_official_source",
                "source_gap",
                "exchange",
                row.get("exchange", ""),
                "Venue has no official source coverage in the current masterfile layer.",
            )

    assess_reference_match(row, official_refs, issues)
    assess_aliases(row, identifier, alias_context, issues)

    evidence_level, evidence_sources = build_evidence(row, scope, identifier, official_refs)
    quality_status = status_for_issues(issues)
    return EntryQualityRow(
        listing_key=listing_key,
        ticker=row.get("ticker", ""),
        exchange=row.get("exchange", ""),
        asset_type=asset_type,
        name=row.get("name", ""),
        country=country,
        country_code=country_code,
        isin=isin,
        instrument_scope=scope.get("instrument_scope", ""),
        scope_reason=scope.get("scope_reason", ""),
        primary_listing_key=scope.get("primary_listing_key", ""),
        venue_status=venue_status,
        evidence_level=evidence_level,
        evidence_sources=evidence_sources,
        quality_status=quality_status,
        quality_score=score_for_issues(issues),
        issues=sorted(issues, key=lambda issue: (-issue.penalty, issue.issue_type, issue.field)),
        recommended_action=action_for_status(quality_status),
    )


def assess_entries(
    listings: list[dict[str, str]],
    *,
    tickers: list[dict[str, str]],
    scopes: list[dict[str, str]],
    identifiers: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
    aliases: list[dict[str, str]],
    coverage_report: dict[str, Any],
) -> list[EntryQualityRow]:
    listing_key_counts = Counter(row.get("listing_key", "") for row in listings)
    duplicate_listing_keys = {key for key, count in listing_key_counts.items() if key and count > 1}
    listing_keys = {row["listing_key"] for row in listings}
    primary_listing_keys = {listing_key_for(row) for row in tickers}
    scope_lookup = build_scope_lookup(scopes)
    identifier_lookup = build_identifier_lookup(identifiers)
    official_lookup = build_official_reference_lookup(masterfiles)
    alias_context = build_alias_context(listings)
    venue_lookup = build_venue_lookup(coverage_report)

    rows: list[EntryQualityRow] = []
    for row in listings:
        key = (row.get("ticker", ""), row.get("exchange", ""), row.get("asset_type", ""))
        rows.append(
            assess_entry(
                row,
                duplicate_listing_keys=duplicate_listing_keys,
                listing_keys=listing_keys,
                primary_listing_keys=primary_listing_keys,
                scope=scope_lookup.get(row.get("listing_key", ""), {}),
                identifier=identifier_lookup.get(row.get("listing_key", ""), {}),
                official_refs=official_lookup.get(key, []),
                venue_status=venue_lookup.get(row.get("exchange", ""), ""),
                alias_context=alias_context,
            )
        )
    return rows


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def summarize(rows: list[EntryQualityRow], generated_at: str, csv_out: Path) -> dict[str, Any]:
    issue_counts = Counter(issue.issue_type for row in rows for issue in row.issues)
    severity_counts = Counter(issue.severity for row in rows for issue in row.issues)
    status_counts = Counter(row.quality_status for row in rows)
    evidence_counts = Counter(row.evidence_level for row in rows)
    exchange_status_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        exchange_status_counts[row.exchange][row.quality_status] += 1

    flagged = [row for row in rows if row.quality_status != "pass"]
    return {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(rows),
            "csv_out": display_path(csv_out),
            "source_files": {
                "listings_csv": str(LISTINGS_CSV.relative_to(ROOT)),
                "tickers_csv": str(TICKERS_CSV.relative_to(ROOT)),
                "instrument_scopes_csv": str(INSTRUMENT_SCOPES_CSV.relative_to(ROOT)),
                "identifiers_extended_csv": str(IDENTIFIERS_EXTENDED_CSV.relative_to(ROOT)),
                "masterfile_reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)),
                "coverage_report_json": str(COVERAGE_REPORT_JSON.relative_to(ROOT)),
            },
        },
        "summary": {
            "status_counts": dict(sorted(status_counts.items())),
            "evidence_level_counts": dict(sorted(evidence_counts.items())),
            "severity_counts": dict(sorted(severity_counts.items())),
            "issue_counts": dict(issue_counts.most_common()),
            "top_flagged_exchanges": [
                {"exchange": exchange, **dict(counts)}
                for exchange, counts in sorted(
                    exchange_status_counts.items(),
                    key=lambda item: -(item[1]["quarantine"] + item[1]["warn"] + item[1]["source_gap"]),
                )[:20]
            ],
        },
        "flagged_items": [
            {
                "listing_key": row.listing_key,
                "ticker": row.ticker,
                "exchange": row.exchange,
                "asset_type": row.asset_type,
                "quality_status": row.quality_status,
                "quality_score": row.quality_score,
                "recommended_action": row.recommended_action,
                "issues": [asdict(issue) for issue in row.issues],
            }
            for row in flagged[:1000]
        ],
    }


def write_csv(path: Path, rows: list[EntryQualityRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "listing_key",
        "ticker",
        "exchange",
        "asset_type",
        "name",
        "country",
        "country_code",
        "isin",
        "instrument_scope",
        "scope_reason",
        "primary_listing_key",
        "venue_status",
        "evidence_level",
        "evidence_sources",
        "quality_status",
        "quality_score",
        "issue_count",
        "issue_types",
        "issues",
        "recommended_action",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "listing_key": row.listing_key,
                    "ticker": row.ticker,
                    "exchange": row.exchange,
                    "asset_type": row.asset_type,
                    "name": row.name,
                    "country": row.country,
                    "country_code": row.country_code,
                    "isin": row.isin,
                    "instrument_scope": row.instrument_scope,
                    "scope_reason": row.scope_reason,
                    "primary_listing_key": row.primary_listing_key,
                    "venue_status": row.venue_status,
                    "evidence_level": row.evidence_level,
                    "evidence_sources": "|".join(row.evidence_sources),
                    "quality_status": row.quality_status,
                    "quality_score": row.quality_score,
                    "issue_count": len(row.issues),
                    "issue_types": "|".join(issue.issue_type for issue in row.issues),
                    "issues": json.dumps([asdict(issue) for issue in row.issues], ensure_ascii=False, separators=(",", ":")),
                    "recommended_action": row.recommended_action,
                }
            )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# Entry Quality Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "## Status Counts",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["status_counts"].items():
        lines.append(f"| {status} | {count:,} |")

    lines.extend(["", "## Issue Counts", "", "| Issue | Rows |", "|---|---:|"])
    for issue, count in list(summary["issue_counts"].items())[:30]:
        lines.append(f"| {issue} | {count:,} |")

    lines.extend(["", "## Top Flagged Exchanges", "", "| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |", "|---|---:|---:|---:|---:|---:|"])
    for row in summary["top_flagged_exchanges"][:20]:
        lines.append(
            f"| {row['exchange']} | {row.get('pass', 0):,} | {row.get('notice', 0):,} | {row.get('source_gap', 0):,} | {row.get('warn', 0):,} | {row.get('quarantine', 0):,} |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.",
            "- `notice` marks soft alias-review hints; it is not a structural row warning.",
            "- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.",
            "- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed quality report for every ticker database entry.")
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--identifiers-extended-csv", type=Path, default=IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def build_report(args: argparse.Namespace) -> tuple[list[EntryQualityRow], dict[str, Any]]:
    rows = assess_entries(
        load_csv(args.listings_csv),
        tickers=load_csv(args.tickers_csv),
        scopes=load_csv(args.instrument_scopes_csv),
        identifiers=load_csv(args.identifiers_extended_csv),
        masterfiles=load_csv(args.masterfile_reference_csv),
        aliases=load_csv(ROOT / "data" / "aliases.csv"),
        coverage_report=load_json(args.coverage_report_json),
    )
    return rows, summarize(rows, utc_now(), args.csv_out)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows, payload = build_report(args)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                **payload["summary"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
