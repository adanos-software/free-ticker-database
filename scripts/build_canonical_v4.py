"""Build reviewable canonical-v4 CSV tables from compatibility exports.

The builder is deterministic for fixed inputs and a fixed commit SHA. It never
fetches data and never mutates the compatibility dataset.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from scripts.build_coverage_contracts import parse_time
    from scripts.build_reference_reconciliation import identity_compatible
    from scripts.lib.canonical_ids import stable_id
    from scripts.lib.identity_integrity import identity_group_is_coherent, names_refer_to_same_identity
    from scripts.lib.listing_lifecycles import ListingLifecycle, build_listing_lifecycles
    from scripts.lib.review_adjudications import valid_isin
    from scripts.normalize_source_registry import normalize_source
except ModuleNotFoundError:  # pragma: no cover
    from build_coverage_contracts import parse_time
    from build_reference_reconciliation import identity_compatible
    from lib.canonical_ids import stable_id
    from lib.identity_integrity import identity_group_is_coherent, names_refer_to_same_identity
    from lib.listing_lifecycles import ListingLifecycle, build_listing_lifecycles
    from lib.review_adjudications import valid_isin
    from normalize_source_registry import normalize_source

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "canonical_v4"
CONTRACT_JSON = ROOT / "schema" / "canonical_v4_contract.json"
SCHEMA_SQL = ROOT / "schema" / "canonical_v4.sql"
LISTINGS_CSV = DATA_DIR / "listings.csv"
SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers_extended.csv"
STATUS_HISTORY_CSV = DATA_DIR / "history" / "listing_status_history.csv"
LISTING_EVENTS_CSV = DATA_DIR / "history" / "listing_events.csv"
REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
SOURCES_JSON = DATA_DIR / "masterfiles" / "sources.json"
MASTERFILE_SUMMARY_JSON = DATA_DIR / "masterfiles" / "summary.json"
COVERAGE_CONTRACTS_CSV = DATA_DIR / "reports" / "coverage_contracts.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"
MIC_MAPPING_CSV = DATA_DIR / "masterfiles" / "venue_mic_mapping.csv"

CRITICAL_FIELDS = ("name", "asset_type", "country_code", "isin", "stock_sector", "etf_category")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
TABLE_ORDER = [
    "sources", "source_observations", "venues", "issuers", "instruments", "listings",
    "identifier_assertions", "field_assertions", "provenance_gaps", "listing_events",
    "coverage_contracts",
]


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_rows_sha(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(list(rows), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return sha256_bytes(payload.encode("utf-8"))


def listing_key(row: Mapping[str, Any]) -> str:
    key = str(row.get("listing_key", "") or "").strip()
    if key:
        return key
    return f"{str(row.get('exchange', '')).strip()}::{str(row.get('ticker', '')).strip().upper()}"


def split_listing_key(key: str) -> tuple[str, str]:
    return tuple(key.split("::", 1)) if "::" in key else ("", key)  # type: ignore[return-value]


def normalized_name(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", (value or "").lower()))


def resolve_git_commit(explicit: str | None = None) -> str:
    value = (explicit or os.environ.get("GITHUB_SHA") or os.environ.get("CANONICAL_GIT_COMMIT") or "").strip().lower()
    if not value:
        try:
            value = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip().lower()
        except (OSError, subprocess.CalledProcessError):
            value = ""
    if not SHA_RE.fullmatch(value):
        raise ValueError("canonical manifest requires a full 40-character hexadecimal git commit")
    return value


def resolve_built_at(explicit: str | None, tickers_json: Path) -> str:
    if explicit:
        value = explicit
    elif tickers_json.exists():
        try:
            value = str(json.loads(tickers_json.read_text(encoding="utf-8")).get("_meta", {}).get("built_at", ""))
        except (OSError, json.JSONDecodeError):
            value = ""
    else:
        value = ""
    if parse_time(value) is None:
        raise ValueError("canonical build requires a timezone-aware built_at timestamp")
    return value


def source_template(key: str, *, provider: str, source_url: str, authority: str, reference_scope: str, internal: bool) -> dict[str, Any]:
    return normalize_source({
        "key": key, "provider": provider, "description": f"Canonical bridge source: {provider}",
        "source_url": source_url, "format": "canonical_bridge", "reference_scope": reference_scope,
        "official": authority == "official", "authority_level": authority,
        "license_status": "internal" if internal else "review_required",
        "derived_facts_redistribution_status": "allowed" if internal else "review_required",
        "commercial_use_status": "allowed" if internal else "review_required",
        "attribution_required": "none" if internal else "review_required",
        "freshness_sla_days": 30, "enabled": True,
    })


class CanonicalBuilder:
    def __init__(self, *, built_at: str, git_commit: str):
        self.built_at = built_at
        self.git_commit = git_commit
        self.sources: dict[str, dict[str, Any]] = {}
        self.observations: dict[str, dict[str, str]] = {}

    def ensure_source(self, row: Mapping[str, Any]) -> str:
        normalized = normalize_source(row)
        key = normalized["key"]
        existing = self.sources.get(key)
        if existing and existing != normalized:
            raise ValueError(f"source registry conflict for {key}")
        self.sources[key] = normalized
        return stable_id("source", key)

    def observation(
        self,
        *,
        source_key: str,
        source_record_id: str,
        observed_at: str,
        raw_uri: str,
        normalized_rows_sha256: str,
        parser_name: str,
        parser_version: str = "1",
        parse_status: str = "success",
        effective_at: str = "",
        raw_sha256: str = "",
        namespace: str = "source_snapshot",
    ) -> str:
        if source_key not in self.sources:
            internal = source_key.startswith("internal_")
            self.ensure_source(source_template(
                source_key,
                provider=source_key.replace("_", " ").title(),
                source_url=f"internal://{source_key}" if internal else f"unresolved://source/{source_key}",
                authority="review" if internal else "registry",
                reference_scope="internal_evidence" if internal else "identifier_enrichment",
                internal=internal,
            ))
        token = f"{namespace}:{source_key}:{source_record_id}:{normalized_rows_sha256}"
        observation_id = stable_id("source_observation", token)
        row = {
            "observation_id": observation_id,
            "source_id": stable_id("source", source_key),
            "source_key": source_key,
            "source_record_id": source_record_id,
            "observed_at": observed_at,
            "effective_at": effective_at,
            "raw_uri": raw_uri,
            "raw_sha256": raw_sha256,
            "normalized_rows_sha256": normalized_rows_sha256,
            "parser_name": parser_name,
            "parser_version": parser_version,
            "parse_status": parse_status,
        }
        existing = self.observations.get(observation_id)
        if existing and existing != row:
            raise ValueError(f"source observation collision: {observation_id}")
        self.observations[observation_id] = row
        return observation_id


def _source_details(path: Path) -> dict[str, Mapping[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("source_details", {}) if isinstance(payload, dict) else {}


def _load_mic_mapping(path: Path) -> dict[str, dict[str, str]]:
    rows = load_csv(path)
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        exchange = row.get("exchange_code", "").strip()
        if not exchange:
            continue
        if exchange in result:
            raise ValueError(f"duplicate MIC mapping for {exchange}")
        for field in ("operating_mic", "segment_mic"):
            value = row.get(field, "").strip().upper()
            if value and not re.fullmatch(r"[A-Z0-9]{4}", value):
                raise ValueError(f"invalid {field} for {exchange}: {value}")
        result[exchange] = row
    return result


def _primary_keys(scopes_csv: Path) -> set[str]:
    return {row.get("primary_listing_key", "") for row in load_csv(scopes_csv) if row.get("primary_listing_key")}


def _representative(rows: Sequence[dict[str, str]], primary_keys: set[str]) -> dict[str, str]:
    return sorted(rows, key=lambda row: (listing_key(row) not in primary_keys, not bool(row.get("isin")), listing_key(row)))[0]


def _identity_quarantine_groups(
    rows: Sequence[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    """Return same-ISIN groups that are not provably one instrument.

    Grouping is complete-linkage and fail-closed: every member must have the
    same asset type and every pair of names must be identity-compatible.
    """

    by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        isin = str(row.get("isin", "")).strip().upper()
        if valid_isin(isin):
            by_isin[isin].append(row)

    unsafe: dict[str, list[dict[str, str]]] = {}
    for isin, members in sorted(by_isin.items()):
        if len(members) < 2:
            continue
        if not identity_group_is_coherent(members):
            unsafe[isin] = sorted(members, key=listing_key)
    return unsafe


def _instrument_id(
    row: Mapping[str, str], *, conflicted_listing_keys: set[str] | None = None
) -> str:
    key = listing_key(row)
    if conflicted_listing_keys and key in conflicted_listing_keys:
        # A disputed identifier must never collapse separate listings into one
        # canonical instrument. Preserve the assertion separately as quarantined.
        return stable_id("instrument", f"quarantined_listing:{key}")
    isin = str(row.get("isin", "")).strip().upper()
    return stable_id("instrument", f"isin:{isin}" if valid_isin(isin) else f"listing:{key}")


def _event_listing(lifecycles: Sequence[ListingLifecycle], when: str, event_type: str) -> ListingLifecycle | None:
    candidates = [
        item for item in lifecycles
        if (not item.valid_from or item.valid_from <= when) and (not item.valid_to or when <= item.valid_to)
    ]
    if candidates:
        return sorted(candidates, key=lambda item: (item.current, item.valid_from), reverse=True)[0]
    if event_type in {"delisted", "suspended", "not_observed"}:
        historical = [item for item in lifecycles if not item.current]
        if historical:
            return historical[-1]
    current = [item for item in lifecycles if item.current]
    return current[0] if current else (lifecycles[-1] if lifecycles else None)


def build(
    *,
    out_dir: Path = OUT_DIR,
    listings_csv: Path = LISTINGS_CSV,
    scopes_csv: Path = SCOPES_CSV,
    identifiers_csv: Path = IDENTIFIERS_CSV,
    status_history_csv: Path = STATUS_HISTORY_CSV,
    listing_events_csv: Path = LISTING_EVENTS_CSV,
    reference_csv: Path = REFERENCE_CSV,
    sources_json: Path = SOURCES_JSON,
    masterfile_summary_json: Path = MASTERFILE_SUMMARY_JSON,
    coverage_contracts_csv: Path = COVERAGE_CONTRACTS_CSV,
    mic_mapping_csv: Path = MIC_MAPPING_CSV,
    contract_json: Path = CONTRACT_JSON,
    schema_sql: Path = SCHEMA_SQL,
    tickers_json: Path = TICKERS_JSON,
    built_at: str | None = None,
    git_commit: str | None = None,
) -> dict[str, Any]:
    built_at = resolve_built_at(built_at, tickers_json)
    commit = resolve_git_commit(git_commit)
    builder = CanonicalBuilder(built_at=built_at, git_commit=commit)
    contract = json.loads(contract_json.read_text(encoding="utf-8"))
    table_specs = contract["tables"]

    registry = json.loads(sources_json.read_text(encoding="utf-8")) if sources_json.exists() else []
    if not isinstance(registry, list):
        raise ValueError("sources registry must be a JSON list")
    for source in registry:
        builder.ensure_source(source)
    builder.ensure_source(source_template("internal_current_snapshot", provider="Canonical current snapshot", source_url="internal://current-snapshot", authority="review", reference_scope="current_snapshot", internal=True))
    builder.ensure_source(source_template("internal_history", provider="Canonical listing history", source_url="internal://listing-history", authority="review", reference_scope="listing_history", internal=True))

    current_rows = [{str(k): str(v or "") for k, v in row.items()} for row in load_csv(listings_csv)]
    current_counts = Counter(listing_key(row) for row in current_rows)
    duplicates = sorted(key for key, count in current_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate current listing keys: {duplicates[:10]}")
    current_by_key = {listing_key(row): row for row in current_rows}
    status_rows = load_csv(status_history_csv)
    lifecycles = build_listing_lifecycles(current_rows, status_rows, observed_at=built_at)
    lifecycles_by_key: dict[str, list[ListingLifecycle]] = defaultdict(list)
    for lifecycle in lifecycles:
        lifecycles_by_key[lifecycle.listing_key].append(lifecycle)

    current_snapshot_hash = sha256_file(listings_csv)
    current_observation = builder.observation(
        source_key="internal_current_snapshot", source_record_id=current_snapshot_hash,
        observed_at=built_at, raw_uri=listings_csv.as_posix(), raw_sha256=current_snapshot_hash,
        normalized_rows_sha256=current_snapshot_hash, parser_name="build_canonical_v4.current_snapshot",
    )

    source_details = _source_details(masterfile_summary_json)
    references = [row for row in load_csv(reference_csv) if row.get("official") == "true" and row.get("listing_status") == "active"]
    references_by_source: dict[str, list[dict[str, str]]] = defaultdict(list)
    references_by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in references:
        source_key = row.get("source_key", "")
        if source_key:
            references_by_source[source_key].append(row)
        references_by_key[listing_key(row)].append(row)
    source_observation_by_key: dict[str, str] = {}
    for source_key, rows in sorted(references_by_source.items()):
        if source_key not in builder.sources:
            builder.ensure_source(source_template(source_key, provider=source_key, source_url=rows[0].get("source_url", "") or f"https://example.invalid/{source_key}", authority="official", reference_scope=rows[0].get("reference_scope", ""), internal=False))
        detail = source_details.get(source_key, {})
        observed = str(detail.get("generated_at", "")) if isinstance(detail, Mapping) else ""
        if parse_time(observed) is None:
            observed = built_at
        digest = canonical_rows_sha(sorted(rows, key=lambda row: (listing_key(row), row.get("isin", ""), row.get("name", ""))))
        source_observation_by_key[source_key] = builder.observation(
            source_key=source_key, source_record_id=digest, observed_at=observed,
            raw_uri=str(builder.sources[source_key].get("source_url", "")),
            normalized_rows_sha256=digest, parser_name=str(builder.sources[source_key].get("format", "masterfile")) or "masterfile",
            parse_status=str(detail.get("mode", "success")) if isinstance(detail, Mapping) else "success",
        )

    mic_mapping = _load_mic_mapping(mic_mapping_csv)
    exchanges = sorted({row.get("exchange", "") for row in current_rows if row.get("exchange")} | {item.exchange for item in lifecycles if item.exchange})
    venues = []
    for exchange in exchanges:
        mapping = mic_mapping.get(exchange, {})
        venues.append({
            "venue_id": stable_id("venue", exchange), "exchange_code": exchange,
            "operating_mic": mapping.get("operating_mic", ""), "segment_mic": mapping.get("segment_mic", ""),
            "canonical_name": mapping.get("canonical_name", "") or exchange,
            "country_code": mapping.get("country_code", ""), "status": "active",
        })

    primary_keys = _primary_keys(scopes_csv)
    identifiers_by_key = {listing_key(row): row for row in load_csv(identifiers_csv)}
    identity_quarantine_groups = _identity_quarantine_groups(current_rows)
    conflicted_listing_keys = {
        listing_key(row)
        for members in identity_quarantine_groups.values()
        for row in members
    }
    current_instrument_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in current_rows:
        current_instrument_groups[
            _instrument_id(row, conflicted_listing_keys=conflicted_listing_keys)
        ].append(row)

    issuers: dict[str, dict[str, str]] = {}
    instruments: dict[str, dict[str, str]] = {}
    instrument_id_by_current_key: dict[str, str] = {}
    issuer_id_by_instrument: dict[str, str] = {}
    for instrument_id, members in sorted(current_instrument_groups.items()):
        representative = _representative(members, primary_keys)
        asset_types = {row.get("asset_type", "") for row in members}
        if len(asset_types) != 1:
            raise ValueError(f"instrument {instrument_id} spans asset types: {sorted(asset_types)}")
        for left in members:
            for right in members:
                if left is right:
                    continue
                if not names_refer_to_same_identity(left.get("name", ""), right.get("name", ""), representative.get("asset_type", "")):
                    raise ValueError(f"instrument {instrument_id} spans incompatible names: {left.get('name')} / {right.get('name')}")
        representative_key = listing_key(representative)
        identity_quarantined = representative_key in conflicted_listing_keys
        extended = identifiers_by_key.get(representative_key, {})
        # The compatibility files do not yet contain a separately adjudicated
        # issuer identity layer. Keep one conservative issuer per instrument;
        # CIK and LEI remain listing-scoped assertions until issuer resolution.
        issuer_id = stable_id("issuer", f"instrument:{instrument_id}")
        country_codes = {row.get("country_code", "") for row in members if row.get("country_code", "")}
        canonical_country = next(iter(country_codes)) if len(country_codes) == 1 else ""
        issuers[issuer_id] = {
            "issuer_id": issuer_id, "legal_name": representative.get("name", "") or listing_key(representative),
            "normalized_name": normalized_name(representative.get("name", "")) or listing_key(representative).lower(),
            "lei": "", "domicile_country_code": canonical_country, "status": "active",
        }
        issuer_id_by_instrument[instrument_id] = issuer_id
        lifecycle_rows = [item for row in members for item in lifecycles_by_key.get(listing_key(row), []) if item.current]
        valid_from = min((item.valid_from for item in lifecycle_rows), default=built_at)
        instruments[instrument_id] = {
            "instrument_id": instrument_id, "issuer_id": issuer_id,
            "isin": (
                representative.get("isin", "").strip().upper()
                if valid_isin(representative.get("isin", "")) and not identity_quarantined
                else ""
            ),
            "asset_type": representative.get("asset_type", "") or "Unknown",
            "security_type": "etf" if representative.get("asset_type") == "ETF" else "equity" if representative.get("asset_type") == "Stock" else "unknown",
            "name": representative.get("name", "") or listing_key(representative),
            "country_code": canonical_country, "status": "active",
            "valid_from": valid_from, "valid_to": "",
        }
        for row in members:
            instrument_id_by_current_key[listing_key(row)] = instrument_id

    listing_rows: list[dict[str, str]] = []
    listing_instrument: dict[str, str] = {}
    for lifecycle in lifecycles:
        current_instrument = instrument_id_by_current_key.get(lifecycle.listing_key)
        if lifecycle.current and current_instrument:
            instrument_id = current_instrument
        else:
            instrument_id = stable_id("instrument", f"historical_listing:{lifecycle.listing_id}")
            if instrument_id not in instruments:
                name = lifecycle.listing_key
                issuer_id = stable_id("issuer", f"historical_listing:{lifecycle.listing_id}")
                issuers[issuer_id] = {
                    "issuer_id": issuer_id, "legal_name": name, "normalized_name": normalized_name(name),
                    "lei": "", "domicile_country_code": "", "status": "historical",
                }
                instruments[instrument_id] = {
                    "instrument_id": instrument_id, "issuer_id": issuer_id, "isin": "", "asset_type": "Unknown",
                    "security_type": "unknown", "name": name, "country_code": "", "status": "historical",
                    "valid_from": lifecycle.valid_from, "valid_to": lifecycle.valid_to,
                }
        listing_instrument[lifecycle.listing_id] = instrument_id
        listing_rows.append({
            "listing_id": lifecycle.listing_id, "listing_key": lifecycle.listing_key,
            "instrument_id": instrument_id, "venue_id": stable_id("venue", lifecycle.exchange),
            "local_symbol": lifecycle.ticker, "is_primary": "true" if lifecycle.listing_key in primary_keys and lifecycle.current else "false",
            "status": lifecycle.status, "valid_from": lifecycle.valid_from, "valid_to": lifecycle.valid_to,
            "current": "true" if lifecycle.current else "false",
            "source_observation_id": current_observation if lifecycle.current else "",
        })

    current_listing_id_by_key = {row["listing_key"]: row["listing_id"] for row in listing_rows if row["current"] == "true"}
    identifier_assertions: dict[str, dict[str, str]] = {}
    field_assertions: dict[str, dict[str, str]] = {}
    accepted_field_keys: set[tuple[str, str]] = set()

    for key, row in sorted(current_by_key.items()):
        instrument_id = instrument_id_by_current_key[key]
        issuer_id = issuer_id_by_instrument[instrument_id]
        listing_id = current_listing_id_by_key[key]
        compatible_refs = [ref for ref in references_by_key.get(key, []) if identity_compatible(ref, row)[0]]
        exact_observations = [source_observation_by_key[ref.get("source_key", "")] for ref in compatible_refs if ref.get("source_key", "") in source_observation_by_key]
        isin = row.get("isin", "").strip().upper()
        if valid_isin(isin):
            identity_quarantined = key in conflicted_listing_keys
            observation_id = ""
            for ref in compatible_refs:
                if ref.get("isin", "").strip().upper() == isin:
                    observation_id = source_observation_by_key.get(ref.get("source_key", ""), "")
                    if observation_id:
                        break
            assertion_id = stable_id("identifier_assertion", f"instrument:{instrument_id}:ISIN:{isin}")
            identifier_assertions[assertion_id] = {
                "assertion_id": assertion_id, "entity_type": "instrument", "entity_id": instrument_id,
                "scheme": "ISIN", "value": isin, "observation_id": observation_id,
                "confidence": (
                    "0.5000" if identity_quarantined else "1.0000" if observation_id else "0.8000"
                ),
                "adjudication_status": (
                    "quarantined_identity_conflict"
                    if identity_quarantined
                    else "accepted" if observation_id else "provisional"
                ),
                "valid_from": instruments[instrument_id]["valid_from"], "valid_to": "",
            }
        extended = identifiers_by_key.get(key, {})
        identifier_specs = [
            ("WKN", extended.get("wkn", ""), "instrument", instrument_id, "wkn_source"),
            ("FIGI", extended.get("figi", ""), "listing", listing_id, "figi_source"),
            ("CIK", extended.get("cik", ""), "listing", listing_id, "cik_source"),
            ("LEI", extended.get("lei", ""), "listing", listing_id, "lei_source"),
        ]
        for scheme, value, entity_type, entity_id, source_field in identifier_specs:
            value = (value or "").strip()
            if not value:
                continue
            raw_source = (extended.get(source_field, "") or scheme).strip()
            known_sources = {
                "OpenFIGI": ("openfigi_enrichment", "https://api.openfigi.com/v3/mapping"),
                "GLEIF": ("gleif_enrichment", "https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file"),
                "SEC company_tickers_exchange.json": ("sec_company_tickers_exchange", "https://www.sec.gov/files/company_tickers_exchange.json"),
            }
            known_source = raw_source in known_sources
            if known_source:
                source_key, source_url = known_sources[raw_source]
            else:
                normalized_source = re.sub(r"[^a-z0-9]+", "_", raw_source.lower()).strip("_")
                source_key = f"unresolved_identifier_{normalized_source or scheme.lower()}"
                source_url = f"unresolved://identifier/{source_key}"
            if source_key not in builder.sources:
                builder.ensure_source(source_template(
                    source_key, provider=raw_source, source_url=source_url, authority="registry",
                    reference_scope="identifier_enrichment", internal=False,
                ))
            record_hash = sha256_bytes(json.dumps({"listing_key": key, "scheme": scheme, "value": value}, sort_keys=True).encode("utf-8"))
            observation_id = builder.observation(
                source_key=source_key, source_record_id=f"{key}:{scheme}", observed_at=built_at,
                raw_uri=str(builder.sources[source_key].get("source_url", "")),
                normalized_rows_sha256=record_hash, parser_name="identifiers_extended",
            )
            assertion_id = stable_id("identifier_assertion", f"{entity_type}:{entity_id}:{scheme}:{value}")
            identifier_assertions[assertion_id] = {
                "assertion_id": assertion_id, "entity_type": entity_type, "entity_id": entity_id,
                "scheme": scheme, "value": value, "observation_id": observation_id,
                "confidence": "0.9500" if known_source else "0.6000",
                "adjudication_status": "accepted" if known_source else "provisional",
                "valid_from": instruments[instrument_id]["valid_from"], "valid_to": "",
            }

        for ref in compatible_refs:
            observation_id = source_observation_by_key.get(ref.get("source_key", ""), "")
            if not observation_id:
                continue
            pairs = {
                "name": (row.get("name", ""), ref.get("name", "")),
                "asset_type": (row.get("asset_type", ""), ref.get("asset_type", "")),
                "isin": (row.get("isin", ""), ref.get("isin", "")),
                "stock_sector": (row.get("stock_sector", ""), ref.get("sector", "") if row.get("asset_type") == "Stock" else ""),
                "etf_category": (row.get("etf_category", ""), ref.get("sector", "") if row.get("asset_type") == "ETF" else ""),
            }
            for field, (value, official_value) in pairs.items():
                if field == "isin" and key in conflicted_listing_keys:
                    # Conflicting identifier assertions cannot become accepted
                    # field truth merely because one source repeats the value.
                    continue
                if not value or not official_value or value.casefold() != official_value.casefold():
                    continue
                entity_type, entity_id = ("listing", listing_id) if field in {"name", "stock_sector", "etf_category"} else ("instrument", instrument_id)
                assertion_id = stable_id("field_assertion", f"{entity_type}:{entity_id}:{field}:{value}:{observation_id}")
                field_assertions[assertion_id] = {
                    "assertion_id": assertion_id, "entity_type": entity_type, "entity_id": entity_id,
                    "field_name": field, "field_value": value, "observation_id": observation_id,
                    "confidence": "1.0000", "adjudication_status": "accepted",
                    "valid_from": instruments[instrument_id]["valid_from"], "valid_to": "",
                }
                accepted_field_keys.add((key, field))

    assertions_by_entity_scheme: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for assertion in identifier_assertions.values():
        assertions_by_entity_scheme[(
            assertion["entity_type"], assertion["entity_id"], assertion["scheme"]
        )].append(assertion)
    for assertions in assertions_by_entity_scheme.values():
        values = {assertion["value"] for assertion in assertions}
        if len(values) <= 1:
            continue
        for assertion in assertions:
            assertion["confidence"] = "0.5000"
            assertion["adjudication_status"] = "quarantined_entity_identifier_conflict"

    provenance_gaps = []
    for key, row in sorted(current_by_key.items()):
        listing_id = current_listing_id_by_key[key]
        instrument_id = instrument_id_by_current_key[key]
        for field in CRITICAL_FIELDS:
            value = row.get(field, "")
            if not value or (key, field) in accepted_field_keys:
                continue
            entity_type, entity_id = ("listing", listing_id) if field in {"name", "stock_sector", "etf_category"} else ("instrument", instrument_id)
            gap_id = stable_id("provenance_gap", f"{entity_type}:{entity_id}:{key}:{field}")
            identity_conflict = field == "isin" and key in conflicted_listing_keys
            provenance_gaps.append({
                "gap_id": gap_id, "entity_type": entity_type, "entity_id": entity_id,
                "listing_key": key, "field_name": field, "current_value": value,
                "gap_class": (
                    "conflicting_identifier_assertion"
                    if identity_conflict
                    else "missing_field_level_observation"
                ),
                "required_evidence": (
                    "explicit listing-keyed identifier adjudication resolving every conflicting identity family"
                    if identity_conflict
                    else "active listing-keyed official or explicitly reviewed source observation"
                ),
            })

    canonical_events = []
    for raw in load_csv(listing_events_csv):
        key = listing_key(raw)
        if not key or key == "::":
            continue
        when = raw.get("effective_at") or raw.get("observed_at") or built_at
        if parse_time(when) is None:
            continue
        lifecycle = _event_listing(lifecycles_by_key.get(key, []), when, raw.get("event_type", ""))
        if lifecycle is None:
            continue
        source_key = raw.get("source_key", "").strip() or "internal_history"
        if source_key not in builder.sources:
            builder.ensure_source(source_template(source_key, provider=source_key, source_url=raw.get("source_url", "") or f"internal://event/{source_key}", authority="review", reference_scope="listing_event", internal=not bool(raw.get("source_url"))))
        raw_token = raw.get("observation_id", "").strip() or sha256_bytes(json.dumps(raw, sort_keys=True).encode("utf-8"))
        normalized_hash = sha256_bytes(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        observation_id = builder.observation(
            source_key=source_key, source_record_id=raw_token, observed_at=raw.get("observed_at") or when,
            effective_at=raw.get("effective_at", ""), raw_uri=raw.get("source_url", "") or raw.get("source_report", "") or listing_events_csv.as_posix(),
            normalized_rows_sha256=normalized_hash, parser_name="listing_events", namespace="listing_event",
        )
        event_id = stable_id("listing_event", f"{lifecycle.listing_id}:{raw.get('event_type','')}:{when}:{raw.get('field_name','')}:{raw.get('old_value','')}:{raw.get('new_value','')}:{raw_token}")
        canonical_events.append({
            "event_id": event_id, "listing_id": lifecycle.listing_id, "listing_key": key,
            "event_type": raw.get("event_type", "") or "unknown", "field_name": raw.get("field_name", ""),
            "old_value": raw.get("old_value", ""), "new_value": raw.get("new_value", ""),
            "effective_at": when, "observed_at": raw.get("observed_at") or when,
            "observation_id": observation_id, "evidence_status": raw.get("evidence_status", "") or "observed_unverified",
        })

    coverage_rows = []
    for raw in load_csv(coverage_contracts_csv):
        contract_key = raw.get("contract_key", "") or f"{raw.get('exchange','')}::{raw.get('asset_type','')}"
        coverage_rows.append({
            "contract_id": stable_id("coverage_contract", contract_key), "contract_key": contract_key,
            "exchange": raw.get("exchange", ""), "asset_type": raw.get("asset_type", ""),
            "claim_type": raw.get("claim_type", ""), "source_keys": raw.get("source_keys", ""),
            "denominator_method": raw.get("denominator_method", ""), "denominator": raw.get("denominator", "0"),
            "covered_reference_keys": raw.get("covered_reference_keys", "0"), "missing_reference_keys": raw.get("missing_reference_keys", "0"),
            "identity_conflict_keys": raw.get("identity_conflict_keys", "0"), "recall_pct": raw.get("recall_pct", ""),
            "freshness_status": raw.get("freshness_status", ""), "license_status": raw.get("license_status", ""),
            "contract_status": raw.get("contract_status", ""), "generated_at": built_at,
        })

    source_rows = []
    for key, row in sorted(builder.sources.items()):
        source_rows.append({
            "source_id": stable_id("source", key), "source_key": key, "provider": row.get("provider", ""),
            "source_url": row.get("source_url", ""), "authority_level": row.get("authority_level", ""),
            "reference_scope": row.get("reference_scope", ""), "license_status": row.get("license_status", ""),
            "license_name": row.get("license_name", ""), "license_url": row.get("license_url", ""),
            "derived_facts_redistribution_status": row.get("derived_facts_redistribution_status", ""),
            "raw_redistribution_allowed": "true" if row.get("raw_redistribution_allowed") else "false",
            "attribution_required": row.get("attribution_required", ""), "commercial_use_status": row.get("commercial_use_status", ""),
            "terms_version": row.get("terms_version", ""), "terms_sha256": row.get("terms_sha256", ""),
            "license_reviewed_at": row.get("license_reviewed_at", ""), "freshness_sla_days": str(row.get("freshness_sla_days", "")),
            "enabled": "true" if row.get("enabled", True) else "false",
        })

    tables: dict[str, list[dict[str, Any]]] = {
        "sources": source_rows,
        "source_observations": sorted(builder.observations.values(), key=lambda row: row["observation_id"]),
        "venues": sorted(venues, key=lambda row: row["exchange_code"]),
        "issuers": sorted(issuers.values(), key=lambda row: row["issuer_id"]),
        "instruments": sorted(instruments.values(), key=lambda row: row["instrument_id"]),
        "listings": sorted(listing_rows, key=lambda row: (row["listing_key"], row["valid_from"], row["listing_id"])),
        "identifier_assertions": sorted(identifier_assertions.values(), key=lambda row: row["assertion_id"]),
        "field_assertions": sorted(field_assertions.values(), key=lambda row: row["assertion_id"]),
        "provenance_gaps": sorted(provenance_gaps, key=lambda row: row["gap_id"]),
        "listing_events": sorted(canonical_events, key=lambda row: (row["effective_at"], row["listing_key"], row["event_id"])),
        "coverage_contracts": sorted(coverage_rows, key=lambda row: row["contract_key"]),
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    file_entries = []
    for table in TABLE_ORDER:
        columns = list(table_specs[table]["columns"])
        path = out_dir / f"{table}.csv"
        write_csv(path, columns, tables[table])
        file_entries.append({"path": path.name, "sha256": sha256_file(path), "rows": len(tables[table])})
    aggregate = sha256_bytes("\n".join(f"{item['path']}:{item['sha256']}" for item in file_entries).encode("utf-8"))
    manifest = {
        "version": contract.get("version", "4.0.0"), "generated_at": built_at,
        "git_commit": commit, "source_dataset_sha256": current_snapshot_hash,
        "schema_contract_sha256": sha256_file(contract_json), "schema_sql_sha256": sha256_file(schema_sql),
        "aggregate_sha256": aggregate, "files": file_entries,
        "counts": {table: len(tables[table]) for table in TABLE_ORDER},
        "identity_quarantine": {
            "conflict_groups": len(identity_quarantine_groups),
            "listing_rows": len(conflicted_listing_keys),
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(manifest["counts"], indent=2))
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--built-at")
    parser.add_argument("--git-commit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build(out_dir=args.out_dir, built_at=args.built_at, git_commit=args.git_commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
