"""Conservatively reconcile official venue/symbol observations to listings.

Coverage credit is venue-specific. A listing on another venue never satisfies a
missing venue line, aggressive symbol normalization is discovery-only, and an
exact venue/symbol key is not accepted when the instrument identity conflicts.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from scripts.lib.identity_integrity import names_refer_to_same_identity
    from scripts.lib.review_adjudications import valid_isin
except ModuleNotFoundError:  # pragma: no cover
    from lib.identity_integrity import names_refer_to_same_identity
    from lib.review_adjudications import valid_isin

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MAPPING_OVERRIDES_CSV = DATA_DIR / "masterfiles" / "symbol_mapping_overrides.csv"
REPORTS_DIR = DATA_DIR / "reports"
OUT_CSV = REPORTS_DIR / "reference_reconciliation.csv"
OUT_JSON = REPORTS_DIR / "reference_reconciliation.json"
OUT_MD = REPORTS_DIR / "reference_reconciliation.md"

NON_TARGET_NAME_RE = re.compile(
    r"\b(?:warrants?|rights?|options?|futures?|certificates?|structured|notes?|bonds?|debentures?|"
    r"preferred|preference|capital securities|subscription receipts?|contingent value rights?)\b",
    re.IGNORECASE,
)
ALLOWED_ASSET_TYPES = {"Stock", "ETF"}
COVERED_CLASSIFICATIONS = {"exact_match", "reviewed_mapping"}
MAPPING_FIELDS = [
    "source_key",
    "reference_key",
    "listing_key",
    "evidence_url",
    "reviewed_at",
    "reviewer",
    "reason",
]


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def normalize_symbol(value: str) -> str:
    """Discovery-only normalization; never sufficient for coverage credit."""
    value = (value or "").strip().upper()
    return re.sub(r"[^A-Z0-9]", "", value).lstrip("0") or "0"


def listing_key(exchange: str, ticker: str) -> str:
    return f"{exchange.strip()}::{ticker.strip().upper()}"


def is_out_of_scope(row: Mapping[str, str]) -> tuple[bool, str]:
    asset_type = str(row.get("asset_type", "")).strip()
    if asset_type and asset_type not in ALLOWED_ASSET_TYPES:
        return True, f"asset_type={asset_type} is outside the Stock/ETF public scope"
    cfi = str(row.get("cfi", "")).strip().upper()
    if cfi and cfi[0] not in {"E", "C"}:
        return True, f"CFI {cfi} is outside equity/collective-investment scope"
    match = NON_TARGET_NAME_RE.search(str(row.get("name", "")))
    if match:
        return True, f"name indicates non-target instrument: {match.group(0).lower()}"
    return False, ""


def parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _valid_reviewed_at(value: str, *, as_of: datetime) -> bool:
    parsed = parse_time(value)
    if parsed is None:
        return False
    return parsed.astimezone(timezone.utc) <= as_of.astimezone(timezone.utc) + timedelta(minutes=5)


@dataclass(frozen=True)
class MappingOverride:
    source_key: str
    reference_key: str
    listing_key: str
    evidence_url: str
    reviewed_at: str
    reviewer: str
    reason: str


def load_mapping_overrides(
    path: Path = MAPPING_OVERRIDES_CSV, *, as_of: datetime | None = None
) -> dict[tuple[str, str], MappingOverride]:
    as_of = as_of or datetime.now(timezone.utc)
    rows = load_csv(path)
    if not rows:
        return {}
    headers = list(rows[0])
    if headers != MAPPING_FIELDS:
        raise ValueError(f"{path} must use exact headers {MAPPING_FIELDS}; got {headers}")
    result: dict[tuple[str, str], MappingOverride] = {}
    listing_targets: dict[str, tuple[str, str]] = {}
    for line, row in enumerate(rows, start=2):
        values = {field: str(row.get(field, "")).strip() for field in MAPPING_FIELDS}
        if not all(values.values()):
            raise ValueError(f"{path}:{line}: all mapping evidence fields are required")
        if "::" not in values["reference_key"] or "::" not in values["listing_key"]:
            raise ValueError(f"{path}:{line}: reference_key and listing_key must be venue::symbol")
        if not values["evidence_url"].startswith("https://"):
            raise ValueError(f"{path}:{line}: evidence_url must use https")
        if not _valid_reviewed_at(values["reviewed_at"], as_of=as_of):
            raise ValueError(
                f"{path}:{line}: reviewed_at must be timezone-aware ISO-8601 and not in the future"
            )
        key = (values["source_key"], values["reference_key"])
        if key in result:
            raise ValueError(f"{path}:{line}: duplicate source/reference mapping {key}")
        target_owner = listing_targets.get(values["listing_key"])
        if target_owner and target_owner != key:
            raise ValueError(
                f"{path}:{line}: listing target {values['listing_key']} is mapped from more than one reference"
            )
        item = MappingOverride(**values)
        result[key] = item
        listing_targets[item.listing_key] = key
    return result


def identity_compatible(reference: Mapping[str, str], listing: Mapping[str, str]) -> tuple[bool, str]:
    ref_asset = str(reference.get("asset_type", "")).strip()
    current_asset = str(listing.get("asset_type", "")).strip()
    if ref_asset and current_asset and ref_asset != current_asset:
        return False, f"asset type conflict: official={ref_asset}, current={current_asset}"

    ref_isin = str(reference.get("isin", "")).strip().upper()
    current_isin = str(listing.get("isin", "")).strip().upper()
    if ref_isin and not valid_isin(ref_isin):
        return False, f"official ISIN is invalid: {ref_isin}"
    if current_isin and not valid_isin(current_isin):
        return False, f"current ISIN is invalid: {current_isin}"
    if ref_isin and current_isin:
        if ref_isin != current_isin:
            return False, f"ISIN conflict: official={ref_isin}, current={current_isin}"
        # A checksum-valid identifier on the exact venue/symbol is stronger than
        # a display-name comparison. Name drift remains visible to the dedicated
        # reconciliation report, but must not turn a confirmed identifier match
        # into a false coverage conflict.
        return True, "same checksum-valid ISIN and compatible asset type"

    ref_name = str(reference.get("name", "")).strip()
    current_name = str(listing.get("name", "")).strip()
    asset_type = ref_asset or current_asset
    if ref_name and current_name and not names_refer_to_same_identity(ref_name, current_name, asset_type):
        return False, "official and current names do not form one conservative identity family"

    if ref_name and current_name:
        return True, "conservative name identity and compatible asset type"
    return False, "insufficient identity evidence"


class ReconciliationIndex:
    """Reusable listing indexes so every source is reconciled in linear time."""

    def __init__(self, listings: Sequence[Mapping[str, str]]):
        key_counts = Counter(
            listing_key(str(row.get("exchange", "")), str(row.get("ticker", ""))) for row in listings
        )
        duplicates = sorted(key for key, count in key_counts.items() if key != "::" and count > 1)
        if duplicates:
            raise ValueError(f"duplicate current listing keys: {duplicates[:10]}")
        self.by_key: dict[str, dict[str, str]] = {}
        self.by_venue_isin: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        self.by_venue_normalized_symbol: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        self.by_venue_name_asset: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
        for raw in listings:
            row = {str(k): str(v or "") for k, v in raw.items()}
            key = listing_key(row.get("exchange", ""), row.get("ticker", ""))
            self.by_key[key] = row
            venue = row.get("exchange", "")
            isin = row.get("isin", "").strip().upper()
            if isin:
                self.by_venue_isin[(venue, isin)].append(row)
            self.by_venue_normalized_symbol[(venue, normalize_symbol(row.get("ticker", "")))].append(row)
            name_key = normalize_text(row.get("name", ""))
            if name_key:
                self.by_venue_name_asset[(venue, row.get("asset_type", ""), name_key)].append(row)

    def reconcile_group(
        self,
        rows: Sequence[Mapping[str, str]],
        *,
        source_key: str,
        mapping_overrides: Mapping[tuple[str, str], MappingOverride],
    ) -> dict[str, Any]:
        if not rows:
            raise ValueError("reference group cannot be empty")
        representative = sorted(
            rows,
            key=lambda row: (
                not bool(str(row.get("isin", "")).strip()),
                not bool(str(row.get("name", "")).strip()),
                str(row.get("source_key", "")),
            ),
        )[0]
        exchange = str(representative.get("exchange", "")).strip()
        ticker = str(representative.get("ticker", "")).strip().upper()
        ref_key = listing_key(exchange, ticker)
        asset_types = sorted({str(row.get("asset_type", "")).strip() for row in rows if str(row.get("asset_type", "")).strip()})
        names = sorted({str(row.get("name", "")).strip() for row in rows if str(row.get("name", "")).strip()})
        isins = sorted({str(row.get("isin", "")).strip().upper() for row in rows if str(row.get("isin", "")).strip()})
        scopes = sorted({str(row.get("reference_scope", "")).strip() for row in rows if str(row.get("reference_scope", "")).strip()})

        result: dict[str, Any] = {
            "reconciliation_key": f"{source_key}::{ref_key}",
            "source_key": source_key,
            "reference_key": ref_key,
            "exchange": exchange,
            "ticker": ticker,
            "asset_types": "|".join(asset_types),
            "names": "|".join(names),
            "isins": "|".join(isins),
            "reference_scopes": "|".join(scopes),
            "source_row_count": len(rows),
            "classification": "missing_from_database",
            "coverage_credit": "false",
            "matched_listing_keys": "",
            "evidence": "No venue-specific identity-compatible listing was found.",
        }

        scope_results = [is_out_of_scope(row) for row in rows]
        out_reasons = [reason for flag, reason in scope_results if flag]
        if out_reasons and len(out_reasons) == len(rows):
            result.update(classification="out_of_scope", evidence="; ".join(sorted(set(out_reasons))))
            return result
        if out_reasons:
            result.update(
                classification="mixed_scope_conflict",
                evidence="Reference rows disagree on whether the instrument is in the Stock/ETF scope: "
                + "; ".join(sorted(set(out_reasons))),
            )
            return result

        current = self.by_key.get(ref_key)
        if current is not None:
            compatible = [identity_compatible(row, current) for row in rows]
            if all(flag for flag, _ in compatible):
                result.update(
                    classification="exact_match",
                    coverage_credit="true",
                    matched_listing_keys=ref_key,
                    evidence="Venue, symbol, and instrument identity match exactly.",
                )
            else:
                reasons = sorted({reason for flag, reason in compatible if not flag})
                result.update(
                    classification="exact_identity_conflict",
                    matched_listing_keys=ref_key,
                    evidence="; ".join(reasons),
                )
            return result

        override = mapping_overrides.get((source_key, ref_key))
        if override:
            target = self.by_key.get(override.listing_key)
            if target is None:
                raise ValueError(f"reviewed mapping target does not exist: {override.listing_key}")
            if str(target.get("exchange", "")) != exchange:
                raise ValueError(f"reviewed mapping crosses venues: {ref_key} -> {override.listing_key}")
            compatible = [identity_compatible(row, target) for row in rows]
            if not all(flag for flag, _ in compatible):
                reasons = sorted({reason for flag, reason in compatible if not flag})
                raise ValueError(f"reviewed mapping is identity-incompatible: {ref_key}: {reasons}")
            result.update(
                classification="reviewed_mapping",
                coverage_credit="true",
                matched_listing_keys=override.listing_key,
                evidence=(
                    f"Reviewed one-to-one venue mapping by {override.reviewer} at {override.reviewed_at}: "
                    f"{override.reason} ({override.evidence_url})"
                ),
            )
            return result

        same_venue_candidates: dict[str, dict[str, str]] = {}
        for row in rows:
            isin = str(row.get("isin", "")).strip().upper()
            if not isin:
                continue
            for candidate in self.by_venue_isin.get((exchange, isin), []):
                compatible, _ = identity_compatible(row, candidate)
                if compatible:
                    same_venue_candidates[listing_key(exchange, candidate.get("ticker", ""))] = candidate
        if len(same_venue_candidates) == 1:
            target_key = next(iter(same_venue_candidates))
            result.update(
                classification="alternate_listing_line",
                coverage_credit="false",
                matched_listing_keys=target_key,
                evidence=(
                    "Same venue and ISIN identify one compatible alternate line, but a different "
                    "venue symbol cannot satisfy this reference without an explicit reviewed mapping."
                ),
            )
            return result
        if len(same_venue_candidates) > 1:
            result.update(
                classification="ambiguous_same_venue_identifier",
                matched_listing_keys="|".join(sorted(same_venue_candidates)),
                evidence="The same venue/ISIN maps to multiple current lines and requires review.",
            )
            return result

        candidates: dict[str, str] = {}
        for candidate in self.by_venue_normalized_symbol.get((exchange, normalize_symbol(ticker)), []):
            candidates[listing_key(exchange, candidate.get("ticker", ""))] = "normalized_symbol"
        for row in rows:
            name_key = normalize_text(str(row.get("name", "")))
            asset_type = str(row.get("asset_type", ""))
            if name_key:
                for candidate in self.by_venue_name_asset.get((exchange, asset_type, name_key), []):
                    candidates[listing_key(exchange, candidate.get("ticker", ""))] = "normalized_name"
        if candidates:
            result.update(
                classification="normalization_candidate",
                matched_listing_keys="|".join(sorted(candidates)),
                evidence="Discovery-only candidate; requires an explicit reviewed one-to-one mapping before coverage credit.",
            )
        return result


def reconcile_references(
    listings: Sequence[Mapping[str, str]],
    references: Sequence[Mapping[str, str]],
    *,
    mapping_overrides: Mapping[tuple[str, str], MappingOverride] | None = None,
) -> list[dict[str, Any]]:
    mapping_overrides = mapping_overrides or {}
    index = ReconciliationIndex(listings)
    grouped: dict[tuple[str, str], list[Mapping[str, str]]] = defaultdict(list)
    for row in references:
        source_key = str(row.get("source_key", "")).strip()
        exchange = str(row.get("exchange", "")).strip()
        ticker = str(row.get("ticker", "")).strip().upper()
        if source_key and exchange and ticker:
            grouped[(source_key, listing_key(exchange, ticker))].append(row)
    return [
        index.reconcile_group(rows, source_key=source_key, mapping_overrides=mapping_overrides)
        for (source_key, _), rows in sorted(grouped.items())
    ]


def build(
    *,
    listings_csv: Path = LISTINGS_CSV,
    reference_csv: Path = REFERENCE_CSV,
    mapping_overrides_csv: Path = MAPPING_OVERRIDES_CSV,
    out_csv: Path = OUT_CSV,
    out_json: Path = OUT_JSON,
    out_md: Path = OUT_MD,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or datetime.now(timezone.utc)
    listings = load_csv(listings_csv)
    references = [
        row
        for row in load_csv(reference_csv)
        if str(row.get("official", "")).lower() == "true" and row.get("listing_status") == "active"
    ]
    mappings = load_mapping_overrides(mapping_overrides_csv, as_of=as_of)
    active_reference_keys = {
        (str(row.get("source_key", "")).strip(), listing_key(str(row.get("exchange", "")), str(row.get("ticker", ""))))
        for row in references
        if str(row.get("source_key", "")).strip()
    }
    orphan_mappings = sorted(set(mappings) - active_reference_keys)
    if orphan_mappings:
        raise ValueError(
            "reviewed mappings reference inactive or missing official observations: "
            + ", ".join(f"{source}:{key}" for source, key in orphan_mappings[:20])
        )
    current_keys = {listing_key(str(row.get("exchange", "")), str(row.get("ticker", ""))) for row in listings}
    missing_targets = sorted(item.listing_key for item in mappings.values() if item.listing_key not in current_keys)
    if missing_targets:
        raise ValueError("reviewed mapping targets do not exist: " + ", ".join(missing_targets[:20]))
    results = reconcile_references(listings, references, mapping_overrides=mappings)
    counts = Counter(row["classification"] for row in results)
    credited = sum(row["coverage_credit"] == "true" for row in results)
    summary = {
        "active_official_reference_rows": len(references),
        "source_specific_reference_keys": len(results),
        "classification_counts": dict(sorted(counts.items())),
        "coverage_credited_keys": credited,
        "unclassified_rows": sum(not row["classification"] for row in results),
        "in_scope_missing_rows": counts.get("missing_from_database", 0),
        "identity_conflict_rows": counts.get("exact_identity_conflict", 0),
        "mapping_candidate_rows": counts.get("normalization_candidate", 0),
    }
    fieldnames = [
        "reconciliation_key", "source_key", "reference_key", "exchange", "ticker",
        "asset_types", "names", "isins", "reference_scopes", "source_row_count",
        "classification", "coverage_credit", "matched_listing_keys", "evidence",
    ]
    write_csv(out_csv, fieldnames, results)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({"summary": summary, "rows": results}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Official reference reconciliation", "",
        f"- Active official source rows: **{len(references):,}**",
        f"- Source-specific venue/symbol keys: **{len(results):,}**",
        f"- Coverage-credited keys: **{credited:,}**",
        f"- Exact identity conflicts: **{summary['identity_conflict_rows']:,}**",
        f"- In-scope missing listings: **{summary['in_scope_missing_rows']:,}**",
        "", "| Classification | Keys |", "|---|---:|",
    ]
    lines.extend(f"| `{name}` | {count:,} |" for name, count in sorted(counts.items()))
    lines.extend([
        "",
        "Coverage credit is venue-specific and identity-aware. Cross-venue ISIN matches and normalized-symbol/name candidates are review queues, not completeness credit.",
        "",
    ])
    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return {"summary": summary, "rows": results}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--reference-csv", type=Path, default=REFERENCE_CSV)
    parser.add_argument("--mapping-overrides-csv", type=Path, default=MAPPING_OVERRIDES_CSV)
    parser.add_argument("--out-csv", type=Path, default=OUT_CSV)
    parser.add_argument("--out-json", type=Path, default=OUT_JSON)
    parser.add_argument("--out-md", type=Path, default=OUT_MD)
    parser.add_argument("--as-of", help="Timezone-aware ISO-8601 evaluation timestamp")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build(
        listings_csv=args.listings_csv,
        reference_csv=args.reference_csv,
        mapping_overrides_csv=args.mapping_overrides_csv,
        out_csv=args.out_csv,
        out_json=args.out_json,
        out_md=args.out_md,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
