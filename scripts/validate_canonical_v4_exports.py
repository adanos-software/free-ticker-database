"""Validate canonical-v4 CSVs, manifest binding, and semantic invariants."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts.build_coverage_contracts import parse_time, source_license_approved
    from scripts.lib.review_adjudications import valid_isin
except ModuleNotFoundError:  # pragma: no cover
    from build_coverage_contracts import parse_time, source_license_approved
    from lib.review_adjudications import valid_isin

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "data" / "canonical_v4"
CONTRACT_JSON = ROOT / "schema" / "canonical_v4_contract.json"
SCHEMA_SQL = ROOT / "schema" / "canonical_v4.sql"
COMPATIBILITY_LISTINGS = ROOT / "data" / "listings.csv"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
LEI_RE = re.compile(r"^[A-Z0-9]{20}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def _validate_type(value: str, type_name: str) -> bool:
    if value == "":
        return True
    if type_name == "text":
        return True
    if type_name == "uuid":
        try:
            uuid.UUID(value); return True
        except ValueError:
            return False
    if type_name == "boolean":
        return value in {"true", "false"}
    if type_name == "integer":
        try:
            int(value); return True
        except ValueError:
            return False
    if type_name == "decimal":
        try:
            Decimal(value); return True
        except InvalidOperation:
            return False
    if type_name == "timestamp":
        return parse_time(value) is not None
    if type_name == "country_code":
        return bool(re.fullmatch(r"[A-Z]{2}", value))
    if type_name == "mic":
        return bool(re.fullmatch(r"[A-Z0-9]{4}", value))
    if type_name == "lei":
        return bool(LEI_RE.fullmatch(value))
    if type_name == "isin":
        return valid_isin(value)
    if type_name == "sha256":
        return bool(SHA256_RE.fullmatch(value))
    raise ValueError(f"unknown contract type: {type_name}")




def schema_table_columns(path: Path) -> dict[str, list[str]]:
    """Parse the repository's line-oriented CREATE TABLE declarations."""

    if not path.exists():
        raise FileNotFoundError(path)
    tables: dict[str, list[str]] = {}
    current: str | None = None
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if current is None:
            match = re.fullmatch(r"create\s+table\s+([a-z_][a-z0-9_]*)\s*\(", stripped, re.IGNORECASE)
            if match:
                current = match.group(1)
                if current in tables:
                    raise ValueError(f"duplicate CREATE TABLE for {current} at line {line_number}")
                tables[current] = []
            continue
        if stripped == ");":
            current = None
            continue
        lowered = stripped.lower()
        if not stripped or lowered.startswith(("constraint ", "primary ", "foreign ", "unique", "check")):
            continue
        match = re.match(r'"?([a-z_][a-z0-9_]*)"?\s+', stripped, re.IGNORECASE)
        if not match:
            raise ValueError(f"unparseable schema line {line_number} in {current}: {raw}")
        tables[current].append(match.group(1))
    if current is not None:
        raise ValueError(f"unterminated CREATE TABLE for {current}")
    return tables


def validate(
    *,
    data_dir: Path = DEFAULT_DIR,
    contract_json: Path = CONTRACT_JSON,
    schema_sql: Path = SCHEMA_SQL,
    compatibility_listings: Path | None = COMPATIBILITY_LISTINGS,
    expected_git_commit: str | None = None,
) -> dict[str, Any]:
    contract = json.loads(contract_json.read_text(encoding="utf-8"))
    tables_spec = contract["tables"]
    tables: dict[str, list[dict[str, str]]] = {}
    errors: list[str] = []

    try:
        sql_columns = schema_table_columns(schema_sql)
    except (OSError, ValueError) as exc:
        sql_columns = {}
        errors.append(f"schema SQL could not be parsed: {exc}")
    expected_tables = list(tables_spec)
    if list(sql_columns) != expected_tables:
        errors.append(
            f"schema table order/set differs: expected {expected_tables}, got {list(sql_columns)}"
        )
    for table, spec in tables_spec.items():
        expected_columns = list(spec["columns"])
        if table in sql_columns and sql_columns[table] != expected_columns:
            errors.append(
                f"schema columns differ for {table}: expected {expected_columns}, got {sql_columns[table]}"
            )

    for table, spec in tables_spec.items():
        path = data_dir / f"{table}.csv"
        if not path.exists():
            errors.append(f"missing table file: {path.name}")
            continue
        headers, rows = read_csv(path)
        expected = list(spec["columns"])
        if headers != expected:
            errors.append(f"{table}: headers differ; expected {expected}, got {headers}")
        tables[table] = rows
        for line, row in enumerate(rows, start=2):
            for column, column_spec in spec["columns"].items():
                value = row.get(column, "")
                if column_spec.get("required") and value == "":
                    errors.append(f"{table}:{line}: required {column} is blank")
                elif not _validate_type(value, column_spec["type"]):
                    errors.append(f"{table}:{line}: invalid {column_spec['type']} in {column}: {value!r}")
        for columns in [spec.get("primary_key", [])]:
            if columns:
                seen: set[tuple[str, ...]] = set()
                for line, row in enumerate(rows, start=2):
                    key = tuple(row.get(column, "") for column in columns)
                    if key in seen:
                        errors.append(f"{table}:{line}: duplicate primary key {columns}={key}")
                    seen.add(key)
        for columns in spec.get("unique", []):
            seen: set[tuple[str, ...]] = set()
            for line, row in enumerate(rows, start=2):
                key = tuple(row.get(column, "") for column in columns)
                if all(value == "" for value in key):
                    continue
                if key in seen:
                    errors.append(f"{table}:{line}: duplicate unique key {columns}={key}")
                seen.add(key)

    for table, spec in tables_spec.items():
        rows = tables.get(table, [])
        for fk in spec.get("foreign_keys", []):
            target_rows = tables.get(fk["table"], [])
            target_values = {tuple(row.get(column, "") for column in fk["target"]) for row in target_rows}
            for line, row in enumerate(rows, start=2):
                value = tuple(row.get(column, "") for column in fk["columns"])
                if fk.get("nullable") and all(item == "" for item in value):
                    continue
                if value not in target_values:
                    errors.append(f"{table}:{line}: foreign key {fk['columns']}={value} missing in {fk['table']}")

    source_by_id = {row["source_id"]: row for row in tables.get("sources", [])}
    for line, row in enumerate(tables.get("source_observations", []), start=2):
        source = source_by_id.get(row["source_id"])
        if source and source["source_key"] != row["source_key"]:
            errors.append(f"source_observations:{line}: source_key disagrees with source_id")

    venue_by_id = {row["venue_id"]: row for row in tables.get("venues", [])}
    instrument_by_id = {row["instrument_id"]: row for row in tables.get("instruments", [])}
    listing_by_id = {row["listing_id"]: row for row in tables.get("listings", [])}
    current_by_key: dict[str, list[dict[str, str]]] = {}
    for line, row in enumerate(tables.get("listings", []), start=2):
        venue = venue_by_id.get(row["venue_id"])
        expected_key = f"{venue['exchange_code']}::{row['local_symbol']}" if venue else ""
        if expected_key and row["listing_key"] != expected_key:
            errors.append(f"listings:{line}: listing_key {row['listing_key']} != {expected_key}")
        if row["valid_to"] and row["valid_from"] > row["valid_to"]:
            errors.append(f"listings:{line}: valid_from is after valid_to")
        if row["current"] == "true":
            current_by_key.setdefault(row["listing_key"], []).append(row)
            if row["status"] != "active" or row["valid_to"]:
                errors.append(f"listings:{line}: current listing must be active with blank valid_to")
            instrument = instrument_by_id.get(row["instrument_id"])
            if instrument and instrument["status"] != "active":
                errors.append(f"listings:{line}: current listing references non-active instrument")
    for key, rows in current_by_key.items():
        if len(rows) != 1:
            errors.append(f"listings: current lifecycle count for {key} is {len(rows)}")

    if compatibility_listings and compatibility_listings.exists():
        _, compatibility = read_csv(compatibility_listings)
        expected_keys = {row.get("listing_key") or f"{row.get('exchange','')}::{row.get('ticker','')}" for row in compatibility}
        actual_keys = set(current_by_key)
        if expected_keys != actual_keys:
            errors.append(f"listings: current keyset mismatch; missing={len(expected_keys-actual_keys)}, extra={len(actual_keys-expected_keys)}")

    isin_to_instruments: dict[str, set[str]] = {}
    for row in tables.get("instruments", []):
        if row["isin"]:
            isin_to_instruments.setdefault(row["isin"], set()).add(row["instrument_id"])
    for isin, ids in isin_to_instruments.items():
        if len(ids) != 1:
            errors.append(f"instruments: ISIN {isin} maps to {len(ids)} instrument IDs")

    entity_tables = {"issuer": "issuers", "instrument": "instruments", "listing": "listings"}
    entity_ids = {
        kind: {row[f"{kind}_id"] for row in tables.get(table, [])}
        for kind, table in entity_tables.items()
    }
    for table in ("identifier_assertions", "field_assertions", "provenance_gaps"):
        for line, row in enumerate(tables.get(table, []), start=2):
            kind = row.get("entity_type", "")
            if kind not in entity_ids or row.get("entity_id", "") not in entity_ids.get(kind, set()):
                errors.append(f"{table}:{line}: unknown {kind} entity_id {row.get('entity_id','')}")

    identifier_statuses = {
        "accepted", "provisional", "quarantined_identity_conflict",
        "quarantined_entity_identifier_conflict",
    }
    non_quarantined_values: dict[tuple[str, str, str], set[str]] = {}
    for line, row in enumerate(tables.get("identifier_assertions", []), start=2):
        scheme = row.get("scheme", "").upper()
        value = row.get("value", "")
        status = row.get("adjudication_status", "")
        if status not in identifier_statuses:
            errors.append(f"identifier_assertions:{line}: unknown adjudication_status {status!r}")
        if not status.startswith("quarantined"):
            key = (row.get("entity_type", ""), row.get("entity_id", ""), scheme)
            non_quarantined_values.setdefault(key, set()).add(value)
        if scheme == "ISIN" and not valid_isin(value):
            errors.append(f"identifier_assertions:{line}: invalid ISIN checksum")
        if scheme == "LEI" and not LEI_RE.fullmatch(value):
            errors.append(f"identifier_assertions:{line}: invalid LEI structure")
        if scheme == "ISIN" and row.get("entity_type") == "instrument":
            instrument = instrument_by_id.get(row.get("entity_id", ""))
            canonical_isin = instrument.get("isin", "") if instrument else ""
            if status.startswith("quarantined") and canonical_isin:
                errors.append(
                    f"identifier_assertions:{line}: quarantined ISIN must not populate canonical instrument identity"
                )
            if status in {"accepted", "provisional"} and canonical_isin != value:
                errors.append(
                    f"identifier_assertions:{line}: {status} ISIN differs from canonical instrument value"
                )


    for key, values in non_quarantined_values.items():
        if len(values) > 1:
            errors.append(
                f"identifier_assertions: non-quarantined {key} has multiple values: {sorted(values)}"
            )

    for line, row in enumerate(tables.get("listing_events", []), start=2):
        listing = listing_by_id.get(row["listing_id"])
        if listing and listing["listing_key"] != row["listing_key"]:
            errors.append(f"listing_events:{line}: listing_key disagrees with listing_id")

    for line, row in enumerate(tables.get("coverage_contracts", []), start=2):
        denominator = int(row["denominator"])
        covered = int(row["covered_reference_keys"])
        missing = int(row["missing_reference_keys"])
        if denominator != covered + missing:
            errors.append(f"coverage_contracts:{line}: denominator != covered + missing")
        if covered > denominator:
            errors.append(f"coverage_contracts:{line}: covered exceeds denominator")

    for line, source in enumerate(tables.get("sources", []), start=2):
        if source["license_status"] == "verified_open":
            approved, reason = source_license_approved(source)
            if not approved:
                errors.append(f"sources:{line}: verified_open source lacks evidence: {reason}")

    manifest_path = data_dir / "manifest.json"
    manifest: Mapping[str, Any] = {}
    if not manifest_path.exists():
        errors.append("manifest.json is missing")
    else:
        try:
            parsed_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"manifest.json is invalid JSON: {exc}")
            parsed_manifest = {}
        if not isinstance(parsed_manifest, dict):
            errors.append("manifest.json must contain an object")
        else:
            manifest = parsed_manifest

        manifest_commit = str(manifest.get("git_commit", "")).lower()
        if not GIT_SHA_RE.fullmatch(manifest_commit):
            errors.append("manifest git_commit is not a full 40-character SHA")
        if expected_git_commit:
            expected = expected_git_commit.strip().lower()
            if not GIT_SHA_RE.fullmatch(expected):
                errors.append("expected_git_commit is not a full 40-character SHA")
            elif manifest_commit != expected:
                errors.append(f"manifest git_commit {manifest_commit} != expected {expected}")
        if str(manifest.get("version", "")) != str(contract.get("version", "")):
            errors.append("manifest version differs from schema contract version")
        if parse_time(str(manifest.get("generated_at", ""))) is None:
            errors.append("manifest generated_at is not timezone-aware ISO-8601")

        if compatibility_listings and compatibility_listings.exists():
            if manifest.get("source_dataset_sha256") != sha256_file(compatibility_listings):
                errors.append("manifest source_dataset_sha256 does not match compatibility listings")
        if manifest.get("schema_contract_sha256") != sha256_file(contract_json):
            errors.append("manifest schema_contract_sha256 mismatch")
        if not schema_sql.exists() or manifest.get("schema_sql_sha256") != sha256_file(schema_sql):
            errors.append("manifest schema_sql_sha256 mismatch")

        expected_names = [f"{table}.csv" for table in tables_spec]
        raw_entries = manifest.get("files", [])
        if not isinstance(raw_entries, list):
            errors.append("manifest files must be an array")
            entries: list[Mapping[str, Any]] = []
        else:
            entries = [item for item in raw_entries if isinstance(item, dict)]
            if len(entries) != len(raw_entries):
                errors.append("manifest files contains a non-object entry")
        manifest_names = [str(item.get("path", "")) for item in entries]
        if manifest_names != expected_names:
            errors.append(f"manifest file order/set differs: expected {expected_names}, got {manifest_names}")
        if len(set(manifest_names)) != len(manifest_names):
            errors.append("manifest contains duplicate file entries")

        aggregate_items: list[str] = []
        for item in entries:
            name = str(item.get("path", ""))
            if not name or Path(name).is_absolute() or ".." in Path(name).parts:
                errors.append(f"manifest contains unsafe path: {name!r}")
                continue
            path = data_dir / name
            if not path.exists():
                errors.append(f"manifest file is missing: {name}")
                continue
            actual_hash = sha256_file(path)
            declared_hash = str(item.get("sha256", ""))
            if not SHA256_RE.fullmatch(declared_hash):
                errors.append(f"manifest hash is malformed for {name}")
            if actual_hash != declared_hash:
                errors.append(f"manifest hash mismatch for {name}")
            table = name.removesuffix(".csv")
            try:
                declared_rows = int(item.get("rows", -1))
            except (TypeError, ValueError):
                declared_rows = -1
            actual_rows = len(tables.get(table, []))
            if declared_rows != actual_rows:
                errors.append(f"manifest row count mismatch for {name}: {declared_rows} != {actual_rows}")
            aggregate_items.append(f"{name}:{actual_hash}")
        aggregate = hashlib.sha256("\n".join(aggregate_items).encode("utf-8")).hexdigest()
        if aggregate != manifest.get("aggregate_sha256"):
            errors.append("manifest aggregate_sha256 mismatch")

        expected_counts = {table: len(tables.get(table, [])) for table in tables_spec}
        if manifest.get("counts") != expected_counts:
            errors.append("manifest counts differ from canonical CSV row counts")

    report = {
        "status": "pass" if not errors else "fail",
        "errors": errors,
        "error_count": len(errors),
        "counts": {table: len(rows) for table, rows in tables.items()},
    }
    if errors:
        raise ValueError("canonical-v4 validation failed:\n" + "\n".join(errors[:100]))
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--contract-json", type=Path, default=CONTRACT_JSON)
    parser.add_argument("--schema-sql", type=Path, default=SCHEMA_SQL)
    parser.add_argument("--compatibility-listings", type=Path, default=COMPATIBILITY_LISTINGS)
    parser.add_argument("--expected-git-commit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = validate(
        data_dir=args.data_dir, contract_json=args.contract_json, schema_sql=args.schema_sql,
        compatibility_listings=args.compatibility_listings,
        expected_git_commit=args.expected_git_commit,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
