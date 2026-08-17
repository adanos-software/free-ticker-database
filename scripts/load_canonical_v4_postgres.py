"""Load validated canonical-v4 CSVs into PostgreSQL and verify row counts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from scripts.validate_canonical_v4_exports import validate
except ModuleNotFoundError:  # pragma: no cover
    from validate_canonical_v4_exports import validate

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "data" / "canonical_v4"
SCHEMA_SQL = ROOT / "schema" / "canonical_v4.sql"
CONTRACT_JSON = ROOT / "schema" / "canonical_v4_contract.json"
TABLE_ORDER = [
    "sources", "source_observations", "venues", "issuers", "instruments", "listings",
    "identifier_assertions", "field_assertions", "provenance_gaps", "listing_events",
    "coverage_contracts",
]


def load(
    *,
    dsn: str,
    data_dir: Path = DEFAULT_DIR,
    schema_sql: Path = SCHEMA_SQL,
    contract_json: Path = CONTRACT_JSON,
    reset: bool = True,
    expected_git_commit: str | None = None,
) -> dict[str, Any]:
    validate(
        data_dir=data_dir,
        contract_json=contract_json,
        schema_sql=schema_sql,
        expected_git_commit=expected_git_commit,
    )
    try:
        import psycopg
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Install requirements-dev.txt to load canonical v4 into PostgreSQL") from exc

    contract = json.loads(contract_json.read_text(encoding="utf-8"))
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            if reset:
                for table in reversed(TABLE_ORDER):
                    cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
            cursor.execute(schema_sql.read_text(encoding="utf-8"))
            for table in TABLE_ORDER:
                columns = list(contract["tables"][table]["columns"])
                quoted = ", ".join(f'"{column}"' for column in columns)
                statement = f'COPY "{table}" ({quoted}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
                with cursor.copy(statement) as copy:
                    with (data_dir / f"{table}.csv").open("r", encoding="utf-8", newline="") as handle:
                        while chunk := handle.read(1024 * 1024):
                            copy.write(chunk)
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                counts[table] = int(cursor.fetchone()[0])
        connection.commit()
    manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
    for table, expected in manifest.get("counts", {}).items():
        if counts.get(table) != expected:
            raise RuntimeError(f"PostgreSQL row count mismatch for {table}: {counts.get(table)} != {expected}")
    return {"status": "pass", "counts": counts}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--schema-sql", type=Path, default=SCHEMA_SQL)
    parser.add_argument("--contract-json", type=Path, default=CONTRACT_JSON)
    parser.add_argument("--no-reset", action="store_true")
    parser.add_argument("--expected-git-commit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    print(json.dumps(load(
        dsn=args.dsn, data_dir=args.data_dir, schema_sql=args.schema_sql,
        contract_json=args.contract_json, reset=not args.no_reset,
        expected_git_commit=args.expected_git_commit,
    ), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
