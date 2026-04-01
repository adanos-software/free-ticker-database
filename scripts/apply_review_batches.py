from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_pr_review_batches import DEFAULT_PR_BATCH_DIR
from scripts.rebuild_dataset import ALIASES_CSV, IDENTIFIERS_CSV, TICKERS_CSV, rebuild, split_aliases


DEFAULT_APPLY_SUMMARY = DEFAULT_PR_BATCH_DIR / "apply_summary.json"


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def operation_key(operation: dict[str, object]) -> tuple[object, ...]:
    return (
        operation.get("operation_type"),
        operation.get("ticker"),
        operation.get("exchange"),
        operation.get("alias"),
        operation.get("field"),
        operation.get("decision"),
        operation.get("proposed_value"),
    )


def resolve_batch_files(
    *,
    manifest_path: Path | None,
    batch_file: Path | None,
    batch_dir: Path | None,
) -> list[Path]:
    if batch_file:
        return [batch_file]
    if manifest_path and manifest_path.exists():
        manifest = load_json(manifest_path)
        return [ROOT / entry["file"] if not Path(entry["file"]).is_absolute() else Path(entry["file"]) for entry in manifest["batches"]]
    if batch_dir and batch_dir.exists():
        return sorted(path for path in batch_dir.glob("pr-batch-*.json") if path.is_file())
    return []


def load_operations(batch_files: list[Path]) -> tuple[list[dict[str, object]], list[str]]:
    operations: list[dict[str, object]] = []
    sources: list[str] = []
    seen: set[tuple[object, ...]] = set()
    for path in batch_files:
        payload = load_json(path)
        sources.append(display_path(path))
        for operation in payload.get("operations", []):
            key = operation_key(operation)
            if key in seen:
                continue
            seen.add(key)
            operations.append(operation)
    return operations, sources


def apply_operations(
    ticker_rows: list[dict[str, str]],
    alias_rows: list[dict[str, str]],
    identifier_rows: list[dict[str, str]],
    operations: list[dict[str, object]],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, object]]:
    tickers = [dict(row) for row in deepcopy(ticker_rows)]
    aliases = [dict(row) for row in deepcopy(alias_rows)]
    identifiers = [dict(row) for row in deepcopy(identifier_rows)]

    ticker_lookup = {row["ticker"]: row for row in tickers}
    identifier_lookup = {row["ticker"]: row for row in identifiers}

    applied: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    for operation in operations:
        ticker = str(operation["ticker"])
        exchange = str(operation.get("exchange", ""))
        ticker_row = ticker_lookup.get(ticker)

        if not ticker_row:
            skipped.append({**operation, "skip_reason": "ticker_not_found"})
            continue
        if exchange and ticker_row.get("exchange") != exchange:
            skipped.append({**operation, "skip_reason": "exchange_mismatch"})
            continue

        op_type = operation["operation_type"]
        if op_type == "remove_alias":
            alias_value = str(operation["alias"])
            before_alias_rows = len(aliases)
            aliases = [
                row
                for row in aliases
                if not (row["ticker"] == ticker and row["alias"] == alias_value)
            ]
            current_aliases = split_aliases(ticker_row.get("aliases", ""))
            updated_aliases = [alias for alias in current_aliases if alias != alias_value]

            if before_alias_rows == len(aliases) and current_aliases == updated_aliases:
                skipped.append({**operation, "skip_reason": "alias_not_found"})
                continue

            ticker_row["aliases"] = "|".join(updated_aliases)
            applied.append(operation)
            continue

        if op_type == "update_metadata":
            field = str(operation["field"])
            decision = str(operation["decision"])
            new_value = "" if decision == "clear" else str(operation.get("proposed_value", ""))

            if field == "isin":
                ticker_row["isin"] = new_value
                identifier_row = identifier_lookup.get(ticker)
                if identifier_row is None:
                    identifier_row = {"ticker": ticker, "isin": "", "wkn": ""}
                    identifiers.append(identifier_row)
                    identifier_lookup[ticker] = identifier_row
                identifier_row["isin"] = new_value
            else:
                if field not in ticker_row:
                    skipped.append({**operation, "skip_reason": "unsupported_field"})
                    continue
                ticker_row[field] = new_value

            applied.append(operation)
            continue

        if op_type == "drop_entry":
            tickers = [row for row in tickers if row["ticker"] != ticker]
            aliases = [row for row in aliases if row["ticker"] != ticker]
            identifiers = [row for row in identifiers if row["ticker"] != ticker]
            ticker_lookup.pop(ticker, None)
            identifier_lookup.pop(ticker, None)
            applied.append(operation)
            continue

        skipped.append({**operation, "skip_reason": "unknown_operation_type"})

    summary = {
        "requested_operations": len(operations),
        "applied_operations": len(applied),
        "skipped_operations": len(skipped),
        "applied_by_type": dict(sorted(Counter(op["operation_type"] for op in applied).items())),
        "skipped_by_reason": dict(sorted(Counter(op["skip_reason"] for op in skipped).items())),
        "affected_tickers": sorted({op["ticker"] for op in applied}),
        "applied": applied,
        "skipped": skipped,
    }
    return tickers, aliases, identifiers, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply PR review batches to the dataset source CSVs.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_PR_BATCH_DIR / "manifest.json")
    parser.add_argument("--batch-file", type=Path, default=None)
    parser.add_argument("--batch-dir", type=Path, default=DEFAULT_PR_BATCH_DIR)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=IDENTIFIERS_CSV)
    parser.add_argument("--summary-out", type=Path, default=DEFAULT_APPLY_SUMMARY)
    parser.add_argument("--execute", action="store_true", help="Write changes back to CSVs.")
    parser.add_argument("--skip-rebuild", action="store_true", help="When executing, do not regenerate derived artifacts.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_files = resolve_batch_files(
        manifest_path=args.manifest,
        batch_file=args.batch_file,
        batch_dir=args.batch_dir,
    )
    operations, sources = load_operations(batch_files)

    ticker_rows = load_csv(args.tickers_csv)
    alias_rows = load_csv(args.aliases_csv)
    identifier_rows = load_csv(args.identifiers_csv)
    updated_tickers, updated_aliases, updated_identifiers, summary = apply_operations(
        ticker_rows,
        alias_rows,
        identifier_rows,
        operations,
    )

    if args.execute:
        write_csv(
            args.tickers_csv,
            ["ticker", "name", "exchange", "asset_type", "sector", "country", "country_code", "isin", "aliases"],
            updated_tickers,
        )
        write_csv(args.aliases_csv, ["ticker", "alias", "alias_type"], updated_aliases)
        write_csv(args.identifiers_csv, ["ticker", "isin", "wkn"], updated_identifiers)
        if not args.skip_rebuild:
            rebuild()

    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_meta": {
            "sources": sources,
            "execute": args.execute,
            "skip_rebuild": args.skip_rebuild,
            "tickers_csv": display_path(args.tickers_csv),
            "aliases_csv": display_path(args.aliases_csv),
            "identifiers_csv": display_path(args.identifiers_csv),
        },
        "summary": summary,
    }
    args.summary_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "batch_files": sources,
                "execute": args.execute,
                "skip_rebuild": args.skip_rebuild,
                "requested_operations": summary["requested_operations"],
                "applied_operations": summary["applied_operations"],
                "skipped_operations": summary["skipped_operations"],
                "summary_out": display_path(args.summary_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
