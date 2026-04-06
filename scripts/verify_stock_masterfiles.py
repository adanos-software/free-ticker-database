from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.listing_keys import row_listing_key
from scripts.rebuild_dataset import alias_matches_company


DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
DEFAULT_OUTPUT_DIR = DATA_DIR / "stock_verification"
BAD_STATUSES = {
    "asset_type_mismatch",
    "name_mismatch",
    "missing_from_official",
    "non_active_official",
}
LOW_CONFIDENCE_MISSING_EXCHANGES = {"OTC"}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_stock_rows(path: Path = TICKERS_CSV) -> list[dict[str, str]]:
    rows = load_csv(path)
    stocks = [row for row in rows if row.get("asset_type") == "Stock"]
    return sorted(stocks, key=lambda row: (row["exchange"], row["ticker"]))


def load_identifier_map(path: Path = IDENTIFIERS_EXTENDED_CSV) -> dict[str, dict[str, str]]:
    return {row_listing_key(row): row for row in load_csv(path)}


def select_chunk(
    rows: list[dict[str, str]],
    *,
    chunk_index: int,
    chunk_count: int,
) -> list[dict[str, str]]:
    if chunk_count < 1:
        raise ValueError("chunk_count must be at least 1")
    if chunk_index < 1 or chunk_index > chunk_count:
        raise ValueError("chunk_index must be between 1 and chunk_count")
    return [row for idx, row in enumerate(rows) if idx % chunk_count == chunk_index - 1]


def chunk_stem(chunk_index: int, chunk_count: int) -> str:
    return f"chunk-{chunk_index:02d}-of-{chunk_count:02d}"


def load_reference_maps(
    path: Path = MASTERFILE_REFERENCE_CSV,
) -> tuple[
    dict[tuple[str, str], list[dict[str, str]]],
    dict[tuple[str, str], list[dict[str, str]]],
    dict[str, list[dict[str, str]]],
    set[str],
    set[str],
]:
    active_by_key: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    any_by_key: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    active_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    covered_exchanges: set[str] = set()
    partial_covered_exchanges: set[str] = set()

    for row in load_csv(path):
        if row.get("official") != "true":
            continue
        reference_scope = row.get("reference_scope", "")
        if not reference_scope or reference_scope == "manual":
            continue
        exchange = row["exchange"]
        ticker = row["ticker"]
        key = (exchange, ticker)
        any_by_key[key].append(row)
        if reference_scope == "exchange_directory":
            covered_exchanges.add(exchange)
        else:
            partial_covered_exchanges.add(exchange)
        if row.get("listing_status") != "active":
            continue
        active_by_key[key].append(row)
        active_by_ticker[ticker].append(row)

    return (
        dict(active_by_key),
        dict(any_by_key),
        dict(active_by_ticker),
        covered_exchanges,
        partial_covered_exchanges - covered_exchanges,
    )


def is_code_like_reference_name(name: str, ticker: str) -> bool:
    compact_name = "".join(character for character in name.lower() if character.isalnum())
    compact_ticker = "".join(character for character in ticker.lower() if character.isalnum())
    return compact_name == compact_ticker


def choose_preferred_reference(rows: list[dict[str, str]], ticker: str) -> dict[str, str]:
    def rank(row: dict[str, str]) -> tuple[int, int, int, str]:
        return (
            int(row.get("source_key") != "sec_company_tickers_exchange"),
            int(not is_code_like_reference_name(row.get("name", ""), ticker)),
            len(row.get("name", "")),
            row.get("source_key", ""),
        )

    return max(rows, key=rank)


def classify_row(
    row: dict[str, str],
    *,
    active_by_key: dict[tuple[str, str], list[dict[str, str]]],
    any_by_key: dict[tuple[str, str], list[dict[str, str]]],
    active_by_ticker: dict[str, list[dict[str, str]]],
    covered_exchanges: set[str],
    partial_covered_exchanges: set[str],
    identifier_map: dict[str, dict[str, str]],
) -> dict[str, Any]:
    ticker = row["ticker"]
    exchange = row["exchange"]
    key = (exchange, ticker)
    listing_key = row_listing_key(row)
    identifiers = identifier_map.get(listing_key, {})
    active_reference_rows = active_by_key.get(key, [])
    status = "reference_gap"
    reason = "No official exchange directory is wired for this exchange yet."
    reference_name = ""
    reference_source = ""

    if active_reference_rows:
        preferred_reference = choose_preferred_reference(active_reference_rows, ticker)
        reference_name = preferred_reference.get("name", "")
        reference_source = preferred_reference.get("source_key", "")
        same_type_rows = [candidate for candidate in active_reference_rows if candidate.get("asset_type") == row.get("asset_type")]
        if same_type_rows:
            if any(alias_matches_company(candidate.get("name", ""), row["name"]) for candidate in same_type_rows):
                status = "verified"
                reason = "Matched active official exchange directory listing."
            elif all(is_code_like_reference_name(candidate.get("name", ""), ticker) for candidate in same_type_rows):
                status = "verified"
                reason = "Official directory name is a compact trading label for this listing."
            else:
                preferred_reference = choose_preferred_reference(same_type_rows, ticker)
                reference_name = preferred_reference.get("name", "")
                reference_source = preferred_reference.get("source_key", "")
                status = "name_mismatch"
                reason = "Dataset name does not match official exchange directory name."
        elif any(candidate.get("source_key") != "sec_company_tickers_exchange" for candidate in active_reference_rows):
            status = "asset_type_mismatch"
            reason = f"Dataset asset_type={row.get('asset_type')} but official reference says {preferred_reference.get('asset_type')}."
        else:
            status = "reference_gap"
            reason = "Only low-confidence asset_type evidence exists for this listing."
    elif exchange in covered_exchanges:
        if exchange in LOW_CONFIDENCE_MISSING_EXCHANGES:
            status = "reference_gap"
            reason = "This exchange is only weakly covered by the current official reference layer."
        else:
            non_active_rows = any_by_key.get(key, [])
            non_active_row = next((candidate for candidate in non_active_rows if candidate.get("listing_status") != "active"), None)
            peers = [peer for peer in active_by_ticker.get(ticker, []) if peer["exchange"] != exchange]
            if non_active_row and non_active_row.get("listing_status") != "active":
                status = "non_active_official"
                reason = f"Official exchange directory marks this symbol as {non_active_row.get('listing_status')}."
                reference_name = non_active_row.get("name", "")
                reference_source = non_active_row.get("source_key", "")
            elif peers:
                status = "cross_exchange_collision"
                peer_preview = ", ".join(sorted({peer['exchange'] for peer in peers})[:4])
                reason = f"Official directory uses this ticker on other exchange(s): {peer_preview}."
            else:
                status = "missing_from_official"
                reason = "Symbol is absent from the active official exchange directory for this exchange."
    elif exchange in partial_covered_exchanges:
        status = "reference_gap"
        reason = "This exchange is only partially covered by the current official reference layer."

    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "asset_type": row["asset_type"],
        "name": row["name"],
        "country": row["country"],
        "country_code": row["country_code"],
        "isin": row["isin"],
        "sector": row["sector"],
        "status": status,
        "reason": reason,
        "official_reference_name": reference_name,
        "official_reference_source": reference_source,
        "covered_by_official_directory": exchange in covered_exchanges,
        "cik": identifiers.get("cik", ""),
        "figi": identifiers.get("figi", ""),
        "lei": identifiers.get("lei", ""),
    }


def summarize_results(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(row["status"] for row in rows)
    bad_examples = [row for row in rows if row["status"] in BAD_STATUSES][:25]
    return {
        "items": len(rows),
        "status_counts": dict(sorted(status_counts.items())),
        "finding_examples": bad_examples,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify all stock listings against official exchange masterfiles.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--chunk-index", type=int, required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_stock_rows(args.tickers_csv)
    rows = select_chunk(rows, chunk_index=args.chunk_index, chunk_count=args.chunk_count)
    if args.limit is not None:
        rows = rows[: args.limit]

    active_by_key, any_by_key, active_by_ticker, covered_exchanges, partial_covered_exchanges = load_reference_maps(
        args.reference_csv
    )
    identifier_map = load_identifier_map(args.identifiers_csv)
    results = [
        classify_row(
            row,
            active_by_key=active_by_key,
            any_by_key=any_by_key,
            active_by_ticker=active_by_ticker,
            covered_exchanges=covered_exchanges,
            partial_covered_exchanges=partial_covered_exchanges,
            identifier_map=identifier_map,
        )
        for row in rows
    ]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = chunk_stem(args.chunk_index, args.chunk_count)
    json_out = args.output_dir / f"{stem}.json"
    csv_out = args.output_dir / f"{stem}.csv"
    summary_out = args.output_dir / f"{stem}.summary.json"
    json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(csv_out, results)
    summary = {
        "chunk_index": args.chunk_index,
        "chunk_count": args.chunk_count,
        **summarize_results(results),
        "json_out": display_path(json_out),
        "csv_out": display_path(csv_out),
    }
    summary_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
