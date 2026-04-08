from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.listing_keys import row_listing_key
from scripts.rebuild_dataset import alias_matches_company, normalize_tokens


DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
DEFAULT_OUTPUT_DIR_BY_ASSET_TYPE = {
    "Stock": DATA_DIR / "stock_verification",
    "ETF": DATA_DIR / "etf_verification",
}
BAD_STATUSES = {
    "asset_type_mismatch",
    "name_mismatch",
    "missing_from_official",
    "non_active_official",
}
LOW_CONFIDENCE_MISSING_EXCHANGES = {
    "AMS",
    "B3",
    "BATS",
    "Euronext",
    "NASDAQ",
    "NYSE",
    "NYSE ARCA",
    "NYSE MKT",
    "OSL",
    "OTC",
}
LOW_CONFIDENCE_MISSING_ASSET_TYPE_KEYS = {
    ("TWSE", "ETF"),
    ("TPEX", "ETF"),
}
LOW_CONFIDENCE_ASSET_TYPE_SOURCE_KEYS = {
    "krx_listed_companies",
    "lse_company_reports",
    "tmx_listed_issuers",
}
LOW_CONFIDENCE_COLLISION_PEER_EXCHANGES = {
    "ASX",
    "Euronext",
    "LSE",
    "OSL",
    "OTC",
    "TSE",
    "XETRA",
}
LOW_CONFIDENCE_COLLISION_SOURCE_KEYS = {
    "krx_listed_companies",
    "lse_company_reports",
    "sec_company_tickers_exchange",
    "tmx_interlisted_companies",
}
LOCAL_LANGUAGE_NAME_MATCH_EXCHANGES = {"KRX", "SSE", "SZSE", "TWSE", "TPEX"}
LOW_CONFIDENCE_NAME_SOURCE_BY_EXCHANGE = {
    "KRX": {"krx_etf_finder", "krx_listed_companies"},
    "KOSDAQ": {"krx_listed_companies"},
    "LSE": {"lse_company_reports", "lse_instrument_search", "lse_instrument_directory"},
    "NASDAQ": {"sec_company_tickers_exchange"},
    "NYSE": {"sec_company_tickers_exchange", "nasdaq_other_listed"},
    "NYSE MKT": {"sec_company_tickers_exchange", "nasdaq_other_listed"},
    "OTC": {"sec_company_tickers_exchange"},
    "TPEX": {"tpex_mainboard_daily_quotes"},
    "TSX": {"tmx_interlisted_companies", "tmx_listed_issuers"},
    "TSXV": {"tmx_interlisted_companies", "tmx_listed_issuers"},
    "XETRA": {"deutsche_boerse_xetra_all_tradable_equities"},
}
ABBREVIATED_OFFICIAL_LABEL_EXCHANGES = {"KRX", "KOSDAQ"}
TMX_ROOT_SUFFIX_ETF_EXCHANGES = {"TSX", "TSXV"}
EURONEXT_LABEL_SPLIT_RE = re.compile(r"[\s./-]+")
ETFISH_REFERENCE_MARKERS = (
    " etf",
    " etn",
    "exchange-traded note",
    "exchange traded note",
    "exchange-traded notes",
    "exchange traded notes",
    "trust shares",
    "shares of beneficial interest",
)
GENERIC_ETF_PLACEHOLDER_NAMES = {
    "common units",
    "shares of beneficial interest",
}


def has_non_latin_name(name: str) -> bool:
    return any(ord(character) > 127 and character.isalpha() for character in name)


def has_strong_company_name_match(left: str, right: str) -> bool:
    if not alias_matches_company(left, right):
        return False
    left_compact = re.sub(r"[^a-z0-9]+", "", left.lower())
    right_compact = re.sub(r"[^a-z0-9]+", "", right.lower())
    if left_compact and right_compact and (
        left_compact in right_compact or right_compact in left_compact
    ):
        return True
    return len(normalize_tokens(left) & normalize_tokens(right)) >= 2


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_asset_rows(path: Path = LISTINGS_CSV, *, asset_type: str = "Stock") -> list[dict[str, str]]:
    rows = load_csv(path)
    filtered = [row for row in rows if row.get("asset_type") == asset_type]
    return sorted(filtered, key=lambda row: (row["exchange"], row["ticker"]))


def load_stock_rows(path: Path = LISTINGS_CSV) -> list[dict[str, str]]:
    return load_asset_rows(path, asset_type="Stock")


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


def is_low_confidence_euronext_label(name: str) -> bool:
    letters = [character for character in name if character.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(character.isupper() for character in letters) / len(letters)
    tokens = [token for token in EURONEXT_LABEL_SPLIT_RE.split(name) if token]
    return upper_ratio >= 0.85 and (len(name) <= 24 or len(tokens) <= 3)


def looks_like_etfish_reference_name(name: str) -> bool:
    lowered = f" {name.lower().strip()} "
    return any(marker in lowered for marker in ETFISH_REFERENCE_MARKERS)


def is_generic_etf_placeholder_name(name: str) -> bool:
    return name.lower().strip() in GENERIC_ETF_PLACEHOLDER_NAMES


def choose_preferred_reference(rows: list[dict[str, str]], ticker: str) -> dict[str, str]:
    def rank(row: dict[str, str]) -> tuple[int, int, int, str]:
        return (
            int(row.get("source_key") != "sec_company_tickers_exchange"),
            int(not is_code_like_reference_name(row.get("name", ""), ticker)),
            len(row.get("name", "")),
            row.get("source_key", ""),
        )

    return max(rows, key=rank)


def load_tmx_root_reference_rows(
    row: dict[str, str],
    active_by_key: dict[tuple[str, str], list[dict[str, str]]],
) -> list[dict[str, str]]:
    if row.get("asset_type") != "ETF":
        return []
    exchange = row.get("exchange", "")
    ticker = row.get("ticker", "")
    if exchange not in TMX_ROOT_SUFFIX_ETF_EXCHANGES or "-" not in ticker:
        return []
    root_ticker = ticker.split("-", 1)[0]
    return [
        candidate
        for candidate in active_by_key.get((exchange, root_ticker), [])
        if candidate.get("asset_type") == row.get("asset_type")
    ]


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
            source_keys = {candidate.get("source_key", "") for candidate in same_type_rows}
            if any(alias_matches_company(candidate.get("name", ""), row["name"]) for candidate in same_type_rows):
                status = "verified"
                reason = "Matched active official exchange directory listing."
            elif (
                exchange in LOCAL_LANGUAGE_NAME_MATCH_EXCHANGES
                and all(has_non_latin_name(candidate.get("name", "")) for candidate in same_type_rows)
            ):
                status = "verified"
                reason = "Matched active official listing with a local-language issuer name."
            elif all(is_code_like_reference_name(candidate.get("name", ""), ticker) for candidate in same_type_rows):
                status = "verified"
                reason = "Official directory name is a compact trading label for this listing."
            elif (
                exchange == "Euronext"
                and source_keys == {"euronext_equities"}
                and all(is_low_confidence_euronext_label(candidate.get("name", "")) for candidate in same_type_rows)
            ):
                status = "reference_gap"
                reason = "Official Euronext feed only exposes a trading label rather than a reliable full issuer name."
            elif (
                row.get("asset_type") == "ETF"
                and all(is_generic_etf_placeholder_name(candidate.get("name", "")) for candidate in same_type_rows)
            ):
                status = "reference_gap"
                reason = "Official reference only exposes a generic ETP placeholder name."
            elif (
                row.get("asset_type") == "Stock"
                and exchange in ABBREVIATED_OFFICIAL_LABEL_EXCHANGES
                and source_keys == {"krx_listed_companies"}
            ):
                status = "verified"
                reason = "Matched active official listing; official directory uses a compact issuer label."
            elif source_keys and source_keys <= LOW_CONFIDENCE_NAME_SOURCE_BY_EXCHANGE.get(exchange, set()):
                status = "reference_gap"
                reason = "Only low-confidence issuer reference evidence exists for this listing."
            else:
                preferred_reference = choose_preferred_reference(same_type_rows, ticker)
                reference_name = preferred_reference.get("name", "")
                reference_source = preferred_reference.get("source_key", "")
                status = "name_mismatch"
                reason = "Dataset name does not match official exchange directory name."
        elif row.get("asset_type") == "ETF" and any(
            looks_like_etfish_reference_name(candidate.get("name", "")) for candidate in active_reference_rows
        ):
            status = "verified"
            reason = "Official directory labels this ETP-like listing as stock, but the issuer name clearly identifies an ETF/ETN trust line."
        elif (
            exchange == "Euronext"
            and row.get("asset_type") == "ETF"
            and looks_like_etfish_reference_name(row.get("name", ""))
            and all(not looks_like_etfish_reference_name(candidate.get("name", "")) for candidate in active_reference_rows)
        ):
            status = "reference_gap"
            reason = "Grouped Euronext feed resolves this ticker to a stock line on another venue."
        elif {
            candidate.get("source_key", "") for candidate in active_reference_rows
        } <= LOW_CONFIDENCE_ASSET_TYPE_SOURCE_KEYS:
            status = "reference_gap"
            reason = "Only low-confidence asset_type evidence exists for this listing."
        elif any(candidate.get("source_key") != "sec_company_tickers_exchange" for candidate in active_reference_rows):
            status = "asset_type_mismatch"
            reason = f"Dataset asset_type={row.get('asset_type')} but official reference says {preferred_reference.get('asset_type')}."
        else:
            status = "reference_gap"
            reason = "Only low-confidence asset_type evidence exists for this listing."
    else:
        tmx_root_reference_rows = load_tmx_root_reference_rows(row, active_by_key)
        if tmx_root_reference_rows:
            preferred_reference = choose_preferred_reference(tmx_root_reference_rows, ticker)
            reference_name = preferred_reference.get("name", "")
            reference_source = preferred_reference.get("source_key", "")
            if any(
                has_strong_company_name_match(candidate.get("name", ""), row["name"])
                or has_strong_company_name_match(row["name"], candidate.get("name", ""))
                for candidate in tmx_root_reference_rows
            ):
                status = "verified"
                reason = "Matched active official TMX root listing; official workbook omits this ETF series suffix."
            else:
                status = "reference_gap"
                reason = "Only an official TMX root listing exists for this ETF series suffix."
        elif exchange in covered_exchanges:
            non_active_rows = any_by_key.get(key, [])
            non_active_row = next((candidate for candidate in non_active_rows if candidate.get("listing_status") != "active"), None)
            peers = [peer for peer in active_by_ticker.get(ticker, []) if peer["exchange"] != exchange]
            if non_active_row and non_active_row.get("listing_status") != "active":
                status = "non_active_official"
                reason = f"Official exchange directory marks this symbol as {non_active_row.get('listing_status')}."
                reference_name = non_active_row.get("name", "")
                reference_source = non_active_row.get("source_key", "")
            elif peers:
                matching_peers = [
                    peer
                    for peer in peers
                    if peer.get("asset_type") == row.get("asset_type")
                    and (
                        has_strong_company_name_match(peer.get("name", ""), row["name"])
                        or has_strong_company_name_match(row["name"], peer.get("name", ""))
                    )
                ]
                if not matching_peers:
                    status = "reference_gap"
                    reason = "Only weak cross-exchange collision evidence exists for this listing."
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

                peer_exchanges = {peer["exchange"] for peer in matching_peers}
                peer_source_keys = {
                    peer.get("source_key", "") for peer in matching_peers if peer.get("source_key", "")
                }
                peer_preview = ", ".join(sorted(peer_exchanges)[:4])
                if peer_exchanges <= LOW_CONFIDENCE_COLLISION_PEER_EXCHANGES or (
                    peer_source_keys and peer_source_keys <= LOW_CONFIDENCE_COLLISION_SOURCE_KEYS
                ):
                    status = "reference_gap"
                    reason = "Only weak cross-exchange collision evidence exists for this listing."
                else:
                    status = "cross_exchange_collision"
                    reason = f"Official directory uses this ticker on other exchange(s): {peer_preview}."
            elif (exchange, row.get("asset_type", "")) in LOW_CONFIDENCE_MISSING_ASSET_TYPE_KEYS:
                status = "reference_gap"
                reason = "This asset type is not fully covered by the current official reference layer for this exchange."
            elif exchange in LOW_CONFIDENCE_MISSING_EXCHANGES:
                status = "reference_gap"
                reason = "This exchange is only weakly covered by the current official reference layer."
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
    parser.add_argument("--tickers-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--asset-type", choices=sorted(DEFAULT_OUTPUT_DIR_BY_ASSET_TYPE), default="Stock")
    parser.add_argument("--chunk-index", type=int, required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_asset_rows(args.tickers_csv, asset_type=args.asset_type)
    rows = select_chunk(rows, chunk_index=args.chunk_index, chunk_count=args.chunk_count)
    if args.limit is not None:
        rows = rows[: args.limit]
    output_dir = args.output_dir or DEFAULT_OUTPUT_DIR_BY_ASSET_TYPE[args.asset_type]

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

    output_dir.mkdir(parents=True, exist_ok=True)
    stem = chunk_stem(args.chunk_index, args.chunk_count)
    json_out = output_dir / f"{stem}.json"
    csv_out = output_dir / f"{stem}.csv"
    summary_out = output_dir / f"{stem}.summary.json"
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
