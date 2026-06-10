"""Compare a Twelve Data stocks.json snapshot against the local listing database."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path


REPORT_DIR = Path("data/reports")

STOCK_LIKE_TYPES = {
    "American Depositary Receipt",
    "Common Stock",
    "Depositary Receipt",
    "Global Depositary Receipt",
    "Limited Partnership",
    "Preferred Stock",
    "REIT",
    "Trust",
}

LOCAL_TO_TWELVE_VENUES = {
    "AMS": {"exchange": {"Euronext"}, "mic": {"XAMS"}},
    "ASX": {"exchange": {"ASX"}, "mic": {"XASX"}},
    "ATHEX": {"exchange": {"ASE"}, "mic": {"ASEX"}},
    "B3": {"exchange": {"Bovespa"}, "mic": {"BVMF"}},
    "BATS": {"exchange": {"CBOE"}, "mic": {"BATS", "BCXE"}},
    "BIST": {"exchange": {"BIST"}, "mic": {"XIST"}},
    "BME": {"exchange": {"BME"}, "mic": {"XMAD"}},
    "BMV": {"exchange": {"BMV"}, "mic": {"XMEX"}},
    "BSE_IN": {"exchange": {"BSE"}, "mic": {"XBOM"}},
    "Bursa": {"exchange": {"MYX"}, "mic": {"XKLS"}},
    "CPH": {"exchange": {"OMXC"}, "mic": {"XCSE"}},
    "CSE": {"exchange": {"CSE"}, "mic": {"XCNQ"}},
    "Euronext": {"exchange": {"Euronext"}, "mic": {"XBRU", "XPAR"}},
    "HEL": {"exchange": {"OMXH"}, "mic": {"XHEL"}},
    "HKEX": {"exchange": {"HKEX"}, "mic": {"XHKG"}},
    "IDX": {"exchange": {"IDX"}, "mic": {"XIDX"}},
    "JSE": {"exchange": {"JSE"}, "mic": {"XJSE"}},
    "KOSDAQ": {"exchange": {"KRX"}, "mic": {"XKRX"}},
    "KRX": {"exchange": {"KRX"}, "mic": {"XKRX"}},
    "LSE": {"exchange": {"LSE"}, "mic": {"AIMX", "XLON"}},
    "NASDAQ": {"exchange": {"NASDAQ"}, "mic": {"XNAS", "XNCM", "XNGS", "XNMS"}},
    "NEO": {"exchange": {"NEO"}, "mic": {"NEOE"}},
    "NSE_IN": {"exchange": {"NSE"}, "mic": {"XNSE"}},
    "NYSE": {"exchange": {"NYSE"}, "mic": {"XNYS"}},
    "NYSE ARCA": {"exchange": {"NYSE"}, "mic": {"ARCX"}},
    "NYSE MKT": {"exchange": {"NYSE"}, "mic": {"XASE"}},
    "OSL": {"exchange": {"OSE"}, "mic": {"XOSL"}},
    "OTC": {"exchange": {"OTC"}, "mic": {"EXPM", "OTCB", "OTCQ", "PINX", "PSGM"}},
    "SET": {"exchange": {"SET"}, "mic": {"XBKK"}},
    "SGX": {"exchange": {"SGX"}, "mic": {"XSES"}},
    "SIX": {"exchange": {"SIX"}, "mic": {"XSWX"}},
    "SSE": {"exchange": {"SSE"}, "mic": {"XSHG"}},
    "STO": {"exchange": {"OMX"}, "mic": {"XSTO"}},
    "SZSE": {"exchange": {"SZSE"}, "mic": {"XSHE"}},
    "TADAWUL": {"exchange": {"Tadawul"}, "mic": {"XSAU"}},
    "TASE": {"exchange": {"TASE"}, "mic": {"XTAE"}},
    "TPEX": {"exchange": {"TWSE"}, "mic": {"ROCO"}},
    "TSE": {"exchange": {"JPX"}, "mic": {"XJPX"}},
    "TSX": {"exchange": {"TSX"}, "mic": {"XTSE"}},
    "TSXV": {"exchange": {"TSXV"}, "mic": {"XTSX"}},
    "TWSE": {"exchange": {"TWSE"}, "mic": {"XTAI"}},
    "WSE": {"exchange": {"GPW"}, "mic": {"XWAR"}},
    "XETRA": {"exchange": {"XETR"}, "mic": {"XETR"}},
}


def norm_symbol(value: str) -> str:
    return (value or "").strip().upper()


def symbol_variants(symbol: str, exchange: str) -> set[str]:
    symbol = norm_symbol(symbol)
    variants = {symbol}
    if exchange == "HKEX" and symbol.isdigit():
        variants.add(symbol.zfill(5))
        variants.add(symbol.lstrip("0") or "0")
    return variants


def norm_name(value: str) -> str:
    value = (value or "").lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\b(co|corp|corporation|inc|incorporated|ltd|limited|plc|sa|ag|nv|spa|se|the)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def name_ratio(left: str, right: str) -> float:
    left_norm = norm_name(left)
    right_norm = norm_name(right)
    if not left_norm or not right_norm:
        return 0.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_summary(path: Path, summary: dict[str, object]) -> None:
    missing_by_exchange = summary["missing_stock_like_by_exchange"][:20]
    local_unmatched_by_exchange = summary["local_unmatched_by_exchange"][:20]
    lines = [
        "# Twelve Data Stock Compare",
        "",
        f"- Twelve Data rows: {summary['twelvedata_rows']:,}",
        f"- Twelve Data stock-like rows: {summary['twelvedata_stock_like_rows']:,}",
        f"- Local listing rows: {summary['local_listing_rows']:,}",
        f"- Matched Twelve Data rows: {summary['twelvedata_matched_rows']:,}",
        f"- Matched local listing keys: {summary['local_matched_listing_keys']:,}",
        f"- Unmatched Twelve Data rows: {summary['twelvedata_unmatched_rows']:,}",
        f"- Unmatched Twelve Data stock-like rows: {summary['twelvedata_stock_like_unmatched_rows']:,}",
        f"- Unmatched local listing rows: {summary['local_unmatched_listing_rows']:,}",
        f"- Low name-similarity matched rows: {summary['name_mismatch_rows']:,}",
        f"- FIGI disagreements where both sides have a FIGI: {summary['figi_mismatch_rows']:,}",
        "",
        "Twelve Data ISIN and CUSIP values in this snapshot are add-on placeholders, so this audit does not use them as identity evidence.",
        "FIGI disagreements are review-only because providers may expose different FIGI levels for the same listed security.",
        "",
        "## Top Unmatched Twelve Data Stock-Like Exchanges",
        "",
        "| Exchange | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {exchange} | {count:,} |" for exchange, count in missing_by_exchange)
    lines.extend(
        [
            "",
            "## Top Unmatched Local Exchanges",
            "",
            "| Exchange | Rows |",
            "| --- | ---: |",
        ]
    )
    lines.extend(f"| {exchange} | {count:,} |" for exchange, count in local_unmatched_by_exchange)
    lines.extend(
        [
            "",
            "## Output Files",
            "",
            "- `data/reports/twelvedata_stock_compare_summary.json`",
            "- `data/reports/twelvedata_missing_stock_like.csv`",
            "- `data/reports/twelvedata_missing_all.csv`",
            "- `data/reports/twelvedata_local_unmatched.csv`",
            "- `data/reports/twelvedata_name_mismatches.csv`",
            "- `data/reports/twelvedata_figi_mismatches.csv`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def td_matches_local_venue(td_row: dict[str, str], local_exchange: str) -> bool:
    td_exchange = td_row.get("exchange", "")
    td_mic = td_row.get("mic_code", "")
    if local_exchange in {td_exchange, td_mic}:
        return True
    venue = LOCAL_TO_TWELVE_VENUES.get(local_exchange)
    if not venue:
        return False
    return td_exchange in venue["exchange"] or td_mic in venue["mic"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("twelvedata_json", type=Path)
    parser.add_argument("--max-samples", type=int, default=5000)
    args = parser.parse_args()

    td_payload = json.loads(args.twelvedata_json.read_text(encoding="utf-8"))
    td_rows = td_payload["data"] if isinstance(td_payload, dict) else td_payload
    local_rows = read_csv(Path("data/listings.csv"))
    identifier_rows = read_csv(Path("data/listing_index.csv"))

    local_by_symbol: dict[str, list[dict[str, str]]] = defaultdict(list)
    local_by_key: dict[str, dict[str, str]] = {}
    for row in local_rows:
        for symbol in symbol_variants(row["ticker"], row["exchange"]):
            local_by_symbol[symbol].append(row)
        local_by_key[row["listing_key"]] = row

    figi_by_key = {row["listing_key"]: row.get("figi", "") for row in identifier_rows}

    matched_keys: set[str] = set()
    matched_td = 0
    matched_stock_like_td = 0
    missing_td_rows: list[dict[str, object]] = []
    name_mismatches: list[dict[str, object]] = []
    figi_mismatches: list[dict[str, object]] = []
    match_method_counts: Counter[str] = Counter()

    for td in td_rows:
        symbol = norm_symbol(td.get("symbol", ""))
        candidates = local_by_symbol.get(symbol, [])
        venue_candidates = [row for row in candidates if td_matches_local_venue(td, row["exchange"])]

        if venue_candidates:
            matched_td += 1
            if td.get("type") in STOCK_LIKE_TYPES:
                matched_stock_like_td += 1
            local = max(venue_candidates, key=lambda row: name_ratio(row["name"], td.get("name", "")))
            matched_keys.add(local["listing_key"])
            if local["exchange"] == td.get("exchange") or local["exchange"] == td.get("mic_code"):
                match_method_counts["direct_exchange_or_mic"] += 1
            else:
                match_method_counts["mapped_exchange"] += 1

            ratio = name_ratio(local["name"], td.get("name", ""))
            if ratio < 0.72:
                name_mismatches.append(
                    {
                        "listing_key": local["listing_key"],
                        "ticker": local["ticker"],
                        "local_exchange": local["exchange"],
                        "twelvedata_exchange": td.get("exchange", ""),
                        "twelvedata_mic": td.get("mic_code", ""),
                        "local_name": local["name"],
                        "twelvedata_name": td.get("name", ""),
                        "name_ratio": round(ratio, 3),
                        "twelvedata_type": td.get("type", ""),
                    }
                )

            local_figi = figi_by_key.get(local["listing_key"], "")
            td_figi = td.get("figi_code", "")
            if local_figi and td_figi and local_figi != td_figi:
                figi_mismatches.append(
                    {
                        "listing_key": local["listing_key"],
                        "ticker": local["ticker"],
                        "local_exchange": local["exchange"],
                        "twelvedata_exchange": td.get("exchange", ""),
                        "twelvedata_mic": td.get("mic_code", ""),
                        "local_name": local["name"],
                        "twelvedata_name": td.get("name", ""),
                        "local_figi": local_figi,
                        "twelvedata_figi": td_figi,
                        "twelvedata_type": td.get("type", ""),
                    }
                )
            continue

        missing_td_rows.append(
            {
                "symbol": td.get("symbol", ""),
                "name": td.get("name", ""),
                "exchange": td.get("exchange", ""),
                "mic_code": td.get("mic_code", ""),
                "country": td.get("country", ""),
                "currency": td.get("currency", ""),
                "type": td.get("type", ""),
                "figi_code": td.get("figi_code", ""),
                "stock_like": td.get("type") in STOCK_LIKE_TYPES,
                "same_symbol_local_exchanges": "|".join(sorted({row["exchange"] for row in candidates})),
            }
        )

    unmatched_local_rows = [
        {
            "listing_key": row["listing_key"],
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "name": row["name"],
            "asset_type": row["asset_type"],
            "country": row["country"],
            "isin": row["isin"],
        }
        for row in local_rows
        if row["listing_key"] not in matched_keys
    ]

    stock_like_missing = [row for row in missing_td_rows if row["stock_like"]]
    summary = {
        "twelvedata_input": str(args.twelvedata_json),
        "twelvedata_rows": len(td_rows),
        "twelvedata_type_counts": Counter(row.get("type", "") for row in td_rows).most_common(),
        "local_listing_rows": len(local_rows),
        "local_matched_listing_keys": len(matched_keys),
        "local_unmatched_listing_rows": len(unmatched_local_rows),
        "twelvedata_matched_rows": matched_td,
        "twelvedata_unmatched_rows": len(missing_td_rows),
        "twelvedata_stock_like_rows": sum(1 for row in td_rows if row.get("type") in STOCK_LIKE_TYPES),
        "twelvedata_stock_like_matched_rows": matched_stock_like_td,
        "twelvedata_stock_like_unmatched_rows": len(stock_like_missing),
        "match_method_counts": match_method_counts,
        "missing_by_type": Counter(row["type"] for row in missing_td_rows).most_common(),
        "missing_stock_like_by_exchange": Counter(row["exchange"] for row in stock_like_missing).most_common(50),
        "local_unmatched_by_exchange": Counter(row["exchange"] for row in unmatched_local_rows).most_common(50),
        "name_mismatch_rows": len(name_mismatches),
        "figi_mismatch_rows": len(figi_mismatches),
        "venue_mapping_exchanges": sorted(LOCAL_TO_TWELVE_VENUES),
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "twelvedata_stock_compare_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown_summary(REPORT_DIR / "twelvedata_stock_compare.md", summary)
    write_csv(
        REPORT_DIR / "twelvedata_missing_stock_like.csv",
        stock_like_missing,
        ["symbol", "name", "exchange", "mic_code", "country", "currency", "type", "figi_code", "stock_like", "same_symbol_local_exchanges"],
    )
    write_csv(
        REPORT_DIR / "twelvedata_missing_all.csv",
        missing_td_rows,
        ["symbol", "name", "exchange", "mic_code", "country", "currency", "type", "figi_code", "stock_like", "same_symbol_local_exchanges"],
    )
    write_csv(
        REPORT_DIR / "twelvedata_local_unmatched.csv",
        unmatched_local_rows,
        ["listing_key", "ticker", "exchange", "name", "asset_type", "country", "isin"],
    )
    write_csv(
        REPORT_DIR / "twelvedata_name_mismatches.csv",
        sorted(name_mismatches, key=lambda row: row["name_ratio"])[: args.max_samples],
        ["listing_key", "ticker", "local_exchange", "twelvedata_exchange", "twelvedata_mic", "local_name", "twelvedata_name", "name_ratio", "twelvedata_type"],
    )
    write_csv(
        REPORT_DIR / "twelvedata_figi_mismatches.csv",
        figi_mismatches[: args.max_samples],
        ["listing_key", "ticker", "local_exchange", "twelvedata_exchange", "twelvedata_mic", "local_name", "twelvedata_name", "local_figi", "twelvedata_figi", "twelvedata_type"],
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False, default=dict))


if __name__ == "__main__":
    main()
