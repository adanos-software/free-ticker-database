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

from scripts.lib.dataio import merge_metadata_updates
from scripts.lib.normalize import GENERIC_SINGLE_TOKEN_NAMES, ascii_fold, names_match
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_OUTPUT_DIR = ROOT / "data" / "same_issuer_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "primary_same_issuer_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "primary_same_issuer_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
PRIMARY_EXCHANGES = frozenset(
    {
        "NASDAQ",
        "NYSE",
        "NYSE MKT",
        "NYSE ARCA",
        "BATS",
        "TSX",
        "TSXV",
        "CSE",
        "NEO",
        "XETRA",
        "TSE",
        "ASX",
        "STO",
        "HKEX",
        "SIX",
        "Euronext",
        "AMS",
        "CPH",
        "B3",
        "BIST",
        "SGX",
        "BMV",
        "JSE",
        "IDX",
        "SSE",
        "SZSE",
        "MIL",
        "HEL",
        "OSE",
        "OSL",
        "WSE",
        "TWSE",
        "KRX",
        "KOSDAQ",
        "SET",
        "TASE",
        "BCBA",
    }
)
SOURCE_EXCHANGES = PRIMARY_EXCHANGES | {"FSX"}
GERMAN_TICKER_EXCHANGES = frozenset({"XETRA", "Munich", "XDUS"})
IDX_PROFILES_JSON = ROOT / "data" / "masterfiles" / "cache" / "idx_company_profiles.json"
LEGAL_PHRASE_RE = re.compile(
    r"\b(s\.?\s*p\.?\s*a\.?|s\.?\s*a\.?b\.?|s\.?\s*a\.?|a\s*/\s*s|d\.?\s*d\.?|n\.?\s*v\.?|"
    r"o\.?\s*n\.?|inh\.?)\b|\(\s*reg\.?\s*s\s*\)",
    re.I,
)
LEGAL_RE = re.compile(
    r"\b(incorporated|corporation|company|limited|ltd|llc|inc|corp|plc|publ|"
    r"sponsored|unsponsored|aktiengesellschaft|aktiebolag|oyj|tbk|bhd|"
    r"nv|sa|sab|spa|asa|as|ag|se|ab|co|pcl)\b",
    re.I,
)
ADR_RE = re.compile(r"\b((?:unsp|usp|sp|p)\.? ?adrs?|adrs?|cdrs?|gdrs?|gdr)\b", re.I)
NOTE_RE = re.compile(r"\b(su\.?\s*nts|sub(?:ordinated)?\s+notes?|notes due|\bbonds?\b|\bwarrants?\b)\b", re.I)
BANK_NAME_RE = re.compile(r"\bbank\b", re.I)
ENERGI_NAME_RE = re.compile(r"\benerg(?:y|ie|ia|i)\b", re.I)
EXCHANGE_NAME_RE = re.compile(r"\bstock exchanges?\b|\bexchange groups?\b", re.I)
REASON_KEY = (
    "Copied stock_sector from a non-OTC primary listing of the same issuer after an exact "
    "issuer key; parenthetical identity kept; unique GICS required."
)
REASON_CUSIP = (
    "Copied stock_sector from a non-OTC primary listing that shares this US/CA CUSIP issuer "
    "stem after name match and unique GICS."
)
REASON_XETRA = (
    "Copied stock_sector from the XETRA line with the same ticker after an ISIN change; "
    "issuer key matched and unique GICS required."
)
REASON_GERMAN = (
    "Copied stock_sector from a German listing venue with the same ticker after an ISIN "
    "change; issuer key matched and unique GICS required."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "source_ticker",
    "source_exchange",
    "source_name",
    "source_isin",
    "sector_update",
    "match_path",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def sector_value(row: dict[str, str]) -> str:
    return (row.get("stock_sector") or row.get("sector") or "").strip()


def issuer_key(name: str) -> str:
    value = ascii_fold(name or "")
    value = value.replace("&", " and ")
    value = ADR_RE.sub(" ", value)
    value = LEGAL_PHRASE_RE.sub(" ", value)
    value = re.sub(r"\bhldgs\b", " holdings ", value, flags=re.I)
    value = re.sub(r"^\s*pt\b", " ", value, flags=re.I)
    value = re.sub(r"\b(series|class)\b", " ", value, flags=re.I)
    value = LEGAL_RE.sub(" ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    tokens = [token for token in value.split() if token not in {"the", "na"}]
    while tokens and tokens[-1] in {"a", "b", "c", "d", "n", "h", "s"}:
        tokens.pop()
    return " ".join(tokens)


def key_usable(key: str) -> bool:
    tokens = key.split()
    if not tokens:
        return False
    if len(tokens) >= 2:
        return True
    token = tokens[0]
    return len(token) >= 4 and token not in GENERIC_SINGLE_TOKEN_NAMES


def _short_key_indexable(key: str) -> bool:
    tokens = key.split()
    if len(tokens) != 1:
        return False
    token = tokens[0]
    return len(token) >= 3 and token not in GENERIC_SINGLE_TOKEN_NAMES


def source_sector_plausible(name: str, sector: str) -> bool:
    folded = ascii_fold(name or "")
    if BANK_NAME_RE.search(folded) and sector != "Financials":
        return False
    if ENERGI_NAME_RE.search(folded) and sector not in {"Energy", "Utilities"}:
        return False
    if EXCHANGE_NAME_RE.search(folded) and sector != "Financials":
        return False
    return True


DEPOSITARY_PREFIXES = frozenset({"US", "CA"})


def one_token_source_allowed(target_isin: str, source_isin: str, *, same_ticker: bool) -> bool:
    if same_ticker:
        return True
    target_prefix = (target_isin or "")[:2]
    source_prefix = (source_isin or "")[:2]
    if not target_prefix or not source_prefix:
        return False
    if target_prefix == source_prefix:
        return True
    return target_prefix in DEPOSITARY_PREFIXES or source_prefix in DEPOSITARY_PREFIXES


def cusip6(isin: str) -> str:
    value = (isin or "").strip().upper()
    if value[:2] in {"US", "CA"} and len(value) >= 12:
        return value[2:8]
    return ""


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def overlay_pending_stock_sectors(
    rows: list[dict[str, str]],
    updates_csv: Path,
) -> list[dict[str, str]]:
    if not updates_csv.exists():
        return rows
    pending: dict[tuple[str, str], str] = {}
    for update in load_csv_rows(updates_csv):
        value = (update.get("proposed_value") or "").strip()
        if update.get("field") != "stock_sector" or update.get("decision") != "update" or not value:
            continue
        pending[(update.get("ticker", ""), update.get("exchange", ""))] = value
    overlaid: list[dict[str, str]] = []
    for row in rows:
        if sector_value(row):
            overlaid.append(row)
            continue
        value = pending.get((row.get("ticker", ""), row.get("exchange", "")), "")
        if value:
            overlaid.append({**row, "stock_sector": value})
        else:
            overlaid.append(row)
    return overlaid


def load_idx_profile_rows(path: Path = IDX_PROFILES_JSON) -> list[dict[str, str]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_rows = payload if isinstance(payload, list) else []
    rows: list[dict[str, str]] = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        sector = normalize_sector(str(item.get("sector") or ""), "Stock")
        if not sector:
            continue
        rows.append(
            {
                "ticker": str(item.get("ticker") or ""),
                "exchange": "IDX",
                "asset_type": "Stock",
                "name": str(item.get("name") or ""),
                "isin": str(item.get("isin") or ""),
                "stock_sector": sector,
            }
        )
    return rows


def index_primary_sources(
    rows: list[dict[str, str]],
) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
    by_key: dict[str, list[dict[str, str]]] = defaultdict(list)
    by_cusip: dict[str, list[dict[str, str]]] = defaultdict(list)
    by_xetra: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        sector = sector_value(row)
        if not sector or not source_sector_plausible(row.get("name") or "", sector):
            continue
        exchange = row.get("exchange")
        if exchange in SOURCE_EXCHANGES:
            key = issuer_key(row.get("name") or "")
            if key_usable(key) or _short_key_indexable(key):
                by_key[key].append(row)
            stem = cusip6(row.get("isin") or "")
            if stem:
                by_cusip[stem].append(row)
        if exchange in GERMAN_TICKER_EXCHANGES and row.get("ticker"):
            by_xetra[row["ticker"]].append(row)
    return by_key, by_cusip, by_xetra


def _unique_sector(peers: list[dict[str, str]]) -> str:
    sectors = {sector_value(peer) for peer in peers if sector_value(peer)}
    if len(sectors) != 1:
        return ""
    return next(iter(sectors))


def _select_peers(peers: list[dict[str, str]]) -> tuple[list[dict[str, str]], str]:
    primary = [peer for peer in peers if peer.get("exchange") in PRIMARY_EXCHANGES]
    sector = _unique_sector(primary)
    if primary and sector:
        return primary, sector
    sector = _unique_sector(peers)
    if peers and sector:
        return peers, sector
    return [], ""


def _accept(base: dict[str, Any], chosen: dict[str, str], sector: str, match_path: str) -> dict[str, Any]:
    return {
        **base,
        "source_ticker": chosen.get("ticker", ""),
        "source_exchange": chosen.get("exchange", ""),
        "source_name": chosen.get("name", ""),
        "source_isin": (chosen.get("isin") or "").strip().upper(),
        "sector_update": sector,
        "match_path": match_path,
        "decision": "accept",
    }


def evaluate_row(
    row: dict[str, str],
    source_rows: list[dict[str, str]],
    *,
    by_key: dict[str, list[dict[str, str]]] | None = None,
    by_cusip: dict[str, list[dict[str, str]]] | None = None,
    by_xetra: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    if by_key is None or by_cusip is None or by_xetra is None:
        by_key, by_cusip, by_xetra = index_primary_sources(source_rows)
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": (row.get("isin") or "").strip().upper(),
        "source_ticker": "",
        "source_exchange": "",
        "source_name": "",
        "source_isin": "",
        "sector_update": "",
        "match_path": "",
    }
    if sector_value(row):
        return {**base, "decision": "already_has_sector"}
    if NOTE_RE.search(row.get("name") or ""):
        return {**base, "decision": "skip_notes"}
    if not is_valid_isin(base["isin"]):
        return {**base, "decision": "missing_isin"}

    stem = cusip6(base["isin"])
    if stem:
        cusip_peers = [
            peer
            for peer in by_cusip.get(stem, [])
            if (peer.get("isin") or "").strip().upper() != base["isin"]
            and names_match(row.get("name") or "", peer.get("name") or "")
        ]
        sector = _unique_sector(cusip_peers)
        if sector:
            return _accept(base, cusip_peers[0], sector, "cusip6")

    key = issuer_key(row.get("name") or "")
    xetra_peers = [
        peer
        for peer in by_xetra.get(row.get("ticker") or "", [])
        if (peer.get("isin") or "").strip().upper() != base["isin"]
        and issuer_key(peer.get("name") or "") == key
    ]
    chosen_german, sector = _select_peers(xetra_peers)
    if chosen_german and sector:
        match_path = "xetra_ticker" if chosen_german[0].get("exchange") == "XETRA" else "german_ticker"
        return _accept(base, chosen_german[0], sector, match_path)

    key_tokens = key.split()
    if not key_usable(key) and not _short_key_indexable(key):
        return {**base, "decision": "short_issuer_key"}
    key_peers = []
    for peer in by_key.get(key, []):
        if (peer.get("isin") or "").strip().upper() == base["isin"]:
            continue
        if peer.get("ticker") == row.get("ticker") and peer.get("exchange") == row.get("exchange"):
            continue
        if len(key_tokens) == 1 and not one_token_source_allowed(
            base["isin"],
            (peer.get("isin") or "").strip().upper(),
            same_ticker=peer.get("ticker") == row.get("ticker"),
        ):
            continue
        key_peers.append(peer)
    chosen, sector = _select_peers(key_peers)
    if chosen and sector:
        return _accept(base, chosen[0], sector, "exact_issuer_key")
    if not key_usable(key):
        return {**base, "decision": "short_issuer_key"}
    return {**base, "decision": "no_primary_match"}


def verify_rows(rows: list[dict[str, str]], listing_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_key, by_cusip, by_xetra = index_primary_sources(listing_rows)
    return [
        evaluate_row(row, listing_rows, by_key=by_key, by_cusip=by_cusip, by_xetra=by_xetra)
        for row in rows
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": {
                    "cusip6": "0.84",
                    "xetra_ticker": "0.83",
                    "german_ticker": "0.83",
                    "exact_issuer_key": "0.82",
                }.get(str(result.get("match_path")), "0.82"),
                "reason": {
                    "cusip6": REASON_CUSIP,
                    "xetra_ticker": REASON_XETRA,
                    "german_ticker": REASON_GERMAN,
                    "exact_issuer_key": REASON_KEY,
                }.get(str(result.get("match_path")), REASON_KEY),
            }
        )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy stock_sector from a non-OTC primary listing of the same issuer."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", default=["FSX"])
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    listing_rows = overlay_pending_stock_sectors(
        load_csv_rows(args.listings_csv),
        args.metadata_updates_csv,
    ) + load_idx_profile_rows()
    exchanges = set(args.exchange)
    candidates = [
        row
        for row in listing_rows
        if row.get("exchange") in exchanges
        and row.get("asset_type") == "Stock"
        and not sector_value(row)
    ]
    results = verify_rows(candidates, listing_rows)
    updates = build_metadata_updates(results)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)
    print(
        json.dumps(
            {
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "json_out": display_path(args.json_out),
                "match_path_counts": dict(
                    Counter(result.get("match_path") for result in results if result["decision"] == "accept")
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
