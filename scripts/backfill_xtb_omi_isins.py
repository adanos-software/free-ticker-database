from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import pdfplumber
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, ascii_fold, is_valid_isin


DEFAULT_PDF_URL = "https://www.xtb.com/int/Specification_Table_Organised_Market_Instruments_OMI.pdf"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "xtb_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "omi_missing_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "omi_missing_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

ISIN_RE = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
XTB_ROW_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9][A-Z0-9.\-]*\.[A-Z]{2}\*{0,3})\s+"
    r"(?P<name>.+?)\s+"
    r"(?P<isin>[A-Z]{2}[A-Z0-9]{9}[0-9])\s+"
    r"(?P<currency>[A-Z]{3})\s+"
)

XTB_SUFFIX_EXCHANGES: dict[str, set[str]] = {
    "BE": {"Euronext"},
    "CH": {"SIX"},
    "DE": {"XETRA"},
    "DK": {"CPH"},
    "ES": {"BME"},
    "FI": {"HEL"},
    "FR": {"Euronext"},
    "IT": {"Euronext"},
    "NL": {"AMS"},
    "NO": {"OSL"},
    "PT": {"Euronext"},
    "SE": {"STO"},
    "UK": {"LSE"},
    "US": {"BATS", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "NYSEARCA"},
}

ETF_COMPATIBLE_ASSET_TYPES = {"ETF"}
LEGAL_NAME_STOPWORDS = {
    "a",
    "ab",
    "adr",
    "ads",
    "ag",
    "as",
    "asa",
    "class",
    "co",
    "common",
    "company",
    "corp",
    "corporation",
    "cv",
    "de",
    "etf",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "llc",
    "lp",
    "ltd",
    "nv",
    "ordinary",
    "plc",
    "sa",
    "sab",
    "se",
    "share",
    "shares",
    "spa",
    "stock",
    "the",
}
GENERIC_SINGLE_TOKEN_NAMES = {
    "capital",
    "energy",
    "financial",
    "fund",
    "global",
    "health",
    "industrial",
    "resources",
    "technology",
    "technologies",
}

REPORT_FIELDNAMES = [
    "xtb_symbol",
    "xtb_base_ticker",
    "xtb_suffix",
    "xtb_name",
    "xtb_asset_type",
    "xtb_isin",
    "xtb_currency",
    "xtb_page",
    "allowed_exchanges",
    "target_ticker",
    "target_exchange",
    "target_asset_type",
    "target_name",
    "name_match",
    "decision",
]


@dataclass(frozen=True)
class XtbInstrument:
    symbol: str
    base_ticker: str
    suffix: str
    name: str
    isin: str
    currency: str
    asset_type: str
    page: int


@dataclass(frozen=True)
class XtbPageRanges:
    stocks_start: int
    etf_start: int
    fractional_start: int


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def parse_toc_page_ranges(first_page_text: str) -> XtbPageRanges:
    def parse_page(label: str, default: int) -> int:
        match = re.search(
            rf"^\s*{re.escape(label)}\s+page\s+(\d+)\s*$",
            first_page_text,
            re.IGNORECASE | re.MULTILINE,
        )
        return int(match.group(1)) if match else default

    return XtbPageRanges(
        stocks_start=parse_page("Stocks", 2),
        etf_start=parse_page("ETF, ETN, ETC", 93),
        fractional_start=parse_page("Specification Table Fractional Rights", 116),
    )


def clean_xtb_symbol(symbol: str) -> tuple[str, str, str]:
    cleaned = symbol.strip().rstrip("*")
    base_ticker, suffix = cleaned.rsplit(".", 1)
    return cleaned, base_ticker, suffix


def clean_xtb_name(name: str) -> str:
    return name.removeprefix("CLOSE ONLY / ").strip()


def parse_xtb_row_line(line: str, asset_type: str, page: int) -> XtbInstrument | None:
    if not ISIN_RE.search(line):
        return None
    match = XTB_ROW_RE.match(line.strip())
    if not match:
        return None
    symbol, base_ticker, suffix = clean_xtb_symbol(match.group("symbol"))
    return XtbInstrument(
        symbol=symbol,
        base_ticker=base_ticker,
        suffix=suffix,
        name=clean_xtb_name(match.group("name")),
        isin=match.group("isin").upper(),
        currency=match.group("currency").upper(),
        asset_type=asset_type,
        page=page,
    )


def parse_xtb_omi_page_texts(page_texts: list[str]) -> list[XtbInstrument]:
    if not page_texts:
        return []

    ranges = parse_toc_page_ranges(page_texts[0])
    instruments: list[XtbInstrument] = []
    for page_index, text in enumerate(page_texts, start=1):
        if ranges.stocks_start <= page_index < ranges.etf_start:
            asset_type = "Stock"
        elif ranges.etf_start <= page_index < ranges.fractional_start:
            asset_type = "ETF"
        else:
            continue

        for line in text.splitlines():
            instrument = parse_xtb_row_line(line, asset_type, page_index)
            if instrument is not None:
                instruments.append(instrument)
    return instruments


def parse_xtb_omi_pdf(pdf_bytes: bytes) -> list[XtbInstrument]:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        page_texts = [page.extract_text() or "" for page in pdf.pages]
    return parse_xtb_omi_page_texts(page_texts)


def download_xtb_pdf(url: str, *, timeout_seconds: float) -> bytes:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"XTB OMI source did not return a PDF: {response.headers.get('content-type', '')}")
    return response.content


def load_missing_isin_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if not row.get("isin", "").strip()]


def index_missing_rows(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    indexed: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[(row["ticker"], row["exchange"])].append(row)
    return indexed


def significant_name_tokens(value: str) -> set[str]:
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\bclass\s+[a-z0-9]+\b", " ", value, flags=re.IGNORECASE)
    tokens = re.findall(r"[a-z0-9]+", ascii_fold(value).lower())
    return {
        token
        for token in tokens
        if len(token) > 1 and not token.isdigit() and token not in LEGAL_NAME_STOPWORDS
    }


def names_match(xtb_name: str, target_name: str) -> bool:
    xtb_tokens = significant_name_tokens(xtb_name)
    target_tokens = significant_name_tokens(target_name)
    if not xtb_tokens or not target_tokens:
        return False

    if xtb_tokens == target_tokens:
        if len(xtb_tokens) == 1:
            token = next(iter(xtb_tokens))
            return len(token) >= 5 and token not in GENERIC_SINGLE_TOKEN_NAMES
        return True

    shared = xtb_tokens & target_tokens
    if not shared:
        return False

    shared_min_ratio = len(shared) / min(len(xtb_tokens), len(target_tokens))
    shared_max_ratio = len(shared) / max(len(xtb_tokens), len(target_tokens))
    return len(shared) >= 2 and shared_min_ratio >= 0.67 and shared_max_ratio >= 0.5


def asset_type_matches(xtb_asset_type: str, target_asset_type: str) -> bool:
    if xtb_asset_type == "Stock":
        return target_asset_type == "Stock"
    if xtb_asset_type == "ETF":
        return target_asset_type in ETF_COMPATIBLE_ASSET_TYPES
    return False


def evaluate_xtb_instrument(
    instrument: XtbInstrument,
    missing_rows_by_key: dict[tuple[str, str], list[dict[str, str]]],
) -> dict[str, Any]:
    allowed_exchanges = sorted(XTB_SUFFIX_EXCHANGES.get(instrument.suffix, set()))
    base = {
        "xtb_symbol": instrument.symbol,
        "xtb_base_ticker": instrument.base_ticker,
        "xtb_suffix": instrument.suffix,
        "xtb_name": instrument.name,
        "xtb_asset_type": instrument.asset_type,
        "xtb_isin": instrument.isin,
        "xtb_currency": instrument.currency,
        "xtb_page": instrument.page,
        "allowed_exchanges": "|".join(allowed_exchanges),
        "target_ticker": "",
        "target_exchange": "",
        "target_asset_type": "",
        "target_name": "",
        "name_match": "",
    }

    if not is_valid_isin(instrument.isin):
        return {**base, "decision": "invalid_isin"}
    if not allowed_exchanges:
        return {**base, "decision": "unsupported_suffix"}

    candidates: list[dict[str, str]] = []
    for exchange in allowed_exchanges:
        candidates.extend(missing_rows_by_key.get((instrument.base_ticker, exchange), []))

    if not candidates:
        return {**base, "decision": "no_missing_isin_match"}
    if len(candidates) > 1:
        return {**base, "decision": "ambiguous_target"}

    target = candidates[0]
    base.update(
        {
            "target_ticker": target["ticker"],
            "target_exchange": target["exchange"],
            "target_asset_type": target["asset_type"],
            "target_name": target["name"],
        }
    )

    if not asset_type_matches(instrument.asset_type, target["asset_type"]):
        return {**base, "decision": "asset_type_mismatch"}

    name_match = names_match(instrument.name, target["name"])
    base["name_match"] = name_match
    if not name_match:
        return {**base, "decision": "name_mismatch"}

    return {**base, "decision": "accept"}


def verify_xtb_instruments(
    instruments: list[XtbInstrument],
    missing_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    missing_rows_by_key = index_missing_rows(missing_rows)
    return [evaluate_xtb_instrument(instrument, missing_rows_by_key) for instrument in instruments]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["target_ticker"],
                "exchange": result["target_exchange"],
                "field": "isin",
                "decision": "update",
                "proposed_value": result["xtb_isin"],
                "confidence": "0.82",
                "reason": f"XTB OMI specification table lists {result['xtb_symbol']} with this valid ISIN; accepted only after ticker suffix/exchange, asset type, and issuer-name gates matched an existing row without ISIN.",
            }
        )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing ISINs from the XTB OMI specification PDF.")
    parser.add_argument("--pdf-url", default=DEFAULT_PDF_URL)
    parser.add_argument("--pdf-path", type=Path, help="Read a local XTB OMI PDF instead of downloading it.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--suffix", action="append", help="Restrict to one or more XTB suffixes, e.g. US or DE.")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.pdf_path:
        pdf_bytes = args.pdf_path.read_bytes()
    else:
        pdf_bytes = download_xtb_pdf(args.pdf_url, timeout_seconds=args.timeout_seconds)

    instruments = parse_xtb_omi_pdf(pdf_bytes)
    if args.suffix:
        suffixes = {suffix.upper() for suffix in args.suffix}
        instruments = [instrument for instrument in instruments if instrument.suffix in suffixes]
    if args.limit is not None:
        instruments = instruments[: args.limit]

    missing_rows = load_missing_isin_rows()
    results = verify_xtb_instruments(instruments, missing_rows)
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps(
            [result for result in results if result["decision"] == "accept"],
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "xtb_instruments": len(instruments),
                "missing_isin_rows": len(missing_rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "accepted_isin_updates": len(updates),
                "unique_accepted_isins": len({update["proposed_value"] for update in updates}),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
