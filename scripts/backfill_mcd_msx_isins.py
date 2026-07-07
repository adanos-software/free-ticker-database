from __future__ import annotations

import argparse
import csv
import html
import json
import ssl
import subprocess
import sys
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.lib.normalize import significant_name_tokens
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin

try:
    import certifi
except ImportError:  # pragma: no cover - optional runtime hardening
    certifi = None


MCD_LISTED_INSTRUMENTS_URL = "https://www.mcd.om/en/Default/Statistic/ListedInstrumentsInfo"
MCD_ISSUER_REGISTRATION_URL = "https://www.mcd.om/en/Registration/Issuer/IssuerRegistration"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "mcd_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "msx_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "msx_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "mcd_security_symbol",
    "mcd_security_name",
    "mcd_security_type",
    "mcd_market_type",
    "mcd_sector",
    "mcd_isin",
    "mcd_issuer_name",
    "issuer_registration_match",
    "name_token_subset_match",
    "decision",
    "source_url",
    "identity_gate_context",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


class IssuerOptionParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_option = False
        self.current_value = ""
        self.current_text: list[str] = []
        self.options: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "option":
            return
        attr = {key: value or "" for key, value in attrs}
        self.current_value = attr.get("value", "").strip().upper()
        self.current_text = []
        self.in_option = True

    def handle_endtag(self, tag: str) -> None:
        if tag != "option" or not self.in_option:
            return
        text = " ".join("".join(self.current_text).split())
        if self.current_value and text and text != "---Select---":
            self.options.append({"mcd_isin": self.current_value, "mcd_issuer_name": html.unescape(text)})
        self.in_option = False

    def handle_data(self, data: str) -> None:
        if self.in_option:
            self.current_text.append(data)


def fetch_mcd_html(url: str, timeout_seconds: float) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": "free-ticker-database/3.0",
        },
    )
    context = ssl.create_default_context(cafile=certifi.where()) if certifi else None
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return response.read().decode("utf-8")
    except URLError:
        # mcd.om currently omits the DigiCert intermediate certificate; system curl can still
        # build the chain from the platform trust store while keeping certificate checks enabled.
        result = subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--max-time",
                str(timeout_seconds),
                "--user-agent",
                "free-ticker-database/3.0",
                url,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout


def fetch_mcd_listed_instruments_html(timeout_seconds: float) -> str:
    return fetch_mcd_html(MCD_LISTED_INSTRUMENTS_URL, timeout_seconds)


def fetch_mcd_issuer_registration_html(timeout_seconds: float) -> str:
    return fetch_mcd_html(MCD_ISSUER_REGISTRATION_URL, timeout_seconds)


def extract_json_array_after_marker(page_html: str, marker: str) -> list[Any]:
    start = page_html.find(marker)
    if start < 0:
        return []
    index = start + len(marker)
    while index < len(page_html) and page_html[index].isspace():
        index += 1
    if index >= len(page_html) or page_html[index] != "[":
        return []

    depth = 0
    in_string = False
    escaped = False
    for pos in range(index, len(page_html)):
        char = page_html[pos]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return json.loads(page_html[index : pos + 1])
    return []


def parse_mcd_listed_instrument_rows(page_html: str) -> list[dict[str, str]]:
    records = extract_json_array_after_marker(page_html, '"data":')
    rows: list[dict[str, str]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        symbol = str(record.get("SecuritySymbol") or "").strip().upper()
        isin = str(record.get("ISINCode") or "").strip().upper()
        if not symbol:
            continue
        rows.append(
            {
                "mcd_security_symbol": symbol,
                "mcd_security_name": str(record.get("SecurityName") or "").strip(),
                "mcd_security_type": str(record.get("SecurityType") or "").strip(),
                "mcd_market_type": str(record.get("MarketType") or "").strip(),
                "mcd_sector": str(record.get("Sector") or "").strip(),
                "mcd_isin": isin,
                "source_url": MCD_LISTED_INSTRUMENTS_URL,
            }
        )
    return rows


def parse_mcd_issuer_registration_rows(page_html: str) -> list[dict[str, str]]:
    parser = IssuerOptionParser()
    parser.feed(page_html)
    return parser.options


def load_missing_msx_rows(path: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") == "MSX"
        and row.get("asset_type") in {"ETF", "Stock"}
        and not row.get("isin", "").strip()
    ]


def name_token_subset_match(source_name: str, target_name: str) -> bool:
    source_tokens = significant_name_tokens(source_name)
    target_tokens = significant_name_tokens(target_name)
    return bool(target_tokens) and len(target_tokens) >= 2 and target_tokens <= source_tokens


def identity_gate_context(row: dict[str, Any]) -> str:
    return (
        f"symbol_exact_match={'true' if row.get('ticker') == row.get('mcd_security_symbol') else 'false'};"
        f"valid_isin_checksum={'true' if is_valid_isin(str(row.get('mcd_isin') or '')) else 'false'};"
        f"om_isin_prefix={'true' if str(row.get('mcd_isin') or '').startswith('OM') else 'false'};"
        f"issuer_registration_match={'true' if row.get('issuer_registration_match') else 'false'};"
        f"name_token_subset_match={'true' if row.get('name_token_subset_match') else 'false'}"
    )


def evaluate_rows(
    target_rows: list[dict[str, str]],
    mcd_rows: list[dict[str, str]],
    issuer_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[dict[str, str]]] = {}
    for row in mcd_rows:
        by_symbol.setdefault(row["mcd_security_symbol"], []).append(row)
    issuer_by_isin = {row["mcd_isin"]: row for row in issuer_rows or []}

    results: list[dict[str, Any]] = []
    for target in target_rows:
        ticker = target.get("ticker", "").strip().upper()
        matches = by_symbol.get(ticker, [])
        base: dict[str, Any] = {
            "ticker": target.get("ticker", ""),
            "exchange": target.get("exchange", ""),
            "asset_type": target.get("asset_type", ""),
            "name": target.get("name", ""),
            "mcd_security_symbol": "",
            "mcd_security_name": "",
            "mcd_security_type": "",
            "mcd_market_type": "",
            "mcd_sector": "",
            "mcd_isin": "",
            "mcd_issuer_name": "",
            "issuer_registration_match": False,
            "name_token_subset_match": False,
            "source_url": MCD_LISTED_INSTRUMENTS_URL,
        }
        if not matches:
            base["decision"] = "no_mcd_symbol_match"
        elif len({match.get("mcd_isin", "") for match in matches}) != 1:
            base.update(matches[0])
            base["name_token_subset_match"] = name_token_subset_match(
                base["mcd_security_name"], target.get("name", "")
            )
            base["decision"] = "ambiguous_mcd_isin"
        else:
            match = matches[0]
            base.update(match)
            issuer_row = issuer_by_isin.get(base["mcd_isin"], {})
            base["mcd_issuer_name"] = issuer_row.get("mcd_issuer_name", "")
            base["issuer_registration_match"] = name_token_subset_match(
                base["mcd_issuer_name"], target.get("name", "")
            )
            base["name_token_subset_match"] = name_token_subset_match(
                base["mcd_security_name"], target.get("name", "")
            )
            if not base["mcd_isin"]:
                base["decision"] = "missing_mcd_isin"
            elif not is_valid_isin(base["mcd_isin"]) or not base["mcd_isin"].startswith("OM"):
                base["decision"] = "invalid_or_non_om_isin"
            elif not base["name_token_subset_match"]:
                base["decision"] = "name_mismatch"
            elif base["mcd_security_type"] == "BOND" and not base["issuer_registration_match"]:
                base["decision"] = "security_type_mismatch"
            else:
                base["decision"] = "accept"
        base["identity_gate_context"] = identity_gate_context(base)
        results.append(base)
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "isin",
            "decision": "update",
            "proposed_value": result["mcd_isin"],
            "confidence": "0.97",
            "reason": (
                "Official Muscat Clearing & Depository ListedInstrumentsInfo supplied a valid OM ISIN "
                "for the exact MSX security symbol; accepted only after exact symbol, instrument/issuer-name token subset, "
                f"OM country prefix, and ISIN checksum gates matched. Source: {result['source_url']}"
            ),
        }
        for result in results
        if result.get("decision") == "accept"
    ]


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing MSX ISINs from official Muscat Clearing & Depository instruments data."
    )
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--source-html", type=Path)
    parser.add_argument("--issuer-source-html", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.source_html:
        page_html = args.source_html.read_text(encoding="utf-8")
    else:
        page_html = fetch_mcd_listed_instruments_html(args.timeout_seconds)
    if args.issuer_source_html:
        issuer_html = args.issuer_source_html.read_text(encoding="utf-8")
    else:
        issuer_html = fetch_mcd_issuer_registration_html(args.timeout_seconds)
    mcd_rows = parse_mcd_listed_instrument_rows(page_html)
    issuer_rows = parse_mcd_issuer_registration_rows(issuer_html)
    target_rows = load_missing_msx_rows(args.tickers_csv)
    results = evaluate_rows(target_rows, mcd_rows, issuer_rows)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    summary = {
        "accepted_isin_updates": len(updates),
        "applied": args.apply,
        "candidates": len(target_rows),
        "csv_out": display_path(args.csv_out),
        "decision_counts": dict(Counter(row["decision"] for row in results)),
        "generated_at": utc_now_iso(),
        "json_out": display_path(args.json_out),
        "mcd_issuer_rows": len(issuer_rows),
        "mcd_rows": len(mcd_rows),
        "source_urls": [MCD_LISTED_INSTRUMENTS_URL, MCD_ISSUER_REGISTRATION_URL],
        "policy": {
            "official_source": "Muscat Clearing & Depository ListedInstrumentsInfo instrument grid and IssuerRegistration selector.",
            "no_guessing": (
                "ISIN updates require exact MSX symbol, instrument-name token subset, OM prefix, "
                "valid checksum, and issuer-registration confirmation when the MCD security type conflicts."
            ),
            "traceability": "Every target row is written to the probe CSV with source and identity gate context.",
        },
    }
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": results}, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
