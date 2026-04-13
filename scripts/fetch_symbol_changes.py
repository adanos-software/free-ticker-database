from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CORPORATE_ACTIONS_DIR = DATA_DIR / "corporate_actions"
REPORTS_DIR = DATA_DIR / "reports"
LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_URL = "https://stockanalysis.com/actions/changes/"
DEFAULT_CHANGES_CSV = CORPORATE_ACTIONS_DIR / "symbol_changes.csv"
DEFAULT_CHANGES_JSON = CORPORATE_ACTIONS_DIR / "symbol_changes.json"
DEFAULT_REVIEW_CSV = REPORTS_DIR / "symbol_changes_review.csv"
DEFAULT_REVIEW_JSON = REPORTS_DIR / "symbol_changes_review.json"
DEFAULT_REVIEW_MD = REPORTS_DIR / "symbol_changes_review.md"

CHANGE_FIELDS = [
    "change_id",
    "effective_date",
    "old_symbol",
    "new_symbol",
    "new_company_name",
    "source",
    "source_url",
    "new_symbol_url",
    "source_exchange_hint",
    "source_confidence",
    "review_needed",
    "observed_at",
]
REVIEW_FIELDS = [
    *CHANGE_FIELDS,
    "match_status",
    "recommended_action",
    "old_listing_keys",
    "new_listing_keys",
    "old_match_count",
    "new_match_count",
]


@dataclass(frozen=True)
class SymbolChange:
    change_id: str
    effective_date: str
    old_symbol: str
    new_symbol: str
    new_company_name: str
    source: str
    source_url: str
    new_symbol_url: str
    source_exchange_hint: str
    source_confidence: str
    review_needed: str
    observed_at: str


@dataclass(frozen=True)
class SymbolChangeReview:
    change: SymbolChange
    match_status: str
    recommended_action: str
    old_listing_keys: str
    new_listing_keys: str
    old_match_count: int
    new_match_count: int


class StockAnalysisChangeTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[dict[str, str]]] = []
        self._in_table = False
        self._table_done = False
        self._in_row = False
        self._in_cell = False
        self._current_row: list[dict[str, str]] = []
        self._current_text: list[str] = []
        self._current_href = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._table_done:
            return
        if tag == "table" and not self._in_table:
            self._in_table = True
            return
        if not self._in_table:
            return
        if tag == "tr":
            self._in_row = True
            self._current_row = []
        elif tag == "td" and self._in_row:
            self._in_cell = True
            self._current_text = []
            self._current_href = ""
        elif tag == "a" and self._in_cell:
            attrs_dict = dict(attrs)
            self._current_href = attrs_dict.get("href") or self._current_href

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._in_cell:
            text = " ".join(part.strip() for part in self._current_text if part.strip()).strip()
            self._current_row.append({"text": text, "href": self._current_href})
            self._in_cell = False
            return
        if tag == "tr" and self._in_row:
            if len(self._current_row) >= 4:
                self.rows.append(self._current_row[:4])
            self._in_row = False
            return
        if tag == "table" and self._in_table:
            self._in_table = False
            self._table_done = True


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace(" ", "")


def parse_effective_date(value: str) -> str:
    parsed = datetime.strptime(value.strip(), "%b %d, %Y")
    return parsed.date().isoformat()


def source_exchange_hint(href: str) -> str:
    href = href.lower()
    if "/quote/otc/" in href:
        return "OTC"
    if "/stocks/" in href:
        return "US_LISTED"
    return ""


def change_id_for(*, effective_date: str, old_symbol: str, new_symbol: str, source_url: str) -> str:
    value = "|".join([effective_date, old_symbol, new_symbol, source_url])
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def parse_stockanalysis_changes(html: str, *, source_url: str, observed_at: str) -> list[SymbolChange]:
    parser = StockAnalysisChangeTableParser()
    parser.feed(html)
    changes: list[SymbolChange] = []
    for cells in parser.rows:
        date_text, old_cell, new_cell, company_cell = cells[:4]
        old_symbol = normalize_symbol(old_cell["text"])
        new_symbol = normalize_symbol(new_cell["text"])
        if not new_symbol:
            continue
        effective_date = parse_effective_date(date_text["text"])
        new_symbol_url = new_cell["href"]
        changes.append(
            SymbolChange(
                change_id=change_id_for(
                    effective_date=effective_date,
                    old_symbol=old_symbol,
                    new_symbol=new_symbol,
                    source_url=source_url,
                ),
                effective_date=effective_date,
                old_symbol=old_symbol,
                new_symbol=new_symbol,
                new_company_name=company_cell["text"].strip(),
                source="stockanalysis_symbol_changes",
                source_url=source_url,
                new_symbol_url=new_symbol_url,
                source_exchange_hint=source_exchange_hint(new_symbol_url),
                source_confidence="secondary_review",
                review_needed="true",
                observed_at=observed_at,
            )
        )
    return changes


def fetch_html(url: str, timeout_seconds: float) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": "free-ticker-database/symbol-change-sync"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def merge_changes(existing_rows: list[dict[str, str]], fetched_changes: list[SymbolChange]) -> list[SymbolChange]:
    merged: dict[str, SymbolChange] = {}
    for row in existing_rows:
        if not row.get("change_id"):
            continue
        merged[row["change_id"]] = SymbolChange(**{field: row.get(field, "") for field in CHANGE_FIELDS})
    for change in fetched_changes:
        merged[change.change_id] = change
    return sorted(
        merged.values(),
        key=lambda change: (change.effective_date, change.old_symbol, change.new_symbol),
        reverse=True,
    )


def build_listing_lookup(listings_rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    lookup: dict[str, list[dict[str, str]]] = {}
    for row in listings_rows:
        ticker = normalize_symbol(row.get("ticker", ""))
        if ticker:
            lookup.setdefault(ticker, []).append(row)
    return lookup


def listing_keys(rows: list[dict[str, str]]) -> str:
    return "|".join(sorted(row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}" for row in rows))


def review_for_change(change: SymbolChange, listing_lookup: dict[str, list[dict[str, str]]]) -> SymbolChangeReview:
    old_matches = listing_lookup.get(change.old_symbol, []) if change.old_symbol else []
    new_matches = listing_lookup.get(change.new_symbol, [])
    if not change.old_symbol:
        match_status = "informational_no_old_symbol"
        recommended_action = "review_new_symbol_only"
    elif old_matches and not new_matches:
        match_status = "old_symbol_present_new_symbol_missing"
        recommended_action = "review_possible_rename_or_delisting"
    elif not old_matches and new_matches:
        match_status = "new_symbol_present_old_symbol_missing"
        recommended_action = "already_reflected_or_new_symbol_added"
    elif old_matches and new_matches:
        match_status = "old_and_new_symbols_present"
        recommended_action = "review_duplicate_or_cross_listing_state"
    else:
        match_status = "no_matching_listing"
        recommended_action = "ignore_or_map_exchange_scope_before_applying"
    return SymbolChangeReview(
        change=change,
        match_status=match_status,
        recommended_action=recommended_action,
        old_listing_keys=listing_keys(old_matches),
        new_listing_keys=listing_keys(new_matches),
        old_match_count=len(old_matches),
        new_match_count=len(new_matches),
    )


def build_reviews(changes: list[SymbolChange], listings_rows: list[dict[str, str]]) -> list[SymbolChangeReview]:
    lookup = build_listing_lookup(listings_rows)
    return [review_for_change(change, lookup) for change in changes]


def review_to_record(review: SymbolChangeReview) -> dict[str, Any]:
    return {
        **asdict(review.change),
        "match_status": review.match_status,
        "recommended_action": review.recommended_action,
        "old_listing_keys": review.old_listing_keys,
        "new_listing_keys": review.new_listing_keys,
        "old_match_count": review.old_match_count,
        "new_match_count": review.new_match_count,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    lines = [
        "# Symbol Changes Review",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.",
        "",
        "## Summary",
        "",
        "| Metric | Rows |",
        "|---|---:|",
        f"| Fetched rows | {summary['fetched_rows']:,} |",
        f"| Merged history rows | {summary['merged_history_rows']:,} |",
        f"| Review rows | {summary['review_rows']:,} |",
        "",
        "## Match Status",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["match_status_counts"].items():
        lines.append(f"| {status} | {count:,} |")

    lines.extend(["", "## Recommended Actions", "", "| Action | Rows |", "|---|---:|"])
    for action, count in summary["recommended_action_counts"].items():
        lines.append(f"| {action} | {count:,} |")

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `source_confidence=secondary_review`: do not auto-merge as official exchange data.",
            "- `review_needed=true`: apply only after exchange/listing-key validation.",
            "- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_payload(
    *,
    fetched_changes: list[SymbolChange],
    merged_changes: list[SymbolChange],
    reviews: list[SymbolChangeReview],
    generated_at: str,
    url: str,
) -> dict[str, Any]:
    match_status_counts = Counter(review.match_status for review in reviews)
    recommended_action_counts = Counter(review.recommended_action for review in reviews)
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_url": url,
            "source_policy": "secondary_review_only",
        },
        "summary": {
            "fetched_rows": len(fetched_changes),
            "merged_history_rows": len(merged_changes),
            "review_rows": len(reviews),
            "match_status_counts": dict(sorted(match_status_counts.items())),
            "recommended_action_counts": dict(sorted(recommended_action_counts.items())),
        },
        "review_items": [review_to_record(review) for review in reviews[:1000]],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and process recent stock ticker symbol changes.")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--html-in", type=Path, default=None, help="Read already downloaded HTML instead of fetching the URL.")
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--changes-csv", type=Path, default=DEFAULT_CHANGES_CSV)
    parser.add_argument("--changes-json", type=Path, default=DEFAULT_CHANGES_JSON)
    parser.add_argument("--review-csv", type=Path, default=DEFAULT_REVIEW_CSV)
    parser.add_argument("--review-json", type=Path, default=DEFAULT_REVIEW_JSON)
    parser.add_argument("--review-md", type=Path, default=DEFAULT_REVIEW_MD)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    observed_at = utc_now()
    html = args.html_in.read_text(encoding="utf-8") if args.html_in else fetch_html(args.url, args.timeout_seconds)
    fetched_changes = parse_stockanalysis_changes(html, source_url=args.url, observed_at=observed_at)
    merged_changes = merge_changes(load_csv(args.changes_csv), fetched_changes)
    reviews = build_reviews(merged_changes, load_csv(args.listings_csv))
    payload = build_payload(
        fetched_changes=fetched_changes,
        merged_changes=merged_changes,
        reviews=reviews,
        generated_at=observed_at,
        url=args.url,
    )
    write_csv(args.changes_csv, CHANGE_FIELDS, [asdict(change) for change in merged_changes])
    write_json(
        args.changes_json,
        {
            "_meta": payload["_meta"],
            "summary": payload["summary"],
            "symbol_changes": [asdict(change) for change in merged_changes],
        },
    )
    write_csv(args.review_csv, REVIEW_FIELDS, [review_to_record(review) for review in reviews])
    write_json(args.review_json, payload)
    write_markdown(args.review_md, payload)
    return payload


def main(argv: list[str] | None = None) -> None:
    payload = run(parse_args(argv))
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
