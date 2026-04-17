from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import alias_matches_company, is_valid_isin

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
DEFAULT_CURRENT_GAPS_CSV = REPORTS_DIR / "financialdata_current_exchange_gaps.csv"
DEFAULT_GLOBAL_EXPANSION_CSV = REPORTS_DIR / "financialdata_global_expansion_candidates.csv"
DEFAULT_SUPPLEMENTS_CSV = MASTERFILES_DIR / "financialdata_isin_supplemental_listings.csv"
DEFAULT_REVIEW_CSV = REPORTS_DIR / "financialdata_isin_supplements_review.csv"
DEFAULT_REVIEW_JSON = REPORTS_DIR / "financialdata_isin_supplements_review.json"
DEFAULT_REVIEW_MD = REPORTS_DIR / "financialdata_isin_supplements_review.md"

SUPPLEMENT_FIELDNAMES = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
    "source_key",
    "source_url",
    "reference_scope",
]
REVIEW_FIELDNAMES = [
    "financialdata_symbol",
    "financialdata_ticker",
    "financialdata_exchange",
    "financialdata_name",
    "financialdata_review_scope",
    "official_ticker",
    "official_exchange",
    "official_name",
    "official_isin",
    "official_source_key",
    "official_reference_scope",
    "decision",
    "reason",
]

EXCHANGE_COUNTRY = {
    "AMS": ("Netherlands", "NL"),
    "B3": ("Brazil", "BR"),
    "BSE_IN": ("India", "IN"),
    "Bursa": ("Malaysia", "MY"),
    "Euronext": ("France", "FR"),
    "HKEX": ("Hong Kong", "HK"),
    "KRX": ("South Korea", "KR"),
    "LSE": ("United Kingdom", "GB"),
    "NSE_IN": ("India", "IN"),
    "TSE": ("Japan", "JP"),
    "XETRA": ("Germany", "DE"),
}

EXCHANGE_PRIORITY = {
    "Bursa": 10,
    "KRX": 10,
    "B3": 20,
    "LSE": 30,
    "NSE_IN": 40,
    "HKEX": 50,
    "BSE_IN": 60,
}


@dataclass(frozen=True)
class ReviewRow:
    financialdata_symbol: str
    financialdata_ticker: str
    financialdata_exchange: str
    financialdata_name: str
    financialdata_review_scope: str
    official_ticker: str
    official_exchange: str
    official_name: str
    official_isin: str
    official_source_key: str
    official_reference_scope: str
    decision: str
    reason: str


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def names_match(left: str, right: str) -> bool:
    if not left or not right:
        return False
    return alias_matches_company(left, right) or alias_matches_company(right, left)


def active_isin_masterfiles(masterfile_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in masterfile_rows
        if row.get("listing_status") == "active" and is_valid_isin(row.get("isin", "").strip().upper())
    ]


def build_masterfile_indexes(
    masterfile_rows: list[dict[str, str]],
) -> tuple[dict[tuple[str, str], list[dict[str, str]]], dict[str, list[dict[str, str]]]]:
    by_exchange_ticker: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    by_exchange: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in active_isin_masterfiles(masterfile_rows):
        by_exchange_ticker[(row["exchange"], row["ticker"])].append(row)
        by_exchange[row["exchange"]].append(row)
    return by_exchange_ticker, by_exchange


def candidate_masterfile_rows(
    financialdata_row: dict[str, str],
    by_exchange_ticker: dict[tuple[str, str], list[dict[str, str]]],
    by_exchange: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    exchange = financialdata_row["mapped_exchange"]
    ticker = financialdata_row["base_ticker"]
    candidates: list[dict[str, str]] = list(by_exchange_ticker.get((exchange, ticker), []))
    if exchange == "HKEX" and ticker.isdigit():
        candidates.extend(by_exchange_ticker.get((exchange, ticker.zfill(5)), []))
    if exchange == "BSE_IN":
        candidates.extend(
            row for row in by_exchange[exchange] if names_match(financialdata_row["registrant_name"], row["name"])
        )

    output: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in candidates:
        key = (row["exchange"], row["ticker"], row["isin"])
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def choose_masterfile_row(financialdata_row: dict[str, str], rows: list[dict[str, str]]) -> dict[str, str]:
    return sorted(
        rows,
        key=lambda row: (
            row["ticker"] != financialdata_row["base_ticker"],
            row["source_key"],
            row["ticker"],
        ),
    )[0]


def review_base(financialdata_row: dict[str, str]) -> dict[str, str]:
    return {
        "financialdata_symbol": financialdata_row.get("financialdata_symbol", ""),
        "financialdata_ticker": financialdata_row.get("base_ticker", ""),
        "financialdata_exchange": financialdata_row.get("mapped_exchange", ""),
        "financialdata_name": financialdata_row.get("registrant_name", ""),
        "financialdata_review_scope": financialdata_row.get("review_scope", ""),
    }


def review_row(financialdata_row: dict[str, str], official_row: dict[str, str] | None, decision: str, reason: str) -> ReviewRow:
    official_row = official_row or {}
    return ReviewRow(
        **review_base(financialdata_row),
        official_ticker=official_row.get("ticker", ""),
        official_exchange=official_row.get("exchange", ""),
        official_name=official_row.get("name", ""),
        official_isin=official_row.get("isin", ""),
        official_source_key=official_row.get("source_key", ""),
        official_reference_scope=official_row.get("reference_scope", ""),
        decision=decision,
        reason=reason,
    )


def supplement_row(financialdata_row: dict[str, str], official_row: dict[str, str]) -> dict[str, str]:
    country, country_code = EXCHANGE_COUNTRY[official_row["exchange"]]
    return {
        "ticker": official_row["ticker"],
        "name": official_row["name"],
        "exchange": official_row["exchange"],
        "asset_type": official_row.get("asset_type", "Stock") or "Stock",
        "sector": official_row.get("sector", ""),
        "country": country,
        "country_code": country_code,
        "isin": official_row["isin"].strip().upper(),
        "aliases": financialdata_row.get("registrant_name", ""),
        "source_key": official_row.get("source_key", ""),
        "source_url": official_row.get("source_url", ""),
        "reference_scope": official_row.get("reference_scope", ""),
    }


def normalize_existing_supplement(row: dict[str, str]) -> dict[str, str]:
    return {field: row.get(field, "") for field in SUPPLEMENT_FIELDNAMES}


def rank_financialdata_row(row: dict[str, str]) -> tuple[int, str, str]:
    return (
        EXCHANGE_PRIORITY.get(row.get("mapped_exchange", ""), 99),
        row.get("mapped_exchange", ""),
        row.get("base_ticker", ""),
    )


def build_financialdata_isin_supplements(
    *,
    financialdata_rows: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    listing_rows: list[dict[str, str]],
    existing_supplement_rows: list[dict[str, str]] | None = None,
) -> tuple[list[dict[str, str]], list[ReviewRow], dict[str, Any]]:
    by_exchange_ticker, by_exchange = build_masterfile_indexes(masterfile_rows)
    existing_keys = {(row["exchange"], row["ticker"]) for row in listing_rows}
    existing_tickers = {row["ticker"] for row in listing_rows}
    existing_isins = {row["isin"].strip().upper() for row in listing_rows if row.get("isin", "").strip()}
    supplements_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in existing_supplement_rows or []:
        normalized = normalize_existing_supplement(row)
        key = (normalized["exchange"], normalized["ticker"])
        if not key[0] or not key[1]:
            continue
        supplements_by_key[key] = normalized

    selected_tickers = {row["ticker"] for row in supplements_by_key.values() if row.get("ticker")}
    selected_isins = {
        row["isin"].strip().upper()
        for row in supplements_by_key.values()
        if row.get("isin", "").strip()
    }
    reviews: list[ReviewRow] = []

    for financialdata_row in sorted(financialdata_rows, key=rank_financialdata_row):
        exchange = financialdata_row.get("mapped_exchange", "")
        if exchange not in EXCHANGE_COUNTRY:
            reviews.append(review_row(financialdata_row, None, "reject", "exchange_not_allowed_for_isin_supplement"))
            continue

        official_candidates = [
            row
            for row in candidate_masterfile_rows(financialdata_row, by_exchange_ticker, by_exchange)
            if names_match(financialdata_row.get("registrant_name", ""), row.get("name", ""))
        ]
        candidate_isins = {row["isin"].strip().upper() for row in official_candidates}
        if not official_candidates:
            reviews.append(review_row(financialdata_row, None, "reject", "no_name_gated_official_isin_match"))
            continue
        if len(candidate_isins) > 1:
            reviews.append(
                review_row(
                    financialdata_row,
                    choose_masterfile_row(financialdata_row, official_candidates),
                    "reject",
                    "ambiguous_official_isin_candidates",
                )
            )
            continue

        official_row = choose_masterfile_row(financialdata_row, official_candidates)
        official_key = (official_row["exchange"], official_row["ticker"])
        official_isin = official_row["isin"].strip().upper()
        if official_key in supplements_by_key:
            reviews.append(review_row(financialdata_row, official_row, "preserve", "already_in_financialdata_supplement"))
            continue
        if official_key in existing_keys:
            reviews.append(review_row(financialdata_row, official_row, "skip", "official_listing_key_already_exists"))
            continue
        if official_row["ticker"] in existing_tickers:
            reviews.append(review_row(financialdata_row, official_row, "skip", "ticker_already_exists_globally"))
            continue
        if official_isin in existing_isins:
            reviews.append(review_row(financialdata_row, official_row, "skip", "isin_already_exists_in_database"))
            continue
        if official_row["ticker"] in selected_tickers:
            reviews.append(review_row(financialdata_row, official_row, "skip", "ticker_already_selected"))
            continue
        if official_isin in selected_isins:
            reviews.append(review_row(financialdata_row, official_row, "skip", "isin_already_selected"))
            continue

        selected_tickers.add(official_row["ticker"])
        selected_isins.add(official_isin)
        supplements_by_key[official_key] = supplement_row(financialdata_row, official_row)
        reviews.append(review_row(financialdata_row, official_row, "accept", "official_isin_name_gated_unique_primary"))

    supplements = list(supplements_by_key.values())
    decision_counts = Counter(row.decision for row in reviews)
    reason_counts = Counter(row.reason for row in reviews)
    exchange_counts = Counter(row["exchange"] for row in supplements)
    summary = {
        "generated_at": utc_now(),
        "input_rows": len(financialdata_rows),
        "preserved_supplement_rows": len(existing_supplement_rows or []),
        "supplement_rows": len(supplements),
        "decision_counts": dict(sorted(decision_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "supplement_rows_by_exchange": dict(exchange_counts.most_common()),
    }
    return sorted(supplements, key=lambda row: (row["exchange"], row["ticker"])), reviews, summary


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# FinancialData ISIN Supplements",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "FinancialData rows are used only as discovery signals. Accepted supplement rows require an official active masterfile row, a valid ISIN, issuer-name gate, no existing global ticker, and no existing/selected ISIN.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Input rows | {summary['input_rows']} |",
        f"| Accepted supplement rows | {summary['supplement_rows']} |",
        "",
        "## Accepted By Exchange",
        "",
        "| Exchange | Rows |",
        "|---|---:|",
    ]
    for exchange, count in summary["supplement_rows_by_exchange"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["decision_counts"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Reasons", "", "| Reason | Rows |", "|---|---:|"])
    for reason, count in summary["reason_counts"].items():
        lines.append(f"| {reason} | {count} |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    financialdata_rows = load_csv(args.current_gaps_csv) + load_csv(args.global_expansion_csv)
    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=financialdata_rows,
        masterfile_rows=load_csv(args.masterfile_reference_csv),
        listing_rows=load_csv(args.listings_csv),
        existing_supplement_rows=load_csv(args.supplements_csv),
    )
    write_csv(args.supplements_csv, SUPPLEMENT_FIELDNAMES, supplements)
    write_csv(args.review_csv, REVIEW_FIELDNAMES, [asdict(row) for row in reviews])
    write_json(
        args.review_json,
        {
            "_meta": {
                "source": "financialdata_official_isin_supplements",
                "generated_at": summary["generated_at"],
            },
            "summary": summary,
            "review_items": [asdict(row) for row in reviews],
        },
    )
    write_markdown(args.review_md, summary)
    print(json.dumps(summary, sort_keys=True))
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build official-ISIN supplements from FinancialData discovery rows.")
    parser.add_argument("--current-gaps-csv", type=Path, default=DEFAULT_CURRENT_GAPS_CSV)
    parser.add_argument("--global-expansion-csv", type=Path, default=DEFAULT_GLOBAL_EXPANSION_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--supplements-csv", type=Path, default=DEFAULT_SUPPLEMENTS_CSV)
    parser.add_argument("--review-csv", type=Path, default=DEFAULT_REVIEW_CSV)
    parser.add_argument("--review-json", type=Path, default=DEFAULT_REVIEW_JSON)
    parser.add_argument("--review-md", type=Path, default=DEFAULT_REVIEW_MD)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
