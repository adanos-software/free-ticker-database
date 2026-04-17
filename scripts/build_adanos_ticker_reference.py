from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import classify_alias_for_natural_language, duplicate_alias_counts

DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
ALIASES_CSV = DATA_DIR / "aliases.csv"
DEFAULT_OUTPUT_DIR = DATA_DIR / "adanos"
DEFAULT_TICKER_REFERENCE_CSV = DEFAULT_OUTPUT_DIR / "ticker_reference.csv"
DEFAULT_NATURAL_ALIASES_CSV = DEFAULT_OUTPUT_DIR / "natural_language_aliases.csv"
DEFAULT_SUMMARY_JSON = DEFAULT_OUTPUT_DIR / "summary.json"

TICKER_REFERENCE_FIELDNAMES = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "country",
    "country_code",
    "isin",
    "aliases",
]
NATURAL_ALIAS_FIELDNAMES = [
    "ticker",
    "alias",
    "alias_type",
    "detection_policy",
    "confidence",
    "reason",
]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def sector_for_api(row: dict[str, str]) -> str:
    return row.get("stock_sector") or row.get("etf_category") or ""


def build_natural_alias_rows(
    tickers: list[dict[str, str]],
    aliases: list[dict[str, str]],
) -> list[dict[str, str]]:
    ticker_lookup = {row["ticker"]: row for row in tickers}
    duplicate_counts = duplicate_alias_counts(aliases)
    rows: list[dict[str, str]] = []

    for alias_row in aliases:
        ticker = alias_row["ticker"]
        ticker_row = ticker_lookup.get(ticker)
        if not ticker_row:
            continue
        decision = classify_alias_for_natural_language(
            alias=alias_row["alias"],
            alias_type=alias_row["alias_type"],
            ticker=ticker,
            duplicate_ticker_count=duplicate_counts.get(alias_row["alias"], 1),
            isin=ticker_row.get("isin", ""),
            wkns=set(),
        )
        if decision.status == "reject":
            continue
        rows.append(
            {
                "ticker": ticker,
                "alias": alias_row["alias"],
                "alias_type": alias_row["alias_type"],
                "detection_policy": decision.detection_policy,
                "confidence": decision.confidence,
                "reason": decision.reason,
            }
        )

    return sorted(rows, key=lambda row: (row["ticker"], row["detection_policy"], row["alias"]))


def build_ticker_reference_rows(
    tickers: list[dict[str, str]],
    natural_aliases: list[dict[str, str]],
) -> list[dict[str, str]]:
    accepted_aliases_by_ticker: dict[str, list[str]] = defaultdict(list)
    for row in natural_aliases:
        if row["detection_policy"] == "safe_natural_language":
            accepted_aliases_by_ticker[row["ticker"]].append(row["alias"])

    rows: list[dict[str, str]] = []
    for ticker in tickers:
        aliases = sorted(dict.fromkeys(accepted_aliases_by_ticker.get(ticker["ticker"], [])))
        rows.append(
            {
                "ticker": ticker["ticker"],
                "name": ticker["name"],
                "exchange": ticker["exchange"],
                "asset_type": ticker["asset_type"],
                "sector": sector_for_api(ticker),
                "country": ticker["country"],
                "country_code": ticker.get("country_code", ""),
                "isin": ticker.get("isin", ""),
                "aliases": json.dumps(aliases, ensure_ascii=False),
            }
        )
    return rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, ticker_reference_rows: list[dict[str, str]], natural_alias_rows: list[dict[str, str]]) -> None:
    policies: dict[str, int] = defaultdict(int)
    for row in natural_alias_rows:
        policies[row["detection_policy"]] += 1
    payload = {
        "generated_at": utc_now(),
        "ticker_reference_rows": len(ticker_reference_rows),
        "natural_language_alias_rows": len(natural_alias_rows),
        "safe_alias_rows": policies.get("safe_natural_language", 0),
        "review_alias_rows": len(natural_alias_rows) - policies.get("safe_natural_language", 0),
        "detection_policy_counts": dict(sorted(policies.items())),
        "files": {
            "ticker_reference_csv": display_path(DEFAULT_TICKER_REFERENCE_CSV),
            "natural_language_aliases_csv": display_path(DEFAULT_NATURAL_ALIASES_CSV),
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Adanos Sentiment API-safe ticker reference exports.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    parser.add_argument("--ticker-reference-csv", type=Path, default=DEFAULT_TICKER_REFERENCE_CSV)
    parser.add_argument("--natural-aliases-csv", type=Path, default=DEFAULT_NATURAL_ALIASES_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = load_csv(args.tickers_csv)
    aliases = load_csv(args.aliases_csv)
    natural_aliases = build_natural_alias_rows(tickers, aliases)
    ticker_reference = build_ticker_reference_rows(tickers, natural_aliases)
    write_csv(args.natural_aliases_csv, NATURAL_ALIAS_FIELDNAMES, natural_aliases)
    write_csv(args.ticker_reference_csv, TICKER_REFERENCE_FIELDNAMES, ticker_reference)
    write_summary(args.summary_json, ticker_reference, natural_aliases)
    print(
        json.dumps(
            {
                "ticker_reference_rows": len(ticker_reference),
                "natural_language_alias_rows": len(natural_aliases),
                "ticker_reference_csv": display_path(args.ticker_reference_csv),
                "natural_aliases_csv": display_path(args.natural_aliases_csv),
                "summary_json": display_path(args.summary_json),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
