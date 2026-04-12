from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, ascii_fold, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "etf_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "name_category_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "name_category_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "category_update",
    "matched_rule",
    "decision",
]
METADATA_UPDATE_FIELDNAMES = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
CLASSIFIER_METADATA_FIELDS = {"sector", "etf_category"}
CLASSIFIER_REASON_PREFIX = "Deterministic ETF-name classifier mapped"


@dataclass(frozen=True)
class CategoryRule:
    category: str
    name: str
    patterns: tuple[re.Pattern[str], ...]


def rule(category: str, name: str, *patterns: str) -> CategoryRule:
    normalized = normalize_sector(category, "ETF")
    if not normalized:
        raise ValueError(f"Unknown ETF category: {category}")
    return CategoryRule(
        category=normalized,
        name=name,
        patterns=tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns),
    )


ETF_CATEGORY_RULES: tuple[CategoryRule, ...] = (
    rule("Money Market Instruments", "money_market", r"\bmoney market\b", r"\bcash\b", "\uba38\ub2c8\ub9c8\ucf13"),
    rule(
        "Trading",
        "trading",
        r"\bleveraged\b",
        r"\binverse\b",
        r"\bbull\b",
        r"\bbear\b",
        r"\b[23]x\b",
        r"\bshort (?!term\b)",
        r"\bcovered call\b",
        r"\bbuffer\b",
        "\ub808\ubc84\ub9ac\uc9c0",
        "\uc778\ubc84\uc2a4",
        "\ucee4\ubc84\ub4dc\ucf5c",
    ),
    rule("Corporate Bonds", "corporate_bonds", r"\bcorporate bonds?\b", "\ud68c\uc0ac\ucc44"),
    rule("Treasury Bonds", "treasury_bonds", r"\btreasur(?:y|ies)\b", r"\bt-?bills?\b", "\uad6d\ucc44"),
    rule("High Yield Bonds", "high_yield_bonds", r"\bhigh yield\b", r"\bjunk bonds?\b"),
    rule("Inflation-Protected Securities", "inflation_protected", r"\btips\b", r"\binflation[- ]protected\b"),
    rule(
        "Investment Grade Bonds",
        "investment_grade_bonds",
        r"\binvestment grade\b",
        r"\bhigh grade\b",
        r"\big bonds?\b",
        r"\baaa[- ]?a\b",
    ),
    rule("Fixed Income", "fixed_income", r"\bfixed income\b", r"\bbonds?\b", r"\bwgbi\b", "\ucc44\uad8c"),
    rule(
        "Commodities Broad Basket",
        "commodities",
        r"\bcommodit(?:y|ies)\b",
        r"\bgold\b",
        r"\bsilver\b",
        r"\bcopper\b",
        r"\boil\b",
        r"\bcrude oil\b",
        r"\bnatural gas\b",
        r"\buranium\b",
        "\uc6d0\uc720",
        "\ucc9c\uc5f0\uac00\uc2a4",
    ),
    rule("Currencies", "currencies", r"\bcurrency(?: basket| etf| fund|shares)\b", r"\bforex\b", r"\bfx\b", r"\bdollar index\b"),
    rule("Small Cap", "small_cap", r"\bsmall[- ]?cap\b"),
    rule("Mid Cap", "mid_cap", r"\bmid[- ]?cap\b"),
    rule(
        "Large Cap",
        "large_cap",
        r"\blarge[- ]?cap\b",
        r"\bs&p ?500\b",
        r"\bnasdaq ?100\b",
        r"\bdow jones(?: industrial average)?\b",
        r"\bdow industrial\b",
        r"\bdjia\b",
        r"\bftse ?100\b",
        r"\bnikkei ?225\b",
    ),
    rule("Growth", "growth", r"\bgrowth\b"),
    rule("Value", "value", r"\bvalue\b", r"\bdividend\b", "\ubc30\ub2f9"),
    rule("Factors", "factors", r"\bfactor\b", r"\bquality\b", r"\bmomentum\b", r"\blow volatility\b", r"\bequal weight\b", r"\besg\b", r"\bsri\b"),
    rule("Emerging Markets", "emerging_markets", r"\bemerging markets?\b", r"\bmsci em\b"),
    rule("Developed Markets", "developed_markets", r"\bdeveloped markets?\b", r"\bmsci world\b", r"\bmsci eafe\b"),
    rule("Health Care", "health_care", r"\bhealth ?care\b", r"\bbiotech\b", r"\bpharma(?:ceuticals?)?\b", r"\bbio\b"),
    rule("Information Technology", "information_technology", r"\btechnology\b", r"\bsemiconductor\b", r"\bsoftware\b", r"\bai\b", r"\brobot"),
    rule("Financials", "financials", r"\bfinancials?\b", r"\bbanks?\b", r"\binsurance\b"),
    rule("Energy", "energy", r"\benergy\b", r"\boil\b", r"\bgas\b"),
    rule("Real Estate", "real_estate", r"\breit\b", r"\breal estate\b"),
    rule("Utilities", "utilities", r"\butilities\b"),
    rule("Materials", "materials", r"\bmaterials\b", r"\bmining\b", r"\bmetals?\b"),
    rule("Consumer Staples", "consumer_staples", r"\bconsumer staples\b", r"\bfood\b"),
    rule("Consumer Discretionary", "consumer_discretionary", r"\bconsumer discretionary\b", r"\bconsumer trends?\b"),
    rule("Communication Services", "communication_services", r"\bcommunication services\b", r"\bmedia\b", r"\btelecom\b"),
    rule("Industrials", "industrials", r"\bindustrials?\b", r"\bmanufactur(?:ing|ers?)\b", r"\binfrastructure\b"),
    rule("Equities", "equities", r"\bequit(?:y|ies)\b", r"\bstocks?\b", r"\bshares?\b", r"\bindex fund\b"),
)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_ticker_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_existing_classifier_update_keys(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {
        (row["ticker"], row["exchange"])
        for row in rows
        if row.get("field") in CLASSIFIER_METADATA_FIELDS and row.get("reason", "").startswith(CLASSIFIER_REASON_PREFIX)
    }


def name_haystack(name: str) -> str:
    return f" {ascii_fold(name).lower()} {name.lower()} "


def classify_etf_category(name: str) -> tuple[str, str]:
    haystack = name_haystack(name)
    for category_rule in ETF_CATEGORY_RULES:
        if any(pattern.search(haystack) for pattern in category_rule.patterns):
            return category_rule.category, category_rule.name
    return "", ""


def evaluate_etf_row(row: dict[str, str]) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "category_update": "",
        "matched_rule": "",
    }
    if row["asset_type"] != "ETF":
        return {**base, "decision": "not_etf"}
    if row.get("sector", "").strip():
        return {**base, "decision": "already_has_category"}

    category, matched_rule = classify_etf_category(row["name"])
    if not category:
        return {**base, "decision": "no_rule_match"}
    return {**base, "category_update": category, "matched_rule": matched_rule, "decision": "accept"}


def verify_etf_categories(
    rows: list[dict[str, str]],
    *,
    exchanges: set[str],
    existing_classifier_update_keys: set[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing_classifier_update_keys = existing_classifier_update_keys or set()
    results: list[dict[str, Any]] = []
    for row in rows:
        key = (row["ticker"], row["exchange"])
        should_refresh_existing_classifier_update = key in existing_classifier_update_keys
        if row["exchange"] not in exchanges or row["asset_type"] != "ETF":
            continue
        if (row.get("etf_category", "") or row.get("sector", "")).strip() and not should_refresh_existing_classifier_update:
            continue
        candidate_row = {**row, "sector": ""} if should_refresh_existing_classifier_update else row
        results.append(evaluate_etf_row(candidate_row))
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "etf_category",
                "decision": "update",
                "proposed_value": result["category_update"],
                "confidence": "0.68",
                "reason": f"Deterministic ETF-name classifier mapped the product name to '{result['category_update']}' via rule '{result['matched_rule']}'. This is a category fill for legacy sector output, not a stock-sector assertion.",
            }
        )
    return updates


def prune_stale_classifier_updates(path: Path, updates: list[dict[str, str]]) -> None:
    if not path.exists():
        return
    current_keys = {(update["ticker"], update["exchange"]) for update in updates}
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    kept_rows = [
        row
        for row in rows
        if not (
            row.get("field") in CLASSIFIER_METADATA_FIELDS
            and row.get("reason", "").startswith(CLASSIFIER_REASON_PREFIX)
            and ((row["ticker"], row["exchange"]) not in current_keys or row.get("field") != "etf_category")
        )
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=METADATA_UPDATE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(kept_rows)


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing ETF categories from deterministic product-name rules.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_ticker_rows(args.tickers_csv)
    exchanges = set(args.exchange or {row["exchange"] for row in rows})
    existing_classifier_update_keys = (
        load_existing_classifier_update_keys(args.metadata_updates_csv) if args.apply else set()
    )
    results = verify_etf_categories(
        rows,
        exchanges=exchanges,
        existing_classifier_update_keys=existing_classifier_update_keys,
    )
    if args.offset:
        results = results[args.offset :]
    if args.limit is not None:
        results = results[: args.limit]
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply:
        prune_stale_classifier_updates(args.metadata_updates_csv, updates)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "candidates": len(results),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "accepted_category_updates": len(updates),
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
