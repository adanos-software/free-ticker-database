from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import (
    ALIASES_CSV,
    COUNTRY_TO_ISO,
    EXCHANGE_TICKER_RE,
    IDENTIFIERS_CSV,
    IDENTIFIER_RE,
    OTC_EXCHANGES,
    TICKERS_CSV,
    VALID_ETF_SECTORS,
    VALID_STOCK_SECTORS,
    alias_matches_company,
    country_from_isin,
    entity_key_for_row,
    has_wrapper_term,
    is_blocked_alias,
    is_company_style_alias,
    is_valid_isin,
    looks_like_identifier,
    normalized_compact,
    normalize_tokens,
    split_aliases,
)


DEFAULT_JSON_OUT = ROOT / "data" / "review_queue.json"
DEFAULT_CSV_OUT = ROOT / "data" / "review_queue.csv"


@dataclass(frozen=True)
class Finding:
    finding_type: str
    severity: str
    score: int
    field: str
    value: str
    reason: str


@dataclass(frozen=True)
class ReviewItem:
    ticker: str
    name: str
    exchange: str
    asset_type: str
    country: str
    country_code: str
    isin: str
    aliases: list[str]
    total_score: int
    findings: list[Finding]


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def add_finding(
    findings_by_ticker: dict[str, list[Finding]],
    ticker: str,
    finding_type: str,
    severity: str,
    score: int,
    field: str,
    value: str,
    reason: str,
) -> None:
    findings_by_ticker[ticker].append(
        Finding(
            finding_type=finding_type,
            severity=severity,
            score=score,
            field=field,
            value=value,
            reason=reason,
        )
    )


def load_identifier_map(identifier_rows: list[dict[str, str]]) -> dict[str, set[str]]:
    identifier_map: dict[str, set[str]] = defaultdict(set)
    for row in identifier_rows:
        wkn = row.get("wkn", "")
        if wkn:
            identifier_map[row["ticker"]].add(wkn)
    return dict(identifier_map)


def looks_like_symbol_alias(alias: str) -> bool:
    if EXCHANGE_TICKER_RE.match(alias):
        return True
    if IDENTIFIER_RE.fullmatch(alias) and alias.upper() == alias:
        return True
    return False


def build_company_alias_groups(
    ticker_rows: list[dict[str, str]],
    alias_rows: list[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    ticker_lookup = {row["ticker"]: row for row in ticker_rows}
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in alias_rows:
        if row["alias_type"] != "name":
            continue
        ticker = row["ticker"]
        ticker_row = ticker_lookup.get(ticker)
        if not ticker_row:
            continue
        alias = row["alias"].strip()
        if not alias:
            continue
        groups[alias.lower()].append(
            {
                "ticker": ticker,
                "entity_key": entity_key_for_row(ticker_row),
                "company_name": ticker_row["name"],
            }
        )

    return groups


def flag_cross_company_alias_collisions(
    ticker_rows: list[dict[str, str]],
    alias_rows: list[dict[str, str]],
    findings_by_ticker: dict[str, list[Finding]],
) -> None:
    alias_groups = build_company_alias_groups(ticker_rows, alias_rows)
    for alias, entries in alias_groups.items():
        entity_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
        for entry in entries:
            entity_groups[entry["entity_key"]].append(entry)

        if len(entity_groups) < 2:
            continue

        matching_entities = {
            entity_key
            for entity_key, entity_entries in entity_groups.items()
            if any(alias_matches_company(alias, entry["company_name"]) for entry in entity_entries)
        }
        if matching_entities:
            flagged_entries = [
                entry
                for entity_key, entity_entries in entity_groups.items()
                if entity_key not in matching_entities
                for entry in entity_entries
            ]
        else:
            flagged_entries = list(entries)

        if not flagged_entries:
            continue

        peer_tickers = sorted({entry["ticker"] for entry in flagged_entries})
        peer_preview = ", ".join(peer_tickers[:6])
        if len(peer_tickers) > 6:
            peer_preview += ", ..."

        for entry in flagged_entries:
            add_finding(
                findings_by_ticker=findings_by_ticker,
                ticker=entry["ticker"],
                finding_type="cross_company_alias_collision",
                severity="high",
                score=45,
                field="alias",
                value=alias,
                reason=f"Alias is shared across different companies: {peer_preview}",
            )


def analyze_dataset(
    ticker_rows: list[dict[str, str]],
    alias_rows: list[dict[str, str]],
    identifier_rows: list[dict[str, str]],
    min_score: int = 40,
) -> list[ReviewItem]:
    ticker_lookup = {row["ticker"]: row for row in ticker_rows}
    wkn_lookup = load_identifier_map(identifier_rows)
    findings_by_ticker: dict[str, list[Finding]] = defaultdict(list)

    flag_cross_company_alias_collisions(ticker_rows, alias_rows, findings_by_ticker)

    for row in ticker_rows:
        ticker = row["ticker"]
        if row["country"] and not row["country_code"]:
            add_finding(
                findings_by_ticker,
                ticker,
                "missing_country_code",
                "high",
                60,
                "country_code",
                "",
                "Country is present but ISO country_code is missing.",
            )

        if row["isin"] and not is_valid_isin(row["isin"]):
            add_finding(
                findings_by_ticker,
                ticker,
                "invalid_isin",
                "critical",
                90,
                "isin",
                row["isin"],
                "ISIN fails format or Luhn checksum validation.",
            )

        inferred_country = country_from_isin(row["isin"]) if row["isin"] else None
        if inferred_country and inferred_country != row["country"]:
            add_finding(
                findings_by_ticker,
                ticker,
                "country_isin_mismatch",
                "high",
                70,
                "country",
                row["country"],
                f"Country disagrees with ISIN prefix country {inferred_country}.",
            )

        if row["asset_type"] == "Stock" and row["sector"] and row["sector"] not in VALID_STOCK_SECTORS:
            add_finding(
                findings_by_ticker,
                ticker,
                "invalid_stock_sector",
                "high",
                60,
                "sector",
                row["sector"],
                "Sector is not a canonical stock GICS sector.",
            )

        if row["asset_type"] == "ETF" and row["sector"] and row["sector"] not in VALID_ETF_SECTORS:
            add_finding(
                findings_by_ticker,
                ticker,
                "invalid_etf_sector",
                "high",
                60,
                "sector",
                row["sector"],
                "Sector is not a standardized ETF category.",
            )

        if row["exchange"] in OTC_EXCHANGES and not row["isin"] and not row["sector"]:
            add_finding(
                findings_by_ticker,
                ticker,
                "thin_otc_metadata",
                "medium",
                20,
                "metadata",
                "",
                "OTC entry has neither ISIN nor sector and should be externally verified.",
            )

    for row in alias_rows:
        ticker = row["ticker"]
        ticker_row = ticker_lookup.get(ticker)
        if not ticker_row:
            continue

        alias = row["alias"].strip()
        alias_type = row["alias_type"]
        lowered = alias.lower()
        wkns = wkn_lookup.get(ticker, set())
        isin = ticker_row["isin"]

        if alias_type == "name" and is_blocked_alias(alias):
            add_finding(
                findings_by_ticker,
                ticker,
                "blocked_alias_present",
                "critical",
                90,
                "alias",
                alias,
                "Alias matches a blocked common-word, contaminated, generic fund-wrapper, or obviously ambiguous term.",
            )

        if alias_type == "name" and has_wrapper_term(alias):
            add_finding(
                findings_by_ticker,
                ticker,
                "wrapper_alias_present",
                "high",
                70,
                "alias",
                alias,
                "Alias contains wrapper terminology such as CDR, DRC, or CEDEAR.",
            )

        if alias_type == "name" and len(alias) <= 2 and not looks_like_identifier(alias, wkns, isin):
            add_finding(
                findings_by_ticker,
                ticker,
                "short_name_alias",
                "high",
                70,
                "alias",
                alias,
                "Very short name alias is too ambiguous for high-quality lookup.",
            )

        if alias_type == "name" and alias.isdigit() and not looks_like_identifier(alias, wkns, isin):
            add_finding(
                findings_by_ticker,
                ticker,
                "numeric_name_alias",
                "high",
                70,
                "alias",
                alias,
                "Pure-numeric alias should not be stored as a name alias.",
            )

        if (
            alias_type == "name"
            and not looks_like_identifier(alias, wkns, isin)
            and not looks_like_symbol_alias(alias)
            and alias
            and not is_company_style_alias(alias)
            and not alias_matches_company(alias, ticker_row["name"])
        ):
            add_finding(
                findings_by_ticker,
                ticker,
                "low_company_name_overlap",
                "low",
                15,
                "alias",
                alias,
                "Alias has weak lexical overlap with the company or fund name and may require review.",
            )

    review_items: list[ReviewItem] = []
    for ticker, findings in findings_by_ticker.items():
        total_score = sum(finding.score for finding in findings)
        if total_score < min_score:
            continue

        row = ticker_lookup[ticker]
        review_items.append(
            ReviewItem(
                ticker=ticker,
                name=row["name"],
                exchange=row["exchange"],
                asset_type=row["asset_type"],
                country=row["country"],
                country_code=row["country_code"],
                isin=row["isin"],
                aliases=split_aliases(row["aliases"]),
                total_score=total_score,
                findings=sorted(findings, key=lambda finding: (-finding.score, finding.finding_type)),
            )
        )

    return sorted(review_items, key=lambda item: (-item.total_score, item.ticker))


def review_items_to_json(review_items: list[ReviewItem], min_score: int) -> dict[str, object]:
    summary_by_type = Counter(
        finding.finding_type
        for item in review_items
        for finding in item.findings
    )
    return {
        "_meta": {
            "min_score": min_score,
            "flagged_entries": len(review_items),
            "generated_from": {
                "tickers_csv": str(TICKERS_CSV.relative_to(ROOT)),
                "aliases_csv": str(ALIASES_CSV.relative_to(ROOT)),
                "identifiers_csv": str(IDENTIFIERS_CSV.relative_to(ROOT)),
            },
        },
        "summary": dict(sorted(summary_by_type.items())),
        "items": [
            {
                **{key: value for key, value in asdict(item).items() if key != "findings"},
                "findings": [asdict(finding) for finding in item.findings],
            }
            for item in review_items
        ],
    }


def write_review_csv(path: Path, review_items: list[ReviewItem]) -> None:
    rows: list[dict[str, str | int]] = []
    for item in review_items:
        alias_blob = "|".join(item.aliases)
        for finding in item.findings:
            rows.append(
                {
                    "ticker": item.ticker,
                    "name": item.name,
                    "exchange": item.exchange,
                    "asset_type": item.asset_type,
                    "country": item.country,
                    "country_code": item.country_code,
                    "isin": item.isin,
                    "aliases": alias_blob,
                    "total_score": item.total_score,
                    "finding_type": finding.finding_type,
                    "severity": finding.severity,
                    "finding_score": finding.score,
                    "field": finding.field,
                    "value": finding.value,
                    "reason": finding.reason,
                }
            )

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "name",
                "exchange",
                "asset_type",
                "country",
                "country_code",
                "isin",
                "aliases",
                "total_score",
                "finding_type",
                "severity",
                "finding_score",
                "field",
                "value",
                "reason",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a scored review queue for suspicious dataset entries.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--aliases-csv", type=Path, default=ALIASES_CSV)
    parser.add_argument("--identifiers-csv", type=Path, default=IDENTIFIERS_CSV)
    parser.add_argument("--min-score", type=int, default=40)
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--csv-out", type=Path, default=None)
    parser.add_argument(
        "--write-defaults",
        action="store_true",
        help=f"Write {DEFAULT_JSON_OUT.relative_to(ROOT)} and {DEFAULT_CSV_OUT.relative_to(ROOT)}.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    tickers = load_csv(args.tickers_csv)
    aliases = load_csv(args.aliases_csv)
    identifiers = load_csv(args.identifiers_csv)
    review_items = analyze_dataset(tickers, aliases, identifiers, min_score=args.min_score)
    payload = review_items_to_json(review_items, args.min_score)

    if args.write_defaults:
        json_out = DEFAULT_JSON_OUT
        csv_out = DEFAULT_CSV_OUT
    else:
        json_out = args.json_out
        csv_out = args.csv_out

    if json_out:
        json_out.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
    if csv_out:
        write_review_csv(csv_out, review_items)

    finding_count = sum(len(item.findings) for item in review_items)
    print(
        json.dumps(
            {
                "flagged_entries": len(review_items),
                "findings": finding_count,
                "min_score": args.min_score,
                "json_out": str(json_out.relative_to(ROOT)) if json_out else None,
                "csv_out": str(csv_out.relative_to(ROOT)) if csv_out else None,
                "finding_types": payload["summary"],
                "top_tickers": [item.ticker for item in review_items[:10]],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
