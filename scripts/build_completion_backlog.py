from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
TICKERS_CSV = DATA_DIR / "tickers.csv"
INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "completion_backlog.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "completion_backlog.json"
DEFAULT_MD_OUT = REPORTS_DIR / "completion_backlog.md"

FIELD_MISSING_ISIN = "missing_isin_primary"
FIELD_MISSING_STOCK_SECTOR = "missing_sector_stock"
FIELD_MISSING_ETF_CATEGORY = "missing_etf_category"

CSV_FIELDNAMES = [
    "priority_rank",
    "priority_bucket",
    "exchange",
    "asset_type",
    "field",
    "target_field",
    "missing_count",
    "stock_missing_count",
    "etf_missing_count",
    "venue_status",
    "official_source_count",
    "reference_scopes",
    "recommended_source",
    "script",
    "review_needed",
    "confidence_policy",
    "notes",
]

ISIN_PRIORITY = ["TSE", "SSE", "TSX", "TSXV", "SZSE", "B3"]
SECTOR_CATEGORY_PRIORITY = ["OTC", "SSE", "SZSE", "XETRA", "B3", "NYSE ARCA", "KRX", "LSE", "TSX"]


@dataclass(frozen=True)
class CompletionBacklogRow:
    priority_rank: int
    priority_bucket: str
    exchange: str
    asset_type: str
    field: str
    target_field: str
    missing_count: int
    stock_missing_count: int
    etf_missing_count: int
    venue_status: str
    official_source_count: int
    reference_scopes: str
    recommended_source: str
    script: str
    review_needed: bool
    confidence_policy: str
    notes: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_venue_lookup(coverage_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in coverage_report.get("by_exchange", []):
        exchange = row.get("exchange", "")
        if exchange:
            lookup[exchange] = row
    return lookup


def policy_for(field: str, exchange: str, asset_type: str) -> tuple[str, str, bool, str, str]:
    if field == FIELD_MISSING_ISIN:
        if exchange == "TSE":
            return (
                "Official JPX/TSE ISIN-capable source; current JPX listed-issues coverage does not fill most ISINs.",
                "scripts/fetch_exchange_masterfiles.py --source <new_tse_isin_source>",
                True,
                "Official source preferred; require listing-key/ticker, issuer-name, asset-type, expected JP prefix, and ISIN checksum gates before applying.",
                "Largest primary ISIN gap; do not use Yahoo as the TSE authority.",
            )
        if exchange in {"SSE", "SZSE"}:
            return (
                "Official SSE/SZSE share and ETF feeds first; reviewed EODHD/XTB fallback only for unresolved rows.",
                "scripts/fetch_exchange_masterfiles.py --source <sse_or_szse_feed>; scripts/backfill_eodhd_metadata.py",
                True,
                "Official exchange feed can be accepted after exact symbol/share-class and checksum gates; secondary feeds require audit reports and reviewed overrides.",
                "China gaps are mixed stocks and ETFs; handle asset types separately.",
            )
        if exchange in {"TSX", "TSXV", "NEO"}:
            return (
                "TMX official issuer/ETF feeds first; EODHD and strict Yahoo only as reviewed fallbacks.",
                "scripts/fetch_exchange_masterfiles.py --source <tmx_feed>; scripts/backfill_eodhd_metadata.py; scripts/backfill_yahoo_missing_isins.py",
                True,
                "Secondary EODHD/Yahoo candidates must remain review-gated with venue, asset-type, name, expected CA prefix, numeric-token, and checksum checks.",
                "Run Canada as one package because TSX, TSXV, and NEO share products and suffix conventions.",
            )
        if exchange == "B3":
            return (
                "Official B3 InstrumentsEquities first; FinanceDatabase reviewed fallback for residual identifiers.",
                "scripts/fetch_exchange_masterfiles.py --source b3_instruments_equities; scripts/backfill_financedatabase_metadata.py --enable-isin",
                True,
                "Official B3 rows can be accepted after exact code and checksum gates; FinanceDatabase ISINs require peer-conflict review.",
                "B3 has strong official coverage but residual identifier and category gaps.",
            )
        if exchange in {"ASX"}:
            return (
                "Official ASX ISIN workbook.",
                "scripts/backfill_asx_isins.py",
                False,
                "Accept only after official ASX code, issuer-name, numeric-token, and checksum gates match.",
                "Official workbook flow already exists.",
            )
        if exchange in {"BATS", "NASDAQ", "NYSE", "NYSE ARCA"}:
            return (
                "Official US exchange directories where available; EODHD or strict Yahoo for reviewed ETF residuals.",
                "scripts/backfill_eodhd_metadata.py; scripts/backfill_yahoo_missing_isins.py",
                True,
                "Broker/API candidates must write audit reports and pass venue, type, name, expected US prefix, numeric-token, and checksum gates before reviewed apply.",
                "Residual US gaps are mostly ETF tails; keep in small batches.",
            )
        return (
            "Official exchange masterfile or reviewed secondary identifier source.",
            "scripts/fetch_exchange_masterfiles.py; scripts/backfill_eodhd_metadata.py",
            True,
            "Prefer official exchange data; secondary identifier candidates require audit report, source label, expected country prefix, name, and checksum gates.",
            "Source research needed before applying.",
        )

    if field == FIELD_MISSING_STOCK_SECTOR:
        if exchange == "OTC":
            return (
                "SEC SIC, Alpha Vantage OVERVIEW, and FinanceDatabase as reviewed stock-sector signals.",
                "scripts/backfill_sec_sic_sectors.py; scripts/backfill_alphavantage_sectors.py; scripts/backfill_financedatabase_metadata.py",
                True,
                "Sector must map to canonical stock GICS sector; secondary sources require ticker/exchange/name gates and audit output.",
                "OTC is noisy; do not apply thin-name sector guesses without issuer evidence.",
            )
        if exchange == "TSE":
            return (
                "Official JPX listed-issues sector mapping.",
                "scripts/backfill_jpx_tse_sectors.py",
                False,
                "Accept after exact TSE code and official JPX sector normalization to canonical stock sectors.",
                "Existing official sector helper should cover most TSE stock residuals.",
            )
        if exchange in {"B3", "XETRA", "LSE", "TSX", "TSXV", "STO", "TASE", "KOSDAQ", "KRX"}:
            return (
                "FinanceDatabase and same-ISIN peer propagation, with official industry feeds preferred when available.",
                "scripts/backfill_financedatabase_metadata.py; scripts/backfill_sector_from_isin_peers.py",
                True,
                "Accept only canonical stock sectors; FinanceDatabase requires exchange/name gates, same-ISIN peers require unanimous same-asset sector.",
                "Good candidate for deterministic local review batches.",
            )
        if exchange in {"SSE", "SZSE", "IDX", "ASX", "PSX", "TWSE"}:
            return (
                "Official exchange industry classifications first; FinanceDatabase as reviewed fallback.",
                "scripts/fetch_exchange_masterfiles.py; scripts/backfill_financedatabase_metadata.py",
                True,
                "Official classifications can be normalized directly; secondary sectors require exchange/name gates and audit output.",
                "Venue-specific taxonomy mapping may be needed.",
            )
        return (
            "Official industry classification or reviewed FinanceDatabase sector fallback.",
            "scripts/fetch_exchange_masterfiles.py; scripts/backfill_financedatabase_metadata.py",
            True,
            "Accept only canonical stock sectors after source-specific normalization and issuer/name gates.",
            "Source research needed before applying.",
        )

    if field == FIELD_MISSING_ETF_CATEGORY:
        return (
            "Same-ISIN peer propagation plus a reviewed ETF-name category classifier; official fund category feeds where available.",
            "scripts/backfill_sector_from_isin_peers.py; <planned scripts/backfill_etf_categories_from_names.py>",
            True,
            "ETF categories must be stored as etf_category internally and surfaced through legacy sector only after deterministic taxonomy mapping.",
            f"{asset_type} rows should target etf_category, not stock_sector.",
        )

    return (
        "Source research needed.",
        "",
        True,
        "No automatic updates without a source-specific audit policy.",
        "",
    )


def target_field_for(field: str) -> str:
    if field == FIELD_MISSING_ISIN:
        return "isin"
    if field == FIELD_MISSING_STOCK_SECTOR:
        return "stock_sector"
    if field == FIELD_MISSING_ETF_CATEGORY:
        return "etf_category"
    return field


def priority_for(field: str, exchange: str, missing_count: int) -> tuple[int, str]:
    priority = ISIN_PRIORITY if field == FIELD_MISSING_ISIN else SECTOR_CATEGORY_PRIORITY
    if exchange in priority:
        return priority.index(exchange) + 1, "top_priority"
    return len(priority) + 1, "ranked_by_missing_count"


def make_row(
    *,
    field: str,
    exchange: str,
    asset_type: str,
    missing_count: int,
    stock_missing_count: int,
    etf_missing_count: int,
    venue: dict[str, Any],
) -> CompletionBacklogRow:
    recommended_source, script, review_needed, confidence_policy, notes = policy_for(field, exchange, asset_type)
    priority_rank, priority_bucket = priority_for(field, exchange, missing_count)
    return CompletionBacklogRow(
        priority_rank=priority_rank,
        priority_bucket=priority_bucket,
        exchange=exchange,
        asset_type=asset_type,
        field=field,
        target_field=target_field_for(field),
        missing_count=missing_count,
        stock_missing_count=stock_missing_count,
        etf_missing_count=etf_missing_count,
        venue_status=venue.get("venue_status", "missing"),
        official_source_count=int(venue.get("official_source_count") or 0),
        reference_scopes="|".join(venue.get("reference_scopes", []) or []),
        recommended_source=recommended_source,
        script=script,
        review_needed=review_needed,
        confidence_policy=confidence_policy,
        notes=notes,
    )


def build_completion_backlog(
    ticker_rows: list[dict[str, str]],
    scope_rows: list[dict[str, str]],
    coverage_report: dict[str, Any],
) -> list[CompletionBacklogRow]:
    venues = build_venue_lookup(coverage_report)
    isin_counts: dict[str, Counter[str]] = defaultdict(Counter)
    stock_sector_counts: Counter[str] = Counter()
    etf_category_counts: Counter[str] = Counter()

    for row in scope_rows:
        if row.get("scope_reason") != "primary_listing_missing_isin":
            continue
        exchange = row.get("exchange", "")
        asset_type = row.get("asset_type", "")
        if exchange and asset_type:
            isin_counts[exchange][asset_type] += 1

    for row in ticker_rows:
        if row.get("sector", "").strip():
            continue
        exchange = row.get("exchange", "")
        if not exchange:
            continue
        asset_type = row.get("asset_type", "")
        if asset_type == "Stock":
            stock_sector_counts[exchange] += 1
        elif asset_type == "ETF":
            etf_category_counts[exchange] += 1

    rows: list[CompletionBacklogRow] = []
    for exchange, asset_counts in isin_counts.items():
        stock_count = asset_counts.get("Stock", 0)
        etf_count = asset_counts.get("ETF", 0)
        total = stock_count + etf_count
        if total:
            rows.append(
                make_row(
                    field=FIELD_MISSING_ISIN,
                    exchange=exchange,
                    asset_type="All",
                    missing_count=total,
                    stock_missing_count=stock_count,
                    etf_missing_count=etf_count,
                    venue=venues.get(exchange, {}),
                )
            )

    for exchange, count in stock_sector_counts.items():
        rows.append(
            make_row(
                field=FIELD_MISSING_STOCK_SECTOR,
                exchange=exchange,
                asset_type="Stock",
                missing_count=count,
                stock_missing_count=count,
                etf_missing_count=0,
                venue=venues.get(exchange, {}),
            )
        )

    for exchange, count in etf_category_counts.items():
        rows.append(
            make_row(
                field=FIELD_MISSING_ETF_CATEGORY,
                exchange=exchange,
                asset_type="ETF",
                missing_count=count,
                stock_missing_count=0,
                etf_missing_count=count,
                venue=venues.get(exchange, {}),
            )
        )

    return rank_backlog_rows(rows)


def rank_backlog_rows(rows: list[CompletionBacklogRow]) -> list[CompletionBacklogRow]:
    ranked_rows: list[CompletionBacklogRow] = []
    fields = [FIELD_MISSING_ISIN, FIELD_MISSING_STOCK_SECTOR, FIELD_MISSING_ETF_CATEGORY]
    for field in fields:
        field_rows = sorted(
            [row for row in rows if row.field == field],
            key=lambda row: (row.priority_rank, -row.missing_count, row.exchange, row.asset_type),
        )
        for rank, row in enumerate(field_rows, start=1):
            ranked_rows.append(replace(row, priority_rank=rank))
    return ranked_rows


def summarize(rows: list[CompletionBacklogRow], coverage_report: dict[str, Any], generated_at: str) -> dict[str, Any]:
    field_totals = Counter()
    exchanges_by_field: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        field_totals[row.field] += row.missing_count
        exchanges_by_field[row.field].add(row.exchange)

    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "field_totals": dict(sorted(field_totals.items())),
        "exchanges_by_field": {field: len(exchanges) for field, exchanges in sorted(exchanges_by_field.items())},
        "official_masterfile_collisions": coverage_report.get("global", {}).get("official_masterfile_collisions", 0),
        "model_notes": {
            "sector_split": "Use stock_sector for stock rows and etf_category for ETF rows; keep sector as legacy derived output.",
            "listing_key_first": "Use listing_key as the internal identity for future full-universe work; global ticker uniqueness still blocks official symbol collisions.",
            "source_blocks": ["TSE ISIN", "China ETF/Sector", "Canada", "B3", "XETRA/LSE ETF categories", "missing venues"],
        },
    }


def rows_to_dicts(rows: list[CompletionBacklogRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def write_csv(path: Path, rows: list[CompletionBacklogRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["review_needed"] = "true" if row.review_needed else "false"
            writer.writerow(payload)


def format_priority_table(rows: list[CompletionBacklogRow], field: str, limit: int = 10) -> str:
    selected = sorted(
        [row for row in rows if row.field == field],
        key=lambda row: (row.priority_rank, -row.missing_count, row.exchange),
    )[:limit]
    if not selected:
        return "_No rows._\n"
    lines = [
        "| Rank | Exchange | Asset type | Missing | Venue | Source | Review |",
        "|---|---|---|---:|---|---|---|",
    ]
    for row in selected:
        review = "yes" if row.review_needed else "no"
        lines.append(
            f"| {row.priority_rank} | {row.exchange} | {row.asset_type} | {row.missing_count} | "
            f"{row.venue_status} | {row.recommended_source} | {review} |"
        )
    return "\n".join(lines) + "\n"


def format_combined_sector_table(rows: list[CompletionBacklogRow], limit: int = 12) -> str:
    counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"stock": 0, "etf": 0, "venue_status": "missing"})
    for row in rows:
        if row.field not in {FIELD_MISSING_STOCK_SECTOR, FIELD_MISSING_ETF_CATEGORY}:
            continue
        counts[row.exchange]["venue_status"] = row.venue_status
        if row.field == FIELD_MISSING_STOCK_SECTOR:
            counts[row.exchange]["stock"] += row.missing_count
        else:
            counts[row.exchange]["etf"] += row.missing_count
    priority_lookup = {exchange: index + 1 for index, exchange in enumerate(SECTOR_CATEGORY_PRIORITY)}
    selected = sorted(
        counts.items(),
        key=lambda item: (priority_lookup.get(item[0], 1000), -(item[1]["stock"] + item[1]["etf"]), item[0]),
    )[:limit]
    if not selected:
        return "_No rows._\n"
    lines = [
        "| Rank | Exchange | Missing total | Missing stock_sector | Missing etf_category | Venue |",
        "|---|---|---:|---:|---:|---|",
    ]
    for rank, (exchange, payload) in enumerate(selected, start=1):
        total = payload["stock"] + payload["etf"]
        lines.append(
            f"| {rank} | {exchange} | {total} | {payload['stock']} | {payload['etf']} | {payload['venue_status']} |"
        )
    return "\n".join(lines) + "\n"


def render_markdown(rows: list[CompletionBacklogRow], summary: dict[str, Any]) -> str:
    field_totals = summary["field_totals"]
    return "\n".join(
        [
            "# Completion Backlog",
            "",
            f"Generated at: `{summary['generated_at']}`",
            "",
            "## Summary",
            "",
            f"- Missing primary ISIN rows: `{field_totals.get(FIELD_MISSING_ISIN, 0)}`",
            f"- Missing stock sectors: `{field_totals.get(FIELD_MISSING_STOCK_SECTOR, 0)}`",
            f"- Missing ETF categories: `{field_totals.get(FIELD_MISSING_ETF_CATEGORY, 0)}`",
            f"- Official symbol collisions blocking global-unique ticker ingestion: `{summary['official_masterfile_collisions']}`",
            "",
            "## Top Missing Primary ISINs",
            "",
            format_priority_table(rows, FIELD_MISSING_ISIN, limit=12),
            "## Top Missing Stock Sectors",
            "",
            format_priority_table(rows, FIELD_MISSING_STOCK_SECTOR, limit=12),
            "## Top Missing ETF Categories",
            "",
            format_priority_table(rows, FIELD_MISSING_ETF_CATEGORY, limit=12),
            "## Combined Sector/ETF Category Priority",
            "",
            format_combined_sector_table(rows, limit=12),
            "## Model Migration Prep",
            "",
            "- `stock_sector` should become the internal target for stock sector backfills.",
            "- `etf_category` should become the internal target for ETF category backfills.",
            "- The existing `sector` field should remain as a legacy derived output until downstream consumers migrate.",
            "- Future full-universe ingestion should be `listing_key`-first because official symbol collisions still block global-unique ticker ingestion.",
            "",
            "## Source Block Order",
            "",
            "1. TSE ISIN",
            "2. China ETF/Sector",
            "3. Canada",
            "4. B3",
            "5. XETRA/LSE ETF categories",
            "6. Missing venues",
            "",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the field-level ticker database completion backlog.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    generated_at = utc_now_iso()
    coverage_report = load_json(args.coverage_report_json)
    rows = build_completion_backlog(
        load_csv(args.tickers_csv),
        load_csv(args.instrument_scopes_csv),
        coverage_report,
    )
    summary = summarize(rows, coverage_report, generated_at)

    write_csv(args.csv_out, rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": rows_to_dicts(rows)}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(render_markdown(rows, summary), encoding="utf-8")

    print(
        json.dumps(
            {
                **summary,
                "csv_out": str(args.csv_out.relative_to(ROOT)),
                "json_out": str(args.json_out.relative_to(ROOT)),
                "md_out": str(args.md_out.relative_to(ROOT)),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
