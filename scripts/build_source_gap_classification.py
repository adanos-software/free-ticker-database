from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_completion_backlog import (
    FIELD_MISSING_ETF_CATEGORY,
    FIELD_MISSING_ISIN,
    FIELD_MISSING_STOCK_SECTOR,
)

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_CORE_LISTINGS_CSV = DATA_DIR / "core_listings.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "source_gap_classification.json"
DEFAULT_MD_OUT = REPORTS_DIR / "source_gap_classification.md"

GAP_CLASS_POLICIES = {
    "adr_cdr_or_depositary_identifier_gap": "Do not infer ISIN from the underlying issuer; require the depositary/CDR program ISIN from an official listing, prospectus, depository, or reviewed identifier feed.",
    "capital_pool_or_halted_identifier_gap": "Require current exchange status and a direct identifier source; keep unresolved if the symbol is halted, suspended, or a capital-pool shell without a current ISIN source.",
    "debt_or_securitized_identifier_gap": "Require official debt/structured-product terms, exchange instrument file, or trustee/prospectus identifier; do not fill from issuer equity ISINs.",
    "fund_or_trust_identifier_gap": "Require fund/trust prospectus, exchange fund masterfile, or reviewed identifier feed; do not propagate from manager or parent-company ISINs.",
    "inactive_or_legacy_identifier_gap": "Require evidence that the listing is active/current and an official identifier source; otherwise keep classified as a source gap.",
    "official_identifier_source_gap": "Require official exchange masterfile/detail feed or a reviewed secondary identifier source with exact symbol, name, country-prefix, and checksum gates.",
    "otc_sector_source_gap": "Require SEC SIC, issuer filing, OTCMarkets profile, or reviewed secondary profile data; no name-only sector guesses.",
    "exchange_industry_source_gap": "Require official exchange industry classification or reviewed secondary profile data mapped to canonical stock sectors.",
    "fundlike_stock_sector_gap": "Require issuer classification; do not map fund/trust-like stock rows from product-name tokens alone.",
    "shell_or_cpc_sector_gap": "Require current issuer business classification; shell, SPAC, and capital-pool rows must stay unfilled without reviewed evidence.",
    "commodity_etf_category_gap": "Require official fund asset class or deterministic product evidence; commodity tokens alone need review if ambiguous.",
    "digital_asset_etf_category_gap": "Require official fund asset class or deterministic product evidence for digital-asset exposure.",
    "fixed_income_etf_category_gap": "Require official fund asset class or deterministic bond/treasury product evidence.",
    "equity_etf_category_gap": "Require official fund asset class or deterministic equity-index product evidence.",
    "generic_etf_category_source_gap": "Require official fund category/asset class or reviewed deterministic product-name classifier evidence.",
}

CSV_FIELDNAMES = [
    "field",
    "target_field",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "gap_class",
    "review_needed",
    "confidence_policy",
    "recommended_next_source",
    "source_gate",
]


@dataclass(frozen=True)
class SourceGapClassificationRow:
    field: str
    target_field: str
    listing_key: str
    ticker: str
    exchange: str
    asset_type: str
    name: str
    gap_class: str
    review_needed: bool
    confidence_policy: str
    recommended_next_source: str
    source_gate: str


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalized_blob(row: dict[str, str]) -> str:
    return f"{row.get('ticker', '')} {row.get('name', '')}".upper()


def has_any(value: str, terms: tuple[str, ...]) -> bool:
    return any(term in value for term in terms)


def target_field_for(field: str) -> str:
    if field == FIELD_MISSING_ISIN:
        return "isin"
    if field == FIELD_MISSING_STOCK_SECTOR:
        return "stock_sector"
    if field == FIELD_MISSING_ETF_CATEGORY:
        return "etf_category"
    return field


def classify_missing_isin(row: dict[str, str]) -> tuple[str, str, str]:
    blob = normalized_blob(row)
    ticker = row.get("ticker", "").upper()
    exchange = row.get("exchange", "")
    if has_any(blob, (" CDR", "CAD HEDGED", "DEPOSITARY", " ADR", " BDR", " RECEIPT")):
        return (
            "adr_cdr_or_depositary_identifier_gap",
            "Depositary/CDR program identifier source, not underlying equity ISIN.",
            "Exact program symbol, issuer/program name, expected country prefix, and ISIN checksum.",
        )
    if "-H" in ticker or ticker.endswith(".H") or ticker.endswith(".P") or "CAPITAL POOL" in blob:
        return (
            "capital_pool_or_halted_identifier_gap",
            "Current exchange issuer/status file or CPC/shell prospectus.",
            "Exact halted/CPC symbol and direct current identifier evidence.",
        )
    if has_any(
        blob,
        (
            " ABS ",
            " ASSET BACKED",
            " MORTGAGE",
            " RECEIVABLE",
            " SERIES ",
            " NOTES",
            " NOTE ",
            " BOND",
            " DEBENTURE",
            " CERTIFICATE",
            " EMTN",
            " TRUST 20",
        ),
    ):
        return (
            "debt_or_securitized_identifier_gap",
            "Official debt/structured-product masterfile, trustee/prospectus, or reviewed identifier feed.",
            "Exact instrument code/name and ISIN checksum; never issuer-equity propagation.",
        )
    if row.get("asset_type") == "ETF" or has_any(blob, (" ETF", " FUND", " FII", " FDO ", " TRUST", " INCOME FUND")):
        return (
            "fund_or_trust_identifier_gap",
            "Official fund/trust masterfile, prospectus, or reviewed identifier feed.",
            "Exact fund/trust symbol and product name with checksum.",
        )
    if exchange in {"B3", "TSXV", "ASX"} and has_any(blob, (" S.A.", " S/A", " LTD", " LIMITED")):
        return (
            "inactive_or_legacy_identifier_gap",
            "Current exchange status/detail feed before any identifier fill.",
            "Exact active listing evidence plus direct identifier source.",
        )
    return (
        "official_identifier_source_gap",
        "Official exchange detail feed or reviewed secondary identifier source.",
        "Exact exchange/symbol/name, expected country prefix where applicable, and checksum.",
    )


def classify_missing_stock_sector(row: dict[str, str]) -> tuple[str, str, str]:
    blob = normalized_blob(row)
    exchange = row.get("exchange", "")
    if exchange == "OTC":
        return (
            "otc_sector_source_gap",
            "SEC SIC, issuer filings, OTCMarkets profile, or reviewed secondary company profile.",
            "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
        )
    if has_any(blob, (" ACQUISITION", " SPAC", " CAPITAL POOL", " HOLDINGS CORP", " VENTURES CORP")):
        return (
            "shell_or_cpc_sector_gap",
            "Current issuer business description or official industry classification.",
            "Canonical sector requires active operating-business evidence.",
        )
    if has_any(blob, (" TRUST", " FUND", " INCOME", " ROYALTIES", " REIT")):
        return (
            "fundlike_stock_sector_gap",
            "Issuer classification from official exchange or reviewed profile source.",
            "Canonical stock sector only with issuer-level classification evidence.",
        )
    return (
        "exchange_industry_source_gap",
        "Official exchange industry feed or reviewed secondary company profile.",
        "Exact exchange/symbol/name mapped to canonical stock sector.",
    )


def classify_missing_etf_category(row: dict[str, str]) -> tuple[str, str, str]:
    blob = normalized_blob(row)
    if has_any(blob, (" BITCOIN", " ETHER", " ETHEREUM", " CRYPTO", " DIGITAL")):
        return (
            "digital_asset_etf_category_gap",
            "Official fund asset class or reviewed product profile.",
            "Exact ETF symbol/name and deterministic digital-asset exposure evidence.",
        )
    if has_any(blob, (" GOLD", " SILVER", " OIL", " GAS", " COMMODITY", " COPPER")):
        return (
            "commodity_etf_category_gap",
            "Official fund asset class or reviewed product profile.",
            "Exact ETF symbol/name and deterministic commodity exposure evidence.",
        )
    if has_any(blob, (" BOND", " TREASURY", " FIXED INCOME", " CREDIT", " CASH", " MONEY MARKET", " GOVT")):
        return (
            "fixed_income_etf_category_gap",
            "Official fund asset class or reviewed product profile.",
            "Exact ETF symbol/name and deterministic fixed-income exposure evidence.",
        )
    if has_any(blob, (" INDEX", " EQUITY", " SHARE", " STOCK", " 500", " CSI", " MSCI", " FTSE")):
        return (
            "equity_etf_category_gap",
            "Official fund asset class or reviewed product profile.",
            "Exact ETF symbol/name and deterministic equity exposure evidence.",
        )
    return (
        "generic_etf_category_source_gap",
        "Official fund category/asset-class feed or reviewed deterministic product-name classifier.",
        "Exact ETF symbol/name and canonical etf_category mapping evidence.",
    )


def make_row(field: str, row: dict[str, str]) -> SourceGapClassificationRow:
    if field == FIELD_MISSING_ISIN:
        gap_class, next_source, source_gate = classify_missing_isin(row)
    elif field == FIELD_MISSING_STOCK_SECTOR:
        gap_class, next_source, source_gate = classify_missing_stock_sector(row)
    elif field == FIELD_MISSING_ETF_CATEGORY:
        gap_class, next_source, source_gate = classify_missing_etf_category(row)
    else:
        raise ValueError(f"Unsupported field: {field}")
    return SourceGapClassificationRow(
        field=field,
        target_field=target_field_for(field),
        listing_key=row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}",
        ticker=row.get("ticker", ""),
        exchange=row.get("exchange", ""),
        asset_type=row.get("asset_type", ""),
        name=clean_text(row.get("name", "")),
        gap_class=gap_class,
        review_needed=True,
        confidence_policy=GAP_CLASS_POLICIES[gap_class],
        recommended_next_source=next_source,
        source_gate=source_gate,
    )


def build_source_gap_classifications(
    *,
    tickers: list[dict[str, str]],
    core_listings: list[dict[str, str]],
) -> list[SourceGapClassificationRow]:
    rows: list[SourceGapClassificationRow] = []
    for row in core_listings:
        if row.get("scope_reason") == "primary_listing_missing_isin":
            rows.append(make_row(FIELD_MISSING_ISIN, row))
    for row in tickers:
        if row.get("asset_type") == "Stock" and not row.get("stock_sector", "").strip():
            rows.append(make_row(FIELD_MISSING_STOCK_SECTOR, row))
        elif row.get("asset_type") == "ETF" and not row.get("etf_category", "").strip():
            rows.append(make_row(FIELD_MISSING_ETF_CATEGORY, row))
    return sorted(rows, key=lambda row: (row.field, row.exchange, row.asset_type, row.ticker, row.listing_key))


def rows_to_dicts(rows: list[SourceGapClassificationRow]) -> list[dict[str, Any]]:
    payload = []
    for row in rows:
        item = asdict(row)
        item["review_needed"] = "true" if row.review_needed else "false"
        payload.append(item)
    return payload


def summarize(rows: list[SourceGapClassificationRow], generated_at: str) -> dict[str, Any]:
    field_counts = Counter(row.field for row in rows)
    class_counts = Counter(row.gap_class for row in rows)
    class_by_field = Counter((row.field, row.gap_class) for row in rows)
    exchange_by_field = Counter((row.field, row.exchange) for row in rows)
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "field_totals": dict(sorted(field_counts.items())),
        "class_totals": dict(sorted(class_counts.items())),
        "class_by_field": {
            f"{field}:{gap_class}": count for (field, gap_class), count in sorted(class_by_field.items())
        },
        "top_exchanges_by_field": {
            field: [
                {"exchange": exchange, "rows": count}
                for (row_field, exchange), count in exchange_by_field.most_common()
                if row_field == field
            ][:10]
            for field in sorted(field_counts)
        },
        "policy": {
            "no_unreviewed_heuristics": "This report classifies residual gaps only; it does not fill metadata values.",
            "release_gate": "Every current residual source gap must have one deterministic class and a source gate.",
        },
    }


def write_csv(path: Path, rows: list[SourceGapClassificationRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows_to_dicts(rows))


def write_json(path: Path, rows: list[SourceGapClassificationRow], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"summary": summary, "rows": rows_to_dicts(rows)}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def format_top_classes(summary: dict[str, Any]) -> str:
    lines = ["| Class | Rows |", "|---|---:|"]
    for gap_class, count in sorted(summary["class_totals"].items(), key=lambda item: (-item[1], item[0]))[:20]:
        lines.append(f"| {gap_class} | {count} |")
    return "\n".join(lines)


def write_markdown(path: Path, rows: list[SourceGapClassificationRow], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    field_totals = summary["field_totals"]
    lines = [
        "# Source Gap Classification",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report classifies residual metadata gaps after official and reviewed free-source backfills. It is a guardrail report: values remain empty unless a future source satisfies the listed source gate.",
        "",
        "## Summary",
        "",
        f"- Missing primary ISIN rows classified: `{field_totals.get(FIELD_MISSING_ISIN, 0)}`",
        f"- Missing stock-sector rows classified: `{field_totals.get(FIELD_MISSING_STOCK_SECTOR, 0)}`",
        f"- Missing ETF-category rows classified: `{field_totals.get(FIELD_MISSING_ETF_CATEGORY, 0)}`",
        "",
        "## Top Classes",
        "",
        format_top_classes(summary),
        "",
        "## Release Policy",
        "",
        "- No value in this report is an inferred metadata fill.",
        "- Future fills must pass the row-level source gate and the normal reviewed override path.",
        "- The database validator fails if current gaps are missing from this classification report or if stale classifications remain.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify residual ticker database source gaps without filling values.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--core-listings-csv", type=Path, default=DEFAULT_CORE_LISTINGS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_source_gap_classifications(
        tickers=load_csv(args.tickers_csv),
        core_listings=load_csv(args.core_listings_csv),
    )
    generated_at = utc_now()
    summary = summarize(rows, generated_at)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, rows, summary)
    write_markdown(args.md_out, rows, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "field_totals": summary["field_totals"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
