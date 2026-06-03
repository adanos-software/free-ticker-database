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
ASX_RESIDUAL_REVIEW_JSON = REPORTS_DIR / "asx_residual_review.json"
JPX_TSE_SECTOR_BACKFILL_JSON = REPORTS_DIR / "tse_sector_backfill.json"
SEC_SIC_SECTOR_BACKFILL_JSON = REPORTS_DIR / "sec_sic_sector_backfill.json"
CANADA_RESIDUAL_REVIEW_JSON = REPORTS_DIR / "canada_residual_review.json"
B3_RESIDUAL_SECTOR_REVIEW_JSON = REPORTS_DIR / "b3_residual_sector_review.json"
WEAK_SECTOR_RESIDUAL_REVIEW_JSON = REPORTS_DIR / "weak_sector_residual_review.json"
SOURCE_GAP_CLASSIFICATION_JSON = REPORTS_DIR / "source_gap_classification.json"
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
                "Official JPX/TSE Stock Data Search detail API; supplements listed-issues rows with ISINs.",
                "scripts/fetch_exchange_masterfiles.py --source jpx_tse_stock_detail",
                True,
                "Accept after exact TSE ticker, official JPX detail payload, expected JP prefix where applicable, and ISIN checksum gates.",
                "Small residual primary ISIN gap; source is official JPX/TSE, not Yahoo.",
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
            "scripts/backfill_sector_from_isin_peers.py; scripts/backfill_etf_categories_from_names.py",
            True,
                "ETF categories must be stored as etf_category after deterministic taxonomy mapping.",
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
    if field == FIELD_MISSING_ISIN and missing_count >= 100:
        return 0, "top_impact"
    if field == FIELD_MISSING_STOCK_SECTOR and missing_count >= 250:
        return 0, "top_impact"
    if field == FIELD_MISSING_ETF_CATEGORY and missing_count >= 200:
        return 0, "top_impact"
    return 0, "ranked_by_missing_count"


def source_order_for(field: str, exchange: str) -> int:
    priority = ISIN_PRIORITY if field == FIELD_MISSING_ISIN else SECTOR_CATEGORY_PRIORITY
    if exchange in priority:
        return priority.index(exchange)
    return len(priority)


def row_stock_sector(row: dict[str, str]) -> str:
    if row.get("asset_type") != "Stock":
        return ""
    return row.get("stock_sector", "") or row.get("sector", "")


def row_etf_category(row: dict[str, str]) -> str:
    if row.get("asset_type") != "ETF":
        return ""
    return row.get("etf_category", "") or row.get("sector", "")


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
        exchange = row.get("exchange", "")
        if not exchange:
            continue
        asset_type = row.get("asset_type", "")
        if asset_type == "Stock" and not row_stock_sector(row).strip():
            stock_sector_counts[exchange] += 1
        elif asset_type == "ETF" and not row_etf_category(row).strip():
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
            key=lambda row: (-row.missing_count, source_order_for(row.field, row.exchange), row.exchange, row.asset_type),
        )
        for rank, row in enumerate(field_rows, start=1):
            ranked_rows.append(replace(row, priority_rank=rank))
    return ranked_rows


def summarize(
    rows: list[CompletionBacklogRow],
    coverage_report: dict[str, Any],
    generated_at: str,
    *,
    asx_residual_review: dict[str, Any] | None = None,
    jpx_tse_sector_backfill: dict[str, Any] | None = None,
    sec_sic_sector_backfill: dict[str, Any] | None = None,
    canada_residual_review: dict[str, Any] | None = None,
    b3_residual_sector_review: dict[str, Any] | None = None,
    weak_sector_residual_review: dict[str, Any] | None = None,
    source_gap_classification: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
        "legacy_primary_ticker_collision_rows": coverage_report.get("global", {}).get("legacy_primary_ticker_collision_rows", 0),
        "model_notes": {
            "sector_split": "Use stock_sector for stock rows and etf_category for ETF rows; the legacy sector export has been removed.",
            "listing_key_first": "The core security model is now listing_key-first. Global ticker uniqueness only remains as a compatibility constraint in tickers.csv.",
            "source_blocks": [
                "High-count primary ISIN residuals",
                "High-count stock-sector residuals",
                "High-count ETF-category residuals",
                "OTC warning review queue",
                "Source-gap venues by missing count",
                "Missing venues",
            ],
        },
        "next_actions": build_next_actions(
            rows,
            asx_residual_review=asx_residual_review or {},
            jpx_tse_sector_backfill=jpx_tse_sector_backfill or {},
            sec_sic_sector_backfill=sec_sic_sector_backfill or {},
            canada_residual_review=canada_residual_review or {},
            b3_residual_sector_review=b3_residual_sector_review or {},
            weak_sector_residual_review=weak_sector_residual_review or {},
            source_gap_classification=source_gap_classification or {},
        ),
    }


def asx_residual_summary(asx_residual_review: dict[str, Any]) -> dict[str, Any]:
    summary = asx_residual_review.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def asx_residual_blocks_direct_apply(asx_residual_review: dict[str, Any]) -> bool:
    summary = asx_residual_summary(asx_residual_review)
    backlog = summary.get("asx_residual_backlog", {})
    if not isinstance(backlog, dict):
        return False
    return (
        int(backlog.get("official_isin_apply_candidate_rows") or 0) == 0
        and int(backlog.get("direct_data_apply_allowed_rows") or 0) == 0
    )


def apply_asx_residual_context(action: dict[str, Any], asx_residual_review: dict[str, Any]) -> dict[str, Any]:
    if action["exchange"] != "ASX" or action["field"] != FIELD_MISSING_ISIN:
        return action
    if not asx_residual_blocks_direct_apply(asx_residual_review):
        return action
    summary = asx_residual_summary(asx_residual_review)
    backlog = summary.get("asx_residual_backlog", {})
    if not isinstance(backlog, dict):
        backlog = {}
    batches = summary.get("top_asx_resolution_review_batches", [])
    first_batch = batches[0] if isinstance(batches, list) and batches and isinstance(batches[0], dict) else {}
    return {
        **action,
        "review_needed": True,
        "recommended_source": first_batch.get("recommended_next_source") or backlog.get("source_gate") or action["recommended_source"],
        "confidence_policy": backlog.get("source_gate") or action["confidence_policy"],
        "why_next": "top_impact residual review-gated workflow",
        "residual_gate": "asx_residual_review_blocks_direct_apply",
        "residual_rows": int(backlog.get("rows") or 0),
        "direct_data_apply_allowed_rows": int(backlog.get("direct_data_apply_allowed_rows") or 0),
        "official_isin_apply_candidate_rows": int(backlog.get("official_isin_apply_candidate_rows") or 0),
    }


def jpx_tse_sector_summary(jpx_tse_sector_backfill: dict[str, Any]) -> dict[str, Any]:
    summary = jpx_tse_sector_backfill.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def jpx_tse_sector_blocks_direct_apply(jpx_tse_sector_backfill: dict[str, Any]) -> bool:
    summary = jpx_tse_sector_summary(jpx_tse_sector_backfill)
    decision_counts = summary.get("decision_counts", {})
    if not isinstance(decision_counts, dict):
        decision_counts = {}
    candidates = int(summary.get("candidates") or 0)
    accepted_updates = int(summary.get("accepted_sector_updates") or 0)
    return candidates > 0 and accepted_updates == 0 and int(decision_counts.get("missing_jpx_industry") or 0) == candidates


def apply_jpx_tse_sector_context(action: dict[str, Any], jpx_tse_sector_backfill: dict[str, Any]) -> dict[str, Any]:
    if action["exchange"] != "TSE" or action["field"] != FIELD_MISSING_STOCK_SECTOR:
        return action
    if not jpx_tse_sector_blocks_direct_apply(jpx_tse_sector_backfill):
        return action
    summary = jpx_tse_sector_summary(jpx_tse_sector_backfill)
    decision_counts = summary.get("decision_counts", {})
    if not isinstance(decision_counts, dict):
        decision_counts = {}
    return {
        **action,
        "review_needed": True,
        "recommended_source": (
            "Official JPX listed-issues verification shows exact TSE matches but no JPX 33-industry values; "
            "use official REIT/infrastructure-fund taxonomy evidence before any stock_sector update."
        ),
        "confidence_policy": (
            "Do not infer stock_sector from REIT/fund names or market segment alone; apply only after official "
            "industry or product-taxonomy evidence maps to a canonical stock sector."
        ),
        "why_next": "official source gap review-gated workflow",
        "residual_gate": "jpx_tse_sector_backfill_blocks_direct_apply",
        "jpx_candidates": int(summary.get("candidates") or 0),
        "accepted_sector_updates": int(summary.get("accepted_sector_updates") or 0),
        "jpx_missing_industry_rows": int(decision_counts.get("missing_jpx_industry") or 0),
    }


def sec_sic_sector_summary(sec_sic_sector_backfill: dict[str, Any]) -> dict[str, Any]:
    summary = sec_sic_sector_backfill.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def sec_sic_otc_has_no_apply_candidates(sec_sic_sector_backfill: dict[str, Any]) -> bool:
    summary = sec_sic_sector_summary(sec_sic_sector_backfill)
    exchanges = summary.get("exchanges", [])
    if not isinstance(exchanges, list) or "OTC" not in exchanges:
        return False
    candidates = int(summary.get("candidates") or 0)
    accepted_updates = int(summary.get("accepted_sector_updates") or 0)
    return candidates > 0 and accepted_updates == 0


def apply_sec_sic_otc_context(action: dict[str, Any], sec_sic_sector_backfill: dict[str, Any]) -> dict[str, Any]:
    if action["exchange"] != "OTC" or action["field"] != FIELD_MISSING_STOCK_SECTOR:
        return action
    if not sec_sic_otc_has_no_apply_candidates(sec_sic_sector_backfill):
        return action
    summary = sec_sic_sector_summary(sec_sic_sector_backfill)
    decision_counts = summary.get("decision_counts", {})
    if not isinstance(decision_counts, dict):
        decision_counts = {}
    return {
        **action,
        "review_needed": True,
        "recommended_source": (
            "Current SEC SIC residual dry-run has no accepted OTC sector candidates; prioritize OTC Markets issuer "
            "evidence, reviewed Alpha Vantage/FinanceDatabase signals, or keep source-gap status."
        ),
        "confidence_policy": (
            "Do not fill OTC stock_sector from SEC ticker presence alone; require exact CIK/ticker/exchange, issuer-name, "
            "numeric-token, SIC, and canonical-sector gates, or a separate reviewed source."
        ),
        "why_next": "top_impact residual review-gated workflow",
        "residual_gate": "sec_sic_otc_no_apply_candidates",
        "sec_sic_candidates": int(summary.get("candidates") or 0),
        "accepted_sector_updates": int(summary.get("accepted_sector_updates") or 0),
        "sec_no_match_rows": int(decision_counts.get("no_sec_match") or 0),
        "sec_missing_sic_rows": int(decision_counts.get("missing_sic") or 0),
        "sec_name_mismatch_rows": int(decision_counts.get("name_mismatch") or 0),
        "sec_requests_made": int(summary.get("requests_made") or 0),
    }


def canada_residual_summary(canada_residual_review: dict[str, Any]) -> dict[str, Any]:
    summary = canada_residual_review.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def canada_residual_blocks_direct_identifier_apply(canada_residual_review: dict[str, Any]) -> bool:
    summary = canada_residual_summary(canada_residual_review)
    backlog = summary.get("canada_identifier_backlog", {})
    if not isinstance(backlog, dict):
        return False
    return (
        int(backlog.get("rows") or 0) > 0
        and int(backlog.get("direct_identifier_apply_allowed_rows") or 0) == 0
    )


def apply_canada_residual_context(action: dict[str, Any], canada_residual_review: dict[str, Any]) -> dict[str, Any]:
    if action["exchange"] not in {"TSX", "TSXV", "NEO"} or action["field"] != FIELD_MISSING_ISIN:
        return action
    if not canada_residual_blocks_direct_identifier_apply(canada_residual_review):
        return action
    summary = canada_residual_summary(canada_residual_review)
    backlog = summary.get("canada_identifier_backlog", {})
    if not isinstance(backlog, dict):
        backlog = {}
    queue_totals = summary.get("canada_resolution_queue_exchange_totals", {})
    if not isinstance(queue_totals, dict):
        queue_totals = {}
    top_batches = summary.get("top_canada_resolution_review_batches", [])
    matching_batches = [
        batch
        for batch in top_batches
        if isinstance(batch, dict)
        and batch.get("exchange") == action["exchange"]
        and str(batch.get("canada_resolution_queue", "")).startswith("missing_isin")
    ]
    first_batch = matching_batches[0] if matching_batches else {}
    direct_allowed = int(backlog.get("direct_identifier_apply_allowed_rows") or 0)
    official_required = int(backlog.get("official_isin_source_required_rows") or 0)
    scope_required = int(backlog.get("scope_decision_required_rows") or 0)
    reviewed_gap_rows = int(backlog.get("reviewed_openfigi_source_gap_rows") or 0)
    missing_isin_official = queue_totals.get("missing_isin_official_canada_masterfiles_do_not_expose_isin", {})
    missing_isin_reviewed = queue_totals.get("missing_isin_reviewed_source_gap", {})
    if not isinstance(missing_isin_official, dict):
        missing_isin_official = {}
    if not isinstance(missing_isin_reviewed, dict):
        missing_isin_reviewed = {}
    return {
        **action,
        "review_needed": True,
        "recommended_source": (
            first_batch.get("recommended_next_source")
            or "Official CSD, issuer, prospectus, transfer-agent, or reviewed Canada identifier source exposing a valid ISIN."
        ),
        "confidence_policy": backlog.get("source_gate") or action["confidence_policy"],
        "why_next": "top_impact Canada residual review-gated workflow",
        "residual_gate": "canada_residual_review_blocks_direct_identifier_apply",
        "canada_identifier_backlog_rows": int(backlog.get("rows") or 0),
        "direct_identifier_apply_allowed_rows": direct_allowed,
        "official_isin_source_required_rows": official_required,
        "scope_decision_required_rows": scope_required,
        "reviewed_openfigi_source_gap_rows": reviewed_gap_rows,
        "exchange_missing_isin_official_source_rows": int(missing_isin_official.get(action["exchange"]) or 0),
        "exchange_missing_isin_reviewed_source_gap_rows": int(missing_isin_reviewed.get(action["exchange"]) or 0),
    }


def b3_residual_sector_summary(b3_residual_sector_review: dict[str, Any]) -> dict[str, Any]:
    summary = b3_residual_sector_review.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def b3_residual_sector_blocks_direct_apply(b3_residual_sector_review: dict[str, Any]) -> bool:
    summary = b3_residual_sector_summary(b3_residual_sector_review)
    eligibility = summary.get("apply_eligibility_totals", {})
    if not isinstance(eligibility, dict):
        return False
    rows = int(summary.get("rows") or 0)
    source_gap_rows = int(eligibility.get("source_gap_keep_blank_until_official_taxonomy_evidence") or 0)
    return rows > 0 and source_gap_rows == rows


def apply_b3_residual_sector_context(action: dict[str, Any], b3_residual_sector_review: dict[str, Any]) -> dict[str, Any]:
    if action["exchange"] != "B3" or action["field"] != FIELD_MISSING_STOCK_SECTOR:
        return action
    if not b3_residual_sector_blocks_direct_apply(b3_residual_sector_review):
        return action
    summary = b3_residual_sector_summary(b3_residual_sector_review)
    top_batches = summary.get("top_b3_sector_review_batches", [])
    first_batch = top_batches[0] if isinstance(top_batches, list) and top_batches and isinstance(top_batches[0], dict) else {}
    probe_decisions = summary.get("b3_probe_decision_totals", {})
    code_shapes = summary.get("b3_code_shape_totals", {})
    if not isinstance(probe_decisions, dict):
        probe_decisions = {}
    if not isinstance(code_shapes, dict):
        code_shapes = {}
    return {
        **action,
        "review_needed": True,
        "recommended_source": first_batch.get("recommended_next_source")
        or "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing.",
        "confidence_policy": first_batch.get("source_gate")
        or "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing.",
        "why_next": "B3 residual source-gap review-gated workflow",
        "residual_gate": "b3_residual_sector_review_blocks_direct_apply",
        "b3_residual_sector_rows": int(summary.get("rows") or 0),
        "b3_source_gap_keep_blank_rows": int(
            summary.get("apply_eligibility_totals", {}).get("source_gap_keep_blank_until_official_taxonomy_evidence") or 0
        )
        if isinstance(summary.get("apply_eligibility_totals", {}), dict)
        else 0,
        "b3_no_code_match_rows": int(probe_decisions.get("no_b3_code_match") or 0),
        "b3_alpha_code_rows": int(code_shapes.get("alpha_b3_code") or 0),
        "b3_alphanumeric_code_rows": int(code_shapes.get("alphanumeric_b3_code") or 0),
    }


def weak_sector_residual_summary(weak_sector_residual_review: dict[str, Any]) -> dict[str, Any]:
    summary = weak_sector_residual_review.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def weak_sector_blocks_direct_apply(weak_sector_residual_review: dict[str, Any]) -> bool:
    summary = weak_sector_residual_summary(weak_sector_residual_review)
    backlog = summary.get("weak_sector_backlog", {})
    if not isinstance(backlog, dict):
        return False
    return int(backlog.get("rows") or 0) > 0 and int(backlog.get("direct_sector_apply_allowed_rows") or 0) == 0


def apply_weak_sector_residual_context(
    action: dict[str, Any],
    weak_sector_residual_review: dict[str, Any],
) -> dict[str, Any]:
    if action["field"] != FIELD_MISSING_STOCK_SECTOR:
        return action
    if not weak_sector_blocks_direct_apply(weak_sector_residual_review):
        return action

    summary = weak_sector_residual_summary(weak_sector_residual_review)
    exchange = action["exchange"]
    exchange_totals = summary.get("venue_backlog_exchange_queue_totals", {})
    if not isinstance(exchange_totals, dict) or exchange not in exchange_totals:
        return action
    exchange_queue_totals = exchange_totals.get(exchange, {})
    if not isinstance(exchange_queue_totals, dict):
        exchange_queue_totals = {}

    backlog = summary.get("weak_sector_backlog", {})
    if not isinstance(backlog, dict):
        backlog = {}
    batches = summary.get("top_weak_sector_resolution_review_batches", [])
    if not isinstance(batches, list):
        batches = []
    matching_batches = [batch for batch in batches if isinstance(batch, dict) and batch.get("exchange") == exchange]
    first_batch = matching_batches[0] if matching_batches else {}

    return {
        **action,
        "review_needed": True,
        "recommended_source": first_batch.get("recommended_next_source")
        or "Updated official masterfile, issuer record, or venue-official taxonomy exposing sector for the exact listing.",
        "confidence_policy": first_batch.get("source_gate") or backlog.get("source_gate") or action["confidence_policy"],
        "why_next": "weak-sector residual review-gated workflow",
        "residual_gate": "weak_sector_residual_review_blocks_direct_apply",
        "weak_sector_rows": int(backlog.get("rows") or 0),
        "direct_sector_apply_allowed_rows": int(backlog.get("direct_sector_apply_allowed_rows") or 0),
        "official_sector_candidate_rows": int(backlog.get("official_sector_candidate_rows") or 0),
        "scope_decision_required_rows": int(backlog.get("scope_decision_required_rows") or 0),
        "masterfile_without_sector_rows": int(backlog.get("masterfile_without_sector_rows") or 0),
        "venue_taxonomy_source_required_rows": int(backlog.get("venue_taxonomy_source_required_rows") or 0),
        "exchange_official_masterfile_without_sector_rows": int(
            exchange_queue_totals.get("official_masterfile_without_sector_source_gap") or 0
        ),
        "exchange_venue_taxonomy_source_required_rows": int(
            exchange_queue_totals.get("venue_official_taxonomy_unavailable_source_gap") or 0
        ),
        "exchange_scope_decision_required_rows": int(
            exchange_queue_totals.get("core_exclusion_candidate_scope_review_before_sector_fill") or 0
        ),
        "exchange_official_sector_candidate_rows": int(
            exchange_queue_totals.get("official_sector_value_requires_canonical_mapping_review") or 0
        ),
    }


def source_gap_classification_summary(source_gap_classification: dict[str, Any]) -> dict[str, Any]:
    summary = source_gap_classification.get("summary", {})
    return summary if isinstance(summary, dict) else {}


def apply_source_gap_classification_context(
    action: dict[str, Any],
    source_gap_classification: dict[str, Any],
) -> dict[str, Any]:
    if action.get("residual_gate"):
        return action
    summary = source_gap_classification_summary(source_gap_classification)
    top_batches = summary.get("top_source_gap_review_batches", [])
    if not isinstance(top_batches, list):
        return action
    matching_batches = [
        batch
        for batch in top_batches
        if isinstance(batch, dict)
        and batch.get("exchange") == action["exchange"]
        and batch.get("field") == action["field"]
    ]
    if not matching_batches:
        return action
    first_batch = matching_batches[0]
    gap_class = str(first_batch.get("gap_class") or "")
    return {
        **action,
        "review_needed": True,
        "recommended_source": first_batch.get("recommended_next_source") or action["recommended_source"],
        "confidence_policy": first_batch.get("source_gate") or action["confidence_policy"],
        "why_next": "source-gap classification review-gated workflow",
        "residual_gate": "source_gap_classification_blocks_direct_apply",
        "source_gap_class": gap_class,
        "source_gap_rows": int(first_batch.get("rows") or 0),
    }


def build_next_actions(
    rows: list[CompletionBacklogRow],
    limit: int = 8,
    *,
    asx_residual_review: dict[str, Any] | None = None,
    jpx_tse_sector_backfill: dict[str, Any] | None = None,
    sec_sic_sector_backfill: dict[str, Any] | None = None,
    canada_residual_review: dict[str, Any] | None = None,
    b3_residual_sector_review: dict[str, Any] | None = None,
    weak_sector_residual_review: dict[str, Any] | None = None,
    source_gap_classification: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    ranked = sorted(
        rows,
        key=lambda row: (
            row.priority_bucket != "top_impact",
            row.review_needed,
            -row.missing_count,
            source_order_for(row.field, row.exchange),
            row.field,
            row.exchange,
        ),
    )
    actions: list[dict[str, Any]] = []
    for row in ranked[:limit]:
        action = {
            "action_rank": len(actions) + 1,
            "safe_action": "candidate_for_official_followup",
            "exchange": row.exchange,
            "field": row.field,
            "target_field": row.target_field,
            "missing_count": row.missing_count,
            "stock_missing_count": row.stock_missing_count,
            "etf_missing_count": row.etf_missing_count,
            "venue_status": row.venue_status,
            "official_source_count": row.official_source_count,
            "recommended_source": row.recommended_source,
            "script": row.script,
            "review_needed": row.review_needed,
            "confidence_policy": row.confidence_policy,
            "why_next": (
                "top_impact official workflow"
                if row.priority_bucket == "top_impact" and not row.review_needed
                else "top_impact review-gated workflow"
                if row.priority_bucket == "top_impact"
                else "highest remaining missing-count workflow"
            ),
        }
        action = apply_asx_residual_context(action, asx_residual_review or {})
        action = apply_jpx_tse_sector_context(action, jpx_tse_sector_backfill or {})
        action = apply_sec_sic_otc_context(action, sec_sic_sector_backfill or {})
        action = apply_canada_residual_context(action, canada_residual_review or {})
        action = apply_b3_residual_sector_context(action, b3_residual_sector_review or {})
        action = apply_weak_sector_residual_context(action, weak_sector_residual_review or {})
        action = apply_source_gap_classification_context(action, source_gap_classification or {})
        actions.append(action)
    return actions


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
    selected = sorted(
        counts.items(),
        key=lambda item: (-(item[1]["stock"] + item[1]["etf"]), item[0]),
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
    next_actions = summary.get("next_actions", [])
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
            f"- Official symbol collisions tracked in exchange references: `{summary['official_masterfile_collisions']}`",
            f"- Core rows hidden only by the legacy global-ticker compatibility export: `{summary['legacy_primary_ticker_collision_rows']}`",
            "",
            "## Next Safe Batches",
            "",
            format_next_actions_table(next_actions),
            "These are orchestration candidates only. They do not authorize direct data changes without the listed official or review-gated evidence.",
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
            "- The legacy `sector` export has been removed to avoid duplicating typed metadata.",
            "- `core_listings.csv` is the collision-safe canonical core export keyed by `listing_key`.",
            "- `tickers.csv` remains the legacy one-row-per-global-ticker compatibility export.",
            "",
            "## Source Block Order",
            "",
            "1. High-count primary ISIN residuals",
            "2. High-count stock-sector residuals",
            "3. High-count ETF-category residuals",
            "4. OTC warning review queue",
            "5. Source-gap venues by missing count",
            "6. Missing venues",
            "",
        ]
    )


def format_next_actions_table(actions: list[dict[str, Any]]) -> str:
    if not actions:
        return "_No actions._\n"
    lines = [
        "| Rank | Exchange | Field | Missing | Safe action | Evidence path | Review |",
        "|---|---|---|---:|---|---|---|",
    ]
    for action in actions:
        review = "yes" if action["review_needed"] else "no"
        lines.append(
            f"| {action['action_rank']} | {action['exchange']} | {action['field']} | "
            f"{action['missing_count']} | {action['safe_action']} | "
            f"{action['recommended_source']} | {review} |"
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the field-level ticker database completion backlog.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--coverage-report-json", type=Path, default=COVERAGE_REPORT_JSON)
    parser.add_argument("--asx-residual-review-json", type=Path, default=ASX_RESIDUAL_REVIEW_JSON)
    parser.add_argument("--jpx-tse-sector-backfill-json", type=Path, default=JPX_TSE_SECTOR_BACKFILL_JSON)
    parser.add_argument("--sec-sic-sector-backfill-json", type=Path, default=SEC_SIC_SECTOR_BACKFILL_JSON)
    parser.add_argument("--canada-residual-review-json", type=Path, default=CANADA_RESIDUAL_REVIEW_JSON)
    parser.add_argument("--b3-residual-sector-review-json", type=Path, default=B3_RESIDUAL_SECTOR_REVIEW_JSON)
    parser.add_argument("--weak-sector-residual-review-json", type=Path, default=WEAK_SECTOR_RESIDUAL_REVIEW_JSON)
    parser.add_argument("--source-gap-classification-json", type=Path, default=SOURCE_GAP_CLASSIFICATION_JSON)
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
    summary = summarize(
        rows,
        coverage_report,
        generated_at,
        asx_residual_review=load_json(args.asx_residual_review_json),
        jpx_tse_sector_backfill=load_json(args.jpx_tse_sector_backfill_json),
        sec_sic_sector_backfill=load_json(args.sec_sic_sector_backfill_json),
        canada_residual_review=load_json(args.canada_residual_review_json),
        b3_residual_sector_review=load_json(args.b3_residual_sector_review_json),
        weak_sector_residual_review=load_json(args.weak_sector_residual_review_json),
        source_gap_classification=load_json(args.source_gap_classification_json),
    )

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
