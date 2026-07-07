from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.lib.dataio import display_path, load_csv, write_csv, write_json
    from scripts.lib.non_equity_guard import classify_non_equity_leakage
    from scripts.lib.normalize import names_match
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import display_path, load_csv, write_csv, write_json
    from lib.non_equity_guard import classify_non_equity_leakage
    from lib.normalize import names_match


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
SYMBOL_CHANGES_REVIEW_CSV = REPORTS_DIR / "symbol_changes_review.csv"
ISIN_IDENTITY_COLLISION_CSV = REPORTS_DIR / "isin_identity_collision_review_queue.csv"
METADATA_UPDATES_CSV = DATA_DIR / "review_overrides" / "metadata_updates.csv"

SECTOR_CSV_OUT = REPORTS_DIR / "m3_sector_category_campaign.csv"
SECTOR_MARKET_CSV_OUT = REPORTS_DIR / "m3_sector_category_campaign_by_market.csv"
SECTOR_JSON_OUT = REPORTS_DIR / "m3_sector_category_campaign.json"
SECTOR_MD_OUT = REPORTS_DIR / "m3_sector_category_campaign.md"
NAME_CSV_OUT = REPORTS_DIR / "m3_name_freshness_campaign.csv"
NAME_JSON_OUT = REPORTS_DIR / "m3_name_freshness_campaign.json"
NAME_MD_OUT = REPORTS_DIR / "m3_name_freshness_campaign.md"
IDENTITY_CSV_OUT = REPORTS_DIR / "m3_identity_residual_campaign.csv"
IDENTITY_JSON_OUT = REPORTS_DIR / "m3_identity_residual_campaign.json"
IDENTITY_MD_OUT = REPORTS_DIR / "m3_identity_residual_campaign.md"
LEAKAGE_CSV_OUT = REPORTS_DIR / "m3_non_equity_leakage_guard.csv"
LEAKAGE_JSON_OUT = REPORTS_DIR / "m3_non_equity_leakage_guard.json"
LEAKAGE_MD_OUT = REPORTS_DIR / "m3_non_equity_leakage_guard.md"
AUDIT_CSV_OUT = REPORTS_DIR / "m3_correctness_audit.csv"
AUDIT_JSON_OUT = REPORTS_DIR / "m3_correctness_audit.json"
AUDIT_MD_OUT = REPORTS_DIR / "m3_correctness_audit.md"
ROLLUP_JSON_OUT = REPORTS_DIR / "m3_correctness_campaigns.json"
ROLLUP_MD_OUT = REPORTS_DIR / "m3_correctness_campaigns.md"

VALID_STOCK_SECTORS = {
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
}
VALID_ETF_CATEGORIES = {
    "Fixed Income",
    "Equity",
    "Commodity",
    "Real Estate",
    "Multi-Asset",
    "Currency",
    "Volatility",
    "Leveraged/Inverse",
    "Alternative",
    "Money Market",
    "Thematic",
    "Other",
}
SECTOR_MAP = {
    "Healthcare": "Health Care",
    "Technology": "Information Technology",
    "Basic Materials": "Materials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Financial Services": "Financials",
    "Banking": "Financials",
    "FINANCIAL SERVICES": "Financials",
    "Communications": "Communication Services",
    "ICT": "Communication Services",
    "Commercial Real Estate": "Real Estate",
    "Residential Real Estate": "Real Estate",
    "REITs": "Real Estate",
    "Property": "Real Estate",
    "CONSTRUCTION/REAL ESTATE": "Real Estate",
    "Agriculture": "Consumer Staples",
    "AGRICULTURE": "Consumer Staples",
    "CONSUMER GOODS": "Consumer Staples",
    "Mining": "Materials",
    "NATURAL RESOURCES": "Materials",
    "OIL AND GAS": "Energy",
    "HEALTHCARE": "Health Care",
    "INDUSTRIAL GOODS": "Industrials",
    "Retail & Wholesale": "Consumer Discretionary",
    "Security Services": "Industrials",
    "Tourism": "Consumer Discretionary",
}
ETF_CATEGORY_MAP = {
    "Blend": "Equity",
    "Bonds": "Fixed Income",
    "Cash": "Money Market",
    "Commodities Broad Basket": "Commodity",
    "Corporate Bonds": "Fixed Income",
    "Currencies": "Currency",
    "Derivatives": "Alternative",
    "Developed Markets": "Equity",
    "Emerging Markets": "Equity",
    "Equities": "Equity",
    "Factors": "Equity",
    "Frontier Markets": "Equity",
    "Government Bonds": "Fixed Income",
    "Growth": "Equity",
    "High Yield Bonds": "Fixed Income",
    "Inflation-Protected Securities": "Fixed Income",
    "Investment Grade Bonds": "Fixed Income",
    "Large Cap": "Equity",
    "Mid Cap": "Equity",
    "Micro Cap": "Equity",
    "Money Market Instruments": "Money Market",
    "Municipal Bonds": "Fixed Income",
    "Small Cap": "Equity",
    "Trading": "Other",
    "Treasury Bonds": "Fixed Income",
    "Value": "Equity",
    **{sector: "Equity" for sector in VALID_STOCK_SECTORS - {"Real Estate"}},
}

SECTOR_FIELDNAMES = [
    "campaign",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "target_field",
    "current_value",
    "official_values",
    "canonical_value",
    "evidence_source",
    "confidence_reason",
    "decision",
    "review_priority",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
]
SECTOR_MARKET_FIELDNAMES = [
    "exchange",
    "asset_type",
    "candidate_rows",
    "applied_rows",
    "blocked_rows",
    "manual_review_rows",
    "metadata_override_pending_rows",
    "verified_current_matches_official_rows",
    "evidence_sources",
]
NAME_FIELDNAMES = [
    "campaign",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "current_name",
    "official_or_feed_name",
    "source_kind",
    "evidence_source",
    "decision",
    "review_priority",
    "confidence_reason",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
]
IDENTITY_FIELDNAMES = [
    "campaign",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "residual_type",
    "severity",
    "cluster_size",
    "evidence_source",
    "decision",
    "review_priority",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
]
LEAKAGE_FIELDNAMES = [
    "campaign",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "guard_decision",
    "leakage_class",
    "confidence",
    "evidence_source",
    "evidence_value",
    "review_strategy",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "recommended_action",
]
AUDIT_FIELDNAMES = [
    "audit_block",
    "campaign",
    "rows_reviewed",
    "manual_review_rows",
    "blocked_rows",
    "applied_rows",
    "changed_rows",
    "adversarial_changed_rows_checked",
    "two_source_possible_rows",
    "two_source_gap_rows",
    "full_row_correctness_claim",
    "metric_context",
    "source_gate",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def row_listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def normalize_sector_value(value: str, asset_type: str) -> str:
    value = (value or "").strip()
    if not value or len(value) > 50:
        return ""
    mapped = SECTOR_MAP.get(value, value)
    if asset_type == "ETF":
        mapped = ETF_CATEGORY_MAP.get(mapped, mapped)
        return mapped if mapped in VALID_ETF_CATEGORIES else ""
    if asset_type == "Stock":
        return mapped if mapped in VALID_STOCK_SECTORS else ""
    return ""


def active_official_reference_lookup(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        key = (row.get("ticker", ""), row.get("exchange", ""), row.get("asset_type", ""))
        lookup[key].append(row)
    return dict(lookup)


def source_text(refs: Iterable[dict[str, str]]) -> str:
    values = sorted(
        {
            f"{row.get('provider', '')}:{row.get('source_key', '')}".strip(":")
            for row in refs
            if row.get("source_key") or row.get("provider")
        }
    )
    return "|".join(values)


def metadata_update_lookup(rows: Iterable[dict[str, str]]) -> set[tuple[str, str, str, str]]:
    return {
        (
            row.get("ticker", ""),
            row.get("exchange", ""),
            row.get("field", ""),
            row.get("proposed_value", ""),
        )
        for row in rows
        if row.get("decision") == "update"
    }


def sector_source_gate(row: dict[str, str]) -> str:
    if row["decision"] == "metadata_override_present_pending_rebuild":
        return "Rebuild and validation gates must prove the override landed without typed leakage."
    if row["decision"] == "manual_review_official_value_conflicts_with_current":
        return "Apply only after exact listing-key official taxonomy evidence and canonical mapping review."
    return "Keep current value or blank until conflicting/noncanonical official taxonomy evidence is resolved."


def build_sector_category_campaign(
    listings: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
    metadata_updates: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    official = active_official_reference_lookup(masterfiles)
    applied_updates = metadata_update_lookup(metadata_updates)
    rows: list[dict[str, Any]] = []
    market_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    market_sources: dict[tuple[str, str], set[str]] = defaultdict(set)

    for listing in listings:
        asset_type = listing.get("asset_type", "")
        if asset_type not in {"Stock", "ETF"}:
            continue
        target_field = "stock_sector" if asset_type == "Stock" else "etf_category"
        refs = official.get((listing.get("ticker", ""), listing.get("exchange", ""), asset_type), [])
        refs_with_sector = [ref for ref in refs if ref.get("sector", "").strip()]
        if not refs_with_sector:
            continue
        current = listing.get(target_field, "").strip()
        source = source_text(refs_with_sector)
        market_key = (listing.get("exchange", ""), asset_type)
        if source:
            market_sources[market_key].update(source.split("|"))
        raw_values = sorted({ref.get("sector", "").strip() for ref in refs_with_sector if ref.get("sector", "").strip()})
        canonical_values = sorted(
            {
                normalized
                for value in raw_values
                if (normalized := normalize_sector_value(value, asset_type))
            }
        )
        if not canonical_values:
            decision = "blocked_noncanonical_official_value"
            canonical = ""
        elif len(canonical_values) > 1:
            decision = "blocked_conflicting_official_values"
            canonical = "|".join(canonical_values)
        else:
            canonical = canonical_values[0]
            if current == canonical:
                market_counts[market_key]["verified_current_matches_official_rows"] += 1
                continue
            if (listing.get("ticker", ""), listing.get("exchange", ""), target_field, canonical) in applied_updates:
                decision = "metadata_override_present_pending_rebuild"
            else:
                decision = "manual_review_official_value_conflicts_with_current"

        if decision.startswith("blocked"):
            market_counts[market_key]["blocked_rows"] += 1
            review_priority = "P1"
        elif decision == "metadata_override_present_pending_rebuild":
            market_counts[market_key]["metadata_override_pending_rows"] += 1
            market_counts[market_key]["manual_review_rows"] += 1
            review_priority = "P3"
        else:
            market_counts[market_key]["candidate_rows"] += 1
            market_counts[market_key]["manual_review_rows"] += 1
            review_priority = "P2"

        result = {
            "campaign": "C1_sector_etf_category_truth",
            "listing_key": row_listing_key(listing),
            "ticker": listing.get("ticker", ""),
            "exchange": listing.get("exchange", ""),
            "asset_type": asset_type,
            "name": listing.get("name", ""),
            "target_field": target_field,
            "current_value": current,
            "official_values": "|".join(raw_values),
            "canonical_value": canonical,
            "evidence_source": source,
            "confidence_reason": "official_active_masterfile_sector_with_conservative_canonical_mapping",
            "decision": decision,
            "review_priority": review_priority,
            "verification_evidence_required": "official_taxonomy_value_exact_listing_key_and_canonical_mapping",
            "recommended_next_source": "Official venue, registry, issuer, or exchange taxonomy for the exact listing.",
            "source_gate": "",
        }
        result["source_gate"] = sector_source_gate(result)
        rows.append(result)

    market_rows = []
    for (exchange, asset_type), counts in sorted(market_counts.items()):
        market_rows.append(
            {
                "exchange": exchange,
                "asset_type": asset_type,
                "candidate_rows": counts.get("candidate_rows", 0),
                "applied_rows": counts.get("applied_rows", 0),
                "blocked_rows": counts.get("blocked_rows", 0),
                "manual_review_rows": counts.get("manual_review_rows", 0),
                "metadata_override_pending_rows": counts.get("metadata_override_pending_rows", 0),
                "verified_current_matches_official_rows": counts.get("verified_current_matches_official_rows", 0),
                "evidence_sources": "|".join(sorted(market_sources[(exchange, asset_type)])),
            }
        )
    summary = summarize_decisions(rows)
    summary["verified_current_matches_official_rows"] = sum(
        int(row["verified_current_matches_official_rows"]) for row in market_rows
    )
    summary["market_rows"] = len(market_rows)
    return rows, market_rows, summary


def choose_official_name(current_name: str, refs: list[dict[str, str]]) -> str:
    names = sorted({ref.get("name", "").strip() for ref in refs if ref.get("name", "").strip()})
    if not names:
        return ""
    matching = [name for name in names if names_match(current_name, name)]
    return (matching or names)[0]


def build_name_freshness_campaign(
    entry_quality_rows: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
    symbol_change_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    official = active_official_reference_lookup(masterfiles)
    rows: list[dict[str, Any]] = []
    for row in entry_quality_rows:
        issues = set(row.get("issue_types", "").split("|"))
        if "official_name_mismatch" not in issues:
            continue
        refs = official.get((row.get("ticker", ""), row.get("exchange", ""), row.get("asset_type", "")), [])
        official_name = choose_official_name(row.get("name", ""), refs)
        rows.append(
            {
                "campaign": "C2_name_freshness",
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "exchange": row.get("exchange", ""),
                "asset_type": row.get("asset_type", ""),
                "current_name": row.get("name", ""),
                "official_or_feed_name": official_name,
                "source_kind": "entry_quality_official_name_mismatch",
                "evidence_source": source_text(refs),
                "decision": "manual_review_name_update_or_matcher_false_positive",
                "review_priority": "P1",
                "confidence_reason": "official_active_reference_name_has_weak_overlap_with_current_name",
                "verification_evidence_required": "official_current_name_plus_same_listing_identity_or_matcher_regression_evidence",
                "recommended_next_source": "Official exchange directory, issuer page, OpenFIGI mismatch evidence, or corporate-action notice.",
                "source_gate": "Do not update names until official current-name evidence proves the same listing identity.",
            }
        )

    for row in symbol_change_rows:
        if row.get("review_needed") != "true":
            continue
        rows.append(
            {
                "campaign": "C2_name_freshness",
                "listing_key": row.get("old_scoped_listing_keys") or row.get("old_listing_keys", ""),
                "ticker": row.get("old_symbol", ""),
                "exchange": row.get("source_exchange_hint", ""),
                "asset_type": "",
                "current_name": row.get("scoped_listing_names", ""),
                "official_or_feed_name": row.get("new_company_name", ""),
                "source_kind": "symbol_change_review",
                "evidence_source": row.get("source", ""),
                "decision": "manual_review_pending_official_rename_or_symbol_reuse_evidence",
                "review_priority": row.get("review_priority", "P2") or "P2",
                "confidence_reason": row.get("workflow_review_context", ""),
                "verification_evidence_required": row.get("verification_evidence_required", ""),
                "recommended_next_source": row.get("recommended_next_source", ""),
                "source_gate": row.get("source_gate", ""),
            }
        )
    return rows, summarize_decisions(rows)


def normalized_name(value: str) -> str:
    return " ".join(value.casefold().split())


def build_identity_residual_campaign(
    listings: list[dict[str, str]],
    entry_quality_rows: list[dict[str, str]],
    collision_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in listings:
        if row.get("isin"):
            by_isin[row["isin"]].append(row)
        if normalized_name(row.get("name", "")) == normalized_name(row.get("ticker", "")):
            rows.append(identity_row(row, "name_equals_ticker", "medium", 1, "data/listings.csv"))

    for row in entry_quality_rows:
        issue_types = set(row.get("issue_types", "").split("|"))
        for issue_type in sorted(
            issue_types & {"country_isin_mismatch", "official_name_mismatch", "official_isin_mismatch"}
        ):
            severity = "high" if issue_type != "official_name_mismatch" else "medium"
            rows.append(identity_row(row, issue_type, severity, 1, "data/reports/entry_quality.csv"))

    for isin, members in sorted(by_isin.items()):
        distinct_names = sorted({normalized_name(row.get("name", "")) for row in members if row.get("name")})
        if len(distinct_names) < 3:
            continue
        first = members[0]
        rows.append(
            {
                **identity_row(
                    {
                        **first,
                        "listing_key": "|".join(row_listing_key(row) for row in members),
                        "ticker": "|".join(sorted({row.get("ticker", "") for row in members})),
                        "exchange": "|".join(sorted({row.get("exchange", "") for row in members})),
                    },
                    "umbrella_isin_three_or_more_distinct_names",
                    "high",
                    len(members),
                    "data/listings.csv",
                ),
                "name": "|".join(sorted({row.get("name", "") for row in members})[:8]),
            }
        )

    for row in collision_rows:
        rows.append(
            {
                "campaign": "C4_identity_residual_burn_down",
                "listing_key": row.get("listing_keys", ""),
                "ticker": row.get("member_tickers", ""),
                "exchange": row.get("member_exchanges", ""),
                "asset_type": "",
                "name": row.get("member_names", ""),
                "isin": row.get("isin", ""),
                "residual_type": "disjoint_isin_identity_collision_queue",
                "severity": "high",
                "cluster_size": row.get("listing_count", ""),
                "evidence_source": "data/reports/isin_identity_collision_review_queue.csv",
                "decision": "manual_review_requires_official_identifier_evidence",
                "review_priority": "P1",
                "verification_evidence_required": "valid_isin_checksum_openfigi_id_isin_and_official_masterfile_or_same_legal_entity_evidence",
                "recommended_next_source": row.get("recommended_next_source", ""),
                "source_gate": row.get("review_gate", ""),
            }
        )
    return rows, summarize_decisions(rows)


def identity_row(
    row: dict[str, str],
    residual_type: str,
    severity: str,
    cluster_size: int,
    evidence_source: str,
) -> dict[str, Any]:
    priority = "P1" if severity == "high" else "P2"
    return {
        "campaign": "C4_identity_residual_burn_down",
        "listing_key": row.get("listing_key", ""),
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": row.get("isin", ""),
        "residual_type": residual_type,
        "severity": severity,
        "cluster_size": cluster_size,
        "evidence_source": evidence_source,
        "decision": "manual_review_requires_official_identifier_evidence",
        "review_priority": priority,
        "verification_evidence_required": "valid_isin_checksum_openfigi_id_isin_and_official_masterfile_or_same_legal_entity_evidence",
        "recommended_next_source": "Official masterfile, national numbering agency, OpenFIGI ID_ISIN, issuer page, CIK/LEI, or corporate-action evidence.",
        "source_gate": "Never collapse or rewrite identities from shared ticker, shared name token, or umbrella ISIN alone.",
    }


def build_non_equity_leakage_campaign(
    listings: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    official = active_official_reference_lookup(masterfiles)
    rows: list[dict[str, Any]] = []
    for listing in listings:
        if listing.get("asset_type") != "Stock":
            continue
        refs = official.get((listing.get("ticker", ""), listing.get("exchange", ""), "Stock"), [])
        evidence_row = dict(listing)
        if refs:
            evidence_row.update({key: value for key, value in refs[0].items() if key not in evidence_row or value})
        result = classify_non_equity_leakage(evidence_row)
        if result["guard_decision"] == "accepted_or_not_applicable":
            continue
        rows.append(
            {
                "campaign": "C5_non_equity_leakage_guard",
                "listing_key": row_listing_key(listing),
                "ticker": listing.get("ticker", ""),
                "exchange": listing.get("exchange", ""),
                "asset_type": listing.get("asset_type", ""),
                "name": listing.get("name", ""),
                "isin": listing.get("isin", ""),
                **result,
            }
        )
    return rows, summarize_decisions(rows, decision_key="guard_decision")


def summarize_decisions(rows: list[dict[str, Any]], decision_key: str = "decision") -> dict[str, Any]:
    decisions = Counter(str(row.get(decision_key, "") or "unknown") for row in rows)
    applied = sum(count for decision, count in decisions.items() if decision.startswith("applied"))
    blocked = sum(count for decision, count in decisions.items() if decision.startswith("blocked"))
    manual = sum(
        count
        for decision, count in decisions.items()
        if "manual" in decision or "review" in decision or "requires_official" in decision
    )
    return {
        "rows": len(rows),
        "candidate_rows": len(rows),
        "applied_rows": applied,
        "blocked_rows": blocked,
        "manual_review_rows": manual,
        "metadata_override_pending_rows": decisions.get("metadata_override_present_pending_rebuild", 0),
        "decision_totals": dict(sorted(decisions.items())),
    }


def build_audit_rows(campaign_summaries: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    audit_blocks = [
        ("after_c1_sector_category", "C1_sector_etf_category_truth"),
        ("after_c2_name_freshness", "C2_name_freshness"),
        ("after_c4_identity_residual", "C4_identity_residual_burn_down"),
        ("after_c5_non_equity_guard", "C5_non_equity_leakage_guard"),
    ]
    for block, campaign in audit_blocks:
        summary = campaign_summaries.get(campaign, {})
        rows_reviewed = int(summary.get("rows", 0) or 0)
        applied = int(summary.get("applied_rows", 0) or 0)
        blocked = int(summary.get("blocked_rows", 0) or 0)
        manual = int(summary.get("manual_review_rows", 0) or 0)
        rows.append(
            {
                "audit_block": block,
                "campaign": campaign,
                "rows_reviewed": rows_reviewed,
                "manual_review_rows": manual,
                "blocked_rows": blocked,
                "applied_rows": applied,
                "changed_rows": applied,
                "adversarial_changed_rows_checked": applied,
                "two_source_possible_rows": 0,
                "two_source_gap_rows": rows_reviewed,
                "full_row_correctness_claim": "not_claimed_from_campaign_report",
                "metric_context": (
                    f"rows={rows_reviewed};manual={manual};blocked={blocked};"
                    f"applied={applied};claim=not_99_percent_without_external_stratified_audit"
                ),
                "source_gate": (
                    "Audit rows are reproducible campaign checks. They do not claim >=99% correctness "
                    "without a stratified external audit using independent sources."
                ),
            }
        )
    return rows


def write_report(
    *,
    csv_out: Path | None,
    json_out: Path,
    md_out: Path,
    fieldnames: list[str],
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    source_files: dict[str, str],
    generated_at: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if csv_out is not None:
        write_csv(csv_out, fieldnames, rows)
    payload = {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(rows),
            "source_files": source_files,
            "policy": (
                "M3 correctness campaign evidence. Missing or manual-review data is preferred to wrong data; "
                "secondary evidence never authorizes direct application."
            ),
        },
        "summary": summary,
        "rows": rows,
    }
    if extra:
        payload.update(extra)
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    return payload


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload.get("summary", {})
    lines = [
        f"# {path.stem.replace('_', ' ').title()}",
        "",
        f"Generated at: `{payload.get('_meta', {}).get('generated_at', '')}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        lines.append(f"| `{key}` | `{str(value).replace('|', '\\|')}` |")
    rows = payload.get("rows", [])
    if isinstance(rows, list):
        lines.extend(["", "## Rows", "", f"Rows: `{len(rows)}`"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_payloads(
    *,
    listings_csv: Path = LISTINGS_CSV,
    masterfile_reference_csv: Path = MASTERFILE_REFERENCE_CSV,
    entry_quality_csv: Path = ENTRY_QUALITY_CSV,
    symbol_changes_review_csv: Path = SYMBOL_CHANGES_REVIEW_CSV,
    isin_identity_collision_csv: Path = ISIN_IDENTITY_COLLISION_CSV,
    metadata_updates_csv: Path = METADATA_UPDATES_CSV,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    listings = load_csv(listings_csv)
    masterfiles = load_csv(masterfile_reference_csv)
    entry_quality = load_csv(entry_quality_csv)
    symbol_changes = load_csv(symbol_changes_review_csv)
    collisions = load_csv(isin_identity_collision_csv)
    metadata_updates = load_csv(metadata_updates_csv)
    source_files = {
        "listings_csv": display_path(listings_csv, ROOT),
        "masterfile_reference_csv": display_path(masterfile_reference_csv, ROOT),
        "entry_quality_csv": display_path(entry_quality_csv, ROOT),
        "symbol_changes_review_csv": display_path(symbol_changes_review_csv, ROOT),
        "isin_identity_collision_csv": display_path(isin_identity_collision_csv, ROOT),
        "metadata_updates_csv": display_path(metadata_updates_csv, ROOT),
    }

    sector_rows, sector_market_rows, sector_summary = build_sector_category_campaign(
        listings,
        masterfiles,
        metadata_updates,
    )
    name_rows, name_summary = build_name_freshness_campaign(entry_quality, masterfiles, symbol_changes)
    identity_rows, identity_summary = build_identity_residual_campaign(listings, entry_quality, collisions)
    leakage_rows, leakage_summary = build_non_equity_leakage_campaign(listings, masterfiles)

    write_csv(SECTOR_MARKET_CSV_OUT, SECTOR_MARKET_FIELDNAMES, sector_market_rows)
    sector_payload = write_report(
        csv_out=SECTOR_CSV_OUT,
        json_out=SECTOR_JSON_OUT,
        md_out=SECTOR_MD_OUT,
        fieldnames=SECTOR_FIELDNAMES,
        rows=sector_rows,
        summary=sector_summary,
        source_files=source_files,
        generated_at=generated_at,
        extra={
            "market_summaries": sector_market_rows,
            "market_csv": display_path(SECTOR_MARKET_CSV_OUT, ROOT),
        },
    )
    name_payload = write_report(
        csv_out=NAME_CSV_OUT,
        json_out=NAME_JSON_OUT,
        md_out=NAME_MD_OUT,
        fieldnames=NAME_FIELDNAMES,
        rows=name_rows,
        summary=name_summary,
        source_files=source_files,
        generated_at=generated_at,
    )
    identity_payload = write_report(
        csv_out=IDENTITY_CSV_OUT,
        json_out=IDENTITY_JSON_OUT,
        md_out=IDENTITY_MD_OUT,
        fieldnames=IDENTITY_FIELDNAMES,
        rows=identity_rows,
        summary=identity_summary,
        source_files=source_files,
        generated_at=generated_at,
    )
    leakage_payload = write_report(
        csv_out=LEAKAGE_CSV_OUT,
        json_out=LEAKAGE_JSON_OUT,
        md_out=LEAKAGE_MD_OUT,
        fieldnames=LEAKAGE_FIELDNAMES,
        rows=leakage_rows,
        summary=leakage_summary,
        source_files=source_files,
        generated_at=generated_at,
    )
    campaign_summaries = {
        "C1_sector_etf_category_truth": sector_summary,
        "C2_name_freshness": name_summary,
        "C4_identity_residual_burn_down": identity_summary,
        "C5_non_equity_leakage_guard": leakage_summary,
    }
    audit_rows = build_audit_rows(campaign_summaries)
    audit_summary = {
        "rows": len(audit_rows),
        "audit_blocks": len(audit_rows),
        "changed_rows": sum(int(row["changed_rows"]) for row in audit_rows),
        "full_row_correctness_claim": "not_claimed_99_percent_without_external_stratified_audit",
        "policy": "Re-audit after each block is generated, but >=99% is not claimed from these campaign artifacts.",
    }
    audit_payload = write_report(
        csv_out=AUDIT_CSV_OUT,
        json_out=AUDIT_JSON_OUT,
        md_out=AUDIT_MD_OUT,
        fieldnames=AUDIT_FIELDNAMES,
        rows=audit_rows,
        summary=audit_summary,
        source_files=source_files,
        generated_at=generated_at,
    )
    rollup = build_rollup(
        generated_at=generated_at,
        source_files=source_files,
        payloads={
            "C1_sector_etf_category_truth": sector_payload,
            "C2_name_freshness": name_payload,
            "C4_identity_residual_burn_down": identity_payload,
            "C5_non_equity_leakage_guard": leakage_payload,
            "C6_reaudit_after_each_block": audit_payload,
        },
    )
    write_json(ROLLUP_JSON_OUT, rollup)
    write_markdown(ROLLUP_MD_OUT, rollup)
    return rollup


def build_rollup(
    *,
    generated_at: str,
    source_files: dict[str, str],
    payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    campaigns = []
    artifact_paths = {
        "C1_sector_etf_category_truth": display_path(SECTOR_JSON_OUT, ROOT),
        "C2_name_freshness": display_path(NAME_JSON_OUT, ROOT),
        "C4_identity_residual_burn_down": display_path(IDENTITY_JSON_OUT, ROOT),
        "C5_non_equity_leakage_guard": display_path(LEAKAGE_JSON_OUT, ROOT),
        "C6_reaudit_after_each_block": display_path(AUDIT_JSON_OUT, ROOT),
    }
    for key, payload in payloads.items():
        summary = payload.get("summary", {})
        campaigns.append(
            {
                "campaign_key": key,
                "artifact": artifact_paths[key],
                "rows": int(summary.get("rows", 0) or 0),
                "applied_rows": int(summary.get("applied_rows", 0) or 0),
                "blocked_rows": int(summary.get("blocked_rows", 0) or 0),
                "manual_review_rows": int(summary.get("manual_review_rows", 0) or 0),
                "generated_at": payload.get("_meta", {}).get("generated_at", ""),
                "status": "generated_reviewable_evidence",
            }
        )
    summary = {
        "campaigns": len(campaigns),
        "required_campaigns_present": all(
            key in payloads
            for key in {
                "C1_sector_etf_category_truth",
                "C2_name_freshness",
                "C4_identity_residual_burn_down",
                "C5_non_equity_leakage_guard",
                "C6_reaudit_after_each_block",
            }
        ),
        "applied_rows_total": sum(row["applied_rows"] for row in campaigns),
        "blocked_rows_total": sum(row["blocked_rows"] for row in campaigns),
        "manual_review_rows_total": sum(row["manual_review_rows"] for row in campaigns),
        "correctness_target": "move_measured_full_row_correctness_toward_99_percent",
        "correctness_claim": "not_claimed_99_percent_without_external_stratified_audit",
    }
    return {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(campaigns),
            "source_files": source_files,
            "policy": (
                "M3 rollup for correctness campaigns C1, C2, C4, C5, and C6. "
                "It records evidence, blocked rows, and manual review queues; it does not make a >=99% claim."
            ),
        },
        "summary": summary,
        "campaigns": campaigns,
        "rows": campaigns,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build M3 correctness campaign and re-audit reports.")
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--symbol-changes-review-csv", type=Path, default=SYMBOL_CHANGES_REVIEW_CSV)
    parser.add_argument("--isin-identity-collision-csv", type=Path, default=ISIN_IDENTITY_COLLISION_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=METADATA_UPDATES_CSV)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rollup = build_payloads(
        listings_csv=args.listings_csv,
        masterfile_reference_csv=args.masterfile_reference_csv,
        entry_quality_csv=args.entry_quality_csv,
        symbol_changes_review_csv=args.symbol_changes_review_csv,
        isin_identity_collision_csv=args.isin_identity_collision_csv,
        metadata_updates_csv=args.metadata_updates_csv,
    )
    print(json.dumps(rollup["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
