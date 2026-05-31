from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from scripts.listing_keys import build_listing_key, row_listing_key
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from listing_keys import build_listing_key, row_listing_key


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "masterfile_collision_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "masterfile_collision_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "masterfile_collision_review.md"

CSV_FIELDNAMES = [
    "target_listing_key",
    "ticker",
    "target_exchange",
    "official_name",
    "official_asset_type",
    "official_isin",
    "official_sector",
    "official_source_key",
    "official_source_url",
    "official_source_context",
    "existing_listing_keys",
    "existing_exchanges",
    "existing_names",
    "existing_asset_types",
    "existing_isins",
    "same_isin_listing_keys",
    "name_exact_match_listing_keys",
    "asset_type_mismatch",
    "existing_dataset_context",
    "identity_evidence",
    "identity_resolution_context",
    "collision_risk_flags",
    "identity_resolution_queue",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "review_decision",
    "clearance_evidence_required",
    "recommended_next_action",
]


def utc_now_iso() -> str:
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


def listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or row_listing_key(row)


def normalize_name(value: str) -> str:
    return " ".join(value.casefold().replace("&", " and ").split())


def compact_join(values: list[str] | set[str]) -> str:
    return "|".join(sorted({value for value in values if value}))


def build_review_decision(
    *,
    official_isin: str,
    same_isin_listing_keys: list[str],
    existing_listing_keys: list[str],
) -> str:
    if official_isin and same_isin_listing_keys:
        return "same_isin_cross_listing_candidate_requires_exchange_scope_review"
    if official_isin and not same_isin_listing_keys:
        return "new_listing_candidate_requires_official_listing_add_review"
    if existing_listing_keys:
        return "symbol_collision_requires_non_symbol_identity_source"
    return "not_a_collision"


def next_action_for(review_decision: str, asset_type_mismatch: str) -> str:
    if asset_type_mismatch == "true":
        return "resolve_instrument_type_before_any_listing_addition"
    if review_decision == "same_isin_cross_listing_candidate_requires_exchange_scope_review":
        return "verify_target_exchange_listing_status_mic_name_and_instrument_type_before_cross_listing_add"
    if review_decision == "new_listing_candidate_requires_official_listing_add_review":
        return "add_only_after_official_listing_key_isin_name_exchange_and_scope_review"
    if review_decision == "symbol_collision_requires_non_symbol_identity_source":
        return "keep_absent_until_official_isin_or_other_non_symbol_identity_source_distinguishes_listing"
    return "no_action"


def identity_evidence_for(
    *,
    official_isin: str,
    same_isin_listing_keys: list[str],
    name_exact_match_listing_keys: list[str],
    asset_type_mismatch: str,
) -> str:
    evidence: list[str] = []
    if official_isin:
        evidence.append("official_isin")
    if same_isin_listing_keys:
        evidence.append("same_isin_existing_listing")
    if name_exact_match_listing_keys:
        evidence.append("exact_name_match")
    evidence.append("asset_type_conflict" if asset_type_mismatch == "true" else "asset_type_consistent")
    return "|".join(evidence)


def collision_risk_flags_for(
    *,
    official_isin: str,
    existing_isins: list[str],
    existing_listing_keys: list[str],
    same_isin_listing_keys: list[str],
    name_exact_match_listing_keys: list[str],
    asset_type_mismatch: str,
) -> str:
    flags: list[str] = ["ticker_reused_on_other_exchange"]
    if not official_isin:
        flags.append("missing_official_isin")
    if official_isin and {isin for isin in existing_isins if isin and isin != official_isin}:
        flags.append("existing_isin_conflict")
    if same_isin_listing_keys:
        flags.append("same_isin_existing_listing")
    if name_exact_match_listing_keys:
        flags.append("exact_name_match")
    else:
        flags.append("no_exact_name_match")
    if len(existing_listing_keys) > 1:
        flags.append("multiple_existing_symbol_rows")
    if asset_type_mismatch == "true":
        flags.append("asset_type_mismatch")
    return "|".join(flags)


def clearance_evidence_for(review_bucket: str) -> str:
    if review_bucket == "same_isin_exact_name_cross_listing_candidate":
        return "official_target_exchange_listing_status_mic_name_instrument_type"
    if review_bucket == "same_isin_cross_listing_needs_name_or_scope_review":
        return "official_target_exchange_listing_status_plus_name_or_scope_reconciliation"
    if review_bucket == "distinct_official_isin_new_listing_candidate":
        return "official_target_exchange_listing_key_isin_name_instrument_type_listing_status"
    if review_bucket == "resolve_asset_type_conflict_before_identity_review":
        return "official_instrument_type_resolution_before_listing_identity_review"
    if review_bucket == "hold_symbol_only_collision_needs_non_symbol_identity":
        return "official_non_symbol_identifier_or_keep_absent"
    return "none"


def identity_resolution_queue_for(review_bucket: str) -> str:
    if review_bucket == "same_isin_exact_name_cross_listing_candidate":
        return "review_cross_listing_same_isin_exact_name"
    if review_bucket == "same_isin_cross_listing_needs_name_or_scope_review":
        return "review_cross_listing_same_isin_name_or_scope_gap"
    if review_bucket == "distinct_official_isin_new_listing_candidate":
        return "review_distinct_official_isin_new_listing"
    if review_bucket == "resolve_asset_type_conflict_before_identity_review":
        return "blocked_asset_type_conflict"
    if review_bucket == "hold_symbol_only_collision_needs_non_symbol_identity":
        return "blocked_symbol_only_missing_non_symbol_identity"
    return "no_collision_action"


def review_bucket_for(
    *,
    review_decision: str,
    same_isin_listing_keys: list[str],
    name_exact_match_listing_keys: list[str],
    asset_type_mismatch: str,
) -> str:
    if asset_type_mismatch == "true":
        return "resolve_asset_type_conflict_before_identity_review"
    if review_decision == "same_isin_cross_listing_candidate_requires_exchange_scope_review":
        if same_isin_listing_keys and name_exact_match_listing_keys:
            return "same_isin_exact_name_cross_listing_candidate"
        return "same_isin_cross_listing_needs_name_or_scope_review"
    if review_decision == "new_listing_candidate_requires_official_listing_add_review":
        return "distinct_official_isin_new_listing_candidate"
    if review_decision == "symbol_collision_requires_non_symbol_identity_source":
        return "hold_symbol_only_collision_needs_non_symbol_identity"
    return "no_collision_action"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket == "same_isin_exact_name_cross_listing_candidate":
        return "P1"
    if review_bucket in {
        "same_isin_cross_listing_needs_name_or_scope_review",
        "distinct_official_isin_new_listing_candidate",
        "resolve_asset_type_conflict_before_identity_review",
    }:
        return "P2"
    if review_bucket == "hold_symbol_only_collision_needs_non_symbol_identity":
        return "P3"
    return "P4"


def pair_review_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "review_cross_listing_same_isin_exact_name":
        return (
            "batch_review_same_isin_exact_name_cross_listing_scope",
            "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
        )
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return (
            "batch_review_same_isin_name_or_scope_reconciliation",
            "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation",
        )
    if queue == "review_distinct_official_isin_new_listing":
        return (
            "batch_review_distinct_isin_new_listing_candidates",
            "official_target_exchange_listing_key_isin_name_instrument_type_listing_status",
        )
    if queue == "blocked_asset_type_conflict":
        return (
            "batch_block_instrument_type_conflict_until_official_resolution",
            "official_instrument_type_resolution_before_listing_identity_review",
        )
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return (
            "batch_hold_symbol_reuse_until_non_symbol_identity_source",
            "official_non_symbol_identifier_or_keep_absent",
        )
    return ("no_collision_strategy", "none")


def pair_recommended_next_source_for(queue: str, exchange_pair: str) -> str:
    if queue == "review_cross_listing_same_isin_exact_name":
        return f"Official active-listing directories for both exchanges in {exchange_pair}."
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return f"Official target-exchange status plus issuer/name or scope reconciliation for {exchange_pair}."
    if queue == "review_distinct_official_isin_new_listing":
        return f"Official target-exchange listing record for {exchange_pair} with listing key, ISIN, name, type, and status."
    if queue == "blocked_asset_type_conflict":
        return f"Official instrument-type evidence resolving the asset-type conflict for {exchange_pair}."
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return f"Official non-symbol identifier evidence for {exchange_pair}, or keep the target listing absent."
    return f"Manual identity-resolution evidence for {exchange_pair}."


def pair_source_gate_for(queue: str) -> str:
    if queue == "review_cross_listing_same_isin_exact_name":
        return "Do not add or merge until both official exchange directories confirm the same active instrument."
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return "Do not resolve identity until official listing status and issuer/name or scope differences are reconciled."
    if queue == "review_distinct_official_isin_new_listing":
        return "Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status."
    if queue == "blocked_asset_type_conflict":
        return "Block identity resolution until official instrument-type evidence resolves the conflict."
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return "Keep absent; ticker equality alone is not identity evidence."
    return "Manual review required; symbol-only evidence is insufficient."


def clearance_readiness_for(queue: str) -> str:
    if queue == "review_cross_listing_same_isin_exact_name":
        return "review_ready_same_isin_exact_name_scope_check"
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return "needs_official_name_or_scope_reconciliation"
    if queue == "review_distinct_official_isin_new_listing":
        return "needs_official_listing_add_review"
    if queue == "blocked_asset_type_conflict":
        return "blocked_until_asset_type_conflict_resolved"
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return "blocked_symbol_only_non_symbol_identity_required"
    return "manual_clearance_review_required"


def row_exchange_context(target_exchange: str, existing_exchanges: str) -> str:
    pairs = [f"{target_exchange}::{exchange}" for exchange in existing_exchanges.split("|") if exchange]
    return "|".join(pairs) or target_exchange


def official_source_context_for(row: dict[str, str]) -> str:
    source_key = row.get("official_source_key") or row.get("source_key") or "none"
    source_url = row.get("official_source_url") or row.get("source_url") or "none"
    return f"official_source_key={source_key};official_source_url={source_url}"


def existing_dataset_context_for(row: dict[str, str]) -> str:
    return (
        f"existing_listing_keys={row.get('existing_listing_keys', '') or 'none'};"
        f"existing_exchanges={row.get('existing_exchanges', '') or 'none'};"
        f"existing_isins={row.get('existing_isins', '') or 'none'};"
        f"same_isin_listing_keys={row.get('same_isin_listing_keys', '') or 'none'};"
        f"name_exact_match_listing_keys={row.get('name_exact_match_listing_keys', '') or 'none'};"
        f"asset_type_mismatch={row.get('asset_type_mismatch', '') or 'none'}"
    )


def identity_resolution_context_for(row: dict[str, str]) -> str:
    exchange_context = row_exchange_context(
        row.get("target_exchange", ""),
        row.get("existing_exchanges", ""),
    )
    return (
        f"exchange_context={exchange_context or 'none'};"
        f"identity_resolution_queue={row.get('identity_resolution_queue', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"identity_evidence={row.get('identity_evidence', '') or 'none'}"
    )


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    dataset_keys = {listing_key(row) for row in tickers if row.get("ticker") and row.get("exchange")}
    dataset_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    dataset_by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in tickers:
        ticker = row.get("ticker", "")
        exchange = row.get("exchange", "")
        if not ticker or not exchange:
            continue
        dataset_by_ticker[ticker].append(row)
        isin = row.get("isin", "")
        if isin:
            dataset_by_isin[isin].append(row)

    rows: list[dict[str, str]] = []
    seen_target_keys: set[str] = set()
    for ref in masterfile_rows:
        if ref.get("listing_status") != "active" or ref.get("reference_scope") != "exchange_directory":
            continue
        ticker = ref.get("ticker", "")
        target_exchange = ref.get("exchange", "")
        if not ticker or not target_exchange:
            continue
        target_key = build_listing_key(ticker, target_exchange)
        if target_key in seen_target_keys:
            continue
        seen_target_keys.add(target_key)
        if target_key in dataset_keys:
            continue

        existing_rows = [
            row
            for row in dataset_by_ticker.get(ticker, [])
            if row.get("exchange") != target_exchange
        ]
        if not existing_rows:
            continue

        official_isin = ref.get("isin", "")
        same_isin_rows = [
            row
            for row in dataset_by_isin.get(official_isin, [])
            if official_isin and listing_key(row) != target_key
        ]
        official_name = ref.get("name", "")
        official_asset_type = ref.get("asset_type", "")
        existing_asset_types = {row.get("asset_type", "") for row in existing_rows if row.get("asset_type", "")}
        asset_type_mismatch = "true" if official_asset_type and existing_asset_types - {official_asset_type} else "false"
        name_exact_match_listing_keys = [
            listing_key(row)
            for row in existing_rows
            if official_name and normalize_name(official_name) == normalize_name(row.get("name", ""))
        ]
        existing_listing_keys = [listing_key(row) for row in existing_rows]
        existing_isins = [row.get("isin", "") for row in existing_rows]
        same_isin_listing_keys = [listing_key(row) for row in same_isin_rows]
        review_decision = build_review_decision(
            official_isin=official_isin,
            same_isin_listing_keys=same_isin_listing_keys,
            existing_listing_keys=existing_listing_keys,
        )
        review_bucket = review_bucket_for(
            review_decision=review_decision,
            same_isin_listing_keys=same_isin_listing_keys,
            name_exact_match_listing_keys=name_exact_match_listing_keys,
            asset_type_mismatch=asset_type_mismatch,
        )

        identity_resolution_queue = identity_resolution_queue_for(review_bucket)
        review_strategy, verification_evidence_required = pair_review_strategy_for(identity_resolution_queue)
        existing_exchanges = compact_join({row.get("exchange", "") for row in existing_rows})
        exchange_context = row_exchange_context(target_exchange, existing_exchanges)
        row = {
            "target_listing_key": target_key,
            "ticker": ticker,
            "target_exchange": target_exchange,
            "official_name": official_name,
            "official_asset_type": official_asset_type,
            "official_isin": official_isin,
            "official_sector": ref.get("sector", ""),
            "official_source_key": ref.get("source_key", ""),
            "official_source_url": ref.get("source_url", ""),
            "existing_listing_keys": compact_join(existing_listing_keys),
            "existing_exchanges": existing_exchanges,
            "existing_names": compact_join([row.get("name", "") for row in existing_rows]),
            "existing_asset_types": compact_join(existing_asset_types),
            "existing_isins": compact_join(existing_isins),
            "same_isin_listing_keys": compact_join(same_isin_listing_keys),
            "name_exact_match_listing_keys": compact_join(name_exact_match_listing_keys),
            "asset_type_mismatch": asset_type_mismatch,
            "identity_evidence": identity_evidence_for(
                official_isin=official_isin,
                same_isin_listing_keys=same_isin_listing_keys,
                name_exact_match_listing_keys=name_exact_match_listing_keys,
                asset_type_mismatch=asset_type_mismatch,
            ),
            "collision_risk_flags": collision_risk_flags_for(
                official_isin=official_isin,
                existing_isins=existing_isins,
                existing_listing_keys=existing_listing_keys,
                same_isin_listing_keys=same_isin_listing_keys,
                name_exact_match_listing_keys=name_exact_match_listing_keys,
                asset_type_mismatch=asset_type_mismatch,
            ),
            "identity_resolution_queue": identity_resolution_queue,
            "review_bucket": review_bucket,
            "review_priority": review_priority_for(review_bucket),
            "review_strategy": review_strategy,
            "verification_evidence_required": verification_evidence_required,
            "recommended_next_source": pair_recommended_next_source_for(identity_resolution_queue, exchange_context),
            "source_gate": pair_source_gate_for(identity_resolution_queue),
            "review_decision": review_decision,
            "clearance_evidence_required": clearance_evidence_for(review_bucket),
            "recommended_next_action": next_action_for(review_decision, asset_type_mismatch),
        }
        row["official_source_context"] = official_source_context_for(row)
        row["existing_dataset_context"] = existing_dataset_context_for(row)
        row["identity_resolution_context"] = identity_resolution_context_for(row)
        rows.append(row)

    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["target_exchange"], row["ticker"], row["target_listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    risk_flags = Counter(
        flag
        for row in rows
        for flag in row.get("collision_risk_flags", "").split("|")
        if flag
    )
    queue_totals = Counter(row["identity_resolution_queue"] for row in rows)
    identity_resolution_queues = sorted({row["identity_resolution_queue"] for row in rows})
    queue_existing_exchange_pair_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_review_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_evidence_required_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_identity_evidence_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_clearance_readiness_counter: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        queue = row["identity_resolution_queue"]
        queue_clearance_readiness_counter[queue][clearance_readiness_for(queue)] += 1
        queue_review_strategy_counter[queue][row.get("review_strategy", "")] += 1
        queue_evidence_required_counter[queue][row.get("verification_evidence_required", "")] += 1
        for evidence in row.get("identity_evidence", "").split("|"):
            if evidence:
                queue_identity_evidence_counter[queue][evidence] += 1
        target_exchange = row["target_exchange"]
        for existing_exchange in row.get("existing_exchanges", "").split("|"):
            if existing_exchange:
                queue_existing_exchange_pair_counter[queue][f"{target_exchange}::{existing_exchange}"] += 1
    pair_review_strategy_totals: dict[str, Counter[str]] = defaultdict(Counter)
    top_pair_review_batches: list[dict[str, Any]] = []
    top_clearance_batches: list[dict[str, Any]] = []
    exact_name_scope_batches: Counter[tuple[str, str, str, str]] = Counter()
    for queue in identity_resolution_queues:
        strategy, evidence_required = pair_review_strategy_for(queue)
        queue_count = sum(queue_clearance_readiness_counter.get(queue, Counter()).values())
        if queue_count:
            top_clearance_batches.append(
                {
                    "identity_resolution_queue": queue,
                    "clearance_readiness": clearance_readiness_for(queue),
                    "rows": queue_count,
                    "review_strategy": strategy,
                    "evidence_required": evidence_required,
                    "recommended_next_source": pair_recommended_next_source_for(queue, "target::existing"),
                    "source_gate": pair_source_gate_for(queue),
                }
            )
        for exchange_pair, count in queue_existing_exchange_pair_counter.get(queue, Counter()).items():
            pair_review_strategy_totals[queue][strategy] += count
        for exchange_pair, count in sorted(
            queue_existing_exchange_pair_counter.get(queue, Counter()).items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]:
            top_pair_review_batches.append(
                {
                    "identity_resolution_queue": queue,
                    "exchange_pair": exchange_pair,
                    "rows": count,
                    "review_strategy": strategy,
                    "evidence_required": evidence_required,
                    "recommended_next_source": pair_recommended_next_source_for(queue, exchange_pair),
                    "source_gate": pair_source_gate_for(queue),
                }
            )
    for row in rows:
        if row.get("identity_resolution_queue") != "review_cross_listing_same_isin_exact_name":
            continue
        target_exchange = row["target_exchange"]
        for existing_exchange in row.get("existing_exchanges", "").split("|"):
            if existing_exchange:
                exact_name_scope_batches[
                    (
                        f"{target_exchange}::{existing_exchange}",
                        row.get("official_source_key", "") or "missing",
                        row.get("official_asset_type", "") or "missing",
                        row.get("clearance_evidence_required", "") or "missing",
                    )
                ] += 1
    top_exact_name_scope_batches = []
    exact_name_strategy, exact_name_evidence = pair_review_strategy_for(
        "review_cross_listing_same_isin_exact_name"
    )
    for (exchange_pair, official_source, asset_type, clearance_evidence), count in sorted(
        exact_name_scope_batches.items(),
        key=lambda item: (-item[1], item[0]),
    )[:25]:
        top_exact_name_scope_batches.append(
            {
                "exchange_pair": exchange_pair,
                "official_source_key": official_source,
                "official_asset_type": asset_type,
                "clearance_evidence_required": clearance_evidence,
                "rows": count,
                "review_strategy": exact_name_strategy,
                "evidence_required": exact_name_evidence,
                "recommended_next_source": pair_recommended_next_source_for(
                    "review_cross_listing_same_isin_exact_name",
                    exchange_pair,
                ),
                "source_gate": pair_source_gate_for("review_cross_listing_same_isin_exact_name"),
            }
        )
    identity_resolution_backlog = {
        "status": "identity_resolution_review_queue_open",
        "rows": len(rows),
        "same_isin_exact_name_scope_review_rows": queue_totals.get("review_cross_listing_same_isin_exact_name", 0),
        "same_isin_name_or_scope_reconciliation_rows": queue_totals.get(
            "review_cross_listing_same_isin_name_or_scope_gap", 0
        ),
        "distinct_official_isin_listing_add_review_rows": queue_totals.get(
            "review_distinct_official_isin_new_listing", 0
        ),
        "asset_type_conflict_blocked_rows": queue_totals.get("blocked_asset_type_conflict", 0),
        "symbol_only_non_symbol_identity_required_rows": queue_totals.get(
            "blocked_symbol_only_missing_non_symbol_identity", 0
        ),
        "direct_listing_add_allowed_rows": 0,
        "symbol_only_resolution_authorized": False,
        "source_gate": (
            "Masterfile collision rows remain review queues only; listing additions, merges, renames, "
            "or enrichments require official non-symbol identity evidence for the target listing."
        ),
    }
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "decision_totals": dict(sorted(Counter(row["review_decision"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "collision_risk_flag_totals": dict(sorted(risk_flags.items())),
        "identity_resolution_queue_totals": dict(sorted(queue_totals.items())),
        "identity_resolution_backlog": identity_resolution_backlog,
        "identity_resolution_risk_flag_totals": {
            queue: dict(
                sorted(
                    Counter(
                        flag
                        for row in rows
                        if row["identity_resolution_queue"] == queue
                        for flag in row.get("collision_risk_flags", "").split("|")
                        if flag
                    ).items()
                )
            )
            for queue in identity_resolution_queues
        },
        "identity_resolution_exchange_totals": {
            queue: dict(sorted(Counter(row["target_exchange"] for row in rows if row["identity_resolution_queue"] == queue).items()))
            for queue in identity_resolution_queues
        },
        "identity_resolution_asset_type_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row["official_asset_type"] or "missing"
                        for row in rows
                        if row["identity_resolution_queue"] == queue
                    ).items()
                )
            )
            for queue in identity_resolution_queues
        },
        "identity_resolution_official_source_totals": {
            queue: dict(
                sorted(
                    Counter(
                        row["official_source_key"] or "missing"
                        for row in rows
                        if row["identity_resolution_queue"] == queue
                    ).items()
                )
            )
            for queue in identity_resolution_queues
        },
        "identity_resolution_existing_exchange_pair_totals": {
            queue: dict(sorted(queue_existing_exchange_pair_counter.get(queue, Counter()).items()))
            for queue in identity_resolution_queues
        },
        "identity_resolution_pair_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(pair_review_strategy_totals.items())
        },
        "identity_resolution_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(queue_review_strategy_counter.items())
        },
        "identity_resolution_evidence_required_totals": {
            queue: dict(sorted(evidence_totals.items()))
            for queue, evidence_totals in sorted(queue_evidence_required_counter.items())
        },
        "identity_resolution_identity_evidence_totals": {
            queue: dict(sorted(evidence_totals.items()))
            for queue, evidence_totals in sorted(queue_identity_evidence_counter.items())
        },
        "identity_resolution_clearance_readiness_totals": dict(
            sorted(
                Counter(
                    clearance_readiness_for(row["identity_resolution_queue"])
                    for row in rows
                ).items()
            )
        ),
        "identity_resolution_queue_clearance_readiness_totals": {
            queue: dict(sorted(readiness_totals.items()))
            for queue, readiness_totals in sorted(queue_clearance_readiness_counter.items())
        },
        "top_identity_resolution_clearance_batches": sorted(
            top_clearance_batches,
            key=lambda row: (-int(row["rows"]), row["identity_resolution_queue"]),
        ),
        "top_identity_resolution_pair_review_batches": top_pair_review_batches,
        "same_isin_exact_name_scope_review_rows": sum(exact_name_scope_batches.values()),
        "top_same_isin_exact_name_scope_review_batches": top_exact_name_scope_batches,
        "clearance_evidence_totals": dict(sorted(Counter(row.get("clearance_evidence_required", "missing") for row in rows).items())),
        "exchange_totals": dict(sorted(Counter(row["target_exchange"] for row in rows).items())),
        "official_asset_type_totals": dict(sorted(Counter(row["official_asset_type"] or "missing" for row in rows).items())),
        "asset_type_mismatch_totals": dict(sorted(Counter(row["asset_type_mismatch"] for row in rows).items())),
        "official_source_totals": dict(sorted(Counter(row["official_source_key"] or "missing" for row in rows).items())),
        "policy": {
            "no_symbol_only_additions": "Rows in this report are review queues only. Ticker equality across exchanges is collision evidence, not listing identity evidence.",
            "truth_required": "A listing addition requires official target-exchange evidence for listing key, name, instrument type, listing status, and an identifier such as ISIN when available.",
            "blank_until_verified": "If a non-symbol identity source is unavailable or conflicts with existing rows, keep the target listing absent and preserve the gap for manual review.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    summary: dict[str, Any],
    rows: list[dict[str, str]],
    *,
    source_files: dict[str, Path],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_json_payload(summary, rows, source_files=source_files)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def build_json_payload(
    summary: dict[str, Any],
    rows: list[dict[str, str]],
    *,
    source_files: dict[str, Path],
) -> dict[str, Any]:
    return {
        "_meta": {
            "generated_at": summary.get("generated_at", utc_now_iso()),
            "source_files": {key: display_path(path) for key, path in source_files.items()},
            "policy": summary.get("policy", {}),
        },
        "summary": summary,
        "rows": rows,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Masterfile Collision Review",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        "This is a review queue only. It does not authorize symbol-only additions.",
        "",
        "## Summary",
        "",
        f"- Rows: {summary['rows']}",
        f"- Asset-type mismatches: {summary['asset_type_mismatch_totals'].get('true', 0)}",
        f"- Direct listing add allowed rows: {summary['identity_resolution_backlog']['direct_listing_add_allowed_rows']}",
        f"- Symbol-only resolution authorized: `{str(summary['identity_resolution_backlog']['symbol_only_resolution_authorized']).lower()}`",
        "",
        "## Identity Resolution Backlog",
        "",
        f"- Status: `{summary['identity_resolution_backlog']['status']}`",
        f"- Rows: `{summary['identity_resolution_backlog']['rows']}`",
        f"- Same-ISIN exact-name scope review rows: `{summary['identity_resolution_backlog']['same_isin_exact_name_scope_review_rows']}`",
        f"- Same-ISIN name/scope reconciliation rows: `{summary['identity_resolution_backlog']['same_isin_name_or_scope_reconciliation_rows']}`",
        f"- Distinct official-ISIN listing-add review rows: `{summary['identity_resolution_backlog']['distinct_official_isin_listing_add_review_rows']}`",
        f"- Asset-type conflict blocked rows: `{summary['identity_resolution_backlog']['asset_type_conflict_blocked_rows']}`",
        f"- Symbol-only non-symbol identity required rows: `{summary['identity_resolution_backlog']['symbol_only_non_symbol_identity_required_rows']}`",
        f"- Source gate: {summary['identity_resolution_backlog']['source_gate']}",
        "",
        "## Identity Resolution Queues",
        "",
        "| Queue | Rows |",
        "| --- | ---: |",
    ]
    for queue, count in summary["identity_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "## Identity Resolution Clearance Readiness", "", "| Readiness | Rows |", "| --- | ---: |"])
    for readiness, count in summary["identity_resolution_clearance_readiness_totals"].items():
        lines.append(f"| {readiness} | {count} |")
    lines.extend(
        [
            "",
            "## Top Clearance Batches",
            "",
            "| Queue | Readiness | Rows | Review Strategy | Evidence Required | Source Gate |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
    )
    for batch in summary["top_identity_resolution_clearance_batches"]:
        lines.append(
            "| "
            f"{batch['identity_resolution_queue']} | "
            f"{batch['clearance_readiness']} | "
            f"{batch['rows']} | "
            f"{batch['review_strategy']} | "
            f"{batch['evidence_required']} | "
            f"{batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Same-ISIN Exact-Name Scope Review Batches",
            "",
            f"Rows: {summary['same_isin_exact_name_scope_review_rows']}",
            "",
            "| Exchange Pair | Official Source | Asset Type | Clearance Evidence | Rows | Recommended Next Source | Source Gate |",
            "| --- | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for batch in summary["top_same_isin_exact_name_scope_review_batches"]:
        lines.append(
            "| "
            f"{batch['exchange_pair']} | "
            f"{batch['official_source_key']} | "
            f"{batch['official_asset_type']} | "
            f"{batch['clearance_evidence_required']} | "
            f"{batch['rows']} | "
            f"{batch['recommended_next_source']} | "
            f"{batch['source_gate']} |"
        )
    lines.extend(["", "## Identity Resolution By Asset Type", "", "| Queue | Asset Type | Rows |", "| --- | --- | ---: |"])
    for queue, asset_type_totals in summary["identity_resolution_asset_type_totals"].items():
        for asset_type, count in asset_type_totals.items():
            lines.append(f"| {queue} | {asset_type} | {count} |")
    lines.extend(["", "## Identity Resolution By Official Source", "", "| Queue | Official Source | Rows |", "| --- | --- | ---: |"])
    for queue, source_totals in summary["identity_resolution_official_source_totals"].items():
        for source, count in source_totals.items():
            lines.append(f"| {queue} | {source} | {count} |")
    lines.extend(["", "## Identity Resolution By Target/Existing Exchange Pair", "", "| Queue | Target/Existing Pair | Rows |", "| --- | --- | ---: |"])
    for queue, pair_totals in summary["identity_resolution_existing_exchange_pair_totals"].items():
        for pair, count in sorted(pair_totals.items(), key=lambda item: (-item[1], item[0]))[:50]:
            lines.append(f"| {queue} | {pair} | {count} |")
    lines.extend(
        [
            "",
            "## Top Pair Review Batches",
            "",
            "| Queue | Exchange Pair | Rows | Review Strategy | Evidence Required | Recommended Next Source | Source Gate |",
            "| --- | --- | ---: | --- | --- | --- | --- |",
        ]
    )
    for batch in summary["top_identity_resolution_pair_review_batches"]:
        lines.append(
            "| "
            f"{batch['identity_resolution_queue']} | "
            f"{batch['exchange_pair']} | "
            f"{batch['rows']} | "
            f"{batch['review_strategy']} | "
            f"{batch['evidence_required']} | "
            f"{batch['recommended_next_source']} | "
            f"{batch['source_gate']} |"
        )
    lines.extend(["", "## Identity Resolution Review Strategies", "", "| Queue | Strategy | Rows |", "| --- | --- | ---: |"])
    for queue, strategy_totals in summary["identity_resolution_review_strategy_totals"].items():
        for strategy, count in strategy_totals.items():
            lines.append(f"| {queue} | {strategy} | {count} |")
    lines.extend(["", "## Identity Resolution Evidence Required", "", "| Queue | Evidence Required | Rows |", "| --- | --- | ---: |"])
    for queue, evidence_totals in summary["identity_resolution_evidence_required_totals"].items():
        for evidence_required, count in evidence_totals.items():
            lines.append(f"| {queue} | {evidence_required} | {count} |")
    lines.extend(["", "## Identity Resolution Identity Evidence", "", "| Queue | Identity Evidence | Rows |", "| --- | --- | ---: |"])
    for queue, evidence_totals in summary["identity_resolution_identity_evidence_totals"].items():
        for evidence, count in evidence_totals.items():
            lines.append(f"| {queue} | {evidence} | {count} |")
    lines.extend(["", "## Identity Resolution Risk Flags", "", "| Queue | Risk Flag | Rows |", "| --- | --- | ---: |"])
    for queue, risk_flag_totals in summary["identity_resolution_risk_flag_totals"].items():
        for risk_flag, count in risk_flag_totals.items():
            lines.append(f"| {queue} | {risk_flag} | {count} |")
    lines.extend(
        [
            "",
            "## Decisions",
            "",
            "| Decision | Rows |",
            "| --- | ---: |",
        ]
    )
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Review Buckets", "", "| Priority | Bucket | Rows |", "| --- | --- | ---: |"])
    bucket_priorities = {
        "same_isin_exact_name_cross_listing_candidate": "P1",
        "same_isin_cross_listing_needs_name_or_scope_review": "P2",
        "distinct_official_isin_new_listing_candidate": "P2",
        "resolve_asset_type_conflict_before_identity_review": "P2",
        "hold_symbol_only_collision_needs_non_symbol_identity": "P3",
        "no_collision_action": "P4",
    }
    for bucket, count in summary["review_bucket_totals"].items():
        lines.append(f"| {bucket_priorities.get(bucket, '')} | {bucket} | {count} |")
    lines.extend(["", "## Clearance Evidence", "", "| Evidence Gate | Rows |", "| --- | ---: |"])
    for gate, count in summary["clearance_evidence_totals"].items():
        lines.append(f"| {gate} | {count} |")
    lines.extend(["", "## Risk Flags", "", "| Flag | Rows |", "| --- | ---: |"])
    for flag, count in summary["collision_risk_flag_totals"].items():
        lines.append(f"| {flag} | {count} |")
    lines.extend(["", "## Top Exchanges", "", "| Exchange | Rows |", "| --- | ---: |"])
    for exchange, count in sorted(summary["exchange_totals"].items(), key=lambda item: (-item[1], item[0]))[:25]:
        lines.append(f"| {exchange} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No ticker-only evidence may create, rename, merge, or enrich a listing.",
            "- Official target-exchange evidence is required before any listing addition.",
            "- Ambiguous or conflicting rows stay absent and remain review gaps.",
            "",
        ]
    )
    return "\n".join(lines)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(summary), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = build_review_rows(
        tickers=load_csv(args.listings_csv),
        masterfile_rows=load_csv(args.masterfile_reference_csv),
    )
    summary = summarize(rows, utc_now_iso())
    write_csv(args.csv_out, rows)
    write_json(
        args.json_out,
        summary,
        rows,
        source_files={
            "listings_csv": args.listings_csv,
            "masterfile_reference_csv": args.masterfile_reference_csv,
        },
    )
    write_markdown(args.md_out, summary)
    print(
        json.dumps(
            {
                "rows": summary["rows"],
                "decision_totals": summary["decision_totals"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
