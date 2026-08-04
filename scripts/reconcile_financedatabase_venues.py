"""Reconcile FinanceDatabase listings against the active official reference.

This command is deliberately read-only.  It freezes a local FinanceDatabase
snapshot, identifies missing listing candidates, and applies the repository's
non-equity and evidence gates venue by venue.  It never changes canonical
exports or supplemental listings.
"""

from __future__ import annotations

import argparse
import bz2
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_financedatabase_metadata import (
    FINANCEDATABASE_EXCHANGE_CODES,
    finance_base_ticker,
    normalized_ticker_key,
)
from scripts.lib.non_equity_guard import classify_non_equity_leakage
from scripts.lib.normalize import names_match, significant_name_tokens
from scripts.rebuild_dataset import (
    is_code_like_name,
    normalize_input_row,
    should_exclude_row,
)

DEFAULT_OUTPUT_DIR = ROOT / "data" / "reports"
DEFAULT_REFERENCE = ROOT / "data" / "masterfiles" / "reference.csv"
DEFAULT_LISTINGS = ROOT / "data" / "listings.csv"
DEFAULT_SUPPLEMENTAL = ROOT / "data" / "masterfiles" / "supplemental_listings.csv"

PRODUCT_RE = re.compile(
    r"(?:exchange[- ]traded|\betp\b|\betn\b|\betf\b|"
    r"leveraged exposure|inverse exposure|tracker certificate|\bcertificate\b|"
    r"\bcert\b|\bwarrant|\brights?\b|\bunits?\b|"
    r"\b(?:bond|bonds|notes?|debentures?)\b|\b(?:FRN|SNR)\b|"
    r"\bcapital securities?\b|\bparticipated cert)",
    re.IGNORECASE,
)
NAME_RE = re.compile(
    r"(?:^LS\s|\bwisdomtree\b|\bleverage shares?\b|\binvesco physical\b|"
    r"\b(?:bond|bonds|notes?|debentures?|warrant|rights?|units?|certificate|cert)\b|"
    r"\b(?:FRN|SNR)\b|\b(?:due|maturing)\b|\b(?:\d+(?:\.\d+)?%))",
    re.IGNORECASE,
)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_revision(path: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return result.stdout.strip()


def load_financedatabase_equities(snapshot_dir: Path) -> tuple[list[dict[str, str]], Path]:
    compressed = snapshot_dir / "compression" / "equities.bz2"
    if not compressed.exists():
        raise FileNotFoundError(f"FinanceDatabase snapshot is missing {compressed}")
    with bz2.open(compressed, "rt", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle)), compressed


def _index_by_key(rows: Iterable[dict[str, str]], *, normalized: bool = False) -> dict[tuple[str, str], list[dict[str, str]]]:
    indexed: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        ticker = row.get("ticker", "")
        if normalized:
            ticker = normalized_ticker_key(ticker)
        indexed[(ticker, row.get("exchange", ""))].append(row)
    return indexed


def _index_isin(rows: Iterable[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        isin = row.get("isin", "").strip().upper()
        if isin:
            indexed[isin].append(row)
    return indexed


def _index_name_tokens(rows: Iterable[dict[str, str]]) -> dict[str, set[int]]:
    indexed: dict[str, set[int]] = defaultdict(set)
    rows = list(rows)
    for index, row in enumerate(rows):
        for token in significant_name_tokens(row.get("name", "")):
            indexed[token].add(index)
    return indexed


def _mapped_exchanges() -> dict[str, set[str]]:
    mapped: dict[str, set[str]] = defaultdict(set)
    for exchange, codes in FINANCEDATABASE_EXCHANGE_CODES.items():
        for code in codes:
            mapped[code].add(exchange)
    return mapped


def build_missing_candidates(
    financedatabase_rows: list[dict[str, str]],
    listings: list[dict[str, str]],
    official_stock_reference: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Return official-reference-correlated rows absent from local Stock listings."""

    local_stock = [row for row in listings if row.get("asset_type") == "Stock"]
    local_by_key = _index_by_key(local_stock, normalized=True)
    local_by_isin = _index_isin(local_stock)
    local_name_tokens = _index_name_tokens(local_stock)
    ref_by_key = _index_by_key(official_stock_reference, normalized=True)
    ref_by_isin = _index_isin(official_stock_reference)
    ref_name_tokens = _index_name_tokens(official_stock_reference)
    code_to_exchange = _mapped_exchanges()
    candidates: list[dict[str, str]] = []

    for source in financedatabase_rows:
        if source.get("delisted", "").strip().lower() == "true":
            continue
        fd_exchange = source.get("exchange", "").strip()
        if fd_exchange not in code_to_exchange:
            continue
        base_ticker = finance_base_ticker(source.get("symbol", ""))
        ticker_key = normalized_ticker_key(base_ticker)
        target_exchanges = code_to_exchange[fd_exchange]
        if any(local_by_key.get((ticker_key, exchange), []) for exchange in target_exchanges):
            continue

        fd_isin = source.get("isin", "").strip().upper()
        if fd_isin and local_by_isin.get(fd_isin):
            continue
        mapped_exchange = sorted(target_exchanges)[0]
        normalized = normalize_input_row(
            {
                "ticker": base_ticker,
                "exchange": mapped_exchange,
                "asset_type": "Stock",
                "name": source.get("name", ""),
                "isin": fd_isin,
                "sector": source.get("sector", ""),
                "aliases": "",
            }
        )
        if normalized.get("asset_type") != "Stock" or should_exclude_row(normalized):
            continue

        name = source.get("name", "").strip()
        summary = source.get("summary", "").strip()
        sector = source.get("sector", "").strip()
        if (
            not name
            or is_code_like_name(name, base_ticker)
            or not sector
            or not summary
            or PRODUCT_RE.search(summary)
            or NAME_RE.search(name)
        ):
            continue

        tokens = significant_name_tokens(name)
        local_pool: set[int] = set()
        for token in tokens:
            local_pool.update(local_name_tokens.get(token, set()))
        if any(names_match(name, local_stock[index].get("name", "")) for index in local_pool):
            continue

        key_matches = ref_by_key.get((ticker_key, mapped_exchange), [])
        isin_matches = [
            row
            for row in ref_by_isin.get(fd_isin, [])
            if row.get("exchange") == mapped_exchange
        ] if fd_isin else []
        key_name_matches = [
            row
            for row in key_matches
            if names_match(name, row.get("name", ""))
            or (fd_isin and row.get("isin", "").strip().upper() == fd_isin)
        ]
        reference_name_pool: set[int] = set()
        for token in tokens:
            reference_name_pool.update(ref_name_tokens.get(token, set()))
        official_name_matches = [
            official_stock_reference[index]
            for index in reference_name_pool
            if official_stock_reference[index].get("exchange") == mapped_exchange
            and names_match(name, official_stock_reference[index].get("name", ""))
        ]

        if isin_matches:
            status = "same_isin"
            matches = isin_matches
        elif key_name_matches:
            status = "same_ticker_name"
            matches = key_name_matches
        elif official_name_matches:
            status = "same_name"
            matches = official_name_matches
        else:
            continue

        official = matches[0]
        candidates.append(
            {
                "status": status,
                "mapped_exchange": mapped_exchange,
                "fd_exchange": fd_exchange,
                "fd_symbol": source.get("symbol", "").strip(),
                "fd_name": name,
                "fd_isin": fd_isin,
                "fd_sector": sector,
                "official_ticker": official.get("ticker", "").strip(),
                "official_name": official.get("name", "").strip(),
                "official_isin": official.get("isin", "").strip().upper(),
                "official_source_key": official.get("source_key", "").strip(),
                "evidence": (
                    "official active reference: exact ISIN"
                    if status == "same_isin"
                    else "official active reference: ticker + name"
                    if status == "same_ticker_name"
                    else "official active reference: name"
                ),
            }
        )

    return sorted(
        candidates,
        key=lambda row: (
            {"same_isin": 0, "same_ticker_name": 1, "same_name": 2}[row["status"]],
            row["mapped_exchange"],
            row["official_ticker"],
            row["fd_symbol"],
        ),
    )


def classify_candidates(
    candidates: list[dict[str, str]],
    references: list[dict[str, str]],
    listings: list[dict[str, str]],
    supplemental: list[dict[str, str]],
) -> list[dict[str, str]]:
    active_references = [
        row
        for row in references
        if row.get("official", "").strip().lower() == "true"
        and row.get("listing_status", "").strip().lower() == "active"
    ]
    reference_by_key = _index_by_key(active_references)
    reference_by_source_key: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    reference_by_isin: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in active_references:
        reference_by_source_key[
            (row.get("source_key", ""), row.get("exchange", ""), row.get("ticker", ""))
        ].append(row)
        if row.get("isin", "").strip():
            reference_by_isin[(row.get("exchange", ""), row.get("isin", "").strip().upper())].append(row)

    local_by_key = _index_by_key(listings)
    local_by_isin = _index_isin(listings)
    supplemental_by_key = _index_by_key(supplemental)
    supplemental_by_isin = _index_isin(supplemental)
    review_rows: list[dict[str, str]] = []

    for candidate in candidates:
        exchange = candidate["mapped_exchange"]
        ticker = candidate["official_ticker"]
        isin = candidate["official_isin"]
        key = (ticker, exchange)
        key_references = sorted(
            reference_by_key.get(key, []),
            key=lambda row: (
                row.get("source_key", ""),
                row.get("asset_type", ""),
                row.get("name", ""),
                row.get("isin", ""),
            ),
        )
        source_references = sorted(
            reference_by_source_key.get(
                (candidate["official_source_key"], exchange, ticker), []
            ),
            key=lambda row: (row.get("asset_type", ""), row.get("name", "")),
        )
        selected_references = source_references or key_references
        selected_cfi = sorted({row.get("cfi", "").strip().upper() for row in selected_references if row.get("cfi", "").strip()})
        selected_sector = sorted({row.get("sector", "").strip() for row in selected_references if row.get("sector", "").strip()})
        asset_types = sorted({row.get("asset_type", "").strip() for row in key_references if row.get("asset_type", "").strip()})
        local_key_rows = local_by_key.get(key, [])
        local_types = sorted({row.get("asset_type", "").strip() for row in local_key_rows if row.get("asset_type", "").strip()})
        if not local_key_rows:
            local_status = "absent"
        elif "Stock" in local_types:
            local_status = "present_same_asset_type"
        else:
            local_status = "present_other_asset_type"
        local_isin_rows = local_by_isin.get(isin, []) if isin else []
        local_isin_venues = sorted(
            f"{row.get('exchange', '')}:{row.get('ticker', '')}:{row.get('asset_type', '')}"
            for row in local_isin_rows
        )
        supplemental_exact = bool(supplemental_by_key.get(key) or (isin and supplemental_by_isin.get(isin)))
        official_cfi = selected_cfi[0] if selected_cfi else ""
        guard = classify_non_equity_leakage(
            {
                "asset_type": "Stock",
                "ticker": ticker,
                "name": candidate["official_name"],
                "cfi": official_cfi,
            }
        )
        if local_status == "present_other_asset_type":
            decision = "existing_local_other_asset_type"
            evidence = "Exact local listing key exists under another asset type."
        elif guard["guard_decision"] == "blocked_non_common_stock":
            decision = "blocked_non_common_stock"
            evidence = "Non-equity guard blocked the exact listing."
        elif guard["guard_decision"] == "manual_review_ambiguous_stock_classification":
            decision = "manual_non_equity_name_signal"
            evidence = "The official name carries a fund/trust signal requiring manual classification."
        elif len(asset_types) > 1:
            decision = "manual_reference_asset_type_conflict"
            evidence = f"Active official references disagree on asset_type: {'|'.join(asset_types)}."
        else:
            decision = "review_security_type_required"
            evidence = "No official CFI/security type is available; ticker/name/ISIN alone is insufficient."

        review_rows.append(
            {
                **candidate,
                "official_sector": selected_sector[0] if selected_sector else "",
                "official_cfi": official_cfi,
                "reference_asset_types": "|".join(asset_types),
                "reference_asset_type_conflict": "true" if len(asset_types) > 1 else "false",
                "official_reference_isin_match": "true" if bool(isin and reference_by_isin.get((exchange, isin))) else "false",
                "local_exact_key_status": local_status,
                "local_same_isin_any_venue": "true" if local_isin_rows else "false",
                "local_same_isin_venues": "|".join(local_isin_venues),
                "supplemental_exact_key": "true" if supplemental_exact else "false",
                "identity_confidence": "high_exact_isin" if candidate["status"] == "same_isin" else "medium_ticker_name",
                "dry_run_decision": decision,
                "dry_run_evidence": evidence,
                "next_evidence_required": (
                    "Official CFI/security type from the venue, issuer registry, OpenFIGI, or equivalent source."
                    if decision == "review_security_type_required"
                    else "Venue/issuer security-type evidence and a source-of-truth decision."
                    if decision == "manual_reference_asset_type_conflict"
                    else "Official product/security type for the exact listing."
                ),
            }
        )
    return review_rows


def build_venue_summary(review_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in review_rows:
        grouped[row["mapped_exchange"]].append(row)
    summary: list[dict[str, str]] = []
    for venue in sorted(grouped):
        rows = grouped[venue]
        decisions = Counter(row["dry_run_decision"] for row in rows)
        groups = Counter(row["status"] for row in rows)
        summary.append(
            {
                "mapped_exchange": venue,
                "rows": str(len(rows)),
                "same_ticker_name_rows": str(groups["same_ticker_name"]),
                "same_isin_rows": str(groups["same_isin"]),
                "review_security_type_required": str(decisions["review_security_type_required"]),
                "manual_reference_asset_type_conflict": str(decisions["manual_reference_asset_type_conflict"]),
                "manual_non_equity_name_signal": str(decisions["manual_non_equity_name_signal"]),
                "existing_local_other_asset_type": str(decisions["existing_local_other_asset_type"]),
                "blocked_non_common_stock": str(decisions["blocked_non_common_stock"]),
                "reference_asset_type_conflict_rows": str(sum(row["reference_asset_type_conflict"] == "true" for row in rows)),
                "local_same_isin_any_venue_rows": str(sum(row["local_same_isin_any_venue"] == "true" for row in rows)),
            }
        )
    return summary


def reconcile(
    snapshot_dir: Path,
    reference_path: Path = DEFAULT_REFERENCE,
    listings_path: Path = DEFAULT_LISTINGS,
    supplemental_path: Path = DEFAULT_SUPPLEMENTAL,
) -> dict[str, object]:
    financedatabase_rows, compressed_path = load_financedatabase_equities(snapshot_dir)
    references = read_csv(reference_path)
    listings = read_csv(listings_path)
    supplemental = read_csv(supplemental_path) if supplemental_path.exists() else []
    official_stock_reference = [
        row
        for row in references
        if row.get("asset_type") == "Stock"
        and row.get("official", "").strip().lower() == "true"
        and row.get("listing_status", "").strip().lower() == "active"
    ]
    candidates = build_missing_candidates(financedatabase_rows, listings, official_stock_reference)
    selected_candidates = [
        row for row in candidates if row["status"] in {"same_isin", "same_ticker_name"}
    ]
    review_rows = classify_candidates(selected_candidates, references, listings, supplemental)
    venue_summary = build_venue_summary(review_rows)
    commit = git_revision(snapshot_dir)
    return {
        "candidates": candidates,
        "review_rows": review_rows,
        "venue_summary": venue_summary,
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "apply_performed": False,
            "finance_database_commit": commit,
            "finance_database_equities_sha256": sha256_file(compressed_path),
            "finance_database_equities_rows": len(financedatabase_rows),
            "reference_sha256": sha256_file(reference_path),
            "reference_rows": len(references),
            "official_active_stock_reference_rows": len(official_stock_reference),
            "local_listings_rows": len(listings),
            "supplemental_rows": len(supplemental),
        },
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_reports(result: dict[str, object], output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = result["candidates"]
    review_rows = result["review_rows"]
    venue_summary = result["venue_summary"]
    metadata = result["metadata"]
    assert isinstance(candidates, list)
    assert isinstance(review_rows, list)
    assert isinstance(venue_summary, list)
    assert isinstance(metadata, dict)
    paths = {
        "candidates_csv": output_dir / "finance_database_venue_candidates.csv",
        "review_csv": output_dir / "finance_database_venue_review.csv",
        "venue_summary_csv": output_dir / "finance_database_venue_summary.csv",
        "summary_json": output_dir / "finance_database_venue_reconciliation.json",
        "summary_md": output_dir / "finance_database_venue_reconciliation.md",
    }
    _write_csv(paths["candidates_csv"], candidates)
    _write_csv(paths["review_csv"], review_rows)
    _write_csv(paths["venue_summary_csv"], venue_summary)
    decisions = Counter(row["dry_run_decision"] for row in review_rows)
    groups = Counter(row["status"] for row in candidates)
    summary = {
        **metadata,
        "candidate_rows": len(candidates),
        "candidate_status_counts": dict(sorted(groups.items())),
        "selected_review_rows": len(review_rows),
        "selected_status_counts": dict(sorted(Counter(row["status"] for row in review_rows).items())),
        "decision_counts": dict(sorted(decisions.items())),
        "official_cfi_present_rows": sum(bool(row.get("official_cfi")) for row in review_rows),
        "supplemental_exact_key_rows": sum(row["supplemental_exact_key"] == "true" for row in review_rows),
        "venue_summary": venue_summary,
        "artifacts": {key: path.name for key, path in paths.items()},
    }
    paths["summary_json"].write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    table = [
        "| Venue | Ticker/name | ISIN-exact | Total | Security-type review | Asset-type conflict | Name signal | Existing other type | Same ISIN elsewhere |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in venue_summary:
        table.append(
            f"| {row['mapped_exchange']} | {row['same_ticker_name_rows']} | {row['same_isin_rows']} | {row['rows']} | {row['review_security_type_required']} | "
            f"{row['manual_reference_asset_type_conflict']} | {row['manual_non_equity_name_signal']} | "
            f"{row['existing_local_other_asset_type']} | {row['local_same_isin_any_venue_rows']} |"
        )
    markdown = "\n".join(
        [
            "# FinanceDatabase venue reconciliation",
            "",
            "This is a read-only, review-gated reconciliation. It does not modify canonical exports, supplemental listings, or reference data.",
            "",
            f"- FinanceDatabase commit: `{metadata.get('finance_database_commit') or '<unknown>'}`",
            f"- FinanceDatabase equities rows: `{metadata['finance_database_equities_rows']}`",
            f"- Candidate rows: `{len(candidates)}` ({', '.join(f'{key}={value}' for key, value in sorted(groups.items()))})",
            f"- Selected venue-review rows: `{len(review_rows)}`",
            f"- Official CFI present: `{summary['official_cfi_present_rows']}`",
            f"- Apply performed: `{metadata['apply_performed']}`",
            "",
            "## Venue summary",
            "",
            *table,
            "",
            "No row is authorized for import from ticker/name/ISIN alone. Each security-type review requires exact venue, issuer, CFI, OpenFIGI, or equivalent official evidence.",
            "",
        ]
    )
    paths["summary_md"].write_text(markdown, encoding="utf-8")
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path, required=True, help="Frozen FinanceDatabase checkout")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--listings", type=Path, default=DEFAULT_LISTINGS)
    parser.add_argument("--supplemental", type=Path, default=DEFAULT_SUPPLEMENTAL)
    args = parser.parse_args()
    result = reconcile(args.snapshot_dir, args.reference, args.listings, args.supplemental)
    paths = write_reports(result, args.output_dir)
    print(
        json.dumps(
            {
                "candidate_rows": len(result["candidates"]),
                "review_rows": len(result["review_rows"]),
                "decision_counts": dict(Counter(row["dry_run_decision"] for row in result["review_rows"])),
                "summary": str(paths["summary_json"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
