"""Validate Twelve Data review candidates against second-source providers.

This script is evidence-only. It does not apply name, identifier, alias, scope,
or listing changes. API keys are read from environment variables and are never
written to outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import requests


DEFAULT_INPUT_CSV = Path("data/reports/twelvedata_all_batches_second_source_queue.csv")
DEFAULT_OUTPUT_CSV = Path("data/reports/twelvedata_all_batches_second_source_validation.csv")
DEFAULT_SUMMARY_JSON = Path("data/reports/twelvedata_all_batches_second_source_validation_summary.json")
DEFAULT_SUMMARY_MD = Path("data/reports/twelvedata_all_batches_second_source_validation.md")

REQUEST_TIMEOUT_SECONDS = 20
USER_AGENT = "free-ticker-database/2.0 second-source-validation"

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"
FMP_PROFILE_URL = "https://financialmodelingprep.com/stable/profile"

OPENFIGI_EXCH_CODE_BY_EXCHANGE = {
    "BATS": "US",
    "NASDAQ": "US",
    "NYSE": "US",
    "NYSE ARCA": "US",
    "NYSE MKT": "US",
    "NEO": "CN",
    "OTC": "US",
    "TSX": "CN",
    "TSXV": "CN",
}

OUTPUT_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "local_name",
    "twelvedata_name",
    "twelvedata_type",
    "name_score",
    "deepseek_decision_candidate",
    "deepseek_safe_action",
    "openfigi_status",
    "openfigi_name",
    "openfigi_figi",
    "openfigi_match",
    "alphavantage_status",
    "alphavantage_name",
    "alphavantage_exchange",
    "alphavantage_match",
    "fmp_status",
    "fmp_name",
    "fmp_exchange",
    "fmp_match",
    "validation_status",
    "evidence_summary",
    "recommended_next_action",
    "review_batch",
]


@dataclass
class ProviderEvidence:
    status: str
    name: str = ""
    exchange: str = ""
    figi: str = ""
    error: str = ""


def read_csv(path: Path, *, limit: int | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return rows[:limit] if limit is not None else rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def compact_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:180]}"


def normalize_name(value: str) -> str:
    text = (value or "").lower().replace("&", " and ")
    keep = [ch if ch.isalnum() else " " for ch in text]
    words = "".join(keep).split()
    stopwords = {"ag", "co", "corp", "corporation", "inc", "incorporated", "ltd", "limited", "plc", "sa", "se", "the"}
    return " ".join(word for word in words if word not in stopwords)


def name_ratio(left: str, right: str) -> float:
    left_norm = normalize_name(left)
    right_norm = normalize_name(right)
    if not left_norm or not right_norm:
        return 0.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def classify_name(provider_name: str, local_name: str, twelvedata_name: str) -> str:
    if not provider_name:
        return "no_name"
    local_score = name_ratio(provider_name, local_name)
    twelve_score = name_ratio(provider_name, twelvedata_name)
    if twelve_score >= 0.82 and twelve_score >= local_score + 0.08:
        return "supports_twelvedata"
    if local_score >= 0.82 and local_score >= twelve_score + 0.08:
        return "supports_local"
    if max(local_score, twelve_score) >= 0.72:
        return "ambiguous_name_similarity"
    return "different_name"


def get_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
) -> Any:
    response = session.get(
        url,
        params=params,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def fetch_openfigi(row: dict[str, str], session: requests.Session, api_key: str) -> ProviderEvidence:
    exch_code = OPENFIGI_EXCH_CODE_BY_EXCHANGE.get(row["exchange"], "")
    if not exch_code:
        return ProviderEvidence(status="skipped_unsupported_exchange")
    headers = {
        "X-OPENFIGI-APIKEY": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    jobs = [{"idType": "TICKER", "idValue": row["ticker"], "exchCode": exch_code}]
    try:
        response = session.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - exercised through tests with fakes
        return ProviderEvidence(status="provider_error", error=compact_error(exc))
    if not isinstance(payload, list) or not payload:
        return ProviderEvidence(status="no_payload")
    data = payload[0].get("data", []) if isinstance(payload[0], dict) else []
    if not data:
        return ProviderEvidence(status="no_match")
    target_ticker = row["ticker"].upper()
    exact = [item for item in data if str(item.get("ticker", "")).upper() == target_ticker]
    item = exact[0] if exact else data[0]
    return ProviderEvidence(
        status="ok",
        name=str(item.get("name", "")),
        exchange=str(item.get("exchCode", "")),
        figi=str(item.get("figi", "")),
    )


def fetch_alphavantage(row: dict[str, str], session: requests.Session, api_key: str) -> ProviderEvidence:
    try:
        payload = get_json(
            session,
            ALPHAVANTAGE_URL,
            params={"function": "OVERVIEW", "symbol": row["ticker"], "apikey": api_key},
        )
    except Exception as exc:  # pragma: no cover
        return ProviderEvidence(status="provider_error", error=compact_error(exc))
    if not isinstance(payload, dict):
        return ProviderEvidence(status="no_payload")
    if payload.get("Note") or payload.get("Information"):
        return ProviderEvidence(status="rate_limited_or_unavailable")
    name = str(payload.get("Name", ""))
    if not name:
        return ProviderEvidence(status="no_match")
    return ProviderEvidence(status="ok", name=name, exchange=str(payload.get("Exchange", "")))


def fetch_fmp(row: dict[str, str], session: requests.Session, api_key: str) -> ProviderEvidence:
    try:
        response = session.get(
            FMP_PROFILE_URL,
            params={"symbol": row["ticker"], "apikey": api_key},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 429:
            return ProviderEvidence(status="rate_limited_or_unavailable")
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover
        return ProviderEvidence(status="provider_error", error=compact_error(exc))
    if not isinstance(payload, list) or not payload:
        return ProviderEvidence(status="no_match")
    item = payload[0]
    if not isinstance(item, dict):
        return ProviderEvidence(status="no_payload")
    name = str(item.get("companyName", ""))
    if not name:
        return ProviderEvidence(status="no_match")
    return ProviderEvidence(status="ok", name=name, exchange=str(item.get("exchangeShortName", "") or item.get("exchange", "")))


def dry_evidence(row: dict[str, str], provider: str) -> ProviderEvidence:
    # Deterministic schema validation only; it intentionally provides no support.
    return ProviderEvidence(status=f"dry_run_{provider}", name="")


def choose_validation_status(matches: list[str], provider_statuses: list[str]) -> tuple[str, str]:
    supports_twelve = matches.count("supports_twelvedata")
    supports_local = matches.count("supports_local")
    ambiguous = matches.count("ambiguous_name_similarity")
    different = matches.count("different_name")
    ok_or_no_match = [status for status in provider_statuses if status in {"ok", "no_match"}]
    if supports_twelve and not supports_local:
        return "second_source_supports_twelvedata_name", "At least one provider name supports Twelve Data over local name."
    if supports_local and not supports_twelve:
        return "second_source_supports_local_name", "At least one provider name supports the local name over Twelve Data."
    if supports_local and supports_twelve:
        return "conflicting_second_source_evidence", "Providers conflict between local and Twelve Data names."
    if ambiguous:
        return "ambiguous_second_source_evidence", "Provider evidence is name-similar but not decisive."
    if different:
        return "provider_found_different_name", "Provider found a name that supports neither local nor Twelve Data clearly."
    if ok_or_no_match:
        return "no_second_source_name_match", "Providers returned no usable matching issuer name."
    return "provider_validation_not_available", "Provider validation was skipped, rate-limited, or errored."


def recommended_next_action(validation_status: str, deepseek_safe_action: str) -> str:
    if validation_status == "second_source_supports_twelvedata_name":
        return "build_manual_apply_candidate_for_name_update_after_official_or_identifier_gate"
    if validation_status == "second_source_supports_local_name":
        return "keep_local_name_and_record_twelvedata_as_non_authoritative_mismatch"
    if validation_status == "conflicting_second_source_evidence":
        return "manual_identity_review_required_before_any_apply"
    if validation_status in {"ambiguous_second_source_evidence", "provider_found_different_name"}:
        return "send_to_manual_or_deepseek_followup_with_additional_evidence"
    if deepseek_safe_action == "candidate_for_official_followup":
        return "official_followup_required"
    return "needs_more_second_source_evidence"


def validate_row(
    row: dict[str, str],
    *,
    session: requests.Session,
    keys: dict[str, str],
    dry_run: bool = False,
    sleep_seconds: float = 0.0,
) -> dict[str, str]:
    if dry_run:
        openfigi = dry_evidence(row, "openfigi")
        alpha = dry_evidence(row, "alphavantage")
        fmp = dry_evidence(row, "fmp")
    else:
        openfigi = (
            fetch_openfigi(row, session, keys["OPENFIGI_API_KEY"])
            if keys.get("OPENFIGI_API_KEY")
            else ProviderEvidence(status="skipped_missing_env")
        )
        alpha = (
            fetch_alphavantage(row, session, keys["ALPHAVANTAGE_API_KEY"])
            if keys.get("ALPHAVANTAGE_API_KEY")
            else ProviderEvidence(status="skipped_missing_env")
        )
        fmp = (
            fetch_fmp(row, session, keys["FMP_API_KEY"])
            if keys.get("FMP_API_KEY")
            else ProviderEvidence(status="skipped_missing_env")
        )
        if sleep_seconds:
            time.sleep(sleep_seconds)

    openfigi_match = classify_name(openfigi.name, row["local_name"], row["twelvedata_name"])
    alpha_match = classify_name(alpha.name, row["local_name"], row["twelvedata_name"])
    fmp_match = classify_name(fmp.name, row["local_name"], row["twelvedata_name"])
    status, evidence_summary = choose_validation_status(
        [openfigi_match, alpha_match, fmp_match],
        [openfigi.status, alpha.status, fmp.status],
    )
    return {
        "listing_key": row["listing_key"],
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "local_name": row["local_name"],
        "twelvedata_name": row["twelvedata_name"],
        "twelvedata_type": row["twelvedata_type"],
        "name_score": row["name_score"],
        "deepseek_decision_candidate": row["deepseek_decision_candidate"],
        "deepseek_safe_action": row["deepseek_safe_action"],
        "openfigi_status": openfigi.status,
        "openfigi_name": openfigi.name[:240],
        "openfigi_figi": openfigi.figi,
        "openfigi_match": openfigi_match,
        "alphavantage_status": alpha.status,
        "alphavantage_name": alpha.name[:240],
        "alphavantage_exchange": alpha.exchange[:80],
        "alphavantage_match": alpha_match,
        "fmp_status": fmp.status,
        "fmp_name": fmp.name[:240],
        "fmp_exchange": fmp.exchange[:80],
        "fmp_match": fmp_match,
        "validation_status": status,
        "evidence_summary": evidence_summary,
        "recommended_next_action": recommended_next_action(status, row["deepseek_safe_action"]),
        "review_batch": row.get("review_batch", ""),
    }


def summarize(rows: list[dict[str, str]], *, dry_run: bool, limit: int | None) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "limit": limit,
        "dry_run": dry_run,
        "validation_status_counts": Counter(row["validation_status"] for row in rows).most_common(),
        "recommended_next_action_counts": Counter(row["recommended_next_action"] for row in rows).most_common(),
        "review_batch_counts": Counter(row["review_batch"] for row in rows).most_common(),
        "openfigi_status_counts": Counter(row["openfigi_status"] for row in rows).most_common(),
        "alphavantage_status_counts": Counter(row["alphavantage_status"] for row in rows).most_common(),
        "fmp_status_counts": Counter(row["fmp_status"] for row in rows).most_common(),
        "provider_match_counts": {
            "openfigi": Counter(row["openfigi_match"] for row in rows).most_common(),
            "alphavantage": Counter(row["alphavantage_match"] for row in rows).most_common(),
            "fmp": Counter(row["fmp_match"] for row in rows).most_common(),
        },
        "env_status": {
            key: "set" if os.getenv(key) else "missing"
            for key in ["OPENFIGI_API_KEY", "ALPHAVANTAGE_API_KEY", "FMP_API_KEY"]
        },
        "policy": (
            "Provider evidence is advisory validation only. It does not authorize applying Twelve Data-driven "
            "changes without a separate apply queue and dataset gates."
        ),
    }


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Twelve Data Batch A Second-Source Validation",
        "",
        str(summary["policy"]),
        "",
        f"- Rows validated: {summary['rows']:,}",
        f"- Dry run: {summary['dry_run']}",
        "",
        "## Validation Status",
        "",
        "| Status | Rows |",
        "| --- | ---: |",
    ]
    lines.extend(f"| {status} | {count:,} |" for status, count in summary["validation_status_counts"])
    lines.extend(["", "## Review Batches", "", "| Batch | Rows |", "| --- | ---: |"])
    lines.extend(f"| {batch} | {count:,} |" for batch, count in summary["review_batch_counts"])
    lines.extend(["", "## Recommended Next Actions", "", "| Action | Rows |", "| --- | ---: |"])
    lines.extend(f"| {action} | {count:,} |" for action, count in summary["recommended_next_action_counts"])
    for provider in ["openfigi", "alphavantage", "fmp"]:
        lines.extend(["", f"## {provider.title()} Status", "", "| Status | Rows |", "| --- | ---: |"])
        lines.extend(f"| {status} | {count:,} |" for status, count in summary[f"{provider}_status_counts"])
    lines.extend(
        [
            "",
            "## Environment",
            "",
            "API keys are read from environment variables only. No key values are stored in this report.",
        ]
    )
    for key, status in summary["env_status"].items():
        lines.append(f"- `{key}`: {status}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--summary-json", type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument("--summary-md", type=Path, default=DEFAULT_SUMMARY_MD)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    keys = {
        "OPENFIGI_API_KEY": os.getenv("OPENFIGI_API_KEY", ""),
        "ALPHAVANTAGE_API_KEY": os.getenv("ALPHAVANTAGE_API_KEY", ""),
        "FMP_API_KEY": os.getenv("FMP_API_KEY", ""),
    }
    session = requests.Session()
    input_rows = read_csv(args.input_csv, limit=args.limit)
    output_rows = [
        validate_row(row, session=session, keys=keys, dry_run=args.dry_run, sleep_seconds=args.sleep_seconds)
        for row in input_rows
    ]
    write_csv(args.output_csv, output_rows)
    summary = summarize(output_rows, dry_run=args.dry_run, limit=args.limit)
    args.summary_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_markdown(args.summary_md, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
