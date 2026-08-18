"""Canonical, tamper-evident evidence helpers for automated delisting actions."""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime, timedelta, timezone
from typing import Mapping

BSE_SOURCE_KEY = "bse_india_scrips"
BSE_STATUS_URL_TEMPLATE = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status={status}"
)
NASDAQ_SOURCE_KEY = "nasdaq_trading_system_adds_deletes"
NASDAQ_ADDS_DELETES_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/TradingSystemAddsDeletes.txt"
)
NASDAQ_DELETE_EXCHANGES = {
    "NASDAQ",
    "NYSE",
    "NYSE ARCA",
    "NYSE MKT",
    "NYSE CHICAGO",
    "AMEX",
    "BATS",
    "IEX",
}
NASDAQ_PRIMARY_MARKET_MAP = {
    "A": "NYSE MKT",
    "G": "NASDAQ",
    "M": "NYSE CHICAGO",
    "N": "NYSE",
    "P": "NYSE ARCA",
    "Q": "NASDAQ",
    "S": "NASDAQ",
    "V": "IEX",
    "Z": "BATS",
}


def evidence_observation_id(candidate: Mapping[str, str], observed_at: str) -> str:
    """Bind the evidence ID to the exact source assertion and listing identity."""

    payload = "|".join(
        [
            str(candidate.get("source_key", "")),
            str(candidate.get("source_url", "")),
            str(candidate.get("exchange", "")),
            str(candidate.get("ticker", "")),
            str(candidate.get("isin", "")),
            str(candidate.get("classification", "")),
            observed_at,
        ]
    )
    return "obs_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _observed_at_is_current(observed_at: str, *, now: datetime | None) -> bool:
    try:
        observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None or observed.utcoffset() is None:
        return False
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return observed.astimezone(timezone.utc) <= current_time.astimezone(timezone.utc) + timedelta(
        minutes=5
    )


def _observation_matches(candidate: Mapping[str, str]) -> bool:
    observed_at = str(candidate.get("observed_at", "")).strip()
    return str(candidate.get("observation_id", "")).strip() == evidence_observation_id(
        candidate, observed_at
    )


def valid_official_bse_delisting_evidence(
    candidate: Mapping[str, str],
    *,
    now: datetime | None = None,
) -> bool:
    """Return true only for an exact, current, self-consistent BSE delisting assertion."""

    if str(candidate.get("exchange", "")).strip() != "BSE_IN":
        return False
    if str(candidate.get("classification", "")).strip() != "delisted":
        return False
    if str(candidate.get("source_key", "")).strip() != BSE_SOURCE_KEY:
        return False
    if str(candidate.get("source_url", "")).strip() != BSE_STATUS_URL_TEMPLATE.format(
        status="Delisted"
    ):
        return False
    observed_at = str(candidate.get("observed_at", "")).strip()
    if not _observed_at_is_current(observed_at, now=now):
        return False
    return _observation_matches(candidate)


def valid_official_nasdaq_delete_evidence(
    candidate: Mapping[str, str],
    *,
    now: datetime | None = None,
) -> bool:
    """Return true only for an exact Nasdaq Trader trading-system Delete."""

    exchange = str(candidate.get("exchange", "")).strip()
    if exchange not in NASDAQ_DELETE_EXCHANGES:
        return False
    if str(candidate.get("classification", "")).strip() != "delisted":
        return False
    if str(candidate.get("source_key", "")).strip() != NASDAQ_SOURCE_KEY:
        return False
    if str(candidate.get("source_url", "")).strip() != NASDAQ_ADDS_DELETES_URL:
        return False
    if str(candidate.get("nasdaq_action", "")).strip() != "Delete":
        return False
    observed_at = str(candidate.get("observed_at", "")).strip()
    if not _observed_at_is_current(observed_at, now=now):
        return False
    return _observation_matches(candidate)


def valid_official_delisting_evidence(
    candidate: Mapping[str, str],
    *,
    now: datetime | None = None,
) -> bool:
    return valid_official_bse_delisting_evidence(
        candidate, now=now
    ) or valid_official_nasdaq_delete_evidence(candidate, now=now)


def parse_nasdaq_trading_system_deletes(text: str) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[-1].lower().startswith("file creation time"):
        lines = lines[:-1]
    rows: list[dict[str, str]] = []
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="|")
    for record in reader:
        if str(record.get("NASDAQ Action", "")).strip() != "Delete":
            continue
        ticker = str(record.get("Symbol", "")).strip()
        market = str(record.get("Primary Listing Market", "")).strip()
        exchange = NASDAQ_PRIMARY_MARKET_MAP.get(market, "")
        if not ticker or exchange not in NASDAQ_DELETE_EXCHANGES:
            continue
        rows.append(
            {
                "ticker": ticker,
                "exchange": exchange,
                "name": str(record.get("Company Name", "")).strip(),
                "nasdaq_action": "Delete",
                "effective_date": str(record.get("Effective Date", "")).strip(),
            }
        )
    return rows
