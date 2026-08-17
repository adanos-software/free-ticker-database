"""Canonical, tamper-evident evidence helpers for automated delisting actions."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Mapping

BSE_SOURCE_KEY = "bse_india_scrips"
BSE_STATUS_URL_TEMPLATE = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status={status}"
)


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
    try:
        observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None or observed.utcoffset() is None:
        return False

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if observed.astimezone(timezone.utc) > current_time.astimezone(timezone.utc) + timedelta(
        minutes=5
    ):
        return False

    supplied_id = str(candidate.get("observation_id", "")).strip()
    expected_id = evidence_observation_id(candidate, observed_at)
    return supplied_id == expected_id
