from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Mapping, Sequence

try:
    from scripts.lib.canonical_ids import stable_id
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from lib.canonical_ids import stable_id

TRUSTED_EVIDENCE = {"official", "reviewed", "verified"}


@dataclass(frozen=True)
class ListingLifecycle:
    listing_id: str
    listing_key: str
    ticker: str
    exchange: str
    status: str
    valid_from: str
    valid_to: str
    current: bool
    evidence_status: str

    def as_dict(self) -> dict[str, str]:
        return {
            "listing_id": self.listing_id,
            "listing_key": self.listing_key,
            "ticker": self.ticker,
            "exchange": self.exchange,
            "status": self.status,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "current": "true" if self.current else "false",
            "evidence_status": self.evidence_status,
        }


def row_key(row: Mapping[str, str]) -> str:
    return str(row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}").strip()


def _time(row: Mapping[str, str]) -> str:
    return str(row.get("effective_at") or row.get("first_observed_at") or row.get("observed_at") or "")


def _trusted_delisting(row: Mapping[str, str]) -> bool:
    return str(row.get("status", "")) == "delisted" and str(row.get("evidence_status", "")) in TRUSTED_EVIDENCE and bool(_time(row))


def build_listing_lifecycles(
    current_rows: Sequence[Mapping[str, str]],
    status_rows: Sequence[Mapping[str, str]],
    *,
    observed_at: str,
) -> list[ListingLifecycle]:
    current_by_key: dict[str, Mapping[str, str]] = {}
    for row in current_rows:
        key = row_key(row)
        if key in current_by_key:
            raise ValueError(f"duplicate current listing key: {key}")
        current_by_key[key] = row

    history_by_key: dict[str, list[Mapping[str, str]]] = defaultdict(list)
    for row in status_rows:
        key = row_key(row)
        if key:
            history_by_key[key].append(row)

    lifecycles: list[ListingLifecycle] = []
    all_keys = sorted(set(current_by_key) | set(history_by_key))
    for key in all_keys:
        current = current_by_key.get(key)
        history = sorted(
            history_by_key.get(key, []),
            key=lambda row: (_time(row), 0 if str(row.get("status", "")) == "active" else 1),
        )
        exchange, ticker = (
            key.split("::", 1)
            if "::" in key
            else (str(current.get("exchange", "")) if current else "", key)
        )

        state: str | None = None
        active_start = ""
        earliest_observation = min(
            (
                str(row.get("first_observed_at") or row.get("effective_at") or row.get("observed_at") or "")
                for row in history
                if str(row.get("first_observed_at") or row.get("effective_at") or row.get("observed_at") or "")
            ),
            default="",
        )

        for row in history:
            when = _time(row)
            if not when:
                continue
            status = str(row.get("status", ""))
            if status == "active":
                if state != "active":
                    active_start = when
                    state = "active"
                elif not active_start or when < active_start:
                    active_start = when
                continue
            if not _trusted_delisting(row):
                continue
            # Repeated terminal evidence without an intervening active observation
            # confirms the same lifecycle; it must not manufacture ticker reuse.
            if state == "delisted":
                continue
            start_at = active_start or earliest_observation or when
            if start_at > when:
                start_at = when
            lifecycle_key = f"{key}|{start_at}|{when}|delisted"
            lifecycles.append(
                ListingLifecycle(
                    listing_id=stable_id("listing", lifecycle_key),
                    listing_key=key,
                    ticker=ticker,
                    exchange=exchange,
                    status="delisted",
                    valid_from=start_at,
                    valid_to=when,
                    current=False,
                    evidence_status=str(row.get("evidence_status", "")),
                )
            )
            state = "delisted"
            active_start = ""

        if current is not None:
            # A current snapshot after a trusted terminal event is itself evidence
            # of a new lifecycle, but its start cannot be backdated without an
            # explicit active/relisted observation after that event.
            if state != "active":
                active_start = observed_at
            if not active_start:
                active_start = earliest_observation or observed_at
            lifecycle_key = f"{key}|{active_start}|active"
            lifecycles.append(
                ListingLifecycle(
                    listing_id=stable_id("listing", lifecycle_key),
                    listing_key=key,
                    ticker=str(current.get("ticker", ticker)),
                    exchange=str(current.get("exchange", exchange)),
                    status="active",
                    valid_from=active_start,
                    valid_to="",
                    current=True,
                    evidence_status="current_snapshot",
                )
            )

    active_counts: dict[str, int] = defaultdict(int)
    for row in lifecycles:
        if row.current:
            active_counts[row.listing_key] += 1
    invalid = [
        key for key in current_by_key
        if active_counts.get(key, 0) != 1
    ]
    if invalid:
        raise RuntimeError(f"listing lifecycle postcondition failed: {invalid[:10]}")
    return sorted(
        lifecycles,
        key=lambda item: (item.listing_key, item.valid_from, item.valid_to, item.listing_id),
    )
