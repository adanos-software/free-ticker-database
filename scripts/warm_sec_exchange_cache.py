"""Best-effort download of SEC company_tickers_exchange.json for CI and local gates."""

from __future__ import annotations

import json
import sys

try:
    from scripts.lib.us_exchange_directory import (
        SEC_COMPANY_TICKERS_CACHE,
        build_sec_exchange_payload_from_committed_sources,
        load_sec_exchange_payload,
    )
    from scripts.check_safe_merge import DEFAULT_IDENTIFIERS_EXTENDED, DEFAULT_LISTING_TRANSITIONS, load_csv
except ModuleNotFoundError:  # pragma: no cover
    from lib.us_exchange_directory import (
        SEC_COMPANY_TICKERS_CACHE,
        build_sec_exchange_payload_from_committed_sources,
        load_sec_exchange_payload,
    )
    from check_safe_merge import DEFAULT_IDENTIFIERS_EXTENDED, DEFAULT_LISTING_TRANSITIONS, load_csv


def main() -> int:
    payload = load_sec_exchange_payload()
    if payload:
        rows = len(payload.get("data", []))
        print(f"SEC company_tickers_exchange ready ({rows} rows) at {SEC_COMPANY_TICKERS_CACHE}")
        return 0
    if SEC_COMPANY_TICKERS_CACHE.exists():
        print(f"Using existing SEC cache at {SEC_COMPANY_TICKERS_CACHE}")
        return 0
    identifier_rows = (
        load_csv(DEFAULT_IDENTIFIERS_EXTENDED) if DEFAULT_IDENTIFIERS_EXTENDED.exists() else []
    )
    transition_rows = (
        load_csv(DEFAULT_LISTING_TRANSITIONS) if DEFAULT_LISTING_TRANSITIONS.exists() else []
    )
    fallback = build_sec_exchange_payload_from_committed_sources(identifier_rows, transition_rows)
    if fallback:
        SEC_COMPANY_TICKERS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        SEC_COMPANY_TICKERS_CACHE.write_text(json.dumps(fallback), encoding="utf-8")
        rows = len(fallback.get("data", []))
        print(
            "SEC company_tickers_exchange unavailable; wrote committed-source CIK fallback "
            f"({rows} rows) to {SEC_COMPANY_TICKERS_CACHE}",
            file=sys.stderr,
        )
        return 0
    print(
        "SEC company_tickers_exchange unavailable; same_cik listing transitions may fail safe merge",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
