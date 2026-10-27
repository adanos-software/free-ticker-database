"""Best-effort download of SEC company_tickers_exchange.json for CI and local gates."""

from __future__ import annotations

import sys

try:
    from scripts.lib.us_exchange_directory import (
        SEC_COMPANY_TICKERS_CACHE,
        load_sec_exchange_payload,
    )
except ModuleNotFoundError:  # pragma: no cover
    from lib.us_exchange_directory import SEC_COMPANY_TICKERS_CACHE, load_sec_exchange_payload


def main() -> int:
    payload = load_sec_exchange_payload()
    if payload:
        rows = len(payload.get("data", []))
        print(f"SEC company_tickers_exchange ready ({rows} rows) at {SEC_COMPANY_TICKERS_CACHE}")
        return 0
    if SEC_COMPANY_TICKERS_CACHE.exists():
        print(f"Using existing SEC cache at {SEC_COMPANY_TICKERS_CACHE}")
        return 0
    print(
        "SEC company_tickers_exchange unavailable; same_cik listing transitions may fail safe merge",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
