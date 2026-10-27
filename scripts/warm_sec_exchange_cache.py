"""Download SEC company_tickers_exchange.json for CI and local safe-merge gates."""

from __future__ import annotations

import sys

try:
    from scripts.lib.us_exchange_directory import (
        SEC_COMPANY_TICKERS_CACHE,
        refresh_sec_exchange_cache,
        shared_sec_cik,
    )
except ModuleNotFoundError:  # pragma: no cover
    from lib.us_exchange_directory import (
        SEC_COMPANY_TICKERS_CACHE,
        refresh_sec_exchange_cache,
        shared_sec_cik,
    )


def main() -> int:
    payload = refresh_sec_exchange_cache()
    if not payload:
        print(
            "SEC company_tickers_exchange unavailable after retries; "
            "same_cik listing transitions will fail safe merge",
            file=sys.stderr,
        )
        return 1
    rows = len(payload.get("data", []))
    print(f"SEC company_tickers_exchange ready ({rows} rows) at {SEC_COMPANY_TICKERS_CACHE}")
    shared_cik = shared_sec_cik("SKK", "RNTZ", payload)
    if shared_cik:
        print(f"SEC payload links SKK→RNTZ via CIK {shared_cik}")
    else:
        print(
            "SEC payload missing SKK/RNTZ shared CIK; same_cik SKK removal may fail safe merge",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
