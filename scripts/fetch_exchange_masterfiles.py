from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
MASTERFILE_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
MASTERFILE_SOURCES_JSON = MASTERFILES_DIR / "sources.json"
MASTERFILE_SUMMARY_JSON = MASTERFILES_DIR / "summary.json"
MASTERFILE_CACHE_DIR = MASTERFILES_DIR / "cache"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
REQUEST_TIMEOUT = 30.0

OTHER_LISTED_EXCHANGE_MAP = {
    "A": "NYSE MKT",
    "M": "NYSE CHICAGO",
    "N": "NYSE",
    "P": "NYSE ARCA",
    "Q": "NASDAQ",
    "V": "IEX",
    "Z": "BATS",
}

SEC_EXCHANGE_MAP = {
    "Nasdaq": "NASDAQ",
    "NYSE": "NYSE",
    "NYSE American": "NYSE MKT",
    "NYSE Arca": "NYSE ARCA",
    "OTC": "OTC",
    "CboeBZX": "BATS",
}

ETF_NAME_MARKERS = (
    " etf",
    " etn",
    " fund",
    " trust",
    " ucits",
    "shares ",
)


@dataclass(frozen=True)
class MasterfileSource:
    key: str
    provider: str
    description: str
    source_url: str
    format: str
    official: bool = True


OFFICIAL_SOURCES = [
    MasterfileSource(
        key="nasdaq_listed",
        provider="Nasdaq Trader",
        description="Official Nasdaq-listed symbol directory",
        source_url=NASDAQ_LISTED_URL,
        format="nasdaq_listed_pipe",
    ),
    MasterfileSource(
        key="nasdaq_other_listed",
        provider="Nasdaq Trader",
        description="Official other-listed/CQS symbol directory",
        source_url=NASDAQ_OTHER_LISTED_URL,
        format="nasdaq_other_listed_pipe",
    ),
    MasterfileSource(
        key="sec_company_tickers_exchange",
        provider="SEC",
        description="Official SEC company ticker to exchange mapping",
        source_url=SEC_COMPANY_TICKERS_URL,
        format="sec_company_tickers_exchange_json",
    ),
]


def ensure_output_dirs() -> None:
    MASTERFILES_DIR.mkdir(parents=True, exist_ok=True)
    MASTERFILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_manual_masterfiles(manual_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not manual_dir.exists():
        return rows

    for path in sorted(manual_dir.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(
                    {
                        "source_key": f"manual:{path.stem}",
                        "provider": "manual",
                        "source_url": str(path.relative_to(ROOT)),
                        "ticker": row.get("ticker", "").strip(),
                        "name": row.get("name", "").strip(),
                        "exchange": row.get("exchange", "").strip(),
                        "asset_type": row.get("asset_type", "").strip() or infer_asset_type(row.get("name", "")),
                        "listing_status": row.get("listing_status", "").strip() or "active",
                        "official": "false",
                    }
                )
    return rows


def infer_asset_type(name: str) -> str:
    lowered = f" {name.lower()} "
    return "ETF" if any(marker in lowered for marker in ETF_NAME_MARKERS) else "Stock"


def fetch_text(url: str, session: requests.Session | None = None) -> str:
    session = session or requests.Session()
    response = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def fetch_json(url: str, session: requests.Session | None = None) -> Any:
    session = session or requests.Session()
    response = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def parse_pipe_table(text: str) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[-1].lower().startswith("file creation time"):
        lines = lines[:-1]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="|")
    return [{key: (value or "").strip() for key, value in row.items()} for row in reader]


def parse_nasdaq_listed(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in parse_pipe_table(text):
        symbol = row.get("Symbol", "")
        if not symbol:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": "NASDAQ",
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "official": "true",
            }
        )
    return rows


def parse_other_listed(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in parse_pipe_table(text):
        symbol = row.get("ACT Symbol", "")
        exchange = OTHER_LISTED_EXCHANGE_MAP.get(row.get("Exchange", ""), row.get("Exchange", ""))
        if not symbol or not exchange:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": exchange,
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "official": "true",
            }
        )
    return rows


def parse_sec_company_tickers_exchange(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    fields = payload.get("fields", [])
    rows: list[dict[str, str]] = []
    for values in payload.get("data", []):
        record = dict(zip(fields, values))
        ticker = str(record.get("ticker", "")).strip()
        exchange = SEC_EXCHANGE_MAP.get(str(record.get("exchange", "")).strip())
        if not ticker or not exchange:
            continue
        name = str(record.get("name", "")).strip()
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": exchange,
                "asset_type": infer_asset_type(name),
                "listing_status": "active",
                "official": "true",
            }
        )
    return rows


def fetch_source_rows(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    if source.format == "nasdaq_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_nasdaq_listed(text, source)
    if source.format == "nasdaq_other_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_other_listed(text, source)
    if source.format == "sec_company_tickers_exchange_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_sec_company_tickers_exchange(payload, source)
    raise ValueError(f"Unsupported source format: {source.format}")


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["source_key"], row["ticker"], row["exchange"], row["listing_status"])
        deduped[key] = row
    return sorted(deduped.values(), key=lambda row: (row["exchange"], row["ticker"], row["source_key"]))


def build_summary(rows: list[dict[str, str]]) -> dict[str, Any]:
    exchanges = sorted({row["exchange"] for row in rows if row["exchange"]})
    source_counts: dict[str, int] = {}
    for row in rows:
        source_counts[row["source_key"]] = source_counts.get(row["source_key"], 0) + 1
    return {
        "rows": len(rows),
        "exchanges": exchanges,
        "source_counts": source_counts,
    }


def fetch_all_sources(
    session: requests.Session | None = None,
    include_manual: bool = True,
    manual_dir: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    ensure_output_dirs()
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    for source in OFFICIAL_SOURCES:
        try:
            rows.extend(fetch_source_rows(source, session=session))
        except requests.RequestException as exc:
            errors.append({"source_key": source.key, "error": str(exc)})
    if include_manual:
        rows.extend(load_manual_masterfiles(manual_dir or MASTERFILES_DIR / "manual"))
    deduped = dedupe_rows(rows)
    summary = build_summary(deduped)
    if errors:
        summary["errors"] = errors
    return deduped, summary


def persist_source_metadata() -> None:
    ensure_output_dirs()
    payload = [asdict(source) for source in OFFICIAL_SOURCES]
    MASTERFILE_SOURCES_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    rows, summary = fetch_all_sources()
    persist_source_metadata()
    write_csv(
        MASTERFILE_REFERENCE_CSV,
        ["source_key", "provider", "source_url", "ticker", "name", "exchange", "asset_type", "listing_status", "official"],
        rows,
    )
    MASTERFILE_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)), **summary}, indent=2))


if __name__ == "__main__":
    main()
