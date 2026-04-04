from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
TICKERS_CSV = DATA_DIR / "tickers.csv"
ALIASES_CSV = DATA_DIR / "aliases.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
LISTING_STATUS_HISTORY_CSV = DATA_DIR / "history" / "listing_status_history.csv"
LISTING_EVENTS_CSV = DATA_DIR / "history" / "listing_events.csv"
COVERAGE_REPORT_JSON = REPORTS_DIR / "coverage_report.json"
COVERAGE_REPORT_MD = REPORTS_DIR / "coverage_report.md"


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_exchange_report(
    tickers: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
    masterfiles: list[dict[str, str]],
) -> list[dict[str, Any]]:
    identifier_lookup = {
        (row["ticker"], row["exchange"]): row for row in identifiers_extended
    }
    master_by_exchange: dict[str, set[str]] = defaultdict(set)
    for row in masterfiles:
        if row.get("listing_status") == "active" and row.get("reference_scope") == "exchange_directory":
            master_by_exchange[row["exchange"]].add(row["ticker"])

    exchanges = sorted(
        ({row["exchange"] for row in tickers if row["exchange"]})
        | {exchange for exchange in master_by_exchange if exchange}
    )

    rows: list[dict[str, Any]] = []
    for exchange in exchanges:
        exchange_rows = [row for row in tickers if row["exchange"] == exchange]
        identifiers = [identifier_lookup.get((row["ticker"], row["exchange"]), {}) for row in exchange_rows]
        dataset_symbols = {row["ticker"] for row in exchange_rows}
        master_symbols = master_by_exchange.get(exchange, set())
        matched = dataset_symbols & master_symbols if master_symbols else set()
        rows.append(
            {
                "exchange": exchange,
                "tickers": len(exchange_rows),
                "isin_coverage": sum(bool(row["isin"]) for row in exchange_rows),
                "sector_coverage": sum(bool(row["sector"]) for row in exchange_rows),
                "cik_coverage": sum(bool(row.get("cik")) for row in identifiers),
                "figi_coverage": sum(bool(row.get("figi")) for row in identifiers),
                "lei_coverage": sum(bool(row.get("lei")) for row in identifiers),
                "masterfile_symbols": len(master_symbols),
                "masterfile_matches": len(matched),
                "masterfile_match_rate": round(len(matched) / len(master_symbols) * 100, 2) if master_symbols else None,
            }
        )
    return rows


def build_country_report(
    tickers: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
) -> list[dict[str, Any]]:
    identifier_lookup = {
        (row["ticker"], row["exchange"]): row for row in identifiers_extended
    }
    rows: list[dict[str, Any]] = []
    for country in sorted({row["country"] for row in tickers if row["country"]}):
        country_rows = [row for row in tickers if row["country"] == country]
        identifiers = [identifier_lookup.get((row["ticker"], row["exchange"]), {}) for row in country_rows]
        rows.append(
            {
                "country": country,
                "tickers": len(country_rows),
                "isin_coverage": sum(bool(row["isin"]) for row in country_rows),
                "sector_coverage": sum(bool(row["sector"]) for row in country_rows),
                "cik_coverage": sum(bool(row.get("cik")) for row in identifiers),
                "figi_coverage": sum(bool(row.get("figi")) for row in identifiers),
                "lei_coverage": sum(bool(row.get("lei")) for row in identifiers),
            }
        )
    return rows


def build_global_summary(
    tickers: list[dict[str, str]],
    aliases: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
    listing_status_history: list[dict[str, str]],
    listing_events: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "tickers": len(tickers),
        "aliases": len(aliases),
        "stocks": sum(row["asset_type"] == "Stock" for row in tickers),
        "etfs": sum(row["asset_type"] == "ETF" for row in tickers),
        "isin_coverage": sum(bool(row["isin"]) for row in tickers),
        "sector_coverage": sum(bool(row["sector"]) for row in tickers),
        "cik_coverage": sum(bool(row.get("cik")) for row in identifiers_extended),
        "figi_coverage": sum(bool(row.get("figi")) for row in identifiers_extended),
        "lei_coverage": sum(bool(row.get("lei")) for row in identifiers_extended),
        "listing_status_rows": len(listing_status_history),
        "listing_events": len(listing_events),
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Coverage Report",
        "",
        "## Global",
        "",
        "| Metric | Value |",
        "|---|---|",
    ]
    for key, value in report["global"].items():
        lines.append(f"| {key} | {value} |")

    lines.extend(["", "## Exchange Coverage", "", "| Exchange | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Match Rate |", "|---|---|---|---|---|---|---|---|---|"])
    for row in report["exchange_coverage"]:
        lines.append(
            f"| {row['exchange']} | {row['tickers']} | {row['isin_coverage']} | {row['sector_coverage']} | {row['cik_coverage']} | {row['figi_coverage']} | {row['lei_coverage']} | {row['masterfile_symbols']} | {row['masterfile_match_rate'] if row['masterfile_match_rate'] is not None else ''} |"
        )

    lines.extend(["", "## Country Coverage", "", "| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |", "|---|---|---|---|---|---|---|"])
    for row in report["country_coverage"]:
        lines.append(
            f"| {row['country']} | {row['tickers']} | {row['isin_coverage']} | {row['sector_coverage']} | {row['cik_coverage']} | {row['figi_coverage']} | {row['lei_coverage']} |"
        )
    return "\n".join(lines) + "\n"


def build_report() -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    tickers = load_csv(TICKERS_CSV)
    aliases = load_csv(ALIASES_CSV)
    identifiers_extended = load_csv(IDENTIFIERS_EXTENDED_CSV)
    masterfiles = load_csv(MASTERFILE_REFERENCE_CSV)
    listing_status_history = load_csv(LISTING_STATUS_HISTORY_CSV)
    listing_events = load_csv(LISTING_EVENTS_CSV)

    report = {
        "global": build_global_summary(
            tickers,
            aliases,
            identifiers_extended,
            listing_status_history,
            listing_events,
        ),
        "exchange_coverage": build_exchange_report(tickers, identifiers_extended, masterfiles),
        "country_coverage": build_country_report(tickers, identifiers_extended),
    }

    COVERAGE_REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    COVERAGE_REPORT_MD.write_text(render_markdown(report), encoding="utf-8")
    return report


def main() -> None:
    report = build_report()
    print(json.dumps(report["global"], indent=2))


if __name__ == "__main__":
    main()
