from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

TICKERS_CSV = DATA_DIR / "tickers.csv"
ALIASES_CSV = DATA_DIR / "aliases.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"
TICKERS_PRETTY_JSON = DATA_DIR / "tickers.pretty.json"
TICKERS_DB = DATA_DIR / "tickers.db"
TICKERS_PARQUET = DATA_DIR / "tickers.parquet"
MANUAL_ISIN_CORRECTIONS = {
    "AAPL": "US0378331005",
    "MSFT": "US5949181045",
    "TSLA": "US88160R1014",
}

BAD_COMMON_ALIASES = {
    "angle",
    "azure",
    "circus",
    "coke",
    "elon",
    "musk",
    "pandora",
    "reserved",
}
BAD_CONTAMINATED_ALIASES = {
    "arima real estate socimi",
    "euv",
    "gpu",
    "jensen",
    "lithography",
    "ubm development",
    "united bus service",
    "urbas grupo financiero",
}
BAD_WRAPPER_TOKENS = ("cdr", "drc", "cedear")
BAD_US_PRIMARY_ALIASES = {
    "aws",
    "bezos",
    "cybertruck",
    "euv",
    "iphone",
    "lithography",
    "model 3",
    "model y",
    "satya",
    "tim cook",
    "windows",
}
US_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
OTC_EXCHANGES = {"OTC", "OTCCE", "OTCMKTS"}
STRICT_NUMERIC_NAMESPACE_EXCHANGES = {"Bursa", "KOSDAQ", "KRX", "TPEX", "TWSE"}
EXCHANGE_TICKER_RE = re.compile(r"^[A-Z0-9-]+\.[A-Z]{1,6}$")
IDENTIFIER_RE = re.compile(r"^[A-Z0-9]{5,12}$")
NUMERIC_TICKER_RE = re.compile(r"^[0-9]{3,6}[A-Z]?$")
NON_COMMON_PATTERNS = (
    re.compile(r"\brights?\b", re.IGNORECASE),
    re.compile(r"\bunits?\b", re.IGNORECASE),
    re.compile(r"\bwarrants?\b", re.IGNORECASE),
    re.compile(r"\bnotes?\b", re.IGNORECASE),
    re.compile(r"\bpref(?:erence)?(?:\s+sh(?:ares?)?)?\b", re.IGNORECASE),
    re.compile(r"\bpfd\b", re.IGNORECASE),
)
PREFERRED_PATTERN = re.compile(r"\bpreferred\b", re.IGNORECASE)
DEPOSITARY_PATTERN = re.compile(r"\bdepositary shares?\b", re.IGNORECASE)
US_CORP_NAME_RE = re.compile(
    r"\b(inc|incorporated|corporation|corp|company|co)\b",
    re.IGNORECASE,
)
COMPANY_STOPWORDS = {
    "a",
    "ab",
    "ag",
    "asa",
    "bank",
    "class",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "ltd",
    "n.v",
    "nv",
    "ordinary",
    "p.l.c",
    "p.l.c.",
    "plc",
    "s.a",
    "s.a.",
    "sa",
    "se",
    "shares",
    "stock",
}
ISIN_PREFIX_COUNTRIES = {
    "AT": "Austria",
    "AU": "Australia",
    "BE": "Belgium",
    "BM": "Bermuda",
    "BR": "Brazil",
    "CA": "Canada",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "EG": "Egypt",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "GG": "Guernsey",
    "GR": "Greece",
    "HK": "Hong Kong",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IM": "Isle of Man",
    "IN": "India",
    "IT": "Italy",
    "JE": "Jersey",
    "JP": "Japan",
    "KR": "South Korea",
    "KY": "Cayman Islands",
    "LU": "Luxembourg",
    "MX": "Mexico",
    "MY": "Malaysia",
    "NL": "Netherlands",
    "NO": "Norway",
    "NZ": "New Zealand",
    "PE": "Peru",
    "PH": "Philippines",
    "PL": "Poland",
    "PT": "Portugal",
    "QA": "Qatar",
    "RO": "Romania",
    "SE": "Sweden",
    "SG": "Singapore",
    "TH": "Thailand",
    "TR": "Turkey",
    "TW": "Taiwan",
    "ZA": "South Africa",
}

# ---------------------------------------------------------------------------
# Sector normalisation
# ---------------------------------------------------------------------------
# Maps variant names to canonical GICS sector names (for stocks) or
# canonical ETF category names.  Anything not listed here and longer than
# 50 characters is treated as garbage and cleared.

SECTOR_STOCK_MAP: dict[str, str] = {
    # GICS synonyms
    "Healthcare": "Health Care",
    "Technology": "Information Technology",
    "Basic Materials": "Materials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Financial Services": "Financials",
    "Communications": "Communication Services",
    "Commercial Real Estate": "Real Estate",
    "Residential Real Estate": "Real Estate",
    "REITs": "Real Estate",
}

# Canonical GICS sectors (for stocks)
VALID_STOCK_SECTORS: set[str] = {
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
}

# ETF-specific categories that are valid as-is
VALID_ETF_SECTORS: set[str] = VALID_STOCK_SECTORS | {
    "Blend",
    "Bonds",
    "Cash",
    "Commodities Broad Basket",
    "Corporate Bonds",
    "Currencies",
    "Derivatives",
    "Developed Markets",
    "Emerging Markets",
    "Equities",
    "Factors",
    "Fixed Income",
    "Frontier Markets",
    "Government Bonds",
    "Growth",
    "High Yield Bonds",
    "Inflation-Protected Securities",
    "Investment Grade Bonds",
    "Large Cap",
    "Mid Cap",
    "Micro Cap",
    "Money Market Instruments",
    "Municipal Bonds",
    "Small Cap",
    "Trading",
    "Treasury Bonds",
    "Value",
}


def normalize_sector(sector: str, asset_type: str) -> str:
    """Return a canonical sector string, or '' if the value is garbage."""
    if not sector or len(sector) > 50:
        return ""
    mapped = SECTOR_STOCK_MAP.get(sector, sector)
    if asset_type == "ETF":
        return mapped if mapped in VALID_ETF_SECTORS else ""
    if asset_type == "Stock":
        return mapped if mapped in VALID_STOCK_SECTORS else ""
    return ""


ISIN_FORMAT_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def is_valid_isin(isin: str) -> bool:
    """Validate ISIN format and Luhn check digit."""
    if not isin or not ISIN_FORMAT_RE.fullmatch(isin):
        return False
    digits = ""
    for char in isin[:-1]:
        if char.isdigit():
            digits += char
        else:
            digits += str(ord(char) - 55)
    total = 0
    for i, digit in enumerate(reversed(digits)):
        n = int(digit)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - (total % 10)) % 10 == int(isin[-1])


def split_aliases(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split("|") if part.strip()]


def dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


@lru_cache(maxsize=None)
def normalize_tokens(value: str) -> set[str]:
    tokens = set(re.findall(r"[a-z0-9]+", value.lower()))
    return {token for token in tokens if len(token) > 1 and token not in COMPANY_STOPWORDS}


def looks_like_identifier(alias: str, wkns: set[str], isin: str) -> bool:
    if not alias:
        return False
    if alias == isin or alias in wkns:
        return True
    if EXCHANGE_TICKER_RE.match(alias):
        return True
    if IDENTIFIER_RE.fullmatch(alias) and any(char.isdigit() for char in alias):
        return True
    return False


def has_wrapper_term(value: str) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in BAD_WRAPPER_TOKENS)


@lru_cache(maxsize=None)
def normalized_compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def alias_matches_company(alias: str, company_name: str) -> bool:
    alias_compact = normalized_compact(alias)
    company_compact = normalized_compact(company_name)
    if alias_compact and company_compact and (
        alias_compact in company_compact or company_compact in alias_compact
    ):
        return True
    return bool(normalize_tokens(alias) & normalize_tokens(company_name))


def is_depositary_row(row: dict[str, str]) -> bool:
    lowered = row["name"].lower()
    return "american depositary" in lowered or " adr" in lowered


def is_strict_numeric_namespace_row(row: dict[str, str]) -> bool:
    return bool(
        row["exchange"] in STRICT_NUMERIC_NAMESPACE_EXCHANGES
        and NUMERIC_TICKER_RE.fullmatch(row["ticker"])
    )


def is_namespace_collision_row(
    row: dict[str, str],
    aliases: list[str],
    wkns: set[str],
) -> bool:
    if not is_strict_numeric_namespace_row(row):
        return False

    isin = MANUAL_ISIN_CORRECTIONS.get(row["ticker"], row["isin"])
    if not isin:
        return False

    prefix = isin[:2]
    exchange = row["exchange"]
    ticker = row["ticker"]

    if exchange in {"KRX", "KOSDAQ"}:
        if ticker.startswith("9"):
            return False
        return prefix != "KR"

    if exchange in {"TWSE", "TPEX"} and row["asset_type"] == "ETF":
        return prefix != "TW"

    if exchange not in {"TWSE", "TPEX", "Bursa"}:
        return False

    expected_prefix = "MY" if exchange == "Bursa" else "TW"
    if prefix == expected_prefix:
        return False

    for alias in aliases:
        if looks_like_identifier(alias, wkns, isin):
            continue
        if not alias_matches_company(alias, row["name"]):
            return True
    return False


def should_exclude_stock_row(row: dict[str, str]) -> bool:
    if row["asset_type"] != "Stock":
        return False
    name = row["name"].lower()
    if "american depositary" in name:
        return False
    if row["ticker"].count("-P-"):
        return True
    if PREFERRED_PATTERN.search(name):
        return True
    if DEPOSITARY_PATTERN.search(name) and "american depositary" not in name:
        return True
    return any(pattern.search(name) for pattern in NON_COMMON_PATTERNS)


def is_suspicious_us_primary(row: dict[str, str], aliases: list[str]) -> bool:
    if row["exchange"] not in US_EXCHANGES:
        return False
    if row["country"] != "United States":
        return False
    isin = row["isin"]
    if not isin or isin.startswith("US"):
        return False
    if not US_CORP_NAME_RE.search(row["name"]):
        return False
    if any(has_wrapper_term(alias) for alias in aliases):
        return True
    return any(alias.lower() in BAD_US_PRIMARY_ALIASES for alias in aliases)


def clean_aliases(
    row: dict[str, str],
    aliases: list[str],
    wkns: set[str],
) -> tuple[str, list[str]]:
    suspicious_us_primary = is_suspicious_us_primary(row, aliases)
    strict_alias_filter = (
        suspicious_us_primary
        or is_depositary_row(row)
        or is_strict_numeric_namespace_row(row)
    )
    cleaned_isin = MANUAL_ISIN_CORRECTIONS.get(row["ticker"], row["isin"])
    cleaned_aliases: list[str] = []

    for alias in aliases:
        lowered = alias.lower()
        if lowered in BAD_COMMON_ALIASES:
            continue
        if lowered in BAD_CONTAMINATED_ALIASES:
            continue
        if has_wrapper_term(alias):
            continue
        if lowered.startswith("1x "):
            continue
        if suspicious_us_primary and lowered in BAD_US_PRIMARY_ALIASES:
            continue
        # Skip very short name aliases (<=2 chars) -- too ambiguous
        if len(alias) <= 2 and not looks_like_identifier(alias, wkns, cleaned_isin):
            continue
        # Skip pure-numeric aliases that aren't identifiers (WKN/ISIN)
        if alias.isdigit() and alias not in wkns and alias != cleaned_isin:
            if not is_strict_numeric_namespace_row(row):
                continue
            cleaned_aliases.append(alias)
            continue
        if strict_alias_filter and not looks_like_identifier(alias, wkns, cleaned_isin):
            if not alias_matches_company(alias, row["name"]):
                continue
        cleaned_aliases.append(alias)

    if suspicious_us_primary:
        cleaned_isin = MANUAL_ISIN_CORRECTIONS.get(row["ticker"], "")

    if cleaned_isin and not is_valid_isin(cleaned_isin):
        cleaned_isin = ""

    return cleaned_isin, dedupe_keep_order(cleaned_aliases)


def country_from_isin(isin: str) -> str | None:
    if len(isin) < 2:
        return None
    prefix = isin[:2]
    return ISIN_PREFIX_COUNTRIES.get(prefix)


def load_data():
    with TICKERS_CSV.open(newline="") as handle:
        ticker_rows = list(csv.DictReader(handle))

    with ALIASES_CSV.open(newline="") as handle:
        alias_rows = list(csv.DictReader(handle))

    with IDENTIFIERS_CSV.open(newline="") as handle:
        identifier_rows = list(csv.DictReader(handle))

    alias_type_lookup: dict[tuple[str, str], str] = {}
    extra_aliases: dict[str, list[str]] = defaultdict(list)
    for row in alias_rows:
        key = (row["ticker"], row["alias"])
        alias_type_lookup.setdefault(key, row["alias_type"])
        if row["alias_type"] != "isin":
            extra_aliases[row["ticker"]].append(row["alias"])

    identifier_lookup = {
        row["ticker"]: {"isin": row["isin"], "wkn": row["wkn"]}
        for row in identifier_rows
    }

    return ticker_rows, alias_type_lookup, extra_aliases, identifier_lookup


def cleaned_rows():
    ticker_rows, alias_type_lookup, extra_aliases, identifier_lookup = load_data()

    base_rows: list[dict[str, str]] = []
    for row in ticker_rows:
        if should_exclude_stock_row(row):
            continue
        merged = dict(row)
        merged_aliases = split_aliases(row["aliases"]) + extra_aliases.get(row["ticker"], [])
        identifier = identifier_lookup.get(row["ticker"])
        if identifier and identifier["wkn"]:
            merged_aliases.append(identifier["wkn"])
        merged["aliases"] = merged_aliases
        base_rows.append(merged)

    output_rows: list[dict[str, str]] = []
    for row in base_rows:
        aliases = dedupe_keep_order(row["aliases"])
        identifier = identifier_lookup.get(row["ticker"], {"wkn": ""})
        wkns = {identifier["wkn"]} if identifier.get("wkn") else set()
        if is_namespace_collision_row(row, aliases, wkns):
            continue
        isin, aliases = clean_aliases(row, aliases, wkns)
        country = row["country"]
        if isin:
            country = country_from_isin(isin) or country

        output_rows.append(
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "sector": normalize_sector(row["sector"], row["asset_type"]),
                "country": country,
                "isin": isin,
                "aliases": aliases,
                "wkn": identifier.get("wkn", ""),
            }
        )

    return sorted(output_rows, key=lambda row: row["ticker"]), alias_type_lookup


def build_alias_rows(rows: list[dict[str, str]], alias_type_lookup: dict[tuple[str, str], str]):
    alias_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        wkns = {row["wkn"]} if row["wkn"] else set()
        all_aliases = []
        if row["isin"]:
            all_aliases.append((row["isin"], "isin"))
        for alias in row["aliases"]:
            if (row["ticker"], alias) in alias_type_lookup:
                alias_type = alias_type_lookup[(row["ticker"], alias)]
            elif alias in wkns:
                alias_type = "wkn"
            elif EXCHANGE_TICKER_RE.match(alias):
                alias_type = "exchange_ticker"
            else:
                alias_type = "name"
            all_aliases.append((alias, alias_type))

        for alias, alias_type in all_aliases:
            key = (row["ticker"], alias)
            if key in seen:
                continue
            seen.add(key)
            alias_rows.append({"ticker": row["ticker"], "alias": alias, "alias_type": alias_type})
    return alias_rows


def build_identifier_rows(rows: list[dict[str, str]]):
    return [{"ticker": row["ticker"], "isin": row["isin"], "wkn": row["wkn"]} for row in rows]


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, str]]):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: list[dict[str, str]]):
    payload = [
        {
            "ticker": row["ticker"],
            "name": row["name"],
            "exchange": row["exchange"],
            "asset_type": row["asset_type"],
            "sector": row["sector"],
            "country": row["country"],
            "isin": row["isin"],
            "aliases": row["aliases"],
        }
        for row in rows
    ]
    TICKERS_JSON.write_text(json.dumps(payload, separators=(",", ":")))
    TICKERS_PRETTY_JSON.write_text(json.dumps(payload, indent=2))


def write_db(rows: list[dict[str, str]], alias_rows: list[dict[str, str]]):
    if TICKERS_DB.exists():
        TICKERS_DB.unlink()
    conn = sqlite3.connect(TICKERS_DB)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(
            """
            CREATE TABLE tickers (
                ticker TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                exchange TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                sector TEXT DEFAULT '',
                country TEXT DEFAULT '',
                isin TEXT DEFAULT ''
            );

            CREATE TABLE aliases (
                ticker TEXT NOT NULL,
                alias TEXT NOT NULL,
                alias_type TEXT NOT NULL,
                PRIMARY KEY (ticker, alias),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            );

            CREATE INDEX idx_aliases_alias ON aliases(alias);
            CREATE INDEX idx_tickers_exchange ON tickers(exchange);
            CREATE INDEX idx_tickers_isin ON tickers(isin);
            CREATE INDEX idx_tickers_sector ON tickers(sector);
            """
        )
        conn.executemany(
            """
            INSERT INTO tickers (ticker, name, exchange, asset_type, sector, country, isin)
            VALUES (:ticker, :name, :exchange, :asset_type, :sector, :country, :isin)
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT INTO aliases (ticker, alias, alias_type)
            VALUES (:ticker, :alias, :alias_type)
            """,
            alias_rows,
        )
        conn.commit()
    finally:
        conn.close()


def write_parquet(rows: list[dict[str, str]]):
    frame = pd.DataFrame(
        [
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "sector": row["sector"],
                "country": row["country"],
                "isin": row["isin"],
                "aliases": row["aliases"],
            }
            for row in rows
        ]
    )
    frame.to_parquet(TICKERS_PARQUET, index=False)


def rebuild():
    rows, alias_type_lookup = cleaned_rows()
    alias_rows = build_alias_rows(rows, alias_type_lookup)
    identifier_rows = build_identifier_rows(rows)

    write_csv(
        TICKERS_CSV,
        ["ticker", "name", "exchange", "asset_type", "sector", "country", "isin", "aliases"],
        (
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "sector": row["sector"],
                "country": row["country"],
                "isin": row["isin"],
                "aliases": "|".join(row["aliases"]),
            }
            for row in rows
        ),
    )
    write_csv(ALIASES_CSV, ["ticker", "alias", "alias_type"], alias_rows)
    write_csv(IDENTIFIERS_CSV, ["ticker", "isin", "wkn"], identifier_rows)
    write_json(rows)
    write_db(rows, alias_rows)
    write_parquet(rows)

    stats = {
        "tickers": len(rows),
        "stocks": sum(1 for row in rows if row["asset_type"] == "Stock"),
        "etfs": sum(1 for row in rows if row["asset_type"] == "ETF"),
        "exchanges": len({row["exchange"] for row in rows if row["exchange"]}),
        "countries": len({row["country"] for row in rows if row["country"]}),
        "isin_coverage": sum(1 for row in rows if row["isin"]),
        "sector_coverage": sum(1 for row in rows if row["sector"]),
        "aliases": len(alias_rows),
    }
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    rebuild()
