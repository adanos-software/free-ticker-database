from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from scripts.listing_keys import row_listing_key
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from listing_keys import row_listing_key


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

TICKERS_CSV = DATA_DIR / "tickers.csv"
LISTINGS_CSV = DATA_DIR / "listings.csv"
ALIASES_CSV = DATA_DIR / "aliases.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"
TICKERS_DB = DATA_DIR / "tickers.db"
TICKERS_PARQUET = DATA_DIR / "tickers.parquet"
CROSS_LISTINGS_CSV = DATA_DIR / "cross_listings.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
REVIEW_OVERRIDES_DIR = DATA_DIR / "review_overrides"
REVIEW_REMOVE_ALIASES_CSV = REVIEW_OVERRIDES_DIR / "remove_aliases.csv"
REVIEW_METADATA_UPDATES_CSV = REVIEW_OVERRIDES_DIR / "metadata_updates.csv"
REVIEW_DROP_ENTRIES_CSV = REVIEW_OVERRIDES_DIR / "drop_entries.csv"
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
BAD_OBVIOUS_AMBIGUOUS_ALIASES = {
    "apes",
    "aws",
    "bezos",
    "bitcoin mining",
    "cash app",
    "chrysler",
    "cybertruck",
    "euv",
    "iphone",
    "jack dorsey",
    "jack ma",
    "jeep",
    "lithography",
    "model 3",
    "model y",
    "norton",
    "ram trucks",
    "satya",
    "tim cook",
    "xfinity",
    "windows",
    "avast",
}
GENERIC_FUND_WRAPPER_NAMES = {
    "ab active etfs",
    "ab active etfs, inc.",
    "advisor managed portfolios",
    "aim etf products trust",
    "allspring exchange-traded funds trust",
    "american century etf trust",
    "bank of montreal",
    "bitwise funds trust",
    "blackrock etf trust",
    "bondbloxx etf trust",
    "calamos etf trust",
    "capital group fixed income etf trust",
    "columbia etf trust i",
    "dimensional etf trust",
    "direxion shares etf trust",
    "ea series trust",
    "elevation series trust",
    "etf series solutions",
    "exchange listed funds trust",
    "exchange traded concepts trust",
    "federated hermes etf trust",
    "fidelity covington trust",
    "first trust exchange-traded fund",
    "first trust exchange-traded fund iv",
    "first trust exchange-traded fund viii",
    "franklin templeton etf trust",
    "global x funds",
    "goldman sachs etf trust",
    "harbor etf trust",
    "innovator etfs trust",
    "ishares trust",
    "j.p. morgan exchange-traded fund trust",
    "john hancock exchange-traded fund trust",
    "kraneshares trust",
    "listed funds trust",
    "matthews international funds",
    "morgan stanley etf trust",
    "neuberger berman etf trust",
    "northern funds",
    "precidian etfs trust",
    "proshares trust",
    "putnam etf trust",
    "select sector spdr trust",
    "simplify exchange traded funds",
    "spinnaker etf series",
    "t. rowe price exchange-traded funds",
    "t. rowe price exchange-traded funds, inc.",
    "tcw etf trust",
    "the 2023 etf series trust",
    "the 2023 etf series trust ii",
    "the advisors inner circle fund ii",
    "the advisors inner circle fund iii",
    "advisors inner circle fund ii",
    "advisors inner circle fund iii",
    "the advisorsâ inner circle fund ii",
    "tidal etf trust",
    "tidal trust ii",
    "tidal trust iii",
    "trust for professional managers",
    "us exchange listed funds trust",
    "virtus etf trust ii",
}
GENERIC_ETF_PLACEHOLDER_NAMES_EXACT = {
    "absolute shares trust",
    "grayscale funds trust",
    "investment managers series trust ii",
    "professionally managed portfolios",
    "series portfolios trust",
    "strategy shares",
    "the rbb fund trust",
    "tidal trust i",
}
BAD_GENERIC_FUND_ALIASES = GENERIC_FUND_WRAPPER_NAMES
US_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
OTC_EXCHANGES = {"OTC", "OTCCE", "OTCMKTS"}
PRIMARY_VENUE_CORRECTION_EXCHANGES = US_EXCHANGES | OTC_EXCHANGES
KOREAN_STOCK_EXCHANGES = {"KRX", "KOSDAQ"}
STRICT_NUMERIC_NAMESPACE_EXCHANGES = {"Bursa", "KOSDAQ", "KRX", "TPEX", "TWSE"}
EXCHANGE_TICKER_RE = re.compile(r"^[A-Z0-9-]+\.[A-Z]{1,6}$")
IDENTIFIER_RE = re.compile(r"^[A-Z0-9]{5,12}$")
NUMERIC_TICKER_RE = re.compile(r"^[0-9]{3,6}[A-Z]?$")
B3_DEPOSITARY_TICKER_RE = re.compile(r".*(31|32|33|34|35|39)$")
B3_FRACTIONAL_TICKER_RE = re.compile(r".*F$")
B3_UNIT_TICKER_RE = re.compile(r".*11$")
PSX_CORPORATE_ACTION_TICKER_RE = re.compile(
    r"^[A-Z0-9]+-(?:CA|CM|CMA|CAP|CAPR|CMAR|CMAY)(?:N1?|)?$"
)
PSX_GOVERNMENT_SECURITY_TICKER_RE = re.compile(r"^P\d{2}(?:GIS|FRR)\d{4}$")
PSX_RIGHTS_TICKER_RE = re.compile(r"^[A-Z0-9]{3,10}R\d+$")
ASX_CAPITAL_NOTE_TICKER_RE = re.compile(r".*P[A-Z]$")
ASX_LOYALTY_TICKER_RE = re.compile(r".*LV$")
TAIWAN_ETF_TICKER_RE = re.compile(r"^00\d{2,4}[A-Z]?$")
TAIWAN_NON_COMMON_STOCK_TICKER_RE = re.compile(r"^\d{4}B$")
NEO_CDR_XDR_RE = re.compile(r"\b(?:cdr|xdr)\b", re.IGNORECASE)
TMX_OFFICIAL_SERIES_TICKER_RE = re.compile(r"^(?P<root>[A-Z0-9]+)\.(?P<suffix>[A-Z0-9]+)$")
TMX_WARRANT_TICKER_RE = re.compile(r"^[A-Z0-9]{1,8}-WT[A-Z]*$")
PREFERRED_TICKER_SUFFIX_RE = re.compile(r"^[A-Z]{1,8}-P[A-Z]$")
PREFERRED_PF_TICKER_RE = re.compile(r"^[A-Z]{1,8}-PF[A-Z]$")
PREFERRED_PR_TICKER_RE = re.compile(r"^[A-Z]{1,8}-PR-[A-Z]$")
PREFERRED_PREF_TICKER_RE = re.compile(r"^[A-Z]{1,8}-PREF$")
US_SERIES_PREFERRED_TICKER_RE = re.compile(r"^[A-Z]{1,8}-P$")
KRX_SECONDARY_LINE_TICKER_RE = re.compile(r"^\d{5}[579KL]$")
EUROPEAN_CERTIFICATE_PATTERN = re.compile(r"\bparticipated cert\b|\bcert\(", re.IGNORECASE)
ETP_NAME_PATTERNS = (
    re.compile(r"\betf\b", re.IGNORECASE),
    re.compile(r"\betn\b", re.IGNORECASE),
    re.compile(r"\betp\b", re.IGNORECASE),
    re.compile(r"\bwisdomtree\b", re.IGNORECASE),
    re.compile(r"\bvaneck\b", re.IGNORECASE),
    re.compile(r"^ish\b", re.IGNORECASE),
    re.compile(r"\bishares?\b", re.IGNORECASE),
)
NON_COMMON_PATTERNS = (
    re.compile(r"\brights?\b", re.IGNORECASE),
    re.compile(r"\bunits?\b", re.IGNORECASE),
    re.compile(r"\bwarrants?\b", re.IGNORECASE),
    re.compile(r"\bnotes\b", re.IGNORECASE),
    re.compile(r"\bpref(?:erence)?(?:\s+sh(?:ares?)?)?\b", re.IGNORECASE),
    re.compile(r"\bpfd\b", re.IGNORECASE),
)
PREFERRED_PATTERN = re.compile(r"\bpreferred\b", re.IGNORECASE)
DEPOSITARY_PATTERN = re.compile(
    r"\bdepositary (?:shares?|receipts?)\b",
    re.IGNORECASE,
)
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
CORPORATE_ALIAS_MARKERS = {
    "bank",
    "biotech",
    "biosciences",
    "capital",
    "corp",
    "corporation",
    "energy",
    "etf",
    "financial",
    "fund",
    "group",
    "holding",
    "holdings",
    "inc",
    "limited",
    "ltd",
    "media",
    "metals",
    "pharmaceuticals",
    "plc",
    "resources",
    "sa",
    "technology",
    "technologies",
    "trust",
    "ventures",
}
TRUSTED_NON_LEXICAL_ALIASES: dict[str, set[str]] = {
    "athleta": {"gap", "the gap"},
    "bmw": {"bayerische motoren werke", "bay.motoren werke ag st"},
    "citibank": {"citigroup", "citigroup inc."},
    "femsa": {"fomento economico mexicano", "fomento económico mexicano, s.a.b. de c.v."},
    "contraladora axel sab": {"controladora axtel, s.a.b. de c.v."},
    "daetwyl i": {"dätwyler holding ag"},
    "evolva holding sa": {"evonext holdings sa"},
    "itaúsa - investimentos itaú": {"itausa", "itausa s.a."},
    "jereissati participações": {"iguatemi", "iguatemi s.a."},
    "keybank": {"keycorp"},
    "leclanche sa": {"leclanché sa"},
    "maseca": {"gruma", "gruma s.a.b. de c.v.", "gruma sab de cv"},
    "munich re": {"muench.rueckversicherungs gesellschaft.vna o.n.", "muench. rueckvers. vna o.n."},
    "münchener rück": {"muench.rueckversicherungs gesellschaft.vna o.n.", "muench. rueckvers. vna o.n."},
    "nortonlifelock": {"gen digital"},
    "old navy": {"gap", "the gap"},
    "randon s.a. implementos e participações": {"randoncorp", "randoncorp s.a."},
    "sena j property pcl": {"sen x public company limited"},
    "shareholder value": {"sharehold.val.bet.na o.n.", "synthaverse"},
    "shareholder value beteiligungen": {"sharehold.val.bet.na o.n.", "synthaverse"},
    "smd100_saintmed": {"smd rise public company limited"},
    "starrag group holding ag": {"starragtornos group ag"},
    "synthaverse": {"sharehold.val.bet.na o.n.", "synthaverse"},
    "tdg gold": {"tdggf", "tdg gold corp"},
    "tiger brokers": {"up fintech", "up fintech holding ltd", "up fintech holding"},
    "banque cantonale du valais": {"walliser kantonalbank"},
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

# Countries present in the dataset that are not covered by the ISIN prefix
# table and therefore need an explicit ISO 3166-1 alpha-2 mapping.
COUNTRY_TO_ISO_EXTRA: dict[str, str] = {
    "Argentina": "AR",
    "Botswana": "BW",
    "Croatia": "HR",
    "Ghana": "GH",
    "Iceland": "IS",
    "Kenya": "KE",
    "Malawi": "MW",
    "Mauritius": "MU",
    "Morocco": "MA",
    "Nigeria": "NG",
    "Pakistan": "PK",
    "Panama": "PA",
    "Rwanda": "RW",
    "Sri Lanka": "LK",
    "Tanzania": "TZ",
    "Uganda": "UG",
    "United States": "US",
    "Vietnam": "VN",
    "Zambia": "ZM",
    "Zimbabwe": "ZW",
}

COUNTRY_TO_ISO: dict[str, str] = {
    country: code for code, country in ISIN_PREFIX_COUNTRIES.items()
} | COUNTRY_TO_ISO_EXTRA

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


BLOCKED_ALIAS_KEYS = {
    normalized_compact(alias)
    for alias in BAD_COMMON_ALIASES
    | BAD_CONTAMINATED_ALIASES
    | BAD_OBVIOUS_AMBIGUOUS_ALIASES
    | BAD_GENERIC_FUND_ALIASES
}


def is_blocked_alias(alias: str) -> bool:
    lowered = alias.lower().strip()
    return lowered in (
        BAD_COMMON_ALIASES
        | BAD_CONTAMINATED_ALIASES
        | BAD_OBVIOUS_AMBIGUOUS_ALIASES
        | BAD_GENERIC_FUND_ALIASES
    ) or normalized_compact(alias) in BLOCKED_ALIAS_KEYS


def is_trusted_non_lexical_alias(alias: str, company_name: str) -> bool:
    trusted_companies = TRUSTED_NON_LEXICAL_ALIASES.get(alias.lower().strip())
    if not trusted_companies:
        return False
    company_compact = normalized_compact(company_name)
    return any(normalized_compact(candidate) in company_compact for candidate in trusted_companies)


def is_company_style_alias(alias: str) -> bool:
    tokens = normalize_tokens(alias)
    lowered = alias.lower()
    return len(tokens) >= 1 and (
        bool(tokens & CORPORATE_ALIAS_MARKERS)
        or bool(
            re.search(
                r"\b(inc|corp|corporation|holdings?|group|plc|ltd|limited|sa|ag|nv|bank|trust|fund|etf)\b",
                lowered,
            )
        )
    )


def alias_matches_company(alias: str, company_name: str) -> bool:
    if (
        is_trusted_non_lexical_alias(alias, company_name)
        or is_trusted_non_lexical_alias(company_name, alias)
    ):
        return True
    alias_compact = normalized_compact(alias)
    company_compact = normalized_compact(company_name)
    if alias_compact and company_compact and (
        alias_compact in company_compact or company_compact in alias_compact
    ):
        return True
    return bool(normalize_tokens(alias) & normalize_tokens(company_name))


def is_depositary_row(row: dict[str, str]) -> bool:
    lowered = row["name"].lower()
    return bool(
        "american depositary" in lowered
        or " adr" in lowered
        or DEPOSITARY_PATTERN.search(lowered)
    )


def is_numeric_exchange_alias(row: dict[str, str], alias: str, wkns: set[str], isin: str) -> bool:
    return bool(
        is_strict_numeric_namespace_row(row)
        and alias.isdigit()
        and alias not in wkns
        and alias != isin
    )


def is_strict_numeric_namespace_row(row: dict[str, str]) -> bool:
    return bool(
        row.get("exchange", "") in STRICT_NUMERIC_NAMESPACE_EXCHANGES
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


def entity_key_for_row(row: dict[str, str]) -> str:
    if row.get("isin"):
        return f"isin:{row['isin']}"
    normalized_tokens = sorted(normalize_tokens(row["name"]))
    if normalized_tokens:
        return f"name:{'|'.join(normalized_tokens)}"
    return f"ticker:{row['ticker']}"


def row_name_fingerprint(row: dict[str, str]) -> str:
    return "|".join(sorted(normalize_tokens(row["name"])))


def build_stock_base_name_index(rows: Iterable[dict[str, str]]) -> dict[str, set[str]]:
    index: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        fingerprint = row_name_fingerprint(row)
        if not fingerprint:
            continue
        index[row["ticker"].upper()].add(fingerprint)
    return index


def build_stock_name_lookup(rows: Iterable[dict[str, str]]) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        name = row.get("name", "").strip()
        if name:
            lookup[row["ticker"].upper()].add(name)
    return lookup


def has_matching_base_listing(
    row: dict[str, str],
    stock_base_name_index: dict[str, set[str]] | None,
) -> bool:
    if not stock_base_name_index:
        return False
    ticker = row["ticker"].upper()
    if "-" not in ticker:
        return False
    base_ticker = ticker.split("-", 1)[0]
    fingerprint = row_name_fingerprint(row)
    if not fingerprint:
        return False
    return fingerprint in stock_base_name_index.get(base_ticker, set())


def has_matching_krx_base_listing(
    row: dict[str, str],
    stock_name_lookup: dict[str, set[str]] | None,
) -> bool:
    if not stock_name_lookup:
        return False
    if row.get("exchange") != "KRX":
        return False
    ticker = row["ticker"].upper()
    if not KRX_SECONDARY_LINE_TICKER_RE.fullmatch(ticker):
        return False
    base_ticker = f"{ticker[:5]}0"
    if base_ticker == ticker:
        return False
    base_names = stock_name_lookup.get(base_ticker, set())
    if not base_names:
        return False
    return any(alias_matches_company(row["name"], base_name) for base_name in base_names)


def should_exclude_stock_row(
    row: dict[str, str],
    stock_base_name_index: dict[str, set[str]] | None = None,
    stock_name_lookup: dict[str, set[str]] | None = None,
) -> bool:
    if row["asset_type"] != "Stock":
        return False
    ticker = row["ticker"].upper()
    name = row["name"].lower()
    if row.get("exchange") == "B3":
        if B3_FRACTIONAL_TICKER_RE.fullmatch(ticker):
            return True
        if B3_DEPOSITARY_TICKER_RE.fullmatch(ticker):
            return True
        if B3_UNIT_TICKER_RE.fullmatch(ticker):
            return True
    if row.get("exchange") == "PSX":
        if PSX_CORPORATE_ACTION_TICKER_RE.fullmatch(ticker):
            return True
        if PSX_GOVERNMENT_SECURITY_TICKER_RE.fullmatch(ticker):
            return True
        if PSX_RIGHTS_TICKER_RE.fullmatch(ticker) and ("(r)" in name or " rights" in name):
            return True
    if row.get("exchange") == "KRX":
        aliases = split_aliases(row.get("aliases", ""))
        if (
            ticker.isdigit()
            and normalized_compact(row.get("name", "")) == normalized_compact(ticker)
            and not row.get("isin")
            and not row.get("sector")
            and (not aliases or {normalized_compact(alias) for alias in aliases} <= {normalized_compact(ticker)})
        ):
            return True
        if "strangle" in name:
            return True
    if has_matching_krx_base_listing(row, stock_name_lookup):
        return True
    if row.get("exchange") == "ASX":
        if ASX_CAPITAL_NOTE_TICKER_RE.fullmatch(ticker):
            return True
        if ASX_LOYALTY_TICKER_RE.fullmatch(ticker):
            return True
        if "deferred settlement" in name or name.endswith("deferred"):
            return True
    if row.get("exchange") in {"TWSE", "TPEX"} and TAIWAN_NON_COMMON_STOCK_TICKER_RE.fullmatch(ticker):
        return True
    if row.get("exchange") in {"AMS", "Euronext"} and EUROPEAN_CERTIFICATE_PATTERN.search(name):
        return True
    if row.get("exchange") in {"TSX", "TSXV"} and TMX_WARRANT_TICKER_RE.fullmatch(ticker):
        return True
    if row["ticker"].count("-P-"):
        return True
    if PREFERRED_PF_TICKER_RE.fullmatch(ticker):
        return True
    if PREFERRED_PR_TICKER_RE.fullmatch(ticker):
        return True
    if PREFERRED_PREF_TICKER_RE.fullmatch(ticker):
        return True
    if PREFERRED_TICKER_SUFFIX_RE.fullmatch(ticker):
        return True
    if row.get("exchange") in US_EXCHANGES and US_SERIES_PREFERRED_TICKER_RE.fullmatch(ticker):
        if has_matching_base_listing(row, stock_base_name_index):
            return True
    if is_depositary_row(row):
        return True
    if PREFERRED_PATTERN.search(name):
        return True
    return any(pattern.search(name) for pattern in NON_COMMON_PATTERNS)


@lru_cache(maxsize=None)
def load_active_official_set_stock_reference_names() -> dict[str, str]:
    if not MASTERFILE_REFERENCE_CSV.exists():
        return {}

    names: dict[str, str] = {}
    with MASTERFILE_REFERENCE_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("official") != "true":
                continue
            if row.get("listing_status") != "active":
                continue
            if row.get("exchange") != "SET":
                continue
            if row.get("asset_type") != "Stock":
                continue
            if row.get("source_key") != "set_listed_companies":
                continue
            ticker = row.get("ticker", "").strip().upper()
            name = row.get("name", "").strip()
            if ticker and name:
                names[ticker] = name
    return names


@lru_cache(maxsize=None)
def load_active_official_set_dr_tickers() -> frozenset[str]:
    if not MASTERFILE_REFERENCE_CSV.exists():
        return frozenset()

    tickers: set[str] = set()
    with MASTERFILE_REFERENCE_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("official") != "true":
                continue
            if row.get("listing_status") != "active":
                continue
            if row.get("exchange") != "SET":
                continue
            if row.get("source_key") != "set_dr_search":
                continue
            ticker = row.get("ticker", "").strip().upper()
            if ticker:
                tickers.add(ticker)
    return frozenset(tickers)


def has_matching_set_base_reference(row: dict[str, str]) -> bool:
    if row.get("exchange") != "SET" or row.get("asset_type") != "Stock":
        return False

    ticker = row.get("ticker", "").strip().upper()
    if not ticker.endswith("-R"):
        return False

    base_name = load_active_official_set_stock_reference_names().get(ticker[:-2])
    if not base_name:
        return False

    current_name = row.get("name", "")
    return alias_matches_company(current_name, base_name) or alias_matches_company(base_name, current_name)


def should_exclude_row(
    row: dict[str, str],
    stock_base_name_index: dict[str, set[str]] | None = None,
    stock_name_lookup: dict[str, set[str]] | None = None,
) -> bool:
    exchange = row.get("exchange")
    ticker = row.get("ticker", "").strip().upper()
    asset_type = row.get("asset_type")
    name = row.get("name", "").lower()

    if exchange == "SET":
        if ticker.startswith("^"):
            return True
        if ticker in load_active_official_set_dr_tickers():
            return True
        if has_matching_set_base_reference(row):
            return True

    if asset_type == "Stock" and exchange == "SSE" and ticker.startswith("508"):
        return True

    if asset_type == "Stock" and exchange == "SZSE" and ticker.startswith("180"):
        return True

    if asset_type == "ETF" and exchange in {"SSE", "SZSE"} and "lof" in name:
        return True

    return should_exclude_stock_row(row, stock_base_name_index, stock_name_lookup)


def normalize_input_row(row: dict[str, str]) -> dict[str, str]:
    normalized = dict(row)
    exchange = normalized.get("exchange", "")
    ticker = normalized.get("ticker", "").strip().upper()
    name = normalized.get("name", "")
    if normalized.get("asset_type") == "ETF" and exchange == "NEO" and NEO_CDR_XDR_RE.search(name):
        normalized["asset_type"] = "Stock"
        return normalized
    if (
        normalized.get("asset_type") == "Stock"
        and any(pattern.search(name) for pattern in ETP_NAME_PATTERNS)
    ):
        normalized["asset_type"] = "ETF"
    if normalized.get("asset_type") == "Stock" and exchange in {"TWSE", "TPEX"}:
        lowered_name = name.lower()
        if (
            TAIWAN_ETF_TICKER_RE.fullmatch(ticker)
            or "reit" in lowered_name
            or "real estate investment trust" in lowered_name
        ):
            normalized["asset_type"] = "ETF"
    return normalized


def is_code_like_name(name: str, ticker: str) -> bool:
    return normalized_compact(name) == normalized_compact(ticker)


def generic_fund_wrapper_match(name: str) -> str | None:
    lowered = " ".join(name.lower().split())
    if lowered in GENERIC_ETF_PLACEHOLDER_NAMES_EXACT:
        return "exact"
    for wrapper in GENERIC_FUND_WRAPPER_NAMES:
        if lowered == wrapper:
            return "exact"
        if (
            lowered.startswith(f"{wrapper} ")
            or lowered.startswith(f"{wrapper} -")
            or lowered.startswith(f"{wrapper}:")
        ):
            return "prefixed"
    return None


@lru_cache(maxsize=None)
def load_active_official_reference_rows() -> dict[tuple[str, str], tuple[dict[str, str], ...]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    if not MASTERFILE_REFERENCE_CSV.exists():
        return {}

    with MASTERFILE_REFERENCE_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("official") != "true":
                continue
            if row.get("listing_status") != "active":
                continue
            if row.get("reference_scope") in {"", "manual"}:
                continue
            grouped[(row["ticker"], row["asset_type"])].append(row)

    return {
        key: tuple(
            sorted(
                rows,
                key=lambda candidate: (
                    int(candidate.get("reference_scope") == "exchange_directory"),
                    len(candidate.get("name", "")),
                    candidate.get("source_key", ""),
                ),
                reverse=True,
            )
        )
        for key, rows in grouped.items()
    }


@lru_cache(maxsize=None)
def load_active_official_isin_fallbacks() -> dict[tuple[str, str, str], str]:
    grouped: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    if not MASTERFILE_REFERENCE_CSV.exists():
        return {}

    with MASTERFILE_REFERENCE_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("official") != "true":
                continue
            if row.get("listing_status") != "active":
                continue
            if row.get("reference_scope") in {"", "manual"}:
                continue
            isin = row.get("isin", "").strip().upper()
            if not is_valid_isin(isin):
                continue
            grouped[(row["ticker"], row["exchange"], row["asset_type"])].add(isin)

    return {
        key: next(iter(isins))
        for key, isins in grouped.items()
        if len(isins) == 1
    }


def choose_preferred_official_reference_row(
    rows: tuple[dict[str, str], ...],
    current_name: str,
) -> dict[str, str]:
    matching = [
        row
        for row in rows
        if alias_matches_company(row.get("name", ""), current_name)
        or alias_matches_company(current_name, row.get("name", ""))
    ]
    return matching[0] if matching else rows[0]


def should_correct_to_official_exchange(
    row: dict[str, str],
    official_row: dict[str, str],
) -> bool:
    current_exchange = row["exchange"]
    target_exchange = official_row["exchange"]
    if current_exchange == target_exchange:
        return False

    safe_name_match = (
        alias_matches_company(row["name"], official_row["name"])
        or alias_matches_company(official_row["name"], row["name"])
    )
    code_like_name = is_code_like_name(row["name"], row["ticker"])
    wrapper_match = generic_fund_wrapper_match(row["name"])

    if row["asset_type"] == "ETF":
        return bool(
            current_exchange in PRIMARY_VENUE_CORRECTION_EXCHANGES
            and target_exchange in US_EXCHANGES
            and (safe_name_match or code_like_name or wrapper_match)
        )

    if {current_exchange, target_exchange} <= KOREAN_STOCK_EXCHANGES:
        return bool(
            row["asset_type"] == "Stock"
            and official_row.get("source_key") == "krx_listed_companies"
        )

    if target_exchange in US_EXCHANGES:
        return bool(
            current_exchange in PRIMARY_VENUE_CORRECTION_EXCHANGES
            and (safe_name_match or code_like_name)
        )

    if target_exchange in {"TSX", "TSXV"}:
        return bool(
            current_exchange in US_EXCHANGES
            and row.get("isin", "").startswith("CA")
            and safe_name_match
        )

    return False


def apply_official_exchange_corrections(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    reference_rows = load_active_official_reference_rows()
    if not reference_rows:
        return rows

    occupied_listing_keys = {row_listing_key(row) for row in rows}
    corrected_rows: list[dict[str, str]] = []

    for row in rows:
        key = (row["ticker"], row["asset_type"])
        candidates = reference_rows.get(key, ())
        if not candidates:
            corrected_rows.append(row)
            continue

        same_exchange_candidates = tuple(candidate for candidate in candidates if candidate["exchange"] == row["exchange"])
        wrapper_match = generic_fund_wrapper_match(row["name"])
        if same_exchange_candidates and (is_code_like_name(row["name"], row["ticker"]) or wrapper_match):
            preferred_same_exchange = choose_preferred_official_reference_row(same_exchange_candidates, row["name"])
            corrected = dict(row)
            corrected["name"] = preferred_same_exchange["name"]
            corrected_rows.append(corrected)
            continue

        official_row: dict[str, str] | None = None
        target_exchanges = {candidate["exchange"] for candidate in candidates}
        if len(target_exchanges) == 1:
            official_row = choose_preferred_official_reference_row(candidates, row["name"])
        elif row["asset_type"] == "ETF":
            us_candidates = [candidate for candidate in candidates if candidate["exchange"] in US_EXCHANGES]
            if row["exchange"] in OTC_EXCHANGES:
                preferred_us_candidates = [
                    candidate
                    for candidate in us_candidates
                    if candidate.get("source_key") in {"nasdaq_listed", "nasdaq_other_listed"}
                ]
                if preferred_us_candidates:
                    us_candidates = preferred_us_candidates
            us_target_exchanges = {candidate["exchange"] for candidate in us_candidates}
            if len(us_target_exchanges) == 1:
                official_row = choose_preferred_official_reference_row(tuple(us_candidates), row["name"])

        if not official_row:
            corrected_rows.append(row)
            continue

        if not should_correct_to_official_exchange(row, official_row):
            corrected_rows.append(row)
            continue

        corrected = dict(row)
        corrected["exchange"] = official_row["exchange"]
        if (
            is_code_like_name(row["name"], row["ticker"])
            or wrapper_match
        ):
            corrected["name"] = official_row["name"]

        old_listing_key = row_listing_key(row)
        new_listing_key = row_listing_key(corrected)
        if new_listing_key in occupied_listing_keys and new_listing_key != old_listing_key:
            corrected_rows.append(row)
            continue

        occupied_listing_keys.discard(old_listing_key)
        occupied_listing_keys.add(new_listing_key)
        corrected_rows.append(corrected)

    return corrected_rows


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
    return any(alias.lower() in BAD_OBVIOUS_AMBIGUOUS_ALIASES for alias in aliases)


def looks_like_external_symbol_alias(alias: str) -> bool:
    compact = re.sub(r"[^A-Z0-9]+", "", alias.upper())
    if EXCHANGE_TICKER_RE.fullmatch(alias.upper()):
        return True
    return bool(
        compact
        and re.fullmatch(r"[A-Z]{4,5}", compact)
        and compact.endswith(("F", "R", "U", "W", "Y"))
    )


def build_alias_context(
    rows: list[dict[str, str]],
    identifier_lookup: dict[tuple[str, str], dict[str, str]],
) -> dict[str, dict[str, set[str]]]:
    entity_tickers: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        entity_tickers[entity_key_for_row(row)].add(row["ticker"].upper())

    alias_context: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {
            "entities": set(),
            "matching_entities": set(),
            "ticker_owner_entities": set(),
        }
    )

    for row in rows:
        entity_key = entity_key_for_row(row)
        identifier = identifier_lookup.get((row["ticker"], row["exchange"]), {"wkn": ""})
        wkns = {identifier["wkn"]} if identifier.get("wkn") else set()
        current_isin = MANUAL_ISIN_CORRECTIONS.get(row["ticker"], row.get("isin", ""))

        for alias in dedupe_keep_order(row["aliases"]):
            if looks_like_identifier(alias, wkns, current_isin):
                continue

            alias_key = alias.lower()
            alias_context[alias_key]["entities"].add(entity_key)
            if alias_matches_company(alias, row["name"]):
                alias_context[alias_key]["matching_entities"].add(entity_key)
            if alias.upper() in entity_tickers[entity_key]:
                alias_context[alias_key]["ticker_owner_entities"].add(entity_key)

    return dict(alias_context)


def should_drop_contextual_alias(
    row: dict[str, str],
    alias: str,
    alias_context: dict[str, dict[str, set[str]]] | None,
) -> bool:
    if not alias_context:
        return False

    context = alias_context.get(alias.lower())
    if not context or len(context["entities"]) < 2:
        return False

    entity_key = entity_key_for_row(row)
    if context["matching_entities"]:
        return entity_key not in context["matching_entities"]
    if context["ticker_owner_entities"]:
        return entity_key not in context["ticker_owner_entities"]
    if is_blocked_alias(alias) or looks_like_external_symbol_alias(alias):
        return True
    if not is_trusted_non_lexical_alias(alias, row["name"]):
        return True
    return False


def clean_aliases(
    row: dict[str, str],
    aliases: list[str],
    wkns: set[str],
    alias_context: dict[str, dict[str, set[str]]] | None = None,
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
        if is_blocked_alias(alias):
            continue
        if has_wrapper_term(alias):
            continue
        if should_drop_contextual_alias(row, alias, alias_context):
            continue
        if lowered.startswith("1x "):
            continue
        # Skip very short name aliases (<=2 chars) -- too ambiguous
        if len(alias) <= 2 and not looks_like_identifier(alias, wkns, cleaned_isin):
            continue
        # Keep strict numeric-namespace codes, but they will be typed as
        # exchange_ticker later instead of ambiguous name aliases.
        if is_numeric_exchange_alias(row, alias, wkns, cleaned_isin):
            cleaned_aliases.append(alias)
            continue
        # Skip pure-numeric aliases that aren't identifiers (WKN/ISIN)
        if alias.isdigit() and alias not in wkns and alias != cleaned_isin:
            continue
        if (
            looks_like_external_symbol_alias(alias)
            and not looks_like_identifier(alias, wkns, cleaned_isin)
            and not alias_matches_company(alias, row["name"])
        ):
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
    source_path = LISTINGS_CSV if LISTINGS_CSV.exists() else TICKERS_CSV
    with source_path.open(newline="") as handle:
        ticker_rows = list(csv.DictReader(handle))
    core_row_keys = {(row["ticker"], row["exchange"]) for row in ticker_rows}

    with ALIASES_CSV.open(newline="") as handle:
        alias_rows = list(csv.DictReader(handle))

    identifier_path = IDENTIFIERS_EXTENDED_CSV if IDENTIFIERS_EXTENDED_CSV.exists() else IDENTIFIERS_CSV
    with identifier_path.open(newline="") as handle:
        identifier_rows = list(csv.DictReader(handle))

    alias_type_lookup: dict[tuple[str, str], str] = {}
    extra_aliases: dict[str, list[str]] = defaultdict(list)
    for row in alias_rows:
        key = (row["ticker"], row["alias"])
        alias_type_lookup.setdefault(key, row["alias_type"])
        if row["alias_type"] == "exchange_ticker":
            extra_aliases[row["ticker"]].append(row["alias"])

    identifier_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in identifier_rows:
        key = (row["ticker"], row.get("exchange", ""))
        identifier_lookup[key] = {"isin": row.get("isin", ""), "wkn": row.get("wkn", "")}

    ticker_rows = merge_supplemental_ticker_rows(ticker_rows)

    return ticker_rows, alias_type_lookup, extra_aliases, identifier_lookup, core_row_keys


def load_supplemental_ticker_rows() -> list[dict[str, str]]:
    if not MASTERFILE_SUPPLEMENT_CSV.exists():
        return []
    with MASTERFILE_SUPPLEMENT_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def merge_ticker_row(base_row: dict[str, str], supplement_row: dict[str, str]) -> dict[str, str]:
    merged = dict(base_row)
    for field in ("name", "asset_type", "country", "country_code"):
        if supplement_row.get(field):
            merged[field] = supplement_row[field]
    for field in ("sector", "isin"):
        if not merged.get(field) and supplement_row.get(field):
            merged[field] = supplement_row[field]
    merged_aliases = dedupe_keep_order(
        split_aliases(base_row.get("aliases", "")) + split_aliases(supplement_row.get("aliases", ""))
    )
    merged["aliases"] = "|".join(merged_aliases)
    return merged


def merge_supplemental_ticker_rows(base_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    supplemental_rows = load_supplemental_ticker_rows()
    tmx_official_aliases: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in supplemental_rows:
        if row["exchange"] not in {"TSX", "TSXV"}:
            continue
        match = TMX_OFFICIAL_SERIES_TICKER_RE.fullmatch(row["ticker"])
        if not match:
            continue
        hyphenated_ticker = f"{match.group('root')}-{match.group('suffix')}"
        tmx_official_aliases[(hyphenated_ticker, row["exchange"], row["asset_type"])] = row

    merged_rows: dict[tuple[str, str], dict[str, str]] = {}
    for row in base_rows:
        candidate = dict(row)
        official_alias = tmx_official_aliases.get((candidate["ticker"], candidate["exchange"], candidate["asset_type"]))
        if official_alias and (
            not candidate.get("name")
            or not official_alias.get("name")
            or alias_matches_company(candidate["name"], official_alias["name"])
            or alias_matches_company(official_alias["name"], candidate["name"])
        ):
            candidate["ticker"] = official_alias["ticker"]
            if "listing_key" in candidate:
                candidate["listing_key"] = row_listing_key(candidate)
        key = (candidate["ticker"], candidate["exchange"])
        if key in merged_rows:
            merged_rows[key] = merge_ticker_row(merged_rows[key], candidate)
        else:
            merged_rows[key] = candidate

    for row in supplemental_rows:
        key = (row["ticker"], row["exchange"])
        if key in merged_rows:
            merged_rows[key] = merge_ticker_row(merged_rows[key], row)
        else:
            merged_rows[key] = {
                "ticker": row["ticker"],
                "name": row["name"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "sector": row.get("sector", ""),
                "country": row.get("country", ""),
                "country_code": row.get("country_code", ""),
                "isin": row.get("isin", ""),
                "aliases": row.get("aliases", ""),
            }
    return list(merged_rows.values())


def load_review_overrides():
    remove_aliases: dict[tuple[str, str], set[str]] = defaultdict(set)
    metadata_updates: dict[tuple[str, str], dict[str, dict[str, str]]] = defaultdict(dict)
    drop_entries: set[tuple[str, str]] = set()

    if REVIEW_REMOVE_ALIASES_CSV.exists():
        with REVIEW_REMOVE_ALIASES_CSV.open(newline="") as handle:
            for row in csv.DictReader(handle):
                remove_aliases[(row["ticker"], row["exchange"])].add(row["alias"])

    if REVIEW_METADATA_UPDATES_CSV.exists():
        with REVIEW_METADATA_UPDATES_CSV.open(newline="") as handle:
            for row in csv.DictReader(handle):
                metadata_updates[(row["ticker"], row["exchange"])][row["field"]] = {
                    "decision": row["decision"],
                    "proposed_value": row.get("proposed_value", ""),
                }

    if REVIEW_DROP_ENTRIES_CSV.exists():
        with REVIEW_DROP_ENTRIES_CSV.open(newline="") as handle:
            for row in csv.DictReader(handle):
                drop_entries.add((row["ticker"], row["exchange"]))

    return remove_aliases, metadata_updates, drop_entries


def apply_input_metadata_overrides(
    row: dict[str, str],
    field_overrides: dict[str, dict[str, str]],
) -> dict[str, str]:
    updated = dict(row)
    for field, override in field_overrides.items():
        if field == "country_code":
            continue
        if field == "aliases":
            if override["decision"] == "clear":
                updated["aliases"] = []
            elif override["decision"] == "update":
                updated["aliases"] = split_aliases(override.get("proposed_value", ""))
            continue
        if field not in updated:
            continue
        if override["decision"] == "clear":
            updated[field] = ""
        elif override["decision"] == "update":
            updated[field] = override.get("proposed_value", "")
    return updated


def apply_output_metadata_overrides(
    row: dict[str, str],
    field_overrides: dict[str, dict[str, str]],
) -> dict[str, str]:
    updated = dict(row)
    for field, override in field_overrides.items():
        if field == "country_code":
            if override["decision"] == "clear":
                updated["country_code"] = ""
            elif override["decision"] == "update":
                updated["country_code"] = override.get("proposed_value", "")
            continue
        if field == "aliases":
            if override["decision"] == "clear":
                updated["aliases"] = []
            elif override["decision"] == "update":
                updated["aliases"] = split_aliases(override.get("proposed_value", ""))
            continue
        if field not in updated:
            continue
        if override["decision"] == "clear":
            updated[field] = ""
        elif override["decision"] == "update":
            updated[field] = override.get("proposed_value", "")

    if "country" in field_overrides and "country_code" not in field_overrides:
        updated["country_code"] = COUNTRY_TO_ISO.get(updated["country"], "")
    return updated


def cleaned_rows():
    ticker_rows, alias_type_lookup, extra_aliases, identifier_lookup, core_row_keys = load_data()
    review_alias_removals, review_metadata_updates, review_drop_entries = load_review_overrides()
    official_isin_fallbacks = load_active_official_isin_fallbacks()

    prepared_rows: list[dict[str, str]] = []
    for row in ticker_rows:
        row_key = (row["ticker"], row["exchange"])
        if row_key in review_drop_entries:
            continue
        merged = normalize_input_row(row)
        merged = apply_input_metadata_overrides(merged, review_metadata_updates.get(row_key, {}))
        prepared_rows.append(merged)

    prepared_rows = apply_official_exchange_corrections(prepared_rows)
    prepared_rows = drop_stale_tmx_etf_duplicates(prepared_rows)
    stock_base_name_index = build_stock_base_name_index(prepared_rows)
    stock_name_lookup = build_stock_name_lookup(prepared_rows)

    base_rows: list[dict[str, str]] = []
    for row in prepared_rows:
        row_key = (row["ticker"], row["exchange"])
        if should_exclude_row(row, stock_base_name_index, stock_name_lookup):
            continue
        merged_aliases = split_aliases(row["aliases"])
        identifier = None
        if row_key in core_row_keys:
            merged_aliases.extend(extra_aliases.get(row["ticker"], []))
            identifier = identifier_lookup.get(row_key) or identifier_lookup.get((row["ticker"], ""))
        alias_removals = review_alias_removals.get(row_key, set())
        if alias_removals:
            merged_aliases = [alias for alias in merged_aliases if alias not in alias_removals]
        if identifier and identifier["wkn"]:
            merged_aliases.append(identifier["wkn"])
        merged = dict(row)
        if not merged.get("isin") and "isin" not in review_metadata_updates.get(row_key, {}):
            merged["isin"] = official_isin_fallbacks.get((row["ticker"], row["exchange"], row["asset_type"]), "")
        merged["aliases"] = merged_aliases
        base_rows.append(merged)

    base_rows = cleanse_conflicting_isin_rows(base_rows)

    alias_context = build_alias_context(base_rows, identifier_lookup)

    output_rows: list[dict[str, str]] = []
    for row in base_rows:
        row_key = (row["ticker"], row["exchange"])
        aliases = dedupe_keep_order(row["aliases"])
        identifier = identifier_lookup.get(row_key) or identifier_lookup.get((row["ticker"], ""), {"wkn": ""})
        wkns = {identifier["wkn"]} if identifier.get("wkn") else set()
        if is_namespace_collision_row(row, aliases, wkns):
            continue
        isin, aliases = clean_aliases(row, aliases, wkns, alias_context)
        country = row["country"]
        inferred_country = country_from_isin(isin) if isin else None
        if inferred_country and inferred_country != country:
            if row["exchange"] in OTC_EXCHANGES and country == "United States":
                country = inferred_country
            else:
                isin = ""
                inferred_country = None
        if inferred_country:
            country = inferred_country

        output_row = {
            "ticker": row["ticker"],
            "name": row["name"],
            "exchange": row["exchange"],
            "asset_type": row["asset_type"],
            "sector": normalize_sector(row["sector"], row["asset_type"]),
            "country": country,
            "country_code": COUNTRY_TO_ISO.get(country, ""),
            "isin": isin,
            "aliases": aliases,
            "wkn": identifier.get("wkn", ""),
        }
        output_rows.append(
            apply_output_metadata_overrides(output_row, review_metadata_updates.get((row["ticker"], row["exchange"]), {}))
        )

    return sorted(output_rows, key=lambda row: row["ticker"]), alias_type_lookup


def cleanse_conflicting_isin_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows_by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("isin"):
            rows_by_isin[row["isin"]].append(row)

    for isin, peers in rows_by_isin.items():
        issuer_families = {
            tuple(sorted(normalize_tokens(peer["name"]))) or (normalized_compact(peer["name"]),)
            for peer in peers
        }
        if len(peers) < 2 or len(issuer_families) < 2:
            continue

        inferred_country = country_from_isin(isin)
        for row in peers:
            cleaned_aliases: list[str] = []
            contaminated = False
            aliases_changed = False
            for alias in row["aliases"]:
                matches_own = alias_matches_company(alias, row["name"])
                matches_peer = any(
                    alias_matches_company(alias, peer["name"]) for peer in peers if peer is not row
                )
                if (
                    not matches_own
                    and not matches_peer
                    and not is_trusted_non_lexical_alias(alias, row["name"])
                    and not looks_like_identifier(alias, set(), isin)
                ):
                    aliases_changed = True
                    continue
                if matches_peer and not matches_own:
                    contaminated = True
                    aliases_changed = True
                    continue
                cleaned_aliases.append(alias)

            if aliases_changed:
                row["aliases"] = cleaned_aliases

            if not contaminated:
                continue

            row["isin"] = ""
            if inferred_country and row.get("country") == inferred_country:
                row["country"] = ""
                row["country_code"] = ""

    return rows


def has_exact_same_exchange_official_reference(
    row: dict[str, str],
    reference_rows: dict[tuple[str, str], tuple[dict[str, str], ...]],
) -> bool:
    candidates = reference_rows.get((row["ticker"], row["asset_type"]), ())
    return any(candidate.get("exchange") == row["exchange"] for candidate in candidates)


def drop_stale_tmx_etf_duplicates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    reference_rows = load_active_official_reference_rows()
    if not reference_rows:
        return rows

    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row.get("asset_type") != "ETF" or row.get("exchange") not in {"TSX", "TSXV"}:
            continue
        fingerprint = row_name_fingerprint(row) or normalized_compact(row.get("name", ""))
        if not fingerprint:
            continue
        grouped[(row["exchange"], fingerprint)].append(row)

    stale_listing_keys: set[str] = set()
    for peers in grouped.values():
        if len(peers) < 2:
            continue
        exact_official_peers = [
            row for row in peers if has_exact_same_exchange_official_reference(row, reference_rows)
        ]
        if not exact_official_peers:
            continue
        for row in peers:
            if row in exact_official_peers:
                continue
            stale_listing_keys.add(row_listing_key(row))

    return [row for row in rows if row_listing_key(row) not in stale_listing_keys]


def build_alias_rows(rows: list[dict[str, str]], alias_type_lookup: dict[tuple[str, str], str]):
    alias_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        wkns = {row["wkn"]} if row["wkn"] else set()
        all_aliases = []
        if row["isin"]:
            all_aliases.append((row["isin"], "isin"))
        for alias in row["aliases"]:
            if is_numeric_exchange_alias(row, alias, wkns, row["isin"]):
                alias_type = "exchange_ticker"
            elif (row["ticker"], alias) in alias_type_lookup:
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


VERSION_FILE = ROOT / "VERSION"


@lru_cache(maxsize=None)
def get_version() -> str:
    return VERSION_FILE.read_text(encoding="utf-8").strip()


def write_json(rows: list[dict[str, str]]):
    meta = {
        "version": get_version(),
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_tickers": len(rows),
    }
    payload = [
        {
            "ticker": row["ticker"],
            "name": row["name"],
            "exchange": row["exchange"],
            "asset_type": row["asset_type"],
            "sector": row["sector"],
            "country": row["country"],
            "country_code": row["country_code"],
            "isin": row["isin"],
            "aliases": row["aliases"],
        }
        for row in rows
    ]
    envelope = {"_meta": meta, "tickers": payload}
    TICKERS_JSON.write_text(
        json.dumps(envelope, separators=(",", ":")),
        encoding="utf-8",
    )


def build_listing_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "listing_key": row_listing_key(row),
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "name": row["name"],
            "asset_type": row["asset_type"],
            "sector": row["sector"],
            "country": row["country"],
            "country_code": row["country_code"],
            "isin": row["isin"],
            "aliases": "|".join(row["aliases"]),
        }
        for row in rows
    ]


def write_db(
    rows: list[dict[str, str]],
    listing_rows: list[dict[str, str]],
    alias_rows: list[dict[str, str]],
    cross_listing_rows: list[dict[str, str]] | None = None,
):
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
                country_code TEXT DEFAULT '',
                isin TEXT DEFAULT ''
            );

            CREATE TABLE listings (
                listing_key TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                exchange TEXT NOT NULL,
                name TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                sector TEXT DEFAULT '',
                country TEXT DEFAULT '',
                country_code TEXT DEFAULT '',
                isin TEXT DEFAULT '',
                aliases TEXT DEFAULT ''
            );

            CREATE TABLE aliases (
                ticker TEXT NOT NULL,
                alias TEXT NOT NULL,
                alias_type TEXT NOT NULL,
                PRIMARY KEY (ticker, alias),
                FOREIGN KEY (ticker) REFERENCES tickers(ticker) ON DELETE CASCADE
            );

            CREATE TABLE cross_listings (
                isin TEXT NOT NULL,
                listing_key TEXT NOT NULL,
                ticker TEXT NOT NULL,
                exchange TEXT NOT NULL,
                is_primary INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (isin, listing_key),
                FOREIGN KEY (listing_key) REFERENCES listings(listing_key) ON DELETE CASCADE
            );

            CREATE INDEX idx_aliases_alias ON aliases(alias);
            CREATE INDEX idx_listings_ticker ON listings(ticker);
            CREATE INDEX idx_listings_exchange ON listings(exchange);
            CREATE INDEX idx_listings_isin ON listings(isin);
            CREATE INDEX idx_tickers_exchange ON tickers(exchange);
            CREATE INDEX idx_tickers_isin ON tickers(isin);
            CREATE INDEX idx_tickers_sector ON tickers(sector);
            CREATE INDEX idx_cross_listings_isin ON cross_listings(isin);
            """
        )
        conn.executemany(
            """
            INSERT INTO tickers (ticker, name, exchange, asset_type, sector, country, country_code, isin)
            VALUES (:ticker, :name, :exchange, :asset_type, :sector, :country, :country_code, :isin)
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT INTO listings (listing_key, ticker, exchange, name, asset_type, sector, country, country_code, isin, aliases)
            VALUES (:listing_key, :ticker, :exchange, :name, :asset_type, :sector, :country, :country_code, :isin, :aliases)
            """,
            listing_rows,
        )
        conn.executemany(
            """
            INSERT INTO aliases (ticker, alias, alias_type)
            VALUES (:ticker, :alias, :alias_type)
            """,
            alias_rows,
        )
        if cross_listing_rows:
            conn.executemany(
                """
                INSERT INTO cross_listings (isin, listing_key, ticker, exchange, is_primary)
                VALUES (:isin, :listing_key, :ticker, :exchange, :is_primary)
                """,
                cross_listing_rows,
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
                "country_code": row["country_code"],
                "isin": row["isin"],
                "aliases": row["aliases"],
            }
            for row in rows
        ]
    )
    frame.to_parquet(TICKERS_PARQUET, index=False)


# ---------------------------------------------------------------------------
# Cross-listings
# ---------------------------------------------------------------------------
# Exchange-to-ISIN-prefix mapping for detecting "home" exchange.
EXCHANGE_ISIN_PREFIX: dict[str, str] = {
    "AMS": "NL", "ASX": "AU", "ATHEX": "GR", "B3": "BR", "BCBA": "AR",
    "BME": "ES", "BMV": "MX", "BSE_BW": "BW", "BSE_HU": "HU", "BVB": "RO",
    "BVL": "PT", "Bursa": "MY", "CPH": "DK", "CSE_LK": "LK", "CSE_MA": "MA",
    "DSE_TZ": "TZ", "EGX": "EG", "Euronext": "FR", "GSE": "GH", "HEL": "FI",
    "HOSE": "VN", "ICE_IS": "IS", "IDX": "ID", "ISE": "IE", "JSE": "ZA",
    "KOSDAQ": "KR", "KRX": "KR", "LSE": "GB", "LUSE": "ZM", "MSE_MW": "MW",
    "NASDAQ": "US", "NEO": "CA", "NGX": "NG", "NSE_KE": "KE",
    "NYSE": "US", "NYSE ARCA": "US", "NYSE MKT": "US", "NYSEARCH": "US",
    "OSL": "NO", "PSE": "PH", "PSE_CZ": "CZ", "PSX": "PK", "RSE": "RW",
    "SEM": "MU", "SET": "TH", "SIX": "CH", "SSE": "CN", "SSE_CL": "CL",
    "STO": "SE", "SZSE": "CN", "TASE": "IL", "TPEX": "TW", "TSX": "CA",
    "TSXV": "CA", "TWSE": "TW", "USE_UG": "UG", "VSE": "AT", "WSE": "PL",
    "XETRA": "DE", "ZSE": "HR", "ZSE_ZW": "ZW",
}

# Preference order: home exchange first, then major exchanges, then rest.
EXCHANGE_RANK: dict[str, int] = {
    "NYSE": 1, "NASDAQ": 2, "NYSE ARCA": 3, "LSE": 4, "XETRA": 5,
    "Euronext Paris": 6, "Euronext Amsterdam": 7, "TSX": 8, "ASX": 9,
    "HKEX": 10, "TSE": 11, "KRX": 12, "SSE": 13, "SZSE": 14, "B3": 15,
}


def cross_listing_sort_key(isin: str, row: dict[str, str]) -> tuple[int, int, str, str]:
    exchange = row["exchange"]
    is_home = 1 if EXCHANGE_ISIN_PREFIX.get(exchange) == isin[:2] else 0
    rank = EXCHANGE_RANK.get(exchange, 99)
    return (-is_home, rank, row["ticker"], exchange)


def build_primary_ticker_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Collapse cross-listed securities to a single primary row in the core export."""
    primary_listing_key_by_isin: dict[str, str] = {}
    isin_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row["isin"]:
            isin_groups[row["isin"]].append(row)

    for isin, group in isin_groups.items():
        preferred = sorted(group, key=lambda candidate: cross_listing_sort_key(isin, candidate))[0]
        primary_listing_key_by_isin[isin] = row_listing_key(preferred)

    primary_rows: list[dict[str, str]] = []
    for row in rows:
        isin = row["isin"]
        if not isin:
            primary_rows.append(row)
            continue
        if row_listing_key(row) == primary_listing_key_by_isin[isin]:
            primary_rows.append(row)
    return primary_rows


def build_cross_listings(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Build cross-listing groups for ISINs shared by multiple tickers."""
    isin_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row["isin"]:
            isin_groups[row["isin"]].append(row)

    result: list[dict[str, str]] = []
    for isin, group in sorted(isin_groups.items()):
        if len(group) < 2:
            continue

        group_sorted = sorted(group, key=lambda candidate: cross_listing_sort_key(isin, candidate))
        primary_listing_key = row_listing_key(group_sorted[0])
        for row in group_sorted:
            result.append({
                "isin": isin,
                "listing_key": row_listing_key(row),
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "is_primary": "1" if row_listing_key(row) == primary_listing_key else "0",
            })

    return result


def rebuild():
    rows, alias_type_lookup = cleaned_rows()
    listing_rows = build_listing_rows(rows)
    primary_rows = build_primary_ticker_rows(rows)
    alias_rows = build_alias_rows(primary_rows, alias_type_lookup)
    identifier_rows = build_identifier_rows(primary_rows)
    cross_listing_rows = build_cross_listings(rows)

    write_csv(
        TICKERS_CSV,
        ["ticker", "name", "exchange", "asset_type", "sector", "country", "country_code", "isin", "aliases"],
        (
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "sector": row["sector"],
                "country": row["country"],
                "country_code": row["country_code"],
                "isin": row["isin"],
                "aliases": "|".join(row["aliases"]),
            }
            for row in primary_rows
        ),
    )
    write_csv(
        LISTINGS_CSV,
        ["listing_key", "ticker", "exchange", "name", "asset_type", "sector", "country", "country_code", "isin", "aliases"],
        listing_rows,
    )
    write_csv(ALIASES_CSV, ["ticker", "alias", "alias_type"], alias_rows)
    write_csv(IDENTIFIERS_CSV, ["ticker", "isin", "wkn"], identifier_rows)
    write_csv(
        CROSS_LISTINGS_CSV,
        ["isin", "listing_key", "ticker", "exchange", "is_primary"],
        cross_listing_rows,
    )
    write_json(primary_rows)
    write_db(primary_rows, listing_rows, alias_rows, cross_listing_rows)
    write_parquet(primary_rows)

    stats = {
        "tickers": len(primary_rows),
        "stocks": sum(1 for row in primary_rows if row["asset_type"] == "Stock"),
        "etfs": sum(1 for row in primary_rows if row["asset_type"] == "ETF"),
        "exchanges": len({row["exchange"] for row in primary_rows if row["exchange"]}),
        "countries": len({row["country"] for row in primary_rows if row["country"]}),
        "isin_coverage": sum(1 for row in primary_rows if row["isin"]),
        "sector_coverage": sum(1 for row in primary_rows if row["sector"]),
        "aliases": len(alias_rows),
    }
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    rebuild()
