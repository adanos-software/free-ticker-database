from __future__ import annotations

import html
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass


IDENTIFIER_RE = re.compile(r"^[A-Z0-9]{5,12}$")
EXCHANGE_TICKER_RE = re.compile(r"^[A-Z0-9-]+\.[A-Z]{1,6}$")
TOKEN_RE = re.compile(r"[a-z0-9]+")

COMMON_SINGLE_WORD_ALIASES = {
    "a",
    "after",
    "all",
    "and",
    "any",
    "bad",
    "bank",
    "before",
    "best",
    "bill",
    "buy",
    "call",
    "calm",
    "camp",
    "capital",
    "car",
    "cash",
    "close",
    "cost",
    "day",
    "ever",
    "fast",
    "five",
    "fly",
    "for",
    "fund",
    "gold",
    "good",
    "group",
    "hard",
    "high",
    "hold",
    "hope",
    "income",
    "key",
    "kids",
    "last",
    "live",
    "long",
    "low",
    "market",
    "mind",
    "new",
    "next",
    "nice",
    "nine",
    "note",
    "old",
    "open",
    "pay",
    "pick",
    "play",
    "plus",
    "put",
    "rare",
    "road",
    "rock",
    "sea",
    "sell",
    "share",
    "short",
    "spot",
    "stock",
    "tax",
    "team",
    "time",
    "trust",
    "value",
    "way",
    "week",
}
COMMON_SINGLE_WORD_ALIASES.update(
    {
    "agent",
    "actions",
    "brave",
    "bimbo",
    "colorado",
    "conduit",
    "crops",
    "desktop",
    "delta",
    "energy",
    "elements",
    "figs",
    "focus",
    "frequency",
    "general",
    "geo",
    "grains",
    "green",
    "growth",
    "guru",
    "healthcare",
    "hella",
    "holdings",
    "human",
    "hyundai",
    "korea",
    "lottery",
    "media",
    "national",
    "nations",
    "members",
    "opportunities",
    "packages",
    "popular",
    "property",
    "science",
    "samsung",
    "starts",
    "target",
    "technologies",
    "tracker",
    "united",
    "vietnam",
    "winners",
    "wills",
    "world",
}
)

TRUSTED_SHORT_BRAND_ALIASES = {
    "3m",
    "amd",
    "asml",
    "bmw",
    "byd",
    "cibc",
    "ford",
    "hsbc",
    "ibm",
    "meta",
    "nike",
    "novo",
    "sony",
    "tsmc",
    "uber",
    "visa",
}

GENERIC_WRAPPER_PATTERNS = (
    re.compile(r"\b\d{4}\s+etf(?:\s+series)?\s+trust\b", re.IGNORECASE),
    re.compile(r"\betf\s+(?:series|opportunities|managers?)\s+trust\b", re.IGNORECASE),
    re.compile(r"\bexchange[- ]traded\s+fund(?:s)?\s+trust\b", re.IGNORECASE),
    re.compile(r"\b(?:funds?|series|portfolios?|shares?)\s+trust\b", re.IGNORECASE),
    re.compile(r"\b(?:active|managed)\s+etfs?\b", re.IGNORECASE),
)

GENERIC_WRAPPER_EXACT = {
    "alps etf trust",
    "amplify etf trust",
    "blackrock etf trust",
    "bondbloxx etf trust",
    "calamos etf trust",
    "direxion shares etf trust",
    "etf opportunities trust",
    "etf series solutions",
    "first trust exchange-traded fund",
    "first trust exchange-traded fund ii",
    "first trust exchange-traded fund vi",
    "global x funds",
    "invesco exchange-traded fund trust ii",
    "ishares trust",
    "listed funds trust",
    "pacer funds trust",
    "pgim etf trust",
    "pimco etf trust",
    "proshares trust",
    "schwab strategic trust",
    "tidal etf trust",
    "tidal trust ii",
    "truth social funds",
}

GENERIC_ORGANIZATION_EXACT = {
    "central bank",
}

GENERIC_MULTIWORD_ALIAS_EXACT = {
    "a capital",
    "ai infrastructure",
    "as one",
    "can do",
    "can one",
    "clean energy",
    "early age",
    "government bond",
    "greater than",
    "monthly income",
    "money market",
    "physical silver",
    "precious metals",
    "premium income",
    "renewable energy",
    "the beach",
    "the global",
    "the lottery",
    "tracker fund",
    "wealth management",
}


@dataclass(frozen=True)
class AliasDecision:
    status: str
    detection_policy: str
    confidence: str
    reason: str


def ascii_fold(value: str) -> str:
    value = value.translate(str.maketrans({"Ł": "L", "ł": "l"}))
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(character for character in normalized if not unicodedata.combining(character))


def normalize_alias_text(value: str) -> str:
    value = html.unescape(value)
    if any(marker in value for marker in ("Ã", "Â", "â", "�")):
        replacements = {
            "â‚¬": " euro ",
            "â€‹": " ",
            "â": "'",
            "â€™": "'",
            "â": "-",
            "â€“": "-",
            "Â": "",
        }
        for source, target in replacements.items():
            value = value.replace(source, target)
        try:
            value = value.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    value = re.sub(r"[®™©]", " ", value)
    value = value.replace("'", "")
    folded = ascii_fold(value)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii")
    ascii_only = re.sub(r"(?<=\w)tm\b", "", ascii_only, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", ascii_only.strip()).strip()


def normalize_natural_language_alias(value: str) -> str:
    return normalize_alias_text(value).lower()


def alias_tokens(value: str) -> list[str]:
    return TOKEN_RE.findall(ascii_fold(value).lower())


def compact_alias(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", ascii_fold(value).lower())


def is_security_identifier_alias(alias: str, *, isin: str = "", wkns: set[str] | None = None) -> bool:
    normalized = alias.strip().upper()
    wkns = wkns or set()
    if not normalized:
        return False
    if normalized == isin or normalized in wkns:
        return True
    return bool(IDENTIFIER_RE.fullmatch(normalized) and any(char.isdigit() for char in normalized))


def is_exchange_ticker_alias(alias: str) -> bool:
    return bool(EXCHANGE_TICKER_RE.fullmatch(alias.strip().upper()))


def is_generic_wrapper_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    if normalized in GENERIC_WRAPPER_EXACT:
        return True
    return any(pattern.search(normalized) for pattern in GENERIC_WRAPPER_PATTERNS)


def is_generic_organization_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    return normalized in GENERIC_ORGANIZATION_EXACT


def is_generic_multiword_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    return normalized in GENERIC_MULTIWORD_ALIAS_EXACT


def is_short_single_word_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    return bool(re.fullmatch(r"[a-z0-9&.+-]+", normalized) and len(compact_alias(normalized)) <= 4)


def is_common_single_word_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    return normalized in COMMON_SINGLE_WORD_ALIASES


def should_keep_short_alias(alias: str) -> bool:
    normalized = normalize_natural_language_alias(alias)
    return normalized in TRUSTED_SHORT_BRAND_ALIASES


def duplicate_alias_counts(alias_rows: list[dict[str, str]]) -> Counter[str]:
    alias_to_tickers: dict[str, set[str]] = {}
    for row in alias_rows:
        alias = row.get("alias", "")
        ticker = row.get("ticker", "")
        if not alias or not ticker:
            continue
        alias_to_tickers.setdefault(alias, set()).add(ticker)
    return Counter({alias: len(tickers) for alias, tickers in alias_to_tickers.items() if len(tickers) > 1})


def classify_alias_for_natural_language(
    *,
    alias: str,
    alias_type: str,
    ticker: str,
    duplicate_ticker_count: int = 1,
    isin: str = "",
    wkns: set[str] | None = None,
) -> AliasDecision:
    normalized = normalize_alias_text(alias)
    if not normalized:
        return AliasDecision("reject", "blocked", "0.99", "empty_alias")
    if alias_type in {"isin", "wkn"} or is_security_identifier_alias(normalized, isin=isin, wkns=wkns):
        return AliasDecision("reject", "identifier_only", "0.99", "identifier_alias")
    if alias_type == "exchange_ticker" or is_exchange_ticker_alias(normalized):
        return AliasDecision("review", "symbol_alias_only", "0.75", "exchange_ticker_alias")
    if duplicate_ticker_count > 1:
        return AliasDecision("review", "ambiguous_duplicate", "0.70", "alias_maps_to_multiple_tickers")
    if is_generic_wrapper_alias(normalized):
        return AliasDecision("reject", "blocked", "0.95", "generic_fund_or_trust_wrapper")
    if is_generic_organization_alias(normalized):
        return AliasDecision("reject", "blocked", "0.95", "generic_organization_alias")
    if is_generic_multiword_alias(normalized):
        return AliasDecision("reject", "blocked", "0.95", "generic_multiword_alias")
    if is_common_single_word_alias(normalized):
        return AliasDecision("reject", "blocked", "0.95", "common_word_alias")
    if is_short_single_word_alias(normalized) and not should_keep_short_alias(normalized):
        return AliasDecision("review", "context_required", "0.65", "short_single_word_alias")
    if ticker.upper() == normalized.upper():
        return AliasDecision("review", "symbol_alias_only", "0.80", "same_as_ticker")
    return AliasDecision("accept", "safe_natural_language", "0.90", "accepted_name_alias")


def should_drop_from_ticker_alias_column(
    *,
    alias: str,
    isin: str = "",
    wkns: set[str] | None = None,
) -> bool:
    normalized = normalize_alias_text(alias)
    if not normalized:
        return True
    if is_security_identifier_alias(normalized, isin=isin, wkns=wkns):
        return True
    if is_exchange_ticker_alias(normalized):
        return True
    if is_generic_wrapper_alias(normalized):
        return True
    if is_generic_organization_alias(normalized):
        return True
    if is_generic_multiword_alias(normalized):
        return True
    if is_common_single_word_alias(normalized):
        return True
    if is_short_single_word_alias(normalized) and not should_keep_short_alias(normalized):
        return True
    return False
