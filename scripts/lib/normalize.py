from __future__ import annotations

import re
import unicodedata


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace(" ", "")


def normalize_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def normalize_listing_symbol(value: str) -> str:
    return normalize_symbol(value).replace(".", "").replace("-", "")


LEGAL_NAME_STOPWORDS = {
    "a",
    "ab",
    "adr",
    "ads",
    "ag",
    "as",
    "asa",
    "class",
    "co",
    "common",
    "company",
    "corp",
    "corporation",
    "cv",
    "de",
    "etf",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "llc",
    "lp",
    "ltd",
    "nv",
    "ordinary",
    "plc",
    "sa",
    "sab",
    "se",
    "share",
    "shares",
    "spa",
    "stock",
    "the",
}
GENERIC_SINGLE_TOKEN_NAMES = {
    "capital",
    "energy",
    "financial",
    "fund",
    "global",
    "health",
    "industrial",
    "resources",
    "technology",
    "technologies",
}


def ascii_fold(value: str) -> str:
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")


def significant_name_tokens(value: str) -> set[str]:
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\bclass\s+[a-z0-9]+\b", " ", value, flags=re.IGNORECASE)
    tokens = re.findall(r"[a-z0-9]+", ascii_fold(value).lower())
    return {
        token
        for token in tokens
        if len(token) > 1 and not token.isdigit() and token not in LEGAL_NAME_STOPWORDS
    }


def names_match(source_name: str, target_name: str) -> bool:
    source_tokens = significant_name_tokens(source_name)
    target_tokens = significant_name_tokens(target_name)
    if not source_tokens or not target_tokens:
        return False

    if source_tokens == target_tokens:
        if len(source_tokens) == 1:
            token = next(iter(source_tokens))
            return len(token) >= 5 and token not in GENERIC_SINGLE_TOKEN_NAMES
        return True

    shared = source_tokens & target_tokens
    if not shared:
        return False

    shared_min_ratio = len(shared) / min(len(source_tokens), len(target_tokens))
    shared_max_ratio = len(shared) / max(len(source_tokens), len(target_tokens))
    return len(shared) >= 2 and shared_min_ratio >= 0.67 and shared_max_ratio >= 0.5
