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
    "etp",
    "fund",
    "group",
    "holding",
    "holdings",
    "inc",
    "incorporated",
    "index",
    "indexed",
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
    "series",
    "share",
    "shares",
    "shr",
    "spa",
    "stock",
    "sub",
    "the",
    "trust",
    "usd",
    "vtg",
    "voting",
}
NAME_TOKEN_ALIASES = {
    "constructi": "construction",
    "grw": "growth",
    "intl": "international",
    "internation": "international",
    "mc": "midcap",
    "mid": "midcap",
    "cap": "midcap",
    "vngrd": "vanguard",
}
OPENFIGI_NAME_NOISE_TOKENS = {"mrngstr"}
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


def _canonical_name_token(token: str) -> str:
    if token in OPENFIGI_NAME_NOISE_TOKENS:
        return ""
    return NAME_TOKEN_ALIASES.get(token, token)


def _name_tokens_equivalent(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    shorter, longer = (left, right) if len(left) <= len(right) else (right, left)
    return len(shorter) >= 5 and longer.startswith(shorter) and len(shorter) / len(longer) >= 0.75


def significant_name_tokens(value: str) -> set[str]:
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\bclass\s+[a-z0-9]+\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"[^a-z0-9]+", " ", ascii_fold(value).lower())
    tokens: set[str] = set()
    for token in value.split():
        if len(token) <= 1 or token.isdigit() or token in LEGAL_NAME_STOPWORDS:
            continue
        canonical = _canonical_name_token(token)
        if canonical:
            tokens.add(canonical)
    return tokens


def _matched_name_token_count(source_tokens: set[str], target_tokens: set[str]) -> int:
    matched = 0
    for source in source_tokens:
        if any(_name_tokens_equivalent(source, target) for target in target_tokens):
            matched += 1
    return matched


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

    matched = _matched_name_token_count(source_tokens, target_tokens)
    if matched == 0:
        return False

    shared_min_ratio = matched / min(len(source_tokens), len(target_tokens))
    shared_max_ratio = matched / max(len(source_tokens), len(target_tokens))
    return matched >= 2 and shared_min_ratio >= 0.67 and shared_max_ratio >= 0.5
