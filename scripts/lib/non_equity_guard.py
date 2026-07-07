from __future__ import annotations

import re
from typing import Any


BLOCKED_SOURCE_GATE = (
    "Block Stock ingestion until official security-type, CFI, exchange, issuer, "
    "or OpenFIGI evidence proves the row is common stock or an allowed depositary receipt."
)
AMBIGUOUS_SOURCE_GATE = (
    "Queue for manual classification; name shape alone does not authorize a Stock or ETF change."
)

PREFERRED_TICKER_PATTERNS = (
    re.compile(r"^[A-Z0-9]{1,10}-P[A-Z]?$"),
    re.compile(r"^[A-Z0-9]{1,10}-PF[A-Z]?$"),
    re.compile(r"^[A-Z0-9]{1,10}-PR-[A-Z]$"),
    re.compile(r"^[A-Z0-9]{1,10}-PREF$"),
)
WARRANT_TICKER_PATTERNS = (
    re.compile(r"^[A-Z0-9]{1,10}-WT[A-Z0-9]*$"),
)
RIGHT_TICKER_PATTERNS = (
    re.compile(r"^[A-Z0-9]{1,10}-RT[A-Z0-9]*$"),
)
UNIT_TICKER_PATTERNS = (
    re.compile(r"^[A-Z0-9]{1,10}-UN$"),
)

DEPOSITARY_ALLOWED_RE = re.compile(
    r"\b(?:american\s+depositary|american\s+depository|global\s+depositary|global\s+depository|adr|gdr)\b",
    re.IGNORECASE,
)
COMMON_STOCK_RE = re.compile(
    r"\b(?:common\s+stock|common\s+shares?|ordinary\s+shares?|ord(?:inary)?\s+shs?)\b",
    re.IGNORECASE,
)
PREFERRED_NAME_RE = re.compile(
    r"\b(?:preferred|preference|pref\.?|pfd\.?)\s+(?:stock|shares?|securities?)\b",
    re.IGNORECASE,
)
WARRANT_NAME_RE = re.compile(r"\bwarrants?\b", re.IGNORECASE)
RIGHT_NAME_RE = re.compile(r"\brights?\b", re.IGNORECASE)
UNIT_NAME_RE = re.compile(r"\bunits?\b", re.IGNORECASE)
DEBT_NAME_RE = re.compile(
    r"\b(?:senior|subordinated|capital|convertible|exchangeable)?\s*"
    r"(?:notes?|bonds?|debentures?)\s+(?:due|maturing|[0-9])",
    re.IGNORECASE,
)
CEF_NAME_RE = re.compile(
    r"\b(?:closed[- ]end fund|closed[- ]end investment|cef|term trust|municipal fund)\b",
    re.IGNORECASE,
)

OFFICIAL_SECURITY_TYPE_FIELDS = (
    "securityType",
    "security_type",
    "openfigi_security_type",
    "marketSecDes",
    "security_description",
    "instrument_type",
    "sec_security_type",
)
CFI_FIELDS = ("cfi", "cfi_code", "cfiCode", "CFICode", "CFI")


def _blank_result() -> dict[str, str]:
    return {
        "guard_decision": "accepted_or_not_applicable",
        "leakage_class": "",
        "confidence": "",
        "evidence_source": "",
        "evidence_value": "",
        "review_strategy": "",
        "verification_evidence_required": "",
        "recommended_next_source": "",
        "source_gate": "",
        "recommended_action": "",
    }


def _blocked(
    *,
    leakage_class: str,
    confidence: str,
    evidence_source: str,
    evidence_value: str,
) -> dict[str, str]:
    return {
        "guard_decision": "blocked_non_common_stock",
        "leakage_class": leakage_class,
        "confidence": confidence,
        "evidence_source": evidence_source,
        "evidence_value": evidence_value,
        "review_strategy": "exclude_or_reclassify_only_after_listing_keyed_official_evidence",
        "verification_evidence_required": "official_security_type_cfi_openfigi_or_exchange_listing_evidence",
        "recommended_next_source": (
            "OpenFIGI securityType, official CFI code, exchange listing directory, "
            "or issuer security description for the exact listing."
        ),
        "source_gate": BLOCKED_SOURCE_GATE,
        "recommended_action": "do_not_ingest_as_stock",
    }


def _manual(
    *,
    leakage_class: str,
    evidence_source: str,
    evidence_value: str,
) -> dict[str, str]:
    return {
        "guard_decision": "manual_review_ambiguous_stock_classification",
        "leakage_class": leakage_class,
        "confidence": "review",
        "evidence_source": evidence_source,
        "evidence_value": evidence_value,
        "review_strategy": "classify_with_official_product_or_security_type_evidence",
        "verification_evidence_required": "official_product_type_or_security_type_for_exact_listing",
        "recommended_next_source": (
            "Official exchange product segment, issuer security page, CFI code, or OpenFIGI securityType."
        ),
        "source_gate": AMBIGUOUS_SOURCE_GATE,
        "recommended_action": "queue_for_classification_review",
    }


def _first_present(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[str, str]:
    for field in fields:
        value = str(row.get(field, "") or "").strip()
        if value:
            return field, value
    return "", ""


def _classify_cfi(field: str, value: str) -> dict[str, str] | None:
    cfi = value.strip().upper()
    if not cfi:
        return None
    if cfi.startswith("ES"):
        return _blank_result()
    if cfi.startswith("EP"):
        return _blocked(
            leakage_class="preferred_equity_cfi",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if cfi.startswith("D"):
        return _blocked(
            leakage_class="debt_instrument_cfi",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if cfi.startswith("R"):
        return _blocked(
            leakage_class="rights_or_warrants_cfi",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if cfi.startswith("C"):
        return _blocked(
            leakage_class="fund_or_collective_investment_cfi",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    return None


def _classify_security_type(field: str, value: str) -> dict[str, str] | None:
    lowered = value.casefold()
    if any(marker in lowered for marker in ("common stock", "common share", "ordinary share")):
        return _blank_result()
    if any(marker in lowered for marker in ("preferred", "preference", "pfd")):
        return _blocked(
            leakage_class="preferred_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if "warrant" in lowered:
        return _blocked(
            leakage_class="warrant_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if "right" in lowered:
        return _blocked(
            leakage_class="right_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if "unit" in lowered:
        return _blocked(
            leakage_class="unit_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if any(marker in lowered for marker in ("note", "bond", "debenture", "debt")):
        return _blocked(
            leakage_class="debt_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    if any(marker in lowered for marker in ("closed-end", "closed end", "fund", "etf", "etn")):
        return _blocked(
            leakage_class="fund_security_type",
            confidence="official",
            evidence_source=field,
            evidence_value=value,
        )
    return None


def classify_non_equity_leakage(row: dict[str, Any]) -> dict[str, str]:
    """Classify non-common-stock leakage for rows currently tagged as Stock.

    The guard intentionally blocks only deterministic classes. Broad words such
    as "trust" or "fund" become manual-review evidence unless official security
    type or CFI evidence proves the product class.
    """

    if row.get("asset_type") != "Stock":
        return _blank_result()

    cfi_field, cfi_value = _first_present(row, CFI_FIELDS)
    cfi_result = _classify_cfi(cfi_field, cfi_value) if cfi_value else None
    if cfi_result and cfi_result["guard_decision"] != "accepted_or_not_applicable":
        return cfi_result

    type_field, type_value = _first_present(row, OFFICIAL_SECURITY_TYPE_FIELDS)
    type_result = _classify_security_type(type_field, type_value) if type_value else None
    if type_result and type_result["guard_decision"] != "accepted_or_not_applicable":
        return type_result

    ticker = str(row.get("ticker", "") or "").strip().upper()
    name = str(row.get("name", "") or "")

    if DEPOSITARY_ALLOWED_RE.search(name) or COMMON_STOCK_RE.search(name):
        return _blank_result()
    if any(pattern.fullmatch(ticker) for pattern in PREFERRED_TICKER_PATTERNS):
        return _blocked(
            leakage_class="preferred_ticker_pattern",
            confidence="deterministic",
            evidence_source="ticker",
            evidence_value=ticker,
        )
    if any(pattern.fullmatch(ticker) for pattern in WARRANT_TICKER_PATTERNS):
        return _blocked(
            leakage_class="warrant_ticker_pattern",
            confidence="deterministic",
            evidence_source="ticker",
            evidence_value=ticker,
        )
    if any(pattern.fullmatch(ticker) for pattern in RIGHT_TICKER_PATTERNS):
        return _blocked(
            leakage_class="right_ticker_pattern",
            confidence="deterministic",
            evidence_source="ticker",
            evidence_value=ticker,
        )
    if any(pattern.fullmatch(ticker) for pattern in UNIT_TICKER_PATTERNS):
        return _blocked(
            leakage_class="unit_ticker_pattern",
            confidence="deterministic",
            evidence_source="ticker",
            evidence_value=ticker,
        )
    if PREFERRED_NAME_RE.search(name):
        return _blocked(
            leakage_class="preferred_name_pattern",
            confidence="deterministic",
            evidence_source="name",
            evidence_value=name,
        )
    if WARRANT_NAME_RE.search(name):
        return _blocked(
            leakage_class="warrant_name_pattern",
            confidence="deterministic",
            evidence_source="name",
            evidence_value=name,
        )
    if RIGHT_NAME_RE.search(name):
        return _blocked(
            leakage_class="right_name_pattern",
            confidence="deterministic",
            evidence_source="name",
            evidence_value=name,
        )
    if UNIT_NAME_RE.search(name):
        return _blocked(
            leakage_class="unit_name_pattern",
            confidence="deterministic",
            evidence_source="name",
            evidence_value=name,
        )
    if DEBT_NAME_RE.search(name):
        return _blocked(
            leakage_class="debt_name_pattern",
            confidence="deterministic",
            evidence_source="name",
            evidence_value=name,
        )
    if CEF_NAME_RE.search(name):
        return _manual(
            leakage_class="possible_closed_end_fund_or_trust",
            evidence_source="name",
            evidence_value=name,
        )
    return _blank_result()


def is_blocked_non_common_stock(row: dict[str, Any]) -> bool:
    return classify_non_equity_leakage(row)["guard_decision"] == "blocked_non_common_stock"
