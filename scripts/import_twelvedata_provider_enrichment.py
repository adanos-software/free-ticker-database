"""Reconcile Twelve Data listing dumps with provider evidence and import safe rows.

The Twelve Data CSVs are discovery inputs only.  A row is importable only when
EODHD confirms the ticker, security type, issuer name, venue and a valid ISIN.
OpenFIGI, OpenDART, Alpha Vantage and GLEIF enrich or cross-check that result;
they do not override an EODHD identity mismatch.

The script is report-only by default.  ``--apply`` appends accepted listing rows
to the repository's coverage-expansion input and merges enriched identifiers.
Provider responses are cached outside the repository by default.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import zipfile
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import COUNTRY_TO_ISO, is_valid_isin, should_exclude_row
from scripts.lib.normalize import names_match


DATA_DIR = ROOT / "data"
DEFAULT_STOCKS = Path("/Users/alexschneider/Downloads/12data_stocks.csv")
DEFAULT_ETFS = Path("/Users/alexschneider/Downloads/12data_etf.csv")
DEFAULT_REPORT_DIR = DATA_DIR / "reports"
DEFAULT_CACHE_DIR = Path("/tmp/adanos-twelvedata-provider-cache")
LISTINGS_CSV = DATA_DIR / "listings.csv"
COVERAGE_CSV = DATA_DIR / "coverage_expansion_listings.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"

EODHD_URL = "https://eodhd.com/api/exchange-symbol-list/{code}"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"
GLEIF_URL = "https://api.gleif.org/api/v1/lei-records"
OPENDART_CORP_URL = "https://opendart.fss.or.kr/api/corpCode.xml"

USER_AGENT = "free-ticker-database/2.0 twelvedata-provider-import"
SAFE_STOCK_TYPES = {
    "American Depositary Receipt",
    "Common Stock",
    "Depositary Receipt",
    "Global Depositary Receipt",
    "Preferred Stock",
    "REIT",
}
EODHD_STOCK_TYPES = {
    "Common Stock",
    "Preferred Stock",
    "REIT",
    "Depositary Receipt",
    "American Depositary Receipt",
    "Global Depositary Receipt",
}

# Internal exchange names are retained for venues already represented in the
# repository.  For a venue that is genuinely new, the Twelve Data exchange name
# is kept as the listing namespace rather than silently pretending it is XETRA.
MIC_TO_INTERNAL = {
    "XAMS": "AMS",
    "XASX": "ASX",
    "ASEX": "ATHEX",
    "BVMF": "B3",
    "BATS": "BATS",
    "BCXE": "BATS",
    "XIST": "BIST",
    "XMAD": "BME",
    "XMEX": "BMV",
    "XBOM": "BSE_IN",
    "XKLS": "Bursa",
    "XCSE": "CPH",
    "XCNQ": "CSE",
    "XBRU": "Euronext",
    "XPAR": "Euronext",
    "XHEL": "HEL",
    "XHKG": "HKEX",
    "XIDX": "IDX",
    "XJSE": "JSE",
    "XKRX": "KRX",
    "XLON": "LSE",
    "AIMX": "LSE",
    "NEOE": "NEO",
    "XNSE": "NSE_IN",
    "XNAS": "NASDAQ",
    "XNCM": "NASDAQ",
    "XNGS": "NASDAQ",
    "XNMS": "NASDAQ",
    "XNYS": "NYSE",
    "ARCX": "NYSE ARCA",
    "XASE": "NYSE MKT",
    "EXPM": "OTC",
    "OTCB": "OTC",
    "OTCQ": "OTC",
    "PINX": "OTC",
    "PSGM": "OTC",
    "XOSL": "OSL",
    "XBKK": "SET",
    "XSES": "SGX",
    "XSWX": "SIX",
    "XSHG": "SSE",
    "XSTO": "STO",
    "XSHE": "SZSE",
    "XSAU": "TADAWUL",
    "XTAE": "TASE",
    "ROCO": "TPEX",
    "XJPX": "TSE",
    "XTSE": "TSX",
    "XTSX": "TSXV",
    "XTAI": "TWSE",
    "XWAR": "WSE",
    "XETR": "XETRA",
    "XMIL": "Borsa Italiana",
    "XWBO": "VSE",
    "XCAI": "EGX",
    "XBSE": "BVB",
    "XBUE": "BCBA",
    "XSGO": "SSE_CL",
    "XCOL": "CSE_LK",
    "XNZE": "NZX",
    "XDFM": "DFM",
    "XSTC": "HOSE",
    "XCAS": "CSE_MA",
    "XKAR": "PSX",
    "XPHS": "PSE",
    "XGHA": "GSE",
    "XLUS": "LUSE",
    "XNAI": "NSE_KE",
    "XUGA": "USE_UG",
    "XDAR": "DSE_TZ",
    "XMSW": "MSE_MW",
    "XZIM": "ZSE_ZW",
}

EODHD_CODE_BY_MIC = {
    "XAMS": "AS",
    "XASX": "AU",
    "ASEX": "AT",
    "BVMF": "SA",
    "XMAD": "MC",
    "XMEX": "MX",
    "XBOM": "IN",
    "XKLS": "KLSE",
    "XCSE": "CO",
    "XBRU": "BR",
    "XPAR": "PA",
    "XHEL": "HE",
    "XLON": "LSE",
    "AIMX": "LSE",
    "NEOE": "NEO",
    "XNAS": "US",
    "XNCM": "US",
    "XNGS": "US",
    "XNMS": "US",
    "XNYS": "US",
    "ARCX": "US",
    "XASE": "US",
    "EXPM": "US",
    "OTCB": "US",
    "OTCQ": "US",
    "PINX": "US",
    "PSGM": "US",
    "XOSL": "OL",
    "XBKK": "BK",
    "XSWX": "SW",
    "XSHG": "SHG",
    "XSTO": "ST",
    "XSHE": "SHE",
    "XSAU": "SAU",
    "XTAE": "TA",
    "ROCO": "TWO",
    "XTSE": "TO",
    "XTSX": "V",
    "XTAI": "TW",
    "XWAR": "WAR",
    "XETR": "XETRA",
    "XWBO": "VI",
    "XCAI": "EGX",
    "XBSE": "RO",
    "XBUE": "BA",
    "XSGO": "SN",
    "XCOL": "CM",
    "XNZE": "NZ",
    "XDFM": "AE",
    "XSTC": "VN",
    "XCAS": "BC",
    "XKAR": "KAR",
    "XPHS": "PSE",
    "XGHA": "GSE",
    "XLUS": "LUSE",
    "XKRX": "KO",
    "XJPX": "JP",
    "XFRA": "F",
    "XSTU": "STU",
    "XMUN": "MU",
    "XDUS": "DU",
    "XHAN": "HA",
    "XHAM": "HM",
}

ALPHA_SECTOR_MAP = {
    "Basic Materials": "Materials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Technology": "Information Technology",
}

REPORT_FIELDS = [
    "source_kind",
    "source_symbol",
    "source_name",
    "source_exchange",
    "source_mic",
    "source_country",
    "source_currency",
    "source_type",
    "canonical_exchange",
    "candidate_reason",
    "eodhd_code",
    "eodhd_exchange",
    "eodhd_name",
    "eodhd_type",
    "eodhd_isin",
    "eodhd_name_match",
    "openfigi_figi",
    "openfigi_name",
    "opendart_corp_code",
    "opendart_name",
    "alphavantage_sector",
    "gleif_lei",
    "decision",
    "decision_reason",
]

ACCEPTED_FIELDS = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
    "source_kind",
    "source_exchange",
    "source_mic",
    "eodhd_exchange",
    "eodhd_type",
    "figi",
    "lei",
    "opendart_corp_code",
]


def read_csv(path: Path, delimiter: str = ",") -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle, delimiter=delimiter))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def norm_symbol(value: str) -> str:
    return (value or "").strip().upper()


def symbol_variants(value: str) -> set[str]:
    symbol = norm_symbol(value)
    variants = {symbol}
    if symbol.isdigit():
        variants.add(symbol.lstrip("0") or "0")
        variants.add(symbol.zfill(5))
    return variants


def normalized_name(value: str) -> str:
    text = (value or "").lower().replace("&", " and ")
    text = re.sub(r"\b(co|corp|corporation|inc|incorporated|ltd|limited|plc|sa|ag|nv|spa|se|the|class|ordinary|shares|common|stock|etf|fund)\b", " ", text)
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def name_ratio(left: str, right: str) -> float:
    left_norm = normalized_name(left)
    right_norm = normalized_name(right)
    if not left_norm or not right_norm:
        return 0.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def numeric_tokens(value: str) -> set[str]:
    return set(re.findall(r"\d+", value or ""))


def exact_name_match(source_name: str, provider_name: str) -> bool:
    if numeric_tokens(source_name) != numeric_tokens(provider_name):
        return False
    return names_match(source_name, provider_name) or name_ratio(source_name, provider_name) >= 0.80


def source_exchange(row: dict[str, str]) -> str:
    mic = row.get("mic_code", "").strip().upper()
    if mic in MIC_TO_INTERNAL:
        return MIC_TO_INTERNAL[mic]
    return row.get("exchange", "").strip() or mic


def provider_cache_path(cache_dir: Path, provider: str, key: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", key)
    return cache_dir / provider / f"{safe}.json"


def get_json(session: requests.Session, url: str, *, params: dict[str, str], attempts: int = 3) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = session.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(str(last))


def load_eodhd_lists(
    session: requests.Session,
    rows: list[dict[str, str]],
    cache_dir: Path,
    api_token: str,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    codes = sorted({EODHD_CODE_BY_MIC.get(row.get("source_mic", "").upper(), "") for row in rows} - {""})
    payloads: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    for code in codes:
        path = provider_cache_path(cache_dir, "eodhd", code)
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
            else:
                payload = get_json(session, EODHD_URL.format(code=code), params={"api_token": api_token, "fmt": "json"})
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            if not isinstance(payload, list):
                raise ValueError("EODHD payload is not a list")
            payloads[code] = payload
        except Exception as exc:
            errors.append(f"{code}: {type(exc).__name__}: {str(exc)[:160]}")
    return payloads, errors


def load_opendart_corp_codes(
    session: requests.Session,
    cache_dir: Path,
    api_key: str,
) -> dict[str, dict[str, str]]:
    path = provider_cache_path(cache_dir, "opendart", "corp_codes")
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {row["stock_code"]: row for row in payload}
    try:
        response = session.get(OPENDART_CORP_URL, params={"crtfc_key": api_key}, timeout=(10, 20))
        response.raise_for_status()
    except requests.RequestException:
        return {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        xml_payload = archive.read("CORPCODE.xml")
    root = ElementTree.fromstring(xml_payload)
    rows: list[dict[str, str]] = []
    for item in root.findall("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        if stock_code and corp_code and corp_name:
            rows.append({"stock_code": stock_code, "corp_code": corp_code, "corp_name": corp_name})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return {row["stock_code"]: row for row in rows}


def opendart_match(row: dict[str, str], corp_codes: dict[str, dict[str, str]]) -> dict[str, str]:
    if row.get("source_mic") not in {"XKRX", "XKOS"} and row.get("source_exchange") not in {"KRX", "KOSDAQ"}:
        return {}
    candidates = [corp_codes.get(variant) for variant in symbol_variants(row["source_symbol"])]
    candidates = [candidate for candidate in candidates if candidate]
    if not candidates:
        return {}
    candidate = candidates[0]
    if exact_name_match(row["source_name"], candidate["corp_name"]):
        return candidate
    return {}


def eodhd_type_matches(expected: str, provider_type: str) -> bool:
    if expected == "ETF":
        return provider_type.strip().upper() == "ETF"
    return provider_type.strip() in EODHD_STOCK_TYPES


def eodhd_exchange_matches(row: dict[str, str], provider_exchange: str) -> bool:
    source_mic = row.get("source_mic", "").upper()
    expected = {
        "XNAS": {"NASDAQ"}, "XNCM": {"NASDAQ"}, "XNGS": {"NASDAQ"}, "XNMS": {"NASDAQ"},
        "XNYS": {"NYSE"}, "ARCX": {"NYSE ARCA", "NYSE MKT"}, "XASE": {"NYSE MKT", "AMEX"},
        "XKRX": {"Korea Stock Exchange", "KO"}, "XWBO": {"Vienna Exchange", "VI"},
        "XPAR": {"Euronext Paris", "PA"}, "XBRU": {"Euronext Brussels", "BR"},
        "XAMS": {"Euronext Amsterdam", "AS"}, "XETR": {"XETRA", "XETRA Stock Exchange"},
        "XFRA": {"Frankfurt Exchange", "F"}, "XSTU": {"Stuttgart Exchange", "STU"},
        "XMUN": {"Munich Exchange", "MU"}, "XDUS": {"Dusseldorf Exchange", "DU"},
        "XHAN": {"Hanover Exchange", "HA"}, "XHAM": {"Hamburg Exchange", "HM"},
        "XSGO": {"Chilean Exchange", "SN"}, "XBSE": {"Bucharest Exchange", "RO"},
        "XBUE": {"Buenos Aires Exchange", "BA"}, "XCOL": {"Colombo Exchange", "CM"},
        "XKRX": {"Korea Stock Exchange", "KO"},
    }.get(source_mic)
    if not expected:
        return True
    return provider_exchange in expected


def build_eodhd_index(payloads: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    index: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for code, rows in payloads.items():
        for candidate in rows:
            for variant in symbol_variants(str(candidate.get("Code") or "")):
                index[code][variant].append(candidate)
    return index


def match_eodhd(row: dict[str, str], index: dict[str, dict[str, list[dict[str, Any]]]]) -> dict[str, Any]:
    code = EODHD_CODE_BY_MIC.get(row.get("source_mic", "").upper(), "")
    if not code:
        return {"decision": "no_eodhd_exchange_mapping"}
    candidates_by_symbol = index.get(code, {})
    candidates = []
    seen_ids: set[int] = set()
    for variant in symbol_variants(row["source_symbol"]):
        for candidate in candidates_by_symbol.get(variant, []):
            if id(candidate) not in seen_ids:
                candidates.append(candidate)
                seen_ids.add(id(candidate))
    candidates = [candidate for candidate in candidates if eodhd_type_matches(row["asset_type"], str(candidate.get("Type") or "")) and eodhd_exchange_matches(row, str(candidate.get("Exchange") or ""))]
    if not candidates:
        return {"decision": "no_eodhd_symbol_match"}
    ranked = sorted(candidates, key=lambda item: name_ratio(row["source_name"], str(item.get("Name") or "")), reverse=True)
    candidate = ranked[0]
    provider_name = str(candidate.get("Name") or "").strip()
    provider_isin = str(candidate.get("Isin") or "").strip().upper()
    exact = exact_name_match(row["source_name"], provider_name)
    result = {
        "eodhd_code": str(candidate.get("Code") or ""),
        "eodhd_exchange": str(candidate.get("Exchange") or ""),
        "eodhd_name": provider_name,
        "eodhd_type": str(candidate.get("Type") or ""),
        "eodhd_isin": provider_isin,
        "eodhd_name_match": exact,
    }
    if len(ranked) > 1 and name_ratio(row["source_name"], str(ranked[1].get("Name") or "")) >= name_ratio(row["source_name"], provider_name) - 0.03:
        return {**result, "decision": "ambiguous_eodhd_symbol_match"}
    if not exact:
        return {**result, "decision": "eodhd_name_mismatch"}
    if not provider_isin or not is_valid_isin(provider_isin):
        return {**result, "decision": "eodhd_missing_or_invalid_isin"}
    return {**result, "decision": "eodhd_exact_match"}


def load_openfigi_by_isin(
    session: requests.Session,
    isins: set[str],
    cache_dir: Path,
    api_key: str,
    batch_size: int,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    path = provider_cache_path(cache_dir, "openfigi", "isin_mapping")
    mapping: dict[str, list[dict[str, Any]]] = {}
    if path.exists():
        mapping = json.loads(path.read_text(encoding="utf-8"))
    missing = sorted(isins - set(mapping))
    errors: list[str] = []
    for start in range(0, len(missing), batch_size):
        batch = missing[start : start + batch_size]
        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in batch]
        try:
            response = session.post(
                OPENFIGI_URL,
                headers={"X-OPENFIGI-APIKEY": api_key, "Content-Type": "application/json", "User-Agent": USER_AGENT},
                json=jobs,
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            for isin, item in zip(batch, payload):
                mapping[isin] = item.get("data", []) if isinstance(item, dict) else []
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            errors.append(f"OpenFIGI batch {batch[0]}..{batch[-1]}: {type(exc).__name__}: {str(exc)[:160]}")
            time.sleep(5.0)
        time.sleep(0.25)
    return mapping, errors


def select_figi(row: dict[str, str], candidates: list[dict[str, Any]]) -> dict[str, str]:
    if not candidates:
        return {}
    ticker = norm_symbol(row["source_symbol"])
    exact_ticker = [item for item in candidates if norm_symbol(str(item.get("ticker") or "")) in symbol_variants(ticker)]
    ranked = sorted(exact_ticker or candidates, key=lambda item: name_ratio(row["source_name"], str(item.get("name") or "")), reverse=True)
    item = ranked[0]
    if not str(item.get("figi") or ""):
        return {}
    if not exact_name_match(row["source_name"], str(item.get("name") or "")) and not exact_ticker:
        return {}
    return {"figi": str(item.get("figi") or ""), "name": str(item.get("name") or "")}


def alpha_sector(session: requests.Session, row: dict[str, str], cache_dir: Path, api_key: str) -> str:
    if row["asset_type"] != "Stock" or row.get("source_exchange") not in {"NASDAQ", "NYSE", "CBOE", "OTC"}:
        return ""
    key = norm_symbol(row["source_symbol"])
    path = provider_cache_path(cache_dir, "alphavantage", key)
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            payload = get_json(session, ALPHAVANTAGE_URL, params={"function": "OVERVIEW", "symbol": key, "apikey": api_key})
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return ""
    if not isinstance(payload, dict) or payload.get("Note") or payload.get("Information"):
        return ""
    sector = str(payload.get("Sector") or "").strip()
    normalized_sector_map = {key.upper(): value for key, value in ALPHA_SECTOR_MAP.items()}
    return normalized_sector_map.get(sector.upper(), sector)


def gleif_lei(session: requests.Session, isin: str, cache_dir: Path) -> str:
    path = provider_cache_path(cache_dir, "gleif", isin)
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            payload = get_json(session, GLEIF_URL, params={"filter[isin]": isin, "page[size]": "1"})
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return ""
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return str(data[0].get("id") or "") if data else ""


def build_candidates(stocks: list[dict[str, str]], etfs: list[dict[str, str]], listings: list[dict[str, str]]) -> list[dict[str, str]]:
    local_by_key: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for listing in listings:
        local_by_key[(listing["exchange"], norm_symbol(listing["ticker"]))].append(listing)
    result: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for source_kind, source_rows in (("stocks", stocks), ("etfs", etfs)):
        for source in source_rows:
            source_type = "ETF" if source_kind == "etfs" else source.get("type", "")
            if source_kind == "stocks" and source_type not in SAFE_STOCK_TYPES:
                continue
            asset_type = "ETF" if source_kind == "etfs" else "Stock"
            canonical = source_exchange(source)
            key = (source_kind, canonical, norm_symbol(source.get("symbol", "")))
            if key in seen:
                continue
            seen.add(key)
            existing = local_by_key.get((canonical, norm_symbol(source.get("symbol", ""))), [])
            if not existing:
                for variant in symbol_variants(source.get("symbol", "")):
                    existing.extend(local_by_key.get((canonical, variant), []))
            if any(row.get("asset_type") == asset_type for row in existing):
                continue
            reason = "new_listing_candidate"
            if existing:
                reason = "same_listing_key_other_asset_type_conflict"
            elif asset_type == "Stock" and should_exclude_row(
                {
                    "ticker": source.get("symbol", "").strip().upper(),
                    "exchange": canonical,
                    "name": source.get("name", "").strip(),
                    "asset_type": asset_type,
                    "aliases": source.get("name", "").strip(),
                    "isin": "",
                    "sector": "",
                }
            ):
                reason = "repository_scope_filter"
            result.append({
                "source_kind": source_kind,
                "source_symbol": source.get("symbol", "").strip(),
                "source_name": source.get("name", "").strip(),
                "source_exchange": source.get("exchange", "").strip(),
                "source_mic": source.get("mic_code", "").strip().upper(),
                "source_country": source.get("country", "").strip(),
                "source_currency": source.get("currency", "").strip(),
                "source_type": source_type,
                "asset_type": asset_type,
                "canonical_exchange": canonical,
                "candidate_reason": reason,
            })
    return result


def reconcile(
    candidates: list[dict[str, str]],
    eodhd_index: dict[str, dict[str, list[dict[str, Any]]]],
    corp_codes: dict[str, dict[str, str]],
    openfigi: dict[str, list[dict[str, Any]]],
    session: requests.Session,
    cache_dir: Path,
    alpha_key: str,
    alpha_limit: int,
    gleif_limit: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    reports: list[dict[str, str]] = []
    accepted: list[dict[str, str]] = []
    alpha_calls = 0
    gleif_calls = 0
    accepted_by_isin: dict[str, str] = {}
    for row in candidates:
        report = {field: "" for field in REPORT_FIELDS}
        report.update(row)
        if row["candidate_reason"] == "repository_scope_filter":
            report.update(
                {
                    "decision": "review",
                    "decision_reason": "repository_scope_filter_rejected_by_rebuild_dataset",
                }
            )
            reports.append(report)
            continue
        if row["candidate_reason"] == "same_listing_key_other_asset_type_conflict":
            report.update({"decision": "blocked", "decision_reason": "existing listing key has another asset type"})
            reports.append(report)
            continue
        eodhd = match_eodhd(row, eodhd_index)
        report.update(eodhd)
        dart = opendart_match(row, corp_codes)
        if dart:
            report["opendart_corp_code"] = dart["corp_code"]
            report["opendart_name"] = dart["corp_name"]
        if eodhd.get("decision") != "eodhd_exact_match":
            report.update({"decision": "review", "decision_reason": eodhd.get("decision", "no_eodhd_match")})
            reports.append(report)
            continue
        figi = select_figi(row, openfigi.get(eodhd.get("eodhd_isin", ""), []))
        report["openfigi_figi"] = figi.get("figi", "")
        report["openfigi_name"] = figi.get("name", "")
        sector = ""
        alpha_eligible = row["asset_type"] == "Stock" and row.get("source_exchange") in {"NASDAQ", "NYSE", "CBOE", "OTC"}
        if alpha_key and alpha_eligible and alpha_calls < alpha_limit:
            sector = alpha_sector(session, row, cache_dir, alpha_key)
            alpha_calls += 1
        report["alphavantage_sector"] = sector
        lei = ""
        if gleif_calls < gleif_limit and eodhd.get("eodhd_isin"):
            lei = gleif_lei(session, eodhd["eodhd_isin"], cache_dir)
            gleif_calls += 1
        report["gleif_lei"] = lei
        isin = eodhd["eodhd_isin"]
        previous = accepted_by_isin.get(isin)
        if previous and previous != row["source_name"]:
            report.update({"decision": "blocked", "decision_reason": "same ISIN resolved to conflicting candidate names"})
            reports.append(report)
            continue
        accepted_by_isin[isin] = row["source_name"]
        accepted_row = {
            "ticker": row["source_symbol"],
            "name": eodhd.get("eodhd_name") or row["source_name"],
            "exchange": row["canonical_exchange"],
            "asset_type": row["asset_type"],
            "stock_sector": sector if row["asset_type"] == "Stock" else "",
            "etf_category": "",
            "country": row["source_country"],
            "country_code": COUNTRY_TO_ISO.get(row["source_country"], ""),
            "isin": isin,
            "aliases": row["source_name"] if row["source_name"] != (eodhd.get("eodhd_name") or row["source_name"]) else "",
            "source_kind": row["source_kind"],
            "source_exchange": row["source_exchange"],
            "source_mic": row["source_mic"],
            "eodhd_exchange": eodhd.get("eodhd_exchange", ""),
            "eodhd_type": eodhd.get("eodhd_type", ""),
            "figi": figi.get("figi", ""),
            "lei": lei,
            "opendart_corp_code": dart.get("corp_code", ""),
        }
        report.update({"decision": "accept", "decision_reason": "EODHD exact ticker, venue, type, name and valid ISIN"})
        accepted.append(accepted_row)
        reports.append(report)
    return reports, accepted


def apply_rows(accepted: list[dict[str, str]]) -> dict[str, int]:
    existing_coverage = read_csv(COVERAGE_CSV)
    existing_keys = {f"{row['exchange']}::{row['ticker']}" for row in existing_coverage}
    existing_listing_keys = {f"{row['exchange']}::{row['ticker']}" for row in read_csv(LISTINGS_CSV)}
    additions = []
    for row in accepted:
        key = f"{row['exchange']}::{row['ticker']}"
        if key in existing_keys or key in existing_listing_keys:
            continue
        additions.append(row)
        existing_keys.add(key)
    fields = ["listing_key", "ticker", "exchange", "name", "asset_type", "stock_sector", "etf_category", "country", "country_code", "isin", "aliases"]
    with COVERAGE_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in existing_coverage:
            writer.writerow({field: row.get(field, "") for field in fields})
        for row in additions:
            writer.writerow({field: row.get(field, "") for field in fields if field != "listing_key"} | {"listing_key": f"{row['exchange']}::{row['ticker']}"})

    existing_identifiers = read_csv(IDENTIFIERS_EXTENDED_CSV)
    by_key = {row["listing_key"]: row for row in existing_identifiers}
    for row in additions:
        key = f"{row['exchange']}::{row['ticker']}"
        by_key[key] = {
            "listing_key": key,
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "isin": row["isin"],
            "wkn": "",
            "figi": row.get("figi", ""),
            "cik": "",
            "lei": row.get("lei", ""),
            "figi_source": "OpenFIGI" if row.get("figi") else "",
            "cik_source": "",
            "lei_source": "GLEIF" if row.get("lei") else "",
        }
    fields = ["listing_key", "ticker", "exchange", "isin", "wkn", "figi", "cik", "lei", "figi_source", "cik_source", "lei_source"]
    write_csv(IDENTIFIERS_EXTENDED_CSV, list(by_key.values()), fields)
    return {"coverage_rows_added": len(additions), "identifier_rows_added": len(additions)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stocks", type=Path, default=DEFAULT_STOCKS)
    parser.add_argument("--etfs", type=Path, default=DEFAULT_ETFS)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--openfigi-batch-size", type=int, default=100)
    parser.add_argument("--alphavantage-limit", type=int, default=500)
    parser.add_argument("--gleif-limit", type=int, default=1000)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    eodhd_key = os.environ.get("EODHD_API_TOKEN", "").strip()
    openfigi_key = os.environ.get("OPENFIGI_API_KEY", "").strip()
    alpha_key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    dart_key = os.environ.get("OPENDART_API_KEY", "").strip()
    if not eodhd_key or not openfigi_key:
        raise SystemExit("EODHD_API_TOKEN and OPENFIGI_API_KEY are required")
    stocks = read_csv(args.stocks, ";")
    etfs = read_csv(args.etfs, ";")
    listings = read_csv(LISTINGS_CSV)
    candidates = build_candidates(stocks, etfs, listings)
    session = requests.Session()
    eodhd_payloads, eodhd_errors = load_eodhd_lists(session, candidates, args.cache_dir, eodhd_key)
    eodhd_index = build_eodhd_index(eodhd_payloads)
    dart_codes = load_opendart_corp_codes(session, args.cache_dir, dart_key) if dart_key else {}
    eodhd_probe = [row for row in candidates if EODHD_CODE_BY_MIC.get(row.get("source_mic", ""), "")]
    eodhd_probe_results = [match_eodhd(row, eodhd_index) for row in eodhd_probe]
    isins = {result.get("eodhd_isin", "") for result in eodhd_probe_results if result.get("decision") == "eodhd_exact_match"}
    openfigi, openfigi_errors = load_openfigi_by_isin(session, isins, args.cache_dir, openfigi_key, args.openfigi_batch_size)
    reports, accepted = reconcile(candidates, eodhd_index, dart_codes, openfigi, session, args.cache_dir, alpha_key, args.alphavantage_limit, args.gleif_limit)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.report_dir / "twelvedata_provider_import_candidates.csv", reports, REPORT_FIELDS)
    write_csv(args.report_dir / "twelvedata_provider_import_accepted.csv", accepted, ACCEPTED_FIELDS)
    write_csv(args.report_dir / "twelvedata_provider_import_review.csv", [row for row in reports if row.get("decision") != "accept"], REPORT_FIELDS)
    summary = {
        "source_rows": {"stocks": len(stocks), "etfs": len(etfs)},
        "safe_candidate_rows": len(candidates),
        "eodhd_exchange_codes": sorted(eodhd_payloads),
        "eodhd_errors": eodhd_errors,
        "openfigi_isins": len(isins),
        "openfigi_errors": openfigi_errors,
        "opendart_corp_codes": len(dart_codes),
        "decision_counts": Counter(row["decision"] for row in reports),
        "decision_reason_counts": Counter(row["decision_reason"] for row in reports),
        "accepted_by_asset_type": Counter(row["asset_type"] for row in accepted),
        "accepted_by_exchange": Counter(row["exchange"] for row in accepted),
        "accepted_with_isin": sum(bool(row.get("isin")) for row in accepted),
        "accepted_with_figi": sum(bool(row.get("figi")) for row in accepted),
        "accepted_with_lei": sum(bool(row.get("lei")) for row in accepted),
        "applied": False,
    }
    if args.apply:
        summary["apply_result"] = apply_rows(accepted)
        summary["applied"] = True
    write_json(args.report_dir / "twelvedata_provider_import_summary.json", summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=dict))


if __name__ == "__main__":
    main()
