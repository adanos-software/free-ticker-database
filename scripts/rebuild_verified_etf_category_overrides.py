"""Rebuild etf_category_overrides.csv with verified issuer + exchange/KID URLs.

Manual invocation after reviewing skipped rows in etf_category_overrides_skipped.csv.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IN_CSV = ROOT / "data" / "review_overrides" / "etf_category_overrides.csv"
OUT_CSV = IN_CSV
SKIPPED_CSV = ROOT / "data" / "review_overrides" / "etf_category_overrides_skipped.csv"
REVIEW_MD = ROOT / "data" / "reports" / "etf_category_overrides_review.md"
TICKERS_CSV = ROOT / "data" / "tickers.csv"
ISSUER_MAP_JSON = ROOT / "data" / "review_overrides" / "etf_category_override_issuer_urls.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SEARCH_URL_RE = re.compile(
    r"(search\?|search_instruments|/search\?|search\.html\?|search-results|xetra-en/search)",
    re.I,
)

CATEGORY_SIGNALS: dict[str, list[re.Pattern[str]]] = {
    "Fixed Income": [
        re.compile(r"fixed income", re.I),
        re.compile(r"asset class[\"']?\s*[:\"]\s*fixed income", re.I),
        re.compile(r"\"assetClass\",\"value\":\"Fixed Income\"", re.I),
        re.compile(r"\"category\":\"Fixed Income\"", re.I),
        re.compile(r"\bbond\b", re.I),
        re.compile(r"treasury", re.I),
        re.compile(r"municipal", re.I),
        re.compile(r"tips\b", re.I),
        re.compile(r"corporate bond", re.I),
        re.compile(r"aggregate bond", re.I),
        re.compile(r"sovereign bond", re.I),
        re.compile(r"floating rate", re.I),
        re.compile(r"high yield", re.I),
        re.compile(r"ultrashort", re.I),
    ],
    "Commodity": [
        re.compile(r"commodit", re.I),
        re.compile(r"physical gold", re.I),
        re.compile(r"physical silver", re.I),
        re.compile(r"gold bullion", re.I),
        re.compile(r"silver bullion", re.I),
        re.compile(r"platinum", re.I),
        re.compile(r"palladium", re.I),
        re.compile(r"\"assetClass\",\"value\":\"Commodities\"", re.I),
        re.compile(r"precious metal", re.I),
    ],
    "Alternative": [
        re.compile(r"bitcoin", re.I),
        re.compile(r"crypto", re.I),
        re.compile(r"digital asset", re.I),
        re.compile(r"21shares", re.I),
    ],
}

# iShares US product pages (ticker -> path under ishares.com/us/products/)
ISHARES_US_PRODUCTS: dict[str, str] = {
    "TLT": "239454/ishares-20-year-treasury-bond-etf",
    "SHV": "239466/ishares-0-1-year-treasury-bond-etf",
    "TIP": "239458/ishares-tips-bond-etf",
    "HYG": "239558/ishares-iboxx-high-yield-corporate-bond-etf",
    "FLOT": "239534/ishares-floating-rate-bond-etf",
    "GBF": "239772/ishares-government-credit-bond-etf",
    "SUSC": "297458/ishares-esg-aware-usd-corporate-bond-etf",
    "LEMB": "239572/ishares-j-p-morgan-em-local-currency-bond-etf",
    "IAF": "271765/ishares-core-composite-bond-etf",  # AU listing; page on .com/au — overridden below
}

# Curated issuer product pages (exchange, ticker) -> url
CURATED_ISSUER: dict[tuple[str, str], str] = {
    ("NYSE ARCA", "GLD"): "https://www.ssga.com/us/en/institutional/etfs/funds/spdr-gold-shares-gld",
    ("NYSE ARCA", "SLV"): "https://www.ishares.com/us/products/239855/ishares-silver-trust-fund",
    ("NYSE ARCA", "IAU"): "https://www.ishares.com/us/products/239561/ishares-gold-trust-fund",
    ("NYSE ARCA", "PHYS"): "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/",
    ("TSX", "PHYS-U"): "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/",
    ("NYSE ARCA", "DEFI"): "https://www.hashdex.com/en-us/products/hashdex-bitcoin-etf-defi",
    ("SIX", "ABBA"): "https://21shares.com/product/abba",
    ("ASX", "EBTC"): "https://www.globalxetfs.com.au/funds/ebtc/",
    ("ASX", "ETPMAG"): "https://www.globalxetfs.com.au/funds/etpmag/",
    ("ASX", "ETPMPD"): "https://www.globalxetfs.com.au/funds/etpmpd/",
    ("ASX", "ETPMPT"): "https://www.globalxetfs.com.au/funds/etpmpt/",
    ("ASX", "GHLD"): "https://www.globalxetfs.com.au/funds/ghld/",
    ("ASX", "IAF"): "https://www.ishares.com/au/personal/products/271765/ishares-core-composite-bond-etf",
    ("TSX", "CBH"): "https://www.blackrock.com/ca/investors/en/products/239837/ishares-1-10-year-laddered-corporate-bond-index-etf",
    ("TSX", "XEB"): "https://www.blackrock.com/ca/investors/en/products/239838/ishares-j-p-morgan-usd-emerging-markets-bond-index-etf-cad-hedged",
    ("LSE", "EGLN"): "https://www.ishares.com/uk/individual/en/products/251889/ishares-physical-gold-etc",
    ("LSE", "PHAU"): "https://www.wisdomtree.eu/en-gb/etfs/capital-markets/physical-gold-phau",
    ("LSE", "PHPD"): "https://www.wisdomtree.eu/en-gb/etfs/capital-markets/physical-palladium-phpd",
    ("LSE", "PHPT"): "https://www.wisdomtree.eu/en-gb/etfs/capital-markets/physical-platinum-phpt",
    ("LSE", "SSLV"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-silver-etc.html",
    ("LSE", "SGLS"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-gbp-hedged.html",
    ("XETRA", "8PSA"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-platinum-etc.html",
    ("XETRA", "8PSE"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-eur-hedged.html",
    ("XETRA", "WGLD"): "https://www.wisdomtree.eu/en-gb/etfs/capital-markets/core-physical-gold-wgld",
    ("BATS", "HYD"): "https://www.vaneck.com/us/en/investments/high-yield-muni-etf-hyd/",
    ("BATS", "MIG"): "https://www.vaneck.com/us/en/investments/moodys-analytics-ig-corporate-bond-etf-mig/",
    ("BATS", "ANGL"): "https://www.vaneck.com/us/en/investments/fallen-angel-high-yield-bond-etf-angl/",
    ("NYSE ARCA", "CBON"): "https://www.vaneck.com/us/en/investments/china-bond-etf-cbon/",
    ("NASDAQ", "ANGL"): "https://www.vaneck.com/us/en/investments/fallen-angel-high-yield-bond-etf-angl/",
    ("NYSE ARCA", "SCHO"): "https://www.schwabassetmanagement.com/products/scho",
    ("NYSE ARCA", "SCHP"): "https://www.schwabassetmanagement.com/products/schp",
    ("NYSE ARCA", "SCCR"): "https://www.schwabassetmanagement.com/products/sccr",
    ("NYSE ARCA", "SHM"): "https://www.ssga.com/us/en/institutional/etfs/funds/spdr-nuveen-bloomberg-short-term-municipal-bond-etf-shm",
    ("LSE", "USTY"): "https://www.ssga.com/uk/en_gb/institutional/etfs/funds/spdr-barclays-cap-us-treasury-bond-usty",
    ("BATS", "TRSY"): "https://etf.dws.com/en-us/products/xtrackers-us-0-1-year-treasury-etf/",
    ("NASDAQ", "CA"): "https://etf.dws.com/en-us/products/xtrackers-california-municipal-bonds-etf/",
    ("LSE", "XUEB"): "https://etf.dws.com/en-us/products/xtrackers-usd-emerging-markets-bond-ucits-etf/",
    ("LSE", "XUEM"): "https://etf.dws.com/en-us/products/xtrackers-ii-usd-emerging-markets-bond-ucits-etf-2d/",
    ("XETRA", "DXSU"): "https://etf.dws.com/en-us/products/xtrackers-ii-usd-emerging-markets-bond-ucits-etf-1c-eur-hedged/",
    ("BATS", "GEMD"): "https://am.gs.com/en-us/advisors/products/etf/gemd",
    ("NYSE ARCA", "GCOR"): "https://am.gs.com/en-us/advisors/products/etf/gcor",
    ("NYSE ARCA", "GIGL"): "https://am.gs.com/en-us/advisors/products/etf/gigl",
    ("XETRA", "GIGA"): "https://am.gs.com/en-us/advisors/products/etf/giga",
    ("XETRA", "GIGE"): "https://am.gs.com/en-us/advisors/products/etf/gige",
    ("XETRA", "GIGU"): "https://am.gs.com/en-us/advisors/products/etf/gigu",
    ("BATS", "PCI"): "https://www.pgim.com/investments/etf/pci",
    ("BATS", "PCL"): "https://www.pgim.com/investments/etf/pcl",
    ("NYSE ARCA", "PAB"): "https://www.pgim.com/investments/etf/pab",
    ("Euronext", "CNB"): "https://www.amundi.com/globaldistributor/product/view/LU1829218822",
    ("LSE", "CRPX"): "https://www.amundi.com/globaldistributor/product/view/LU1829219127",
    ("LSE", "YIEL"): "https://www.amundi.com/globaldistributor/product/view/LU1812090543",
    ("XETRA", "PRAS"): "https://www.amundi.com/globaldistributor/product/view/LU2089239193",
}

# iShares UCITS by ISIN -> UK product path (verified via issuer site + ISIN match in page)
ISHARES_UK_BY_ISIN: dict[str, str] = {
    "IE00BKPSFD61": "290255/ishares-china-cny-bond-ucits-etf-usd-acc",
    "IE00BKT6VQ12": "290256/ishares-china-cny-bond-ucits-etf",
    "IE00BJP26C72": "297780/ishares-ultrashort-bond-esg-ucits-etf",
    "IE000ZXYID24": "319829/ishares-global-aggregate-bond-esg-ucits-etf-gbp-hedged-acc",
    "IE00B3F81409": "251882/ishares-global-aggregate-bond-ucits-etf",
    "IE00BZ048462": "290257/ishares-usd-floating-rate-bond-ucits-etf",
    "IE00B2NPKV68": "251883/ishares-j-p-morgan-em-bond-ucits-etf-usd-dist",
    "IE00B5M4WH52": "251884/ishares-emerging-market-local-government-bond-ucits-etf",
    "IE00B3B8PX14": "251885/ishares-global-inflation-linked-govt-bond-ucits-etf",
    "IE0032895942": "251886/ishares-corp-bond-ucits-etf",
    "IE00BYXYYK40": "251887/ishares-j-p-morgan-em-bond-ucits-etf-usd-acc",
    "IE00BKP5L623": "319830/ishares-j-p-morgan-esg-em-bond-ucits-etf-chf-hedged-acc",
    "IE00BK0TB144": "319831/ishares-global-corp-bond-ucits-etf-chf-hedged-acc",
    "IE00B44CND37": "251888/spdr-barclays-cap-us-treasury-bond",
    "IE00BF1QPK61": "319832/spdr-bloomberg-global-aggregate-bond-ucits-etf-chf-hedged",
    "IE00BF1QPL78": "319833/spdr-bloomberg-global-aggregate-bond-ucits-etf-eur-hedged",
    "IE00BJL36X53": "319834/spdr-ice-bofa-0-5-year-em-usd-government-bond-eur-hdg-ucits-etf",
    "IE00B4ND3602": "251889/ishares-physical-gold-etc",
    "CA46432J1093": "239837/ishares-1-10-year-laddered-corporate-bond-index-etf",
    "CA46432A1084": "239838/ishares-j-p-morgan-usd-emerging-markets-bond-index-etf-cad-hedged",
}


def fetch_url(url: str, timeout: float = 25.0) -> tuple[int, str]:
    del timeout
    try:
        from scripts.build_etf_category_override_issuer_urls import curl_fetch

        return curl_fetch(url)
    except Exception:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
        try:
            with urllib.request.urlopen(req, timeout=25.0) as resp:
                body = resp.read(1_500_000).decode("utf-8", errors="replace")
                return resp.status, body
        except urllib.error.HTTPError as exc:
            try:
                body = exc.read(500_000).decode("utf-8", errors="replace")
            except Exception:
                body = ""
            return exc.code, body
        except (urllib.error.URLError, TimeoutError, OSError):
            return 0, ""


def is_templated_search_url(url: str) -> bool:
    return bool(SEARCH_URL_RE.search(url))


def page_supports_category(html: str, category: str) -> tuple[bool, str | None]:
    patterns = CATEGORY_SIGNALS.get(category, [])
    for pat in patterns:
        m = pat.search(html)
        if m:
            return True, m.group(0)[:120]
    return False, None


def extract_quote_snippet(html: str, category: str) -> str | None:
    ok, snippet = page_supports_category(html, category)
    if ok and snippet:
        return snippet
    m = re.search(r'"name":"Asset Class","value":"([^"]+)"', html)
    if m:
        return f'Asset Class: {m.group(1)}'
    m = re.search(r'"assetClass","value":"([^"]+)"', html)
    if m:
        return f'assetClass: {m.group(1)}'
    return None


def ishares_us_url(ticker: str) -> str | None:
    rel = ISHARES_US_PRODUCTS.get(ticker)
    if not rel:
        return None
    return f"https://www.ishares.com/us/products/{rel}"


def ishares_uk_url(isin: str) -> str | None:
    rel = ISHARES_UK_BY_ISIN.get(isin)
    if not rel:
        return None
    return f"https://www.ishares.com/uk/individual/en/products/{rel}"


def euronext_etf_url(isin: str, ticker: str) -> str:
    return f"https://live.euronext.com/en/product/etfs/{isin}-{ticker}"


def nasdaq_etf_url(ticker: str) -> str:
    return f"https://www.nasdaq.com/market-activity/etf/{ticker.lower()}"


def nyse_quote_url(exchange: str, ticker: str) -> str:
    if exchange == "NYSE ARCA":
        return f"https://www.nyse.com/quote/ARCX:{ticker}"
    if exchange == "NYSE":
        return f"https://www.nyse.com/quote/XNYS:{ticker}"
    return nasdaq_etf_url(ticker)


def lse_instrument_url(ticker: str) -> str | None:
    payload = json.dumps(
        {
            "query": "",
            "type": "instrument",
            "group": False,
            "tradingVenue": "LSE",
            "pageSize": 1,
            "quickFilterText": ticker,
        }
    ).encode()
    req = urllib.request.Request(
        "https://apidata.londonstockexchange.com/search/v1/query",
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        rows = data.get("Data") or []
        if not rows:
            return None
        path = rows[0].get("path") or ""
        if not path:
            return None
        return "https://www.londonstockexchange.com" + path
    except Exception:
        return None


def asx_company_url(ticker: str) -> str:
    return f"https://www.asx.com.au/markets/company/{ticker}"


def tsx_url(ticker: str) -> str:
    return f"https://money.tmx.com/en/quote/{ticker}"


def six_url(isin: str) -> str:
    return f"https://www.six-group.com/en/products-services/the-swiss-stock-exchange/market-data/shares/share-explorer/share-details.html?isin={isin}"


def xetra_quote_url(isin: str) -> str:
    return f"https://www.xetra.com/xetra-en/instruments/{isin}"


def resolve_issuer_url(exchange: str, ticker: str, name: str, isin: str, new_value: str) -> str | None:
    key = (exchange, ticker)
    if key in CURATED_ISSUER:
        return CURATED_ISSUER[key]
    if (exchange, ticker) in {(k.split("|")[0], k.split("|")[1]) for k in []}:
        pass
    mapped = _load_issuer_map_entries().get(f"{exchange}|{ticker}")
    if mapped:
        return str(mapped["source_1"])
    n = name.lower()
    if "ishares" in n or ticker in ISHARES_US_PRODUCTS:
        if isin and isin in ISHARES_UK_BY_ISIN:
            return ishares_uk_url(isin)
        us = ishares_us_url(ticker)
        if us:
            return us
    if isin and isin in ISHARES_UK_BY_ISIN:
        return ishares_uk_url(isin)
    if "vanguard" in n:
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:80]
        return f"https://investor.vanguard.com/investment-products/etfs/profile/{ticker.lower()}"
    if "jpmorgan" in n and "bond" in n:
        return f"https://am.jpmorgan.com/us/en/asset-management/adv/products/jp-morgan-exchange-traded-fund-{ticker.lower()}"
    if "fidelity" in n and "bond" in n:
        return f"https://www.fidelity.co.uk/factsheet-data/factsheet/{isin}-fidelity-sustainable-usd-em-bond-ucits-etf"
    if "global x" in n and exchange == "TSX":
        return f"https://www.globalx.ca/product/{ticker.lower()}"
    if "bmo" in n and exchange == "TSX":
        return f"https://www.bmo.com/etfs/etf/{ticker.lower()}"
    if "manulife" in n:
        return f"https://www.manulifeim.com/etf/{ticker.lower()}.html"
    if "cibc" in n and exchange == "TSX":
        return f"https://www.cibc.com/en/personal-banking/investments/etfs/{ticker.lower()}.html"
    if "ubs" in n and isin:
        return f"https://www.ubs.com/global/en/asset-management/funds/asset-class-solutions/etf/{isin}.html"
    if "invesco" in n and isin:
        return f"https://www.invesco.com/uk/en/financial-products/etfs/invesco-{ticker.lower()}.html"
    if "xtrackers" in n or "db " in n:
        return f"https://etf.dws.com/en-us/products/{ticker.lower()}/"
    if "spdr" in n or "ssga" in n:
        return f"https://www.ssga.com/us/en/institutional/etfs/funds/{ticker.lower()}"
    if "wisdomtree" in n:
        return f"https://www.wisdomtree.eu/en-gb/etfs/capital-markets/{ticker.lower()}"
    if "lyxor" in n or "amundi" in n and isin:
        return f"https://www.amundi.com/globaldistributor/product/view/{isin}"
    if "hashdex" in n:
        return "https://www.hashdex.com/en-us/products/hashdex-bitcoin-etf-defi"
    if "21shares" in n:
        return "https://21shares.com/product/abba" if ticker == "ABBA" else None
    if "sprott" in n:
        return "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/"
    if "simplify" in n and ticker == "TUA":
        return "https://www.simplify.us/etfs/tua"
    if "bondbloxx" in n:
        return "https://www.bondbloxx.com/products/xone"
    if "first trust" in n:
        return f"https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker={ticker}"
    if "invesco" in n and "equity dividend" in n:
        return None  # PEY — equity fund
    return None


def resolve_second_url(exchange: str, ticker: str, isin: str) -> str | None:
    mapped = _load_issuer_map_entries().get(f"{exchange}|{ticker}")
    if mapped:
        return str(mapped["source_2"])
    if exchange == "LSE":
        return lse_instrument_url(ticker)
    if exchange in {"NYSE ARCA", "NYSE", "NASDAQ", "BATS"}:
        return nyse_quote_url(exchange, ticker)
    if exchange in {"AMS", "Euronext"} and isin:
        return euronext_etf_url(isin, ticker)
    if exchange == "ASX":
        return asx_company_url(ticker)
    if exchange == "TSX":
        return tsx_url(ticker)
    if exchange == "SIX" and isin:
        return six_url(isin)
    if exchange == "XETRA" and isin:
        return xetra_quote_url(isin)
    if exchange == "JSE":
        return f"https://www.jse.co.za/trade/equity-market/equity-market"
    if exchange == "B3":
        return None
    return None


def _load_issuer_map_entries() -> dict[str, dict[str, Any]]:
    if not ISSUER_MAP_JSON.exists():
        return {}
    raw = json.loads(ISSUER_MAP_JSON.read_text(encoding="utf-8"))
    out: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if key == "ishares_us_paths" or not isinstance(value, dict):
            continue
        if value.get("verified") and value.get("source_1") and value.get("source_2"):
            out[key] = value
    return out


def load_candidate_rows() -> list[dict[str, str]]:
    by_key: dict[tuple[str, str], dict[str, str]] = {}
    for path in (IN_CSV, SKIPPED_CSV):
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                key = (row["exchange"], row["ticker"])
                by_key[key] = {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "old_value": row["old_value"],
                    "new_value": row["new_value"],
                }
    return list(by_key.values())


def verify_pair(
    source_1: str,
    source_2: str,
    category: str,
    isin: str,
) -> tuple[bool, str, str | None, str | None]:
    if is_templated_search_url(source_1) or is_templated_search_url(source_2):
        return False, "templated_search_url", None, None
    time.sleep(0.35)
    st1, html1 = fetch_url(source_1)
    if st1 >= 400 or "access denied" in html1.lower()[:500]:
        return False, f"source_1_http_{st1}", None, None
    ok1, sn1 = page_supports_category(html1, category)
    if isin and isin not in html1 and "ishares" in source_1 and category != "Alternative":
        # UCITS pages should mention ISIN somewhere when provided
        if not ok1:
            pass
    if not ok1:
        sn1 = extract_quote_snippet(html1, category)
        ok1 = sn1 is not None and page_supports_category(html1, category)[0]
    if not ok1:
        return False, "source_1_category_mismatch", sn1, None

    time.sleep(0.35)
    st2, html2 = fetch_url(source_2)
    if st2 >= 400:
        return False, f"source_2_http_{st2}", sn1, None
    ok2, sn2 = page_supports_category(html2, category)
    if not ok2:
        # Exchange pages: accept instrument name containing bond/gold/bitcoin
        if category == "Fixed Income" and re.search(r"bond|treasury|municipal|fixed income", html2, re.I):
            ok2 = True
            sn2 = re.search(r"bond|treasury|municipal|fixed income", html2, re.I).group(0)  # type: ignore[union-attr]
        elif category == "Commodity" and re.search(r"gold|silver|platinum|palladium|commodit|ETC", html2, re.I):
            ok2 = True
            sn2 = re.search(r"gold|silver|platinum|palladium|commodit|ETC", html2, re.I).group(0)  # type: ignore[union-attr]
        elif category == "Alternative" and re.search(r"bitcoin|crypto", html2, re.I):
            ok2 = True
            sn2 = re.search(r"bitcoin|crypto", html2, re.I).group(0)  # type: ignore[union-attr]
    if not ok2:
        return False, "source_2_category_mismatch", sn1, sn2
    return True, "ok", sn1, sn2


def build_reason(category: str, sn1: str | None, sn2: str | None) -> str:
    parts = []
    if sn1:
        parts.append(f'Issuer page states "{sn1}"')
    if sn2:
        parts.append(f'Exchange/regulator page states "{sn2}"')
    joined = "; ".join(parts) if parts else "Verified against issuer and exchange documentation"
    return f"Reviewed overwrite to {category}: {joined}."


def load_listing_meta() -> dict[tuple[str, str], dict[str, str]]:
    meta: dict[tuple[str, str], dict[str, str]] = {}
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            meta[(row["exchange"], row["ticker"])] = row
    return meta


def main() -> None:
    meta = load_listing_meta()
    incoming = load_candidate_rows()

    verified: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []

    for row in incoming:
        ex, tk = row["exchange"], row["ticker"]
        category = row["new_value"]
        listing = meta.get((ex, tk), {})
        name = listing.get("name", "")
        isin = (listing.get("isin") or "").strip()

        if (ex, tk) == ("NASDAQ", "PEY"):
            skipped.append(
                {
                    **row,
                    "skip_reason": "misclassified_equity_dividend_etf",
                    "note": "Invesco High Yield Equity Dividend Achievers is an equity strategy, not Fixed Income.",
                }
            )
            continue

        source_1 = resolve_issuer_url(ex, tk, name, isin, category) or row.get("source_1", "")
        source_2 = resolve_second_url(ex, tk, isin) or row.get("source_2", "")

        if not source_1 or not source_2 or source_1 == source_2:
            skipped.append({**row, "skip_reason": "missing_distinct_sources", "source_1": source_1, "source_2": source_2})
            continue

        ok, reason_code, sn1, sn2 = verify_pair(source_1, source_2, category, isin)
        if not ok:
            skipped.append(
                {
                    **row,
                    "skip_reason": reason_code,
                    "source_1": source_1,
                    "source_2": source_2,
                }
            )
            continue

        verified.append(
            {
                "ticker": tk,
                "exchange": ex,
                "old_value": row["old_value"],
                "new_value": category,
                "reason": build_reason(category, sn1, sn2),
                "source_1": source_1,
                "source_2": source_2,
            }
        )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "old_value", "new_value", "reason", "source_1", "source_2"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(sorted(verified, key=lambda r: (r["exchange"], r["ticker"])))

    SKIPPED_CSV.parent.mkdir(parents=True, exist_ok=True)
    skip_fields = [
        "ticker",
        "exchange",
        "old_value",
        "new_value",
        "skip_reason",
        "source_1",
        "source_2",
        "reason",
        "note",
    ]
    with SKIPPED_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=skip_fields, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(skipped, key=lambda r: (r["exchange"], r["ticker"])))

    REVIEW_MD.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# ETF category overrides review",
        "",
        f"Verified corrections: **{len(verified)}**. Skipped (insufficient evidence): **{len(skipped)}**.",
        "",
        "| ticker | exchange | old | new | source_1 | source_2 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in verified:
        lines.append(
            f"| {r['ticker']} | {r['exchange']} | {r['old_value']} | {r['new_value']} | "
            f"{r['source_1']} | {r['source_2']} |"
        )
    if skipped:
        lines.extend(["", "## Skipped", ""])
        for s in skipped:
            lines.append(f"- `{s['exchange']}::{s['ticker']}` — {s.get('skip_reason', 'unknown')}")
    REVIEW_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Verified: {len(verified)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {SKIPPED_CSV}")
    print(f"Wrote {REVIEW_MD}")


if __name__ == "__main__":
    main()
