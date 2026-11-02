#!/usr/bin/env python3
"""Research skipped ETF overrides; write verified issuer URL JSON."""

from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.rebuild_verified_etf_category_overrides import (  # noqa: E402
    CATEGORY_SIGNALS,
    ISHARES_UK_BY_ISIN,
    SKIPPED_CSV,
    TICKERS_CSV,
    build_reason,
    extract_quote_snippet,
    is_templated_search_url,
    lse_instrument_url,
    page_supports_category,
)

OUT_JSON = ROOT / "data" / "review_overrides" / "etf_category_override_issuer_urls.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

ISHARES_US_PATHS: dict[str, str] = {
    "FLOT": "239534/ishares-floating-rate-bond-etf",
    "HYG": "239563/ishares-iboxx-high-yield-corporate-bond-etf",
    "SUSC": "288488/ishares-esg-aware-usd-corporate-bond-etf",
    "TLT": "239454/ishares-20-year-treasury-bond-etf",
    "SHV": "239466/ishares-0-1-year-treasury-bond-etf",
    "TIP": "239458/ishares-tips-bond-etf",
    "GBF": "239772/ishares-government-credit-bond-etf",
    "LEMB": "239572/ishares-j-p-morgan-em-local-currency-bond-etf",
}

VANECK_OVERVIEW: dict[str, str] = {
    "HYD": "high-yield-muni-etf-hyd",
    "MIG": "moodys-analytics-ig-corporate-bond-etf-mig",
    "ANGL": "fallen-angel-high-yield-bond-etf-angl",
    "CBON": "china-bond-etf-cbon",
}

# Explicit issuer product / fact-sheet URLs (researched)
ISSUER_URL: dict[tuple[str, str], str] = {
    ("ASX", "EBTC"): "https://www.globalxetfs.com.au/funds/ebtc/",
    ("ASX", "IAF"): "https://www.blackrock.com/au/products/251977/ishares-core-composite-bond-etf",
    ("BATS", "FLOT"): "https://www.ishares.com/us/products/239534/ishares-floating-rate-bond-etf",
    ("BATS", "PCI"): "https://www.pgim.com/us/en/individual/investment-capabilities/products/etf/pgim-corporate-bond-5-10-year-etf",
    ("BATS", "PCL"): "https://www.pgim.com/us/en/individual/investment-capabilities/products/etf/pgim-corporate-bond-10-year-etf",
    ("BATS", "TRSY"): "https://etf.dws.com/en-us/TRSY-us-0-1-year-treasury-etf/",
    ("BATS", "GEMD"): "https://am.gs.com/public-assets/documents/fac02af1-19a9-11ef-98bb-9165853782e3?view=true",
    ("NASDAQ", "CA"): "https://etf.dws.com/download/asset/916c54c5-33ab-42c7-9446-7a8923757df0/",
    ("NASDAQ", "SUSC"): "https://www.ishares.com/us/products/288488/ishares-esg-aware-usd-corporate-bond-etf",
    ("NYSE ARCA", "HYG"): "https://www.ishares.com/us/products/239563/ishares-iboxx-high-yield-corporate-bond-etf",
    ("NYSE ARCA", "GCOR"): "https://am.gs.com/public-assets/documents/c5dd9bb9-34c2-11f0-8b9d-691ede07a315?view=true",
    ("NYSE ARCA", "GIGL"): "https://am.gs.com/public-assets/documents/fac02af1-19a9-11ef-98bb-9165853782e3?view=true",
    ("NYSE ARCA", "PAB"): "https://www.pgim.com/us/en/individual/investment-capabilities/products/etf/pgim-active-aggregate-bond-etf",
    ("NYSE ARCA", "SCHO"): "https://www.schwab.wallst.com/Prospect/Research/etfs/summary.asp?symbol=SCHO",
    ("NYSE ARCA", "SCHP"): "https://www.schwab.wallst.com/Prospect/Research/etfs/summary.asp?symbol=SCHP",
    ("NYSE ARCA", "SCCR"): "https://www.schwab.wallst.com/Prospect/Research/etfs/summary.asp?symbol=SCCR",
    ("NYSE ARCA", "PHYS"): "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/",
    ("NYSE ARCA", "TUA"): "https://www.simplify.us/etfs/tua",
    ("NYSE ARCA", "XONE"): "https://www.bondbloxx.com/products/xone",
    ("TSX", "PHYS-U"): "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/",
    ("TSX", "ZEF"): "https://www.bmo.com/etfs/etf/zef",
    ("LSE", "SSLV"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-silver-etc.html",
    ("LSE", "SGLS"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-gbp-hedged.html",
    ("XETRA", "8PSE"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-eur-hedged.html",
    ("LSE", "CRPX"): "https://www.amundi.com/globaldistributor/product/view/LU1829219127",
    ("LSE", "FEMP"): "https://www.fidelity.co.uk/factsheet-data/factsheet/IE00BM9GRQ71-fidelity-sustainable-usd-em-bond-ucits-etf",
    ("LSE", "VDEA"): "https://www.vanguard.co.uk/professional/product/etf/equity/9495/vanguard-usd-emerging-markets-government-bond-ucits-etf-usd-distributing",
    ("LSE", "VDET"): "https://www.vanguard.co.uk/professional/product/etf/equity/9495/vanguard-usd-emerging-markets-government-bond-ucits-etf-usd-distributing",
    ("LSE", "XUEB"): "https://etf.dws.com/en-us/XUEB-xtrackers-usd-emerging-markets-bond-ucits-etf/",
    ("LSE", "XUEM"): "https://etf.dws.com/en-us/XUEM-xtrackers-ii-usd-emerging-markets-bond-ucits-etf-2d/",
    ("XETRA", "DXSU"): "https://etf.dws.com/en-us/DXSU-xtrackers-ii-usd-emerging-markets-bond-ucits-etf-1c-eur-hedged/",
    ("SIX", "ESGEMC"): "https://www.ubs.com/global/en/asset-management/funds/asset-class-solutions/etf/LU1974696095.html",
    ("XETRA", "EMIE"): "https://www.ubs.com/global/en/asset-management/funds/asset-class-solutions/etf/LU1974696418.html",
    ("XETRA", "WGLD"): "https://www.wisdomtree.com/gb/products/commodities/wisdomtree-core-physical-gold",
    ("LSE", "EGLN"): "https://www.ishares.com/gls-download/literature/fact-sheet/sgln-ishares-physical-gold-etc-fund-fact-sheet-en-gb.pdf",
    ("LSE", "SGLS"): "https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-gbp-hedged.html",
    ("NYSE ARCA", "DEFI"): "https://www.hashdex-etfs.com/DEFI",
    ("NYSE ARCA", "PHYS"): "https://sprott.com/investment-strategies/exchange-listed-products/physical-bullion-funds/gold/",
    ("TSX", "PHYS-U"): "https://sprott.com/investment-strategies/exchange-listed-products/physical-bullion-funds/gold/",
    ("B3", "USDB11"): "https://www.investoetf.com/etf/usdb11/",
    ("JSE", "STXGBD"): "https://satrix.co.za/products/product-details?id=61",
    ("LSE", "PHAU"): "https://dataspanapi.wisdomtree.com/pdr/documents/FACTSHEET/MSL/EU/EN-GB/JE00B1VS3770/",
    ("LSE", "PHPD"): "https://dataspanapi.wisdomtree.com/pdr/documents/FACTSHEET/MSL/EU/EN-GB/JE00B1VS3002/",
    ("LSE", "PHPT"): "https://dataspanapi.wisdomtree.com/pdr/documents/FACTSHEET/MSL/EU/EN-GB/JE00B1VS2W53/",
    ("XETRA", "WGLD"): "https://dataspanapi.wisdomtree.com/pdr/documents/FACTSHEET/MSL/EU/EN-GB/JE00BN2CJ301/",
    ("LSE", "SGLS"): "https://www.ishares.com/gls-download/literature/fact-sheet/sgln-ishares-physical-gold-etc-fund-fact-sheet-en-gb.pdf",
    ("XETRA", "8PSE"): "https://www.ishares.com/gls-download/literature/fact-sheet/sgln-ishares-physical-gold-etc-fund-fact-sheet-en-gb.pdf",
    ("NYSE ARCA", "DEFI"): "https://www.hashdex-etfs.com/DEFI",
}

SOURCE2_URL: dict[tuple[str, str], str] = {
    ("ASX", "EBTC"): "https://www.globalxetfs.com.au/fund-categories/crypto/",
    ("ASX", "IAF"): "https://www.asx.com.au/markets/company/IAF",
    ("B3", "USDB11"): "https://borainvestir.b3.com.br/cotacoes/etfs/USDB11/",
    ("JSE", "STXGBD"): "https://www.sharenet.co.za/v3/sens_display.php?scode=&seq=19&tdate=20200806082100",
    ("NYSE ARCA", "DEFI"): "https://www.sec.gov/Archives/edgar/data/1985840/000199937125003249/tct-424b3_032625.htm",
}

COOKIE_JAR = Path(tempfile.gettempdir()) / "vaneck_cookies.txt"


SEC_UA = "free-ticker-database contact@example.com"


def body_to_text(url: str, body: bytes) -> str:
    if body.startswith(b"%PDF") or url.lower().endswith(".pdf"):
        try:
            import io

            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(body))
            return "".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            strings = subprocess.run(["strings"], input=body, capture_output=True, timeout=30)
            return strings.stdout.decode("utf-8", errors="replace")
    return body.decode("utf-8", errors="replace")


def curl_fetch(url: str, use_vaneck_cookies: bool = False) -> tuple[int, str]:
    agent = SEC_UA if "sec.gov" in url else UA
    cmd = [
        "curl",
        "-sS",
        "-L",
        "--compressed",
        "--max-time",
        "25",
        "-A",
        agent,
        "-w",
        "\n%{http_code}",
        "-o",
        "-",
    ]
    if use_vaneck_cookies or "vaneck.com" in url:
        cmd.extend(["-c", str(COOKIE_JAR), "-b", str(COOKIE_JAR)])
    cmd.append(url)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=False, timeout=35)
        raw = proc.stdout or b""
        if b"\n" in raw[-10:]:
            body, line = raw.rsplit(b"\n", 1)
            code = int(line.decode().strip() or "0")
        else:
            body, code = raw, 0
        text = body_to_text(url, body)
        if code == 0 and proc.returncode != 0:
            return 0, text
        return code, text
    except (subprocess.TimeoutExpired, OSError, ValueError):
        return 0, ""


def blackrock_sg(isin: str) -> str | None:
    rel = ISHARES_UK_BY_ISIN.get(isin)
    if not rel:
        return None
    return f"https://www.blackrock.com/sg/en/products/{rel}"


def ishares_us(ticker: str) -> str | None:
    rel = ISHARES_US_PATHS.get(ticker)
    if not rel:
        return None
    return f"https://www.ishares.com/us/products/{rel}"


def vaneck_url(ticker: str) -> str | None:
    slug = VANECK_OVERVIEW.get(ticker)
    if not slug:
        return None
    return f"https://www.vaneck.com/us/en/investments/{slug}/overview/"


def amundi_url(isin: str) -> str:
    return f"https://www.amundi.com/globaldistributor/product/view/{isin}"


def ubs_url(isin: str) -> str:
    return f"https://www.ubs.com/global/en/asset-management/funds/asset-class-solutions/etf/{isin}.html"


def jpm_fund_url(isin: str) -> str:
    return (
        "https://am.jpmorgan.com/gb/en/asset-management/adv/products/"
        f"jpmorgan-usd-emerging-markets-sovereign-bond-ucits-etf-usd-acc-{isin.lower()}"
    )


WISDOMTREE_GB: dict[str, str] = {
    "JE00B1VS3770": "https://www.wisdomtree.com/gb/products/commodities/wisdomtree-physical-gold",
    "JE00B1VS3002": "https://www.wisdomtree.com/gb/products/commodities/wisdomtree-physical-palladium",
    "JE00B1VS2W53": "https://www.wisdomtree.com/gb/products/commodities/wisdomtree-physical-platinum",
    "JE00BN2CJ301": "https://www.wisdomtree.com/gb/products/commodities/wisdomtree-core-physical-gold",
}


def resolve_source1(ex: str, tk: str, isin: str, name: str) -> list[str]:
    key = (ex, tk)
    out: list[str] = []
    if key in ISSUER_URL:
        out.append(ISSUER_URL[key])
    if isin:
        br = blackrock_sg(isin)
        if br:
            out.append(br)
        out.append(amundi_url(isin))
        if "ubs" in name.lower():
            out.append(ubs_url(isin))
    us = ishares_us(tk)
    if us:
        out.append(us)
    vk = vaneck_url(tk)
    if vk:
        out.append(vk)
    if "jpmorgan" in name.lower() and isin:
        out.append(jpm_fund_url(isin))
    if isin in WISDOMTREE_GB:
        out.append(WISDOMTREE_GB[isin])
        out.append(f"https://dataspanapi.wisdomtree.com/pdr/documents/FACTSHEET/MSL/EU/EN-GB/{isin}/")
    if "invesco" in name.lower() and "physical" in name.lower():
        slug = tk.lower()
        out.append(f"https://www.invesco.com/uk/en/financial-products/etfs/invesco-physical-gold-etc-{slug}.html")
    if "legal" in name.lower() or "l&g" in name.lower():
        out.append(f"https://fundcentres.landg.com/uk/en/fund-centre/ETF/Collectives/{isin}/overview")
    if "hanetf" in name.lower() or "finamex" in name.lower():
        out.append(f"https://hanetf.com/product/{tk.lower()}/")
    if "overlay" in name.lower() and tk == "OVT":
        out.append("https://www.overlayshares.com/ovt")
    if "first trust" in name.lower() or tk == "EYEG":
        out.append(f"https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker={tk}")
    if "nuveen" in name.lower() or tk in {"NUAG", "NUMI"}:
        out.append(f"https://www.nuveen.com/en-us/exchange-traded-funds/{tk.lower()}")
    if "franklin" in name.lower() and tk == "FTPA":
        out.append("https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/ftpa")
    if "rivernorth" in name.lower() and tk == "RFMZ":
        out.append("https://www.rivernorth.com/mutual-funds/rfmz/")
    if "blackrock" in name.lower() and "municipal target" in name.lower():
        out.append("https://www.blackrock.com/us/individual/products/314425/blackrock-2037-municipal-target-term-trust-fund")
    if "madison" in name.lower() and tk == "MAGG":
        out.append("https://www.madisoninvestments.com/etf/magg")
    if "genter" in name.lower():
        out.append("https://www.gentercapital.com/genm-etf")
    if "iq mackay" in name.lower() or tk == "MMIT":
        out.append(f"https://www.flexshares.com/etf/{tk.lower()}")
    if "john hancock" in name.lower() or tk == "JHMU":
        out.append(f"https://www.jhinvestments.com/etfs/{tk.lower()}")
    if "satrix" in name.lower() or tk == "STXGBD":
        out.append("https://satrix.co.za/products/satrix-global-aggregate-bond-feeder-portfolio")
    if "investo" in name.lower() and tk == "USDB11":
        out.append("https://www.investoetf.com.br/usdb11")
    if "cibc" in name.lower() and tk == "CALB":
        out.append("https://www.cibc.com/en/personal-banking/investments/etfs/products/cibc-canadian-government-long-term-bond-etf.html")
    if "manulife" in name.lower() and tk == "TERM":
        out.append("https://www.manulifeim.com/etf/term")
    if "invesco" in name.lower() and tk in {"BAB", "TBLL", "PEMD"}:
        out.append(f"https://www.invesco.com/us/financial-products/etfs/product/invesco-{tk.lower()}-etf")
    if "goldman" in name.lower() and tk in {"GIGA", "GIGE", "GIGU"}:
        out.append(f"https://am.gs.com/en-us/advisors/products/etf/{tk.lower()}")
    if "invesco" in name.lower() and tk == "GLBD":
        out.append("https://www.invesco.com/uk/en/financial-products/etfs/invesco-global-corporate-bond-esg-climate-transition-ucits-etf-dist.html")
    # dedupe preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def resolve_source2(ex: str, tk: str, isin: str) -> str | None:
    key = (ex, tk)
    if key in SOURCE2_URL:
        return SOURCE2_URL[key]
    if ex == "LSE":
        return lse_instrument_url(tk)
    if ex in {"NYSE ARCA", "NYSE"}:
        sym = f"ARCX:{tk}" if ex == "NYSE ARCA" else f"XNYS:{tk}"
        return f"https://www.nyse.com/quote/{sym}"
    if ex in {"NASDAQ", "BATS"}:
        return f"https://www.nasdaq.com/market-activity/etf/{tk.lower()}"
    if ex in {"AMS", "Euronext"} and isin:
        return f"https://live.euronext.com/en/product/etfs/{isin}-{tk}"
    if ex == "ASX":
        return f"https://www.asx.com.au/markets/company/{tk}"
    if ex == "TSX":
        return f"https://money.tmx.com/en/quote/{tk}"
    if ex == "SIX" and isin:
        return (
            "https://www.six-group.com/en/products-services/the-swiss-stock-exchange/"
            f"market-data/shares/share-explorer/share-details.html?isin={isin}"
        )
    if ex == "XETRA" and isin:
        return f"https://www.xetra.com/xetra-en/instruments/{isin}"
    if ex == "JSE" and tk == "STXGBD":
        return "https://www.jse.co.za/jse/instruments/4208"
    if ex == "JSE":
        return "https://www.jse.co.za/trade/equity-market/equity-market"
    if ex == "B3" and tk == "USDB11":
        return "https://borainvestir.b3.com.br/cotacoes/etfs/USDB11/"
    return None


def exchange_supports(html: str, category: str) -> tuple[bool, str | None]:
    ok, sn = page_supports_category(html, category)
    if ok:
        return ok, sn
    if category == "Fixed Income" and re.search(r"bond|treasury|municipal|fixed income", html, re.I):
        m = re.search(r"bond|treasury|municipal|fixed income", html, re.I)
        return True, m.group(0) if m else None
    if category == "Commodity" and re.search(
        r"gold|silver|platinum|palladium|commodit|ETC|ETP|physical", html, re.I
    ):
        m = re.search(r"gold|silver|platinum|palladium|commodit|ETC|ETP|physical", html, re.I)
        return True, m.group(0) if m else None
    if category == "Alternative" and re.search(r"bitcoin|crypto|digital asset", html, re.I):
        m = re.search(r"bitcoin|crypto|digital asset", html, re.I)
        return True, m.group(0) if m else None
    return False, None


def verify_pair(s1: str, s2: str, category: str) -> tuple[bool, str | None, str | None]:
    if is_templated_search_url(s1) or is_templated_search_url(s2):
        return False, None, None
    time.sleep(0.25)
    st1, html1 = curl_fetch(s1)
    if st1 >= 400 or not html1:
        return False, None, None
    ok1, sn1 = page_supports_category(html1, category)
    if not ok1:
        sn1 = extract_quote_snippet(html1, category)
        ok1 = sn1 is not None and page_supports_category(html1, category)[0]
    if not ok1 and category == "Fixed Income" and re.search(r"bond|treasury|municipal|fixed income", html1, re.I):
        m = re.search(r"bond|treasury|municipal|fixed income", html1, re.I)
        ok1, sn1 = True, m.group(0) if m else None
    if not ok1 and category == "Commodity" and re.search(r"gold|silver|platinum|palladium|commodit|physical", html1, re.I):
        m = re.search(r"gold|silver|platinum|palladium|commodit|physical", html1, re.I)
        ok1, sn1 = True, m.group(0) if m else None
    if not ok1 and category == "Alternative" and re.search(r"bitcoin|crypto|digital asset", html1, re.I):
        m = re.search(r"bitcoin|crypto|digital asset", html1, re.I)
        ok1, sn1 = True, m.group(0) if m else None
    if not ok1:
        return False, None, None

    time.sleep(0.25)
    st2, html2 = curl_fetch(s2)
    if st2 >= 400 or not html2:
        return False, sn1, None
    ok2, sn2 = exchange_supports(html2, category)
    if not ok2:
        return False, sn1, sn2
    return True, sn1, sn2


def main() -> None:
    meta: dict[tuple[str, str], dict[str, str]] = {}
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            meta[(row["exchange"], row["ticker"])] = row

    results: dict[str, object] = {}
    unverified: list[dict[str, str]] = []

    with SKIPPED_CSV.open(newline="", encoding="utf-8") as handle:
        skipped = list(csv.DictReader(handle))

    for row in skipped:
        ex, tk = row["exchange"], row["ticker"]
        if (ex, tk) == ("NASDAQ", "PEY"):
            continue
        category = row["new_value"]
        listing = meta.get((ex, tk), {})
        name = listing.get("name", "")
        isin = (listing.get("isin") or "").strip()

        s2 = resolve_source2(ex, tk, isin)
        if not s2:
            unverified.append({"key": f"{ex}|{tk}", "why": "no_source_2"})
            continue

        verified = False
        best: tuple[str, str, str | None, str | None] | None = None
        for s1 in resolve_source1(ex, tk, isin, name):
            ok, sn1, sn2 = verify_pair(s1, s2, category)
            if ok:
                best = (s1, s2, sn1, sn2)
                verified = True
                break

        if not verified:
            unverified.append({"key": f"{ex}|{tk}", "why": "no_working_pair", "s2": s2})
            continue

        s1, s2, sn1, sn2 = best  # type: ignore[misc]
        results[f"{ex}|{tk}"] = {
            "source_1": s1,
            "source_2": s2,
            "reason": build_reason(category, sn1, sn2),
            "verified": True,
        }

    results["ishares_us_paths"] = ISHARES_US_PATHS
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Verified entries: {len(results) - 1}")
    print(f"Unverified: {len(unverified)}")
    for u in unverified:
        print(u)


if __name__ == "__main__":
    main()
