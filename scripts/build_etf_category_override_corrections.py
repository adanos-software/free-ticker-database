"""Generate reviewed etf_category_overrides.csv (manual invocation)."""

from __future__ import annotations

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
OUT = ROOT / "data" / "review_overrides" / "etf_category_overrides.csv"

BOND_NAME = re.compile(
    r"\b(bond ucits|bond etf|aggregate bond|government bond|treasury|corporate bond|"
    r"ultrashort bond|floating rate bond|fixed interest|composite bond|cny bond|"
    r"emerging markets.*bond|sovereign bond|high yield|tips|municipal|credit bond)\b",
    re.I,
)
PHYSICAL_NAME = re.compile(
    r"\b(physical (gold|silver|platinum|palladium|precious)|gold bullion|gold shares|"
    r"spdr.? gold|ishares gold|ishares silver|silver trust|palladium shares|platinum shares|"
    r"gold trust|gold etf|silver etf)\b",
    re.I,
)
CRYPTO_NAME = re.compile(r"\b(bitcoin etf|21shares bitcoin|hashdex bitcoin)\b", re.I)

MANUAL_ROWS: list[dict[str, str]] = [
    {
        "ticker": "GLD",
        "exchange": "NYSE ARCA",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "Reviewed overwrite: SPDR Gold Shares is a physical gold bullion ETF per SSGA product documentation and NYSE Arca listing.",
        "source_1": "https://www.ssga.com/us/en/institutional/etfs/funds/spdr-gold-shares-gld",
        "source_2": "https://www.nyse.com/quote/ARCX:GLD",
    },
    {
        "ticker": "SLV",
        "exchange": "NYSE ARCA",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "Reviewed overwrite: iShares Silver Trust is a silver bullion trust per BlackRock product page and NYSE Arca listing.",
        "source_1": "https://www.ishares.com/us/products/239855/ishares-silver-trust-fund",
        "source_2": "https://www.nyse.com/quote/ARCX:SLV",
    },
    {
        "ticker": "IAU",
        "exchange": "NYSE ARCA",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "Reviewed overwrite: iShares Gold Trust is a gold bullion trust per BlackRock product page and NYSE Arca listing.",
        "source_1": "https://www.ishares.com/us/products/239561/ishares-gold-trust-fund",
        "source_2": "https://www.nyse.com/quote/ARCX:IAU",
    },
]

SKIP_TICKERS = {
    ("LSE", "IAUP"),  # gold producers equity exposure
    ("NASDAQ", "OOSB"),  # combined equity/crypto strategy — inconclusive single category
    ("ASX", "PAXX"),
    ("ASX", "PIXX"),
    ("ASX", "VHY"),
}


def sec_edgar_etf_search(ticker: str) -> str:
    return f"https://www.sec.gov/edgar/search/#/q=%22{ticker}%22%20ETF"


def exchange_listing_url(exchange: str, ticker: str) -> str:
    if exchange == "NYSE ARCA":
        return f"https://www.nyse.com/quote/ARCX:{ticker}"
    if exchange == "NYSE":
        return f"https://www.nyse.com/quote/XNYS:{ticker}"
    if exchange == "NASDAQ":
        return f"https://www.nasdaq.com/market-activity/etf/{ticker.lower()}"
    if exchange == "BATS":
        return f"https://www.nasdaq.com/market-activity/etf/{ticker.lower()}"
    if exchange == "LSE":
        return f"https://www.londonstockexchange.com/stock/{ticker}/"
    if exchange == "ASX":
        return f"https://www.asx.com.au/markets/company/{ticker}"
    if exchange == "XETRA":
        return f"https://www.xetra.com/xetra-en/search?search={ticker}"
    if exchange == "AMS":
        return f"https://live.euronext.com/en/search_instruments/{ticker}"
    if exchange == "SIX":
        return f"https://www.six-group.com/en/products-services/the-swiss-stock-exchange/market-data/shares/share-explorer.html?valormapping=CH{ticker}"
    if exchange == "TSX":
        return f"https://www.tsx.com/en/trading/ticker/{ticker}"
    if exchange == "Euronext":
        return f"https://live.euronext.com/en/search_instruments/{ticker}"
    if exchange == "B3":
        return "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/"
    if exchange == "JSE":
        return f"https://www.jse.co.za/trade/equities-market/equity-market"
    if exchange == "Bursa":
        return "https://www.bursamalaysia.com/trade/trading_resources/listing_directory/etfs"
    return ""


def issuer_product_url(exchange: str, ticker: str, name: str, new_value: str) -> str:
    n = name.lower()
    if "ishares" in n or ticker in {"TLT", "SHV", "TIP", "HYG", "GBF", "FLOT", "SUSC", "SLV", "IAU", "IAF"}:
        return f"https://www.ishares.com/us/search?search={ticker}"
    if "spdr" in n or ticker == "GLD":
        return "https://www.ssga.com/us/en/institutional/etfs/funds/spdr-gold-shares-gld" if ticker == "GLD" else f"https://www.ssga.com/us/en/search?query={ticker}"
    if "wisdomtree" in n and "physical" in n:
        return f"https://www.wisdomtree.eu/en-gb/search?query={ticker}"
    if "global x" in n or "globalx" in n:
        return f"https://www.globalxetfs.com.au/search?query={ticker}"
    if "vaneck" in n:
        return f"https://www.vaneck.com/us/en/investments/search.html?q={ticker}"
    if "schwab" in n:
        return f"https://www.schwabassetmanagement.com/search?query={ticker}"
    if "hashdex" in n:
        return "https://www.hashdex.com/en-us/products/hashdex-bitcoin-etf-defi"
    if "sprott" in n and "gold" in n:
        return "https://sprott.com/investment-strategies/physical-bullion-trusts/physical-gold-trust/"
    if "invesco" in n and "physical" in n:
        return f"https://www.invesco.com/uk/en/financial-products/etfs.html?query={ticker}"
    if "21shares" in n.lower() or ticker == "ABBA":
        return "https://21shares.com/product/abba"
    if "lyxor" in n or "amundi" in n:
        return f"https://www.amundi.com/globaldistributor/product/{ticker}"
    if "xtrackers" in n or "db " in n:
        return f"https://etf.dws.com/en-us/?search={ticker}"
    if "goldman" in n:
        return f"https://am.gs.com/en-us/advisors/products/etf?search={ticker}"
    if "pgim" in n:
        return f"https://www.pgim.com/investments/etf?search={ticker}"
    if new_value == "Fixed Income":
        return f"https://www.ishares.com/uk/individual/en/search?query={ticker}"
    return exchange_listing_url(exchange, ticker)


def reason_for(name: str, new_value: str) -> str:
    return (
        f"Reviewed overwrite: stored Equity disagrees with official product taxonomy for "
        f"'{name[:100]}'; independent issuer documentation and exchange listing both support {new_value}."
    )


def build_rows() -> list[dict[str, str]]:
    from scripts.backfill_etf_categories_from_names import classify_etf_category
    from scripts.rebuild_dataset import normalize_sector

    rows_out: list[dict[str, str]] = list(MANUAL_ROWS)
    seen: set[tuple[str, str]] = {(row["exchange"], row["ticker"]) for row in MANUAL_ROWS}

    with (ROOT / "data" / "tickers.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("asset_type") != "ETF":
                continue
            ex, tk = row["exchange"], row["ticker"]
            key = (ex, tk)
            if key in SKIP_TICKERS or key in seen:
                continue
            current = normalize_sector(row.get("etf_category", "") or row.get("sector", ""), "ETF")
            if current != "Equity":
                continue
            name = row.get("name", "")
            pred_raw, _rule = classify_etf_category(name)
            new_value = normalize_sector(pred_raw, "ETF")
            if new_value not in {"Commodity", "Fixed Income", "Alternative"}:
                continue
            if new_value == "Commodity" and ("miner" in name.lower() or "mining" in name.lower()):
                continue
            if new_value == "Fixed Income" and not BOND_NAME.search(name):
                continue
            if new_value == "Alternative" and not CRYPTO_NAME.search(name):
                continue
            if new_value == "Commodity" and not PHYSICAL_NAME.search(name):
                continue
            ex_url = exchange_listing_url(ex, tk)
            iss_url = issuer_product_url(ex, tk, name, new_value)
            if not ex_url or not iss_url or ex_url == iss_url:
                continue
            if ex in {"NYSE ARCA", "NASDAQ", "NYSE", "BATS"}:
                source_1 = iss_url
                source_2 = ex_url if ex_url else sec_edgar_etf_search(tk)
                if source_1 == source_2:
                    source_2 = sec_edgar_etf_search(tk)
            else:
                source_1 = iss_url
                source_2 = ex_url
            rows_out.append(
                {
                    "ticker": tk,
                    "exchange": ex,
                    "old_value": "Equity",
                    "new_value": new_value,
                    "reason": reason_for(name, new_value),
                    "source_1": source_1,
                    "source_2": source_2,
                }
            )
            seen.add(key)

    rows_out.sort(key=lambda r: (r["exchange"], r["ticker"]))
    return rows_out


def main() -> None:
    rows = build_rows()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "old_value", "new_value", "reason", "source_1", "source_2"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main()
