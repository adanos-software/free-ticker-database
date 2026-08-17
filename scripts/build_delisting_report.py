"""Weekly delisting-candidate report (deterministic exchange-master diff).

Companion to the drift report. For the exchanges where a BOARD-COMPLETE official
current-listing master is freely available, diffs our primary stock holdings
against the master: holdings that are absent from the current master are
delisting / rename candidates for review.

Unlike a naive source-vs-dataset diff (intentionally avoided by the drift
report because partial/scope-mismatched masters yield thousands of false
candidates), this is scoped to masters that are *board-complete* and verified
low-false-positive:

  * US   - NASDAQ Trader nasdaqlisted.txt + otherlisted.txt (full US-exchange universe)
  * JP   - JPX data_j.xls (TSE Prime/Standard/Growth, all domestic boards)
  * AU   - ASX ListedCompanies.csv (full ASX list)
  * IN-NSE - EQUITY_L.csv + SME_EQUITY_L.csv (main board + EMERGE SME -> complete)
  * IN-BSE - BSE ListofScripData API with authoritative Active/Suspended/Delisted status

Detection only; nothing is auto-applied. Candidates must be verified (rename vs
delisting vs SME/suspended) and applied through the verified override/verify
pipeline. A failed master fetch SKIPS that market entirely (it never emits
candidates) so a network/session hiccup can never falsely flag a whole exchange.

Writes data/reports/delisting_report.{json,md}; in GitHub Actions also writes
``delisting_detected=true|false`` to ``$GITHUB_OUTPUT``.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TICKERS_CSV = ROOT / "data" / "tickers.csv"
REPORT_JSON = ROOT / "data" / "reports" / "delisting_report.json"
REPORT_MD = ROOT / "data" / "reports" / "delisting_report.md"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
US_EXCHANGES = {"NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "AMEX", "BATS"}

# Minimum plausible master size per market; below this we treat the fetch as
# failed and SKIP the market (never emit candidates from a truncated master).
MIN_MASTER = {"US": 9000, "TSE": 3000, "ASX": 1500, "NSE_IN": 2000, "BSE_IN": 3000}
BSE_STATUS_URL_TEMPLATE = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status={status}"
)


def evidence_observation_id(candidate: dict[str, str], observed_at: str) -> str:
    payload = "|".join([candidate.get("source_key", ""), candidate.get("source_url", ""), candidate.get("exchange", ""), candidate.get("ticker", ""), candidate.get("isin", ""), candidate.get("classification", ""), observed_at])
    return "obs_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _norm(sym: str) -> str:
    return sym.strip().upper().replace(".", "").replace("-", "")


# --------------------------------------------------------------------------- #
# Pure logic (unit-tested, no network)
# --------------------------------------------------------------------------- #
def holdings_by_exchange(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in rows:
        if r.get("asset_type") == "Stock":
            out.setdefault(r["exchange"], []).append(r)
    return out


def master_absent(holdings: list[dict], master_syms: set[str], master_isins: set[str]) -> list[dict]:
    """Holdings whose ticker (normalized) and ISIN are both absent from the master."""
    cand = []
    for r in holdings:
        sym_ok = _norm(r["ticker"]) in master_syms
        isin_ok = bool(r.get("isin")) and r["isin"] in master_isins
        if not sym_ok and not isin_ok:
            cand.append(r)
    return cand


def classify_bse(candidates: list[dict], delisted_isins: set[str], delisted_ids: set[str],
                 suspended_isins: set[str], suspended_ids: set[str]) -> list[dict]:
    out = []
    for r in candidates:
        iz = r.get("isin", "")
        tid = r["ticker"].strip().upper()
        if (iz and iz in delisted_isins) or tid in delisted_ids:
            status = "delisted"
        elif (iz and iz in suspended_isins) or tid in suspended_ids:
            status = "suspended"
        else:
            status = "master_absent"
        out.append({**r, "classification": status})
    return out


def compute_detected(candidate_keys: set[tuple[str, str]], prior_keys: set[tuple[str, str]] | None) -> bool:
    """Detect drift: candidate set changed vs the prior committed report."""
    if prior_keys is None:
        return bool(candidate_keys)
    return candidate_keys != prior_keys


# --------------------------------------------------------------------------- #
# Network fetchers (each returns (syms, isins) or raises; caller guards)
# --------------------------------------------------------------------------- #
def _session():
    import requests
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def fetch_us(session) -> tuple[set[str], set[str]]:
    syms: set[str] = set()
    for url, col in (
        ("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", 0),
        ("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", 0),
    ):
        text = session.get(url, timeout=60).text
        rdr = csv.reader(io.StringIO(text), delimiter="|")
        next(rdr, None)
        for row in rdr:
            if row and not row[0].startswith("File Creation"):
                syms.add(_norm(row[col]))
    return syms, set()


def fetch_jpx(session) -> tuple[set[str], set[str]]:
    import xlrd
    raw = session.get(
        "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
        timeout=90).content
    sh = xlrd.open_workbook(file_contents=raw).sheet_by_index(0)
    syms: set[str] = set()
    for r in range(1, sh.nrows):
        code = str(sh.cell_value(r, 1)).strip().replace(".0", "")
        if code:
            syms.add(_norm(code))
    return syms, set()


def fetch_asx(session) -> tuple[set[str], set[str]]:
    text = session.get("https://www.asx.com.au/asx/research/ASXListedCompanies.csv", timeout=90).text
    syms: set[str] = set()
    for line in io.StringIO(text):
        parts = next(csv.reader([line]), [])
        if len(parts) >= 2 and parts[1].strip() and parts[1].strip() != "ASX code":
            syms.add(_norm(parts[1]))
    return syms, set()


def fetch_nse(session) -> tuple[set[str], set[str]]:
    # NSE needs session cookies: hit the homepage first.
    session.get("https://www.nseindia.com/", timeout=30,
                headers={"Referer": "https://www.nseindia.com/"})
    syms: set[str] = set()
    isins: set[str] = set()
    for url in (
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://nsearchives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv",
    ):
        text = session.get(url, timeout=60, headers={"Referer": "https://www.nseindia.com/"}).text
        for r in csv.DictReader(io.StringIO(text)):
            sym = (r.get("SYMBOL") or "").strip()
            if sym:
                syms.add(_norm(sym))
            iz = (r.get(" ISIN NUMBER") or r.get("ISIN_NUMBER") or r.get("ISIN NUMBER") or "").strip()
            if iz:
                isins.add(iz)
    return syms, isins


def fetch_bse_status(session, status: str) -> tuple[set[str], set[str]]:
    """Return (scrip_id set, ISIN set) for one BSE status (Active/Suspended/Delisted)."""
    url = BSE_STATUS_URL_TEMPLATE.format(status=status)
    data = session.get(url, timeout=90, headers={
        "Origin": "https://www.bseindia.com",
        "Referer": "https://www.bseindia.com/corporates/List_Scrips.html",
        "Accept": "application/json",
    }).json()
    ids = {str(x.get("scrip_id", "")).strip().upper() for x in data if str(x.get("scrip_id", "")).strip()}
    isins = {str(x.get("ISIN_NUMBER", "")).strip() for x in data if str(x.get("ISIN_NUMBER", "")).strip()}
    return ids, isins


# --------------------------------------------------------------------------- #
def build_candidates() -> tuple[list[dict], list[str], list[dict]]:
    """Returns (candidates, markets_checked, markets_skipped[{market,reason}])."""
    rows = list(csv.DictReader(TICKERS_CSV.open(newline="", encoding="utf-8")))
    by_ex = holdings_by_exchange(rows)
    session = _session()
    candidates: list[dict] = []
    checked: list[str] = []
    skipped: list[dict] = []

    def market_holdings(*exchanges):
        out = []
        for ex in exchanges:
            out.extend(by_ex.get(ex, []))
        return out

    # --- US / JP / AU / NSE : master-absent diff ---
    simple = [
        ("US", US_EXCHANGES, fetch_us),
        ("TSE", {"TSE"}, fetch_jpx),
        ("ASX", {"ASX"}, fetch_asx),
        ("NSE_IN", {"NSE_IN"}, fetch_nse),
    ]
    for market, exchanges, fetcher in simple:
        try:
            syms, isins = fetcher(session)
        except Exception as exc:  # noqa: BLE001 - any fetch failure -> skip market
            skipped.append({"market": market, "reason": f"fetch failed: {type(exc).__name__}"})
            continue
        if len(syms) < MIN_MASTER.get(market, 1):
            skipped.append({"market": market, "reason": f"master too small ({len(syms)}); treated as failed"})
            continue
        checked.append(market)
        for r in master_absent(market_holdings(*exchanges), syms, isins):
            candidates.append({"exchange": r["exchange"], "ticker": r["ticker"],
                               "name": r.get("name", ""), "isin": r.get("isin", ""),
                               "classification": "master_absent"})

    # --- BSE : status-classified ---
    try:
        active_ids, active_isins = fetch_bse_status(session, "Active")
        del_ids, del_isins = fetch_bse_status(session, "Delisted")
        sus_ids, sus_isins = fetch_bse_status(session, "Suspended")
        if len(active_ids) < MIN_MASTER["BSE_IN"]:
            raise ValueError(f"active master too small ({len(active_ids)})")
        checked.append("BSE_IN")
        bse_absent = master_absent(by_ex.get("BSE_IN", []), active_ids, active_isins)
        candidates.extend(classify_bse(bse_absent, del_isins, del_ids, sus_isins, sus_ids))
    except Exception as exc:  # noqa: BLE001
        skipped.append({"market": "BSE_IN", "reason": f"fetch failed: {type(exc).__name__}: {exc}"})

    candidates.sort(key=lambda c: (c["exchange"], c["ticker"]))
    return candidates, checked, skipped


def build_markdown(summary: dict) -> str:
    from collections import Counter
    cls = Counter(c["classification"] for c in summary["candidates"])
    checked_str = ", ".join(summary["markets_checked"]) or "(none)"
    skipped_str = "; ".join(f"{s['market']} ({s['reason']})" for s in summary["markets_skipped"]) or "(none)"
    lines = [
        "# Delisting-candidate report", "",
        f"Generated: {summary['generated_at']}", "",
        f"**delisting_detected: {summary['delisting_detected']}**", "",
        f"Markets checked: {checked_str}",
        f"Markets skipped: {skipped_str}",
        "", f"Candidates: {len(summary['candidates'])} "
        f"(delisted={cls.get('delisted', 0)}, suspended={cls.get('suspended', 0)}, "
        f"master_absent={cls.get('master_absent', 0)})", "",
        "Detection only — verify each (delisting vs rename vs SME/suspended) and "
        "apply via the override/verify pipeline. `delisted` (BSE authoritative) are "
        "drop-ready; `master_absent` need rename-vs-delisting verification; "
        "`suspended` are kept by policy (can resume).", "",
        "| Exchange | Ticker | Classification | Name | ISIN |",
        "|---|---|---|---|---|",
    ]
    for c in summary["candidates"][:200]:
        lines.append(f"| {c['exchange']} | {c['ticker']} | {c['classification']} | "
                     f"{c['name'][:40]} | {c['isin'] or ''} |")
    if len(summary["candidates"]) > 200:
        lines.append(f"| … | … | … | (+{len(summary['candidates']) - 200} more) | |")
    return "\n".join(lines) + "\n"


def load_prior_keys() -> set[tuple[str, str]] | None:
    try:
        prior = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
        return {(c["exchange"], c["ticker"]) for c in prior.get("candidates", [])}
    except Exception:
        return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", default=None, help="override generated_at (ISO) for reproducible tests")
    args = parser.parse_args(argv)

    candidates, checked, skipped = build_candidates()
    prior_keys = load_prior_keys()
    cand_keys = {(c["exchange"], c["ticker"]) for c in candidates}
    detected = compute_detected(cand_keys, prior_keys)

    now = args.now or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for candidate in candidates:
        if candidate.get("exchange") == "BSE_IN" and candidate.get("classification") in {"delisted", "suspended"}:
            status = candidate["classification"].title()
            candidate["source_key"] = "bse_india_scrips"
            candidate["source_url"] = BSE_STATUS_URL_TEMPLATE.format(status=status)
            candidate["observed_at"] = now
            candidate["observation_id"] = evidence_observation_id(candidate, now)
    summary = {
        "generated_at": now,
        "markets_checked": checked,
        "markets_skipped": skipped,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "delisting_detected": detected,
    }
    REPORT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT_MD.write_text(build_markdown(summary), encoding="utf-8")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as handle:
            handle.write(f"delisting_detected={'true' if detected else 'false'}\n")

    from collections import Counter
    cls = Counter(c["classification"] for c in candidates)
    print(json.dumps({"markets_checked": checked, "markets_skipped": skipped,
                      "candidate_count": len(candidates), "by_class": dict(cls),
                      "delisting_detected": detected}, indent=2))


if __name__ == "__main__":
    main()
