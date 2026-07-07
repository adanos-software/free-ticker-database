"""Full-DB ISIN identity validation via OpenFIGI (free, deterministic).

Validates every primary ticker that carries an ISIN against OpenFIGI's
``ID_ISIN`` mapping — the one external check that can cover ALL rows for free.
For each ISIN, OpenFIGI returns the security/securities it identifies; we
classify identity consistency:

  * ``match``   - our ticker is among OpenFIGI's returned tickers, OR our name
                  shares a significant token with an OpenFIGI security name
                  (the ISIN genuinely belongs to our security).
  * ``mismatch``- OpenFIGI HAS data for the ISIN but neither the ticker nor the
                  name matches -> the ISIN likely belongs to a DIFFERENT
                  security (wrong/stale ISIN) -> review candidate.
  * ``no_data`` - OpenFIGI has no record (coverage gap, common for frontier
                  markets) -> NOT an error, cannot validate.

Name-token consistency is used instead of a naive ticker-equality check because
OpenFIGI's ticker convention differs from local exchange tickers outside the US
(that would produce thousands of false mismatches).

Incremental: results are cached in the report JSON keyed by ISIN plus a
ticker/name/exchange fingerprint; a re-run only queries ISINs not already cached
(or whose ticker/name changed), so the weekly CI is cheap after the first full
pass. Checkpoints every CHECKPOINT_EVERY ISINs.
Set OPENFIGI_API_KEY for larger batches / higher rate.

Detection only — nothing is auto-applied. Writes
data/reports/isin_validation_report.{json,md}; in GitHub Actions also writes
``isin_issues_detected=true|false`` to ``$GITHUB_OUTPUT``.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import unicodedata
import urllib.request
import urllib.error
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TICKERS_CSV = ROOT / "data" / "tickers.csv"
REPORT_JSON = ROOT / "data" / "reports" / "isin_validation_report.json"
REPORT_MD = ROOT / "data" / "reports" / "isin_validation_report.md"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
CHECKPOINT_EVERY = 2000
UA = "free-ticker-database/isin-validation (+https://github.com/adanos-software/free-ticker-database)"

STOPWORDS = {
    "inc", "incorporated", "corp", "corporation", "ltd", "limited", "plc", "sa",
    "sab", "se", "ag", "nv", "oyj", "asa", "spa", "holdings", "holding", "group",
    "company", "co", "the", "class", "common", "stock", "shares", "share", "of",
    "and", "fund", "etf", "trust", "ord", "ordinary", "ag.", "&", "de", "cv",
}


# --------------------------------------------------------------------------- #
# Pure logic (unit-tested, no network)
# --------------------------------------------------------------------------- #
# Non-decomposing letters that NFKD won't fold (ø, æ, ß, …) — map explicitly.
_TRANSLIT = str.maketrans({
    "ø": "o", "æ": "ae", "œ": "oe", "ð": "d", "þ": "th", "ß": "ss",
    "ł": "l", "đ": "d", "ı": "i", "ø": "o",
})


def _fold(s: str) -> str:
    """Lowercase + strip diacritics so 'Frøy'=='froy', 'Energía'=='energia'."""
    s = (s or "").lower().translate(_TRANSLIT)
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def norm_ticker(s: str) -> str:
    t = (s or "").strip().upper()
    for ch in ".- /":
        t = t.replace(ch, "")  # separators incl. LSE slash (RM vs RM/)
    # Asian exchanges zero-pad numeric codes (HKEX 00001, 00700) while OpenFIGI
    # returns them unpadded (1, 700) — strip leading zeros so they compare equal.
    return t.lstrip("0") or t


def name_tokens(name: str) -> set[str]:
    out = set()
    for w in "".join(c if c.isalnum() else " " for c in _fold(name)).split():
        if len(w) >= 3 and w not in STOPWORDS:
            out.add(w)
    return out


def _normname(name: str) -> str:
    return "".join(c for c in _fold(name) if c.isalnum())


def classify(our_ticker: str, our_name: str, figi_records: list[dict] | None) -> str:
    """Classify one ISIN's identity consistency vs OpenFIGI records."""
    if not figi_records:
        return "no_data"
    tks = {norm_ticker(r.get("ticker", "")) for r in figi_records if r.get("ticker")}
    if norm_ticker(our_ticker) in tks:
        return "match"
    ours = name_tokens(our_name)
    for r in figi_records:
        if ours & name_tokens(r.get("name", "")):
            return "match"
    # Fallback for names that tokenize to nothing (all short words / stopwords,
    # e.g. "LY Corp" vs "LY CORP LTD"): normalized-name containment.
    on = _normname(our_name)
    if len(on) >= 5:
        for r in figi_records:
            fn = _normname(r.get("name", ""))
            if fn and (on in fn or fn in on):
                return "match"
    return "mismatch"


def compute_detected(mismatch_keys: set[str], prior_mismatch_keys: set[str] | None) -> bool:
    if prior_mismatch_keys is None:
        return bool(mismatch_keys)
    return bool(mismatch_keys - prior_mismatch_keys)  # only NEW mismatches trigger


def mismatch_triage(entry: dict) -> dict[str, str]:
    figi_tickers = ", ".join(entry.get("figi_tickers", [])[:3]) or "none"
    figi_name = entry.get("figi_name", "") or "none"
    return {
        "triage_decision": "review_required_openfigi_resolves_different_security",
        "triage_bucket": "possible_wrong_or_stale_isin",
        "triage_rationale": (
            "OpenFIGI has data for this ISIN, but neither the normalized ticker nor issuer-name tokens "
            f"match the dataset row; OpenFIGI tickers={figi_tickers}; OpenFIGI name={figi_name}."
        ),
        "next_action": (
            "Verify against official exchange, issuer, CSD, or regulator evidence before correcting via "
            "metadata override; do not auto-apply from OpenFIGI mismatch alone."
        ),
    }


def enrich_mismatch(entry: dict) -> dict:
    enriched = dict(entry)
    enriched.update(mismatch_triage(entry))
    return enriched


def build_residual_triage(
    verdicts: dict[str, str],
    mismatches: list[dict],
    isin_rows: dict[str, dict],
) -> dict:
    mismatch_by_exchange = Counter(
        isin_rows.get(entry["isin"], {}).get("exchange", entry.get("exchange", "unknown"))
        for entry in mismatches
    )
    no_data_by_exchange = Counter(
        isin_rows.get(isin, {}).get("exchange", "unknown")
        for isin, verdict in verdicts.items()
        if verdict == "no_data"
    )
    mismatch_decisions = Counter(entry["triage_decision"] for entry in mismatches)
    no_data_count = sum(1 for verdict in verdicts.values() if verdict == "no_data")
    return {
        "policy": {
            "mismatch": (
                "OpenFIGI resolved the ISIN to a different ticker/name; keep as review-required until "
                "official evidence proves a correction, stale listing, cross-listing ambiguity, or provider limitation."
            ),
            "no_data": (
                "OpenFIGI returned no ID_ISIN mapping; classify as provider coverage gap, not a data error, "
                "until a stronger source proves otherwise."
            ),
            "no_auto_apply": "This report is detection and triage only; data changes must go through reviewed overrides.",
        },
        "mismatch_rows": len(mismatches),
        "no_data_rows": no_data_count,
        "mismatch_by_exchange": dict(sorted(mismatch_by_exchange.items())),
        "no_data_by_exchange": dict(sorted(no_data_by_exchange.items())),
        "mismatch_triage_decision_totals": dict(sorted(mismatch_decisions.items())),
        "no_data_triage_decision_totals": {
            "provider_coverage_gap_openfigi_no_data": no_data_count,
        },
        "remaining_unclassified_residuals": 0,
    }


# --------------------------------------------------------------------------- #
def load_isin_rows() -> dict[str, dict]:
    """Unique ISIN -> {ticker, exchange, name} (first primary row per ISIN)."""
    out: dict[str, dict] = {}
    with TICKERS_CSV.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            iz = (r.get("isin") or "").strip()
            if iz and iz not in out:
                out[iz] = {"ticker": r["ticker"], "exchange": r["exchange"], "name": r.get("name", "")}
    return out


def row_cache_fingerprint(row: dict[str, str]) -> str:
    """Stable row identity for cache invalidation when ticker/name/exchange moves."""
    return "\0".join(
        [
            (row.get("ticker") or "").strip().upper(),
            (row.get("exchange") or "").strip().upper(),
            (row.get("name") or "").strip(),
        ]
    )


def load_cache() -> tuple[dict[str, str], dict[str, dict], dict[str, str]]:
    """Return cached verdicts, mismatch detail, and row fingerprints by ISIN."""
    try:
        prior = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}, {}
    verdicts = dict(prior.get("verdict_cache", {}))
    detail = {m["isin"]: m for m in prior.get("mismatches", [])}
    context = dict(prior.get("cache_row_fingerprints", {}))
    return verdicts, detail, context


def select_valid_cache(
    isin_rows: dict[str, dict],
    cache_verdicts: dict[str, str],
    cache_detail: dict[str, dict],
    cache_context: dict[str, str],
) -> tuple[dict[str, str], dict[str, dict]]:
    """Carry forward only cached verdicts whose row context still matches."""
    verdicts: dict[str, str] = {}
    detail: dict[str, dict] = {}
    for isin, row in isin_rows.items():
        if isin not in cache_verdicts:
            continue
        current_fingerprint = row_cache_fingerprint(row)
        cached_fingerprint = cache_context.get(isin)
        if cached_fingerprint is None:
            # Legacy reports did not include fingerprints. Mismatch rows still
            # embed the prior ticker/name/exchange, so they can be invalidated.
            prior_detail = cache_detail.get(isin)
            if prior_detail and row_cache_fingerprint(prior_detail) != current_fingerprint:
                continue
        elif cached_fingerprint != current_fingerprint:
            continue
        verdicts[isin] = cache_verdicts[isin]
        if isin in cache_detail:
            detail[isin] = cache_detail[isin]
    return verdicts, detail


# Sentinel: this ISIN could not be queried (transient failure). It is left
# UNcached so the next run retries it — distinct from None (OpenFIGI returned
# no record, a legitimate cacheable verdict).
FAILED = object()


def openfigi_batch(isins: list[str], api_key: str | None) -> list:
    """Return one element per input ISIN, aligned: a data list / None
    (OpenFIGI has no record) on success, or FAILED on a hard query failure."""
    body = json.dumps([{"idType": "ID_ISIN", "idValue": i} for i in isins]).encode()
    headers = {"Content-Type": "application/json", "User-Agent": UA}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    for attempt in range(6):
        try:
            req = urllib.request.Request(OPENFIGI_URL, data=body, headers=headers)
            resp = json.loads(urllib.request.urlopen(req, timeout=45).read())
            if not isinstance(resp, list) or len(resp) != len(isins):
                time.sleep(5)  # malformed/short response -> retry, never misalign
                continue
            return [(r.get("data") if isinstance(r, dict) else None) for r in resp]
        except urllib.error.HTTPError as exc:
            time.sleep((8 * (attempt + 1)) if exc.code == 429 else 5)
        except Exception:
            time.sleep(5)
    return [FAILED] * len(isins)  # give up this batch -> retried next run, not cached


def write_report(verdicts: dict[str, str], mism_detail: dict[str, dict], queried: int,
                 isin_rows: dict[str, dict],
                 *, now: str, detected: bool, partial: bool) -> None:
    cls = Counter(verdicts.values())
    # keep mismatch detail only for ISINs still classified as mismatch
    mism = [enrich_mismatch(mism_detail[iz]) for iz in sorted(mism_detail)
            if verdicts.get(iz) == "mismatch"]
    residual_triage = build_residual_triage(verdicts, mism, isin_rows)
    summary = {
        "generated_at": now,
        "total_isins": len(verdicts),
        "queried_this_run": queried,
        "partial": partial,
        "by_verdict": dict(cls),
        "mismatch_count": len(mism),
        "isin_issues_detected": detected,
        "residual_triage": residual_triage,
        "mismatches": mism,
        "verdict_cache": dict(sorted(verdicts.items())),
        "cache_row_fingerprints": {
            iz: row_cache_fingerprint(isin_rows[iz])
            for iz in sorted(verdicts)
            if iz in isin_rows
        },
    }
    REPORT_JSON.write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# ISIN identity validation (OpenFIGI)", "",
        f"Generated: {now}{'  (PARTIAL run)' if partial else ''}", "",
        f"**isin_issues_detected: {detected}**", "",
        f"ISINs validated: {len(verdicts)} | match={cls.get('match', 0)} "
        f"mismatch={cls.get('mismatch', 0)} no_data={cls.get('no_data', 0)}", "",
        "Detection only. `mismatch` = OpenFIGI resolves the ISIN to a security whose "
        "ticker AND name differ from ours (likely wrong/stale ISIN) — verify before "
        "correcting via the override pipeline. `no_data` = OpenFIGI has no record "
        "(coverage gap, not an error).", "",
        "## Residual triage", "",
        f"- Mismatch residuals: `{residual_triage['mismatch_rows']}` "
        f"({', '.join(residual_triage['mismatch_triage_decision_totals'])})",
        f"- OpenFIGI no-data residuals: `{residual_triage['no_data_rows']}` "
        "(provider coverage gap)",
        f"- Remaining unclassified residuals: `{residual_triage['remaining_unclassified_residuals']}`",
        "",
        "### Mismatch residuals by exchange",
        "",
        "| Exchange | Rows |",
        "|---|---:|",
    ]
    for exchange, count in sorted(residual_triage["mismatch_by_exchange"].items(), key=lambda item: (-item[1], item[0]))[:30]:
        lines.append(f"| {exchange} | {count} |")
    lines.extend([
        "",
        "### OpenFIGI no-data residuals by exchange",
        "",
        "| Exchange | Rows |",
        "|---|---:|",
    ])
    for exchange, count in sorted(residual_triage["no_data_by_exchange"].items(), key=lambda item: (-item[1], item[0]))[:30]:
        lines.append(f"| {exchange} | {count} |")
    lines.extend([
        "",
        "## Mismatch review queue",
        "",
        "| ISIN | Our ticker | Our name | OpenFIGI ticker(s) | OpenFIGI name | Triage |",
        "|---|---|---|---|---|---|",
    ])
    for r in sorted(mism, key=lambda r: r["isin"])[:300]:
        lines.append(f"| {r['isin']} | {r['ticker']} | {r.get('name', '')[:30]} | "
                     f"{','.join(r.get('figi_tickers', [])[:3])} | {r.get('figi_name', '')[:30]} | "
                     f"{r['triage_decision']} |")
    if len(mism) > 300:
        lines.append(f"| … | | | (+{len(mism) - 300} more) | | |")
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="max ISINs to query this run (incremental)")
    parser.add_argument("--delay", type=float, default=2.6, help="seconds between batches (keyless rate)")
    parser.add_argument("--now", default=None)
    args = parser.parse_args(argv)

    api_key = os.environ.get("OPENFIGI_API_KEY") or None
    batch_size = 100 if api_key else 10
    delay = 0.4 if api_key else args.delay

    rows = load_isin_rows()
    cache_verdicts, cache_detail, cache_context = load_cache()
    prior_keys = ({iz for iz, v in cache_verdicts.items() if v == "mismatch"}
                  if cache_verdicts else None)
    # carry over cached verdicts/detail for ISINs still in the DB
    verdicts, mism_detail = select_valid_cache(rows, cache_verdicts, cache_detail, cache_context)
    to_query = [iz for iz in rows if iz not in verdicts]
    if args.limit:
        to_query = to_query[:args.limit]

    queried = 0
    for k in range(0, len(to_query), batch_size):
        batch = to_query[k:k + batch_size]
        data = openfigi_batch(batch, api_key)
        for iz, recs in zip(batch, data):
            if recs is FAILED:
                continue  # leave uncached; the next run retries it
            row = rows[iz]
            verdict = classify(row["ticker"], row["name"], recs)
            verdicts[iz] = verdict
            if verdict == "mismatch" and recs:
                mism_detail[iz] = {
                    "isin": iz, "ticker": row["ticker"], "exchange": row["exchange"],
                    "name": row["name"],
                    "figi_tickers": sorted({r.get("ticker", "") for r in recs if r.get("ticker")}),
                    "figi_name": recs[0].get("name", ""),
                }
            else:
                mism_detail.pop(iz, None)
        queried += len(batch)
        if queried % CHECKPOINT_EVERY < batch_size:
            now = args.now or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            write_report(verdicts, mism_detail, queried, rows, now=now, detected=False, partial=True)
        time.sleep(delay)

    mismatch_keys = {iz for iz, v in verdicts.items() if v == "mismatch"}
    detected = compute_detected(mismatch_keys, prior_keys)
    partial = any(iz not in verdicts for iz in rows)
    now = args.now or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_report(verdicts, mism_detail, queried, rows, now=now, detected=detected, partial=partial)

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as handle:
            handle.write(f"isin_issues_detected={'true' if detected else 'false'}\n")

    print(json.dumps({
        "total_isins_in_db": len(rows), "validated": len(verdicts),
        "queried_this_run": queried, "partial": partial,
        "by_verdict": dict(Counter(verdicts.values())),
        "isin_issues_detected": detected,
    }, indent=2))


if __name__ == "__main__":
    main()
