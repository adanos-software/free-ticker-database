"""Build a reviewable queue of ISIN identity collisions.

An ISIN identifies exactly one security/issuer. When the same ISIN is attached
to two or more listings whose issuer names belong to clearly different issuers
(no shared significant name token), at least one of those listings carries a
wrong ISIN. This is a provable data anomaly, independent of any external source.

The most common root cause in this dataset is a ticker-keyed identifier backfill
that attached a foreign issuer's ISIN to an unrelated listing that merely shares
the same trading symbol (for example a US-listed fund inheriting an Austrian or
Australian ISIN because both trade under the same ticker).

This script only *reports and gates* the collisions. It never edits, blanks, or
reassigns any ISIN, country, or name. Per the data-truth policy, the correct
identifier must come from an official numbering agency, issuer, or exchange
security master keyed to the exact listing before any change is applied.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_LISTINGS_CSV = ROOT / "data" / "listings.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "isin_identity_collision_review_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "isin_identity_collision_review_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "isin_identity_collision_review_queue.md"

REVIEW_QUEUE = "manual_isin_identity_review"
REVIEW_GATE = (
    "Do not change, blank, or reassign any ISIN, country, or name until official "
    "listing-keyed identifier evidence (national numbering agency / issuer / exchange "
    "security master) confirms which listing holds the ISIN."
)
RECOMMENDED_NEXT_SOURCE = (
    "Official national numbering agency (ISIN registry) or issuer/exchange security "
    "master keyed to the exact listing_key."
)
CLOSURE_STATUS_OPEN = "open_needs_official_identifier_evidence"

CSV_FIELDNAMES = [
    "isin",
    "registered_country",
    "isin_valid",
    "cluster_count",
    "listing_count",
    "listing_keys",
    "member_names",
    "member_exchanges",
    "member_tickers",
    "shared_tickers",
    "collision_signals",
    "decision_candidate",
    "review_queue",
    "review_gate",
    "recommended_next_source",
    "closure_status",
]

# Corporate-form suffixes used both to detect full issuer names and to strip noise
# before name comparison so that share-class and depositary variants of the same
# issuer cluster together.
_SUFFIX_RE = re.compile(
    r"\b("
    r"LIMITED|LTD|INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO|PLC|LLC|LP|"
    r"AG|SA|SE|NV|BV|OYJ|ASA|AB|SPA|BHD|TBK|PCL|PJSC|GMBH|KGAA|"
    r"GROUP|HOLDING|HOLDINGS|PARTNERS|TRUST|FUND|REIT|"
    r"CLASS|ORD|ORDINARY|COMMON|STOCK|SHARES|SHARE|ADR|GDR|SPONSORED|UNSPONSORED|"
    r"DEPOSITARY|RECEIPTS|UNIT|UNITS|THE|NEW|REGISTERED"
    r")\b"
)

# A trailing trading-currency code appended by some venue feeds (for example SGX
# "HongkongLand USD"). Only a *trailing* code is stripped, and only during
# token/compact-key comparison -- never used to decide whether a name is a full
# issuer name. This keeps an abbreviated mnemonic like "DFIRG USD" out of the
# full-name set, and leaves a mid-name currency token in place (so heavily
# abbreviated product names that happen to share one are not split apart). A bare
# trailing currency token never identifies an issuer, so stripping it cannot merge
# two genuinely distinct issuers.
_TRAILING_CURRENCY_RE = re.compile(
    r"(?:\s|^)("
    r"USD|EUR|GBP|GBX|CHF|HKD|SGD|JPY|CNY|CNH|AUD|CAD|NZD|SEK|NOK|DKK|"
    r"ZAR|INR|KRW|TWD|THB|MYR|IDR|BRL|MXN|ILS|AED|SAR|PLN|HUF|CZK"
    r")\s*$"
)
_STOP_TOKENS = {
    "A",
    "B",
    "C",
    "D",
    "H",
    "N",
    "AND",
    "OF",
    "DE",
    "DA",
    "DO",
    "EL",
    "LA",
}

# Transliterations for letters that do not decompose under NFKD, so that the same
# issuer written with native orthography (for example "Møller"/"Mærsk") and its
# ASCII spelling ("Moeller"/"Maersk") tokenise to the same form.
_TRANSLITERATIONS = {
    "Ø": "O",
    "ø": "o",
    "Æ": "AE",
    "æ": "ae",
    "Å": "A",
    "å": "a",
    "ß": "ss",
    "Đ": "D",
    "đ": "d",
    "Ð": "D",
    "ð": "d",
    "Þ": "TH",
    "þ": "th",
    "Ł": "L",
    "ł": "l",
    "Œ": "OE",
    "œ": "oe",
}

# German-style expansion of umlauts. Applied as an *alternative* folding so that
# both "Suedzucker"/"Südzucker" (German oe/ue/ae convention) and
# "Huhtamaki"/"Huhtamäki" (Nordic diacritic-drop convention) reconcile, even
# though the two conventions disagree on the same code point.
_GERMAN_EXPANSIONS = {
    "Ä": "AE",
    "ä": "ae",
    "Ö": "OE",
    "ö": "oe",
    "Ü": "UE",
    "ü": "ue",
}

_DROP_TABLE = str.maketrans(_TRANSLITERATIONS)
_GERMAN_TABLE = str.maketrans({**_TRANSLITERATIONS, **_GERMAN_EXPANSIONS})


def _strip_combining(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def fold_unicode(text: str) -> str:
    """Fold accents and non-ASCII letters to a stable ASCII form for comparison.

    Diacritics are dropped (the Nordic convention: ``ä`` -> ``a``); the German
    expansion is handled separately by :func:`fold_unicode_german`.
    """

    return _strip_combining(text.translate(_DROP_TABLE))


def fold_unicode_german(text: str) -> str:
    """Fold using the German umlaut convention (``ä`` -> ``ae``, ``ü`` -> ``ue``)."""

    return _strip_combining(text.translate(_GERMAN_TABLE))


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _strip_noise(name: str, fold) -> str:
    cleaned = _SUFFIX_RE.sub(" ", fold(name or "").upper())
    return _TRAILING_CURRENCY_RE.sub(" ", cleaned)


def _name_tokens_with(name: str, fold) -> set[str]:
    cleaned = re.sub(r"[^A-Z0-9 ]", " ", _strip_noise(name, fold))
    return {tok for tok in cleaned.split() if len(tok) > 1 and tok not in _STOP_TOKENS}


def _compact_key_with(name: str, fold) -> str:
    return re.sub(r"[^A-Z0-9]", "", _strip_noise(name, fold))


def name_tokens(name: str) -> set[str]:
    """Significant uppercase tokens of an issuer name, stripped of corporate forms."""

    return _name_tokens_with(name, fold_unicode)


def compact_name_key(name: str) -> str:
    """Suffix-stripped alphanumeric fingerprint, collapsing spacing/punctuation.

    Variants of one issuer such as ``Lend Lease Group`` / ``Lendlease`` or
    ``Moody's`` / ``Moodys`` collapse to the same order-preserving key, while
    distinct issuers keep distinct keys. Returns an empty string when no
    significant characters remain.
    """

    return _compact_key_with(name, fold_unicode)


def name_token_variants(name: str) -> set[str]:
    """Tokens under both the diacritic-drop and German umlaut folding conventions."""

    return _name_tokens_with(name, fold_unicode) | _name_tokens_with(name, fold_unicode_german)


def compact_key_variants(name: str) -> set[str]:
    """Compact keys under both folding conventions (empty keys excluded)."""

    return {
        key
        for key in (
            _compact_key_with(name, fold_unicode),
            _compact_key_with(name, fold_unicode_german),
        )
        if key
    }


def is_full_name(name: str) -> bool:
    """Whether a name is a full issuer name rather than an exchange trading mnemonic.

    Abbreviated uppercase trading names (for example ``CHINA RES BEER``) are not
    reliable for distinguishing issuers, so only full names participate in the
    disjoint-issuer test. A name qualifies if it carries a recognised corporate
    suffix or is mixed-case with at least two words.
    """

    if not name:
        return False
    if _SUFFIX_RE.search(name.upper()):
        return True
    return any(ch.islower() for ch in name) and len(name.split()) >= 2


def cluster_listings_by_name(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Cluster listings that share at least one significant name token.

    Greedy single-link clustering: a listing joins the first existing cluster it
    shares a token with, otherwise it starts a new cluster. Listings whose names
    are not full issuer names (trading mnemonics) are ignored for clustering.
    """

    clusters: list[dict[str, Any]] = []
    for row in rows:
        name = row.get("name", "")
        if not is_full_name(name):
            continue
        tokens = name_token_variants(name)
        if not tokens:
            continue
        compacts = compact_key_variants(name)
        target: dict[str, Any] | None = None
        for cluster in clusters:
            if (cluster["tokens"] & tokens) or (compacts & cluster["compact_keys"]):
                target = cluster
                break
        if target is None:
            target = {"tokens": set(), "compact_keys": set(), "rows": []}
            clusters.append(target)
        target["tokens"] |= tokens
        target["compact_keys"] |= compacts
        target["rows"].append(row)
    return clusters


def shared_tickers_across_clusters(clusters: list[dict[str, Any]]) -> list[str]:
    """Tickers that appear in two or more distinct name clusters."""

    ticker_clusters: dict[str, set[int]] = defaultdict(set)
    for index, cluster in enumerate(clusters):
        for row in cluster["rows"]:
            ticker = (row.get("ticker", "") or "").strip().upper()
            if ticker:
                ticker_clusters[ticker].add(index)
    return sorted(ticker for ticker, indices in ticker_clusters.items() if len(indices) > 1)


def join_sorted_unique(values: list[str]) -> str:
    return " | ".join(sorted({value for value in values if value}))


def build_collision_row(
    isin: str,
    clusters: list[dict[str, Any]],
    *,
    isin_valid_fn,
    country_from_isin_fn,
) -> dict[str, Any]:
    members = [row for cluster in clusters for row in cluster["rows"]]
    shared = shared_tickers_across_clusters(clusters)
    signals = ["name_clusters_disjoint"]
    if shared:
        signals.append("ticker_collision")
        decision = "ticker_collision_isin_misassignment_suspected"
    else:
        decision = "isin_shared_by_distinct_issuers"
    registered_country = country_from_isin_fn(isin) or ""
    if registered_country:
        signals.append("registered_country_known")
    else:
        signals.append("registered_country_unknown")
    listing_keys = sorted(row.get("listing_key", "") for row in members if row.get("listing_key"))
    cluster_names = [cluster["rows"][0].get("name", "") for cluster in clusters]
    return {
        "isin": isin,
        "registered_country": registered_country,
        "isin_valid": "true" if isin_valid_fn(isin) else "false",
        "cluster_count": str(len(clusters)),
        "listing_count": str(len(members)),
        "listing_keys": "|".join(listing_keys),
        "member_names": join_sorted_unique(cluster_names),
        "member_exchanges": join_sorted_unique([row.get("exchange", "") for row in members]),
        "member_tickers": join_sorted_unique([row.get("ticker", "") for row in members]),
        "shared_tickers": "|".join(shared),
        "collision_signals": ";".join(signals),
        "decision_candidate": decision,
        "review_queue": REVIEW_QUEUE,
        "review_gate": REVIEW_GATE,
        "recommended_next_source": RECOMMENDED_NEXT_SOURCE,
        "closure_status": CLOSURE_STATUS_OPEN,
        "_members": [
            {
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "exchange": row.get("exchange", ""),
                "name": row.get("name", ""),
                "country": row.get("country", ""),
                "country_code": row.get("country_code", ""),
                "asset_type": row.get("asset_type", ""),
            }
            for row in sorted(members, key=lambda r: r.get("listing_key", ""))
        ],
    }


def build_queue_rows(
    listings_rows: list[dict[str, str]],
    *,
    isin_valid_fn,
    country_from_isin_fn,
) -> list[dict[str, Any]]:
    by_isin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in listings_rows:
        isin = (row.get("isin", "") or "").strip()
        if isin:
            by_isin[isin].append(row)

    queue_rows: list[dict[str, Any]] = []
    for isin, rows in by_isin.items():
        if len(rows) < 2:
            continue
        clusters = cluster_listings_by_name(rows)
        if len(clusters) < 2:
            continue
        queue_rows.append(
            build_collision_row(
                isin,
                clusters,
                isin_valid_fn=isin_valid_fn,
                country_from_isin_fn=country_from_isin_fn,
            )
        )
    queue_rows.sort(
        key=lambda row: (
            0 if row["decision_candidate"] == "ticker_collision_isin_misassignment_suspected" else 1,
            -int(row["listing_count"]),
            row["isin"],
        )
    )
    return queue_rows


def next_review_batches(queue_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: Counter[tuple[str, str]] = Counter()
    for row in queue_rows:
        grouped[(row["decision_candidate"], row["registered_country"] or "unknown")] += 1
    batches = [
        {
            "decision_candidate": decision,
            "registered_country": country,
            "collision_groups": count,
        }
        for (decision, country), count in grouped.items()
    ]
    batches.sort(key=lambda batch: (-batch["collision_groups"], batch["decision_candidate"], batch["registered_country"]))
    return batches


def summarize(queue_rows: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    decision_totals = Counter(row["decision_candidate"] for row in queue_rows)
    country_totals = Counter(row["registered_country"] or "unknown" for row in queue_rows)
    listings_involved = sum(int(row["listing_count"]) for row in queue_rows)
    ticker_collision_groups = sum(
        1 for row in queue_rows if row["decision_candidate"] == "ticker_collision_isin_misassignment_suspected"
    )
    return {
        "generated_at": generated_at,
        "collision_groups": len(queue_rows),
        "listings_involved": listings_involved,
        "ticker_collision_groups": ticker_collision_groups,
        "decision_candidate_totals": dict(sorted(decision_totals.items())),
        "registered_country_totals": dict(sorted(country_totals.items())),
        "open_groups": sum(1 for row in queue_rows if row["closure_status"] == CLOSURE_STATUS_OPEN),
        "closed_groups": sum(1 for row in queue_rows if row["closure_status"] != CLOSURE_STATUS_OPEN),
        "direct_identifier_apply_allowed_rows": 0,
        "next_review_batches": next_review_batches(queue_rows),
        "policy": (
            "An ISIN identifies exactly one issuer; an ISIN shared by distinct issuer "
            "names is a provable anomaly. This queue reports and gates the collisions and "
            "applies no ISIN, country, or name change without official listing-keyed evidence."
        ),
    }


def build_payload(
    *,
    listings_csv: Path,
    isin_valid_fn,
    country_from_isin_fn,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    listings_rows = load_csv(listings_csv)
    queue_rows = build_queue_rows(
        listings_rows,
        isin_valid_fn=isin_valid_fn,
        country_from_isin_fn=country_from_isin_fn,
    )
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {"listings_csv": display_path(listings_csv)},
            "policy": (
                "ISIN identity collisions are reported for review only. No ISIN, country, "
                "name, or scope change is authorized without official listing-keyed evidence."
            ),
        },
        "summary": summarize(queue_rows, generated_at=generated_at),
        "items": queue_rows,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def markdown_escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# ISIN Identity Collision Review Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: an ISIN identifies exactly one issuer. This report flags ISINs shared by "
        "distinct issuer names (a provable anomaly) and applies no ISIN, country, or name "
        "change without official listing-keyed evidence.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Collision groups | {summary['collision_groups']} |",
        f"| Listings involved | {summary['listings_involved']} |",
        f"| Ticker-collision groups | {summary['ticker_collision_groups']} |",
        f"| Open groups | {summary['open_groups']} |",
        f"| Direct identifier apply allowed rows | {summary['direct_identifier_apply_allowed_rows']} |",
        "",
        "## Decision Candidates",
        "",
        "| Decision candidate | Groups |",
        "| --- | ---: |",
    ]
    for decision, count in summary["decision_candidate_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Next Review Batches", "", "| Decision candidate | Registered country | Groups |", "| --- | --- | ---: |"])
    for batch in summary["next_review_batches"][:25]:
        lines.append(
            f"| {markdown_escape(batch['decision_candidate'])} "
            f"| {markdown_escape(batch['registered_country'])} "
            f"| {batch['collision_groups']} |"
        )
    lines.extend(
        [
            "",
            "## Highest-Risk Groups",
            "",
            "| ISIN | Registered country | Listings | Shared tickers | Names |",
            "| --- | --- | ---: | --- | --- |",
        ]
    )
    for row in payload["items"][:40]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["isin"]),
                    markdown_escape(row["registered_country"] or "unknown"),
                    row["listing_count"],
                    markdown_escape(row["shared_tickers"] or "-"),
                    markdown_escape(row["member_names"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Gate",
            "",
            f"- {REVIEW_GATE}",
            f"- Recommended next source: {RECOMMENDED_NEXT_SOURCE}",
            "- Direct identifier apply allowed rows: `0`.",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], csv_out: Path, json_out: Path, md_out: Path) -> None:
    write_csv(csv_out, payload["items"])
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(render_markdown(payload), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an ISIN identity collision review queue.")
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    from scripts.rebuild_dataset import country_from_isin, is_valid_isin

    args = parse_args()
    payload = build_payload(
        listings_csv=args.listings_csv,
        isin_valid_fn=is_valid_isin,
        country_from_isin_fn=country_from_isin,
    )
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        f"Wrote {payload['summary']['collision_groups']} ISIN identity collision groups "
        f"({payload['summary']['ticker_collision_groups']} ticker-collision suspected) "
        f"covering {payload['summary']['listings_involved']} listings."
    )


if __name__ == "__main__":
    main()
