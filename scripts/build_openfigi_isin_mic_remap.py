"""Audit explicit OpenFIGI levels using only valid ISIN-plus-MIC requests."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_sec_sic_sectors import normalized_ticker_key
from scripts.build_canonical_v4 import _load_mic_mapping
from scripts.lib.dataio import load_csv, write_csv, write_json
from scripts.lib.normalize import _matched_name_token_count, names_match, significant_name_tokens
from scripts.lib.official_change_evidence import is_valid_isin
from scripts.lib.reference_audit import sha256, write_report
from scripts.lib.keys import split_listing_key

URL = "https://api.openfigi.com/v3/mapping"
COMMITTED_PROBE_MAX_REQUESTS = 200
STOLEN_ISIN_CANDIDATES_PRIOR_NAMES_MATCH = 21961
FIELDS = ["listing_key", "isin", "operating_mic", "segment_mic", "figi_share_class", "figi_composite", "figi_venue",
          "local_figi", "figi_level_agreement", "securityType", "name_family_flag", "stolen_isin_candidate", "reason", "status"]
REVIEW_QUEUE_FIELDS = [
    "listing_key", "exchange", "ticker", "isin", "operating_mic", "segment_mic", "local_name", "openfigi_name",
    "local_figi", "figi_venue", "figi_level_agreement", "securityType", "name_mismatch_confidence", "reason",
    "apply_allowed",
]


def request_for(row: dict, mapping: dict) -> dict:
    return {"idType": "ID_ISIN", "idValue": row["isin"],
            "micCode": mapping.get("segment_mic") or mapping["operating_mic"], "includeUnlistedEquities": False}


def request_key(job: dict) -> str:
    return hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()


def classify(row: dict, local_figi: str, result: dict) -> dict:
    output = {"local_figi": local_figi, "stolen_isin_candidate": False, "reason": "",
              "figi_level_agreement": "unresolved", "name_family_flag": "not_scored"}
    if "error" in result:
        return {**output, "status": "api_error", "reason": str(result["error"])}
    if ("data" not in result and "warning" not in result) or ("data" in result and not isinstance(result["data"], list)):
        raise ValueError("Malformed OpenFIGI mapping result")
    data = result.get("data", [])
    if any(not isinstance(item, dict) for item in data):
        raise ValueError("Malformed OpenFIGI instrument")
    if not data:
        return {**output, "status": "unmapped", "reason": result.get("warning", "No listed ISIN/MIC mapping")}
    matches = [item for item in data if normalized_ticker_key(item.get("ticker", "")) == normalized_ticker_key(row["ticker"])]
    if not matches:
        return {**output, "status": "ticker_not_resolved", "reason": "ISIN/MIC response has no exact normalized local ticker"}
    for field in ["shareClassFIGI", "compositeFIGI", "figi"]:
        if len({item.get(field) for item in matches if item.get(field)}) > 1:
            return {**output, "status": "ambiguous_mapping", "reason": f"Multiple {field} values for the same ticker and MIC"}
    item = matches[0]
    output.update(figi_share_class=item.get("shareClassFIGI", ""), figi_composite=item.get("compositeFIGI", ""),
                  figi_venue=item.get("figi", ""), securityType=item.get("securityType", ""), status="mapped",
                  openfigi_name=item.get("name", ""))
    if not local_figi:
        output["figi_level_agreement"] = "local_figi_missing"
    elif local_figi == item.get("figi"):
        output["figi_level_agreement"] = "venue_match"
    elif local_figi == item.get("compositeFIGI"):
        output["figi_level_agreement"] = "local_composite_level"
    elif local_figi == item.get("shareClassFIGI"):
        output["figi_level_agreement"] = "local_share_class_level"
    else:
        output["figi_level_agreement"] = "local_level_unknown_or_disagreement"
    # An unknown local FIGI level alone never proves an ISIN error.
    if row.get("name") and item.get("name"):
        compatible = names_match(row["name"], item["name"])
        output["name_family_flag"] = "compatible" if compatible else "review_name_family"
        if not compatible:
            output.update(stolen_isin_candidate=True, reason="Exact ticker/MIC maps the current ISIN to a different name family; review only, OpenFIGI is not official identity evidence")
    return output


def name_mismatch_confidence(local_name: str, openfigi_name: str) -> float:
    source_tokens = significant_name_tokens(local_name)
    target_tokens = significant_name_tokens(openfigi_name)
    if not source_tokens or not target_tokens:
        return 1.0
    matched = _matched_name_token_count(source_tokens, target_tokens)
    overlap = matched / max(len(source_tokens), len(target_tokens))
    return round(max(0.0, 1.0 - overlap), 3)


def build_review_queue(rows: list[dict], listings: dict[str, dict]) -> list[dict]:
    queue = []
    for row in rows:
        if not row.get("stolen_isin_candidate"):
            continue
        listing = listings[row["listing_key"]]
        exchange, ticker = split_listing_key(row["listing_key"])
        openfigi_name = row.get("openfigi_name", "")
        queue.append({
            "listing_key": row["listing_key"],
            "exchange": exchange,
            "ticker": ticker,
            "isin": row["isin"],
            "operating_mic": row["operating_mic"],
            "segment_mic": row.get("segment_mic", ""),
            "local_name": listing.get("name", ""),
            "openfigi_name": openfigi_name,
            "local_figi": row.get("local_figi", ""),
            "figi_venue": row.get("figi_venue", ""),
            "figi_level_agreement": row.get("figi_level_agreement", ""),
            "securityType": row.get("securityType", ""),
            "name_mismatch_confidence": name_mismatch_confidence(listing.get("name", ""), openfigi_name),
            "reason": row.get("reason", ""),
            "apply_allowed": 0,
        })
    return sorted(queue, key=lambda item: (-item["name_mismatch_confidence"], item["listing_key"]))


def _counter_breakdown(rows: list[dict], key_field: str, status_field: str = "status") -> dict[str, dict[str, int]]:
    breakdown: dict[str, Counter] = {}
    for row in rows:
        key = row.get(key_field, "") or "unknown"
        breakdown.setdefault(key, Counter())[row.get(status_field, "unknown")] += 1
    return {key: dict(sorted(counter.items())) for key, counter in sorted(breakdown.items())}


def _append_report_sections(md_path: Path, *, figi_levels: dict[str, int], by_exchange: dict, by_mic: dict,
                            cache_manifest: dict[str, object]) -> None:
    lines = md_path.read_text(encoding="utf-8").splitlines()
    lines.extend(["", "## FIGI level agreement (mapped rows only)", "", "| Level | Count |", "| --- | ---: |"])
    lines.extend(f"| {level} | {count} |" for level, count in sorted(figi_levels.items()))
    lines.extend(["", "## Cache manifest", ""])
    for key, value in cache_manifest.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Status by exchange (top 15 by eligible rows)", ""])
    top_exchanges = sorted(by_exchange.items(), key=lambda item: sum(item[1].values()), reverse=True)[:15]
    for exchange, counts in top_exchanges:
        lines.append(f"- `{exchange}`: {counts}")
    lines.extend(["", "## Status by operating MIC (top 15)", ""])
    top_mics = sorted(by_mic.items(), key=lambda item: sum(item[1].values()), reverse=True)[:15]
    for mic, counts in top_mics:
        lines.append(f"- `{mic}`: {counts}")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")


def fetch_jobs(jobs: list[dict], cache_dir: Path) -> None:
    api_key = os.environ.get("OPENFIGI_API_KEY", "")
    headers = {"X-OPENFIGI-APIKEY": api_key} if api_key else {}
    size, delay = (100, 0.3) if api_key else (10, 2.5)
    cache_dir.mkdir(parents=True, exist_ok=True)
    pending = [job for job in jobs if not (cache_dir / (request_key(job) + ".json")).exists()]
    with requests.Session() as session:
        for start in range(0, len(pending), size):
            batch = pending[start:start + size]
            for attempt in range(3):
                response = session.post(URL, json=batch, headers=headers, timeout=45)
                if response.status_code != 429:
                    break
                time.sleep(float(response.headers.get("retry-after", "60")))
            response.raise_for_status()
            results = response.json()
            if not isinstance(results, list) or len(results) != len(batch):
                raise ValueError("OpenFIGI returned an incomplete batch")
            for job, result in zip(batch, results, strict=True):
                if not isinstance(result, dict):
                    raise ValueError("OpenFIGI mapping result must be an object")
                write_json(cache_dir / (request_key(job) + ".json"),
                           {"request": job, "response": result, "fetched_at": datetime.now(timezone.utc).isoformat()})
            time.sleep(delay)


def run(listings_csv: Path, identifiers_csv: Path, mapping_csv: Path, cache_dir: Path,
        out_dir: Path, *, fetch: bool = False, limit: int = 0, probe_json: Path | None = None,
        write_full_probe: bool = False, cache_tarball_sha256: str = "") -> dict:
    before = {path: sha256(path) for path in [listings_csv, identifiers_csv]}
    mapping = _load_mic_mapping(mapping_csv)
    identifiers = {r["listing_key"]: r.get("figi", "") for r in load_csv(identifiers_csv)}
    eligible = [r for r in load_csv(listings_csv) if is_valid_isin(r.get("isin", ""))
                and mapping.get(r["exchange"], {}).get("operating_mic")]
    jobs = {request_key(request_for(r, mapping[r["exchange"]])): request_for(r, mapping[r["exchange"]]) for r in eligible}
    selected = sorted(jobs)[:limit or None]
    if fetch:
        fetch_jobs([jobs[key] for key in selected], cache_dir)
    results, sources = {}, []
    if probe_json is not None:
        probe = json.loads(probe_json.read_text())
        if probe.get("endpoint") != URL:
            raise ValueError("Pinned probe is not from the OpenFIGI mapping endpoint")
        for record in probe["requests"]:
            key = request_key(record["request"])
            response = record["response"]
            if key != record["request_key"] or hashlib.sha256(json.dumps(response, sort_keys=True).encode()).hexdigest() != record["response_sha256"]:
                raise ValueError("Pinned OpenFIGI probe hash mismatch")
            if key in jobs and record["request"] == jobs[key]:
                results[key] = response
    for key in sorted(jobs):
        path = cache_dir / (key + ".json")
        if path.exists():
            cached = json.loads(path.read_text())
            if cached.get("request") != jobs[key] or not isinstance(cached.get("response"), dict):
                raise ValueError(f"Cache request mismatch: {path.name}")
            results[key] = cached["response"]
    for key, response in sorted(results.items()):
        sources.append({"request_key": key, "response": response,
                        "response_sha256": hashlib.sha256(json.dumps(response, sort_keys=True).encode()).hexdigest()})
    listings_by_key = {row["listing_key"]: row for row in eligible}
    rows = []
    for row in eligible:
        venue = mapping[row["exchange"]]
        key = request_key(request_for(row, venue))
        result = classify(row, identifiers.get(row["listing_key"], ""), results[key]) if key in results else {"status": "not_probed", "stolen_isin_candidate": False, "reason": "No pinned response; not an unmapped or failed listing"}
        rows.append({"listing_key": row["listing_key"], "isin": row["isin"], "operating_mic": venue["operating_mic"],
                     "segment_mic": venue.get("segment_mic", ""), "local_figi": identifiers.get(row["listing_key"], ""), **result})
    mapped_rows = [row for row in rows if row.get("status") == "mapped"]
    figi_levels = dict(sorted(Counter(row.get("figi_level_agreement", "unknown") for row in mapped_rows).items()))
    by_exchange = _counter_breakdown([{**row, "exchange": listings_by_key[row["listing_key"]]["exchange"]} for row in rows], "exchange")
    by_mic = _counter_breakdown(rows, "operating_mic")
    stolen_count = sum(r["stolen_isin_candidate"] for r in rows)
    summary = dict(sorted(Counter(r["status"] for r in rows).items()))
    summary.update(eligible_listings=len(eligible), unique_isin_mic_requests=len(jobs), cached_requests=len(results),
                   response_with_data=sum(1 for response in results.values() if response.get("data")),
                   response_with_warning=sum(1 for response in results.values() if "warning" in response and not response.get("data")),
                   stolen_isin_review_candidates=stolen_count,
                   stolen_isin_review_candidates_before_names_match_tightening=STOLEN_ISIN_CANDIDATES_PRIOR_NAMES_MATCH,
                   figi_level_agreement=figi_levels, breakdown_by_exchange=by_exchange, breakdown_by_operating_mic=by_mic,
                   cache_tarball_sha256=cache_tarball_sha256 or "not supplied",
                   repo_main_sha_at_fetch="55c95a410485b557c72a55e40983ff6ff8f18d75",
                   openfigi_fetch_window_utc="2026-10-31T06:19:58Z/2026-10-31T06:32:00Z")
    pin = out_dir / "openfigi_isin_mic_remap_probe.json"
    manifest_path = out_dir / "openfigi_isin_mic_remap_probe.manifest.json"
    probe_payload = {"endpoint": URL, "requests": [{"request": jobs[source["request_key"]], **source} for source in sources]}
    if len(sources) <= COMMITTED_PROBE_MAX_REQUESTS or write_full_probe:
        write_json(pin, probe_payload)
        source_sha = sha256(pin)
        manifest = {"mode": "inline_probe", "request_count": len(sources), "probe_sha256": source_sha}
    else:
        manifest = {
            "mode": "external_cache",
            "request_count": len(sources),
            "cached_requests": len(results),
            "cache_dir": str(cache_dir.relative_to(ROOT)) if cache_dir.is_relative_to(ROOT) else str(cache_dir),
            "cache_tarball_sha256": cache_tarball_sha256 or "not supplied",
            "committed_probe_path": str(pin.relative_to(ROOT)),
            "committed_probe_sha256": sha256(pin) if pin.exists() else "",
            "note": "Full probe not committed; replay uses extracted cache or the pinned 200-request probe.",
        }
        write_json(manifest_path, manifest)
        source_sha = sha256(manifest_path)
    review_queue = build_review_queue(rows, listings_by_key)
    review_prefix = out_dir / "openfigi_stolen_isin_review_queue"
    write_csv(review_prefix.with_suffix(".csv"), REVIEW_QUEUE_FIELDS, review_queue)
    review_payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "summary": {
            "queue_rows": len(review_queue),
            "apply_allowed_rows": 0,
            "direct_identifier_apply_allowed_rows": 0,
            "stolen_isin_review_candidates_before_names_match_tightening": STOLEN_ISIN_CANDIDATES_PRIOR_NAMES_MATCH,
            "stolen_isin_review_candidates_after_names_match_tightening": stolen_count,
        },
        "limitations": [
            "Review only. OpenFIGI name-family differences are not proof of a stolen ISIN.",
            "apply_allowed is 0 on every row; do not auto-apply ISIN or identity changes from this queue.",
        ],
    }
    write_json(review_prefix.with_suffix(".json"), review_payload)
    review_md = [
        "# OpenFIGI stolen-ISIN review queue",
        "",
        "Report only. No listing or identifier changes.",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| queue_rows | {len(review_queue)} |",
        f"| apply_allowed_rows | 0 |",
        f"| stolen_isin_review_candidates_before_names_match_tightening | {STOLEN_ISIN_CANDIDATES_PRIOR_NAMES_MATCH} |",
        f"| stolen_isin_review_candidates_after_names_match_tightening | {stolen_count} |",
        "",
        "Sorted by `name_mismatch_confidence` descending (highest-confidence name-family disagreements first).",
        "",
    ]
    review_prefix.with_suffix(".md").write_text("\n".join(review_md), encoding="utf-8")
    flat_summary = {key: value for key, value in summary.items() if not isinstance(value, dict)}
    report = write_report(out_dir / "openfigi_isin_mic_remap", "OpenFIGI ISIN/MIC remap audit", flat_summary, rows, FIELDS,
                          [{"url": URL, "sha256": source_sha, "last_modified": ""}],
                          ["Probe order is the SHA-256 of the exact request. --limit bounds new requests; zero fetches the complete eligible universe.",
                           "Only valid ISIN plus mapped MIC is sent. Unlisted equities are excluded. Cached jobs can be resumed.",
                           "FIGI prefixes do not establish a level. Composite and share-class IDs are classified separately from venue FIGIs.",
                           "Unknown local levels remain unresolved. Name-family differences are review candidates, never proven stolen ISINs.",
                           "Unprobed listings are explicit and excluded from accuracy or API-failure claims. OpenFIGI descriptions are not adopted."],
                          inputs=(listings_csv, identifiers_csv, mapping_csv))
    full_payload = json.loads((out_dir / "openfigi_isin_mic_remap.json").read_text())
    full_payload["summary"] = summary
    write_json(out_dir / "openfigi_isin_mic_remap.json", full_payload)
    _append_report_sections(out_dir / "openfigi_isin_mic_remap.md", figi_levels=figi_levels, by_exchange=by_exchange,
                            by_mic=by_mic, cache_manifest=manifest)
    report["summary"] = summary
    report["direct_identifier_apply_allowed_rows"] = 0
    if any(sha256(path) != digest for path, digest in before.items()):
        raise RuntimeError("OpenFIGI audit mutated canonical input")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listings-csv", type=Path, default=ROOT / "data/listings.csv")
    parser.add_argument("--identifiers-csv", type=Path, default=ROOT / "data/identifiers_extended.csv")
    parser.add_argument("--mapping-csv", type=Path, default=ROOT / "data/masterfiles/venue_mic_mapping.csv")
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "data/openfigi_verification/isin_mic_remap_cache")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data/reports")
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--probe-json", type=Path, help="Replay the committed probe without network access")
    parser.add_argument("--write-full-probe", action="store_true",
                        help="Write the full request/response probe JSON (large; not for commit)")
    parser.add_argument("--cache-tarball-sha256", default="", help="SHA-256 of the extracted isin_mic_remap cache tarball")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be nonnegative")
    print(json.dumps(run(args.listings_csv, args.identifiers_csv, args.mapping_csv, args.cache_dir,
                         args.out_dir, fetch=args.fetch, limit=args.limit, probe_json=args.probe_json,
                         write_full_probe=args.write_full_probe,
                         cache_tarball_sha256=args.cache_tarball_sha256)["summary"], indent=2))


if __name__ == "__main__":
    main()
