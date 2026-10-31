"""Compare pinned official FIRDS ISIN/MIC observations; export metrics only."""
from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import load_csv, write_json
from scripts.build_canonical_v4 import _load_mic_mapping
from scripts.lib.official_change_evidence import is_valid_isin
from scripts.lib.reference_audit import sha256, snapshot_metadata, write_report

TARGETS = {"LSE", "AMS", "Euronext", "XETRA", "FSX", "SIX"}
ISO_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
POOL_METRICS = [
    "official_unique_isins",
    "matched_isin_mic",
    "local_unique_isins",
    "local_not_observed_review",
    "local_listing_rows",
    "local_listing_rows_observed",
    "multi_currency_unresolved",
]


def parse_xml(handle):
    count = 0
    for _, element in etree.iterparse(handle, events=("end",), tag="{*}RefData",
                                     resolve_entities=False, no_network=True):
        count += 1
        def text(path):
            return element.findtext(path, default="").strip()
        row = {"isin": text("{*}FinInstrmGnlAttrbts/{*}Id"),
               "cfi": text("{*}FinInstrmGnlAttrbts/{*}ClssfctnTp"),
               "currency": text("{*}FinInstrmGnlAttrbts/{*}NtnlCcy"),
               "mic": text("{*}TradgVnRltdAttrbts/{*}Id"),
               "first_traded": text("{*}TradgVnRltdAttrbts/{*}FrstTradDt"),
               "terminated": text("{*}TradgVnRltdAttrbts/{*}TermntnDt")}
        if not row["isin"] or not row["cfi"] or not row["mic"]:
            raise ValueError("FIRDS RefData lacks ISIN, CFI, or venue MIC")
        yield row
        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]
    if not count:
        raise ValueError("FIRDS XML contains no RefData records")


def _row_key(mic: str, resolution: str, asset: str, cfi_class: str, metric: str) -> tuple:
    return mic, resolution, asset, cfi_class, metric


def compare(listings: list[dict[str, str]], mapping: list[dict[str, str]],
            observations, mic_registry: list[dict[str, str]], as_of: str) -> list[dict]:
    venues = {row["exchange_code"]: row for row in mapping if row["exchange_code"] in TARGETS}
    registry = {row["MIC"]: row for row in mic_registry}
    local = defaultdict(set)
    local_listing_keys = defaultdict(set)
    for row in listings:
        venue = venues.get(row["exchange"])
        if venue and is_valid_isin(row.get("isin", "")):
            mic = venue.get("segment_mic") or venue["operating_mic"]
            local[mic].add((row["isin"], row["asset_type"]))
            local_listing_keys[(mic, row["isin"], row["asset_type"])].add(row["listing_key"])
    expected = {mic for venue in venues.values() for mic in [venue["operating_mic"], venue.get("segment_mic", "")] if mic}
    official = defaultdict(set)
    official_cfi: dict[tuple[str, str, str, str], str] = {}
    currencies = defaultdict(set)
    counts: Counter[tuple] = Counter()
    snapshot_mic_rows: Counter[str] = Counter()
    for row in observations:
        mic, isin = row["mic"], row["isin"]
        if mic in expected:
            snapshot_mic_rows[mic] += 1
        entry = registry.get(mic, {})
        operating = entry.get("OPERATING MIC", mic)
        if mic not in expected and operating not in expected:
            category = entry.get("MARKET CATEGORY CODE", "unknown")
            counts[_row_key("OTHER", "outside_target", "all", "all", f"outside_target_{category}_rows")] += 1
            continue
        if (row.get("terminated") and row["terminated"][:10] <= as_of) or row.get("first_traded", "")[:10] > as_of:
            counts[_row_key(mic, "date_scope", "all", "all", "inactive_or_future_rows")] += 1
            continue
        cfi = row["cfi"]
        asset = "ETF" if cfi.startswith("CE") else "Stock" if cfi.startswith(("ES", "ED")) else ""
        if not asset or not is_valid_isin(isin):
            counts[_row_key(mic, "security_scope", "all", "all", "excluded_cfi_or_invalid_isin_rows")] += 1
            continue
        resolution = "exact_mic" if mic in expected else "operating_vs_segment_mic"
        category = entry.get("MARKET CATEGORY CODE", "unknown")
        if category in {"SINT", "MLTF", "OTFS"}:
            resolution = "si_mtf_out_of_scope"
        official[(mic, resolution)].add((isin, asset))
        official_cfi[(mic, resolution, isin, asset)] = cfi[:2]
        currencies[(mic, isin)].add(row["currency"])
    for mic in sorted(expected):
        if snapshot_mic_rows[mic] == 0:
            counts[_row_key(mic, "exact_mic", "all", "all", "mic_not_in_snapshot")] = 1
    for (mic, resolution), pairs in official.items():
        for asset in {asset for _, asset in pairs}:
            scoped = {isin for isin, kind in pairs if kind == asset}
            local_isins = {isin for isin, kind in local.get(mic, set()) if kind == asset}
            counts[_row_key(mic, resolution, asset, "all", "official_unique_isins")] = len(scoped)
            counts[_row_key(mic, resolution, asset, "all", "matched_isin_mic")] = len(scoped & local_isins)
            for isin in scoped - local_isins:
                cfi_class = official_cfi.get((mic, resolution, isin, asset), "XX")
                counts[_row_key(mic, resolution, asset, cfi_class, "firds_only_review")] += 1
            counts[_row_key(mic, resolution, asset, "all", "multi_currency_unresolved")] = sum(
                len(currencies[(mic, isin)]) > 1 for isin in scoped & local_isins)
    for mic, pairs in local.items():
        observed = set().union(*(p for (m, resolution), p in official.items() if m == mic and resolution == "exact_mic"))
        for asset in {kind for _, kind in pairs}:
            scoped = {isin for isin, kind in pairs if kind == asset}
            found = {isin for isin, kind in observed if kind == asset}
            counts[_row_key(mic, "exact_mic", asset, "all", "local_unique_isins")] = len(scoped)
            counts[_row_key(mic, "exact_mic", asset, "all", "local_not_observed_review")] = len(scoped - found)
            counts[_row_key(mic, "exact_mic", asset, "all", "local_listing_rows")] = sum(
                len(local_listing_keys[(mic, isin, asset)]) for isin in scoped)
            counts[_row_key(mic, "exact_mic", asset, "all", "local_listing_rows_observed")] = sum(
                len(local_listing_keys[(mic, isin, asset)]) for isin in scoped & found)
    return [{"mic": mic, "resolution": resolution, "asset_type": asset, "cfi_class": cfi_class,
             "metric": metric, "value": count}
            for (mic, resolution, asset, cfi_class, metric), count in sorted(counts.items())]


def _unscorable_pairs(rows: list[dict], *, provider: str | None = None) -> set[tuple[str, str]]:
    pairs = {(row["provider"], row["mic"]) for row in rows
             if row.get("metric") == "mic_not_in_snapshot" and row["value"]}
    if provider is not None:
        pairs = {(prov, mic) for prov, mic in pairs if prov == provider}
    return pairs


def _unscorable_mics(rows: list[dict], *, provider: str | None = None) -> list[str]:
    return sorted({mic for _, mic in _unscorable_pairs(rows, provider=provider)})


def _pool_totals(rows: list[dict], *, provider: str | None = None, mic: str | None = None) -> dict[str, int]:
    skip = _unscorable_pairs(rows, provider=provider)
    if mic is not None and provider is not None and (provider, mic) in skip:
        return {metric: 0 for metric in POOL_METRICS} | {"firds_only_review": 0}
    subset = [row for row in rows
              if row["resolution"] == "exact_mic"
              and (provider is None or row["provider"] == provider)
              and (mic is None or row["mic"] == mic)
              and (row["provider"], row["mic"]) not in skip]
    totals = {metric: sum(row["value"] for row in subset if row["metric"] == metric and row["cfi_class"] == "all")
              for metric in POOL_METRICS}
    totals["firds_only_review"] = sum(row["value"] for row in subset if row["metric"] == "firds_only_review")
    return totals


def _pct(numerator: int, denominator: int) -> float | None:
    return round(100 * numerator / denominator, 2) if denominator else None


def _pool_summary(rows: list[dict], *, provider: str | None = None, mic: str | None = None) -> dict:
    totals = _pool_totals(rows, provider=provider, mic=mic)
    summary = dict(totals)
    summary["snapshot_pair_recall_pct"] = _pct(totals["matched_isin_mic"], totals["official_unique_isins"])
    summary["local_pair_observed_pct"] = _pct(totals["matched_isin_mic"], totals["local_unique_isins"])
    summary["mics_not_in_snapshot"] = _unscorable_mics(rows, provider=provider)
    return summary


def _append_pool_rows(rows: list[dict]) -> None:
    for provider in ["esma", "fca", None]:
        label = provider or "all"
        pooled = _pool_summary(rows, provider=provider)
        for metric, value in pooled.items():
            if metric == "mics_not_in_snapshot":
                continue
            if value is None:
                continue
            rows.append({"provider": provider or "", "mic": "", "resolution": "pooled", "asset_type": "all",
                         "cfi_class": "all", "metric": metric, "value": value if not isinstance(value, list) else len(value)})
    target_mics = sorted({row["mic"] for row in rows if row["mic"] and row["resolution"] == "exact_mic"})
    for provider in ["esma", "fca"]:
        for mic in target_mics:
            pooled = _pool_summary(rows, provider=provider, mic=mic)
            for metric in [*POOL_METRICS, "firds_only_review", "snapshot_pair_recall_pct", "local_pair_observed_pct"]:
                value = pooled.get(metric)
                if value is None:
                    continue
                rows.append({"provider": provider, "mic": mic, "resolution": "pooled", "asset_type": "all",
                             "cfi_class": "all", "metric": metric, "value": value})


def _extend_markdown(md_path: Path, summary: dict) -> None:
    lines = md_path.read_text(encoding="utf-8").splitlines()
    insert_at = next(i for i, line in enumerate(lines) if line.startswith("## Sources"))
    extra = ["", "## Pooled metrics (scorable MICs only)", "",
             "Target MICs with zero rows in a provider snapshot are marked `mic_not_in_snapshot` and excluded from pooled ratios.",
             "", "### By provider", "", "| Provider | Recall % | Local observed % | Not in snapshot |",
             "| --- | ---: | ---: | --- |"]
    for provider in ["esma", "fca"]:
        block = summary["by_provider"][provider]
        extra.append(f"| {provider.upper()} | {block.get('snapshot_pair_recall_pct')} | "
                     f"{block.get('local_pair_observed_pct')} | {', '.join(block.get('mics_not_in_snapshot') or []) or '—'} |")
    extra.extend(["", "### By MIC (exact MIC, per provider)", "",
                  "| Provider | MIC | Recall % | Local observed % | Status |",
                  "| --- | --- | ---: | ---: | --- |"])
    for key, block in sorted(summary["by_mic"].items()):
        provider, mic = key.split(":", 1)
        status = "mic_not_in_snapshot" if mic in summary["by_provider"][provider].get("mics_not_in_snapshot", []) else "scorable"
        extra.append(f"| {provider.upper()} | {mic} | {block.get('snapshot_pair_recall_pct')} | "
                     f"{block.get('local_pair_observed_pct')} | {status} |")
    md_path.write_text("\n".join(lines[:insert_at] + extra + [""] + lines[insert_at:]), encoding="utf-8")


def run(listings_csv: Path, mapping_csv: Path, manifest_json: Path, iso_csv: Path, out_dir: Path) -> dict:
    before = sha256(listings_csv)
    manifest = json.loads(manifest_json.read_text())
    if not manifest or {item["provider"] for item in manifest} != {"esma", "fca"}:
        raise ValueError("Provide pinned official ESMA and FCA full-file snapshots")
    for provider in ["esma", "fca"]:
        parts = defaultdict(set)
        totals = defaultdict(set)
        for item in manifest:
            if item["provider"] != provider:
                continue
            match = re.fullmatch(r"FULINS_([CE])_\d{8}_(\d{2})of(\d{2})\.zip", Path(item["path"]).name)
            if not match:
                raise ValueError("Expected equity and collective-investment full-file names")
            category, part, total = match.groups()
            parts[category].add(int(part))
            totals[category].add(int(total))
        if set(parts) != {"C", "E"} or any(len(totals[c]) != 1 or parts[c] != set(range(1, next(iter(totals[c])) + 1)) for c in parts):
            raise ValueError("Incomplete FIRDS full snapshot: all C and E parts are required")
    sources = [snapshot_metadata(mapping_csv, "data/masterfiles/venue_mic_mapping.csv"), snapshot_metadata(iso_csv, ISO_URL)]
    rows: list[dict] = []
    for provider in ["esma", "fca"]:
        files = [item for item in manifest if item["provider"] == provider]
        dates = {item["publication_date"][:10] for item in files}
        if len(dates) != 1:
            raise ValueError("Do not combine different FIRDS full-snapshot dates")
        def observations():
            for item in files:
                host = urlparse(item["url"]).hostname
                if host != {"esma": "firds.esma.europa.eu", "fca": "data.fca.org.uk"}[provider] or not Path(item["path"]).name.startswith("FULINS_"):
                    raise ValueError("Only official full FIRDS files are accepted")
                path = Path(item["path"])
                metadata = snapshot_metadata(path, item["url"])
                if item.get("sha256", metadata["sha256"]) != metadata["sha256"]:
                    raise ValueError("Pinned FIRDS snapshot hash mismatch")
                sources.append({**metadata, "publication_date": item["publication_date"]})
                with zipfile.ZipFile(path) as archive:
                    members = [name for name in archive.namelist() if name.lower().endswith(".xml")]
                    if not members:
                        raise ValueError("FIRDS ZIP contains no XML")
                    for name in members:
                        with archive.open(name) as handle:
                            yield from parse_xml(handle)
        for row in compare(load_csv(listings_csv), list(_load_mic_mapping(mapping_csv).values()), observations(), load_csv(iso_csv), next(iter(dates))):
            rows.append({"provider": provider, **row})
    _append_pool_rows(rows)
    pooled = _pool_summary(rows)
    by_provider = {provider: _pool_summary(rows, provider=provider) for provider in ["esma", "fca"]}
    target_mics = sorted({row["mic"] for row in rows if row["mic"] and row["resolution"] == "exact_mic"})
    by_mic = {f"{provider}:{mic}": _pool_summary(rows, provider=provider, mic=mic)
              for provider in ["esma", "fca"] for mic in target_mics}
    summary = {**pooled, "by_provider": by_provider, "by_mic": by_mic}
    limitations = [
        "Metrics only. Official XML and ISIN/MIC rows are not redistributed into the MIT dataset or packages.",
        "ES/ED common equity and depositary receipts plus CE ETFs only. Other CFI categories stay outside scope.",
        "Operating and segment MICs remain separate. SI/MTF observations do not become missing-listing claims.",
        "FIRDS has no reliable trading ticker. Same-listing-key ISIN mismatches cannot be scored from these files.",
        "Local exports lack currency. Multiple FIRDS currencies remain unresolved; no currency-specific completeness claim.",
        "Weekly full snapshots are not current-day truth. Missing pairs are review candidates, not proven false positives.",
        "Snapshot ratios pool scorable provider/MIC/instrument pairs only; ESMA and FCA overlap. They are not global recall or identity accuracy.",
        "German operating MICs XETR and XFRA and Swiss XSWX often have zero rows in ESMA FULINS; those MICs are mic_not_in_snapshot for ESMA and excluded from ESMA pooled ratios. Frankfurt segment MICs such as FRAB, STUB, HAND, and MUND may appear under SI/MTF and are never merged into XETR.",
        "UK XLON is absent from ESMA FULINS but present in FCA FIRDS; score LSE via FCA, not ESMA.",
    ]
    report = write_report(out_dir / "firds_listing_compare", "Official FIRDS listing comparison", pooled, rows,
                          ["provider", "mic", "resolution", "asset_type", "cfi_class", "metric", "value"], sources,
                          limitations, inputs=(listings_csv, mapping_csv, manifest_json, iso_csv))
    report["summary"] = summary
    write_json(out_dir / "firds_listing_compare.json", report)
    _extend_markdown(out_dir / "firds_listing_compare.md", summary)
    if sha256(listings_csv) != before:
        raise RuntimeError("FIRDS comparison mutated listings")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--iso-csv", type=Path, required=True)
    parser.add_argument("--listings-csv", type=Path, default=ROOT / "data/listings.csv")
    parser.add_argument("--mapping-csv", type=Path, default=ROOT / "data/masterfiles/venue_mic_mapping.csv")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data/reports")
    args = parser.parse_args()
    print(json.dumps(run(args.listings_csv, args.mapping_csv, args.manifest, args.iso_csv, args.out_dir)["summary"], indent=2))


if __name__ == "__main__":
    main()
