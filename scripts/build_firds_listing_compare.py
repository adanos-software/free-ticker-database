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

from scripts.lib.dataio import load_csv
from scripts.build_canonical_v4 import _load_mic_mapping
from scripts.lib.official_change_evidence import is_valid_isin
from scripts.lib.reference_audit import sha256, snapshot_metadata, write_report

TARGETS = {"LSE", "AMS", "Euronext", "XETRA", "FSX", "SIX"}
ISO_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"


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
    currencies = defaultdict(set)
    counts = Counter()
    for row in observations:
        mic, isin = row["mic"], row["isin"]
        entry = registry.get(mic, {})
        operating = entry.get("OPERATING MIC", mic)
        if mic not in expected and operating not in expected:
            category = entry.get("MARKET CATEGORY CODE", "unknown")
            counts[("OTHER", "outside_target", "all", f"outside_target_{category}_rows")] += 1
            continue
        if (row.get("terminated") and row["terminated"][:10] <= as_of) or row.get("first_traded", "")[:10] > as_of:
            counts[(mic, "date_scope", "all", "inactive_or_future_rows")] += 1
            continue
        cfi = row["cfi"]
        asset = "ETF" if cfi.startswith("CE") else "Stock" if cfi.startswith(("ES", "ED")) else ""
        if not asset or not is_valid_isin(isin):
            counts[(mic, "security_scope", "all", "excluded_cfi_or_invalid_isin_rows")] += 1
            continue
        resolution = "exact_mic" if mic in expected else "operating_vs_segment_mic"
        category = entry.get("MARKET CATEGORY CODE", "unknown")
        if category in {"SINT", "MLTF", "OTFS"}:
            resolution = "si_mtf_out_of_scope"
        official[(mic, resolution)].add((isin, asset))
        currencies[(mic, isin)].add(row["currency"])
    for (mic, resolution), pairs in official.items():
        for asset in {asset for _, asset in pairs}:
            scoped = {isin for isin, kind in pairs if kind == asset}
            local_isins = {isin for isin, kind in local.get(mic, set()) if kind == asset}
            counts[(mic, resolution, asset, "official_unique_isins")] = len(scoped)
            counts[(mic, resolution, asset, "matched_isin_mic")] = len(scoped & local_isins)
            counts[(mic, resolution, asset, "firds_only_review")] = len(scoped - local_isins)
            counts[(mic, resolution, asset, "multi_currency_unresolved")] = sum(len(currencies[(mic, isin)]) > 1 for isin in scoped & local_isins)
    for mic, pairs in local.items():
        observed = set().union(*(p for (m, resolution), p in official.items() if m == mic and resolution == "exact_mic"))
        for asset in {kind for _, kind in pairs}:
            scoped = {isin for isin, kind in pairs if kind == asset}
            found = {isin for isin, kind in observed if kind == asset}
            counts[(mic, "exact_mic", asset, "local_unique_isins")] = len(scoped)
            counts[(mic, "exact_mic", asset, "local_not_observed_review")] = len(scoped - found)
            counts[(mic, "exact_mic", asset, "local_listing_rows")] = sum(len(local_listing_keys[(mic, isin, asset)]) for isin in scoped)
            counts[(mic, "exact_mic", asset, "local_listing_rows_observed")] = sum(len(local_listing_keys[(mic, isin, asset)]) for isin in scoped & found)
    return [{"mic": mic, "resolution": resolution, "asset_type": asset, "metric": metric, "value": count}
            for (mic, resolution, asset, metric), count in sorted(counts.items())]


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
    rows = []
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
    summary = {metric: sum(row["value"] for row in rows if row["metric"] == metric and row["resolution"] == "exact_mic")
               for metric in ["official_unique_isins", "matched_isin_mic", "firds_only_review", "local_unique_isins", "local_not_observed_review", "local_listing_rows", "local_listing_rows_observed", "multi_currency_unresolved"]}
    summary["snapshot_pair_recall_pct"] = round(100 * summary["matched_isin_mic"] / summary["official_unique_isins"], 2) if summary["official_unique_isins"] else None
    summary["local_pair_observed_pct"] = round(100 * summary["matched_isin_mic"] / summary["local_unique_isins"], 2) if summary["local_unique_isins"] else None
    report = write_report(out_dir / "firds_listing_compare", "Official FIRDS listing comparison", summary, rows,
                          ["provider", "mic", "resolution", "asset_type", "metric", "value"], sources,
                          ["Metrics only. Official XML and ISIN/MIC rows are not redistributed into the MIT dataset or packages.",
                           "ES/ED common equity and depositary receipts plus CE ETFs only. Other CFI categories stay outside scope.",
                           "Operating and segment MICs remain separate. SI/MTF observations do not become missing-listing claims.",
                           "FIRDS has no reliable trading ticker. Same-listing-key ISIN mismatches cannot be scored from these files.",
                           "Local exports lack currency. Multiple FIRDS currencies remain unresolved; no currency-specific completeness claim.",
                           "Weekly full snapshots are not current-day truth. Missing pairs are review candidates, not proven false positives.",
                           "Snapshot ratios pool provider/MIC/instrument pairs; ESMA and FCA overlap. They are not global recall or identity accuracy."],
                          inputs=(listings_csv, mapping_csv, manifest_json, iso_csv))
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
