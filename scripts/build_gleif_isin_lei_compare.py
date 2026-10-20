"""Measure issuer-level LEI coverage against a pinned GLEIF relationship file."""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import load_csv, write_csv
from scripts.lib.official_change_evidence import is_valid_isin
from scripts.lib.reference_audit import sha256, snapshot_metadata, write_report

SOURCE_URL = "https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files"
QUEUE_FIELDS = ["listing_key", "isin", "local_lei", "gleif_lei", "flags", "direct_identifier_apply_allowed_rows"]


def read_relationships(path: Path, wanted: set[str]) -> tuple[dict[str, set[str]], int]:
    result = defaultdict(set)
    count = 0
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError("Expected one GLEIF relationship CSV in the ZIP")
        with archive.open(members[0]) as handle:
            reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig"))
            if not {"ISIN", "LEI"} <= set(reader.fieldnames or []):
                raise ValueError("GLEIF file lacks ISIN and LEI columns")
            for row in reader:
                count += 1
                isin, lei = row["ISIN"].strip(), row["LEI"].strip()
                if isin in wanted and lei:
                    result[isin].add(lei)
    return dict(result), count


def compare(listings: list[dict[str, str]], identifiers: list[dict[str, str]],
            relationships: dict[str, set[str]]) -> tuple[list[dict], list[dict]]:
    local = {row["listing_key"]: row.get("lei", "") for row in identifiers}
    counts = Counter()
    queue = []
    for row in listings:
        isin = row.get("isin", "")
        if not is_valid_isin(isin):
            continue
        lei = local.get(row["listing_key"], "")
        candidates = relationships.get(isin, set())
        flags = []
        if not candidates:
            flags.append("lei_missing_in_gleif")
        elif not lei:
            flags.append("lei_missing_in_adanos")
        elif lei not in candidates:
            flags.append("lei_mismatch")
        else:
            flags.append("lei_match")
        if len(candidates) > 1:
            flags.append("same_isin_multiple_lei")
        for flag in flags:
            counts[(row["exchange"], flag)] += 1
        if "lei_mismatch" in flags or "same_isin_multiple_lei" in flags:
            queue.append({"listing_key": row["listing_key"], "isin": isin, "local_lei": lei,
                          "gleif_lei": "|".join(sorted(candidates)), "flags": "|".join(flags),
                          "direct_identifier_apply_allowed_rows": 0})
    metrics = [{"exchange": exchange, "flag": flag, "listing_rows": count}
               for (exchange, flag), count in sorted(counts.items())]
    return metrics, queue


def run(listings_csv: Path, identifiers_csv: Path, source_zip: Path, out_dir: Path) -> dict:
    before = {path: sha256(path) for path in [listings_csv, identifiers_csv]}
    listings = load_csv(listings_csv)
    wanted = {row["isin"] for row in listings if is_valid_isin(row.get("isin", ""))}
    relationships, source_rows = read_relationships(source_zip, wanted)
    metrics, queue = compare(listings, load_csv(identifiers_csv), relationships)
    summary = {flag: sum(row["listing_rows"] for row in metrics if row["flag"] == flag)
               for flag in sorted({row["flag"] for row in metrics})}
    summary.update(source_relationship_rows=source_rows, local_unique_isins=len(wanted),
                   local_isins_in_gleif=len(relationships), conflict_review_rows=len(queue))
    report = write_report(out_dir / "gleif_isin_lei_compare", "GLEIF ISIN-to-LEI comparison",
                          summary, metrics, ["exchange", "flag", "listing_rows"],
                          [snapshot_metadata(source_zip, SOURCE_URL)],
                          ["Issuer-grain evidence only. LEI is not a listing join key.",
                           "The bulk relationship file stays in the ignored cache. Only metrics and conflict review rows are exported.",
                           "Flags can overlap. Missing coverage does not prove a wrong local identifier.",
                           "Mapping-file reuse rights are separate from LEI reference-data CC0 terms. No names or identifiers are applied."],
                          inputs=(listings_csv, identifiers_csv))
    write_csv(out_dir / "gleif_lei_conflict_review_queue.csv", QUEUE_FIELDS, queue)
    if any(sha256(path) != digest for path, digest in before.items()):
        raise RuntimeError("GLEIF audit mutated canonical input")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-zip", type=Path, required=True)
    parser.add_argument("--listings-csv", type=Path, default=ROOT / "data/listings.csv")
    parser.add_argument("--identifiers-csv", type=Path, default=ROOT / "data/identifiers_extended.csv")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data/reports")
    args = parser.parse_args()
    print(json.dumps(run(args.listings_csv, args.identifiers_csv, args.source_zip, args.out_dir)["summary"], indent=2))


if __name__ == "__main__":
    main()
