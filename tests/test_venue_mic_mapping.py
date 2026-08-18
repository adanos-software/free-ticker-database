from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIC_CSV = ROOT / "data" / "masterfiles" / "venue_mic_mapping.csv"
LISTINGS_CSV = ROOT / "data" / "listings.csv"
COVERAGE_JSON = ROOT / "data" / "reports" / "coverage_report.json"
MIC_RE = re.compile(r"^[A-Z0-9]{4}$")


def load_mapping() -> list[dict[str, str]]:
    with MIC_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_venue_mic_mapping_covers_current_listing_venues() -> None:
    rows = load_mapping()
    by_code = {row["exchange_code"]: row for row in rows}
    assert len(by_code) == len(rows)

    listing_exchanges = set()
    with LISTINGS_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            exchange = row.get("exchange", "").strip()
            if exchange:
                listing_exchanges.add(exchange)
    assert listing_exchanges <= set(by_code)

    coverage = json.loads(COVERAGE_JSON.read_text(encoding="utf-8"))
    coverage_exchanges = {row["exchange"] for row in coverage["exchange_coverage"]}
    assert coverage_exchanges <= set(by_code)


def test_venue_mic_mapping_rows_are_iso_10383_shaped() -> None:
    for row in load_mapping():
        assert MIC_RE.fullmatch(row["operating_mic"] or "") or MIC_RE.fullmatch(row["segment_mic"] or "")
        if row["operating_mic"]:
            assert MIC_RE.fullmatch(row["operating_mic"])
        if row["segment_mic"]:
            assert MIC_RE.fullmatch(row["segment_mic"])
        assert row["country_code"]
        assert row["canonical_name"]
        assert row["evidence_url"].startswith("https://")
        assert row["reviewed_at"].endswith("Z")
        assert row["reviewer"]
