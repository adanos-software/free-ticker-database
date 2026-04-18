from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.alias_policy import normalize_alias_text

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_REFERENCE_CSV = DATA_DIR / "adanos" / "ticker_reference.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "adanos_detection_simulation.json"
DEFAULT_MD_OUT = REPORTS_DIR / "adanos_detection_simulation.md"

BUILTIN_POSITIVE_PROBES = [
    {"id": "positive_msft", "text": "Microsoft reported stronger cloud revenue.", "expected_ticker": "MSFT"},
    {"id": "positive_nvda", "text": "Nvidia shares moved higher after the chip update.", "expected_ticker": "NVDA"},
    {"id": "positive_tsla", "text": "Tesla deliveries are back in focus this quarter.", "expected_ticker": "TSLA"},
    {"id": "positive_amzn", "text": "Amazon announced another investment in logistics.", "expected_ticker": "AMZN"},
    {"id": "positive_meta", "text": "Meta Platforms launched a new advertising product.", "expected_ticker": "META"},
]

BUILTIN_NEGATIVE_PROBES = [
    {"id": "negative_common_market", "text": "The market moved higher before the close."},
    {"id": "negative_common_bank", "text": "The central bank held rates steady."},
    {"id": "negative_common_gold", "text": "Gold and cash were discussed as defensive assets."},
    {"id": "negative_common_call", "text": "We have a call next week about the product roadmap."},
    {"id": "negative_common_energy", "text": "Energy costs remain high for consumers."},
]


@dataclass(frozen=True)
class AliasEntry:
    ticker: str
    alias: str
    pattern: re.Pattern[str]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parse_aliases(value: str) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [alias for alias in parsed if isinstance(alias, str) and alias.strip()]


def alias_pattern(alias: str) -> re.Pattern[str]:
    escaped = re.escape(alias)
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", re.IGNORECASE)


def build_alias_index(reference_rows: list[dict[str, str]]) -> list[AliasEntry]:
    entries: list[AliasEntry] = []
    seen: set[tuple[str, str]] = set()
    for row in reference_rows:
        ticker = row.get("ticker", "")
        for raw_alias in parse_aliases(row.get("aliases", "")):
            alias = normalize_alias_text(raw_alias).lower()
            if len(alias) < 3:
                continue
            key = (ticker, alias)
            if key in seen:
                continue
            seen.add(key)
            entries.append(AliasEntry(ticker=ticker, alias=alias, pattern=alias_pattern(alias)))
    entries.sort(key=lambda entry: (-len(entry.alias), entry.ticker, entry.alias))
    return entries


def detect_tickers(text: str, alias_index: Iterable[AliasEntry]) -> list[dict[str, str]]:
    normalized_text = normalize_alias_text(text).lower()
    matches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in alias_index:
        if entry.pattern.search(normalized_text):
            key = (entry.ticker, entry.alias)
            if key not in seen:
                seen.add(key)
                matches.append({"ticker": entry.ticker, "alias": entry.alias})
    return matches


def load_positive_probes(path: Path | None) -> list[dict[str, str]]:
    if not path:
        return BUILTIN_POSITIVE_PROBES
    rows = load_csv(path)
    return [
        {
            "id": row.get("id") or f"positive_{index}",
            "text": row["text"],
            "expected_ticker": row["expected_ticker"],
        }
        for index, row in enumerate(rows, start=1)
        if row.get("text") and row.get("expected_ticker")
    ]


def load_negative_probes(path: Path | None) -> list[dict[str, str]]:
    if not path:
        return BUILTIN_NEGATIVE_PROBES
    rows: list[dict[str, str]] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if text:
            rows.append({"id": f"negative_{index}", "text": text})
    return rows


def build_simulation_report(
    *,
    reference_rows: list[dict[str, str]],
    positive_probes: list[dict[str, str]],
    negative_probes: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict[str, object]:
    generated_at = generated_at or utc_now()
    alias_index = build_alias_index(reference_rows)
    alias_counts_by_ticker = defaultdict(int)
    for entry in alias_index:
        alias_counts_by_ticker[entry.ticker] += 1

    positive_results = []
    for probe in positive_probes:
        matches = detect_tickers(probe["text"], alias_index)
        matched_tickers = {match["ticker"] for match in matches}
        positive_results.append(
            {
                **probe,
                "matched": probe["expected_ticker"] in matched_tickers,
                "matches": matches[:25],
            }
        )

    negative_results = []
    for probe in negative_probes:
        matches = detect_tickers(probe["text"], alias_index)
        negative_results.append({**probe, "matched": bool(matches), "matches": matches[:25]})

    positive_misses = [row for row in positive_results if not row["matched"]]
    negative_hits = [row for row in negative_results if row["matched"]]
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {"adanos_reference_csv": display_path(DEFAULT_REFERENCE_CSV)},
        },
        "summary": {
            "reference_rows": len(reference_rows),
            "alias_entries": len(alias_index),
            "tickers_with_aliases": len(alias_counts_by_ticker),
            "positive_probes": len(positive_results),
            "positive_misses": len(positive_misses),
            "negative_probes": len(negative_results),
            "negative_hits": len(negative_hits),
        },
        "positive_misses": positive_misses[:100],
        "negative_hits": negative_hits[:100],
        "positive_results": positive_results,
        "negative_results": negative_results,
    }


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, object]) -> None:
    summary = payload["summary"]  # type: ignore[index]
    lines = [
        "# Adanos Detection Simulation",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",  # type: ignore[index]
        "",
        "This report smoke-tests natural-language aliases from `data/adanos/ticker_reference.csv` against positive and negative text probes.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in summary.items():  # type: ignore[union-attr]
        lines.append(f"| {key} | {value:,} |" if isinstance(value, int) else f"| {key} | {value} |")

    lines.extend(["", "## Positive Misses", "", "| Probe | Expected | Text | Matches |", "|---|---|---|---|"])
    for row in payload["positive_misses"]:  # type: ignore[index]
        lines.append(
            f"| {row['id']} | {row['expected_ticker']} | {row['text']} | {json.dumps(row['matches'], ensure_ascii=False)} |"
        )
    if not payload["positive_misses"]:  # type: ignore[index]
        lines.append("| _none_ |  |  |  |")

    lines.extend(["", "## Negative Hits", "", "| Probe | Text | Matches |", "|---|---|---|"])
    for row in payload["negative_hits"]:  # type: ignore[index]
        lines.append(f"| {row['id']} | {row['text']} | {json.dumps(row['matches'], ensure_ascii=False)} |")
    if not payload["negative_hits"]:  # type: ignore[index]
        lines.append("| _none_ |  |  |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate Adanos ticker alias detection on text probes.")
    parser.add_argument("--reference-csv", type=Path, default=DEFAULT_REFERENCE_CSV)
    parser.add_argument("--positive-csv", type=Path, help="CSV with id,text,expected_ticker columns.")
    parser.add_argument("--negative-txt", type=Path, help="Plain text file with one negative probe per line.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--fail-on-positive-miss", action="store_true")
    parser.add_argument("--fail-on-negative-hit", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_simulation_report(
        reference_rows=load_csv(args.reference_csv),
        positive_probes=load_positive_probes(args.positive_csv),
        negative_probes=load_negative_probes(args.negative_txt),
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "summary": payload["summary"],
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    summary = payload["summary"]  # type: ignore[assignment]
    if args.fail_on_positive_miss and summary["positive_misses"]:  # type: ignore[index]
        return 1
    if args.fail_on_negative_hit and summary["negative_hits"]:  # type: ignore[index]
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
