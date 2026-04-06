from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_FINDINGS_CSV = DATA_DIR / "stock_verification" / "run-20260405-verify10-final" / "findings.csv"
DEFAULT_METADATA_UPDATES = DATA_DIR / "review_overrides" / "metadata_updates.csv"
DEFAULT_DROP_ENTRIES = DATA_DIR / "review_overrides" / "drop_entries.csv"

US_OFFICIAL_SOURCES = {"nasdaq_listed", "nasdaq_other_listed"}
US_VERIFICATION_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "BATS"}
STRONG_RENAME_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT"}
RENAME_INCLUDE_MARKERS = (
    "Common Stock",
    "Common Shares",
    "Class A Common Stock",
    "Ordinary Shares",
    "Class A Ordinary Shares",
    "American Depositary Shares",
)
RENAME_EXCLUDE_MARKERS = (
    "Warrant",
    "Depositary Shares",
    "American Depositary Shares",
    "Notes",
    "Unit",
    "Units",
    "Preferred",
    "Trust Certificates",
)
NAME_SUFFIX_PATTERNS = [
    re.compile(r"\s+-\s+Class A Common Stock$", re.IGNORECASE),
    re.compile(r"\s+-\s+Common Stock$", re.IGNORECASE),
    re.compile(r"\s+Common Stock$", re.IGNORECASE),
    re.compile(r"\s+-\s+Common Shares$", re.IGNORECASE),
    re.compile(r"\s+-\s+Class A Ordinary Shares$", re.IGNORECASE),
    re.compile(r"\s+-\s+Ordinary Shares$", re.IGNORECASE),
    re.compile(r"\s+-\s+ordinary shares$", re.IGNORECASE),
    re.compile(r"\s+-\s+American Depositary Shares$", re.IGNORECASE),
    re.compile(r"\s{2,}", re.IGNORECASE),
]
B3_PHANTOM_TICKER_RE = re.compile(r"TF\d{3}$")
US_STRUCTURED_LINE_TICKER_RE = re.compile(r".*-(?:U|UN|WS|W|R|RW|PR-[A-Z]|PA|PB|PC|PI)$")
US_BANKRUPTCY_TICKER_RE = re.compile(r"[A-Z]{3,5}Q$")
SPAC_COMMON_MARKERS = (
    " acquisition corp",
    " acquisition corporation",
    " acquisition holdings",
)
SPAC_SHARE_MARKERS = (
    " ordinary shares",
    " class a ordinary shares",
    " common stock",
    " class a common stock",
)
FOREIGN_US_LINE_TICKER_RE = re.compile(r"[A-Z]{4,5}[FY]$")
US_WARRANT_TICKER_RE = re.compile(r"[A-Z]{4}W$")
US_PREFERRED_TICKER_RE = re.compile(r"[A-Z]{4}P$")
US_TEMPORARY_D_TICKER_RE = re.compile(r"[A-Z]{4}D$")
US_PREFERRED_HYPHEN_TICKER_RE = re.compile(r".*-PRI$")
US_TEST_LINE_TICKER_RE = re.compile(r"NTEST-[A-Z]$")


def load_csv(path: Path | str) -> list[dict[str, str]]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in fieldnames} for row in rows])


def clean_official_name(name: str) -> str:
    cleaned = name.strip()
    for pattern in NAME_SUFFIX_PATTERNS[:-1]:
        cleaned = pattern.sub("", cleaned)
    cleaned = NAME_SUFFIX_PATTERNS[-1].sub(" ", cleaned).strip(" -")
    return cleaned


def is_strong_rename_candidate(row: dict[str, str]) -> bool:
    if row.get("status") != "name_mismatch":
        return False
    if row.get("exchange") not in STRONG_RENAME_EXCHANGES:
        return False
    if row.get("official_reference_source") not in US_OFFICIAL_SOURCES:
        return False
    reference_name = row.get("official_reference_name", "")
    if not any(marker in reference_name for marker in RENAME_INCLUDE_MARKERS):
        return False
    if any(marker in reference_name for marker in RENAME_EXCLUDE_MARKERS):
        return False
    cleaned = clean_official_name(reference_name)
    return bool(cleaned and cleaned != row.get("name", "").strip())


def is_excluded_security_reference(row: dict[str, str]) -> bool:
    if row.get("status") != "name_mismatch":
        return False
    reference_name = row.get("official_reference_name", "")
    return any(marker in reference_name for marker in RENAME_EXCLUDE_MARKERS)


def is_b3_phantom_missing(row: dict[str, str]) -> bool:
    if row.get("status") != "missing_from_official":
        return False
    if row.get("exchange") != "B3":
        return False
    ticker = row.get("ticker", "")
    return bool(B3_PHANTOM_TICKER_RE.fullmatch(ticker) or ticker in {"IFNC", "ITAG"})


def is_us_stale_missing_line(row: dict[str, str]) -> bool:
    if row.get("status") != "missing_from_official":
        return False
    if row.get("exchange") not in US_VERIFICATION_EXCHANGES:
        return False
    ticker = row.get("ticker", "")
    name = row.get("name", "").lower()
    country = row.get("country", "")

    if US_STRUCTURED_LINE_TICKER_RE.fullmatch(ticker):
        return True
    if US_BANKRUPTCY_TICKER_RE.fullmatch(ticker):
        return True
    if any(marker in name for marker in SPAC_COMMON_MARKERS):
        return True
    if any(marker in name for marker in SPAC_SHARE_MARKERS):
        return True
    if US_WARRANT_TICKER_RE.fullmatch(ticker):
        return True
    if US_PREFERRED_TICKER_RE.fullmatch(ticker):
        return True
    if US_TEMPORARY_D_TICKER_RE.fullmatch(ticker):
        return True
    if US_PREFERRED_HYPHEN_TICKER_RE.fullmatch(ticker):
        return True
    if US_TEST_LINE_TICKER_RE.fullmatch(ticker):
        return True
    if country and country != "United States" and FOREIGN_US_LINE_TICKER_RE.fullmatch(ticker):
        return True
    return False


def merge_metadata_updates(
    existing_rows: list[dict[str, str]],
    generated_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[tuple[str, str, str], dict[str, str]] = {
        (row["ticker"], row["exchange"], row["field"]): dict(row)
        for row in existing_rows
    }
    for row in generated_rows:
        merged[(row["ticker"], row["exchange"], row["field"])] = dict(row)
    return sorted(merged.values(), key=lambda row: (row["ticker"], row["exchange"], row["field"]))


def merge_drop_entries(
    existing_rows: list[dict[str, str]],
    generated_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[tuple[str, str], dict[str, str]] = {
        (row["ticker"], row["exchange"]): dict(row)
        for row in existing_rows
    }
    for row in generated_rows:
        merged[(row["ticker"], row["exchange"])] = dict(row)
    return sorted(merged.values(), key=lambda row: (row["ticker"], row["exchange"]))


def build_generated_updates(findings: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    metadata_rows: list[dict[str, str]] = []
    drop_rows: list[dict[str, str]] = []

    for row in findings:
        ticker = row["ticker"]
        exchange = row["exchange"]
        status = row["status"]
        reference_name = row.get("official_reference_name", "").strip()

        if status == "non_active_official":
            drop_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "confidence": "0.99",
                    "reason": row["reason"],
                }
            )
            continue

        if is_b3_phantom_missing(row):
            drop_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "confidence": "0.99",
                    "reason": "Official B3 equity directory does not list this phantom/index-style symbol as an active common equity.",
                }
            )
            continue

        if is_us_stale_missing_line(row):
            drop_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "confidence": "0.98",
                    "reason": "Official US exchange directory does not list this stale non-common or bankruptcy line as an active common equity.",
                }
            )
            continue

        if status == "asset_type_mismatch":
            metadata_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "field": "asset_type",
                    "decision": "update",
                    "proposed_value": "ETF",
                    "confidence": "0.99",
                    "reason": f"Official reference classifies this listing as ETF/ETP: {reference_name}",
                }
            )
            cleaned_name = clean_official_name(reference_name)
            if cleaned_name and cleaned_name != row.get("name", "").strip():
                metadata_rows.append(
                    {
                        "ticker": ticker,
                        "exchange": exchange,
                        "field": "name",
                        "decision": "update",
                        "proposed_value": cleaned_name,
                        "confidence": "0.97",
                        "reason": f"Official reference provides a more specific ETF/ETP name: {reference_name}",
                    }
                )
            continue

        if is_excluded_security_reference(row):
            drop_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "confidence": "0.98",
                    "reason": f"Official listing name indicates this symbol is an excluded security line, not common equity: {reference_name}",
                }
            )
            continue

        if is_strong_rename_candidate(row):
            metadata_rows.append(
                {
                    "ticker": ticker,
                    "exchange": exchange,
                    "field": "name",
                    "decision": "update",
                    "proposed_value": clean_official_name(reference_name),
                    "confidence": "0.95",
                    "reason": f"Official listing name indicates a newer company name: {reference_name}",
                }
            )

    return metadata_rows, drop_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Turn verified stock-masterfile findings into conservative review overrides.")
    parser.add_argument("--findings-csv", type=Path, default=DEFAULT_FINDINGS_CSV)
    parser.add_argument("--metadata-updates", type=Path, default=DEFAULT_METADATA_UPDATES)
    parser.add_argument("--drop-entries", type=Path, default=DEFAULT_DROP_ENTRIES)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    findings = load_csv(args.findings_csv)
    existing_metadata = load_csv(args.metadata_updates)
    existing_drops = load_csv(args.drop_entries)

    generated_metadata, generated_drops = build_generated_updates(findings)
    merged_metadata = merge_metadata_updates(existing_metadata, generated_metadata)
    merged_drops = merge_drop_entries(existing_drops, generated_drops)

    write_csv(
        args.metadata_updates,
        ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        merged_metadata,
    )
    write_csv(
        args.drop_entries,
        ["ticker", "exchange", "confidence", "reason"],
        merged_drops,
    )

    print(
        {
            "generated_metadata_updates": len(generated_metadata),
            "generated_drop_entries": len(generated_drops),
            "merged_metadata_updates": len(merged_metadata),
            "merged_drop_entries": len(merged_drops),
        }
    )


if __name__ == "__main__":
    main()
