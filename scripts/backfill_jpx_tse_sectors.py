from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, normalize_sector


DEFAULT_JPX_LISTED_ISSUES_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "jpx_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "tse_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "tse_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "jpx_code",
    "jpx_name",
    "jpx_market_segment",
    "jpx_33_industry_code",
    "jpx_33_industry",
    "sector_update",
    "decision",
]

JPX_33_INDUSTRY_TO_GICS: dict[str, str] = {
    "水産・農林業": "Consumer Staples",
    "鉱業": "Materials",
    "建設業": "Industrials",
    "食料品": "Consumer Staples",
    "繊維製品": "Consumer Discretionary",
    "パルプ・紙": "Materials",
    "化学": "Materials",
    "医薬品": "Health Care",
    "石油・石炭製品": "Energy",
    "ゴム製品": "Materials",
    "ガラス・土石製品": "Materials",
    "鉄鋼": "Materials",
    "非鉄金属": "Materials",
    "金属製品": "Industrials",
    "機械": "Industrials",
    "電気機器": "Information Technology",
    "輸送用機器": "Consumer Discretionary",
    "精密機器": "Health Care",
    "その他製品": "Consumer Discretionary",
    "電気・ガス業": "Utilities",
    "陸運業": "Industrials",
    "海運業": "Industrials",
    "空運業": "Industrials",
    "倉庫・運輸関連業": "Industrials",
    "情報・通信業": "Communication Services",
    "卸売業": "Industrials",
    "小売業": "Consumer Discretionary",
    "銀行業": "Financials",
    "証券、商品先物取引業": "Financials",
    "保険業": "Financials",
    "その他金融業": "Financials",
    "不動産業": "Real Estate",
    "サービス業": "Industrials",
}


@dataclass(frozen=True)
class JpxListedIssue:
    code: str
    name: str
    market_segment: str
    industry_code_33: str
    industry_33: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def download_jpx_listed_issues(url: str, *, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read()


def normalize_jpx_code(value: Any) -> str:
    code = str(value or "").strip()
    if code.endswith(".0"):
        code = code[:-2]
    return code


def parse_jpx_listed_issues(xls_bytes: bytes) -> list[JpxListedIssue]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - exercised via CLI environment
        raise SystemExit("pandas is required for this backfill.") from exc

    from io import BytesIO

    frame = pd.read_excel(BytesIO(xls_bytes)).fillna("")
    issues: list[JpxListedIssue] = []
    for record in frame.to_dict("records"):
        code = normalize_jpx_code(record.get("コード"))
        if not code:
            continue
        issues.append(
            JpxListedIssue(
                code=code,
                name=str(record.get("銘柄名") or "").strip(),
                market_segment=str(record.get("市場・商品区分") or "").strip(),
                industry_code_33=str(record.get("33業種コード") or "").strip(),
                industry_33=str(record.get("33業種区分") or "").strip(),
            )
        )
    return issues


def load_missing_tse_sector_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] == "TSE"
        and row["asset_type"] == "Stock"
        and not row.get("sector", "").strip()
    ]


def normalize_jpx_sector(industry_33: str) -> str:
    mapped = JPX_33_INDUSTRY_TO_GICS.get(industry_33, "")
    return normalize_sector(mapped, "Stock")


def evaluate_jpx_sector_row(row: dict[str, str], candidates: list[JpxListedIssue]) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "jpx_code": "",
        "jpx_name": "",
        "jpx_market_segment": "",
        "jpx_33_industry_code": "",
        "jpx_33_industry": "",
        "sector_update": "",
    }
    if not candidates:
        return {**base, "decision": "no_jpx_match"}
    if len(candidates) > 1:
        return {**base, "decision": "ambiguous_jpx_match"}

    candidate = candidates[0]
    base.update(
        {
            "jpx_code": candidate.code,
            "jpx_name": candidate.name,
            "jpx_market_segment": candidate.market_segment,
            "jpx_33_industry_code": candidate.industry_code_33,
            "jpx_33_industry": candidate.industry_33,
        }
    )
    if candidate.industry_33 in {"", "-"}:
        return {**base, "decision": "missing_jpx_industry"}
    sector_update = normalize_jpx_sector(candidate.industry_33)
    if not sector_update:
        return {**base, "decision": "unmapped_jpx_industry"}
    return {**base, "sector_update": sector_update, "decision": "accept"}


def verify_jpx_tse_sectors(
    rows: list[dict[str, str]],
    issues: list[JpxListedIssue],
) -> list[dict[str, Any]]:
    issues_by_code: dict[str, list[JpxListedIssue]] = {}
    for issue in issues:
        issues_by_code.setdefault(issue.code, []).append(issue)
    return [evaluate_jpx_sector_row(row, issues_by_code.get(row["ticker"], [])) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.74",
                "reason": f"Official JPX listed-issues file maps TSE code {result['ticker']} to JPX 33-industry '{result['jpx_33_industry']}', which was normalized to a canonical stock sector.",
            }
        )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing TSE stock sectors from the official JPX listed-issues file.")
    parser.add_argument("--url", default=DEFAULT_JPX_LISTED_ISSUES_URL)
    parser.add_argument("--xls-path", type=Path, help="Read a local JPX listed-issues XLS instead of downloading it.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=40.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    xls_bytes = args.xls_path.read_bytes() if args.xls_path else download_jpx_listed_issues(args.url, timeout_seconds=args.timeout_seconds)
    rows = load_missing_tse_sector_rows()
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results = verify_jpx_tse_sectors(rows, parse_jpx_listed_issues(xls_bytes))
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "candidates": len(rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "accepted_sector_updates": len(updates),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
