from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_dataset import DEFAULT_JSON_OUT as DEFAULT_REVIEW_QUEUE_JSON
from scripts.rebuild_dataset import (
    REVIEW_REMOVE_ALIASES_CSV,
    alias_matches_company,
    is_blocked_alias,
    is_company_style_alias,
)


DEFAULT_OUTPUT_DIR = ROOT / "data" / "yahoo_verification"
DEFAULT_JSON_OUT = DEFAULT_OUTPUT_DIR / "high_risk_aliases.json"
DEFAULT_CSV_OUT = DEFAULT_OUTPUT_DIR / "high_risk_aliases.csv"
DEFAULT_FINDING_TYPES = ("cross_company_alias_collision", "low_company_name_overlap")


def load_review_items(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("items", []))


def extract_alias_cases(
    items: list[dict[str, Any]],
    *,
    finding_types: set[str],
) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        for finding in item.get("findings", []):
            if finding["finding_type"] not in finding_types or finding["field"] != "alias":
                continue
            case = {
                "ticker": item["ticker"],
                "exchange": item["exchange"],
                "name": item["name"],
                "asset_type": item["asset_type"],
                "country": item["country"],
                "country_code": item["country_code"],
                "isin": item["isin"],
                "alias": finding["value"],
                "finding_type": finding["finding_type"],
                "finding_reason": finding["reason"],
            }
            key = (case["ticker"], case["exchange"], case["alias"])
            if key in seen:
                continue
            seen.add(key)
            cases.append(case)
    return cases


def fetch_yahoo_alias_search(alias: str, *, max_results: int = 5, timeout: float = 10.0) -> list[dict[str, Any]]:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - CLI only
        raise SystemExit(
            "yfinance is required for Yahoo alias verification. Install it locally with `pip install yfinance`."
        ) from exc

    search = yf.Search(
        alias,
        max_results=max_results,
        news_count=0,
        lists_count=0,
        recommended=max_results,
        timeout=timeout,
        raise_errors=False,
    )
    results: list[dict[str, Any]] = []
    for quote in search.quotes[:max_results]:
        results.append(
            {
                "symbol": str(quote.get("symbol") or ""),
                "shortname": str(quote.get("shortname") or ""),
                "longname": str(quote.get("longname") or ""),
                "quote_type": str(quote.get("quoteType") or ""),
                "exchange": str(quote.get("exchange") or ""),
                "exchange_display": str(quote.get("exchDisp") or ""),
            }
        )
    return results


def quote_matches_row(quote: dict[str, Any], row: dict[str, Any]) -> bool:
    if quote.get("symbol", "").upper() == str(row["ticker"]).upper():
        return True
    for field in ("longname", "shortname"):
        candidate = str(quote.get(field) or "").strip()
        if candidate and alias_matches_company(candidate, str(row["name"])):
            return True
    return False


def evaluate_alias_case(case: dict[str, Any], search_results: list[dict[str, Any]]) -> dict[str, Any]:
    alias = str(case["alias"])
    if alias_matches_company(alias, str(case["name"])):
        status = "supported_local"
    elif any(quote_matches_row(result, case) for result in search_results):
        status = "supported_yahoo"
    elif is_blocked_alias(alias):
        status = "remove_blocked"
    elif search_results and case["finding_type"] == "cross_company_alias_collision":
        status = "likely_other_entity"
    elif search_results and not is_company_style_alias(alias):
        status = "likely_other_entity"
    elif search_results:
        status = "needs_human"
    else:
        status = "unknown"

    return {
        **case,
        "status": status,
        "search_results": search_results,
    }


def verify_alias_cases(
    cases: list[dict[str, Any]],
    *,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        try:
            search_results = fetch_yahoo_alias_search(case["alias"], timeout=timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    **case,
                    "status": "error",
                    "error": str(exc),
                    "search_results": [],
                }
            )
        else:
            results.append(evaluate_alias_case(case, search_results))

        if delay_seconds > 0 and index < len(cases) - 1:
            time.sleep(delay_seconds)
    return results


def build_remove_overrides(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    overrides: list[dict[str, str]] = []
    for result in results:
        if result["status"] not in {"remove_blocked", "likely_other_entity"}:
            continue
        overrides.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "alias": result["alias"],
                "reason": f"Yahoo alias verification marked `{result['alias']}` as {result['status']}.",
            }
        )
    return overrides


def merge_remove_overrides(path: Path, overrides: list[dict[str, str]]) -> None:
    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    else:
        rows = []

    by_key = {(row["ticker"], row["exchange"], row["alias"]): row for row in rows}
    for override in overrides:
        by_key[(override["ticker"], override["exchange"], override["alias"])] = override

    merged_rows = sorted(by_key.values(), key=lambda row: (row["ticker"], row["exchange"], row["alias"]))
    preferred_field_order = ["ticker", "exchange", "alias", "reason"]
    fieldnames = preferred_field_order + sorted(
        {
            key
            for row in merged_rows
            for key in row.keys()
            if key not in preferred_field_order
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)
    temp_path.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ticker",
        "exchange",
        "name",
        "asset_type",
        "country",
        "country_code",
        "isin",
        "alias",
        "finding_type",
        "status",
        "finding_reason",
        "search_results",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized = dict(row)
            serialized["search_results"] = json.dumps(row.get("search_results", []), ensure_ascii=False)
            writer.writerow(serialized)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Use Yahoo Finance search to conservatively verify high-risk name aliases."
    )
    parser.add_argument("--review-queue-json", type=Path, default=DEFAULT_REVIEW_QUEUE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--finding-type", action="append", default=list(DEFAULT_FINDING_TYPES))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--merge-remove-overrides", action="store_true")
    parser.add_argument("--remove-overrides-csv", type=Path, default=REVIEW_REMOVE_ALIASES_CSV)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cases = extract_alias_cases(
        load_review_items(args.review_queue_json),
        finding_types=set(args.finding_type),
    )
    if args.limit is not None:
        cases = cases[: args.limit]

    results = verify_alias_cases(
        cases,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(args.csv_out, results)

    overrides = build_remove_overrides(results)
    if args.merge_remove_overrides and overrides:
        merge_remove_overrides(args.remove_overrides_csv, overrides)

    status_counts: dict[str, int] = {}
    for row in results:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    print(
        json.dumps(
            {
                "review_queue": str(args.review_queue_json.relative_to(ROOT)),
                "cases": len(cases),
                "status_counts": status_counts,
                "remove_overrides": len(overrides),
                "json_out": str(args.json_out.relative_to(ROOT)),
                "csv_out": str(args.csv_out.relative_to(ROOT)),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
