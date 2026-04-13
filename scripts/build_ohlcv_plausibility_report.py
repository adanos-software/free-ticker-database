from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import LISTINGS_CSV

REPORTS_DIR = ROOT / "data" / "reports"
ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "ohlcv_plausibility.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "ohlcv_plausibility.json"
DEFAULT_MD_OUT = REPORTS_DIR / "ohlcv_plausibility.md"

ISSUE_PENALTIES = {
    "medium": 35,
    "low": 15,
    "source_gap": 10,
}

US_YAHOO_EXCHANGES = {"BATS", "NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "OTC"}
YAHOO_SUFFIX_BY_EXCHANGE: dict[str, str] = {
    "AMS": ".AS",
    "ASX": ".AX",
    "B3": ".SA",
    "BME": ".MC",
    "BMV": ".MX",
    "BRU": ".BR",
    "CPH": ".CO",
    "HEL": ".HE",
    "HKEX": ".HK",
    "IDX": ".JK",
    "KOSDAQ": ".KQ",
    "KRX": ".KS",
    "LIS": ".LS",
    "LSE": ".L",
    "MIL": ".MI",
    "OSL": ".OL",
    "PAR": ".PA",
    "SET": ".BK",
    "SIX": ".SW",
    "SSE": ".SS",
    "STO": ".ST",
    "SZSE": ".SZ",
    "TASE": ".TA",
    "TPEX": ".TWO",
    "TSE": ".T",
    "TSX": ".TO",
    "TSXV": ".V",
    "TWSE": ".TW",
    "WSE": ".WA",
    "XETRA": ".DE",
}

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "entry_quality_status",
    "ohlcv_source",
    "ohlcv_symbol",
    "plausibility_status",
    "plausibility_score",
    "bar_count",
    "first_bar_date",
    "last_bar_date",
    "max_price_jump",
    "zero_volume_streak",
    "stagnant_close_streak",
    "invalid_bar_count",
    "issue_count",
    "issue_types",
    "issues",
    "recommended_action",
]


@dataclass(frozen=True)
class OhlcvBar:
    timestamp: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None


@dataclass(frozen=True)
class OhlcvIssue:
    issue_type: str
    severity: str
    field: str
    value: str
    message: str

    @property
    def penalty(self) -> int:
        return ISSUE_PENALTIES[self.severity]


@dataclass(frozen=True)
class OhlcvPlausibilityRow:
    listing_key: str
    ticker: str
    exchange: str
    asset_type: str
    name: str
    isin: str
    entry_quality_status: str
    ohlcv_source: str
    ohlcv_symbol: str
    plausibility_status: str
    plausibility_score: int
    bar_count: int
    first_bar_date: str
    last_bar_date: str
    max_price_jump: float
    zero_volume_streak: int
    stagnant_close_streak: int
    invalid_bar_count: int
    issues: list[OhlcvIssue]
    recommended_action: str


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def add_issue(
    issues: list[OhlcvIssue],
    issue_type: str,
    severity: str,
    field: str,
    value: str,
    message: str,
) -> None:
    issues.append(
        OhlcvIssue(
            issue_type=issue_type,
            severity=severity,
            field=field,
            value=value,
            message=message,
        )
    )


def safe_float(value: Any) -> float | None:
    if value in {"", None}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def yahoo_symbol_candidates(ticker: str, exchange: str) -> list[str]:
    yahoo_ticker = ticker.replace(".", "-")
    if exchange in US_YAHOO_EXCHANGES:
        return [yahoo_ticker]
    suffix = YAHOO_SUFFIX_BY_EXCHANGE.get(exchange)
    if not suffix:
        return []
    return [f"{yahoo_ticker}{suffix}"]


def build_entry_quality_lookup(rows: Iterable[dict[str, str]]) -> dict[str, str]:
    return {row["listing_key"]: row.get("quality_status", "") for row in rows if row.get("listing_key")}


def normalize_ohlcv_header(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def load_local_ohlcv(path: Path) -> list[OhlcvBar]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    bars: list[OhlcvBar] = []
    for row in rows:
        normalized = {normalize_ohlcv_header(key): value for key, value in row.items()}
        timestamp = (
            normalized.get("timestamp")
            or normalized.get("date")
            or normalized.get("datetime")
            or normalized.get("time")
            or ""
        )
        bars.append(
            OhlcvBar(
                timestamp=timestamp,
                open=safe_float(normalized.get("open")),
                high=safe_float(normalized.get("high")),
                low=safe_float(normalized.get("low")),
                close=safe_float(normalized.get("close")),
                volume=safe_float(normalized.get("volume") or normalized.get("vol")),
            )
        )
    return bars


def local_ohlcv_candidates(row: dict[str, str], ohlcv_dir: Path | None) -> list[Path]:
    if ohlcv_dir is None:
        return []
    listing_key = row.get("listing_key", "")
    exchange = row.get("exchange", "")
    ticker = row.get("ticker", "")
    safe_listing_key = listing_key.replace("::", "__").replace("/", "_")
    candidates = [
        ohlcv_dir / f"{safe_listing_key}.csv",
        ohlcv_dir / exchange / f"{ticker}.csv",
    ]
    candidates.extend(ohlcv_dir / f"{symbol}.csv" for symbol in yahoo_symbol_candidates(ticker, exchange))
    return candidates


def load_first_local_ohlcv(row: dict[str, str], ohlcv_dir: Path | None) -> tuple[str, list[OhlcvBar]]:
    for path in local_ohlcv_candidates(row, ohlcv_dir):
        if path.exists():
            return display_path(path), load_local_ohlcv(path)
    return "", []


def fetch_yahoo_ohlcv(symbol: str, *, chart_range: str, interval: str, timeout_seconds: float) -> list[OhlcvBar]:
    import requests

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol, safe='')}"
    response = requests.get(
        url,
        params={"range": chart_range, "interval": interval, "includePrePost": "false"},
        headers={"User-Agent": "free-ticker-database/ohlcv-plausibility"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        return []

    timestamps = result.get("timestamp") or []
    quote_payload = ((result.get("indicators") or {}).get("quote") or [{}])[0]

    def quote_value(field: str, index: int) -> Any:
        values = quote_payload.get(field) or []
        return values[index] if index < len(values) else None

    bars: list[OhlcvBar] = []
    for index, timestamp in enumerate(timestamps):
        bars.append(
            OhlcvBar(
                timestamp=datetime.fromtimestamp(timestamp, UTC).date().isoformat(),
                open=safe_float(quote_value("open", index)),
                high=safe_float(quote_value("high", index)),
                low=safe_float(quote_value("low", index)),
                close=safe_float(quote_value("close", index)),
                volume=safe_float(quote_value("volume", index)),
            )
        )
    return bars


def longest_true_streak(values: Iterable[bool]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def price_is_invalid(bar: OhlcvBar) -> bool:
    prices = [bar.open, bar.high, bar.low, bar.close]
    if any(price is None or price <= 0 for price in prices):
        return True
    assert bar.open is not None
    assert bar.high is not None
    assert bar.low is not None
    assert bar.close is not None
    return bar.high < bar.low or bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close)


def max_open_gap(bars: list[OhlcvBar]) -> float:
    gaps: list[float] = []
    previous_close: float | None = None
    for bar in bars:
        if bar.open and previous_close and previous_close > 0:
            gaps.append(abs(bar.open / previous_close - 1))
        if bar.close and bar.close > 0:
            previous_close = bar.close
    return max(gaps, default=0.0)


def stagnant_close_streak(bars: list[OhlcvBar]) -> int:
    streak_values: list[bool] = []
    previous_close: float | None = None
    for bar in bars:
        is_stagnant = previous_close is not None and bar.close == previous_close
        streak_values.append(is_stagnant)
        if bar.close is not None:
            previous_close = bar.close
    return longest_true_streak(streak_values)


def last_bar_age_days(last_bar_date: str, now: datetime) -> int | None:
    if not last_bar_date:
        return None
    try:
        parsed = datetime.fromisoformat(last_bar_date.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{last_bar_date}T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (now - parsed).days


def status_for_issues(issues: list[OhlcvIssue], *, checked: bool) -> str:
    if not checked:
        return "not_checked"
    severities = {issue.severity for issue in issues}
    if severities & {"medium"}:
        return "warn"
    if "low" in severities:
        return "notice"
    if "source_gap" in severities:
        return "source_gap"
    return "pass"


def score_for_issues(issues: list[OhlcvIssue], *, checked: bool) -> int:
    if not checked:
        return 0
    return max(0, 100 - min(100, sum(issue.penalty for issue in issues)))


def action_for_status(status: str) -> str:
    return {
        "not_checked": "provide_local_ohlcv_or_run_fetch_yahoo_sample",
        "pass": "none",
        "notice": "review_if_prioritizing_market_data_quality",
        "source_gap": "find_market_data_source_or_confirm_stale_listing",
        "warn": "review_ohlcv_anomaly_against_listing_status_and_corporate_actions",
    }[status]


def assess_bars(
    row: dict[str, str],
    *,
    entry_quality_status: str,
    ohlcv_source: str,
    ohlcv_symbol: str,
    bars: list[OhlcvBar],
    checked: bool,
    min_bars: int,
    stale_days: int,
    price_jump_threshold: float,
    zero_volume_streak_threshold: int,
    stagnant_close_streak_threshold: int,
    now: datetime | None = None,
) -> OhlcvPlausibilityRow:
    now = now or datetime.now(UTC)
    issues: list[OhlcvIssue] = []
    sorted_bars = sorted(bars, key=lambda bar: bar.timestamp)
    bar_count = len(sorted_bars)
    first_bar = sorted_bars[0].timestamp if sorted_bars else ""
    last_bar = sorted_bars[-1].timestamp if sorted_bars else ""
    invalid_bar_count = sum(1 for bar in sorted_bars if price_is_invalid(bar))
    jump = max_open_gap(sorted_bars)
    zero_streak = longest_true_streak((bar.volume == 0 for bar in sorted_bars))
    stagnant_streak = stagnant_close_streak(sorted_bars)

    if not checked:
        add_issue(
            issues,
            "no_ohlcv_sample",
            "source_gap",
            "ohlcv_source",
            "",
            "No local OHLCV sample was provided and network fetching was not requested.",
        )
    elif not sorted_bars:
        add_issue(
            issues,
            "no_ohlcv_bars",
            "source_gap",
            "ohlcv_symbol",
            ohlcv_symbol,
            "OHLCV source returned no bars for the listing.",
        )
    else:
        if bar_count < min_bars:
            add_issue(
                issues,
                "short_history",
                "low",
                "bar_count",
                str(bar_count),
                f"OHLCV sample has fewer than {min_bars} bars.",
            )
        age_days = last_bar_age_days(last_bar, now)
        if age_days is not None and age_days > stale_days:
            add_issue(
                issues,
                "stale_last_bar",
                "medium",
                "last_bar_date",
                last_bar,
                f"Last OHLCV bar is {age_days} days old; threshold is {stale_days} days.",
            )
        if invalid_bar_count:
            add_issue(
                issues,
                "invalid_ohlcv_bar",
                "medium",
                "ohlcv",
                str(invalid_bar_count),
                "One or more bars have missing, non-positive, or internally inconsistent OHLC prices.",
            )
        if jump > price_jump_threshold:
            add_issue(
                issues,
                "large_price_jump",
                "medium",
                "open",
                f"{jump:.6f}",
                "Open-to-previous-close jump exceeds the configured threshold; verify split/corporate-action handling.",
            )
        if zero_streak >= zero_volume_streak_threshold:
            add_issue(
                issues,
                "long_zero_volume_streak",
                "low",
                "volume",
                str(zero_streak),
                "OHLCV sample has a long zero-volume streak, which can indicate illiquid or stale market data.",
            )
        if stagnant_streak >= stagnant_close_streak_threshold:
            add_issue(
                issues,
                "long_stagnant_close_streak",
                "low",
                "close",
                str(stagnant_streak),
                "OHLCV sample has a long unchanged-close streak, which can indicate stale market data.",
            )

    status = status_for_issues(issues, checked=checked)
    return OhlcvPlausibilityRow(
        listing_key=row.get("listing_key", ""),
        ticker=row.get("ticker", ""),
        exchange=row.get("exchange", ""),
        asset_type=row.get("asset_type", ""),
        name=row.get("name", ""),
        isin=row.get("isin", ""),
        entry_quality_status=entry_quality_status,
        ohlcv_source=ohlcv_source,
        ohlcv_symbol=ohlcv_symbol,
        plausibility_status=status,
        plausibility_score=score_for_issues(issues, checked=checked),
        bar_count=bar_count,
        first_bar_date=first_bar,
        last_bar_date=last_bar,
        max_price_jump=round(jump, 6),
        zero_volume_streak=zero_streak,
        stagnant_close_streak=stagnant_streak,
        invalid_bar_count=invalid_bar_count,
        issues=sorted(issues, key=lambda issue: (-issue.penalty, issue.issue_type, issue.field)),
        recommended_action=action_for_status(status),
    )


def filter_listing_rows(
    rows: list[dict[str, str]],
    *,
    exchanges: set[str],
    asset_types: set[str],
    focus_statuses: set[str],
    entry_quality_lookup: dict[str, str],
    max_rows: int,
) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for row in rows:
        if exchanges and row.get("exchange") not in exchanges:
            continue
        if asset_types and row.get("asset_type") not in asset_types:
            continue
        entry_quality_status = entry_quality_lookup.get(row.get("listing_key", ""), "")
        if focus_statuses and entry_quality_status not in focus_statuses:
            continue
        filtered.append(row)
        if max_rows and len(filtered) >= max_rows:
            break
    return filtered


def iter_assessed_rows(
    rows: list[dict[str, str]],
    *,
    entry_quality_lookup: dict[str, str],
    ohlcv_dir: Path | None,
    fetch_yahoo: bool,
    include_not_checked: bool,
    max_fetch: int,
    delay_seconds: float,
    timeout_seconds: float,
    chart_range: str,
    interval: str,
    min_bars: int,
    stale_days: int,
    price_jump_threshold: float,
    zero_volume_streak_threshold: int,
    stagnant_close_streak_threshold: int,
) -> Iterable[OhlcvPlausibilityRow]:
    fetched = 0
    for row in rows:
        entry_quality_status = entry_quality_lookup.get(row.get("listing_key", ""), "")
        local_source, local_bars = load_first_local_ohlcv(row, ohlcv_dir)
        if local_bars:
            yield assess_bars(
                row,
                entry_quality_status=entry_quality_status,
                ohlcv_source=local_source,
                ohlcv_symbol="",
                bars=local_bars,
                checked=True,
                min_bars=min_bars,
                stale_days=stale_days,
                price_jump_threshold=price_jump_threshold,
                zero_volume_streak_threshold=zero_volume_streak_threshold,
                stagnant_close_streak_threshold=stagnant_close_streak_threshold,
            )
            continue

        symbols = yahoo_symbol_candidates(row.get("ticker", ""), row.get("exchange", ""))
        if fetch_yahoo and fetched < max_fetch and symbols:
            fetched += 1
            symbol = symbols[0]
            try:
                bars = fetch_yahoo_ohlcv(
                    symbol,
                    chart_range=chart_range,
                    interval=interval,
                    timeout_seconds=timeout_seconds,
                )
                yield assess_bars(
                    row,
                    entry_quality_status=entry_quality_status,
                    ohlcv_source="yahoo_chart",
                    ohlcv_symbol=symbol,
                    bars=bars,
                    checked=True,
                    min_bars=min_bars,
                    stale_days=stale_days,
                    price_jump_threshold=price_jump_threshold,
                    zero_volume_streak_threshold=zero_volume_streak_threshold,
                    stagnant_close_streak_threshold=stagnant_close_streak_threshold,
                )
            except Exception as exc:  # noqa: BLE001
                error_row = assess_bars(
                    row,
                    entry_quality_status=entry_quality_status,
                    ohlcv_source="yahoo_chart",
                    ohlcv_symbol=symbol,
                    bars=[],
                    checked=True,
                    min_bars=min_bars,
                    stale_days=stale_days,
                    price_jump_threshold=price_jump_threshold,
                    zero_volume_streak_threshold=zero_volume_streak_threshold,
                    stagnant_close_streak_threshold=stagnant_close_streak_threshold,
                )
                issues = sorted(
                    [
                        *error_row.issues,
                        OhlcvIssue(
                            issue_type="ohlcv_fetch_error",
                            severity="source_gap",
                            field="ohlcv_symbol",
                            value=symbol,
                            message=str(exc),
                        ),
                    ],
                    key=lambda issue: (-issue.penalty, issue.issue_type, issue.field),
                )
                status = status_for_issues(issues, checked=True)
                yield replace(
                    error_row,
                    issues=issues,
                    plausibility_status=status,
                    plausibility_score=score_for_issues(issues, checked=True),
                    recommended_action=action_for_status(status),
                )
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            continue

        if include_not_checked:
            yield assess_bars(
                row,
                entry_quality_status=entry_quality_status,
                ohlcv_source="",
                ohlcv_symbol="|".join(symbols),
                bars=[],
                checked=False,
                min_bars=min_bars,
                stale_days=stale_days,
                price_jump_threshold=price_jump_threshold,
                zero_volume_streak_threshold=zero_volume_streak_threshold,
                stagnant_close_streak_threshold=stagnant_close_streak_threshold,
            )


def assess_rows(
    rows: list[dict[str, str]],
    *,
    entry_quality_lookup: dict[str, str],
    ohlcv_dir: Path | None,
    fetch_yahoo: bool,
    include_not_checked: bool,
    max_fetch: int,
    delay_seconds: float,
    timeout_seconds: float,
    chart_range: str,
    interval: str,
    min_bars: int,
    stale_days: int,
    price_jump_threshold: float,
    zero_volume_streak_threshold: int,
    stagnant_close_streak_threshold: int,
) -> list[OhlcvPlausibilityRow]:
    return list(
        iter_assessed_rows(
            rows,
            entry_quality_lookup=entry_quality_lookup,
            ohlcv_dir=ohlcv_dir,
            fetch_yahoo=fetch_yahoo,
            include_not_checked=include_not_checked,
            max_fetch=max_fetch,
            delay_seconds=delay_seconds,
            timeout_seconds=timeout_seconds,
            chart_range=chart_range,
            interval=interval,
            min_bars=min_bars,
            stale_days=stale_days,
            price_jump_threshold=price_jump_threshold,
            zero_volume_streak_threshold=zero_volume_streak_threshold,
            stagnant_close_streak_threshold=stagnant_close_streak_threshold,
        )
    )


def summarize(
    rows: list[OhlcvPlausibilityRow],
    generated_at: str,
    csv_out: Path,
    parameters: dict[str, Any],
    selected_rows: int,
) -> dict[str, Any]:
    status_counts = Counter(row.plausibility_status for row in rows)
    issue_counts = Counter(issue.issue_type for row in rows for issue in row.issues)
    exchange_status_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        exchange_status_counts[row.exchange][row.plausibility_status] += 1

    flagged = [row for row in rows if row.plausibility_status in {"warn", "source_gap", "notice"}]
    return {
        "_meta": {
            "generated_at": generated_at,
            "rows": len(rows),
            "selected_rows": selected_rows,
            "skipped_not_checked_rows": max(0, selected_rows - len(rows)),
            "csv_out": display_path(csv_out),
            "source_files": {
                "listings_csv": str(LISTINGS_CSV.relative_to(ROOT)),
                "entry_quality_csv": str(ENTRY_QUALITY_CSV.relative_to(ROOT)),
            },
            "method": "Kronos-inspired OHLCV plausibility heuristics: missing bars, stale bars, invalid OHLC, open/previous-close jumps, zero-volume streaks, and stagnant-close streaks.",
            "parameters": parameters,
        },
        "summary": {
            "status_counts": dict(sorted(status_counts.items())),
            "issue_counts": dict(issue_counts.most_common()),
            "top_flagged_exchanges": [
                {"exchange": exchange, **dict(counts)}
                for exchange, counts in sorted(
                    exchange_status_counts.items(),
                    key=lambda item: -(item[1]["warn"] + item[1]["source_gap"] + item[1]["notice"] + item[1]["not_checked"]),
                )[:20]
            ],
        },
        "flagged_items": [
            {
                "listing_key": row.listing_key,
                "ticker": row.ticker,
                "exchange": row.exchange,
                "asset_type": row.asset_type,
                "plausibility_status": row.plausibility_status,
                "plausibility_score": row.plausibility_score,
                "ohlcv_source": row.ohlcv_source,
                "ohlcv_symbol": row.ohlcv_symbol,
                "recommended_action": row.recommended_action,
                "issues": [asdict(issue) for issue in row.issues],
            }
            for row in flagged[:1000]
        ],
    }


def row_to_csv_record(row: OhlcvPlausibilityRow) -> dict[str, Any]:
    return {
        "listing_key": row.listing_key,
        "ticker": row.ticker,
        "exchange": row.exchange,
        "asset_type": row.asset_type,
        "name": row.name,
        "isin": row.isin,
        "entry_quality_status": row.entry_quality_status,
        "ohlcv_source": row.ohlcv_source,
        "ohlcv_symbol": row.ohlcv_symbol,
        "plausibility_status": row.plausibility_status,
        "plausibility_score": row.plausibility_score,
        "bar_count": row.bar_count,
        "first_bar_date": row.first_bar_date,
        "last_bar_date": row.last_bar_date,
        "max_price_jump": row.max_price_jump,
        "zero_volume_streak": row.zero_volume_streak,
        "stagnant_close_streak": row.stagnant_close_streak,
        "invalid_bar_count": row.invalid_bar_count,
        "issue_count": len(row.issues),
        "issue_types": "|".join(issue.issue_type for issue in row.issues),
        "issues": json.dumps([asdict(issue) for issue in row.issues], ensure_ascii=False, separators=(",", ":")),
        "recommended_action": row.recommended_action,
    }


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def issue_from_csv_payload(payload: dict[str, Any]) -> OhlcvIssue:
    severity = str(payload.get("severity") or "source_gap")
    if severity not in ISSUE_PENALTIES:
        severity = "source_gap"
    return OhlcvIssue(
        issue_type=str(payload.get("issue_type") or ""),
        severity=severity,
        field=str(payload.get("field") or ""),
        value=str(payload.get("value") or ""),
        message=str(payload.get("message") or ""),
    )


def row_from_csv_record(record: dict[str, str]) -> OhlcvPlausibilityRow:
    try:
        issue_payloads = json.loads(record.get("issues") or "[]")
    except json.JSONDecodeError:
        issue_payloads = []
    issues = [
        issue_from_csv_payload(issue_payload)
        for issue_payload in issue_payloads
        if isinstance(issue_payload, dict)
    ]
    return OhlcvPlausibilityRow(
        listing_key=record.get("listing_key", ""),
        ticker=record.get("ticker", ""),
        exchange=record.get("exchange", ""),
        asset_type=record.get("asset_type", ""),
        name=record.get("name", ""),
        isin=record.get("isin", ""),
        entry_quality_status=record.get("entry_quality_status", ""),
        ohlcv_source=record.get("ohlcv_source", ""),
        ohlcv_symbol=record.get("ohlcv_symbol", ""),
        plausibility_status=record.get("plausibility_status", ""),
        plausibility_score=safe_int(record.get("plausibility_score")),
        bar_count=safe_int(record.get("bar_count")),
        first_bar_date=record.get("first_bar_date", ""),
        last_bar_date=record.get("last_bar_date", ""),
        max_price_jump=safe_float(record.get("max_price_jump")) or 0.0,
        zero_volume_streak=safe_int(record.get("zero_volume_streak")),
        stagnant_close_streak=safe_int(record.get("stagnant_close_streak")),
        invalid_bar_count=safe_int(record.get("invalid_bar_count")),
        issues=issues,
        recommended_action=record.get("recommended_action", ""),
    )


def read_report_rows(path: Path) -> list[OhlcvPlausibilityRow]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [row_from_csv_record(record) for record in csv.DictReader(handle) if record.get("listing_key")]


def read_completed_listing_keys(path: Path) -> set[str]:
    return {row.listing_key for row in read_report_rows(path) if row.listing_key}


def write_csv(path: Path, rows: list[OhlcvPlausibilityRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row_to_csv_record(row))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    lines = [
        "# OHLCV Plausibility Report",
        "",
        f"Generated at: `{payload['_meta']['generated_at']}`",
        "",
        "This report uses Kronos-inspired deterministic OHLCV hygiene checks. It does not fill ISINs, sectors, or ETF categories.",
        "",
        "## Run Scope",
        "",
        "| Metric | Rows |",
        "|---|---:|",
        f"| Selected listing rows | {payload['_meta'].get('selected_rows', 0):,} |",
        f"| Checked rows written | {payload['_meta'].get('rows', 0):,} |",
        f"| Unchecked rows skipped | {payload['_meta'].get('skipped_not_checked_rows', 0):,} |",
        "",
        "## Status Counts",
        "",
        "| Status | Rows |",
        "|---|---:|",
    ]
    for status, count in summary["status_counts"].items():
        lines.append(f"| {status} | {count:,} |")

    lines.extend(["", "## Issue Counts", "", "| Issue | Rows |", "|---|---:|"])
    for issue, count in list(summary["issue_counts"].items())[:30]:
        lines.append(f"| {issue} | {count:,} |")

    lines.extend(["", "## Top Flagged Exchanges", "", "| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |", "|---|---:|---:|---:|---:|---:|"])
    for row in summary["top_flagged_exchanges"][:20]:
        lines.append(
            f"| {row['exchange']} | {row.get('not_checked', 0):,} | {row.get('pass', 0):,} | {row.get('notice', 0):,} | {row.get('source_gap', 0):,} | {row.get('warn', 0):,} |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.",
            "- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.",
            "- `source_gap` means a market-data lookup was attempted but no usable bars were found.",
            "- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.",
            "- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --max-fetch 250 --focus-status source_gap`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def report_parameters(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "fetch_yahoo": args.fetch_yahoo,
        "include_not_checked": args.include_not_checked,
        "max_fetch": args.max_fetch,
        "chart_range": args.chart_range,
        "interval": args.interval,
        "min_bars": args.min_bars,
        "stale_days": args.stale_days,
        "price_jump_threshold": args.price_jump_threshold,
        "zero_volume_streak_threshold": args.zero_volume_streak_threshold,
        "stagnant_close_streak_threshold": args.stagnant_close_streak_threshold,
        "filters": {
            "exchange": args.exchange,
            "asset_type": args.asset_type,
            "focus_status": args.focus_status,
            "max_rows": args.max_rows,
        },
    }


def selected_listing_rows(args: argparse.Namespace, entry_quality_lookup: dict[str, str]) -> list[dict[str, str]]:
    return filter_listing_rows(
        load_csv(args.listings_csv),
        exchanges=set(args.exchange),
        asset_types=set(args.asset_type),
        focus_statuses=set(args.focus_status),
        entry_quality_lookup=entry_quality_lookup,
        max_rows=args.max_rows,
    )


def stream_report(args: argparse.Namespace) -> tuple[list[OhlcvPlausibilityRow], dict[str, Any]]:
    entry_quality_lookup = build_entry_quality_lookup(load_csv(args.entry_quality_csv))
    rows = selected_listing_rows(args, entry_quality_lookup)
    selected_keys = {row.get("listing_key", "") for row in rows}
    existing_rows = [
        row
        for row in read_report_rows(args.csv_out)
        if args.resume and row.listing_key in selected_keys
    ]
    completed_keys = {row.listing_key for row in existing_rows}
    rows_to_process = [row for row in rows if row.get("listing_key", "") not in completed_keys]

    args.csv_out.parent.mkdir(parents=True, exist_ok=True)
    append = bool(args.resume and args.csv_out.exists() and existing_rows)
    mode = "a" if append else "w"
    processed_rows: list[OhlcvPlausibilityRow] = []
    with args.csv_out.open(mode, newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        if not append:
            writer.writeheader()
        for index, row in enumerate(
            iter_assessed_rows(
                rows_to_process,
                entry_quality_lookup=entry_quality_lookup,
                ohlcv_dir=args.ohlcv_dir,
                fetch_yahoo=args.fetch_yahoo,
                include_not_checked=args.include_not_checked,
                max_fetch=args.max_fetch,
                delay_seconds=args.delay_seconds,
                timeout_seconds=args.timeout_seconds,
                chart_range=args.chart_range,
                interval=args.interval,
                min_bars=args.min_bars,
                stale_days=args.stale_days,
                price_jump_threshold=args.price_jump_threshold,
                zero_volume_streak_threshold=args.zero_volume_streak_threshold,
                stagnant_close_streak_threshold=args.stagnant_close_streak_threshold,
            ),
            start=1,
        ):
            writer.writerow(row_to_csv_record(row))
            handle.flush()
            processed_rows.append(row)
            if args.progress_every and index % args.progress_every == 0:
                print(
                    json.dumps(
                        {
                            "processed_this_run": index,
                            "resumed_rows": len(existing_rows),
                            "selected_rows": len(rows),
                            "csv_out": display_path(args.csv_out),
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )

    report_rows = [*existing_rows, *processed_rows]
    payload = summarize(report_rows, utc_now(), args.csv_out, report_parameters(args), selected_rows=len(rows))
    payload["_meta"]["stream"] = True
    payload["_meta"]["resume"] = args.resume
    payload["_meta"]["resumed_rows"] = len(existing_rows)
    payload["_meta"]["processed_rows"] = len(processed_rows)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    return report_rows, payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Kronos-inspired OHLCV plausibility report for listing rows.")
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=ENTRY_QUALITY_CSV)
    parser.add_argument("--ohlcv-dir", type=Path, default=None, help="Optional directory with local OHLCV CSV samples.")
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--exchange", action="append", default=[], help="Limit to one exchange. Repeat for multiple.")
    parser.add_argument("--asset-type", action="append", default=[], help="Limit to one asset type. Repeat for multiple.")
    parser.add_argument("--focus-status", action="append", default=[], help="Limit by entry_quality status. Repeat for multiple.")
    parser.add_argument("--max-rows", type=int, default=0, help="Maximum listing rows to include; 0 means all.")
    parser.add_argument("--fetch-yahoo", action="store_true", help="Fetch Yahoo chart data for rows without local OHLCV samples.")
    parser.add_argument("--include-not-checked", action="store_true", help="Write rows that had no local OHLCV sample and were not fetched.")
    parser.add_argument("--max-fetch", type=int, default=250, help="Maximum Yahoo chart requests when --fetch-yahoo is set.")
    parser.add_argument("--stream", action="store_true", help="Write CSV rows as they are assessed instead of buffering the full run.")
    parser.add_argument("--resume", action="store_true", help="Resume from an existing CSV output by skipping completed listing_key rows.")
    parser.add_argument("--progress-every", type=int, default=100, help="Print one progress JSON line every N streamed rows; 0 disables progress.")
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--chart-range", default="1y")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--min-bars", type=int, default=128)
    parser.add_argument("--stale-days", type=int, default=21)
    parser.add_argument("--price-jump-threshold", type=float, default=0.30)
    parser.add_argument("--zero-volume-streak-threshold", type=int, default=5)
    parser.add_argument("--stagnant-close-streak-threshold", type=int, default=10)
    return parser.parse_args(argv)


def build_report(args: argparse.Namespace) -> tuple[list[OhlcvPlausibilityRow], dict[str, Any]]:
    entry_quality_lookup = build_entry_quality_lookup(load_csv(args.entry_quality_csv))
    rows = selected_listing_rows(args, entry_quality_lookup)
    report_rows = assess_rows(
        rows,
        entry_quality_lookup=entry_quality_lookup,
        ohlcv_dir=args.ohlcv_dir,
        fetch_yahoo=args.fetch_yahoo,
        include_not_checked=args.include_not_checked,
        max_fetch=args.max_fetch,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        chart_range=args.chart_range,
        interval=args.interval,
        min_bars=args.min_bars,
        stale_days=args.stale_days,
        price_jump_threshold=args.price_jump_threshold,
        zero_volume_streak_threshold=args.zero_volume_streak_threshold,
        stagnant_close_streak_threshold=args.stagnant_close_streak_threshold,
    )
    return report_rows, summarize(report_rows, utc_now(), args.csv_out, report_parameters(args), selected_rows=len(rows))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.stream or args.resume:
        rows, payload = stream_report(args)
    else:
        rows, payload = build_report(args)
        write_csv(args.csv_out, rows)
        write_json(args.json_out, payload)
        write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "selected_rows": payload["_meta"]["selected_rows"],
                "skipped_not_checked_rows": payload["_meta"]["skipped_not_checked_rows"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                **payload["summary"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
