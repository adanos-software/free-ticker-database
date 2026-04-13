from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "tickers.csv"
DEFAULT_OUTPUT = ROOT / "error.txt"
DEFAULT_CHECKPOINT = ROOT / "data" / "llm_plausibility_review" / "gemma4_findings.jsonl"
DEFAULT_ACCEPTED_FINDINGS = ROOT / "data" / "review_overrides" / "llm_plausibility_accepts.csv"
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
DEFAULT_MODEL = "gemma4:26b"
DEFAULT_BATCH_SIZE = 10

REVIEW_FIELDS = [
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
]
VALID_FIELDS = {
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "sector",
    "stock_sector",
    "etf_category",
    "country",
    "country_code",
    "isin",
    "aliases",
}
VALID_SEVERITIES = {"low", "medium", "high"}


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def row_key(row: dict[str, str]) -> tuple[str, str]:
    return row["ticker"], row["exchange"]


def read_ticker_rows(path: Path, *, offset: int = 0, limit: int | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    selected = rows[offset:]
    if limit is not None:
        selected = selected[:limit]
    return selected


def trim(value: object, max_length: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


def compact_row(row: dict[str, str]) -> dict[str, str]:
    return {field: trim(row.get(field, "")) for field in REVIEW_FIELDS}


def chunk_rows(rows: list[dict[str, str]], batch_size: int) -> list[list[dict[str, str]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [rows[index : index + batch_size] for index in range(0, len(rows), batch_size)]


def build_prompt(rows: list[dict[str, str]]) -> str:
    entries = [compact_row(row) for row in rows]
    return (
        "You are a conservative plausibility reviewer for a global ticker database.\n"
        "Review ONLY the entries provided below. Do not browse, do not invent facts, and do not flag uncertainty.\n"
        "Flag only obvious internal plausibility issues, for example: country/ISIN prefix mismatch, ETF category on a stock, "
        "stock sector on an ETF, impossible country code, obvious company/name mismatch, or clearly wrong asset type.\n"
        "Do NOT flag missing ISIN, missing sector, missing stock_sector, or missing etf_category by itself.\n"
        "Do NOT flag a row merely because you do not know the ticker.\n"
        "Return JSON only, with this exact top-level shape:\n"
        '{"findings":[{"ticker":"...","exchange":"...","field":"isin","severity":"low|medium|high",'
        '"issue":"short explanation","suggestion":"short suggested check or fix","confidence":0.0}]}\n'
        "Use field only from: ticker, name, exchange, asset_type, sector, stock_sector, etf_category, country, "
        "country_code, isin, aliases.\n"
        "If there are no obvious issues, return {\"findings\":[]}.\n\n"
        f"Entries:\n{json.dumps(entries, ensure_ascii=True, indent=2)}"
    )


def parse_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        cleaned = cleaned.removeprefix("json").strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or start >= end:
            raise
        parsed = json.loads(cleaned[start : end + 1])

    if isinstance(parsed, list):
        return {"findings": parsed}
    if not isinstance(parsed, dict):
        raise ValueError("LLM response must be a JSON object or findings array.")
    return parsed


def is_review_error_finding(finding: dict[str, object]) -> bool:
    if finding.get("_review_error"):
        return True
    issue = str(finding.get("issue", ""))
    return issue.startswith("LLM response could not be parsed:")


def normalize_checkpoint_finding(finding: dict[str, object]) -> dict[str, object]:
    if is_review_error_finding(finding) and not finding.get("_review_error"):
        return {**finding, "_review_error": True}
    return finding


def load_accepted_findings(path: Path = DEFAULT_ACCEPTED_FINDINGS) -> set[tuple[str, str, str, str]]:
    if not path.exists():
        return set()

    accepted: set[tuple[str, str, str, str]] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            accepted.add(
                (
                    row.get("ticker", ""),
                    row.get("exchange", ""),
                    row.get("field", ""),
                    row.get("current_value", ""),
                )
            )
    return accepted


def current_finding_value(finding: dict[str, object], current_row: dict[str, str]) -> str:
    return str(current_row.get(str(finding.get("field", "")), ""))


def is_accepted_false_positive(
    finding: dict[str, object],
    current_rows_by_key: dict[tuple[str, str], dict[str, str]],
    accepted_findings: set[tuple[str, str, str, str]],
) -> bool:
    key = (str(finding.get("ticker", "")), str(finding.get("exchange", "")))
    current_row = current_rows_by_key.get(key)
    if current_row is None:
        return False
    accepted_key = (
        key[0],
        key[1],
        str(finding.get("field", "")),
        current_finding_value(finding, current_row),
    )
    return accepted_key in accepted_findings


def is_resolved_against_current_rows(
    finding: dict[str, object],
    current_rows_by_key: dict[tuple[str, str], dict[str, str]],
) -> bool:
    key = (str(finding.get("ticker", "")), str(finding.get("exchange", "")))
    current_row = current_rows_by_key.get(key)
    if current_row is None:
        return True

    snapshot = finding.get("row", {})
    if not isinstance(snapshot, dict):
        return False

    field = str(finding.get("field", ""))
    old_value = str(snapshot.get(field, ""))
    current_values = [str(current_row.get(field, ""))]
    if field == "sector":
        current_values.extend(
            [
                str(current_row.get("stock_sector", "")),
                str(current_row.get("etf_category", "")),
            ]
        )
    if field == "name" and "isin" in str(finding.get("issue", "")).lower():
        return any(
            str(snapshot.get(metadata_field, "")) != str(current_row.get(metadata_field, ""))
            for metadata_field in ("isin", "country", "country_code", "aliases")
        )
    return all(value != old_value for value in current_values)


def is_stale_review_error(
    finding: dict[str, object],
    current_rows_by_key: dict[tuple[str, str], dict[str, str]],
) -> bool:
    key = (str(finding.get("ticker", "")), str(finding.get("exchange", "")))
    current_row = current_rows_by_key.get(key)
    if current_row is None:
        return True

    snapshot = finding.get("row", {})
    if not isinstance(snapshot, dict):
        return False

    return any(str(snapshot.get(field, "")) != str(current_row.get(field, "")) for field in REVIEW_FIELDS)


def split_open_findings(
    findings: list[dict[str, object]],
    current_rows: list[dict[str, str]] | None = None,
    accepted_findings: set[tuple[str, str, str, str]] | None = None,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    review_errors = [finding for finding in findings if is_review_error_finding(finding)]
    data_findings = [finding for finding in findings if not is_review_error_finding(finding)]
    if current_rows is None:
        return data_findings, review_errors, [], [], []

    current_rows_by_key = {row_key(row): row for row in current_rows}
    accepted_findings = accepted_findings or set()
    accepted_false_positives: list[dict[str, object]] = []
    resolved_findings: list[dict[str, object]] = []
    open_findings: list[dict[str, object]] = []
    for finding in data_findings:
        if is_accepted_false_positive(finding, current_rows_by_key, accepted_findings):
            accepted_false_positives.append(finding)
        elif is_resolved_against_current_rows(finding, current_rows_by_key):
            resolved_findings.append(finding)
        else:
            open_findings.append(finding)

    open_review_errors = [
        finding for finding in review_errors if not is_stale_review_error(finding, current_rows_by_key)
    ]
    stale_review_errors = [
        finding for finding in review_errors if is_stale_review_error(finding, current_rows_by_key)
    ]
    return open_findings, open_review_errors, resolved_findings, accepted_false_positives, stale_review_errors


def load_checkpoint(path: Path) -> tuple[set[tuple[str, str]], list[dict[str, object]]]:
    processed: set[tuple[str, str]] = set()
    findings: list[dict[str, object]] = []
    if not path.exists():
        return processed, findings

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            for key in payload.get("keys", []):
                if isinstance(key, (list, tuple)) and len(key) == 2:
                    processed.add((str(key[0]), str(key[1])))
            for finding in payload.get("findings", []):
                if isinstance(finding, dict):
                    findings.append(normalize_checkpoint_finding(finding))
    return processed, findings


def normalize_confidence(value: object) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return 0.3
    return min(1.0, max(0.0, confidence))


def normalize_finding(
    finding: dict[str, object],
    row_lookup: dict[tuple[str, str], dict[str, str]],
) -> dict[str, object] | None:
    ticker = trim(finding.get("ticker"))
    exchange = trim(finding.get("exchange"))
    if (ticker, exchange) not in row_lookup:
        return None

    field = trim(finding.get("field")).lower()
    if field not in VALID_FIELDS:
        field = "name"

    issue = trim(finding.get("issue"), 500)
    if not issue:
        return None

    severity = trim(finding.get("severity")).lower()
    if severity not in VALID_SEVERITIES:
        severity = "low"

    return {
        "ticker": ticker,
        "exchange": exchange,
        "field": field,
        "severity": severity,
        "issue": issue,
        "suggestion": trim(finding.get("suggestion"), 500),
        "confidence": normalize_confidence(finding.get("confidence")),
        "row": compact_row(row_lookup[(ticker, exchange)]),
    }


def normalize_findings(
    payload: dict[str, object],
    rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    row_lookup = {row_key(row): row for row in rows}
    raw_findings = payload.get("findings", [])
    if not isinstance(raw_findings, list):
        raise ValueError("LLM response must contain findings array.")

    normalized: list[dict[str, object]] = []
    for finding in raw_findings:
        if not isinstance(finding, dict):
            continue
        normalized_finding = normalize_finding(finding, row_lookup)
        if normalized_finding is not None:
            normalized.append(normalized_finding)
    return normalized


def call_ollama(
    *,
    url: str,
    model: str,
    prompt: str,
    timeout_seconds: int,
    temperature: float,
    num_predict: int,
) -> dict[str, Any]:
    request_payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": temperature, "num_predict": num_predict},
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(request_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    response_text = payload.get("response", "")
    if not isinstance(response_text, str):
        raise ValueError("Ollama response did not contain text response.")
    return parse_json_object(response_text)


def review_batch_with_fallback(
    rows: list[dict[str, str]],
    *,
    url: str,
    model: str,
    timeout_seconds: int,
    temperature: float,
    num_predict: int,
) -> list[tuple[list[dict[str, str]], list[dict[str, object]]]]:
    try:
        payload = call_ollama(
            url=url,
            model=model,
            prompt=build_prompt(rows),
            timeout_seconds=timeout_seconds,
            temperature=temperature,
            num_predict=num_predict,
        )
        return [(rows, normalize_findings(payload, rows))]
    except (TimeoutError, json.JSONDecodeError, ValueError) as exc:
        if len(rows) == 1:
            return [
                (
                    rows,
                    [
                        {
                            "_review_error": True,
                            "ticker": rows[0]["ticker"],
                            "exchange": rows[0]["exchange"],
                            "field": "name",
                            "severity": "low",
                            "issue": f"LLM response could not be parsed: {exc}",
                            "suggestion": "Retry this row or inspect manually.",
                            "confidence": 0.0,
                            "row": compact_row(rows[0]),
                        }
                    ],
                )
            ]
        split_at = max(1, len(rows) // 2)
        return [
            *review_batch_with_fallback(
                rows[:split_at],
                url=url,
                model=model,
                timeout_seconds=timeout_seconds,
                temperature=temperature,
                num_predict=num_predict,
            ),
            *review_batch_with_fallback(
                rows[split_at:],
                url=url,
                model=model,
                timeout_seconds=timeout_seconds,
                temperature=temperature,
                num_predict=num_predict,
            ),
        ]


def append_checkpoint(
    path: Path,
    *,
    model: str,
    input_path: Path,
    rows: list[dict[str, str]],
    findings: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "reviewed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "model": model,
        "input": display_path(input_path),
        "keys": [list(row_key(row)) for row in rows],
        "findings": findings,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def write_error_report(
    path: Path,
    *,
    model: str,
    input_path: Path,
    checkpoint_path: Path,
    reviewed_count: int,
    total_selected: int,
    findings: list[dict[str, object]],
    current_rows: list[dict[str, str]] | None = None,
    accepted_findings: set[tuple[str, str, str, str]] | None = None,
) -> None:
    (
        data_findings,
        review_errors,
        resolved_findings,
        accepted_false_positives,
        stale_review_errors,
    ) = split_open_findings(
        findings,
        current_rows,
        accepted_findings,
    )
    lines = [
        "Gemma Plausibility Review",
        f"generated_at: {datetime.now(UTC).isoformat(timespec='seconds')}",
        f"model: {model}",
        f"input: {display_path(input_path)}",
        f"checkpoint: {display_path(checkpoint_path)}",
        f"reviewed_rows: {reviewed_count}/{total_selected}",
        f"findings: {len(data_findings)}",
        f"review_errors: {len(review_errors)}",
        f"resolved_or_removed_findings: {len(resolved_findings)}",
        f"accepted_false_positive_findings: {len(accepted_false_positives)}",
        f"resolved_or_removed_review_errors: {len(stale_review_errors)}",
        "policy: conservative; missing ISIN/sector/category alone is not a finding",
        "",
    ]
    if not findings:
        lines.append("No findings recorded.")
    for finding in data_findings:
        row = finding.get("row", {})
        row_text = ""
        if isinstance(row, dict):
            row_text = (
                f" | name={row.get('name', '')}"
                f" | asset_type={row.get('asset_type', '')}"
                f" | country={row.get('country', '')}"
                f" | isin={row.get('isin', '')}"
            )
        lines.append(
            "[{severity}] {ticker} {exchange} {field}: {issue} | suggestion={suggestion} "
            "| confidence={confidence:.2f}{row_text}".format(
                severity=str(finding["severity"]).upper(),
                ticker=finding["ticker"],
                exchange=finding["exchange"],
                field=finding["field"],
                issue=finding["issue"],
                suggestion=finding.get("suggestion", ""),
                confidence=float(finding["confidence"]),
                row_text=row_text,
            )
        )
    if review_errors:
        lines.extend(["", "Review Errors"])
        for finding in review_errors:
            row = finding.get("row", {})
            row_text = ""
            if isinstance(row, dict):
                row_text = f" | name={row.get('name', '')} | isin={row.get('isin', '')}"
            lines.append(
                "[REVIEW_ERROR] {ticker} {exchange}: {issue} | suggestion={suggestion}{row_text}".format(
                    ticker=finding["ticker"],
                    exchange=finding["exchange"],
                    issue=finding["issue"],
                    suggestion=finding.get("suggestion", ""),
                    row_text=row_text,
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def select_rows(
    rows: list[dict[str, str]],
    processed: set[tuple[str, str]],
) -> list[dict[str, str]]:
    return [row for row in rows if row_key(row) not in processed]


def run(args: argparse.Namespace) -> int:
    input_path = Path(args.input)
    output_path = Path(args.out)
    checkpoint_path = Path(args.checkpoint)

    if args.reset:
        output_path.unlink(missing_ok=True)
        checkpoint_path.unlink(missing_ok=True)

    rows = read_ticker_rows(input_path, offset=args.offset, limit=args.limit)
    processed, findings = load_checkpoint(checkpoint_path)
    accepted_findings = load_accepted_findings()
    current_keys = {row_key(row) for row in rows}
    remaining = select_rows(rows, processed)
    batches = chunk_rows(remaining, args.batch_size)
    if args.max_batches is not None:
        batches = batches[: args.max_batches]

    for batch_index, batch in enumerate(batches, start=1):
        try:
            review_results = review_batch_with_fallback(
                batch,
                url=args.ollama_url,
                model=args.model,
                timeout_seconds=args.timeout_seconds,
                temperature=args.temperature,
                num_predict=args.num_predict,
            )
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            print(f"batch {batch_index} failed: {exc}", file=sys.stderr)
            write_error_report(
                output_path,
                model=args.model,
                input_path=input_path,
                checkpoint_path=checkpoint_path,
                reviewed_count=len(processed & current_keys),
                total_selected=len(rows),
                findings=findings,
                current_rows=rows,
                accepted_findings=accepted_findings,
            )
            return 2

        for reviewed_rows, batch_findings in review_results:
            append_checkpoint(
                checkpoint_path,
                model=args.model,
                input_path=input_path,
                rows=reviewed_rows,
                findings=batch_findings,
            )
            processed.update(row_key(row) for row in reviewed_rows)
            findings.extend(batch_findings)
            write_error_report(
                output_path,
                model=args.model,
                input_path=input_path,
                checkpoint_path=checkpoint_path,
                reviewed_count=len(processed & current_keys),
                total_selected=len(rows),
                findings=findings,
                current_rows=rows,
                accepted_findings=accepted_findings,
            )
            print(
                f"reviewed {len(processed & current_keys)}/{len(rows)} rows; "
                f"findings={len(findings)}; last_batch={len(batch_findings)}",
                flush=True,
            )
        if args.sleep_seconds:
            time.sleep(args.sleep_seconds)

    if not batches:
        write_error_report(
            output_path,
            model=args.model,
            input_path=input_path,
            checkpoint_path=checkpoint_path,
            reviewed_count=len(processed & current_keys),
            total_selected=len(rows),
            findings=findings,
            current_rows=rows,
            accepted_findings=accepted_findings,
        )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a conservative local Gemma plausibility review over tickers.csv.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="CSV input, defaults to data/tickers.csv")
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT), help="Human-readable findings file, defaults to error.txt")
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT), help="JSONL checkpoint for resumable runs")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL, help="Ollama generate API URL")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-batches", type=int, help="Stop after this many batches in the current invocation")
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--num-predict", type=int, default=1024, help="Maximum tokens Ollama may generate per request")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--reset", action="store_true", help="Delete existing output/checkpoint before running")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
