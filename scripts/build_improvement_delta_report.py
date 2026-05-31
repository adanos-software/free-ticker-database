from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from scripts.build_improvement_baseline_report import build_payload as build_current_snapshot
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from build_improvement_baseline_report import build_payload as build_current_snapshot


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_BASELINE_JSON = REPORTS_DIR / "improvement_baseline.json"
DEFAULT_JSON_OUT = REPORTS_DIR / "improvement_deltas.json"
DEFAULT_MD_OUT = REPORTS_DIR / "improvement_deltas.md"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def compare_values(baseline: Any, current: Any) -> dict[str, Any] | None:
    if isinstance(baseline, dict) or isinstance(current, dict):
        baseline_dict = baseline if isinstance(baseline, dict) else {}
        current_dict = current if isinstance(current, dict) else {}
        children = compare_mapping(baseline_dict, current_dict)
        return {"children": children} if children else None
    if is_number(baseline) and is_number(current):
        delta = current - baseline
        return {"baseline": baseline, "current": current, "delta": delta}
    if baseline != current:
        return {"baseline": baseline, "current": current, "delta": None}
    return {"baseline": baseline, "current": current, "delta": 0} if is_number(current) else None


def compare_mapping(baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in sorted(set(baseline) | set(current)):
        compared = compare_values(baseline.get(key), current.get(key))
        if compared is not None:
            result[key] = compared
    return result


def flatten_numeric_deltas(tree: dict[str, Any], prefix: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, value in tree.items():
        path = f"{prefix}.{key}" if prefix else key
        if not isinstance(value, dict):
            continue
        if "children" in value:
            rows.extend(flatten_numeric_deltas(value["children"], path))
        elif is_number(value.get("delta")):
            rows.append(
                {
                    "path": path,
                    "baseline": value.get("baseline"),
                    "current": value.get("current"),
                    "delta": value.get("delta"),
                }
            )
    return rows


def markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def append_source_files_markdown(lines: list[str], payload: dict[str, Any]) -> None:
    meta = payload.get("_meta", {})
    source_files = meta.get("source_files", {}) if isinstance(meta, dict) else {}
    if not isinstance(source_files, dict):
        source_files = {}
    lines.extend(["", "## Source Files", "", "| Key | Path |", "|---|---|"])
    for key, path in sorted(source_files.items()):
        lines.append(f"| `{markdown_cell(key)}` | `{markdown_cell(path)}` |")


def numeric_delta(
    baseline_global: dict[str, Any],
    current_global: dict[str, Any],
    key: str,
) -> dict[str, Any]:
    baseline = baseline_global.get(key, 0)
    current = current_global.get(key, 0)
    if not is_number(baseline):
        baseline = 0
    if not is_number(current):
        current = 0
    return {"baseline": baseline, "current": current, "delta": current - baseline}


def delta_direction(metric: str, delta: Any) -> str:
    if not is_number(delta):
        return "non_numeric_delta_review_required"
    if delta == 0:
        return "unchanged"
    lower_is_better = (
        "source_gap" in metric
        or "warn" in metric
        or "quarantine" in metric
        or "collision" in metric
        or "missing" in metric
    )
    if lower_is_better:
        return "improved" if delta < 0 else "regressed"
    return "improved" if delta > 0 else "regressed"


def delta_review_policy(metric: str, delta: Any) -> str:
    direction = delta_direction(metric, delta)
    if direction == "unchanged":
        return "no_data_change_inferred"
    if direction == "improved":
        return "source_level_review_required_before_claiming_completion"
    if direction == "regressed":
        return "regression_review_required_before_release"
    return "manual_delta_review_required"


def delta_context(metric: str, row: dict[str, Any], exchange: str = "") -> str:
    scope = exchange or "global"
    delta = row.get("delta")
    return (
        f"scope={scope};"
        f"metric={metric};"
        f"baseline={row.get('baseline', 0)};"
        f"current={row.get('current', 0)};"
        f"delta={delta};"
        f"direction={delta_direction(metric, delta)};"
        f"review_policy={delta_review_policy(metric, delta)}"
    )


def with_delta_context(metric: str, row: dict[str, Any], exchange: str = "") -> dict[str, Any]:
    result = dict(row)
    result["delta_context"] = delta_context(metric, result, exchange)
    return result


def build_acceptance_delta_matrix(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    baseline_global = baseline_payload.get("global_baseline", {})
    current_global = current_payload.get("global_baseline", {})
    return {
        "isin_delta": with_delta_context(
            "isin_delta", numeric_delta(baseline_global, current_global, "isin_coverage")
        ),
        "sector_delta": with_delta_context(
            "sector_delta", numeric_delta(baseline_global, current_global, "stock_sector_coverage")
        ),
        "category_delta": with_delta_context(
            "category_delta", numeric_delta(baseline_global, current_global, "etf_category_coverage")
        ),
        "source_gap_delta": with_delta_context(
            "source_gap_delta", numeric_delta(baseline_global, current_global, "source_gap_rows")
        ),
        "warn_delta": with_delta_context(
            "warn_delta", numeric_delta(baseline_global, current_global, "entry_quality_warn_rows")
        ),
        "quarantine_delta": with_delta_context(
            "quarantine_delta", numeric_delta(baseline_global, current_global, "entry_quality_quarantine_rows")
        ),
    }


def exchange_acceptance_deltas(
    baseline_payload: dict[str, Any],
    current_payload: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    baseline_exchanges = baseline_payload.get("exchange_baseline", {})
    current_exchanges = current_payload.get("exchange_baseline", {})
    rows: dict[str, dict[str, dict[str, Any]]] = {}
    for exchange in sorted(set(baseline_exchanges) | set(current_exchanges)):
        baseline = baseline_exchanges.get(exchange, {})
        current = current_exchanges.get(exchange, {})
        rows[exchange] = {
            "isin_delta": with_delta_context(
                "isin_delta", numeric_delta(baseline, current, "isin_coverage"), exchange
            ),
            "sector_delta": with_delta_context(
                "sector_delta", numeric_delta(baseline, current, "sector_coverage"), exchange
            ),
            "source_gap_delta": with_delta_context(
                "source_gap_delta", numeric_delta(baseline, current, "source_gap_rows"), exchange
            ),
            "warn_delta": with_delta_context(
                "warn_delta", numeric_delta(baseline, current, "entry_quality_warn_rows"), exchange
            ),
            "quarantine_delta": with_delta_context(
                "quarantine_delta", numeric_delta(baseline, current, "entry_quality_quarantine_rows"), exchange
            ),
            "quality_source_gap_delta": with_delta_context(
                "quality_source_gap_delta",
                numeric_delta(baseline, current, "entry_quality_source_gap_rows"),
                exchange,
            ),
            "masterfile_collision_delta": with_delta_context(
                "masterfile_collision_delta", numeric_delta(baseline, current, "masterfile_collisions"), exchange
            ),
            "masterfile_missing_delta": with_delta_context(
                "masterfile_missing_delta", numeric_delta(baseline, current, "masterfile_missing"), exchange
            ),
        }
    return rows


def build_delta_payload(baseline_payload: dict[str, Any], current_payload: dict[str, Any]) -> dict[str, Any]:
    global_deltas = compare_mapping(
        baseline_payload.get("global_baseline", {}),
        current_payload.get("global_baseline", {}),
    )
    campaign_deltas = compare_mapping(
        baseline_payload.get("campaign_baseline", {}),
        current_payload.get("campaign_baseline", {}),
    )
    exchange_deltas = compare_mapping(
        baseline_payload.get("exchange_baseline", {}),
        current_payload.get("exchange_baseline", {}),
    )
    numeric_rows = [
        *flatten_numeric_deltas(global_deltas, "global_baseline"),
        *flatten_numeric_deltas(campaign_deltas, "campaign_baseline"),
        *flatten_numeric_deltas(exchange_deltas, "exchange_baseline"),
    ]
    changed_numeric_rows = [row for row in numeric_rows if row["delta"] != 0]
    acceptance_delta_matrix = build_acceptance_delta_matrix(baseline_payload, current_payload)
    exchange_matrix = exchange_acceptance_deltas(baseline_payload, current_payload)
    changed_exchange_rows = {
        exchange: values
        for exchange, values in exchange_matrix.items()
        if any(value["delta"] != 0 for value in values.values())
    }
    current_meta = current_payload.get("_meta", {})
    current_source_files = {}
    if isinstance(current_meta, dict):
        source_files = current_meta.get("source_files") or current_meta.get("source_reports") or {}
        if isinstance(source_files, dict):
            current_source_files = {
                f"current_snapshot_{key}": value
                for key, value in source_files.items()
            }
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "baseline_generated_at": baseline_payload.get("_meta", {}).get("generated_at", ""),
            "current_generated_at": current_payload.get("_meta", {}).get("generated_at", ""),
            "baseline_path": "data/reports/improvement_baseline.json",
            "source_files": {
                "baseline_snapshot": "data/reports/improvement_baseline.json",
                **current_source_files,
            },
            "policy": "Delta report only. Positive or negative deltas require source-level review before any data change is inferred.",
        },
        "summary": {
            "numeric_delta_rows": len(numeric_rows),
            "changed_numeric_delta_rows": len(changed_numeric_rows),
            "global_changed_numeric_delta_rows": sum(1 for row in changed_numeric_rows if row["path"].startswith("global_baseline.")),
            "campaign_changed_numeric_delta_rows": sum(1 for row in changed_numeric_rows if row["path"].startswith("campaign_baseline.")),
            "exchange_changed_numeric_delta_rows": sum(1 for row in changed_numeric_rows if row["path"].startswith("exchange_baseline.")),
            "changed_exchange_rows": len(changed_exchange_rows),
        },
        "global_deltas": global_deltas,
        "campaign_deltas": campaign_deltas,
        "exchange_deltas": exchange_deltas,
        "acceptance_delta_matrix": acceptance_delta_matrix,
        "exchange_acceptance_delta_matrix": exchange_matrix,
        "changed_exchange_acceptance_deltas": changed_exchange_rows,
        "changed_numeric_deltas": changed_numeric_rows,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Improvement Deltas",
        "",
        f"Generated: `{payload['_meta']['generated_at']}`",
        f"Baseline: `{payload['_meta']['baseline_generated_at']}`",
        "",
        "This report compares current campaign metrics against `data/reports/improvement_baseline.json`.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in payload["summary"].items():
        lines.append(f"| {key} | {value} |")
    lines.extend(
        [
            "",
            "## Acceptance Delta Matrix",
            "",
            "| Acceptance Metric | Baseline | Current | Delta | Review Context |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for key, value in payload["acceptance_delta_matrix"].items():
        lines.append(
            f"| `{key}` | {value['baseline']} | {value['current']} | {value['delta']} | "
            f"`{markdown_cell(value.get('delta_context', ''))}` |"
        )
    lines.extend(["", "## Changed Exchange Acceptance Deltas", "", "| Exchange | Changed Metrics |", "|---|---|"])
    for exchange, values in payload["changed_exchange_acceptance_deltas"].items():
        changed = {key: value for key, value in values.items() if value["delta"] != 0}
        lines.append(f"| {exchange} | `{json.dumps(changed, ensure_ascii=False, sort_keys=True)}` |")
    if not payload["changed_exchange_acceptance_deltas"]:
        lines.append("| none | `{}` |")
    lines.extend(["", "## Changed Numeric Deltas", "", "| Path | Baseline | Current | Delta |", "|---|---:|---:|---:|"])
    for row in payload["changed_numeric_deltas"][:200]:
        lines.append(f"| `{row['path']}` | {row['baseline']} | {row['current']} | {row['delta']} |")
    if not payload["changed_numeric_deltas"]:
        lines.append("| none |  |  | 0 |")
    append_source_files_markdown(lines, payload)
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare current improvement metrics against a baseline snapshot.")
    parser.add_argument("--baseline-json", type=Path, default=DEFAULT_BASELINE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseline = load_json(args.baseline_json)
    current = build_current_snapshot()
    payload = build_delta_payload(baseline, current)
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.write_text(render_markdown(payload) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))


if __name__ == "__main__":
    main()
