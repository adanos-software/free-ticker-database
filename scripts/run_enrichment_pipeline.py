from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SECTOR_PRIORITY_EXCHANGES = [
    "OTC",
    "SSE",
    "SZSE",
    "XETRA",
    "B3",
    "NYSE ARCA",
    "KRX",
    "LSE",
    "TSX",
    "TSXV",
    "TSE",
    "STO",
    "TASE",
]


@dataclass(frozen=True)
class StageCommand:
    name: str
    command: list[str]
    mutates_data: bool
    notes: str


@dataclass(frozen=True)
class PipelineOptions:
    python: str
    include_fetch: bool = True
    include_reviewed_backfills: bool = True
    include_secondary_network: bool = False
    apply_reviewed_backfills: bool = False
    only_stages: tuple[str, ...] = ()


def command_with_exchanges(base: list[str], exchanges: list[str]) -> list[str]:
    command = list(base)
    for exchange in exchanges:
        command.extend(["--exchange", exchange])
    return command


def maybe_apply(command: list[str], *, apply: bool) -> list[str]:
    return [*command, "--apply"] if apply else command


def build_pipeline_commands(options: PipelineOptions) -> list[StageCommand]:
    commands: list[StageCommand] = []
    py = options.python

    if options.include_fetch:
        commands.append(
            StageCommand(
                name="fetch_masterfiles",
                command=[py, "scripts/fetch_exchange_masterfiles.py"],
                mutates_data=True,
                notes="Refresh official exchange masterfiles before deriving backlog priorities.",
            )
        )

    commands.append(
        StageCommand(
            name="completion_backlog_before",
            command=[py, "scripts/build_completion_backlog.py"],
            mutates_data=True,
            notes="Build field-level completion backlog from current exports and coverage report.",
        )
    )

    if options.include_reviewed_backfills:
        peer_command = command_with_exchanges(
            [py, "scripts/backfill_sector_from_isin_peers.py"],
            SECTOR_PRIORITY_EXCHANGES,
        )
        finance_command = command_with_exchanges(
            [py, "scripts/backfill_financedatabase_metadata.py"],
            SECTOR_PRIORITY_EXCHANGES,
        )
        etf_category_command = command_with_exchanges(
            [py, "scripts/backfill_etf_categories_from_names.py"],
            SECTOR_PRIORITY_EXCHANGES,
        )
        commands.extend(
            [
                StageCommand(
                    name="same_isin_sector_peer_backfill",
                    command=maybe_apply(peer_command, apply=options.apply_reviewed_backfills),
                    mutates_data=options.apply_reviewed_backfills,
                    notes="Local peer propagation; safe only when same-asset same-ISIN peers unanimously agree.",
                ),
                StageCommand(
                    name="financedatabase_sector_backfill",
                    command=maybe_apply(finance_command, apply=options.apply_reviewed_backfills),
                    mutates_data=options.apply_reviewed_backfills,
                    notes="Secondary FinanceDatabase sectors; reviewed name/exchange gates before apply.",
                ),
                StageCommand(
                    name="etf_name_category_backfill",
                    command=maybe_apply(etf_category_command, apply=options.apply_reviewed_backfills),
                    mutates_data=options.apply_reviewed_backfills,
                    notes="Deterministic ETF-name classifier for reviewed etf_category fills.",
                ),
            ]
        )

    if options.include_secondary_network:
        eodhd_command = command_with_exchanges(
            [py, "scripts/backfill_eodhd_metadata.py"],
            ["TSX", "TSXV", "NEO", "B3", "SSE", "SZSE"],
        )
        yahoo_command = command_with_exchanges(
            [py, "scripts/backfill_yahoo_missing_isins.py", "--asset-type", "ETF"],
            ["BATS", "NASDAQ", "NYSE ARCA", "TSX", "TSXV", "NEO"],
        )
        commands.extend(
            [
                StageCommand(
                    name="eodhd_reviewed_isin_backfill",
                    command=maybe_apply(eodhd_command, apply=options.apply_reviewed_backfills),
                    mutates_data=options.apply_reviewed_backfills,
                    notes="Secondary EODHD identifier candidates; requires API token and strict review gates.",
                ),
                StageCommand(
                    name="yahoo_reviewed_etf_isin_backfill",
                    command=maybe_apply(yahoo_command, apply=options.apply_reviewed_backfills),
                    mutates_data=options.apply_reviewed_backfills,
                    notes="Secondary Yahoo ETF identifier candidates; use only as small reviewed batches.",
                ),
            ]
        )

    commands.extend(
        [
            StageCommand(
                name="rebuild_dataset",
                command=[py, "scripts/rebuild_dataset.py"],
                mutates_data=True,
                notes="Rebuild canonical exports after accepted review overrides.",
            ),
            StageCommand(
                name="enrich_global_identifiers",
                command=[py, "scripts/enrich_global_identifiers.py"],
                mutates_data=True,
                notes="Refresh CIK/FIGI/LEI overlay snapshots from configured sources/cache.",
            ),
            StageCommand(
                name="build_listing_history",
                command=[py, "scripts/build_listing_history.py"],
                mutates_data=True,
                notes="Refresh listing-keyed status history after the rebuild.",
            ),
            StageCommand(
                name="build_coverage_report",
                command=[py, "scripts/build_coverage_report.py"],
                mutates_data=True,
                notes="Refresh coverage and collision metrics.",
            ),
            StageCommand(
                name="audit_dataset",
                command=[py, "scripts/audit_dataset.py", "--write-defaults"],
                mutates_data=True,
                notes="Refresh review queue for suspicious rows after enrichment.",
            ),
            StageCommand(
                name="completion_backlog_after",
                command=[py, "scripts/build_completion_backlog.py"],
                mutates_data=True,
                notes="Refresh completion backlog after the final coverage report.",
            ),
        ]
    )

    if options.only_stages:
        selected = set(options.only_stages)
        commands = [command for command in commands if command.name in selected]
    return commands


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ticker database enrichment pipeline in a reproducible order.")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned stages without executing them.")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip official masterfile refresh.")
    parser.add_argument("--skip-reviewed-backfills", action="store_true", help="Skip local reviewed backfill report stages.")
    parser.add_argument(
        "--include-secondary-network",
        action="store_true",
        help="Include EODHD/Yahoo reviewed candidate stages. These require credentials/network and stay non-applying unless --apply-reviewed-backfills is set.",
    )
    parser.add_argument(
        "--apply-reviewed-backfills",
        action="store_true",
        help="Pass --apply to reviewed backfill stages so accepted candidates merge into review overrides.",
    )
    parser.add_argument("--only-stage", action="append", default=[], help="Run only the named stage. Repeat for multiple stages.")
    return parser.parse_args(argv)


def stage_to_json(stage: StageCommand) -> dict[str, object]:
    return {
        "name": stage.name,
        "command": stage.command,
        "mutates_data": stage.mutates_data,
        "notes": stage.notes,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    options = PipelineOptions(
        python=sys.executable,
        include_fetch=not args.skip_fetch,
        include_reviewed_backfills=not args.skip_reviewed_backfills,
        include_secondary_network=args.include_secondary_network,
        apply_reviewed_backfills=args.apply_reviewed_backfills,
        only_stages=tuple(args.only_stage),
    )
    commands = build_pipeline_commands(options)

    if args.dry_run:
        print(json.dumps({"stages": [stage_to_json(stage) for stage in commands]}, indent=2))
        return

    for stage in commands:
        print(json.dumps(stage_to_json(stage), sort_keys=True))
        subprocess.run(stage.command, cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
