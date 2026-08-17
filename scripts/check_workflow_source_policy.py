from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
ALLOWED_CONTENTS_WRITE_WORKFLOWS = {
    "delisting-report.yml",
    "freshness.yml",
    "isin-validation.yml",
    "masterfile-rotation.yml",
    "nasdaq-us-new-listings.yml",
    "release.yml",
    "symbol-changes.yml",
}
FORBIDDEN_PAYLOAD_SUFFIXES = (".patch.xz.b64", ".tar.xz.b64")
GIT_PUSH_RE = re.compile(r"(?<![A-Za-z0-9_-])git\s+push(?![A-Za-z0-9_-])", re.IGNORECASE)
CONTENTS_WRITE_RE = re.compile(r"^\s*contents\s*:\s*write\s*(?:#.*)?$", re.MULTILINE)


@dataclass(frozen=True)
class PolicyViolation:
    path: str
    rule: str
    message: str


def relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def iter_repository_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in {".git", ".pytest_cache", "__pycache__"} for part in path.parts):
            continue
        yield path


def check_repository(root: Path = ROOT) -> list[PolicyViolation]:
    violations: list[PolicyViolation] = []
    bootstrap = root / ".bootstrap"
    if bootstrap.exists():
        violations.append(
            PolicyViolation(
                path=".bootstrap",
                rule="no_bootstrap_source_transport",
                message="Materialize production source files; do not commit a hidden bootstrap tree.",
            )
        )

    for path in iter_repository_files(root):
        rel = relative(path, root)
        if rel.endswith(FORBIDDEN_PAYLOAD_SUFFIXES):
            violations.append(
                PolicyViolation(
                    path=rel,
                    rule="no_compressed_source_payloads",
                    message="Compressed source patches are not reviewable in a pull request.",
                )
            )

    workflows_dir = root / ".github" / "workflows"
    if not workflows_dir.exists():
        return violations

    for path in sorted(workflows_dir.glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        rel = relative(path, root)
        if GIT_PUSH_RE.search(text):
            violations.append(
                PolicyViolation(
                    path=rel,
                    rule="no_workflow_self_push",
                    message="Validation workflows must not mutate or push the branch they review.",
                )
            )
        if CONTENTS_WRITE_RE.search(text) and path.name not in ALLOWED_CONTENTS_WRITE_WORKFLOWS:
            violations.append(
                PolicyViolation(
                    path=rel,
                    rule="contents_write_allowlist",
                    message="Repository write permission is limited to reviewed maintenance/release workflows.",
                )
            )
    return violations


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reject opaque source payloads and self-mutating workflows.")
    parser.add_argument("--root", type=Path, default=ROOT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    violations = check_repository(args.root.resolve())
    if violations:
        for violation in violations:
            print(f"ERROR {violation.rule}: {violation.path}: {violation.message}")
        return 1
    print("Workflow/source policy passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
