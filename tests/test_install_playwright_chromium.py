"""Prove the Playwright apt install retries a failing stub and fails over the Azure mirror."""

from __future__ import annotations

import os
import subprocess
import textwrap
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "install_playwright_chromium.sh"

AZURE = "http://azure.archive.ubuntu.com/ubuntu/\tpriority:1\n"
ARCHIVE = "https://archive.ubuntu.com/ubuntu/\tpriority:2\n"
SECURITY = "https://security.ubuntu.com/ubuntu/\tpriority:3\n"


def _write(path: Path, text: str, mode: int = 0o755) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(mode)


def _run(
    tmp_path: Path,
    *,
    failures: int,
    attempts: str = "4",
    delay: str = "0",
    mirror_text: str | None = AZURE + ARCHIVE + SECURITY,
    mirror_mode: int = 0o644,
    update_exit: int = 0,
    sudo_log: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    count_file = tmp_path / "install-count"
    update_log = tmp_path / "update-log"
    install_stub = tmp_path / "install-stub.sh"
    update_stub = tmp_path / "update-stub.sh"
    _write(
        install_stub,
        textwrap.dedent(
            f"""\
            #!/bin/bash
            n=0
            if [ -f "{count_file}" ]; then n=$(cat "{count_file}"); fi
            n=$((n + 1))
            echo "$n" > "{count_file}"
            if [ "$n" -le {failures} ]; then exit 100; fi
            exit 0
            """
        ),
    )
    _write(
        update_stub,
        textwrap.dedent(
            f"""\
            #!/bin/bash
            echo update >> "{update_log}"
            exit {update_exit}
            """
        ),
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    sudo_body = "#!/bin/sh\necho \"unexpected sudo: $*\" >&2\nexit 99\n"
    if sudo_log is not None:
        sudo_body = textwrap.dedent(
            f"""\
            #!/bin/sh
            printf '%s\n' "$*" >> "{sudo_log}"
            if [ "$1" = "-n" ] && [ "$2" = "cp" ]; then
              chmod u+w "$4"
              cat "$3" > "$4"
              exit 0
            fi
            echo "unexpected sudo: $*" >&2
            exit 99
            """
        )
    _write(bin_dir / "sudo", sudo_body)

    mirror_file = tmp_path / "apt-mirrors.txt"
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        "PLAYWRIGHT_INSTALL_CMD": str(install_stub),
        "APT_GET_UPDATE_CMD": str(update_stub),
        "PLAYWRIGHT_INSTALL_ATTEMPTS": attempts,
        "PLAYWRIGHT_INSTALL_DELAY_SECONDS": delay,
        "APT_MIRRORS_FILE": str(mirror_file),
    }
    if mirror_text is None:
        env["APT_MIRRORS_FILE"] = str(tmp_path / "missing-mirrors.txt")
    else:
        mirror_file.write_text(mirror_text, encoding="utf-8")
        mirror_file.chmod(mirror_mode)

    return subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _install_count(tmp_path: Path) -> int:
    return int((tmp_path / "install-count").read_text(encoding="utf-8"))


def _update_count(tmp_path: Path) -> int:
    log = tmp_path / "update-log"
    if not log.exists():
        return 0
    return len(log.read_text(encoding="utf-8").splitlines())


def test_first_attempt_success_leaves_the_azure_mirror_alone(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=0)
    assert result.returncode == 0, result.stderr
    assert _install_count(tmp_path) == 1
    assert _update_count(tmp_path) == 0
    assert (tmp_path / "apt-mirrors.txt").read_text(encoding="utf-8") == AZURE + ARCHIVE + SECURITY
    assert "failed" not in result.stderr


def test_retry_refreshes_apt_and_drops_the_azure_mirror(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=1, attempts="4", delay="0")
    assert result.returncode == 0, result.stderr
    assert _install_count(tmp_path) == 2
    assert _update_count(tmp_path) == 1
    assert (tmp_path / "apt-mirrors.txt").read_text(encoding="utf-8") == ARCHIVE + SECURITY
    assert "attempt 1/4, exit 100" in result.stderr
    assert "Dropping azure.archive.ubuntu.com" in result.stderr
    assert "Refreshing apt indexes, then retrying in 0s" in result.stderr


def test_exhausted_retries_keep_the_stub_exit_code(tmp_path: Path) -> None:
    started = time.monotonic()
    result = _run(tmp_path, failures=9, attempts="3", delay="1")
    elapsed = time.monotonic() - started
    assert result.returncode == 100, result.stderr
    assert _install_count(tmp_path) == 3
    assert _update_count(tmp_path) == 2
    assert "attempt 3/3, exit 100" in result.stderr
    # Backoff is 1s then 2s between the three attempts.
    assert elapsed >= 3.0
    assert (tmp_path / "apt-mirrors.txt").read_text(encoding="utf-8") == ARCHIVE + SECURITY


def test_azure_only_mirror_list_is_rewritten_not_emptied(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=1, attempts="2", mirror_text=AZURE)
    assert result.returncode == 0, result.stderr
    rewritten = (tmp_path / "apt-mirrors.txt").read_text(encoding="utf-8")
    assert rewritten == "https://archive.ubuntu.com/ubuntu/\tpriority:1\n"
    assert "azure.archive.ubuntu.com" not in rewritten
    assert "Replacing azure.archive.ubuntu.com" in result.stderr


def test_missing_mirror_file_still_retries(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=1, attempts="2", mirror_text=None)
    assert result.returncode == 0, result.stderr
    assert _install_count(tmp_path) == 2
    assert _update_count(tmp_path) == 1
    assert not (tmp_path / "missing-mirrors.txt").exists()


def test_apt_update_failure_does_not_block_the_next_attempt(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=1, attempts="2", update_exit=1)
    assert result.returncode == 0, result.stderr
    assert _install_count(tmp_path) == 2
    assert "apt-get update failed; continuing to retry the install" in result.stderr


def test_unwritable_mirror_file_is_replaced_via_sudo(tmp_path: Path) -> None:
    sudo_log = tmp_path / "sudo-log"
    result = _run(
        tmp_path,
        failures=1,
        attempts="2",
        mirror_mode=0o444,
        sudo_log=sudo_log,
    )
    assert result.returncode == 0, result.stderr
    mirror = tmp_path / "apt-mirrors.txt"
    assert mirror.read_text(encoding="utf-8") == ARCHIVE + SECURITY
    sudo_line = sudo_log.read_text(encoding="utf-8").strip()
    assert sudo_line.startswith("-n cp ")
    assert sudo_line.endswith(f" {mirror}")


def test_rejects_a_non_positive_attempt_count(tmp_path: Path) -> None:
    result = _run(tmp_path, failures=0, attempts="0")
    assert result.returncode == 2
    assert "PLAYWRIGHT_INSTALL_ATTEMPTS" in result.stderr
    assert not (tmp_path / "install-count").exists()
