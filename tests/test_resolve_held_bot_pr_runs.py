"""Held bot pull_request CI runs are approved, or cancelled when approve is denied."""

from __future__ import annotations

import json
import os
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "resolve_held_bot_pr_runs.sh"
BRANCH = "automation/symbol-changes"
REPO = "adanos-software/free-ticker-database"


def _run(
    tmp_path: Path,
    runs: list[dict[str, object]],
    *,
    branch: str = BRANCH,
    attempts: str = "1",
    delay: str = "0",
    repo: str | None = REPO,
    approve_fail: list[int] | None = None,
    cancel_fail: list[int] | None = None,
    list_failures: int = 0,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    state_path = tmp_path / "state.json"
    log_path = tmp_path / "gh-log.jsonl"
    state = {
        "list_calls": 0,
        "list_failures_remaining": list_failures,
        "runs": runs,
        "approve_fail": approve_fail or [],
        "cancel_fail": cancel_fail or [],
        "statuses": {str(run["id"]): run["status"] for run in runs},
        "actions": [],
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "gh"
    stub.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            import sys
            from pathlib import Path

            state_path = Path(os.environ["GH_STUB_STATE"])
            log_path = Path(os.environ["GH_STUB_LOG"])
            args = sys.argv[1:]
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(args) + "\\n")

            state = json.loads(state_path.read_text(encoding="utf-8"))

            def save() -> None:
                state_path.write_text(json.dumps(state), encoding="utf-8")

            method = "GET"
            jq = None
            fields = {}
            positionals = []
            index = 0
            while index < len(args):
                arg = args[index]
                if arg in {"--paginate", "--silent", "--slurp"}:
                    index += 1
                    continue
                if arg in {"-X", "--method"}:
                    method = args[index + 1]
                    index += 2
                    continue
                if arg in {"-q", "--jq"}:
                    jq = args[index + 1]
                    index += 2
                    continue
                if arg in {"-f", "--raw-field"}:
                    key, value = args[index + 1].split("=", 1)
                    fields[key] = value
                    index += 2
                    continue
                if arg == "--repo":
                    fields["repo"] = args[index + 1]
                    index += 2
                    continue
                if arg.startswith("-"):
                    print(f"unexpected flag {arg}", file=sys.stderr)
                    raise SystemExit(2)
                positionals.append(arg)
                index += 1

            state.setdefault("actions", [])

            if positionals[:1] == ["api"]:
                endpoint = positionals[1]
                if endpoint.endswith("/runs") and "/actions/workflows/" in endpoint:
                    state["list_calls"] = state.get("list_calls", 0) + 1
                    remaining = int(state.get("list_failures_remaining", 0))
                    if remaining:
                        state["list_failures_remaining"] = remaining - 1
                        state["actions"].append(
                            {"op": "list", "failed": True, "fields": fields, "endpoint": endpoint, "jq": jq}
                        )
                        save()
                        print("stub list failure", file=sys.stderr)
                        raise SystemExit(1)
                    listed = []
                    for run in state["runs"]:
                        run_id = str(run["id"])
                        status = state["statuses"].get(run_id, run["status"])
                        if fields.get("event") and run["event"] != fields["event"]:
                            continue
                        if fields.get("branch") and run["head_branch"] != fields["branch"]:
                            continue
                        if fields.get("status") and status != fields["status"]:
                            continue
                        if int(run.get("appear_on_list_call", 1)) > state["list_calls"]:
                            continue
                        actor = (run.get("actor") or {}).get("login")
                        if actor != "github-actions[bot]" or run["event"] != "pull_request":
                            continue
                        listed.append(run_id)
                    state["actions"].append(
                        {
                            "op": "list",
                            "failed": False,
                            "fields": fields,
                            "endpoint": endpoint,
                            "jq": jq,
                            "ids": listed,
                        }
                    )
                    save()
                    if jq is None:
                        print("missing jq", file=sys.stderr)
                        raise SystemExit(2)
                    if listed:
                        print("\\n".join(listed))
                    raise SystemExit(0)

                if endpoint.endswith("/approve") and "/actions/runs/" in endpoint:
                    run_id = endpoint.rstrip("/").split("/")[-2]
                    state["actions"].append(
                        {"op": "approve", "id": run_id, "method": method, "endpoint": endpoint}
                    )
                    if run_id in {str(item) for item in state.get("approve_fail", [])}:
                        save()
                        print(f"approve denied for {run_id}", file=sys.stderr)
                        raise SystemExit(1)
                    state["statuses"][run_id] = "queued"
                    save()
                    raise SystemExit(0)

                if (
                    "/actions/runs/" in endpoint
                    and endpoint.rstrip("/").split("/")[-1].isdigit()
                    and method == "GET"
                ):
                    run_id = endpoint.rstrip("/").split("/")[-1]
                    read_status = next(
                        (
                            run.get("read_status")
                            for run in state["runs"]
                            if str(run["id"]) == run_id and "read_status" in run
                        ),
                        None,
                    )
                    status = (
                        str(read_status)
                        if read_status is not None
                        else state["statuses"].get(run_id, "")
                    )
                    state["actions"].append({"op": "status", "id": run_id, "jq": jq, "status": status})
                    save()
                    if jq != ".status":
                        print(f"unexpected status jq {jq}", file=sys.stderr)
                        raise SystemExit(2)
                    print(status)
                    raise SystemExit(0)

                print(f"unexpected api call {endpoint}", file=sys.stderr)
                raise SystemExit(2)

            if positionals[:2] == ["run", "cancel"]:
                run_id = positionals[2]
                state["actions"].append({"op": "cancel", "id": run_id, "repo": fields.get("repo")})
                if run_id in {str(item) for item in state.get("cancel_fail", [])}:
                    save()
                    print(f"cancel failed for {run_id}", file=sys.stderr)
                    raise SystemExit(1)
                state["statuses"][run_id] = "cancelled"
                save()
                raise SystemExit(0)

            print(f"unexpected gh args {args}", file=sys.stderr)
            raise SystemExit(2)
            """
        ),
        encoding="utf-8",
    )
    stub.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        "GH_STUB_STATE": str(state_path),
        "GH_STUB_LOG": str(log_path),
        "GH_TOKEN": "stub-token",
        "HELD_RUN_POLL_ATTEMPTS": attempts,
        "HELD_RUN_POLL_DELAY_SECONDS": delay,
    }
    if repo is None:
        env.pop("GITHUB_REPOSITORY", None)
    else:
        env["GITHUB_REPOSITORY"] = repo
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", str(SCRIPT), branch],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _state(tmp_path: Path) -> dict[str, object]:
    return json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))


def _actions(tmp_path: Path) -> list[dict[str, object]]:
    actions = _state(tmp_path)["actions"]
    assert isinstance(actions, list)
    return actions


def _ops(tmp_path: Path) -> list[str]:
    return [str(action["op"]) for action in _actions(tmp_path)]


def _bot_run(
    run_id: int,
    *,
    status: str = "action_required",
    event: str = "pull_request",
    branch: str = BRANCH,
    actor: str = "github-actions[bot]",
    appear_on_list_call: int = 1,
    read_status: str | None = None,
) -> dict[str, object]:
    run: dict[str, object] = {
        "id": run_id,
        "status": status,
        "event": event,
        "head_branch": branch,
        "actor": {"login": actor},
        "appear_on_list_call": appear_on_list_call,
    }
    if read_status is not None:
        run["read_status"] = read_status
    return run


def test_no_held_runs_exits_clean_without_approve_or_cancel(tmp_path: Path) -> None:
    result = _run(tmp_path, [])

    assert result.returncode == 0, result.stderr
    assert "No held pull_request CI runs" in result.stdout
    assert _ops(tmp_path) == ["list", "list"]
    statuses = [action["fields"]["status"] for action in _actions(tmp_path)]  # type: ignore[index]
    assert statuses == ["action_required", "waiting"]


def test_approve_success_does_not_cancel(tmp_path: Path) -> None:
    result = _run(tmp_path, [_bot_run(43738088875)], attempts="2")

    assert result.returncode == 0, result.stderr
    assert "Approved held CI run 43738088875" in result.stdout
    assert "cancel" not in _ops(tmp_path)
    approve = next(action for action in _actions(tmp_path) if action["op"] == "approve")
    assert approve["method"] == "POST"
    assert approve["endpoint"] == f"repos/{REPO}/actions/runs/43738088875/approve"
    listed = [action for action in _actions(tmp_path) if action["op"] == "list" and not action["failed"]]
    assert listed[0]["endpoint"] == f"repos/{REPO}/actions/workflows/ci.yml/runs"
    assert listed[0]["fields"] == {
        "branch": BRANCH,
        "event": "pull_request",
        "status": "action_required",
        "per_page": "100",
    }
    assert listed[0]["jq"] == (
        '.workflow_runs[] | select(.actor.login == "github-actions[bot]" '
        f'and .event == "pull_request" and .head_branch == "{BRANCH}") | .id'
    )


def test_failed_approve_cancels_run_that_is_still_held(tmp_path: Path) -> None:
    result = _run(tmp_path, [_bot_run(12, status="waiting")], approve_fail=[12])

    assert result.returncode == 0, result.stderr
    assert "Cancelled held CI run 12" in result.stdout
    cancel = next(action for action in _actions(tmp_path) if action["op"] == "cancel")
    assert cancel == {"op": "cancel", "id": "12", "repo": REPO}
    assert _state(tmp_path)["statuses"]["12"] == "cancelled"  # type: ignore[index]


def test_failed_approve_leaves_a_run_that_already_moved_on(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        [_bot_run(13, read_status="completed")],
        approve_fail=[13],
    )

    assert result.returncode == 0, result.stderr
    assert "CI run 13 is completed after the failed approve" in result.stdout
    assert "cancel" not in _ops(tmp_path)


def test_ignores_human_and_non_pull_request_runs(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        [
            _bot_run(21, actor="octocat"),
            _bot_run(22, event="workflow_dispatch"),
            _bot_run(23, branch="automation/other"),
        ],
    )

    assert result.returncode == 0, result.stderr
    assert "No held pull_request CI runs" in result.stdout
    assert _ops(tmp_path) == ["list", "list"]


def test_run_that_appears_on_a_later_poll_is_resolved(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        [_bot_run(31, appear_on_list_call=3)],
        approve_fail=[31],
        attempts="3",
    )

    assert result.returncode == 0, result.stderr
    assert "Cancelled held CI run 31" in result.stdout
    assert _ops(tmp_path).count("list") >= 3


def test_list_failure_is_retried(tmp_path: Path) -> None:
    result = _run(
        tmp_path,
        [_bot_run(41)],
        approve_fail=[41],
        list_failures=2,
        attempts="2",
    )

    assert result.returncode == 0, result.stderr
    assert "Cancelled held CI run 41" in result.stdout


def test_unresolved_cancel_fails_the_script(tmp_path: Path) -> None:
    result = _run(tmp_path, [_bot_run(51)], approve_fail=[51], cancel_fail=[51])

    assert result.returncode == 1
    assert "Could not cancel held CI run 51" in result.stderr
    assert "Left 1 held CI run" in result.stderr


def test_every_list_failing_fails_the_script(tmp_path: Path) -> None:
    result = _run(tmp_path, [], list_failures=4, attempts="2")

    assert result.returncode == 1
    assert "Could not list pull_request CI runs" in result.stderr
    assert "cancel" not in _ops(tmp_path)


def test_missing_repository_and_unsafe_branch_are_rejected(tmp_path: Path) -> None:
    missing = _run(tmp_path, [], repo=None)
    assert missing.returncode == 2
    assert "GITHUB_REPOSITORY is required" in missing.stderr

    unsafe_dir = tmp_path / "unsafe"
    unsafe_dir.mkdir()
    unsafe = _run(unsafe_dir, [], branch="automation/foo;id")
    assert unsafe.returncode == 2
    assert "unsupported branch name" in unsafe.stderr
