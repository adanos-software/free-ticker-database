#!/usr/bin/env bash
# Clear pull_request CI runs that GitHub holds for github-actions[bot].
#
# A pull request opened with the workflow token starts ci.yml on
# pull_request and leaves that run action_required: approval is required
# and no jobs start. The automation workflow also dispatches ci.yml on the
# same branch. That workflow_dispatch run is the required "test" check
# (GitHub Actions app) that auto-merge waits for. Nobody approves the held
# pull_request run, so when the PR is merged or closed GitHub completes it
# as failure with zero jobs.
#
# Approve is attempted first (POST /repos/{owner}/{repo}/actions/runs/{id}/approve).
# The workflow token usually cannot approve a run it caused. When approve
# does not succeed and the run is still action_required or waiting, the run
# is cancelled so it ends as cancelled instead of failure. Cancel does not
# create check runs, so it does not replace the dispatched "test" result.
#
# ci.yml keeps its pull_request trigger. Draft review PRs are not dispatched,
# and a later human push to an automation branch must still run CI. Ignoring
# automation/** on pull_request would remove that path.
#
# Caller needs actions: write, which the automation workflows already grant
# so they can dispatch ci.yml.
set -euo pipefail

if [[ $# -ne 1 || -z "${1}" ]]; then
  echo "usage: resolve_held_bot_pr_runs.sh <branch>" >&2
  exit 2
fi

branch="$1"
repo="${GITHUB_REPOSITORY:-}"
workflow="${HELD_CI_WORKFLOW:-ci.yml}"
attempts="${HELD_RUN_POLL_ATTEMPTS:-12}"
delay="${HELD_RUN_POLL_DELAY_SECONDS:-5}"

if [[ -z "$repo" ]]; then
  echo "GITHUB_REPOSITORY is required" >&2
  exit 2
fi
if [[ ! "$branch" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  echo "unsupported branch name: ${branch}" >&2
  exit 2
fi
if [[ ! "$repo" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
  echo "unsupported GITHUB_REPOSITORY: ${repo}" >&2
  exit 2
fi
if [[ ! "$workflow" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "unsupported workflow: ${workflow}" >&2
  exit 2
fi
if [[ ! "$attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "HELD_RUN_POLL_ATTEMPTS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$delay" =~ ^[0-9]+$ ]]; then
  echo "HELD_RUN_POLL_DELAY_SECONDS must be a non-negative integer" >&2
  exit 2
fi

# Branch characters are restricted above, so this filter cannot break out of jq.
jq_filter=".workflow_runs[] | select(.actor.login == \"github-actions[bot]\" and .event == \"pull_request\" and .head_branch == \"${branch}\") | .id"

list_ids_for_status() {
  local status="$1"
  gh api --paginate --method GET \
    "repos/${repo}/actions/workflows/${workflow}/runs" \
    -f "branch=${branch}" \
    -f event=pull_request \
    -f "status=${status}" \
    -f per_page=100 \
    --jq "$jq_filter"
}

declare -A handled=()
unresolved=0
listed_ok=0
attempt=1

while [[ "$attempt" -le "$attempts" ]]; do
  new_ids=0
  list_failed=0
  for status in action_required waiting; do
    ids=""
    if ! ids="$(list_ids_for_status "$status")"; then
      echo "Failed to list ${status} pull_request CI runs for ${branch}" >&2
      list_failed=1
      continue
    fi
    listed_ok=1
    while IFS= read -r id; do
      [[ -z "$id" ]] && continue
      if [[ ! "$id" =~ ^[0-9]+$ ]]; then
        echo "unexpected run id: ${id}" >&2
        exit 2
      fi
      if [[ -n "${handled[$id]:-}" ]]; then
        continue
      fi
      new_ids=1
      if gh api --method POST --silent "repos/${repo}/actions/runs/${id}/approve"; then
        echo "Approved held CI run ${id} on ${branch}"
        handled[$id]=approved
        continue
      fi
      echo "Approve failed for held CI run ${id} on ${branch}" >&2
      current=""
      if ! current="$(gh api --method GET "repos/${repo}/actions/runs/${id}" --jq .status)"; then
        echo "Could not read status for CI run ${id}" >&2
        handled[$id]=failed
        unresolved=$((unresolved + 1))
        continue
      fi
      if [[ -z "$current" ]]; then
        echo "Empty status for CI run ${id}" >&2
        handled[$id]=failed
        unresolved=$((unresolved + 1))
        continue
      fi
      if [[ "$current" != "action_required" && "$current" != "waiting" ]]; then
        echo "CI run ${id} is ${current} after the failed approve; leaving it"
        handled[$id]=cleared
        continue
      fi
      if gh run cancel "$id" --repo "$repo"; then
        echo "Cancelled held CI run ${id} on ${branch}"
        handled[$id]=cancelled
      else
        echo "Could not cancel held CI run ${id} on ${branch}" >&2
        handled[$id]=failed
        unresolved=$((unresolved + 1))
      fi
    done <<< "$ids"
  done

  if [[ "$new_ids" -eq 1 && "$attempt" -lt "$attempts" ]]; then
    sleep "$delay"
    attempt=$((attempt + 1))
    continue
  fi
  if [[ "$new_ids" -eq 0 && "${#handled[@]}" -gt 0 ]]; then
    break
  fi
  if [[ "$attempt" -ge "$attempts" ]]; then
    break
  fi
  # List misses and an empty result both wait for the pull_request run to appear.
  if [[ "$list_failed" -eq 1 || "$new_ids" -eq 0 ]]; then
    sleep "$delay"
  fi
  attempt=$((attempt + 1))
done

if [[ "$unresolved" -gt 0 ]]; then
  echo "Left ${unresolved} held CI run(s) unresolved on ${branch}" >&2
  exit 1
fi
if [[ "$listed_ok" -eq 0 ]]; then
  echo "Could not list pull_request CI runs for ${branch}" >&2
  exit 1
fi
if [[ "${#handled[@]}" -eq 0 ]]; then
  echo "No held pull_request CI runs for ${branch}"
fi
