#!/usr/bin/env bash
# Install Playwright Chromium and the apt packages it needs.
#
# GitHub-hosted Ubuntu lists azure.archive.ubuntu.com first in
# /etc/apt/apt-mirrors.txt. When that name fails DNS, `playwright install
# --with-deps` still treats apt-get update as success (warnings only) and then
# apt-get install of the Chromium font packages exits 100
# ("Unable to fetch some archives"). The daily masterfile rotation stops there.
# Run 43034415289 failed this way on both attempts (2026-11-11); other hosts
# such as packages.microsoft.com resolved, and apt never finished the install
# from the later mirrors in the list.
#
# Retry with backoff. After the first failure, drop the Azure mirror when
# another Ubuntu mirror is listed (otherwise rewrite that line to
# archive.ubuntu.com) and refresh indexes, so the next attempt is not the
# same dead lookup. The first attempt still uses the runner's default mirror.
#
# PLAYWRIGHT_INSTALL_CMD and APT_GET_UPDATE_CMD override the real commands
# so the retry loop can be proved with a stub. Production leaves them unset.
set -euo pipefail

mirror_file="${APT_MIRRORS_FILE:-/etc/apt/apt-mirrors.txt}"
attempts="${PLAYWRIGHT_INSTALL_ATTEMPTS:-4}"
delay="${PLAYWRIGHT_INSTALL_DELAY_SECONDS:-15}"

if ! [[ "$attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "PLAYWRIGHT_INSTALL_ATTEMPTS must be a positive integer, got: ${attempts}" >&2
  exit 2
fi
if ! [[ "$delay" =~ ^[0-9]+$ ]]; then
  echo "PLAYWRIGHT_INSTALL_DELAY_SECONDS must be a non-negative integer, got: ${delay}" >&2
  exit 2
fi

run_install() {
  if [ -n "${PLAYWRIGHT_INSTALL_CMD:-}" ]; then
    bash -c "$PLAYWRIGHT_INSTALL_CMD"
  else
    python -m playwright install --with-deps chromium
  fi
}

run_apt_update() {
  if [ -n "${APT_GET_UPDATE_CMD:-}" ]; then
    bash -c "$APT_GET_UPDATE_CMD"
  elif [ "$(id -u)" -eq 0 ]; then
    apt-get update
  else
    sudo -n apt-get update
  fi
}

write_mirror_file() {
  local tmp="$1"
  local status=0
  if [ -w "$mirror_file" ]; then
    cat "$tmp" > "$mirror_file" || status=$?
  else
    sudo -n cp "$tmp" "$mirror_file" || status=$?
  fi
  rm -f "$tmp"
  return "$status"
}

# Drop or replace azure.archive.ubuntu.com. archive.ubuntu.com is a substring
# of that host, so match the URL host rather than the bare domain.
rewrite_azure_mirror() {
  if [ ! -f "$mirror_file" ] || ! grep -q 'azure\.archive\.ubuntu\.com' "$mirror_file"; then
    return 0
  fi
  local tmp
  tmp="$(mktemp)"
  if grep -Eq 'https?://(archive|security)\.ubuntu\.com/' "$mirror_file"; then
    echo "Dropping azure.archive.ubuntu.com from ${mirror_file}; retries will use the remaining Ubuntu mirrors" >&2
    grep -v 'azure\.archive\.ubuntu\.com' "$mirror_file" > "$tmp" || true
  else
    echo "Replacing azure.archive.ubuntu.com in ${mirror_file} with archive.ubuntu.com" >&2
    sed -E 's|https?://azure\.archive\.ubuntu\.com/|https://archive.ubuntu.com/|g' "$mirror_file" > "$tmp"
  fi
  if [ ! -s "$tmp" ]; then
    rm -f "$tmp"
    echo "Refusing to empty ${mirror_file}" >&2
    return 1
  fi
  write_mirror_file "$tmp"
}

attempt=1
while [ "$attempt" -le "$attempts" ]; do
  if run_install; then
    exit 0
  else
    status=$?
  fi
  echo "playwright install --with-deps chromium failed (attempt ${attempt}/${attempts}, exit ${status})" >&2
  if [ "$attempt" -eq "$attempts" ]; then
    exit "$status"
  fi
  rewrite_azure_mirror || echo "Could not update ${mirror_file}; retrying the install anyway" >&2
  echo "Refreshing apt indexes, then retrying in ${delay}s" >&2
  run_apt_update || echo "apt-get update failed; continuing to retry the install" >&2
  if [ "$delay" -gt 0 ]; then
    sleep "$delay"
  fi
  attempt=$((attempt + 1))
  delay=$((delay * 2))
done
