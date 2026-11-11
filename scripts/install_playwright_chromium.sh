#!/usr/bin/env bash
# Install Playwright's Chromium build for the masterfile rotation.
#
# ubuntu-latest (24.04) already ships Chromium's shared libraries because
# Google Chrome is preinstalled. `playwright install --with-deps chromium`
# still runs apt-get install for font packages that are not on the image
# (fonts-ipafont-gothic, fonts-freefont-ttf, fonts-tlwg-loma-otf,
# fonts-unifont, fonts-wqy-zenhei, xfonts-*). The browser binary itself is
# downloaded from Playwright's CDN, not from apt.
#
# On runs 43034415289 and 43050621958 every Ubuntu apt host failed DNS for
# hours: azure.archive.ubuntu.com, then archive.ubuntu.com and
# security.ubuntu.com after the Azure line was removed. Rewriting mirrors
# cannot help when Ubuntu apt does not resolve at all. packages.microsoft.com
# still resolved, which is why apt-get update looked half-successful.
#
# Fast path: `python -m playwright install chromium` (no --with-deps), then
# launch that browser headless the same way the fetchers do
# (`chromium.launch(headless=True)`, open a page, set_content, read it back,
# close). Masterfile fetches read DOM HTML, in-page fetch() JSON, cookies,
# request headers, or PDF bytes. None of them screenshot or OCR rendered
# glyphs, so the extra apt fonts do not change scraped fields. fonts-liberation
# and fonts-noto-color-emoji are already on the runner image.
#
# Only if that launch fails do we fall back to `playwright install --with-deps`
# and the mirror-rewrite retry loop. That loop still matters on an image that
# is missing shared libraries.
#
# Stub overrides, all unset in production:
# PLAYWRIGHT_INSTALL_NO_DEPS_CMD is the fast-path browser download.
# PLAYWRIGHT_SMOKE_CMD is the fast-path headless launch.
# PLAYWRIGHT_INSTALL_CMD is the --with-deps fallback.
# APT_GET_UPDATE_CMD is apt-get update between fallback attempts.
# Tests that set only PLAYWRIGHT_INSTALL_CMD and APT_GET_UPDATE_CMD keep
# driving the fallback loop and do not run the fast path.
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

run_install_nodeps() {
  if [ -n "${PLAYWRIGHT_INSTALL_NO_DEPS_CMD:-}" ]; then
    bash -c "$PLAYWRIGHT_INSTALL_NO_DEPS_CMD"
  else
    python -m playwright install chromium
  fi
}

# Same launch shape as Bolsa de Santiago, TASE, Bursa, and SET stock search:
# bundled Chromium, headless, then a page. set_content plus page.content()
# stands in for those fetchers' DOM / in-page fetch work without touching the
# network. EGX prefers channel="chrome" and falls back to this same binary.
run_smoke() {
  if [ -n "${PLAYWRIGHT_SMOKE_CMD:-}" ]; then
    bash -c "$PLAYWRIGHT_SMOKE_CMD"
    return
  fi
  python - <<'PY'
from playwright.sync_api import sync_playwright

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    try:
        page = browser.new_page()
        page.set_content("<!doctype html><title>playwright-smoke</title><p>ok</p>")
        html = page.content()
    finally:
        browser.close()

if "playwright-smoke" not in html or ">ok<" not in html:
    raise SystemExit("headless Chromium did not return the smoke document")
PY
}

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

# Legacy stub tests set PLAYWRIGHT_INSTALL_CMD and leave the fast-path
# overrides unset so they exercise only the fallback loop.
if [ -z "${PLAYWRIGHT_INSTALL_CMD:-}" ] || [ -n "${PLAYWRIGHT_INSTALL_NO_DEPS_CMD:-}" ] || [ -n "${PLAYWRIGHT_SMOKE_CMD:-}" ]; then
  if run_install_nodeps; then
    nodeps_status=0
  else
    nodeps_status=$?
  fi
  if [ "$nodeps_status" -eq 0 ]; then
    echo "Smoke-testing headless Chromium (launch, set_content, close)"
    if run_smoke; then
      echo "Playwright Chromium launched headless without apt packages"
      exit 0
    fi
    echo "Headless Chromium launch failed; falling back to playwright install --with-deps" >&2
  else
    echo "playwright install chromium (no --with-deps) failed (exit ${nodeps_status}); falling back to playwright install --with-deps" >&2
  fi
fi

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
