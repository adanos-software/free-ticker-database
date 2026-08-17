"""Apply idempotent README documentation for canonical-v4 quality profiles."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
START = "<!-- canonical-v4-quality:start -->"
END = "<!-- canonical-v4-quality:end -->"
SECTION = f"""{START}
## Canonical v4 and release truth

`listing_key` is the collision-safe **current venue/symbol key**. It is not a permanent historical identifier because symbols can change or be reused. `data/canonical_v4/listings.csv` separates listing lifecycles from instruments and venues, while source observations and assertion tables preserve the evidence behind accepted values.

The quality contract is cumulative: `merge` blocks structural, identity, history, safe-merge, source-governance, and canonical-schema failures; `stable` additionally requires passing official-full coverage, verified contributing-source rights, complete field provenance, and MIC mappings; `complete` additionally requires zero metadata and official-reference gaps. A green merge check never claims that the database is already complete or legally ready for a stable data release.

Canonical source is committed as ordinary reviewable files. CI rejects compressed source payloads, workflow-time patching, and self-pushing workflows. It validates the canonical CSV contract, loads the result into PostgreSQL, and verifies deterministic repeat builds.

Operational rebuilds use:

```bash
python scripts/rebuild_canonical.py
```

Direct execution of `scripts/rebuild_dataset.py` remains available only for compatibility-export validation.
{END}
"""


def update() -> bool:
    text = README.read_text(encoding="utf-8")
    text = text.replace(
        "`core_listings.csv` is the canonical core security export; `listing_key` is the stable identity.",
        "`core_listings.csv` is the canonical core security export; `listing_key` is its collision-safe current venue/symbol key.",
    )
    if START in text and END in text:
        before, tail = text.split(START, 1)
        _, after = tail.split(END, 1)
        updated = before + SECTION + after.lstrip("\n")
    else:
        marker = "## Quality\n"
        if marker not in text:
            raise RuntimeError("README quality heading not found")
        updated = text.replace(marker, SECTION + "\n" + marker, 1)
    changed = updated != text
    README.write_text(updated, encoding="utf-8")
    return changed


def main() -> None:
    print("updated" if update() else "unchanged")


if __name__ == "__main__":
    main()
